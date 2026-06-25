"""ClickHouse data source — Arrow-native streaming via clickhouse-connect.

ClickHouse returns query results as Arrow directly (over HTTP), so the data path
hands the engine a streaming ``RecordBatchReader`` with no per-row Python — the
same shape as the ADBC PostgreSQL path. This makes ClickHouse a clean, fast
remote source for the federated engine (and a fair benchmark fact store, since
neither engine gets it "for free" the way a local DuckDB table is).
"""

from typing import List, Dict, Any, Iterator, Optional
import logging

import pyarrow as pa

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    ColumnMetadata,
    TableStatistics,
    ColumnStatistics,
)

logger = logging.getLogger(__name__)

# ClickHouse column type -> the SQL type name the binder/catalog expects, kept
# consistent with the duckdb/postgres connectors. Wrappers (Nullable, etc.) and
# parameters (DateTime64(3), FixedString(16)) are stripped before lookup.
_CH_TO_SQL = {
    "Int8": "SMALLINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "Int128": "BIGINT",
    "Int256": "BIGINT",
    "UInt8": "SMALLINT",
    "UInt16": "INTEGER",
    "UInt32": "BIGINT",
    "UInt64": "BIGINT",
    "Float32": "REAL",
    "Float64": "DOUBLE",
    "Decimal": "DOUBLE",
    "String": "VARCHAR",
    "FixedString": "VARCHAR",
    "UUID": "VARCHAR",
    "Enum8": "VARCHAR",
    "Enum16": "VARCHAR",
    "Date": "DATE",
    "Date32": "DATE",
    "DateTime": "TIMESTAMP",
    "DateTime64": "TIMESTAMP",
    "Bool": "BOOLEAN",
}


def _strip_ch_type(ch_type: str) -> str:
    """Reduce a ClickHouse type to its base name (drop Nullable/params)."""
    base = ch_type
    for wrapper in ("Nullable(", "LowCardinality("):
        if base.startswith(wrapper):
            base = base[len(wrapper) : -1]
    paren = base.find("(")
    if paren != -1:
        base = base[:paren]
    return base


class ClickHouseDataSource(DataSource):
    """ClickHouse connector that fetches results as streamed Arrow batches."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """Config: host, port (HTTP, default 8123), user, password, database."""
        super().__init__(name, config)
        self._client = None
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 8123)
        self.username = config.get("user", "default")
        self.password = config.get("password", "")
        self.database = config.get("database", "default")

    def connect(self) -> None:
        """Open the ClickHouse HTTP client."""
        import clickhouse_connect

        logger.info(f"Connecting to ClickHouse at {self.host}:{self.port}")
        # No sticky session id: the engine issues overlapping requests (a schema
        # probe, then a streamed scan, and concurrent scans across a join), and a
        # shared ClickHouse session serializes them and raises SESSION_IS_LOCKED.
        # Without a session each HTTP query is independent over the client's pool.
        self._client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            autogenerate_session_id=False,
        )
        self._connected = True

    def disconnect(self) -> None:
        """Close the ClickHouse client."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._connected = False

    def get_capabilities(self) -> List[DataSourceCapability]:
        """ClickHouse supports the full SQL feature set the engine pushes down."""
        return [
            DataSourceCapability.AGGREGATIONS,
            DataSourceCapability.JOINS,
            DataSourceCapability.WINDOW_FUNCTIONS,
            DataSourceCapability.SUBQUERIES,
            DataSourceCapability.CTE,
            DataSourceCapability.DISTINCT,
            DataSourceCapability.LIMIT,
            DataSourceCapability.ORDER_BY,
        ]

    def list_schemas(self) -> List[str]:
        """List ClickHouse databases (a database is the engine's 'schema')."""
        rows = self._client.query("SELECT name FROM system.databases").result_rows
        schemas = []
        for row in rows:
            schemas.append(row[0])
        return schemas

    def list_tables(self, schema: str) -> List[str]:
        """List base tables in a ClickHouse database."""
        rows = self._client.query(
            "SELECT name FROM system.tables WHERE database = {s:String} ORDER BY name",
            parameters={"s": schema},
        ).result_rows
        tables = []
        for row in rows:
            tables.append(row[0])
        return tables

    def get_table_metadata(self, schema: str, table: str) -> TableMetadata:
        """Column metadata from system.columns, types mapped to engine SQL types."""
        rows = self._client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {d:String} AND table = {t:String} ORDER BY position",
            parameters={"d": schema, "t": table},
        ).result_rows
        columns = []
        for name, ch_type in rows:
            sql_type = _CH_TO_SQL.get(_strip_ch_type(ch_type), "VARCHAR")
            columns.append(
                ColumnMetadata(
                    name=name,
                    data_type=sql_type,
                    nullable=ch_type.startswith("Nullable("),
                )
            )
        return TableMetadata(schema_name=schema, table_name=table, columns=columns)

    def get_table_statistics(
        self, schema: str, table: str
    ) -> Optional[TableStatistics]:
        """Row count + per-column distinct/null fraction (one scan in ClickHouse)."""
        metadata = self.get_table_metadata(schema, table)
        row = self._client.query(
            self._statistics_query(schema, table, metadata)
        ).result_rows[0]
        row_count = row[0]
        column_stats = self._column_statistics(metadata, row, row_count)
        return TableStatistics(
            row_count=row_count,
            total_size_bytes=row_count * 100,
            column_stats=column_stats,
        )

    def _statistics_query(self, schema, table, metadata: TableMetadata) -> str:
        """Build one query: count() plus uniqExact/nulls for every column."""
        parts = ["count()"]
        for column in metadata.columns:
            quoted = f'"{column.name}"'
            parts.append(f"uniqExact({quoted})")
            parts.append(f"countIf({quoted} IS NULL)")
        return f'SELECT {", ".join(parts)} FROM "{schema}"."{table}"'

    def _column_statistics(
        self, metadata: TableMetadata, row, row_count: int
    ) -> Dict[str, ColumnStatistics]:
        """Unpack the stats row (count, then distinct/null pairs per column)."""
        stats: Dict[str, ColumnStatistics] = {}
        index = 1
        for column in metadata.columns:
            distinct, nulls = row[index], row[index + 1]
            index += 2
            null_fraction = nulls / row_count if row_count > 0 else 0.0
            stats[column.name] = ColumnStatistics(
                num_distinct=distinct, null_fraction=null_fraction, avg_width=10
            )
        return stats

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Stream the result as Arrow batches; ClickHouse drives the read."""
        logger.debug(f"Executing query (clickhouse) on {self.name}: {query[:100]}...")
        with self._client.query_arrow_stream(query) as reader:
            for batch in reader:
                yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Real Arrow schema of a query, via a zero-row probe."""
        table = self._client.query_arrow(f"SELECT * FROM ({query}) AS q LIMIT 0")
        return table.schema
