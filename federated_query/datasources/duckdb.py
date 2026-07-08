"""DuckDB data source implementation."""

from typing import List, Dict, Any, Iterator, Optional
import pyarrow as pa
import duckdb
import logging

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    TableStatistics,
    ColumnStatistics,
)

logger = logging.getLogger(__name__)


class DuckDBDataSource(DataSource):
    """DuckDB data source connector."""

    render_dialect = "duckdb"

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize DuckDB data source.

        Config should include:
            - path: Path to DuckDB database file (or :memory: for in-memory)
            - read_only: Whether to open in read-only mode (default: True)
        """
        super().__init__(name, config)
        self.connection = None
        self.db_path = config.get("path", ":memory:")
        self.read_only = config.get("read_only", True)

    def connect(self) -> None:
        """Establish connection to DuckDB."""
        logger.info(f"Connecting to DuckDB at '{self.db_path}'")
        self.connection = duckdb.connect(self.db_path, read_only=self.read_only)
        self._connected = True
        logger.info(f"Successfully connected to DuckDB: {self.name}")

    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            logger.info(f"Disconnected from DuckDB: {self.name}")
            self.connection = None
            self._connected = False

    def get_capabilities(self) -> List[DataSourceCapability]:
        """DuckDB supports most SQL features."""
        return [
            DataSourceCapability.AGGREGATIONS,
            DataSourceCapability.JOINS,
            DataSourceCapability.WINDOW_FUNCTIONS,
            DataSourceCapability.SUBQUERIES,
            DataSourceCapability.CTE,
            DataSourceCapability.DISTINCT,
            DataSourceCapability.LIMIT,
            DataSourceCapability.ORDER_BY,
            DataSourceCapability.SHIP_TARGET,
        ]

    def list_schemas(self) -> List[str]:
        """List available schemas."""
        result = self.connection.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
        schemas = []
        for row in result:
            schemas.append(row[0])
        return schemas

    def list_tables(self, schema: str) -> List[str]:
        """List tables in a schema."""
        result = self.connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            [schema],
        ).fetchall()
        tables = []
        for row in result:
            tables.append(row[0])
        return tables

    def get_table_metadata(self, schema: str, table: str) -> TableMetadata:
        """Get table metadata."""
        result = self.connection.execute(
            """
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()

        return self._metadata_from_information_schema(schema, table, result)

    def get_table_statistics(
        self, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """Catalog row count plus one approximate stats scan for the requested
        columns only - never a full COUNT(DISTINCT) pass over every column."""
        row_count = self._catalog_row_count(schema, table)
        column_stats = self._approx_column_statistics(schema, table, columns, row_count)
        return self._build_table_statistics(row_count, column_stats)

    def _catalog_row_count(self, schema: str, table: str) -> int:
        """The table's row count from duckdb_tables() - a catalog read, not a
        scan (exact for native tables). Raises for a table DuckDB does not know:
        answering with a guess would silently mis-cost every plan over it."""
        result = self.connection.execute(
            "SELECT estimated_size FROM duckdb_tables()"
            " WHERE schema_name = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        if result is None:
            raise ValueError(
                f"duckdb_tables() knows no table {schema}.{table} on {self.name}"
            )
        return result[0]

    def _approx_column_statistics(
        self, schema: str, table: str, columns: List[str], row_count: int
    ) -> Dict[str, ColumnStatistics]:
        """Approximate NDV / nulls / min / max for the requested columns, all
        gathered in ONE vectorized aggregate scan over the table."""
        if not columns:
            return {}
        row = self.connection.execute(
            self._column_stats_query(schema, table, columns)
        ).fetchone()
        return self._unpack_column_stats(columns, row, row_count)

    def _column_stats_query(self, schema: str, table: str, columns: List[str]) -> str:
        """Build the single aggregate scan: 4 measures per requested column."""
        parts = []
        for column in columns:
            quoted = f'"{column}"'
            parts.append(f"approx_count_distinct({quoted})")
            parts.append(f"count({quoted})")
            parts.append(f"min({quoted})")
            parts.append(f"max({quoted})")
        return f'SELECT {", ".join(parts)} FROM "{schema}"."{table}"'

    def _unpack_column_stats(
        self, columns: List[str], row, row_count: int
    ) -> Dict[str, ColumnStatistics]:
        """Unpack the stats row (ndv, non-null count, min, max per column)."""
        stats: Dict[str, ColumnStatistics] = {}
        for index, column in enumerate(columns):
            ndv, non_null, min_value, max_value = row[index * 4 : index * 4 + 4]
            null_fraction = (row_count - non_null) / row_count if row_count > 0 else 0.0
            # Per-column catalog statistics from the single approximate scan;
            # avg_width stays a placeholder (DuckDB exposes no cheap width).
            stats[column] = ColumnStatistics.create(
                num_distinct=ndv,
                null_fraction=null_fraction,
                avg_width=10,
                min_value=min_value,
                max_value=max_value,
            )
        return stats

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and yield Arrow record batches."""
        logger.debug(f"Executing query on {self.name}: {query[:100]}...")
        result = self.connection.execute(query)
        arrow_table = result.to_arrow_table()

        for batch in arrow_table.to_batches(max_chunksize=self._fetch_batch_size):
            yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get a query's real Arrow schema without materializing rows.

        Running the query under ``LIMIT 0`` lets DuckDB report the exact
        column types (int, double, timestamp, ...). Typing every column as
        mismatched the executed data and crashed FULL OUTER joins.
        """
        result = self.connection.execute(self._schema_probe_sql(query))
        empty_table = result.to_arrow_table()
        return empty_table.schema
