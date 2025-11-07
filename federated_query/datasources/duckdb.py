"""DuckDB data source implementation."""

from typing import List, Dict, Any, Iterator, Optional
import pyarrow as pa
import duckdb
import logging

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    ColumnMetadata,
    TableStatistics,
    ColumnStatistics,
)

logger = logging.getLogger(__name__)


class DuckDBDataSource(DataSource):
    """DuckDB data source connector."""

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

        columns = []
        for row in result:
            columns.append(
                ColumnMetadata(
                    name=row[0],
                    data_type=row[1],
                    nullable=row[2] == "YES",
                )
            )

        return TableMetadata(schema_name=schema, table_name=table, columns=columns)

    def get_table_statistics(
        self, schema: str, table: str
    ) -> Optional[TableStatistics]:
        """Get table statistics."""
        result = self.connection.execute(
            f"SELECT COUNT(*) FROM {schema}.{table}"
        ).fetchone()
        row_count = result[0] if result else 0

        metadata = self.get_table_metadata(schema, table)
        column_stats = self._collect_column_statistics(schema, table, metadata, row_count)

        return TableStatistics(
            row_count=row_count,
            total_size_bytes=row_count * 100,
            column_stats=column_stats,
        )

    def _collect_column_statistics(
        self, schema: str, table: str, metadata: TableMetadata, row_count: int
    ) -> Dict[str, ColumnStatistics]:
        """Collect statistics for each column."""
        column_stats = {}
        for col in metadata.columns:
            stats = self._get_single_column_stats(schema, table, col.name, row_count)
            if stats:
                column_stats[col.name] = stats
        return column_stats

    def _get_single_column_stats(
        self, schema: str, table: str, col_name: str, row_count: int
    ) -> Optional[ColumnStatistics]:
        """Get statistics for a single column."""
        result = self.connection.execute(
            f"SELECT COUNT(DISTINCT {col_name}), COUNT(*) - COUNT({col_name}) FROM {schema}.{table}"
        ).fetchone()

        n_distinct = result[0] if result else 0
        null_count = result[1] if result else 0
        null_fraction = null_count / row_count if row_count > 0 else 0.0

        return ColumnStatistics(
            num_distinct=n_distinct,
            null_fraction=null_fraction,
            avg_width=10,
        )

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and yield Arrow record batches."""
        logger.debug(f"Executing query on {self.name}: {query[:100]}...")
        result = self.connection.execute(query)
        arrow_table = result.fetch_arrow_table()

        batch_size = 10000
        for batch in arrow_table.to_batches(max_chunksize=batch_size):
            yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get query schema without executing."""
        result = self.connection.execute(f"DESCRIBE {query}")
        rows = result.fetchall()

        fields = []
        for row in rows:
            col_name = row[0]
            col_type = row[1]
            fields.append(pa.field(col_name, pa.string()))

        return pa.schema(fields)
