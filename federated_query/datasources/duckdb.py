"""DuckDB data source implementation."""

from typing import List, Dict, Any, Iterator, Optional
import pyarrow as pa
import duckdb

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    ColumnMetadata,
    TableStatistics,
    ColumnStatistics,
)


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
        self.connection = duckdb.connect(self.db_path, read_only=self.read_only)

    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

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
        return [row[0] for row in result]

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
        return [row[0] for row in result]

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
        try:
            # Get row count
            result = self.connection.execute(
                f"SELECT COUNT(*) FROM {schema}.{table}"
            ).fetchone()
            row_count = result[0] if result else 0

            # Get approximate distinct counts for each column
            # This is expensive, so we might want to cache or sample
            metadata = self.get_table_metadata(schema, table)

            column_stats = {}
            for col in metadata.columns:
                try:
                    result = self.connection.execute(
                        f"SELECT COUNT(DISTINCT {col.name}), COUNT(*) - COUNT({col.name}) FROM {schema}.{table}"
                    ).fetchone()

                    n_distinct = result[0] if result else 0
                    null_count = result[1] if result else 0
                    null_fraction = null_count / row_count if row_count > 0 else 0.0

                    column_stats[col.name] = ColumnStatistics(
                        num_distinct=n_distinct,
                        null_fraction=null_fraction,
                        avg_width=10,  # Placeholder
                    )
                except Exception:
                    # Skip if we can't get stats for this column
                    pass

            return TableStatistics(
                row_count=row_count,
                total_size_bytes=row_count * 100,  # Rough estimate
                column_stats=column_stats,
            )

        except Exception as e:
            return None

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and yield Arrow record batches."""
        # DuckDB has excellent Arrow support
        result = self.connection.execute(query)

        # Fetch in batches
        while True:
            batch = result.fetch_record_batch(rows_per_batch=10000)
            if batch is None or len(batch) == 0:
                break
            yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get query schema without executing."""
        # DuckDB can describe queries
        result = self.connection.execute(f"DESCRIBE {query}")
        rows = result.fetchall()

        fields = []
        for row in rows:
            col_name = row[0]
            col_type = row[1]
            # TODO: Proper type mapping from DuckDB to Arrow
            fields.append(pa.field(col_name, pa.string()))

        return pa.schema(fields)
