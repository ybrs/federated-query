"""PostgreSQL data source implementation."""

from typing import List, Dict, Any, Iterator, Optional
import pyarrow as pa
import psycopg2
from psycopg2.extras import RealDictCursor

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    ColumnMetadata,
    TableStatistics,
    ColumnStatistics,
)


class PostgreSQLDataSource(DataSource):
    """PostgreSQL data source connector."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize PostgreSQL data source.

        Config should include:
            - host: Database host
            - port: Database port
            - database: Database name
            - user: Username
            - password: Password
            - schemas: List of schemas to include (optional)
        """
        super().__init__(name, config)
        self.connection = None
        self.schemas = config.get("schemas", ["public"])

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        self.connection = psycopg2.connect(
            host=self.config["host"],
            port=self.config.get("port", 5432),
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_capabilities(self) -> List[DataSourceCapability]:
        """PostgreSQL supports most SQL features."""
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
        return self.schemas

    def list_tables(self, schema: str) -> List[str]:
        """List tables in a schema."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            return [row[0] for row in cursor.fetchall()]

    def get_table_metadata(self, schema: str, table: str) -> TableMetadata:
        """Get table metadata from information_schema."""
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get columns
            cursor.execute(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )

            columns = []
            for row in cursor.fetchall():
                columns.append(
                    ColumnMetadata(
                        name=row["column_name"],
                        data_type=row["data_type"],
                        nullable=row["is_nullable"] == "YES",
                    )
                )

            return TableMetadata(
                schema_name=schema, table_name=table, columns=columns
            )

    def get_table_statistics(
        self, schema: str, table: str
    ) -> Optional[TableStatistics]:
        """Get table statistics from pg_stats."""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get row count
                cursor.execute(
                    f"SELECT reltuples::bigint as row_count FROM pg_class WHERE relname = %s",
                    (table,),
                )
                row = cursor.fetchone()
                row_count = row["row_count"] if row else 0

                # Get column statistics
                cursor.execute(
                    """
                    SELECT
                        attname as column_name,
                        n_distinct,
                        null_frac
                    FROM pg_stats
                    WHERE schemaname = %s AND tablename = %s
                    """,
                    (schema, table),
                )

                column_stats = {}
                for row in cursor.fetchall():
                    col_name = row["column_name"]
                    # n_distinct is negative for fraction of total rows, positive for absolute count
                    n_distinct = row["n_distinct"]
                    if n_distinct < 0:
                        n_distinct = int(abs(n_distinct) * row_count)
                    else:
                        n_distinct = int(n_distinct)

                    column_stats[col_name] = ColumnStatistics(
                        num_distinct=n_distinct,
                        null_fraction=row["null_frac"] or 0.0,
                        avg_width=10,  # Placeholder
                    )

                return TableStatistics(
                    row_count=row_count,
                    total_size_bytes=row_count * 100,  # Rough estimate
                    column_stats=column_stats,
                )

        except Exception as e:
            # Statistics may not be available
            return None

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and yield Arrow record batches."""
        # TODO: Implement streaming with Arrow
        # For now, fetch all and convert
        with self.connection.cursor() as cursor:
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch in batches
            batch_size = 10000
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Convert to Arrow
                # TODO: Proper type mapping
                data = {col: [] for col in columns}
                for row in rows:
                    for i, col in enumerate(columns):
                        data[col].append(row[i])

                batch = pa.RecordBatch.from_pydict(data)
                yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get query schema without executing."""
        # Use LIMIT 0 to get schema without data
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM ({query}) AS q LIMIT 0")
            columns = [desc[0] for desc in cursor.description]

            # TODO: Proper type mapping from PostgreSQL to Arrow
            fields = [pa.field(col, pa.string()) for col in columns]
            return pa.schema(fields)
