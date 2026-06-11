"""PostgreSQL data source implementation."""

from contextlib import contextmanager
from typing import List, Dict, Any, Iterator, Optional
import pyarrow as pa
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
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


class PostgreSQLDataSource(DataSource):
    """PostgreSQL data source connector with connection pooling."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize PostgreSQL data source.

        Config should include:
            - host: Database host
            - port: Database port
            - database: Database name
            - user: Username
            - password: Password
            - schemas: List of schemas to include (optional)
            - min_connections: Minimum connections in pool (default: 1)
            - max_connections: Maximum connections in pool (default: 5)
        """
        super().__init__(name, config)
        self.schemas = config.get("schemas", ["public"])
        self._pool = None
        self._min_connections = config.get("min_connections", 1)
        self._max_connections = config.get("max_connections", 5)

    def connect(self) -> None:
        """Establish connection pool to PostgreSQL.

        psycopg2 errors are allowed to propagate unchanged; wrapping them in
        a ConnectionError would hide the real cause (CLAUDE.md rule #1).
        """
        logger.info(
            f"Connecting to PostgreSQL database '{self.config['database']}' "
            f"at {self.config['host']}"
        )
        # Constructing the pool eagerly opens ``min_connections``, so it fails
        # loudly here if the server is unreachable; no separate smoke test or
        # stored connection is needed (this source is pool-based, not
        # single-connection).
        self._pool = pool.ThreadedConnectionPool(
            self._min_connections,
            self._max_connections,
            host=self.config["host"],
            port=self.config.get("port", 5432),
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )
        self._connected = True
        logger.info(f"Successfully connected to PostgreSQL: {self.name}")

    def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()
            logger.info(f"Disconnected from PostgreSQL: {self.name}")
            self._pool = None
            self._connected = False

    def _get_connection(self):
        """Get a connection from the pool."""
        if not self._pool:
            raise RuntimeError(f"Not connected to {self.name}")
        return self._pool.getconn()

    def _return_connection(self, conn):
        """Return a connection to the pool."""
        if self._pool:
            self._pool.putconn(conn)

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Yield a pooled connection, returning it to the pool on exit.

        Intended for callers that need a raw psycopg2 connection (for
        example, test fixtures issuing DDL) without managing the pool.
        """
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._return_connection(conn)

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
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    (schema,),
                )
                rows = cursor.fetchall()
                tables = []
                for row in rows:
                    tables.append(row[0])
                return tables
        except psycopg2.Error as e:
            logger.error(f"Error listing tables in schema {schema}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def get_table_metadata(self, schema: str, table: str) -> TableMetadata:
        """Get table metadata from information_schema."""
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
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
        except psycopg2.Error as e:
            logger.error(f"Error getting metadata for {schema}.{table}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def get_table_statistics(
        self, schema: str, table: str
    ) -> Optional[TableStatistics]:
        """Get table statistics from pg_stats."""
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
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
        finally:
            self._return_connection(conn)

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute query and yield Arrow record batches."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                logger.debug(f"Executing query on {self.name}: {query[:100]}...")
                cursor.execute(query)

                columns = self._extract_column_names(cursor.description)
                schema = pa.schema(self._build_arrow_fields(cursor.description))

                batch_size = 10000
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    data = self._build_column_data(columns, rows)
                    self._coerce_floats(data, schema)
                    batch = pa.RecordBatch.from_pydict(data, schema=schema)
                    yield batch
        except psycopg2.Error as e:
            logger.error(f"Query execution failed on {self.name}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get query schema without executing."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM ({query}) AS q LIMIT 0")
                fields = self._build_arrow_fields(cursor.description)
                return pa.schema(fields)
        except psycopg2.Error as e:
            logger.error(f"Failed to get query schema: {e}")
            raise
        finally:
            self._return_connection(conn)

    def _extract_column_names(self, description) -> List[str]:
        """Extract column names from cursor description."""
        columns = []
        for desc in description:
            columns.append(desc[0])
        return columns

    # PostgreSQL type OIDs -> Arrow types. NUMERIC maps to float64: the
    # engine computes over floats, so sources surface decimals as doubles.
    _OID_TO_ARROW = {
        16: pa.bool_(),  # bool
        20: pa.int64(),  # int8
        21: pa.int64(),  # int2
        23: pa.int64(),  # int4
        700: pa.float64(),  # float4
        701: pa.float64(),  # float8
        1700: pa.float64(),  # numeric
        25: pa.string(),  # text
        1042: pa.string(),  # bpchar
        1043: pa.string(),  # varchar
        1082: pa.date32(),  # date
        1114: pa.timestamp("us"),  # timestamp
        1184: pa.timestamp("us", tz="UTC"),  # timestamptz
    }

    def _build_arrow_fields(self, description) -> List[pa.Field]:
        """Build typed Arrow fields from a cursor description."""
        fields = []
        for column in description:
            arrow_type = self._OID_TO_ARROW.get(column[1], pa.string())
            fields.append(pa.field(column[0], arrow_type))
        return fields

    def _coerce_floats(self, data: Dict[str, List], schema: pa.Schema) -> None:
        """Convert Decimal values to float for float64-typed columns.

        Arrow refuses to place ``decimal.Decimal`` values into float64
        arrays, and NUMERIC columns surface as float64 in this engine.
        """
        for field in schema:
            if not pa.types.is_float64(field.type):
                continue
            values = data[field.name]
            for index in range(len(values)):
                if values[index] is not None:
                    values[index] = float(values[index])

    def _build_column_data(self, columns: List[str], rows: List) -> Dict[str, List]:
        """Build column data dictionary from rows."""
        data = {}
        for col in columns:
            data[col] = []

        for row in rows:
            for i, col in enumerate(columns):
                data[col].append(row[i])

        return data
