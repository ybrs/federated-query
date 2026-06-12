"""PostgreSQL data source implementation."""

from contextlib import contextmanager
from typing import List, Dict, Any, Iterator, Optional
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
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


def _hex_byte_table() -> np.ndarray:
    """Build the 256-entry table of two-hex-char strings, one per byte value."""
    table = []
    for value in range(256):
        table.append("%02x" % value)
    return np.array(table, dtype="U2")


# Used to format uuid bytes vectorized (no per-row Python uuid/hex call).
_HEX256 = _hex_byte_table()


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
        # Optional Arrow-native data path. ``driver: adbc`` fetches query
        # results through the ADBC PostgreSQL driver (postgres wire -> Arrow,
        # no per-cell Python objects); metadata/stats still use psycopg2.
        self._use_adbc = config.get("driver") == "adbc"
        self._adbc_conn = None

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
        """Close all connections in the pool (and the ADBC connection)."""
        if self._adbc_conn is not None:
            self._adbc_conn.close()
            self._adbc_conn = None
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
        """Execute a query and yield Arrow record batches."""
        if self._use_adbc:
            return self._execute_query_adbc(query)
        return self._execute_query_psycopg2(query)

    def _execute_query_psycopg2(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute via psycopg2, building Arrow batches per fetched chunk."""
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                logger.debug(f"Executing query on {self.name}: {query[:100]}...")
                cursor.execute(query)

                schema = pa.schema(self._build_arrow_fields(cursor.description))

                batch_size = 10000
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    arrays = self._build_column_arrays(rows, schema)
                    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
                    yield batch
        except psycopg2.Error as e:
            logger.error(f"Query execution failed on {self.name}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def _execute_query_adbc(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute via the ADBC driver, returning Arrow directly."""
        logger.debug(f"Executing query (adbc) on {self.name}: {query[:100]}...")
        table = self._adbc_fetch(query)
        for batch in table.to_batches(max_chunksize=10000):
            yield batch

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get a query's Arrow schema without materializing rows."""
        wrapped = f"SELECT * FROM ({query}) AS q LIMIT 0"
        if self._use_adbc:
            return self._adbc_fetch(wrapped).schema
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(wrapped)
                fields = self._build_arrow_fields(cursor.description)
                return pa.schema(fields)
        except psycopg2.Error as e:
            logger.error(f"Failed to get query schema: {e}")
            raise
        finally:
            self._return_connection(conn)

    def _adbc_uri(self) -> str:
        """Build a libpq URI for the ADBC PostgreSQL driver from the config."""
        user = self.config["user"]
        password = self.config.get("password")
        auth = f"{user}:{password}" if password else user
        host = self.config["host"]
        port = self.config.get("port", 5432)
        database = self.config["database"]
        return f"postgresql://{auth}@{host}:{port}/{database}"

    def _adbc_connection(self):
        """Lazily open (and cache) the ADBC connection.

        ``autocommit=True`` keeps read-only fetches from leaving the connection
        idle in a transaction holding locks on the scanned tables.
        """
        if self._adbc_conn is None:
            from adbc_driver_postgresql import dbapi

            self._adbc_conn = dbapi.connect(self._adbc_uri(), autocommit=True)
        return self._adbc_conn

    def _adbc_fetch(self, query: str) -> pa.Table:
        """Run a query through ADBC and normalize its Arrow types."""
        cursor = self._adbc_connection().cursor()
        try:
            cursor.execute(query)
            table = cursor.fetch_arrow_table()
        finally:
            cursor.close()
        return self._normalize_table(table)

    def _normalize_table(self, table: pa.Table) -> pa.Table:
        """Align ADBC's Arrow types with the psycopg2 path's, so the driver is
        a drop-in: uuid->string, numeric->float64, every integer width->int64,
        and real(float32)->float64. Anything already matching is left untouched.
        """
        columns = []
        for index in range(table.num_columns):
            field = table.schema.field(index)
            columns.append(self._normalize_column(table.column(index), field.type))
        return pa.Table.from_arrays(columns, names=table.schema.names)

    def _normalize_column(self, column, column_type):
        """Convert one column from ADBC's type to the engine's expected type."""
        if getattr(column_type, "extension_name", None) == "arrow.opaque":
            return self._normalize_opaque(column, column_type)
        if pa.types.is_decimal(column_type):
            return pc.cast(column, pa.float64())
        if pa.types.is_integer(column_type) and not pa.types.is_int64(column_type):
            return pc.cast(column, pa.int64())
        if pa.types.is_floating(column_type) and not pa.types.is_float64(column_type):
            return pc.cast(column, pa.float64())
        return column

    def _normalize_opaque(self, column, column_type):
        """Convert an ADBC opaque extension column to the engine's type.

        Postgres ``uuid`` arrives as opaque binary and ``numeric`` as an opaque
        string; the engine wants a canonical uuid string and a float64
        respectively (matching psycopg2). Unknown opaque types fall back to
        their string storage.
        """
        type_name = getattr(column_type, "type_name", "")
        storage = column.combine_chunks().storage
        if type_name == "uuid":
            return self._uuid_bytes_to_string(storage)
        if type_name == "numeric":
            return pc.cast(storage, pa.float64())
        return storage

    def _uuid_bytes_to_string(self, storage) -> pa.Array:
        """Format a binary(16) uuid column to canonical strings, vectorized.

        Reshapes the contiguous bytes to (n, 16), maps each byte through the hex
        table, and joins the dashed groups with numpy string ops — no per-row
        Python loop. NULL slots carry garbage bytes, masked back out at the end.
        """
        count = len(storage)
        fixed = pc.cast(storage, pa.binary(16))
        raw = np.frombuffer(fixed.buffers()[1], dtype=np.uint8)
        start = fixed.offset * 16
        matrix = raw[start : start + count * 16].reshape(count, 16)
        text = self._join_uuid_groups(_HEX256[matrix])
        mask = pc.is_null(fixed).to_numpy(zero_copy_only=False)
        return pa.array(text, mask=mask, type=pa.string())

    def _join_uuid_groups(self, pairs: np.ndarray) -> np.ndarray:
        """Join the 8-4-4-4-12 hex groups of each row into a dashed uuid."""
        groups = [(0, 4), (4, 6), (6, 8), (8, 10), (10, 16)]
        rendered = []
        for start, stop in groups:
            rendered.append(self._concat_columns(pairs, start, stop))
        out = rendered[0]
        for group in rendered[1:]:
            out = np.char.add(np.char.add(out, "-"), group)
        return out

    def _concat_columns(self, pairs: np.ndarray, start: int, stop: int) -> np.ndarray:
        """Concatenate the hex-pair columns ``[start, stop)`` row-wise."""
        out = pairs[:, start]
        for index in range(start + 1, stop):
            out = np.char.add(out, pairs[:, index])
        return out

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

    def _build_column_arrays(self, rows: List, schema: pa.Schema) -> List[pa.Array]:
        """Build one Arrow array per column position.

        Indexing by position (not by name) is what lets a result with duplicate
        column names — e.g. ``SELECT *`` over a join where both tables expose an
        ``id`` column — round-trip faithfully instead of collapsing the
        same-named columns together.
        """
        arrays: List[pa.Array] = []
        for column_index in range(len(schema.names)):
            field = schema.field(column_index)
            values = self._column_values(rows, column_index, field)
            arrays.append(pa.array(values, type=field.type))
        return arrays

    def _column_values(self, rows: List, column_index: int, field: pa.Field) -> List:
        """Collect one column's values, coercing Decimal to float for float64.

        Arrow refuses to place ``decimal.Decimal`` values into float64 arrays,
        and NUMERIC columns surface as float64 in this engine.
        """
        coerce = pa.types.is_float64(field.type)
        values = []
        for row in rows:
            value = row[column_index]
            if coerce and value is not None:
                value = float(value)
            values.append(value)
        return values
