"""PostgreSQL data source implementation."""

import datetime
from contextlib import contextmanager
from typing import List, Dict, Any, Iterator, Optional
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import warnings
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import logging

from .base import (
    DataSource,
    DataSourceCapability,
    TableMetadata,
    TableStatistics,
    ColumnStatistics,
)
from ..plan.expressions import DataType

logger = logging.getLogger(__name__)


def _hex_byte_table() -> np.ndarray:
    """Build the 256-entry table of two-hex-char ascii bytes, one per byte value."""
    table = []
    for value in range(256):
        table.append(b"%02x" % value)
    return np.array(table, dtype="S2")


# Used to format uuid bytes vectorized (no per-row Python uuid/hex call).
_HEX256 = _hex_byte_table()
# Canonical uuid text is 32 hex chars plus 4 dashes (8-4-4-4-12).
_UUID_TEXT_WIDTH = 36
_DASH = ord("-")

# Postgres OID for NUMERIC/DECIMAL. A declared numeric(p,s) is surfaced as an
# exact Arrow decimal128 (see _numeric_arrow_type); the largest exact decimal128
# Arrow supports is 38 digits.
_NUMERIC_OID = 1700
_MAX_DECIMAL128_PRECISION = 38

# Cap each streamed ADBC batch so a huge scan yields many bounded batches rather
_ADBC_BATCH_BYTES = 4 * 1024 * 1024


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
        # Pool of idle ADBC connections. A streamed COPY keeps its connection
        # busy for the life of the reader, and libpq allows only one COPY per
        # connection, so concurrent scans (e.g. both sides of a join) each need
        self._adbc_idle: List[Any] = []

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
        for connection in self._adbc_idle:
            connection.close()
        self._adbc_idle = []
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
            DataSourceCapability.SHIP_TARGET,
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

                rows = []
                for row in cursor.fetchall():
                    rows.append(
                        (row["column_name"], row["data_type"], row["is_nullable"])
                    )
                return self._metadata_from_information_schema(schema, table, rows)
        except psycopg2.Error as e:
            logger.error(f"Error getting metadata for {schema}.{table}: {e}")
            raise
        finally:
            self._return_connection(conn)

    def get_table_statistics(
        self, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """Catalog statistics: reltuples for the row count, pg_stats for the
        requested columns. Pure catalog reads - never a scan of the table."""
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                row_count = self._catalog_row_count(cursor, schema, table)
                column_stats = self._pg_stats_columns(
                    cursor, schema, table, columns, row_count
                )
                return self._build_table_statistics(row_count, column_stats)
        finally:
            self._return_connection(conn)

    def _catalog_row_count(self, cursor, schema: str, table: str) -> Optional[int]:
        """reltuples of the schema-qualified table, or None when unknown.

        The pg_namespace join is required: reltuples by relname alone would
        return an arbitrary same-named table from another schema. reltuples is
        -1 (never vacuumed/analyzed, PG14+) or a missing relation is None -
        an unknown count is reported honestly, never coerced to 0.
        """
        cursor.execute(
            "SELECT c.reltuples::bigint AS row_count"
            " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = %s AND c.relname = %s",
            (schema, table),
        )
        row = cursor.fetchone()
        if row is None or row["row_count"] < 0:
            return None
        return row["row_count"]

    def _pg_stats_columns(
        self, cursor, schema, table, columns: List[str], row_count: Optional[int]
    ) -> Dict[str, ColumnStatistics]:
        """pg_stats rows for exactly the requested columns.

        A column ANALYZE has never seen is simply absent from pg_stats and
        therefore absent from the result - the estimator substitutes a named
        default with provenance; the source never fabricates.
        """
        if not columns:
            return {}
        cursor.execute(
            "SELECT attname, n_distinct, null_frac, avg_width,"
            " histogram_bounds::text::text[] AS bounds"
            " FROM pg_stats"
            " WHERE schemaname = %s AND tablename = %s AND attname = ANY(%s)",
            (schema, table, list(columns)),
        )
        column_stats: Dict[str, ColumnStatistics] = {}
        for row in cursor.fetchall():
            column_stats[row["attname"]] = self._decode_pg_stats_row(row, row_count)
        return column_stats

    def _decode_pg_stats_row(self, row, row_count: Optional[int]) -> ColumnStatistics:
        """One pg_stats row decoded into engine column statistics."""
        min_value, max_value = self._histogram_ends(row["bounds"])
        # n_distinct decoded from Postgres' signed encoding; min/max are the
        # histogram's real end points when the column carries a histogram.
        return ColumnStatistics.create(
            num_distinct=self._decode_n_distinct(row["n_distinct"], row_count),
            null_fraction=row["null_frac"] or 0.0,
            avg_width=row["avg_width"],
            min_value=min_value,
            max_value=max_value,
        )

    def _decode_n_distinct(self, n_distinct, row_count: Optional[int]) -> Optional[int]:
        """Decode pg_stats.n_distinct: negative = fraction of rows, positive =
        absolute count. A fraction without a known row count is unknown (None) -
        multiplying a fraction by a guessed count would fabricate a statistic."""
        if n_distinct >= 0:
            return int(n_distinct)
        if row_count is None:
            return None
        return int(abs(n_distinct) * row_count)

    def _histogram_ends(self, bounds):
        """The first and last histogram bound parsed to a comparable Python
        value (number, date, or timestamp); a bound of any other shape is
        reported as None (unusable for range-selectivity interpolation)."""
        if not bounds:
            return None, None
        return self._parse_bound(bounds[0]), self._parse_bound(bounds[-1])

    def _parse_bound(self, text: str):
        """One histogram bound parsed as int, float, then ISO date/timestamp
        (pg_stats renders every bound as text); None when none of them fit."""
        try:
            return int(text)
        except ValueError:
            pass
        try:
            return float(text)
        except ValueError:
            pass
        try:
            return datetime.datetime.fromisoformat(text)
        except ValueError:
            return None

    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield Arrow record batches."""
        if self._use_adbc:
            return self._execute_query_adbc(query)
        return self._execute_query_psycopg2(query)

    def _execute_query_psycopg2(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute via psycopg2, building Arrow batches per fetched chunk.

        The read runs in autocommit mode so the SELECT does not leave the
        connection idle-in-transaction holding an AccessShareLock. The result is
        streamed (handed to the merge engine as a lazy reader), so it may be
        abandoned on an upstream early stop; without autocommit that lingering
        lock would block later DDL such as DROP. The ADBC path does the same.
        """
        conn = self._get_connection()
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                logger.debug(f"Executing query on {self.name}: {query[:100]}...")
                cursor.execute(query)

                schema = pa.schema(self._build_arrow_fields(cursor.description))

                while True:
                    rows = cursor.fetchmany(self._fetch_batch_size)
                    if not rows:
                        break

                    arrays = self._build_column_arrays(rows, schema)
                    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
                    yield batch
        except psycopg2.Error as e:
            logger.error(f"Query execution failed on {self.name}: {e}")
            raise
        finally:
            self._release_read_connection(conn)

    def _release_read_connection(self, conn) -> None:
        """Restore transactional mode and return a read connection to the pool."""
        conn.autocommit = False
        self._return_connection(conn)

    def _execute_query_adbc(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute via ADBC and STREAM the result, never draining it ourselves.

        The result is yielded from a lazy ``RecordBatchReader`` so the consumer
        and can spill or stop early, instead of us buffering the whole scan into
        an Arrow table first. The cursor stays open for the life of the stream;
        ``finally`` closes it when the stream is exhausted or the consumer stops.
        """
        logger.debug(f"Executing query (adbc) on {self.name}: {query[:100]}...")
        connection = self._acquire_adbc()
        cursor = connection.cursor()
        try:
            self._bound_adbc_batch_size(cursor)
            cursor.execute(query)
            for batch in self._adbc_record_batches(cursor):
                yield from self._normalize_batch(batch)
        finally:
            cursor.close()
            self._release_adbc(connection)

    def _bound_adbc_batch_size(self, cursor) -> None:
        """Cap the streamed batch size via the ADBC statement option.

        Not wrapped in a best-effort catch: if the driver rejects this known
        PostgreSQL option the engine must surface it, not silently run unhinted.
        """
        cursor.adbc_statement.set_options(
            **{"adbc.postgresql.batch_size_hint_bytes": str(_ADBC_BATCH_BYTES)}
        )

    def _adbc_record_batches(self, cursor) -> Iterator[pa.RecordBatch]:
        """Lazy ``RecordBatchReader`` for an executed cursor (no full drain)."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return cursor.fetch_record_batch()

    def _normalize_batch(self, batch: pa.RecordBatch) -> Iterator[pa.RecordBatch]:
        """Apply the ADBC->engine type normalization to a single streamed batch."""
        normalized = self._normalize_table(pa.Table.from_batches([batch]))
        yield from normalized.to_batches()

    def get_query_schema(self, query: str) -> pa.Schema:
        """Get a query's Arrow schema without materializing rows."""
        wrapped = self._schema_probe_sql(query)
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

    def _acquire_adbc(self):
        """Take an idle ADBC connection from the pool, or open a new one.

        ``autocommit=True`` keeps read-only fetches from leaving the connection
        idle in a transaction holding locks on the scanned tables.
        """
        if self._adbc_idle:
            return self._adbc_idle.pop()
        from adbc_driver_postgresql import dbapi

        return dbapi.connect(self._adbc_uri(), autocommit=True)

    def _release_adbc(self, connection) -> None:
        """Return a connection to the idle pool for reuse by the next scan."""
        self._adbc_idle.append(connection)

    def _adbc_fetch(self, query: str) -> pa.Table:
        """Run a query through ADBC and normalize its Arrow types (drains fully).

        Used for the small schema-probe (``LIMIT 0``); the streamed data path is
        ``_execute_query_adbc``.
        """
        connection = self._acquire_adbc()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            table = cursor.fetch_arrow_table()
        finally:
            cursor.close()
            self._release_adbc(connection)
        return self._normalize_table(table)

    def _normalize_table(self, table: pa.Table) -> pa.Table:
        """Align ADBC's Arrow types with the psycopg2 path's, so the driver is
        a drop-in: uuid->string, numeric decimal preserved as decimal128, every
        integer width->int64, and real(float32)->float64. Anything already
        matching is left untouched.
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
            return column
        if pa.types.is_integer(column_type) and not pa.types.is_int64(column_type):
            return pc.cast(column, pa.int64())
        if pa.types.is_floating(column_type) and not pa.types.is_float64(column_type):
            return pc.cast(column, pa.float64())
        return column

    def _normalize_opaque(self, column, column_type):
        """Convert an ADBC opaque extension column to the engine's type.

        Postgres ``uuid`` arrives as opaque binary and becomes a canonical uuid
        string. A ``numeric`` here is an opaque string with no recoverable
        precision/scale, so it falls back to float64 (the same fallback the
        psycopg2 path uses for an unconstrained numeric); a declared numeric
        instead arrives as a native decimal and is preserved as decimal128 by
        _normalize_column. Any other opaque type raises rather than passing its
        raw storage through, which would mistype the column silently.
        """
        type_name = getattr(column_type, "type_name", "")
        storage = column.combine_chunks().storage
        if type_name == "uuid":
            return self._uuid_bytes_to_string(storage)
        if type_name == "numeric":
            return pc.cast(storage, pa.float64())
        raise ValueError(f"Unsupported ADBC opaque type: {type_name!r}")

    def _uuid_bytes_to_string(self, storage) -> pa.Array:
        """Format a binary(16) uuid column to canonical strings, vectorized.

        Renders every row's 36-char text into one contiguous byte buffer (hex
        bytes via a lookup table, dashes at the fixed positions) and wraps it as
        slow numpy string concatenation. NULLs are restored at the end.
        """
        fixed = pc.cast(storage, pa.binary(16))
        count = len(fixed)
        text = self._render_uuid_text(fixed, count)
        array = self._string_array_from_fixed_width(text, count)
        return self._restore_nulls(array, fixed)

    def _render_uuid_text(self, fixed, count: int) -> np.ndarray:
        """Render ``count`` uuids into a contiguous (count, 36) byte matrix."""
        raw = np.frombuffer(fixed.buffers()[1], dtype=np.uint8)
        start = fixed.offset * 16
        matrix = raw[start : start + count * 16].reshape(count, 16)
        hex_bytes = np.frombuffer(_HEX256[matrix].tobytes(), dtype=np.uint8)
        return self._lay_out_uuid(hex_bytes.reshape(count, 32), count)

    def _lay_out_uuid(self, hex_bytes: np.ndarray, count: int) -> np.ndarray:
        """Place 32 hex bytes and 4 dashes into the 8-4-4-4-12 canonical layout."""
        out = np.empty((count, _UUID_TEXT_WIDTH), dtype=np.uint8)
        out[:, 8] = out[:, 13] = out[:, 18] = out[:, 23] = _DASH
        out[:, 0:8] = hex_bytes[:, 0:8]
        out[:, 9:13] = hex_bytes[:, 8:12]
        out[:, 14:18] = hex_bytes[:, 12:16]
        out[:, 19:23] = hex_bytes[:, 16:20]
        out[:, 24:36] = hex_bytes[:, 20:32]
        return out

    def _string_array_from_fixed_width(self, text: np.ndarray, count: int) -> pa.Array:
        """Wrap a (count, width) byte matrix as a UTF-8 string array."""
        width = _UUID_TEXT_WIDTH
        offsets = np.arange(0, width * (count + 1), width, dtype=np.int32)
        return pa.StringArray.from_buffers(
            count, pa.py_buffer(offsets.tobytes()), pa.py_buffer(text.tobytes())
        )

    def _restore_nulls(self, array: pa.Array, fixed) -> pa.Array:
        """Mark rows null wherever the source uuid column was null."""
        mask = pc.is_null(fixed).to_numpy(zero_copy_only=False)
        if not mask.any():
            return array
        return pc.if_else(pa.array(~mask), array, pa.scalar(None, type=pa.string()))

    def _extract_column_names(self, description) -> List[str]:
        """Extract column names from cursor description."""
        columns = []
        for desc in description:
            columns.append(desc[0])
        return columns

    # PostgreSQL type OIDs -> Arrow types. NUMERIC (1700) is handled per-column
    # by _numeric_arrow_type (declared precision/scale -> exact decimal128); the
    # entry here is only the fallback for an unconstrained/oversized numeric.
    _OID_TO_ARROW = {
        16: pa.bool_(),  # bool
        20: pa.int64(),  # int8
        21: pa.int64(),  # int2
        23: pa.int64(),  # int4
        700: pa.float32(),  # float4 (32-bit; matches DataType.FLOAT and ADBC)
        701: pa.float64(),  # float8
        1700: pa.float64(),  # numeric
        25: pa.string(),  # text
        1042: pa.string(),  # bpchar
        1043: pa.string(),  # varchar
        1082: pa.date32(),  # date
        1114: pa.timestamp("us"),  # timestamp
        1184: pa.timestamp("us", tz="UTC"),  # timestamptz
        2950: pa.string(),  # uuid (rendered as text, matching the ADBC path)
    }

    # Native Postgres type names the generic SQL mapper does not cover, mapped
    # to the engine DataType the fetch path also yields (uuid OID 2950 above is
    # fetched as text). Keeps catalog metadata and execution in agreement.
    _NATIVE_TYPE_OVERRIDES = {
        "UUID": DataType.VARCHAR,
    }

    def map_native_type(self, type_str: str) -> DataType:
        """Map a Postgres type name, honoring types only Postgres exposes.

        Without the uuid override the catalog would raise on a uuid column the
        engine can actually query (the fetch path renders uuid as text), so the
        two paths would disagree on the same column.
        """
        normalized = type_str.upper().split("(")[0].strip()
        if normalized in self._NATIVE_TYPE_OVERRIDES:
            return self._NATIVE_TYPE_OVERRIDES[normalized]
        return super().map_native_type(type_str)

    def _build_arrow_fields(self, description) -> List[pa.Field]:
        """Build typed Arrow fields from a cursor description.

        An unmapped type OID raises instead of defaulting to string: a silent
        string fallback would mistype the column (e.g. a uuid/json/time value)
        and could diverge from the same column fetched via the ADBC path.
        """
        fields = []
        for column in description:
            fields.append(self._arrow_field_for(column))
        return fields

    def _arrow_field_for(self, column) -> pa.Field:
        """Build one Arrow field, surfacing a declared NUMERIC as decimal128.

        A NUMERIC column with a declared precision and scale becomes an exact
        Arrow decimal128 so cross-source arithmetic and aggregation over
        Postgres decimals stay exact (matching how DuckDB surfaces DECIMAL),
        instead of drifting through float64. Every other type uses the static
        OID map; an unmapped OID raises rather than silently defaulting.
        """
        name, oid = column[0], column[1]
        if oid == _NUMERIC_OID:
            return pa.field(name, self._numeric_arrow_type(column[4], column[5]))
        if oid not in self._OID_TO_ARROW:
            raise ValueError(
                f"Unsupported PostgreSQL type OID {oid} for column {name!r}"
            )
        return pa.field(name, self._OID_TO_ARROW[oid])

    def _numeric_arrow_type(self, precision, scale) -> pa.DataType:
        """Arrow type for a NUMERIC column: exact decimal128, else float64.

        An unconstrained NUMERIC (no declared precision, which Postgres reports
        as a 65535 sentinel) or one beyond Arrow's 38-digit decimal128 limit
        cannot be represented exactly, so it falls back to float64 (the engine's
        prior behavior for every numeric).
        """
        if self._is_exact_decimal(precision, scale):
            return pa.decimal128(precision, scale)
        return pa.float64()

    def _is_exact_decimal(self, precision, scale) -> bool:
        """Whether a declared (precision, scale) fits an exact Arrow decimal128."""
        if precision is None or scale is None:
            return False
        if not 1 <= precision <= _MAX_DECIMAL128_PRECISION:
            return False
        return 0 <= scale <= precision

    def _build_column_arrays(self, rows: List, schema: pa.Schema) -> List[pa.Array]:
        """Build one Arrow array per column position.

        Indexing by position (not by name) is what lets a result with duplicate
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
        so an unconstrained NUMERIC (the float64 fallback) is coerced. A
        declared NUMERIC now surfaces as decimal128, so its Decimal values pass
        straight through into the decimal128 array unchanged.
        """
        coerce = pa.types.is_float64(field.type)
        values = []
        for row in rows:
            value = row[column_index]
            if coerce and value is not None:
                value = float(value)
            values.append(value)
        return values
