"""Base data source interface."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterator, Optional
from enum import Enum
import pyarrow as pa
import sqlglot

from ..model import StateModel
from ..plan.expressions import DataType


class DataSourceCapability(Enum):
    """Capabilities that a data source may support."""

    AGGREGATIONS = "aggregations"
    JOINS = "joins"
    WINDOW_FUNCTIONS = "window_functions"
    SUBQUERIES = "subqueries"
    CTE = "cte"
    DISTINCT = "distinct"
    LIMIT = "limit"
    ORDER_BY = "order_by"
    # The source can receive a shipped relation as a session TEMP TABLE (dim
    # shipping materializes a small foreign dimension into the fact's source).
    # Writable sources (DuckDB, PostgreSQL) support it; read-only ones do not.
    SHIP_TARGET = "ship_target"


class ColumnMetadata(StateModel):
    """Metadata about a column."""

    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Referenced table.column

    @classmethod
    def create(
        cls,
        *,
        name: str,
        data_type: str,
        nullable: bool,
        primary_key: bool = False,
        foreign_key: Optional[str] = None,
    ) -> "ColumnMetadata":
        """Sanctioned fresh-construction path for ColumnMetadata.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            name=name,
            data_type=data_type,
            nullable=nullable,
            primary_key=primary_key,
            foreign_key=foreign_key,
        )


class TableMetadata(StateModel):
    """Metadata about a table."""

    schema_name: str
    table_name: str
    columns: List[ColumnMetadata]
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None

    @classmethod
    def create(
        cls,
        *,
        schema_name: str,
        table_name: str,
        columns: List[ColumnMetadata],
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
    ) -> "TableMetadata":
        """Sanctioned fresh-construction path for TableMetadata.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            row_count=row_count,
            size_bytes=size_bytes,
        )


class ColumnStatistics(StateModel):
    """Statistics about a column.

    ``num_distinct`` is None when the source cannot provide a distinct count;
    a source never fabricates a statistic it does not know - the estimator
    substitutes a NAMED default and records the substitution as provenance.
    """

    num_distinct: Optional[int]
    null_fraction: float
    avg_width: int  # Average size in bytes
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None

    @classmethod
    def create(
        cls,
        *,
        num_distinct: Optional[int],
        null_fraction: float,
        avg_width: int,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
    ) -> "ColumnStatistics":
        """Sanctioned fresh-construction path for ColumnStatistics.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            num_distinct=num_distinct,
            null_fraction=null_fraction,
            avg_width=avg_width,
            min_value=min_value,
            max_value=max_value,
        )


class TableStatistics(StateModel):
    """Statistics about a table.

    ``row_count`` is None when the source honestly does not know its row count
    (e.g. PostgreSQL reltuples = -1 before the first ANALYZE); a source never
    fabricates a count - the estimator substitutes a NAMED default and records
    the substitution as provenance.
    """

    row_count: Optional[int]
    total_size_bytes: int
    column_stats: Dict[str, ColumnStatistics]

    @classmethod
    def create(
        cls,
        *,
        row_count: Optional[int],
        total_size_bytes: int,
        column_stats: Dict[str, ColumnStatistics],
    ) -> "TableStatistics":
        """Sanctioned fresh-construction path for TableStatistics.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            row_count=row_count,
            total_size_bytes=total_size_bytes,
            column_stats=column_stats,
        )


class DataSource(ABC):
    """Abstract base class for data sources."""

    # sqlglot dialect a pushed query is rendered in for this source. The engine
    # generates Postgres-flavored SQL internally; rendering it through the
    # source's own dialect transpiles dialect-divergent syntax (function names,
    # TABLESAMPLE, ordered-set aggregates) to what the source actually accepts.
    # Subclasses override this; the default suits Postgres-compatible sources.
    render_dialect = "postgres"

    # Rows per Arrow record batch when streaming a query result. One default for
    # every driver's fetch loop; a source with different memory characteristics
    # overrides it.
    _fetch_batch_size = 10000

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize data source.

        Args:
            name: Unique name for this data source
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
        # NOTE: the base intentionally has no `connection` attribute. A single
        # connection is an implementation detail of single-connection sources
        # (DuckDB declares its own); pooled sources (PostgreSQL) borrow per
        # operation. Connectedness is tracked by `_connected`/is_connected().
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass

    @abstractmethod
    def get_capabilities(self) -> List[DataSourceCapability]:
        """Return list of capabilities supported by this data source."""
        pass

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List all available schemas."""
        pass

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        """List all tables in a schema.

        Args:
            schema: Schema name

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def get_table_metadata(self, schema: str, table: str) -> TableMetadata:
        """Get metadata for a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Table metadata including columns and types
        """
        pass

    @abstractmethod
    def get_table_statistics(
        self, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """Get catalog statistics for a table, lazily per column.

        ``columns`` names exactly the columns the optimizer needs statistics
        for (join keys and filtered columns); an empty list requests the row
        count only. A source returns only what its catalog can provide
        cheaply and honestly: unknown values are None / omitted columns,
        never fabricated.

        Args:
            schema: Schema name
            table: Table name
            columns: Column names to fetch column-level statistics for

        Returns:
            Table statistics if the source supports them, None otherwise
        """
        pass

    def estimate_scan_rows(self, schema: str, table: str, sql: str):
        """The SOURCE PLANNER's row estimate for a rendered single-table scan
        (its EXPLAIN output), or None when this source offers none.

        Consulted when the engine's estimator cannot fully price a scan's
        predicate (LIKE patterns, column-vs-column ranges): the source's own
        planner prices it from ITS statistics - an informed estimate with real
        provenance, where the engine would otherwise only have the
        no-reduction bound. None is an honest abstention (the base class has
        no planner to ask; a connector overrides), never an error path.
        """
        return None

    @abstractmethod
    def execute_query(self, query: str) -> Iterator[pa.RecordBatch]:
        """Execute a SQL query and return results as Arrow record batches.

        Args:
            query: SQL query string

        Returns:
            Iterator of Arrow record batches
        """
        pass

    @abstractmethod
    def get_query_schema(self, query: str) -> pa.Schema:
        """Get the schema of a query without executing it.

        Args:
            query: SQL query string

        Returns:
            Arrow schema
        """
        pass

    def _schema_probe_sql(self, query: str) -> str:
        """Wrap a query so it returns its schema with no rows (a LIMIT 0 probe).

        Every source reports a query's exact column types by running it under
        ``LIMIT 0``; only how the empty result becomes an Arrow schema differs
        per driver, so the wrapping SQL lives here once.
        """
        return f"SELECT * FROM ({query}) AS q LIMIT 0"

    def _metadata_from_information_schema(self, schema, table, rows) -> TableMetadata:
        """Build TableMetadata from information_schema (name, type, is_nullable) rows.

        Shared by sources that read information_schema.columns verbatim; nullable
        follows the SQL ``is_nullable = 'YES'`` convention.
        """
        columns = []
        for name, data_type, is_nullable in rows:
            # One column descriptor decoded from an information_schema row, with
            # nullability read off the SQL 'YES'/'NO' is_nullable flag.
            columns.append(
                ColumnMetadata.create(
                    name=name, data_type=data_type, nullable=(is_nullable == "YES")
                )
            )
        # The table descriptor gathering the columns just decoded above under the
        # requested schema and table names.
        return TableMetadata.create(
            schema_name=schema, table_name=table, columns=columns
        )

    def _build_column_statistics(
        self, num_distinct, null_count, row_count
    ) -> ColumnStatistics:
        """Build ColumnStatistics, deriving the null fraction from a null count."""
        null_fraction = null_count / row_count if row_count > 0 else 0.0
        # Per-column stats for the optimizer, turning the raw null count into the
        # null fraction it expects; avg_width is a fixed placeholder estimate.
        return ColumnStatistics.create(
            num_distinct=num_distinct, null_fraction=null_fraction, avg_width=10
        )

    def _build_table_statistics(self, row_count, column_stats) -> TableStatistics:
        """Wrap a row count and per-column stats into TableStatistics."""
        size_bytes = row_count * 100 if row_count is not None else 0
        # Table-level stats for cost estimation; byte size is approximated from
        # the row count (0 when the row count itself is unknown) since sources
        # here do not report an exact size.
        return TableStatistics.create(
            row_count=row_count,
            total_size_bytes=size_bytes,
            column_stats=column_stats,
        )

    def map_native_type(self, type_str: str) -> DataType:
        """Map this source's native column-type name to an engine DataType.

        The connector is the authority on its own types, so the catalog and the
        execution path agree on what a column is. The default handles the common
        SQL type names; a source with extra native types (e.g. Postgres uuid)
        overrides this and falls back here. Most specific first: TIMESTAMP /
        DATETIME before DATE, and word-aware integer matching so POINT (which
        contains INT) is not read as an integer.
        """
        normalized = type_str.upper().split("(")[0].strip()
        temporal = self._map_temporal_type(normalized)
        if temporal is not None:
            return temporal
        numeric = self._map_numeric_type(normalized)
        if numeric is not None:
            return numeric
        return self._map_textual_type(normalized)

    def _map_temporal_type(self, type_str: str) -> Optional[DataType]:
        """Map date/time types, checking TIMESTAMP/DATETIME before DATE."""
        if "TIMESTAMP" in type_str or "DATETIME" in type_str:
            return DataType.TIMESTAMP
        if "DATE" in type_str:
            return DataType.DATE
        if "TIME" in type_str:
            return DataType.TIMESTAMP
        return None

    def _map_numeric_type(self, type_str: str) -> Optional[DataType]:
        """Map numeric types; integer match avoids the POINT/INT trap."""
        if "DOUBLE" in type_str or "NUMERIC" in type_str or "DECIMAL" in type_str:
            return DataType.DOUBLE
        if "FLOAT" in type_str or "REAL" in type_str:
            return DataType.FLOAT
        if "BIGINT" in type_str or "INT8" in type_str or "BIGSERIAL" in type_str:
            return DataType.BIGINT
        if self._is_integer_type(type_str):
            return DataType.INTEGER
        return None

    def _is_integer_type(self, type_str: str) -> bool:
        """Whether a type name denotes an integer (word-aware, not POINT)."""
        return type_str.startswith("INT") or type_str in (
            "SMALLINT",
            "SERIAL",
            "INTEGER",
        )

    def _map_textual_type(self, type_str: str) -> DataType:
        """Map boolean and string types; raise on a type the engine does not model.

        An unknown type is NOT silently coerced to VARCHAR: a mis-typed column is
        a wrong answer with no error. An unmodeled type must be added explicitly.
        """
        if "BOOL" in type_str:
            return DataType.BOOLEAN
        if "CHAR" in type_str or "TEXT" in type_str or "STRING" in type_str:
            return DataType.VARCHAR if "VAR" in type_str else DataType.TEXT
        raise ValueError(f"Unsupported column type for catalog mapping: {type_str}")

    def parse_query(self, query: str):
        """Parse query text into a sqlglot AST with NATURAL joins marked."""
        from ..parser.dialect import FedQPostgres

        ast = sqlglot.parse_one(query, dialect=FedQPostgres)
        self._mark_natural_joins(ast)
        return ast

    def _mark_natural_joins(self, ast) -> None:
        """Set an explicit ``natural`` flag on NATURAL joins in the AST.

        sqlglot records a NATURAL join only as ``method='NATURAL'``; exposing a
        boolean ``natural`` arg gives callers a uniform way to detect one.
        """
        for join in ast.find_all(sqlglot.exp.Join):
            method = join.args.get("method")
            if method and str(method).upper() == "NATURAL":
                join.set("natural", True)

    def supports_capability(self, capability: DataSourceCapability) -> bool:
        """Check if data source supports a capability.

        Args:
            capability: Capability to check

        Returns:
            True if supported, False otherwise
        """
        return capability in self.get_capabilities()

    def is_connected(self) -> bool:
        """Check if data source is connected.

        Returns:
            True if connected, False otherwise
        """
        return self._connected

    def ensure_connected(self) -> None:
        """Ensure data source is connected.

        Raises:
            ConnectionError: If connection cannot be established
        """
        if not self.is_connected():
            self.connect()
            self._connected = True

    def __enter__(self):
        """Context manager entry."""
        self.ensure_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        self._connected = False
        return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
