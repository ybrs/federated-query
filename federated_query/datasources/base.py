"""Base data source interface."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterator, Optional
from enum import Enum
import pyarrow as pa
import sqlglot

from ..model import StateModel


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


class ColumnMetadata(StateModel):
    """Metadata about a column."""

    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Referenced table.column


class TableMetadata(StateModel):
    """Metadata about a table."""

    schema_name: str
    table_name: str
    columns: List[ColumnMetadata]
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


class ColumnStatistics(StateModel):
    """Statistics about a column."""

    num_distinct: int
    null_fraction: float
    avg_width: int  # Average size in bytes
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


class TableStatistics(StateModel):
    """Statistics about a table."""

    row_count: int
    total_size_bytes: int
    column_stats: Dict[str, ColumnStatistics]


class DataSource(ABC):
    """Abstract base class for data sources."""

    # sqlglot dialect a pushed query is rendered in for this source. The engine
    # generates Postgres-flavored SQL internally; rendering it through the
    # source's own dialect transpiles dialect-divergent syntax (function names,
    # TABLESAMPLE, ordered-set aggregates) to what the source actually accepts.
    # Subclasses override this; the default suits Postgres-compatible sources.
    render_dialect = "postgres"

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
        self, schema: str, table: str
    ) -> Optional[TableStatistics]:
        """Get statistics for a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Table statistics if available, None otherwise
        """
        pass

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

    def _metadata_from_information_schema(self, schema, table, rows) -> TableMetadata:
        """Build TableMetadata from information_schema (name, type, is_nullable) rows.

        Shared by sources that read information_schema.columns verbatim; nullable
        follows the SQL ``is_nullable = 'YES'`` convention.
        """
        columns = []
        for name, data_type, is_nullable in rows:
            columns.append(
                ColumnMetadata(name=name, data_type=data_type, nullable=(is_nullable == "YES"))
            )
        return TableMetadata(schema_name=schema, table_name=table, columns=columns)

    def _build_column_statistics(self, num_distinct, null_count, row_count) -> ColumnStatistics:
        """Build ColumnStatistics, deriving the null fraction from a null count."""
        null_fraction = null_count / row_count if row_count > 0 else 0.0
        return ColumnStatistics(
            num_distinct=num_distinct, null_fraction=null_fraction, avg_width=10
        )

    def _build_table_statistics(self, row_count, column_stats) -> TableStatistics:
        """Wrap a row count and per-column stats into TableStatistics."""
        return TableStatistics(
            row_count=row_count, total_size_bytes=row_count * 100, column_stats=column_stats
        )

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
