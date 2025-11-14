"""Base data source interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional
from enum import Enum
import pyarrow as pa
import sqlglot


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


@dataclass
class ColumnMetadata:
    """Metadata about a column."""

    name: str
    data_type: str
    nullable: bool
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Referenced table.column


@dataclass
class TableMetadata:
    """Metadata about a table."""

    schema_name: str
    table_name: str
    columns: List[ColumnMetadata]
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


@dataclass
class TableStatistics:
    """Statistics about a table."""

    row_count: int
    total_size_bytes: int
    column_stats: Dict[str, "ColumnStatistics"]


@dataclass
class ColumnStatistics:
    """Statistics about a column."""

    num_distinct: int
    null_fraction: float
    avg_width: int  # Average size in bytes
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


class DataSource(ABC):
    """Abstract base class for data sources."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize data source.

        Args:
            name: Unique name for this data source
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
        self.connection = None
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
    def get_table_statistics(self, schema: str, table: str) -> Optional[TableStatistics]:
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

    def parse_query(self, query: str):
        """Parse query text into a sqlglot AST."""
        return sqlglot.parse_one(query, dialect="postgres")

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
