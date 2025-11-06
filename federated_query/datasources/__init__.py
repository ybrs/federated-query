"""Data source connectors."""

from .base import DataSource, DataSourceCapability
from .postgresql import PostgreSQLDataSource
from .duckdb import DuckDBDataSource

__all__ = [
    "DataSource",
    "DataSourceCapability",
    "PostgreSQLDataSource",
    "DuckDBDataSource",
]
