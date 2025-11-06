"""Statistics collection and management."""

from typing import Dict, Optional
from ..catalog.catalog import Catalog
from ..datasources.base import TableStatistics


class StatisticsCollector:
    """Collects and caches statistics from data sources."""

    def __init__(self, catalog: Catalog):
        """Initialize statistics collector.

        Args:
            catalog: Catalog for accessing data sources
        """
        self.catalog = catalog
        self.cache: Dict[tuple, TableStatistics] = {}

    def get_table_statistics(
        self, datasource: str, schema: str, table: str, refresh: bool = False
    ) -> Optional[TableStatistics]:
        """Get statistics for a table.

        Args:
            datasource: Data source name
            schema: Schema name
            table: Table name
            refresh: Whether to refresh cached statistics

        Returns:
            Table statistics if available, None otherwise
        """
        key = (datasource, schema, table)

        # Check cache
        if not refresh and key in self.cache:
            return self.cache[key]

        # Fetch from data source
        ds = self.catalog.get_datasource(datasource)
        if ds:
            stats = ds.get_table_statistics(schema, table)
            if stats:
                self.cache[key] = stats
            return stats

        return None

    def clear_cache(self) -> None:
        """Clear statistics cache."""
        self.cache.clear()

    def __repr__(self) -> str:
        return f"StatisticsCollector(cached={len(self.cache)})"
