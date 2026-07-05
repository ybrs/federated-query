"""Session-cached, per-column statistics collection from source catalogs.

The cost-based optimizer asks for statistics at optimization time; this
collector fetches them from the owning datasource's CATALOG and accumulates
them per (datasource, schema, table) for the life of the session. Fetching is
lazy per column: a query needing stats for two join keys fetches exactly those
two columns, and a later query needing one more fetches only the missing one.
A column the source has no statistics for is attempted once and never
re-fetched - its absence is itself cached.
"""

from typing import Dict, List, Optional, Set, Tuple

from ..catalog.catalog import Catalog
from ..datasources.base import ColumnStatistics, TableStatistics


class _TableStatsEntry:
    """The accumulated statistics state for one (datasource, schema, table)."""

    def __init__(self, row_count: Optional[int], total_size_bytes: int):
        """Start an entry from the first fetch's table-level values."""
        self.row_count = row_count
        self.total_size_bytes = total_size_bytes
        self.column_stats: Dict[str, ColumnStatistics] = {}
        # Every column ever requested, whether or not the source had stats
        # for it - so an absent column is fetched exactly once per session.
        self.attempted_columns: Set[str] = set()

    def merged_view(self) -> TableStatistics:
        """The accumulated state as a TableStatistics snapshot."""
        # Snapshot of everything accumulated so far for this table; callers
        # get a plain TableStatistics and never see the cache bookkeeping.
        return TableStatistics.create(
            row_count=self.row_count,
            total_size_bytes=self.total_size_bytes,
            column_stats=dict(self.column_stats),
        )


class StatisticsCollector:
    """Collects and caches per-column statistics from data source catalogs."""

    def __init__(self, catalog: Catalog):
        """Wire the collector to the catalog it resolves datasources through."""
        self.catalog = catalog
        self.cache: Dict[Tuple[str, str, str], _TableStatsEntry] = {}

    def get_table_statistics(
        self, datasource: str, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """Statistics for a table covering at least the requested columns.

        Fetches only what the cache is missing. Returns None only when the
        SOURCE does not support statistics; an unknown datasource name raises -
        a typo here would silently disable costing for every query.
        """
        source = self.catalog.get_datasource(datasource)
        if source is None:
            raise ValueError(
                f"StatisticsCollector: catalog knows no datasource {datasource!r}"
            )
        entry = self._entry_covering(source, datasource, schema, table, columns)
        if entry is None:
            return None
        return entry.merged_view()

    def _entry_covering(self, source, datasource, schema, table, columns):
        """The cache entry after fetching any columns it does not cover yet."""
        key = (datasource, schema, table)
        entry = self.cache.get(key)
        missing = self._missing_columns(entry, columns)
        if entry is not None and not missing:
            return entry
        fetched = source.get_table_statistics(schema, table, missing)
        if fetched is None:
            return entry
        return self._absorb(key, entry, fetched, missing)

    def _missing_columns(self, entry, columns: List[str]) -> List[str]:
        """The requested columns this entry has not attempted yet (all of them
        when there is no entry), preserving the caller's order."""
        missing = []
        for column in columns:
            if entry is None or column not in entry.attempted_columns:
                missing.append(column)
        return missing

    def _absorb(self, key, entry, fetched: TableStatistics, missing: List[str]):
        """Merge one fetch into the cache entry (created on first fetch)."""
        if entry is None:
            entry = _TableStatsEntry(fetched.row_count, fetched.total_size_bytes)
            self.cache[key] = entry
        for column in missing:
            entry.attempted_columns.add(column)
        entry.column_stats.update(fetched.column_stats)
        return entry

    def clear_cache(self) -> None:
        """Drop every cached statistic (a fresh session's view)."""
        self.cache.clear()

    def __repr__(self) -> str:
        return f"StatisticsCollector(cached={len(self.cache)})"
