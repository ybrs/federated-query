"""Session-cached, per-column statistics collection from source catalogs.

The cost-based optimizer asks for statistics at optimization time; this
collector fetches them from the owning datasource's CATALOG and accumulates
them per (datasource, schema, table) for the life of the session. Fetching is
lazy per column: a query needing stats for two join keys fetches exactly those
two columns, and a later query needing one more fetches only the missing one.
A column the source has no statistics for is attempted once and never
re-fetched - its absence is itself cached.
"""

import os
from typing import Dict, List, Optional, Set, Tuple

from ..catalog.catalog import Catalog
from ..datasources.base import ColumnStatistics, DataSource, TableStatistics
from ..plan.physical import PhysicalScan


def _ask_source_enabled() -> bool:
    """Whether source-planner scan estimates are on. Shares the probe family's
    kill switch (FEDQ_STATS_PROBE=0): both are 'measure at the source what the
    engine cannot know'; anything else, including unset, is on."""
    return os.environ.get("FEDQ_STATS_PROBE", "1") != "0"


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

    def __init__(self, catalog: Catalog, stats_catalog=None, learned_ttl_seconds=None):
        """Wire the collector to the metadata catalog it resolves datasources
        through, and optionally a learned-stats catalog whose measured row
        counts / NDVs overlay the source's (None disables the read path)."""
        self.catalog = catalog
        self.cache: Dict[Tuple[str, str, str], _TableStatsEntry] = {}
        self.stats_catalog = stats_catalog
        self.learned_ttl_seconds = learned_ttl_seconds
        # Source-planner (EXPLAIN) row estimates for rendered filtered scans,
        # keyed by (datasource, rendered SQL) - one round trip per distinct
        # scan per session, however often the join enumerator re-estimates.
        self.planner_estimates: Dict[Tuple[str, str], Optional[int]] = {}

    def get_table_statistics(
        self, datasource: str, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """Statistics for a table covering at least the requested columns, with
        learned (measured) values overlaid over the source's when available.

        Returns None only when neither the source nor the catalog knows anything;
        an unknown datasource name raises - a typo here would silently disable
        costing for every query.
        """
        source_stats = self._source_statistics(datasource, schema, table, columns)
        if self.stats_catalog is None:
            return source_stats
        return self._overlay_learned(datasource, schema, table, columns, source_stats)

    def _source_statistics(
        self, datasource: str, schema: str, table: str, columns: List[str]
    ) -> Optional[TableStatistics]:
        """The source's own statistics (cached), or None when it has none."""
        source = self.catalog.get_datasource(datasource)
        if source is None:
            raise ValueError(
                f"StatisticsCollector: catalog knows no datasource {datasource!r}"
            )
        entry = self._entry_covering(source, datasource, schema, table, columns)
        if entry is None:
            return None
        return entry.merged_view()

    def _overlay_learned(self, datasource, schema, table, columns, source_stats):
        """FILL a table's missing statistics from the catalog - the row count or
        a column NDV the source does not provide (the warehouse case: pg has no
        stats, the catalog measured 10). This does NOT override values the source
        already has: replacing source estimates wholesale destabilizes the
        reduction/orientation decisions tuned against them (measured +5s at SF10),
        while filling only the gaps keeps the win without the churn. Safe either
        way - these values only steer plan choice; the TTL bounds staleness.
        Returns the source stats unchanged when nothing was learned."""
        rows = self.stats_catalog.table_rows(
            datasource, schema, table, self.learned_ttl_seconds
        )
        ndvs = self._learned_ndvs(datasource, schema, table, columns)
        if rows is None and not ndvs:
            return source_stats
        return self._merge_learned(source_stats, rows, ndvs)

    def _learned_ndvs(self, datasource, schema, table, columns):
        """The catalog's learned NDV for each requested column that has one."""
        ndvs = {}
        for column in columns:
            ndv = self.stats_catalog.column_ndv(
                datasource, schema, table, column, self.learned_ttl_seconds
            )
            if ndv is not None:
                ndvs[column] = ndv
        return ndvs

    def _merge_learned(self, source_stats, rows, ndvs):
        """A TableStatistics with learned values FILLING the gaps the source left
        (a None row count, a column with no distinct count); present source
        values are kept."""
        row_count = self._source_rows(source_stats)
        if row_count is None:
            row_count = rows
        column_stats = dict(source_stats.column_stats) if source_stats else {}
        for column, ndv in ndvs.items():
            column_stats[column] = self._filled_column(column_stats.get(column), ndv)
        total = source_stats.total_size_bytes if source_stats else 0
        # create, not model_copy: this MERGES two origins (learned row count /
        # NDVs over the source's), so there is no single node to copy from -
        # source_stats may even be None. All three fields are named here.
        return TableStatistics.create(
            row_count=row_count, total_size_bytes=total, column_stats=column_stats
        )

    def _source_rows(self, source_stats):
        """The source's row count, or None when it provided no statistics."""
        return source_stats.row_count if source_stats else None

    def _filled_column(self, existing, ndv):
        """The column's stats with the learned NDV FILLED IN only where the
        source lacks one: a present source distinct count is kept (fill, not
        override)."""
        if existing is not None and existing.num_distinct is not None:
            return existing
        if existing is not None:
            return existing.model_copy(update={"num_distinct": ndv})
        # create, not model_copy: the source had NO stats for this column, so
        # there is nothing to copy - a fresh node with the learned NDV and
        # neutral defaults for the fields we did not measure. All fields named.
        return ColumnStatistics.create(num_distinct=ndv, null_fraction=0.0, avg_width=8)

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

    def scan_planner_estimate(self, scan) -> Optional[int]:
        """The SOURCE PLANNER's row estimate for a filtered logical scan, or
        None (source offers none / kill switch off). Consulted by the cost
        model when its own predicate pricing left gaps: the source prices the
        predicate from ITS statistics - an informed estimate with provenance,
        where the engine would otherwise only hold the no-reduction bound."""
        if not _ask_source_enabled():
            return None
        source = self.catalog.get_datasource(scan.datasource)
        if source is None:
            raise ValueError(
                f"StatisticsCollector: catalog knows no datasource {scan.datasource!r}"
            )
        if not isinstance(source, DataSource):
            # Only a real connector carries a source planner to ask; a bare
            # stats-serving stand-in (tests) honestly has no estimate.
            return None
        sql = self._render_probe_scan(scan, source)
        key = (scan.datasource, sql)
        if key not in self.planner_estimates:
            self.planner_estimates[key] = source.estimate_scan_rows(
                scan.schema_name, scan.table_name, sql
            )
        return self.planner_estimates[key]

    def _render_probe_scan(self, scan, source) -> str:
        """The scan rendered in the source dialect exactly as execution would
        render it - same emitter, same transpile boundary - minus any grouping:
        the estimate prices the FILTERED INPUT rows, which is what the group
        clamp consumes."""
        # create, not model_copy: a throwaway PHYSICAL scan built from a
        # LOGICAL one purely to reuse the canonical SQL renderer; only the
        # rendering-relevant fields exist on both, the rest keep defaults.
        probe = PhysicalScan.create(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=list(scan.columns),
            filters=scan.filters,
            alias=scan.alias,
            datasource_connection=source,
        )
        return probe._render_source_sql()

    def clear_cache(self) -> None:
        """Drop every cached statistic (a fresh session's view)."""
        self.cache.clear()

    def __repr__(self) -> str:
        return f"StatisticsCollector(cached={len(self.cache)})"
