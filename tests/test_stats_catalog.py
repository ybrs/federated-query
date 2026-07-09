"""Tests for the learned-stats SQLite catalog: upsert self-healing, TTL reads,
and the correctness-neutral freshness contract."""

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

from federated_query.catalog.stats_catalog import StatsCatalog


@pytest.fixture
def catalog_path():
    """A throwaway on-disk catalog file, removed after the test."""
    handle, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(handle)
    yield path
    os.remove(path)


class _Clock:
    """A controllable clock returning ISO stamps, so tests set freshness without
    sleeping."""

    def __init__(self, start):
        """Start the clock at the given datetime."""
        self.now = start

    def advance(self, seconds):
        """Move the clock forward by the given number of seconds."""
        self.now = self.now + timedelta(seconds=seconds)

    def __call__(self):
        """Return the current instant as an ISO-8601 string."""
        return self.now.isoformat()


def _catalog(path, clock=None):
    """Open a StatsCatalog, optionally with an injected clock."""
    if clock is None:
        return StatsCatalog(path)
    return StatsCatalog(path, clock=clock)


def test_table_rows_roundtrip(catalog_path):
    """A recorded base row count reads back."""
    catalog = _catalog(catalog_path)
    catalog.record_table_rows("pg", "public", "store_sales", 28800991)
    assert catalog.table_rows("pg", "public", "store_sales") == 28800991
    catalog.close()


def test_absent_reads_return_none(catalog_path):
    """An unrecorded table/column/predicate reads as None (planner falls back)."""
    catalog = _catalog(catalog_path)
    assert catalog.table_rows("pg", "public", "nope") is None
    assert catalog.column_ndv("pg", "public", "nope", "c") is None
    assert catalog.predicate_selectivity("pg", "public", "nope", "x = $1") is None
    catalog.close()


def test_column_ndv_roundtrip(catalog_path):
    """A recorded column NDV reads back, independent of the table-level row."""
    catalog = _catalog(catalog_path)
    catalog.record_table_rows("pg", "public", "warehouse", 10)
    catalog.record_column_ndv("pg", "public", "warehouse", "w_warehouse_sk", 10)
    assert catalog.table_rows("pg", "public", "warehouse") == 10
    assert catalog.column_ndv("pg", "public", "warehouse", "w_warehouse_sk") == 10
    catalog.close()


def test_upsert_self_heals(catalog_path):
    """Re-recording a table overwrites the stale value (self-healing on measure)
    and bumps observation_count rather than inserting a duplicate."""
    catalog = _catalog(catalog_path)
    catalog.record_table_rows("duck", "main", "catalog_sales", 100)
    catalog.record_table_rows("duck", "main", "catalog_sales", 14401261)
    assert catalog.table_rows("duck", "main", "catalog_sales") == 14401261
    row = catalog._conn.execute(
        "SELECT observation_count FROM table_stats WHERE table_name='catalog_sales'"
    ).fetchone()
    assert row[0] == 2
    catalog.close()


def test_predicate_selectivity_roundtrip(catalog_path):
    """A recorded predicate template reads back its measured selectivity."""
    catalog = _catalog(catalog_path)
    catalog.record_predicate(
        "pg", "public", "date_dim", "d_month_seq BETWEEN $1 AND $2",
        input_rows=73049, output_rows=366,
    )
    selectivity = catalog.predicate_selectivity(
        "pg", "public", "date_dim", "d_month_seq BETWEEN $1 AND $2"
    )
    assert selectivity == pytest.approx(366 / 73049)
    catalog.close()


def test_ttl_expires_stale_reads(catalog_path):
    """A value older than the TTL reads as None; within the TTL it reads back."""
    clock = _Clock(datetime(2026, 7, 9, tzinfo=timezone.utc))
    catalog = _catalog(catalog_path, clock=clock)
    catalog.record_table_rows("pg", "public", "store_sales", 28800991)
    clock.advance(30)
    assert catalog.table_rows("pg", "public", "store_sales", max_age_seconds=60) == 28800991
    clock.advance(120)
    assert catalog.table_rows("pg", "public", "store_sales", max_age_seconds=60) is None
    # No TTL: the value is always served, however old.
    assert catalog.table_rows("pg", "public", "store_sales") == 28800991
    catalog.close()


def test_persists_across_reopen(catalog_path):
    """The catalog is on disk: a value survives closing and reopening (learning
    carries across sessions, not just within one)."""
    catalog = _catalog(catalog_path)
    catalog.record_table_rows("pg", "public", "item", 102000)
    catalog.close()
    reopened = _catalog(catalog_path)
    assert reopened.table_rows("pg", "public", "item") == 102000
    reopened.close()


def test_persist_observations_writes_by_target(catalog_path):
    """The write path joins engine measurements with build_ir provenance and
    dispatches each to its catalog table; a measurement with no provenance (a
    merge fragment) is skipped."""
    from federated_query.executor.rust_ir import _persist_observations

    catalog = _catalog(catalog_path)
    provenance = {
        "b1": {"target": "table_rows", "datasource": "duck",
               "schema": "main", "table": "store_sales"},
        "b2": {"target": "column_ndv", "datasource": "pg", "schema": "public",
               "table": "warehouse", "column": "w_warehouse_sk"},
    }
    measurements = [("b1", 28800991), ("b2", 10), ("b3", 999)]
    _persist_observations(catalog, provenance, measurements)
    assert catalog.table_rows("duck", "main", "store_sales") == 28800991
    assert catalog.column_ndv("pg", "public", "warehouse", "w_warehouse_sk") == 10
    catalog.close()


def test_persist_unknown_target_raises(catalog_path):
    """An observation naming an unknown catalog target raises rather than being
    silently dropped (defensive: a new provenance kind must be handled)."""
    from federated_query.executor.rust_ir import _persist_observations

    catalog = _catalog(catalog_path)
    with pytest.raises(ValueError):
        _persist_observations(catalog, {"b1": {"target": "bogus"}}, [("b1", 5)])
    catalog.close()


class _FakeSource:
    """A datasource returning canned TableStatistics regardless of columns."""

    def __init__(self, stats):
        """Hold the canned statistics this fake source serves."""
        self.stats = stats

    def get_table_statistics(self, schema, table, columns):
        """Serve the canned statistics."""
        return self.stats


class _FakeCatalog:
    """A metadata catalog resolving every datasource name to one fake source."""

    def __init__(self, source):
        """Hold the single source every lookup resolves to."""
        self.source = source

    def get_datasource(self, name):
        """Resolve any datasource name to the fake source."""
        return self.source


def _collector(source_stats, stats_catalog):
    """A StatisticsCollector over a fake source, with the learned catalog wired."""
    from federated_query.optimizer.statistics import StatisticsCollector

    return StatisticsCollector(
        _FakeCatalog(_FakeSource(source_stats)), stats_catalog=stats_catalog
    )


def test_overlay_fills_missing_source_row_count(catalog_path):
    """A learned row count fills the source's None (the warehouse case: pg has
    no stats, the catalog measured 10)."""
    from federated_query.datasources.base import TableStatistics

    catalog = _catalog(catalog_path)
    catalog.record_table_rows("pg", "public", "warehouse", 10)
    source_stats = TableStatistics.create(
        row_count=None, total_size_bytes=0, column_stats={}
    )
    collector = _collector(source_stats, catalog)
    stats = collector.get_table_statistics("pg", "public", "warehouse", [])
    assert stats.row_count == 10
    catalog.close()


def test_overlay_fills_missing_column_ndv(catalog_path):
    """A learned column NDV fills a source that lacks that column's stats."""
    from federated_query.datasources.base import TableStatistics

    catalog = _catalog(catalog_path)
    catalog.record_column_ndv("pg", "public", "warehouse", "w_warehouse_sk", 10)
    source_stats = TableStatistics.create(
        row_count=10, total_size_bytes=0, column_stats={}
    )
    collector = _collector(source_stats, catalog)
    stats = collector.get_table_statistics(
        "pg", "public", "warehouse", ["w_warehouse_sk"]
    )
    assert stats.column_stats["w_warehouse_sk"].num_distinct == 10
    catalog.close()


def test_overlay_fills_not_overrides_row_count(catalog_path):
    """A present source row count is KEPT even when the catalog learned a
    different one - fill gaps, do not override (overriding destabilizes
    orientation decisions tuned against source estimates)."""
    from federated_query.datasources.base import TableStatistics

    catalog = _catalog(catalog_path)
    catalog.record_table_rows("pg", "public", "customer", 999)
    source_stats = TableStatistics.create(
        row_count=500000, total_size_bytes=0, column_stats={}
    )
    collector = _collector(source_stats, catalog)
    stats = collector.get_table_statistics("pg", "public", "customer", [])
    assert stats.row_count == 500000
    catalog.close()


def test_overlay_absent_leaves_source_unchanged(catalog_path):
    """With nothing learned for a table, the source's stats pass through."""
    from federated_query.datasources.base import TableStatistics

    catalog = _catalog(catalog_path)
    source_stats = TableStatistics.create(
        row_count=500, total_size_bytes=0, column_stats={}
    )
    collector = _collector(source_stats, catalog)
    stats = collector.get_table_statistics("pg", "public", "other", [])
    assert stats.row_count == 500
    catalog.close()


def test_unknown_field_raises(catalog_path):
    """The internal upsert refuses a field outside its allow-list (defensive:
    no dynamic SQL over an unvetted column name)."""
    catalog = _catalog(catalog_path)
    with pytest.raises(ValueError):
        catalog._upsert_table_stat("pg", "public", "t", "", "measured_junk", 1)
    catalog.close()
