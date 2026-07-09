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


def test_unknown_field_raises(catalog_path):
    """The internal upsert refuses a field outside its allow-list (defensive:
    no dynamic SQL over an unvetted column name)."""
    catalog = _catalog(catalog_path)
    with pytest.raises(ValueError):
        catalog._upsert_table_stat("pg", "public", "t", "", "measured_junk", 1)
    catalog.close()
