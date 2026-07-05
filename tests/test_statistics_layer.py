"""Tests for the lazy, per-column statistics layer (M1 of the cost-based
join-ordering optimizer).

Sources fetch statistics from their CATALOGS cheaply: PostgreSQL from
pg_class/pg_stats (schema-qualified, honest about never-analyzed tables),
DuckDB from duckdb_tables() plus one approx_count_distinct scan restricted to
the requested columns. The StatisticsCollector accumulates per-column results
in a session cache and never re-fetches a column it has already attempted.
"""

import os

import pytest

from federated_query.catalog.catalog import Catalog
from federated_query.datasources.base import ColumnStatistics, TableStatistics
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.optimizer.statistics import StatisticsCollector
from tests.duckdb_tmp import duckdb_path


# ------------------------------- DuckDB -----------------------------------


@pytest.fixture
def duck_source():
    """An on-disk DuckDB source with one small table with known statistics."""
    source = DuckDBDataSource("duck", {"path": duckdb_path(), "read_only": False})
    source.connect()
    source.connection.execute(
        "CREATE TABLE main.stats_t (id INTEGER, grp VARCHAR, maybe INTEGER)"
    )
    source.connection.execute(
        "INSERT INTO main.stats_t VALUES"
        " (1, 'a', 10), (2, 'a', NULL), (3, 'b', 30), (4, 'b', NULL)"
    )
    yield source
    source.disconnect()


class _RecordingConnection:
    """Wrap a DuckDB connection, recording every SQL string executed."""

    def __init__(self, connection):
        self.connection = connection
        self.queries = []

    def execute(self, sql, *args, **kwargs):
        """Record the SQL, then delegate to the real connection."""
        self.queries.append(sql)
        return self.connection.execute(sql, *args, **kwargs)

    def close(self):
        """Delegate teardown to the real connection."""
        self.connection.close()


def test_duckdb_row_count_only(duck_source):
    """columns=[] fetches the row count from the catalog and no column stats."""
    stats = duck_source.get_table_statistics("main", "stats_t", [])
    assert stats.row_count == 4
    assert stats.column_stats == {}


def test_duckdb_fetches_only_requested_columns(duck_source):
    """Only the requested columns appear; ndv/nulls/min/max are populated."""
    stats = duck_source.get_table_statistics("main", "stats_t", ["grp", "maybe"])
    assert sorted(stats.column_stats) == ["grp", "maybe"]
    assert stats.column_stats["grp"].num_distinct == 2
    assert stats.column_stats["grp"].null_fraction == 0.0
    assert stats.column_stats["maybe"].null_fraction == 0.5
    assert stats.column_stats["maybe"].min_value == 10
    assert stats.column_stats["maybe"].max_value == 30


def test_duckdb_single_approx_scan_no_count_distinct(duck_source):
    """One stats scan per fetch, using approx_count_distinct, never
    COUNT(DISTINCT ...) (the old full-scan-per-column path)."""
    recorder = _RecordingConnection(duck_source.connection)
    duck_source.connection = recorder
    duck_source.get_table_statistics("main", "stats_t", ["id", "grp"])
    scans = []
    for sql in recorder.queries:
        assert "COUNT(DISTINCT" not in sql.upper()
        if "approx_count_distinct" in sql:
            scans.append(sql)
    assert len(scans) == 1


def test_duckdb_unknown_table_raises(duck_source):
    """A stats request for a table the catalog does not know must raise."""
    with pytest.raises(Exception):
        duck_source.get_table_statistics("main", "no_such_table", [])


# ----------------------------- PostgreSQL ---------------------------------


def _pg_config():
    """PostgreSQL connection config from the standard environment variables."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": ["public"],
    }


@pytest.fixture
def pg_source():
    """A PostgreSQL source with two same-named tables in different schemas."""
    source = PostgreSQLDataSource("pg", _pg_config())
    source.connect()
    connection = source._get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS stats_a CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS stats_b CASCADE")
            cursor.execute("CREATE SCHEMA stats_a")
            cursor.execute("CREATE SCHEMA stats_b")
            cursor.execute("CREATE TABLE stats_a.t (k INTEGER, v VARCHAR)")
            cursor.execute("CREATE TABLE stats_b.t (k INTEGER, v VARCHAR)")
            cursor.execute(
                "INSERT INTO stats_a.t SELECT g, 'x' || (g % 5)"
                " FROM generate_series(1, 200) g"
            )
            cursor.execute("INSERT INTO stats_b.t VALUES (1, 'only')")
            cursor.execute("ANALYZE stats_a.t")
            cursor.execute("ANALYZE stats_b.t")
            cursor.execute("CREATE TABLE stats_a.never_analyzed (k INTEGER)")
            cursor.execute("INSERT INTO stats_a.never_analyzed VALUES (1), (2)")
        connection.commit()
    finally:
        source._return_connection(connection)
    yield source
    connection = source._get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP SCHEMA stats_a CASCADE")
            cursor.execute("DROP SCHEMA stats_b CASCADE")
        connection.commit()
    finally:
        source._return_connection(connection)
    source.disconnect()


def test_pg_row_count_is_schema_qualified(pg_source):
    """Same table name in two schemas: each reports its own row count."""
    stats_a = pg_source.get_table_statistics("stats_a", "t", [])
    stats_b = pg_source.get_table_statistics("stats_b", "t", [])
    assert stats_a.row_count == 200
    assert stats_b.row_count == 1


def test_pg_never_analyzed_row_count_is_none(pg_source):
    """reltuples of a never-analyzed table (-1) must surface as None, not 0."""
    stats = pg_source.get_table_statistics("stats_a", "never_analyzed", [])
    assert stats.row_count is None


def test_pg_negative_n_distinct_decodes_against_row_count(pg_source):
    """A unique column stores n_distinct=-1 (fraction); it decodes to the
    row count. A low-cardinality column stores a positive absolute count."""
    stats = pg_source.get_table_statistics("stats_a", "t", ["k", "v"])
    assert stats.column_stats["k"].num_distinct == 200
    assert stats.column_stats["v"].num_distinct == 5


def test_pg_min_max_from_histogram_bounds(pg_source):
    """A histogram-carrying integer column exposes its real min and max."""
    stats = pg_source.get_table_statistics("stats_a", "t", ["k"])
    assert stats.column_stats["k"].min_value == 1
    assert stats.column_stats["k"].max_value == 200


def test_pg_only_requested_columns(pg_source):
    """Only the requested columns are fetched from pg_stats."""
    stats = pg_source.get_table_statistics("stats_a", "t", ["k"])
    assert sorted(stats.column_stats) == ["k"]


# --------------------------- StatisticsCollector ---------------------------


class _FakeSource:
    """A stats-serving fake datasource that records every fetch request."""

    def __init__(self):
        self.requests = []

    def get_table_statistics(self, schema, table, columns):
        """Serve canned stats for the requested columns, recording the call."""
        self.requests.append((schema, table, tuple(columns)))
        column_stats = {}
        for column in columns:
            if column == "absent":
                continue
            # Canned per-column stats; the collector under test only cares
            # that the column appears with some concrete values.
            column_stats[column] = ColumnStatistics.create(
                num_distinct=7, null_fraction=0.0, avg_width=4
            )
        # Canned table stats with a fixed row count; size is irrelevant here.
        # The collector merges these into its per-table cache entry.
        return TableStatistics.create(
            row_count=100, total_size_bytes=0, column_stats=column_stats
        )


def _collector_with_fake():
    """A StatisticsCollector over a catalog holding one fake datasource."""
    catalog = Catalog()
    fake = _FakeSource()
    catalog.datasources["fake"] = fake
    return StatisticsCollector(catalog), fake


def test_collector_fetches_only_missing_columns():
    """A second request re-fetches nothing it already has."""
    collector, fake = _collector_with_fake()
    first = collector.get_table_statistics("fake", "s", "t", ["a"])
    second = collector.get_table_statistics("fake", "s", "t", ["a", "b"])
    assert fake.requests == [("s", "t", ("a",)), ("s", "t", ("b",))]
    assert first.row_count == 100
    assert sorted(second.column_stats) == ["a", "b"]


def test_collector_cached_columns_issue_no_fetch():
    """A fully-cached request reaches the source zero times."""
    collector, fake = _collector_with_fake()
    collector.get_table_statistics("fake", "s", "t", ["a", "b"])
    collector.get_table_statistics("fake", "s", "t", ["a", "b"])
    assert len(fake.requests) == 1


def test_collector_does_not_refetch_absent_columns():
    """A column the source has no stats for is attempted exactly once."""
    collector, fake = _collector_with_fake()
    first = collector.get_table_statistics("fake", "s", "t", ["absent"])
    second = collector.get_table_statistics("fake", "s", "t", ["absent"])
    assert len(fake.requests) == 1
    assert "absent" not in first.column_stats
    assert "absent" not in second.column_stats


def test_collector_unknown_datasource_raises():
    """A stats request against a datasource name the catalog does not know is
    a caller bug (a typo would silently disable costing); it must raise."""
    collector, _ = _collector_with_fake()
    with pytest.raises(Exception):
        collector.get_table_statistics("no_such_source", "s", "t", [])
