"""The cross-query plan cache: unit behavior (LRU, TTL, kill switch) and the
end-to-end contract - a hit skips planning but re-executes against the source,
so results always reflect CURRENT data."""

import pyarrow as pa

from federated_query.executor.plan_cache import (
    PlanCache,
    plan_cache_from_env,
)
from tests.e2e_pushdown.conftest import *  # noqa: F401,F403 (env fixtures)
from tests.e2e_pushdown.helpers import build_runtime


class _Clock:
    """A hand-advanced clock so TTL tests never sleep."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_hit_returns_same_plan_and_expires_on_ttl():
    """A stored plan serves back identically until the TTL, then drops."""
    clock = _Clock()
    cache = PlanCache(ttl_seconds=10.0, clock=clock)
    plan = object()
    cache.put("SELECT 1", plan)
    assert cache.get("SELECT 1") is plan
    clock.now = 11.0
    assert cache.get("SELECT 1") is None
    assert cache.get("SELECT 1") is None  # stays dropped, not resurrected


def test_lru_evicts_least_recently_used():
    """Past the size bound the least-recently-USED entry evicts, not the
    oldest-stored one."""
    cache = PlanCache(max_entries=2, ttl_seconds=100.0, clock=_Clock())
    first, second, third = object(), object(), object()
    cache.put("a", first)
    cache.put("b", second)
    assert cache.get("a") is first  # touch: 'b' is now least recent
    cache.put("c", third)
    assert cache.get("b") is None
    assert cache.get("a") is first
    assert cache.get("c") is third


def test_kill_switch_disables(monkeypatch):
    """FEDQ_PLAN_CACHE=0 yields no cache at all."""
    monkeypatch.setenv("FEDQ_PLAN_CACHE", "0")
    assert plan_cache_from_env() is None
    monkeypatch.delenv("FEDQ_PLAN_CACHE")
    assert plan_cache_from_env() is not None


def test_cached_plan_reexecutes_and_sees_new_data():
    """The contract that makes the cache safe: a HIT skips planning but
    re-reads the source, so rows inserted between executions appear. Uses a
    Postgres-backed table (server-side writes are visible to every reader;
    the DuckDB test envs pin snapshots per connection, which would test the
    environment, not the cache)."""
    import os
    from federated_query.catalog.catalog import Catalog
    from federated_query.config.config import Config
    from federated_query.datasources.postgresql import PostgreSQLDataSource
    from federated_query.cli.fedq import FedQRuntime

    source = PostgreSQLDataSource("pg", {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": ["public"],
    })
    source.connect()
    connection = source._get_connection()
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS plan_cache_probe")
        cursor.execute("CREATE TABLE plan_cache_probe (k INTEGER)")
        cursor.execute("INSERT INTO plan_cache_probe VALUES (1), (2), (3)")
    connection.commit()
    source._return_connection(connection)
    try:
        catalog = Catalog()
        catalog.register_datasource(source)
        catalog.load_metadata()
        runtime = FedQRuntime(catalog, Config())
        executor = runtime.query_executor
        sql = ("SELECT count(*) AS n FROM pg.public.plan_cache_probe"
               " WHERE k > 0")
        first = runtime.execute(sql)
        assert executor.plan_cache.hits == 0
        assert first.column("n")[0].as_py() == 3
        connection = source._get_connection()
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO plan_cache_probe VALUES (4)")
        connection.commit()
        source._return_connection(connection)
        second = runtime.execute(sql)
        assert executor.plan_cache.hits == 1
        assert second.column("n")[0].as_py() == 4
    finally:
        connection = source._get_connection()
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE plan_cache_probe")
        connection.commit()
        source._return_connection(connection)
        source.disconnect()


def test_explain_never_caches(single_source_env):
    """EXPLAIN output must reflect current planning decisions; its plan is
    never stored and never served."""
    runtime = build_runtime(single_source_env)
    executor = runtime.query_executor
    sql = "EXPLAIN SELECT id FROM duckdb_primary.main.products"
    runtime.execute(sql)
    runtime.execute(sql)
    assert executor.plan_cache.hits == 0
