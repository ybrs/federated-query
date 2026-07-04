"""Parity tests for the Rust (fedqrs) execution backend.

Each test plans a query with the real pipeline, executes it through the Rust
engine (``execute_via_rust``), and asserts the result equals the existing
DuckDB merge-engine path (``QueryExecutor.execute``). A cross-source query is
simulated by registering the same PostgreSQL instance under two datasource
names, so the planner produces a genuine cross-source plan.

Requires a PostgreSQL harness (see README-test-harness-setup.md) and the
``fedqrs`` extension; both are skipped cleanly if absent.
"""

import os

import psycopg2
import pytest

from federated_query.catalog import Catalog
from federated_query.config.config import ExecutorConfig
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.executor import Executor
from federated_query.optimizer import (
    AggregatePushdownRule,
    LimitPushdownRule,
    OrderByPushdownRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    RuleBasedOptimizer,
)
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser
from federated_query.processor import QueryExecutor, StarExpansionProcessor

fedqrs = pytest.importorskip("fedqrs")
pytest.importorskip("adbc_driver_postgresql")

from federated_query.executor.rust_ir import (  # noqa: E402
    UnsupportedIR,
    build_ir,
    execute_via_rust,
)

SCHEMA = "rustq"


def _pg_config():
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": [SCHEMA],
        "driver": "adbc",
    }


def _raw_connect(cfg):
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["user"], password=cfg["password"],
    )


@pytest.fixture(scope="module")
def engine():
    """Two datasources over one PG (cross-source), with a small fixture schema."""
    cfg = _pg_config()
    try:
        setup = _raw_connect(cfg)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL not available: {exc}")
    setup.autocommit = True
    with setup.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
        cur.execute(f"CREATE TABLE {SCHEMA}.region (r_regionkey INT, r_name TEXT)")
        cur.execute(
            f"INSERT INTO {SCHEMA}.region VALUES "
            "(0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST')"
        )
        cur.execute(
            f"CREATE TABLE {SCHEMA}.nation "
            "(n_nationkey INT, n_name TEXT, n_regionkey INT, n_amount NUMERIC(10,2))"
        )
        cur.execute(
            f"INSERT INTO {SCHEMA}.nation VALUES "
            "(0,'ALGERIA',0,10.50),(1,'ARGENTINA',1,20.25),(2,'BRAZIL',1,5.10),"
            "(3,'CANADA',1,7.75),(4,'EGYPT',4,3.30),(5,'ETHIOPIA',0,9.90),"
            "(6,'FRANCE',3,100.01),(7,'GERMANY',3,2.20),(8,'INDIA',2,4.40),"
            "(9,'INDONESIA',2,8.80)"
        )
        # High-cardinality pair (> the 2000-key IN cap) so the engine exercises
        # the temp-table / parallel dynamic-filter strategies. hc_build has 3000
        # distinct keys; hc_probe spreads over 10000, so the filter selects ~30%
        # (the selective temp-table band).
        cur.execute(f"CREATE TABLE {SCHEMA}.hc_build (k INT)")
        cur.execute(f"INSERT INTO {SCHEMA}.hc_build SELECT g FROM generate_series(1,3000) g")
        cur.execute(f"CREATE TABLE {SCHEMA}.hc_probe (k INT, v INT)")
        cur.execute(f"INSERT INTO {SCHEMA}.hc_probe SELECT g, g*2 FROM generate_series(1,10000) g")
        cur.execute(f"ANALYZE {SCHEMA}.hc_build")
        cur.execute(f"ANALYZE {SCHEMA}.hc_probe")

    ds_a = PostgreSQLDataSource(name="srcA", config=dict(cfg))
    ds_b = PostgreSQLDataSource(name="srcB", config=dict(cfg))
    catalog = Catalog()
    for ds in (ds_a, ds_b):
        ds.connect()
        catalog.register_datasource(ds)
    catalog.load_metadata()

    parser = Parser()
    optimizer = RuleBasedOptimizer(catalog)
    for rule in (
        PredicatePushdownRule(), ProjectionPushdownRule(), AggregatePushdownRule(),
        OrderByPushdownRule(), LimitPushdownRule(),
    ):
        optimizer.add_rule(rule)
    qe = QueryExecutor(
        catalog=catalog, parser=parser, binder=Binder(catalog), optimizer=optimizer,
        planner=PhysicalPlanner(catalog), physical_executor=Executor(ExecutorConfig()),
        processors=[StarExpansionProcessor(catalog, dialect=parser.dialect)],
        decorrelator=Decorrelator(),
    )

    yield qe, [ds_a, ds_b]

    for ds in (ds_a, ds_b):
        ds.disconnect()
    with setup.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    setup.close()


def _table_rows(table):
    """Materialize a pyarrow table as a list of Python-value row tuples."""
    rows = []
    for index in range(table.num_rows):
        row = []
        for column in table.columns:
            row.append(column[index].as_py())
        rows.append(tuple(row))
    return rows


def _rows(table):
    """Row tuples, sorted, for order-insensitive comparison."""
    return sorted(_table_rows(table))


def _assert_parity(qe, datasources, sql):
    """Rust result must equal the DuckDB merge-engine result for the same SQL."""
    plan = qe._plan_pipeline(sql, profiler=None)
    rust = execute_via_rust(plan, datasources)
    reference = qe.execute(sql)
    assert _rows(rust) == _rows(reference)
    return rust


def test_cross_source_inner_join(engine):
    qe, datasources = engine
    sql = (
        "SELECT n.n_name, r.r_name "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcB.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey"
    )
    result = _assert_parity(qe, datasources, sql)
    assert result.num_rows == 10


def test_cross_source_join_with_pushed_filter(engine):
    qe, datasources = engine
    sql = (
        "SELECT n.n_name, r.r_name "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcB.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey < 2"
    )
    result = _assert_parity(qe, datasources, sql)
    assert set(result.column("r_name").to_pylist()) == {"AFRICA", "AMERICA"}


def test_single_source_aggregate(engine):
    qe, datasources = engine
    sql = (
        "SELECT r.r_name, count(*) AS n "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcA.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey GROUP BY r.r_name"
    )
    _assert_parity(qe, datasources, sql)


def test_cross_source_aggregate(engine):
    """GROUP BY on top of a cross-source join, aggregation in Rust/DataFusion."""
    qe, datasources = engine
    sql = (
        "SELECT r.r_name, count(*) AS n, sum(n.n_regionkey) AS s "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcB.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey GROUP BY r.r_name"
    )
    _assert_parity(qe, datasources, sql)


def test_cross_source_decimal_aggregate(engine):
    """Postgres NUMERIC arrives over ADBC as opaque strings; the engine casts
    them to a real number on read, so decimal arithmetic/SUM works. Compared to
    the DuckDB path within float precision."""
    qe, datasources = engine
    sql = (
        "SELECT r.r_name, sum(n.n_amount * 2) AS total "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcB.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey GROUP BY r.r_name"
    )
    plan = qe._plan_pipeline(sql, profiler=None)
    rust_tbl = execute_via_rust(plan, datasources)
    rust = dict(zip(rust_tbl.column("r_name").to_pylist(), rust_tbl.column("total").to_pylist()))
    ref_tbl = qe.execute(sql)
    ref = {r: t for r, t in zip(
        ref_tbl.column("r_name").to_pylist(), ref_tbl.column("total").to_pylist())}
    assert rust.keys() == ref.keys()
    for k in ref:
        assert abs(float(rust[k]) - float(ref[k])) < 1e-6


def test_high_cardinality_dynamic_filter(engine):
    """> 2000 distinct build keys, so the engine leaves the IN-list path and
    uses a temp-table / parallel strategy in Rust. Result must still match."""
    qe, datasources = engine
    sql = (
        "SELECT count(*) AS n, sum(p.v) AS s "
        f"FROM srcA.{SCHEMA}.hc_probe p JOIN srcB.{SCHEMA}.hc_build b ON p.k = b.k"
    )
    _assert_parity(qe, datasources, sql)


def _ordered(table):
    """Row tuples in table order, for order-sensitive comparison."""
    return _table_rows(table)


def test_cross_source_order_by(engine):
    """ORDER BY on top of a cross-source join, sorted in Rust/DataFusion.
    Compared order-sensitively against the DuckDB path."""
    qe, datasources = engine
    sql = (
        "SELECT n.n_name, r.r_name "
        f"FROM srcA.{SCHEMA}.nation n JOIN srcB.{SCHEMA}.region r "
        "ON n.n_regionkey = r.r_regionkey ORDER BY n.n_name DESC"
    )
    plan = qe._plan_pipeline(sql, profiler=None)
    rust = execute_via_rust(plan, datasources)
    reference = qe.execute(sql)
    assert _ordered(rust) == _ordered(reference)


def test_unsupported_shape_raises(engine):
    """A shape the serializer does not yet cover (here a cross-source correlated
    aggregate subquery with its own GROUP BY, which still decorrelates to a
    LATERAL) must raise, never silently emit a plan that could produce wrong rows."""
    qe, _ = engine
    sql = (
        "SELECT n.n_name, "
        f"  (SELECT MAX(r.r_regionkey) FROM srcB.{SCHEMA}.region r "
        "   WHERE r.r_regionkey < n.n_regionkey GROUP BY r.r_name) AS m "
        f"FROM srcA.{SCHEMA}.nation n"
    )
    plan = qe._plan_pipeline(sql, profiler=None)
    with pytest.raises(UnsupportedIR):
        build_ir(plan)
