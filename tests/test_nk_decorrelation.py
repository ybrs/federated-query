"""Neumann-Kemper general decorrelation: differential tests vs DuckDB.

Each query is planned through the engine (decorrelated to regular relational
algebra, executed on the Rust engine by default) and compared, exact, to DuckDB
reading the same data. A helper also asserts the decorrelated plan contains NO
LateralJoin - the whole point is that these correlations lower to ordinary joins.

Requires the fedqrs extension and DuckDB; skips cleanly if fedqrs is absent.
"""

import os
import tempfile

import duckdb
import pytest

pytest.importorskip("fedqrs")

from federated_query.catalog import Catalog
from federated_query.config.config import ExecutorConfig
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.executor.executor import Executor
from federated_query.optimizer import (
    AggregatePushdownRule, LimitPushdownRule, OrderByPushdownRule,
    PredicatePushdownRule, ProjectionPushdownRule, RuleBasedOptimizer,
)
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser
from federated_query.plan.physical import PhysicalLateralJoin
from federated_query.processor import QueryExecutor, StarExpansionProcessor


def _make_source(directory, name, ddl_rows):
    """Create a one-table DuckDB file and return a connected datasource."""
    path = os.path.join(directory, f"{name}.duckdb")
    setup = duckdb.connect(path)
    for statement in ddl_rows:
        setup.execute(statement)
    setup.close()
    ds = DuckDBDataSource(name, {"path": path, "read_only": True})
    ds.connect()
    return ds, path


@pytest.fixture(scope="module")
def env():
    """orders and products in SEPARATE DuckDB sources, so a correlation between
    them genuinely crosses sources (cannot push to one) and must decorrelate. A
    single DuckDB with both tables is the oracle."""
    directory = tempfile.mkdtemp()
    ds_o, _ = _make_source(directory, "src_o", [
        "CREATE TABLE orders (order_id INTEGER, price DOUBLE)",
        "INSERT INTO orders VALUES (1, 100.0), (2, 30.0), (3, 5.0), (4, 250.0), (5, NULL)",
    ])
    ds_p, _ = _make_source(directory, "src_p", [
        "CREATE TABLE products (id INTEGER, base_price DOUBLE)",
        "INSERT INTO products VALUES (1, 10.0), (2, 20.0), (3, 40.0), (4, 200.0)",
    ])
    oracle_path = os.path.join(directory, "oracle.duckdb")
    setup = duckdb.connect(oracle_path)
    setup.execute("CREATE TABLE orders (order_id INTEGER, price DOUBLE)")
    setup.execute("INSERT INTO orders VALUES (1, 100.0), (2, 30.0), (3, 5.0), (4, 250.0), (5, NULL)")
    setup.execute("CREATE TABLE products (id INTEGER, base_price DOUBLE)")
    setup.execute("INSERT INTO products VALUES (1, 10.0), (2, 20.0), (3, 40.0), (4, 200.0)")
    setup.close()

    catalog = Catalog()
    catalog.register_datasource(ds_o)
    catalog.register_datasource(ds_p)
    catalog.load_metadata()
    parser = Parser()
    optimizer = RuleBasedOptimizer(catalog)
    for rule in (PredicatePushdownRule(), ProjectionPushdownRule(),
                 AggregatePushdownRule(), OrderByPushdownRule(), LimitPushdownRule()):
        optimizer.add_rule(rule)
    qe = QueryExecutor(
        catalog=catalog, parser=parser, binder=Binder(catalog), optimizer=optimizer,
        planner=PhysicalPlanner(catalog), physical_executor=Executor(ExecutorConfig()),
        processors=[StarExpansionProcessor(catalog, dialect=parser.dialect)],
        decorrelator=Decorrelator())
    oracle = duckdb.connect(oracle_path, read_only=True)
    yield qe, oracle
    ds_o.disconnect()
    ds_p.disconnect()
    oracle.close()


def _has_lateral(node) -> bool:
    """True if any node in the physical plan is a LateralJoin."""
    if isinstance(node, PhysicalLateralJoin):
        return True
    for child in node.children():
        if _has_lateral(child):
            return True
    return False


def _rows(table):
    """Order-insensitive, decimal/float-normalized rows from an Arrow table."""
    out = []
    for i in range(table.num_rows):
        row = []
        for column in table.columns:
            value = column[i].as_py()
            row.append(round(float(value), 4) if isinstance(value, float) else value)
        out.append(tuple(row))
    return sorted(out, key=lambda r: tuple(str(x) for x in r))


def _oracle_rows(oracle, sql):
    """Reference rows from DuckDB, normalized like _rows."""
    normed = []
    for row in oracle.execute(sql).fetchall():
        normed.append(tuple(round(float(x), 4) if isinstance(x, float) else x for x in row))
    return sorted(normed, key=lambda r: tuple(str(x) for x in r))


def _assert_nk(env, engine_sql, oracle_sql=None):
    """Plan+run on the engine; assert no LATERAL and an exact match to DuckDB."""
    qe, oracle = env
    plan = qe._plan_pipeline(engine_sql, profiler=None)
    assert not _has_lateral(plan), "plan still contains a LateralJoin (not decorrelated)"
    got = _rows(qe.execute(engine_sql))
    expected = _oracle_rows(oracle, oracle_sql or engine_sql)
    assert got == expected, f"got {got}\nexpected {expected}"


def test_non_equi_scalar_aggregate_max(env):
    """MAX with a non-equi correlation must decorrelate (no LATERAL) and match."""
    _assert_nk(env, (
        "SELECT o.order_id, "
        "  (SELECT MAX(p.base_price) FROM src_p.main.products p WHERE p.base_price < o.price) AS m "
        "FROM src_o.main.orders o"
    ), (
        "SELECT o.order_id, "
        "  (SELECT MAX(p.base_price) FROM products p WHERE p.base_price < o.price) AS m "
        "FROM orders o"
    ))


def test_non_equi_correlated_count_bug(env):
    """COUNT over an empty non-equi correlation must be 0 (the count bug), not NULL."""
    _assert_nk(env, (
        "SELECT o.order_id, "
        "  (SELECT COUNT(*) FROM src_p.main.products p WHERE p.base_price < o.price) AS c "
        "FROM src_o.main.orders o"
    ), (
        "SELECT o.order_id, "
        "  (SELECT COUNT(*) FROM products p WHERE p.base_price < o.price) AS c "
        "FROM orders o"
    ))


@pytest.mark.parametrize("agg,op", [
    ("MAX", "<"), ("MIN", ">"), ("SUM", "<"), ("AVG", "<="), ("COUNT", ">"),
])
def test_non_equi_scalar_aggregate_variants(env, agg, op):
    """Every (aggregate, non-equi operator) pair unnests and matches DuckDB,
    including empty-match groups (MAX/SUM/AVG -> NULL, COUNT -> 0)."""
    inner = "*" if agg == "COUNT" else "p.base_price"
    _assert_nk(env, (
        f"SELECT o.order_id, (SELECT {agg}({inner}) FROM src_p.main.products p "
        f"WHERE p.base_price {op} o.price) AS v FROM src_o.main.orders o"
    ), (
        f"SELECT o.order_id, (SELECT {agg}({inner}) FROM products p "
        f"WHERE p.base_price {op} o.price) AS v FROM orders o"
    ))


def test_two_free_vars(env):
    """A correlation on two outer columns builds a two-column domain."""
    _assert_nk(env, (
        "SELECT o.order_id, (SELECT COUNT(*) FROM src_p.main.products p "
        "WHERE p.base_price < o.price AND p.id > o.order_id) AS c "
        "FROM src_o.main.orders o"
    ), (
        "SELECT o.order_id, (SELECT COUNT(*) FROM products p "
        "WHERE p.base_price < o.price AND p.id > o.order_id) AS c "
        "FROM orders o"
    ))


def test_non_equi_aggregate_in_where(env):
    """A non-equi correlated scalar aggregate used in a WHERE comparison."""
    _assert_nk(env, (
        "SELECT o.order_id FROM src_o.main.orders o WHERE o.price > "
        "(SELECT MIN(p.base_price) FROM src_p.main.products p WHERE p.base_price < o.price)"
    ), (
        "SELECT o.order_id FROM orders o WHERE o.price > "
        "(SELECT MIN(p.base_price) FROM products p WHERE p.base_price < o.price)"
    ))
