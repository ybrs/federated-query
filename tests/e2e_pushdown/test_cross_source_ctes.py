"""Cross-source recursive CTE (WITH RECURSIVE) execution tests.

A recursive CTE that feeds a cross-source join runs wholly in the merge engine
(``PhysicalCTEMergeQuery``): every base source relation is materialized and
registered, and DuckDB computes the fixpoint locally.

WITH RECURSIVE is not yet supported by the Rust parser, so these two tests fail
at parse today - the known remaining parser gap. The non-recursive cross-source
CTE tests were ported to Rust end-to-end tests in
``crates/fq-runtime/tests/it/ctes.rs``; only the recursive ones remain here.

Results are checked against the same query on one combined DuckDB seeded with
identical data.
"""

import duckdb
import pytest

from federated_query.catalog import Catalog
from federated_query.plan.physical import PhysicalCTEMergeQuery
from tests.e2e_pushdown.conftest import (
    ProxyingDuckDBDataSource,
    QueryEnvironment,
    _seed_orders,
    _seed_products,
    _seed_customers,
)
from tests.e2e_pushdown.helpers import build_runtime
from tests.duckdb_tmp import duckdb_path

_EMPLOYEE_ROWS = (
    "INSERT INTO employees VALUES "
    "(1, NULL, 'ceo'), (2, 1, 'vp'), (3, 2, 'mgr'), (4, 3, 'eng'), (5, 1, 'cfo')"
)
_BONUS_ROWS = "INSERT INTO bonuses VALUES (1, 100), (2, 50), (3, 30), (4, 10), (5, 40)"


def _seed_employees(cursor) -> None:
    """Create a self-referencing management hierarchy."""
    cursor.execute(
        "CREATE TABLE employees (id INTEGER, manager_id INTEGER, name VARCHAR)"
    )
    cursor.execute(_EMPLOYEE_ROWS)


def _seed_bonuses(cursor) -> None:
    """Create a per-employee bonus table (a second source)."""
    cursor.execute("CREATE TABLE bonuses (emp_id INTEGER, amount INTEGER)")
    cursor.execute(_BONUS_ROWS)


def _hierarchy_source(name: str, seed) -> ProxyingDuckDBDataSource:
    """Build a connected DuckDB source seeded by the given function."""
    ds = ProxyingDuckDBDataSource(
        name=name, config={"path": duckdb_path(), "read_only": False}
    )
    ds.connect()
    seed(ds.connection)
    return ds


@pytest.fixture
def hierarchy_env():
    """Two sources: a management hierarchy and a per-employee bonus table."""
    employees = _hierarchy_source("ds_emp", _seed_employees)
    bonuses = _hierarchy_source("ds_bonus", _seed_bonuses)
    catalog = Catalog()
    catalog.register_datasource(employees)
    catalog.register_datasource(bonuses)
    catalog.load_metadata()
    env = QueryEnvironment(catalog=catalog, datasources=[employees, bonuses])
    yield env
    employees.disconnect()
    bonuses.disconnect()


def _hierarchy_reference():
    """One DuckDB holding both hierarchy tables, for the expected result."""
    con = duckdb.connect()
    _seed_employees(con)
    _seed_bonuses(con)
    return con


def _reference():
    """A single DuckDB holding all three tables, for the expected result."""
    con = duckdb.connect()
    _seed_orders(con)
    _seed_products(con)
    _seed_customers(con)
    return con


def _rows(table):
    """Sort an Arrow result table into a list of value tuples."""
    columns = table.to_pydict()
    out = []
    for index in range(table.num_rows):
        row = []
        for name in table.column_names:
            row.append(columns[name][index])
        out.append(tuple(row))
    return sorted(out)


def _assert_matches(multi_source_env, engine_sql, reference_sql):
    """Compare the engine's cross-source result to the combined-DB reference."""
    runtime = build_runtime(multi_source_env)
    engine_rows = _rows(runtime.execute(engine_sql))
    con = _reference()
    expected = sorted(con.execute(reference_sql).fetchall())
    con.close()
    assert engine_rows == expected


def _physical_plan(multi_source_env, sql):
    """Build the physical plan for a query without executing it."""
    runtime = build_runtime(multi_source_env)
    return runtime.query_executor._plan_pipeline(sql, None)


def _collect(plan, node_type):
    """Collect every node of a given type in a physical plan tree."""
    found = []
    if isinstance(plan, node_type):
        found.append(plan)
    for child in plan.children():
        found.extend(_collect(child, node_type))
    return found


def test_recursive_cross_source(multi_source_env):
    """A recursive CTE feeding a cross-source join runs in the merge engine."""
    engine_sql = (
        "WITH RECURSIVE seq(n) AS ("
        "  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3"
        ") "
        "SELECT o.order_id, c.segment FROM seq "
        "JOIN duckdb_orders.main.orders o ON o.quantity = seq.n "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id"
    )
    reference_sql = (
        "WITH RECURSIVE seq(n) AS ("
        "  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3"
        ") "
        "SELECT o.order_id, c.segment FROM seq "
        "JOIN orders o ON o.quantity = seq.n "
        "JOIN customers c ON o.customer_id = c.customer_id"
    )
    _assert_matches(multi_source_env, engine_sql, reference_sql)

    plan = _physical_plan(multi_source_env, engine_sql)
    merge_queries = _collect(plan, PhysicalCTEMergeQuery)
    assert len(merge_queries) == 1
    # Both source relations are materialized and registered for DuckDB.
    assert len(merge_queries[0].inputs) == 2


def test_recursive_cross_source_hierarchy(hierarchy_env):
    """A recursive hierarchy traversal in one source, joined to a second.

    The management chain is walked recursively over ``ds_emp.employees`` and the
    whole WITH RECURSIVE runs in the merge engine over both materialized
    sources.
    """
    engine_sql = (
        "WITH RECURSIVE chain(id, name, lvl) AS ("
        "  SELECT id, name, 0 FROM ds_emp.main.employees WHERE manager_id IS NULL"
        "  UNION ALL"
        "  SELECT e.id, e.name, c.lvl + 1 FROM ds_emp.main.employees e "
        "  JOIN chain c ON e.manager_id = c.id"
        ") "
        "SELECT ch.name, ch.lvl, b.amount FROM chain ch "
        "JOIN ds_bonus.main.bonuses b ON b.emp_id = ch.id"
    )
    reference_sql = (
        "WITH RECURSIVE chain(id, name, lvl) AS ("
        "  SELECT id, name, 0 FROM employees WHERE manager_id IS NULL"
        "  UNION ALL"
        "  SELECT e.id, e.name, c.lvl + 1 FROM employees e "
        "  JOIN chain c ON e.manager_id = c.id"
        ") "
        "SELECT ch.name, ch.lvl, b.amount FROM chain ch "
        "JOIN bonuses b ON b.emp_id = ch.id"
    )
    runtime = build_runtime(hierarchy_env)
    engine_rows = _rows(runtime.execute(engine_sql))
    con = _hierarchy_reference()
    expected = sorted(con.execute(reference_sql).fetchall())
    con.close()
    assert engine_rows == expected

    plan = runtime.query_executor._plan_pipeline(engine_sql, None)
    merge_queries = _collect(plan, PhysicalCTEMergeQuery)
    assert len(merge_queries) == 1
