"""Cross-source CTE (WITH clause) execution tests.

A CTE whose body or references span data sources cannot push to one source as a
single ``WITH``. Two strategies run in the merge engine:

  ``PhysicalCTE`` producer) and every reference reads that one table (a
  ``PhysicalCTEScan``); the child's CTE-vs-other-source joins run in the merge
  engine.
  (``PhysicalCTEMergeQuery``): every base source relation is materialized and
  registered, and DuckDB computes the fixpoint locally.

Results are checked against the same query on one combined DuckDB seeded with
identical data.
"""

import duckdb
import pytest

from federated_query.catalog import Catalog
from federated_query.plan.physical import (
    PhysicalCTE,
    PhysicalCTEScan,
    PhysicalCTEMergeQuery,
)
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


def test_body_single_source_child_joins_other_source(multi_source_env):
    """A single-source CTE body joined in the child against a second source."""
    engine_sql = (
        "WITH eu AS ("
        "  SELECT order_id, customer_id FROM duckdb_orders.main.orders "
        "  WHERE region = 'EU'"
        ") "
        "SELECT e.order_id, c.segment FROM eu e "
        "JOIN duckdb_customers.main.customers c ON e.customer_id = c.customer_id"
    )
    reference_sql = (
        "WITH eu AS ("
        "  SELECT order_id, customer_id FROM orders WHERE region = 'EU'"
        ") "
        "SELECT e.order_id, c.segment FROM eu e "
        "JOIN customers c ON e.customer_id = c.customer_id"
    )
    _assert_matches(multi_source_env, engine_sql, reference_sql)


def test_body_itself_cross_source(multi_source_env):
    """The CTE body is a cross-source join; the child filters its output."""
    engine_sql = (
        "WITH joined AS ("
        "  SELECT o.order_id, c.segment "
        "  FROM duckdb_orders.main.orders o "
        "  JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id"
        ") "
        "SELECT order_id FROM joined WHERE segment = 'enterprise'"
    )
    reference_sql = (
        "WITH joined AS ("
        "  SELECT o.order_id, c.segment FROM orders o "
        "  JOIN customers c ON o.customer_id = c.customer_id"
        ") "
        "SELECT order_id FROM joined WHERE segment = 'enterprise'"
    )
    _assert_matches(multi_source_env, engine_sql, reference_sql)


def test_multiple_references_materialize_once(multi_source_env):
    """A CTE referenced twice is materialized once: one producer, two scans."""
    engine_sql = (
        "WITH cust AS ("
        "  SELECT customer_id, segment FROM duckdb_customers.main.customers "
        "  WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id FROM duckdb_orders.main.orders o "
        "JOIN cust a ON o.customer_id = a.customer_id "
        "JOIN cust b ON o.customer_id = b.customer_id"
    )
    reference_sql = (
        "WITH cust AS ("
        "  SELECT customer_id, segment FROM customers WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id FROM orders o "
        "JOIN cust a ON o.customer_id = a.customer_id "
        "JOIN cust b ON o.customer_id = b.customer_id"
    )
    _assert_matches(multi_source_env, engine_sql, reference_sql)

    plan = _physical_plan(multi_source_env, engine_sql)
    scans = _collect(plan, PhysicalCTEScan)
    assert len(scans) == 2
    producers = _collect(plan, PhysicalCTE)
    # Both scans share one producer, so the body materializes a single time.
    producer_ids = set()
    for scan in scans:
        producer_ids.add(id(scan.producer))
    assert len(producer_ids) == 1
    assert producers[0] is scans[0].producer


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


def test_cross_source_explicit_column_list(multi_source_env):
    """A cross-source CTE with an explicit column list relabels its output."""
    engine_sql = (
        "WITH t(cid, seg) AS ("
        "  SELECT customer_id, segment FROM duckdb_customers.main.customers "
        "  WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id, t.seg FROM duckdb_orders.main.orders o "
        "JOIN t ON o.customer_id = t.cid"
    )
    reference_sql = (
        "WITH t(cid, seg) AS ("
        "  SELECT customer_id, segment FROM customers WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id, t.seg FROM orders o JOIN t ON o.customer_id = t.cid"
    )
    _assert_matches(multi_source_env, engine_sql, reference_sql)


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


_SCHEMA_SQL = (
    "WITH RECURSIVE chain(id, name, lvl) AS ("
    "  SELECT id, name, 0 FROM ds_emp.main.employees WHERE manager_id IS NULL"
    "  UNION ALL"
    "  SELECT e.id, e.name, c.lvl + 1 FROM ds_emp.main.employees e "
    "  JOIN chain c ON e.manager_id = c.id"
    ") "
    "SELECT ch.name, ch.lvl, b.amount FROM chain ch "
    "JOIN ds_bonus.main.bonuses b ON b.emp_id = ch.id"
)


def test_merge_query_output_names_carried(hierarchy_env):
    """``PhysicalCTEMergeQuery`` carries its result column names as a plan node.

    The Rust engine types the (recursive) WITH result natively; the physical
    node is a pure representation, so its declared output names are what the plan
    exposes. The output types come from execution, so ``schema()`` is not
    computed in Python (it raises) - this pins the static column contract.
    """
    plan = _physical_plan(hierarchy_env, _SCHEMA_SQL)
    merge_query = _collect(plan, PhysicalCTEMergeQuery)[0]
    assert merge_query.output_names == ["name", "lvl", "amount"]
    assert "PhysicalCTEMergeQuery" in repr(merge_query)


def test_producer_and_scan_repr(multi_source_env):
    """A CTE referenced twice yields one producer and readable node reprs."""
    engine_sql = (
        "WITH cust AS ("
        "  SELECT customer_id, segment FROM duckdb_customers.main.customers "
        "  WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id FROM duckdb_orders.main.orders o "
        "JOIN cust a ON o.customer_id = a.customer_id "
        "JOIN cust b ON o.customer_id = b.customer_id"
    )
    plan = _physical_plan(multi_source_env, engine_sql)
    producer = _collect(plan, PhysicalCTE)[0]
    assert "PhysicalCTE(cust)" in repr(producer)
    scan = _collect(plan, PhysicalCTEScan)[0]
    assert "PhysicalCTEScan(cust)" in repr(scan)


def test_cte_ref_without_producer_fails_fast():
    """Planning a CTERef with no registered producer is an internal invariant.

    A bound query always registers its CTE before the child is planned, so this
    fail-fast is unreachable through SQL; it guards against a planner bug.
    """
    from federated_query.catalog import Catalog
    from federated_query.optimizer.physical_planner import PhysicalPlanner
    from federated_query.plan.logical import CTERef

    planner = PhysicalPlanner(Catalog())
    with pytest.raises(ValueError, match="not in scope"):
        planner._plan_cte_ref(CTERef(name="ghost"))


def test_constant_cte_in_multi_source_catalog(multi_source_env):
    """A scan-less CTE in a multi-source catalog has no single source to push to.

    With several sources, pushdown cannot default the pure-computation body to
    one of them, so it materializes through the producer path and the child
    """
    plan = _physical_plan(multi_source_env, "WITH x AS (SELECT 1 AS n) SELECT n FROM x")
    producers = _collect(plan, PhysicalCTE)
    assert len(producers) == 1
    runtime = build_runtime(multi_source_env)
    assert _rows(runtime.execute("WITH x AS (SELECT 1 AS n) SELECT n FROM x")) == [(1,)]


def test_recursive_cross_source_unrenderable_fails_fast(monkeypatch, hierarchy_env):
    """An unrenderable recursive cross-source CTE fails fast, never silently.

    The recursive-merge path needs the whole WITH rendered to SQL for DuckDB;
    if the renderer cannot express the CTE, the planner raises rather than
    producing a wrong result. Forced here by making the renderer decline.
    """
    from federated_query.optimizer.single_source_pushdown import SingleSourcePushdown

    monkeypatch.setattr(
        SingleSourcePushdown,
        "render_correlated_sql",
        lambda self, node, scan_names: None,
    )
    runtime = build_runtime(hierarchy_env)
    with pytest.raises(ValueError, match="not renderable"):
        runtime.query_executor._plan_pipeline(_SCHEMA_SQL, None)


def test_multiple_references_emit_body_once(multi_source_env):
    """The IR executes a twice-referenced CTE body exactly ONCE.

    The shared producer used to be re-emitted per reference, re-running the
    whole body (TPC-DS q04 executed its 6-times-referenced year_total body
    18 times). The body's scan must appear once in the IR, and its binding
    must feed BOTH references (the engine clones a shared binding until its
    last consumer takes it).
    """
    from collections import Counter

    from federated_query.executor.rust_ir import build_ir

    engine_sql = (
        "WITH cust AS ("
        "  SELECT customer_id, segment FROM duckdb_customers.main.customers "
        "  WHERE segment = 'enterprise'"
        ") "
        "SELECT o.order_id FROM duckdb_orders.main.orders o "
        "JOIN cust a ON o.customer_id = a.customer_id "
        "JOIN cust b ON o.customer_id = b.customer_id"
    )
    plan = _physical_plan(multi_source_env, engine_sql)
    ir = build_ir(plan)

    body_scans = 0
    for step in ir["steps"]:
        if step["op"] == "source_scan" and "customers" in str(step.get("scan")):
            body_scans += 1
    assert body_scans == 1

    consumed = Counter()
    for step in ir["steps"]:
        if step["op"] == "merge":
            for binding in step["inputs"].values():
                consumed[binding] += 1
    assert max(consumed.values()) >= 2
