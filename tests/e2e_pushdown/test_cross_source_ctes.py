"""Cross-source CTE (WITH clause) execution tests.

A CTE whose body or references span data sources cannot push to one source as a
single ``WITH``. Two strategies run in the merge engine:

- **Non-recursive** — the body is materialized into Arrow *once* (a
  ``PhysicalCTE`` producer) and every reference reads that one table (a
  ``PhysicalCTEScan``); the child's CTE-vs-other-source joins run in the merge
  engine.
- **Recursive** — the whole ``WITH RECURSIVE`` runs inside the in-memory DuckDB
  (``PhysicalCTEMergeQuery``): every base source relation is materialized and
  registered, and DuckDB computes the fixpoint locally.

Results are checked against the same query on one combined DuckDB seeded with
identical data.
"""

import duckdb

from federated_query.plan.physical import (
    PhysicalCTE,
    PhysicalCTEScan,
    PhysicalCTEMergeQuery,
)
from tests.e2e_pushdown.conftest import (
    _seed_orders,
    _seed_products,
    _seed_customers,
)
from tests.e2e_pushdown.helpers import build_runtime


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
