"""OR-of-subqueries (disjunctive decorrelation) execution tests.

The decorrelator rewrites each subquery disjunct into a boolean flag via a
SEMI/ANTI union split; predicate pushdown must then distribute the remaining
WHERE conjuncts into every union branch, or the branches plan as cartesian
products (TPC-DS q10/q35/q45 - they OOMed before the set-operation pushdown).
These tests pin the execution semantics of the whole pipeline cross-source.
"""

from tests.e2e_pushdown.helpers import build_runtime


def test_or_of_correlated_exists(multi_source_env):
    """EXISTS(...) OR EXISTS(...) on the same outer key (the q10/q35 shape).

    Customers with an NA order OR an APAC order: customer 1 and 4 (NA only),
    2 and 3 (APAC); customer 5 has only EU orders and must drop.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT c.customer_id FROM duckdb_customers.main.customers c "
        "WHERE EXISTS (SELECT 1 FROM duckdb_orders.main.orders o "
        "              WHERE o.customer_id = c.customer_id AND o.region = 'NA') "
        "   OR EXISTS (SELECT 1 FROM duckdb_orders.main.orders o "
        "              WHERE o.customer_id = c.customer_id AND o.region = 'APAC') "
        "ORDER BY c.customer_id"
    )
    table = runtime.execute(sql)
    assert table.column("customer_id").to_pylist() == [1, 2, 3, 4]


def test_plain_predicate_or_in_subquery(multi_source_env):
    """A plain predicate OR'd with an IN-subquery (the q45 shape).

    APAC orders (3, 7) plus orders of a 'home' product (105 -> order 9,
    106 -> order 10); every other order drops. The join keeps the query
    cross-source so the union split runs in the merge engine.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "WHERE o.region = 'APAC' "
        "   OR o.product_id IN (SELECT id FROM duckdb_products.main.products "
        "                       WHERE category = 'home') "
        "ORDER BY o.order_id"
    )
    table = runtime.execute(sql)
    assert table.column("order_id").to_pylist() == [3, 7, 9, 10]


def test_or_of_exists_preserves_outer_multiplicity(multi_source_env):
    """The union split must keep every qualifying outer row exactly once.

    The customers-orders join duplicates each customer per order; a
    distinct-union rewrite would collapse duplicates, and a naive OR of
    flags could double rows matched by BOTH disjuncts (customer 2 has an
    EU order AND an APAC order). Expected: one row per qualifying order.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "WHERE EXISTS (SELECT 1 FROM duckdb_orders.main.orders x "
        "              WHERE x.customer_id = c.customer_id AND x.region = 'EU') "
        "   OR EXISTS (SELECT 1 FROM duckdb_orders.main.orders x "
        "              WHERE x.customer_id = c.customer_id AND x.region = 'APAC') "
        "ORDER BY o.order_id"
    )
    table = runtime.execute(sql)
    # Customers with an EU or APAC order: 2 (EU+APAC), 3 (APAC+EU), 5 (EU).
    # Their orders, each exactly once: 2,3,5,7,8,10.
    assert table.column("order_id").to_pylist() == [2, 3, 5, 7, 8, 10]
