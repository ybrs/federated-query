"""Section 6 guardrails for cross-datasource queries."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    datasource_query_map,
    explain_document,
    from_table_name,
    select_column_names,
    unwrap_parens,
)


def _assert_single_source_scan(select_ast: exp.Select, expected_table: str):
    """Ensure a SELECT represents a single-table scan against the given table."""
    joins = select_ast.args.get("joins") or []
    assert not joins, "cross-source remote joins must not occur"
    assert from_table_name(select_ast) == expected_table


def test_orders_products_join_stays_local(multi_source_env):
    """Ensures cross-source orders/products joins emit independent scans."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id, p.name "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_products.main.products p ON o.product_id = p.id"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {"duckdb_orders", "duckdb_products"}
    orders_query = queries["duckdb_orders"]
    products_query = queries["duckdb_products"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(products_query, "products")
    assert set(select_column_names(orders_query)) == {"order_id", "product_id"}
    assert set(select_column_names(products_query)) == {"id", "name"}


def test_orders_customers_join_scans_each_source(multi_source_env):
    """Validates joining orders with customers keeps per-datasource scans."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id, c.segment "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c "
        "ON o.customer_id = c.customer_id"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {"duckdb_orders", "duckdb_customers"}
    orders_query = queries["duckdb_orders"]
    customers_query = queries["duckdb_customers"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(customers_query, "customers")
    assert set(select_column_names(orders_query)) == {"order_id", "customer_id"}
    assert set(select_column_names(customers_query)) == {"customer_id", "segment"}


def test_three_source_join_emits_three_queries(multi_source_env):
    """Checks three-way joins across datasources produce three independent scans."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id, p.name, c.segment "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_products.main.products p ON o.product_id = p.id "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {
        "duckdb_orders",
        "duckdb_products",
        "duckdb_customers",
    }
    orders_query = queries["duckdb_orders"]
    products_query = queries["duckdb_products"]
    customers_query = queries["duckdb_customers"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(products_query, "products")
    _assert_single_source_scan(customers_query, "customers")
    assert set(select_column_names(orders_query)) == {
        "order_id",
        "product_id",
        "customer_id",
    }
    assert set(select_column_names(products_query)) == {"id", "name"}
    assert set(select_column_names(customers_query)) == {"customer_id", "segment"}


def test_cross_source_filters_remain_local(multi_source_env):
    """Ensures predicates on cross-source joins are not pushed to remote scans."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_products.main.products p ON o.product_id = p.id "
        "WHERE o.region = 'EU' AND p.category = 'clothing'"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {"duckdb_orders", "duckdb_products"}
    orders_query = queries["duckdb_orders"]
    products_query = queries["duckdb_products"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(products_query, "products")
    orders_where = orders_query.args.get("where")
    products_where = products_query.args.get("where")
    assert orders_where is not None, "orders filter should push down"
    assert products_where is not None, "products filter should push down"
    assert isinstance(unwrap_parens(orders_where.this), exp.EQ)
    assert isinstance(unwrap_parens(products_where.this), exp.EQ)


def test_cross_source_aggregates_and_limits_stay_local(multi_source_env):
    """Verifies aggregates and limits across datasources never push down remotely."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.region, SUM(p.base_price) AS total_cost "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_products.main.products p ON o.product_id = p.id "
        "GROUP BY o.region "
        "LIMIT 5"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {"duckdb_orders", "duckdb_products"}
    orders_query = queries["duckdb_orders"]
    products_query = queries["duckdb_products"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(products_query, "products")
    assert orders_query.args.get("group") is None
    assert orders_query.args.get("limit") is None
    assert set(select_column_names(orders_query)) == {"region", "product_id"}
    assert set(select_column_names(products_query)) == {"id", "base_price"}


def test_cross_source_order_by_stays_local(multi_source_env):
    """ORDER BY across datasources must not push into remote scans."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id, p.name "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_products.main.products p ON o.product_id = p.id "
        "ORDER BY p.name"
    )
    document = explain_document(runtime, sql)
    queries = datasource_query_map(document)
    assert set(queries) == {"duckdb_orders", "duckdb_products"}
    orders_query = queries["duckdb_orders"]
    products_query = queries["duckdb_products"]
    _assert_single_source_scan(orders_query, "orders")
    _assert_single_source_scan(products_query, "products")
    assert orders_query.args.get("order") is None
    assert products_query.args.get("order") is None
