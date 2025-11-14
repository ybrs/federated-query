"""Four or more table join pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    join_table_names,
    select_column_names,
    unwrap_parens,
)


def test_four_table_inner_join_chain(single_source_env):
    """Verifies 4-table INNER JOIN chain pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2


def test_five_table_inner_join_chain(single_source_env):
    """Validates 5-table INNER JOIN chain pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "JOIN duckdb_primary.main.orders O3 ON O2.status = O3.status"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 3


def test_four_table_mixed_join_types(single_source_env):
    """Ensures 4-table query with mixed INNER/LEFT/RIGHT pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "INNER JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "LEFT JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "RIGHT JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2


def test_star_schema_one_fact_three_dimensions(single_source_env):
    """Validates star schema pattern (1 fact + 3 dimension tables)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.order_id = O2.order_id "
        "JOIN duckdb_primary.main.products P2 ON O.product_id = P2.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2


def test_four_table_with_where_on_all_tables(single_source_env):
    """Checks 4-table join with WHERE predicates on all tables."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "WHERE O.status = 'shipped' "
        "AND P.active = true "
        "AND O2.quantity > 5 "
        "AND P2.base_price > 100"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_four_table_with_aggregation(single_source_env):
    """Validates 4-table join with GROUP BY aggregation."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "GROUP BY O.region"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2
    group_clause = ast.args.get("group")
    assert group_clause is not None


def test_four_table_with_having(single_source_env):
    """Ensures 4-table join with HAVING clause pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(O.quantity) AS total_qty "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "GROUP BY O.region "
        "HAVING SUM(O.quantity) > 100"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_four_table_with_order_by_limit(single_source_env):
    """Checks 4-table join with ORDER BY and LIMIT."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "ORDER BY O.order_id DESC "
        "LIMIT 10"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2
    order_clause = ast.args.get("order")
    assert order_clause is not None
    limit_clause = ast.args.get("limit")
    assert limit_clause is not None


def test_six_table_join(single_source_env):
    """Validates 6-table join pushes or falls back appropriately."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "JOIN duckdb_primary.main.orders O3 ON O2.status = O3.status "
        "JOIN duckdb_primary.main.products P3 ON O3.product_id = P3.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2


def test_complex_multi_table_with_subquery_filters(single_source_env):
    """Ensures complex multi-table query with computed predicates."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "JOIN duckdb_primary.main.orders O2 ON O.region = O2.region "
        "JOIN duckdb_primary.main.products P2 ON O2.product_id = P2.id "
        "WHERE O.price * O.quantity > 1000 "
        "AND P.base_price < 500"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert len(joins) >= 2
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
