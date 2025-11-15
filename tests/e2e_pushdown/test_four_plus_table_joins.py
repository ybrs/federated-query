"""Four or more table join pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    join_table_names,
    select_column_names,
    unwrap_parens,
)


def _assert_join_count(select_ast: exp.Select, expected: int) -> None:
    """Assert exact number of joins in the SELECT."""
    joins = select_ast.args.get("joins") or []
    assert len(joins) == expected, f"Expected {expected} joins, got {len(joins)}"


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
    _assert_join_count(ast, 3)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "name" in projection


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
    _assert_join_count(ast, 4)

    projection = select_column_names(ast)
    assert "order_id" in projection


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
    _assert_join_count(ast, 3)

    joins = ast.args.get("joins") or []
    first_join = joins[0]
    first_kind = first_join.args.get("kind") or first_join.args.get("side")
    assert first_kind is not None


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
    _assert_join_count(ast, 3)

    joins = ast.args.get("joins") or []
    for join in joins:
        on_clause = join.args.get("on")
        assert on_clause is not None
        condition = unwrap_parens(on_clause)
        assert isinstance(condition, exp.EQ)


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
    _assert_join_count(ast, 3)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)


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
    _assert_join_count(ast, 3)

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_col = unwrap_parens(expressions[0])
    assert isinstance(group_col, exp.Column)
    assert group_col.name.lower() == "region"


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
    _assert_join_count(ast, 3)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left_expr = unwrap_parens(predicate.left)
    assert isinstance(left_expr, exp.Sum)


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
    _assert_join_count(ast, 3)

    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    assert first_order.args.get("desc") is True

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 10


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
    _assert_join_count(ast, 5)

    projection = select_column_names(ast)
    assert "order_id" in projection


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
    _assert_join_count(ast, 3)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)

    left_pred = unwrap_parens(predicate.left)
    right_pred = unwrap_parens(predicate.right)
    assert isinstance(left_pred, exp.GT)
    assert isinstance(right_pred, exp.LT)

    left_expr = unwrap_parens(left_pred.left)
    assert isinstance(left_expr, exp.Mul)
