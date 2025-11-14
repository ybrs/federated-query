"""Conditional expressions and type cast pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


# Conditional Expressions


def test_case_expression_in_where(single_source_env):
    """Verifies CASE expression pushes down in WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE CASE WHEN quantity > 5 THEN 'high' ELSE 'low' END = 'high'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Case)


def test_nested_case_expressions(single_source_env):
    """Validates nested CASE expressions push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "CASE "
        "  WHEN quantity > 10 THEN 'bulk' "
        "  WHEN quantity > 5 THEN 'medium' "
        "  ELSE 'small' "
        "END AS size_category "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "size_category" in projection
    found_case = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Case):
            found_case = True
            break
    assert found_case


def test_case_in_group_by(single_source_env):
    """Ensures CASE expressions push in GROUP BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT "
        "CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END AS price_tier, "
        "COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    found_case = False
    for expression in group_clause.expressions:
        node = unwrap_parens(expression)
        if isinstance(node, exp.Case):
            found_case = True
            break
    assert found_case


def test_coalesce_in_where(single_source_env):
    """Checks COALESCE function pushes for NULL fallback handling."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE COALESCE(region, 'UNKNOWN') = 'EU'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Coalesce)


def test_nullif_function(single_source_env):
    """Validates NULLIF function pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, NULLIF(region, '') AS clean_region "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "clean_region" in projection
    found_nullif = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Nullif):
            found_nullif = True
            break
    assert found_nullif


# Type Casts


def test_cast_in_where_clause(single_source_env):
    """Verifies CAST() function pushes in WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE CAST(quantity AS VARCHAR) LIKE '1%'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)
    left = unwrap_parens(predicate.this)
    assert isinstance(left, exp.Cast)


def test_postgres_cast_syntax(single_source_env):
    """Validates PostgreSQL :: cast syntax pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE price::INTEGER > 50"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Cast)


def test_cast_in_join_condition(single_source_env):
    """Ensures CAST pushes in JOIN ON conditions."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON CAST(O.product_id AS VARCHAR) = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    if joins:
        join = joins[0]
        on_clause = join.args.get("on")
        if on_clause:
            condition = unwrap_parens(on_clause)
            assert isinstance(condition, exp.EQ)


def test_cast_in_group_by(single_source_env):
    """Checks CAST function pushes in GROUP BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT CAST(price AS INTEGER) AS price_int, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY CAST(price AS INTEGER)"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    found_cast = False
    for expression in group_clause.expressions:
        node = unwrap_parens(expression)
        if isinstance(node, exp.Cast):
            found_cast = True
            break
    assert found_cast


def test_cast_in_order_by(single_source_env):
    """Validates CAST function pushes in ORDER BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, quantity "
        "FROM duckdb_primary.main.orders "
        "ORDER BY CAST(quantity AS DECIMAL(10,2)) DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Cast)
