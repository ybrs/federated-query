"""Conditional expressions and type cast pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
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

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert right.this == "high"


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
    assert "order_id" in projection
    assert "size_category" in projection

    case_expr = find_alias_expression(ast, "size_category")
    assert case_expr is not None
    assert isinstance(case_expr, exp.Case)

    ifs = case_expr.args.get("ifs") or []
    assert len(ifs) == 2


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
    projection = select_column_names(ast)
    assert "price_tier" in projection
    assert "cnt" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_expr = unwrap_parens(expressions[0])
    assert isinstance(group_expr, exp.Case)


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
    coalesce_args = left.expressions
    assert len(coalesce_args) == 2

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert right.this == "EU"


def test_nullif_function(single_source_env):
    """Validates NULLIF function pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, NULLIF(region, '') AS clean_region "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "clean_region" in projection

    nullif_expr = find_alias_expression(ast, "clean_region")
    assert nullif_expr is not None
    assert isinstance(nullif_expr, exp.Nullif)

    first_arg = unwrap_parens(nullif_expr.this)
    assert isinstance(first_arg, exp.Column)
    assert first_arg.name.lower() == "region"


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
    cast_expr = unwrap_parens(left.this)
    assert isinstance(cast_expr, exp.Column)
    assert cast_expr.name.lower() == "quantity"

    pattern = predicate.expression
    assert isinstance(pattern, exp.Literal)
    assert pattern.this == "1%"


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
    cast_expr = unwrap_parens(left.this)
    assert isinstance(cast_expr, exp.Column)
    assert cast_expr.name.lower() == "price"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 50


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
    assert len(joins) == 1

    join = joins[0]
    on_clause = join.args.get("on")
    assert on_clause is not None
    condition = unwrap_parens(on_clause)
    assert isinstance(condition, exp.EQ)

    left = unwrap_parens(condition.left)
    assert isinstance(left, exp.Cast)
    cast_expr = unwrap_parens(left.this)
    assert isinstance(cast_expr, exp.Column)
    assert cast_expr.table == "O"
    assert cast_expr.name.lower() == "product_id"


def test_cast_in_group_by(single_source_env):
    """Checks CAST function pushes in GROUP BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT CAST(price AS INTEGER) AS price_int, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY CAST(price AS INTEGER)"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "price_int" in projection
    assert "cnt" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_expr = unwrap_parens(expressions[0])
    assert isinstance(group_expr, exp.Cast)
    cast_expr = unwrap_parens(group_expr.this)
    assert isinstance(cast_expr, exp.Column)
    assert cast_expr.name.lower() == "price"


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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Cast)
    cast_expr = unwrap_parens(order_expr.this)
    assert isinstance(cast_expr, exp.Column)
    assert cast_expr.name.lower() == "quantity"

    assert first_order.args.get("desc") is True
