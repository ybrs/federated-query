"""Advanced NULL handling scenarios pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
    select_column_names,
    unwrap_parens,
)


def test_coalesce_multiple_nulls(single_source_env):
    """Verifies COALESCE with multiple NULL fallbacks pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "COALESCE(region, status, 'UNKNOWN') AS location "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "location" in projection

    coalesce_expr = find_alias_expression(ast, "location")
    assert coalesce_expr is not None
    assert isinstance(coalesce_expr, exp.Coalesce)

    coalesce_args = coalesce_expr.expressions
    assert len(coalesce_args) == 3


def test_null_in_arithmetic(single_source_env):
    """Validates NULL propagation in arithmetic (5 + NULL) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price + quantity AS total "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "total" in projection

    add_expr = find_alias_expression(ast, "total")
    assert add_expr is not None
    assert isinstance(add_expr, exp.Add)


def test_null_in_string_concatenation(single_source_env):
    """Checks NULL behavior in string concatenation pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region || ' - ' || status AS location_status "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "location_status" in projection

    concat_expr = find_alias_expression(ast, "location_status")
    assert concat_expr is not None


def test_null_in_case_expressions(single_source_env):
    """Ensures NULL handling in CASE expressions pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "CASE "
        "  WHEN region IS NULL THEN 'No Region' "
        "  WHEN region = 'EU' THEN 'Europe' "
        "  ELSE region "
        "END AS region_name "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "region_name" in projection

    case_expr = find_alias_expression(ast, "region_name")
    assert case_expr is not None
    assert isinstance(case_expr, exp.Case)

    ifs = case_expr.args.get("ifs") or []
    assert len(ifs) == 2


def test_complex_null_propagation(single_source_env):
    """Validates complex NULL propagation through nested expressions."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE COALESCE(price, 0) * COALESCE(quantity, 1) > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Mul)

    left_coalesce = unwrap_parens(left.left)
    assert isinstance(left_coalesce, exp.Coalesce)

    right_coalesce = unwrap_parens(left.right)
    assert isinstance(right_coalesce, exp.Coalesce)
