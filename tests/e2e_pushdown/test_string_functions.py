"""String function pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
    select_column_names,
    unwrap_parens,
)


def test_upper_function_in_where(single_source_env):
    """Verifies UPPER() function pushes down in WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE UPPER(status) = 'PROCESSING'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Upper)
    col = unwrap_parens(left.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "status"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert right.this == "PROCESSING"


def test_lower_function_with_like(single_source_env):
    """Validates LOWER() with LIKE pattern pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE LOWER(region) LIKE '%eu%'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)

    left = unwrap_parens(predicate.this)
    assert isinstance(left, exp.Lower)
    col = unwrap_parens(left.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "region"

    pattern = predicate.expression
    assert isinstance(pattern, exp.Literal)
    assert pattern.this == "%eu%"


def test_length_function_comparison(single_source_env):
    """Ensures LENGTH() function pushes down for string length checks."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "WHERE LENGTH(name) > 10"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Length)
    col = unwrap_parens(left.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "name"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 10


def test_substring_function_in_where(single_source_env):
    """Validates SUBSTRING() function pushes for prefix matching."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "WHERE SUBSTRING(name, 1, 3) = 'Pro'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Substring)
    col = unwrap_parens(left.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "name"


def test_trim_function_in_where(single_source_env):
    """Checks TRIM() function pushes for whitespace removal."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE TRIM(region) = 'EU'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Trim)
    col = unwrap_parens(left.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "region"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert right.this == "EU"


def test_string_concatenation_operator(single_source_env):
    """Validates string concatenation with || operator pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name || ' (product)' AS labeled_name "
        "FROM duckdb_primary.main.products"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "labeled_name" in projection

    concat_expr = find_alias_expression(ast, "labeled_name")
    assert concat_expr is not None
    assert isinstance(concat_expr, (exp.Concat, exp.DPipe))


def test_concat_function_multiple_columns(single_source_env):
    """Ensures CONCAT() function with multiple args pushes down."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT CONCAT(name, ' - ', category) AS full_label "
        "FROM duckdb_primary.main.products"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "full_label" in projection

    concat_expr = find_alias_expression(ast, "full_label")
    assert concat_expr is not None
    assert isinstance(concat_expr, exp.Concat)


def test_string_functions_in_select_projection(single_source_env):
    """Validates UPPER() and LOWER() push in SELECT projection."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT UPPER(status) AS status_upper, LOWER(region) AS region_lower "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "status_upper" in projection
    assert "region_lower" in projection

    upper_expr = find_alias_expression(ast, "status_upper")
    assert upper_expr is not None
    assert isinstance(upper_expr, exp.Upper)
    upper_col = unwrap_parens(upper_expr.this)
    assert isinstance(upper_col, exp.Column)
    assert upper_col.name.lower() == "status"

    lower_expr = find_alias_expression(ast, "region_lower")
    assert lower_expr is not None
    assert isinstance(lower_expr, exp.Lower)
    lower_col = unwrap_parens(lower_expr.this)
    assert isinstance(lower_col, exp.Column)
    assert lower_col.name.lower() == "region"


def test_string_function_in_group_by(single_source_env):
    """Checks string functions push down in GROUP BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT UPPER(status) AS status_norm, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY UPPER(status)"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "status_norm" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_expr = unwrap_parens(expressions[0])
    assert isinstance(group_expr, exp.Upper)
    col = unwrap_parens(group_expr.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "status"


def test_string_function_in_order_by(single_source_env):
    """Ensures string functions push down in ORDER BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "ORDER BY LOWER(name)"
    )
    ast = explain_datasource_query(runtime, sql)

    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Lower)
    col = unwrap_parens(order_expr.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "name"
