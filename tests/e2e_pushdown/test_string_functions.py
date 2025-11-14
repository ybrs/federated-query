"""String function pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
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
    assert isinstance(left.this, exp.Column)


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
    found_concat = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, (exp.Concat, exp.DPipe)):
            found_concat = True
            break
    assert found_concat


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
    found_concat = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Concat):
            found_concat = True
            break
    assert found_concat


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
    found_upper = False
    found_lower = False
    for expression in ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        if isinstance(node, exp.Upper):
            found_upper = True
        if isinstance(node, exp.Lower):
            found_lower = True
    assert found_upper and found_lower


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
    found_upper = False
    for expression in group_clause.expressions:
        node = unwrap_parens(expression)
        if isinstance(node, exp.Upper):
            found_upper = True
            break
    assert found_upper


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
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Lower)
