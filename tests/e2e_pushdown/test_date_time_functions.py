"""Date and time function pushdown tests.

Note: Some tests may fail if test data lacks date/time columns.
These tests define expected behavior for date/time pushdown.
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    find_alias_expression,
    select_column_names,
    unwrap_parens,
)


def test_extract_year_in_where(single_source_env):
    """Verifies EXTRACT(YEAR FROM date) pushes down in WHERE clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE EXTRACT(YEAR FROM created_at) = 2024"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Extract)

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 2024


def test_date_trunc_function(single_source_env):
    """Validates DATE_TRUNC() function pushes for date bucketing."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DATE_TRUNC('month', created_at) AS month, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY DATE_TRUNC('month', created_at)"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "month" in projection
    assert "cnt" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_expr = unwrap_parens(expressions[0])
    assert isinstance(group_expr, exp.DateTrunc)


def test_date_arithmetic_addition(single_source_env):
    """Ensures date arithmetic with INTERVAL pushes down."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at > CURRENT_DATE - INTERVAL '30 days'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Column)
    assert left.name.lower() == "created_at"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Sub)


def test_date_between_range(single_source_env):
    """Validates date BETWEEN range pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Between)

    col = unwrap_parens(predicate.this)
    assert isinstance(col, exp.Column)
    assert col.name.lower() == "created_at"


def test_age_function_interval_comparison(single_source_env):
    """Checks AGE() function pushes for date difference calculations."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE AGE(CURRENT_DATE, created_at) > INTERVAL '30 days'"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Anonymous) or hasattr(left, 'name')


def test_current_date_in_select(single_source_env):
    """Validates CURRENT_DATE function pushes in SELECT projection."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, CURRENT_DATE AS today "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "order_id" in projection
    assert "today" in projection

    today_expr = find_alias_expression(ast, "today")
    assert today_expr is not None
    assert isinstance(today_expr, (exp.CurrentDate, exp.CurrentTimestamp))


def test_extract_in_group_by(single_source_env):
    """Ensures EXTRACT function pushes in GROUP BY for date aggregation."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT EXTRACT(MONTH FROM created_at) AS month, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders "
        "GROUP BY EXTRACT(MONTH FROM created_at)"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "month" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    expressions = group_clause.expressions or []
    assert len(expressions) == 1

    group_expr = unwrap_parens(expressions[0])
    assert isinstance(group_expr, exp.Extract)


def test_date_function_in_order_by(single_source_env):
    """Checks date functions push in ORDER BY clause."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, created_at "
        "FROM duckdb_primary.main.orders "
        "ORDER BY DATE_TRUNC('day', created_at) DESC"
    )
    ast = explain_datasource_query(runtime, sql)

    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.DateTrunc)

    assert first_order.args.get("desc") is True


def test_timestamp_comparison(single_source_env):
    """Validates timestamp comparisons push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at >= '2024-01-01 00:00:00'::timestamp"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GTE)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Column)
    assert left.name.lower() == "created_at"


def test_date_arithmetic_edge_cases(single_source_env):
    """Ensures complex date arithmetic expressions push down."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE created_at + INTERVAL '1 year' < CURRENT_DATE"
    )
    ast = explain_datasource_query(runtime, sql)

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.LT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Add)

    add_left = unwrap_parens(left.left)
    assert isinstance(add_left, exp.Column)
    assert add_left.name.lower() == "created_at"
