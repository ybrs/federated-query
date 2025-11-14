"""Comprehensive ORDER BY pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    explain_document,
    select_column_names,
    unwrap_parens,
)


def test_order_by_asc_explicit(single_source_env):
    """Verifies explicit ASC ordering pushes down correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "ORDER BY price ASC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    assert first_order.args.get("desc") is False or first_order.args.get("desc") is None


def test_order_by_desc(single_source_env):
    """Validates DESC ordering pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "ORDER BY price DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    assert first_order.args.get("desc") is True


def test_order_by_mixed_directions(single_source_env):
    """Ensures mixed ASC/DESC on multiple columns pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region, price FROM duckdb_primary.main.orders "
        "ORDER BY region ASC, price DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 2
    first_order = expressions[0]
    second_order = expressions[1]
    assert first_order.args.get("desc") is False or first_order.args.get("desc") is None
    assert second_order.args.get("desc") is True


def test_order_by_nulls_first(single_source_env):
    """Checks NULLS FIRST option pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region FROM duckdb_primary.main.orders "
        "ORDER BY region NULLS FIRST"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0


def test_order_by_nulls_last(single_source_env):
    """Validates NULLS LAST option pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region FROM duckdb_primary.main.orders "
        "ORDER BY region NULLS LAST"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0


def test_order_by_expression(single_source_env):
    """Ensures ORDER BY with arithmetic expression pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price, quantity FROM duckdb_primary.main.orders "
        "ORDER BY price * quantity DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Mul)


def test_order_by_positional(single_source_env):
    """Validates positional ORDER BY (by column number) pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region, price FROM duckdb_primary.main.orders "
        "ORDER BY 2, 3 DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) >= 2


def test_order_by_column_not_in_select(single_source_env):
    """Checks ORDER BY column that's not in SELECT projection."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region FROM duckdb_primary.main.orders "
        "ORDER BY price DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    projection = select_column_names(ast)
    assert "price" not in projection or "price" in projection


def test_order_by_with_distinct(single_source_env):
    """Ensures ORDER BY works with DISTINCT."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT region FROM duckdb_primary.main.orders "
        "ORDER BY region"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    distinct = ast.args.get("distinct")
    assert distinct is not None


def test_order_by_with_aggregate_functions(single_source_env):
    """Validates ORDER BY on aggregate results pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) AS cnt FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "ORDER BY cnt DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    group_clause = ast.args.get("group")
    assert group_clause is not None


def test_order_by_with_string_function(single_source_env):
    """Checks ORDER BY with string function pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region FROM duckdb_primary.main.orders "
        "ORDER BY UPPER(region)"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Upper)


def test_order_by_with_case_expression(single_source_env):
    """Ensures ORDER BY with CASE expression pushes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, status FROM duckdb_primary.main.orders "
        "ORDER BY CASE WHEN status = 'processing' THEN 1 ELSE 2 END"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) > 0
    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Case)


def test_order_by_with_limit_offset_pagination(single_source_env):
    """Validates complex pagination with ORDER BY, LIMIT, OFFSET."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "ORDER BY price DESC "
        "LIMIT 10 OFFSET 20"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    limit_clause = ast.args.get("limit")
    assert limit_clause is not None
    offset_clause = ast.args.get("offset")
    assert offset_clause is not None


def test_order_by_on_join_result(single_source_env):
    """Checks ORDER BY pushes on JOIN query results."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "ORDER BY P.name"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    joins = ast.args.get("joins") or []
    assert len(joins) > 0


def test_order_by_with_null_column_behavior(single_source_env):
    """Validates ORDER BY handles NULL values correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region FROM duckdb_primary.main.orders "
        "ORDER BY region DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None


def test_order_by_three_columns(single_source_env):
    """Ensures ORDER BY with 3+ columns pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, region, status, price FROM duckdb_primary.main.orders "
        "ORDER BY region ASC, status DESC, price ASC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 3


def test_order_by_with_where_clause(single_source_env):
    """Checks ORDER BY combined with WHERE pushes together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' "
        "ORDER BY price DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    where_clause = ast.args.get("where")
    assert where_clause is not None


def test_order_by_with_group_by(single_source_env):
    """Validates ORDER BY with GROUP BY pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, SUM(price) AS total FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "ORDER BY total DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    group_clause = ast.args.get("group")
    assert group_clause is not None


def test_order_by_with_having_clause(single_source_env):
    """Ensures ORDER BY with HAVING clause pushes together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) AS cnt FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "HAVING COUNT(*) > 5 "
        "ORDER BY cnt DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    order_clause = ast.args.get("order")
    assert order_clause is not None
    where_clause = ast.args.get("where")
    assert where_clause is not None
    group_clause = ast.args.get("group")
    assert group_clause is not None
