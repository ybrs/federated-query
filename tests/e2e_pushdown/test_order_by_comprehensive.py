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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "price"
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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "price"
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
    first_col = unwrap_parens(first_order.this)
    assert isinstance(first_col, exp.Column)
    assert first_col.name.lower() == "region"
    assert first_order.args.get("desc") is False or first_order.args.get("desc") is None

    second_order = expressions[1]
    second_col = unwrap_parens(second_order.this)
    assert isinstance(second_col, exp.Column)
    assert second_col.name.lower() == "price"
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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "region"


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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "region"


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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Mul)

    left_col = unwrap_parens(order_expr.left)
    right_col = unwrap_parens(order_expr.right)
    assert isinstance(left_col, exp.Column)
    assert isinstance(right_col, exp.Column)
    assert left_col.name.lower() == "price"
    assert right_col.name.lower() == "quantity"

    assert first_order.args.get("desc") is True


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
    assert len(expressions) == 2

    first_order = expressions[0]
    assert first_order.args.get("desc") is False or first_order.args.get("desc") is None

    second_order = expressions[1]
    assert second_order.args.get("desc") is True


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "price"
    assert first_order.args.get("desc") is True


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "region"

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
    projection = select_column_names(ast)
    assert "region" in projection
    assert "cnt" in projection

    order_clause = ast.args.get("order")
    assert order_clause is not None
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    assert first_order.args.get("desc") is True

    group_clause = ast.args.get("group")
    assert group_clause is not None
    group_expressions = group_clause.expressions or []
    assert len(group_expressions) == 1


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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Upper)

    upper_col = unwrap_parens(order_expr.this)
    assert isinstance(upper_col, exp.Column)
    assert upper_col.name.lower() == "region"


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
    assert len(expressions) == 1

    first_order = expressions[0]
    order_expr = unwrap_parens(first_order.this)
    assert isinstance(order_expr, exp.Case)

    ifs = order_expr.args.get("ifs") or []
    assert len(ifs) == 1


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "price"
    assert first_order.args.get("desc") is True

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 10

    offset_clause = ast.args.get("offset")
    assert isinstance(offset_clause, exp.Offset)
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 20


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.table == "P"
    assert order_col.name.lower() == "name"

    joins = ast.args.get("joins") or []
    assert len(joins) == 1


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "region"
    assert first_order.args.get("desc") is True


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

    first_order = expressions[0]
    first_col = unwrap_parens(first_order.this)
    assert isinstance(first_col, exp.Column)
    assert first_col.name.lower() == "region"
    assert first_order.args.get("desc") is False or first_order.args.get("desc") is None

    second_order = expressions[1]
    second_col = unwrap_parens(second_order.this)
    assert isinstance(second_col, exp.Column)
    assert second_col.name.lower() == "status"
    assert second_order.args.get("desc") is True

    third_order = expressions[2]
    third_col = unwrap_parens(third_order.this)
    assert isinstance(third_col, exp.Column)
    assert third_col.name.lower() == "price"
    assert third_order.args.get("desc") is False or third_order.args.get("desc") is None


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
    expressions = order_clause.expressions
    assert len(expressions) == 1

    first_order = expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "price"
    assert first_order.args.get("desc") is True

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)


def test_order_by_with_group_by(single_source_env):
    """Validates ORDER BY with GROUP BY pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, SUM(price) AS total FROM duckdb_primary.main.orders "
        "GROUP BY region "
        "ORDER BY total DESC"
    )
    ast = explain_datasource_query(runtime, sql)
    projection = select_column_names(ast)
    assert "region" in projection
    assert "total" in projection

    order_clause = ast.args.get("order")
    assert order_clause is not None
    order_expressions = order_clause.expressions
    assert len(order_expressions) == 1
    first_order = order_expressions[0]
    assert first_order.args.get("desc") is True

    group_clause = ast.args.get("group")
    assert group_clause is not None
    group_expressions = group_clause.expressions or []
    assert len(group_expressions) == 1
    group_col = unwrap_parens(group_expressions[0])
    assert isinstance(group_col, exp.Column)
    assert group_col.name.lower() == "region"


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
    projection = select_column_names(ast)
    assert "region" in projection
    assert "cnt" in projection

    order_clause = ast.args.get("order")
    assert order_clause is not None
    order_expressions = order_clause.expressions
    assert len(order_expressions) == 1
    first_order = order_expressions[0]
    assert first_order.args.get("desc") is True

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left_expr = unwrap_parens(predicate.left)
    assert isinstance(left_expr, exp.Count)

    right_expr = unwrap_parens(predicate.right)
    assert isinstance(right_expr, exp.Literal)
    assert int(right_expr.this) == 5

    group_clause = ast.args.get("group")
    assert group_clause is not None
    group_expressions = group_clause.expressions or []
    assert len(group_expressions) == 1
    group_col = unwrap_parens(group_expressions[0])
    assert isinstance(group_col, exp.Column)
    assert group_col.name.lower() == "region"
