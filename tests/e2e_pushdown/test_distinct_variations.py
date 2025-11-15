"""DISTINCT variations pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_distinct_single_column(single_source_env):
    """Verifies DISTINCT on single column pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT DISTINCT region FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    projection = select_column_names(ast)
    assert len(projection) == 1
    assert "region" in projection


def test_distinct_multiple_columns(single_source_env):
    """Validates DISTINCT on multiple columns pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT DISTINCT region, status FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    projection = select_column_names(ast)
    assert len(projection) == 2
    assert "region" in projection
    assert "status" in projection


def test_distinct_with_where(single_source_env):
    """Checks DISTINCT combined with WHERE clause pushes together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT region FROM duckdb_primary.main.orders "
        "WHERE price > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)

    left = unwrap_parens(predicate.left)
    assert isinstance(left, exp.Column)
    assert left.name.lower() == "price"

    right = unwrap_parens(predicate.right)
    assert isinstance(right, exp.Literal)
    assert int(right.this) == 100


def test_distinct_with_join(single_source_env):
    """Ensures DISTINCT with JOIN pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT O.region FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "WHERE P.price > 50"
    )
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    joins = ast.args.get("joins") or []
    assert len(joins) == 1

    where_clause = ast.args.get("where")
    assert where_clause is not None

    projection = select_column_names(ast)
    assert "region" in projection


def test_distinct_with_aggregation(single_source_env):
    """Validates DISTINCT used with aggregation in same query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(DISTINCT status) AS unique_statuses "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    projection = select_column_names(ast)
    assert "region" in projection
    assert "unique_statuses" in projection

    group_clause = ast.args.get("group")
    assert group_clause is not None
    group_expressions = group_clause.expressions or []
    assert len(group_expressions) == 1

    group_col = unwrap_parens(group_expressions[0])
    assert isinstance(group_col, exp.Column)
    assert group_col.name.lower() == "region"

    expressions = ast.expressions
    assert len(expressions) == 2

    count_expr = expressions[1]
    if isinstance(count_expr, exp.Alias):
        count_func = unwrap_parens(count_expr.this)
        assert isinstance(count_func, exp.Count)
        assert count_func.args.get("distinct") is True


def test_distinct_with_order_by_not_in_select(single_source_env):
    """Checks DISTINCT with ORDER BY on column not in SELECT."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT region FROM duckdb_primary.main.orders "
        "ORDER BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    order_clause = ast.args.get("order")
    assert order_clause is not None
    order_expressions = order_clause.expressions
    assert len(order_expressions) == 1

    first_order = order_expressions[0]
    order_col = unwrap_parens(first_order.this)
    assert isinstance(order_col, exp.Column)
    assert order_col.name.lower() == "region"

    projection = select_column_names(ast)
    assert "region" in projection


def test_distinct_with_limit_offset(single_source_env):
    """Validates DISTINCT with LIMIT and OFFSET pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT region FROM duckdb_primary.main.orders "
        "ORDER BY region LIMIT 5 OFFSET 2"
    )
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 5

    offset_clause = ast.args.get("offset")
    assert isinstance(offset_clause, exp.Offset)
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 2

    order_clause = ast.args.get("order")
    assert order_clause is not None


def test_distinct_star(single_source_env):
    """Ensures DISTINCT * (all columns) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT DISTINCT * FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)

    distinct = ast.args.get("distinct")
    assert distinct is not None

    expressions = ast.expressions
    assert len(expressions) == 1
    star_expr = expressions[0]
    assert isinstance(star_expr, exp.Star)
