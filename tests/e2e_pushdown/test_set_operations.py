"""Set operations (UNION, INTERSECT, EXCEPT) pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    select_column_names,
    unwrap_parens,
)


def test_union_all_basic(single_source_env):
    """Verifies basic UNION ALL pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "UNION ALL "
        "SELECT order_id, price FROM duckdb_primary.main.orders WHERE region = 'US'"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Union)
    assert ast.args.get("distinct") is False or ast.args.get("distinct") is None

    left_query = ast.this
    assert isinstance(left_query, exp.Select)
    left_where = left_query.args.get("where")
    assert left_where is not None

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)
    right_where = right_query.args.get("where")
    assert right_where is not None


def test_union_distinct_basic(single_source_env):
    """Validates UNION (with implicit DISTINCT) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region FROM duckdb_primary.main.orders WHERE price > 100 "
        "UNION "
        "SELECT region FROM duckdb_primary.main.orders WHERE quantity > 5"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Union)
    assert ast.args.get("distinct") is True

    left_query = ast.this
    assert isinstance(left_query, exp.Select)
    left_projection = select_column_names(left_query)
    assert "region" in left_projection

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)
    right_projection = select_column_names(right_query)
    assert "region" in right_projection


def test_intersect_basic(single_source_env):
    """Checks INTERSECT operation pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "INTERSECT "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE price > 50"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Intersect)

    left_query = ast.this
    assert isinstance(left_query, exp.Select)

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)


def test_except_basic(single_source_env):
    """Validates EXCEPT operation pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "EXCEPT "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE status = 'cancelled'"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Except)

    left_query = ast.this
    assert isinstance(left_query, exp.Select)
    left_projection = select_column_names(left_query)
    assert "order_id" in left_projection

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)
    right_projection = select_column_names(right_query)
    assert "order_id" in right_projection


def test_union_three_queries(single_source_env):
    """Ensures UNION of 3+ queries pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "UNION ALL "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'US' "
        "UNION ALL "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'ASIA'"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Union)

    left_part = ast.this
    assert isinstance(left_part, exp.Union)

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)
    right_projection = select_column_names(right_query)
    assert "order_id" in right_projection


def test_union_with_order_by(single_source_env):
    """Validates UNION with ORDER BY pushes together."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, price FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "UNION ALL "
        "SELECT order_id, price FROM duckdb_primary.main.orders WHERE region = 'US' "
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

    from_clause = ast.args.get("from")
    assert from_clause is not None
    subquery = from_clause.this
    assert isinstance(subquery, exp.Subquery)
    union_ast = subquery.this
    assert isinstance(union_ast, exp.Union)


def test_union_with_limit(single_source_env):
    """Checks UNION with LIMIT pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "UNION ALL "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'US' "
        "LIMIT 20"
    )
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 20

    from_clause = ast.args.get("from")
    assert from_clause is not None
    subquery = from_clause.this
    assert isinstance(subquery, exp.Subquery)
    union_ast = subquery.this
    assert isinstance(union_ast, exp.Union)


def test_nested_set_operations(single_source_env):
    """Validates nested set operations push correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "(SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'EU' "
        "UNION "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE region = 'US') "
        "INTERSECT "
        "SELECT order_id FROM duckdb_primary.main.orders WHERE price > 100"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Intersect)

    left_part = ast.this
    assert isinstance(left_part, exp.Union)

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)


def test_union_with_aggregation(single_source_env):
    """Ensures UNION with aggregated queries pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) AS cnt FROM duckdb_primary.main.orders "
        "WHERE price > 100 GROUP BY region "
        "UNION ALL "
        "SELECT region, COUNT(*) AS cnt FROM duckdb_primary.main.orders "
        "WHERE quantity > 5 GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)

    assert isinstance(ast, exp.Union)

    left_query = ast.this
    assert isinstance(left_query, exp.Select)
    left_group = left_query.args.get("group")
    assert left_group is not None
    left_group_expressions = left_group.expressions or []
    assert len(left_group_expressions) == 1

    right_query = ast.expression
    assert isinstance(right_query, exp.Select)
    right_group = right_query.args.get("group")
    assert right_group is not None
    right_group_expressions = right_group.expressions or []
    assert len(right_group_expressions) == 1


def test_cross_datasource_union_fallback(multi_source_env):
    """Validates cross-datasource UNION does NOT push to single source."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT id FROM duckdb_primary.main.products "
        "UNION ALL "
        "SELECT user_id FROM postgres_secondary.public.users"
    )

    doc = runtime.explain(sql)
    datasources = doc.get("datasources") or {}

    duckdb_queries = datasources.get("duckdb_primary", [])
    postgres_queries = datasources.get("postgres_secondary", [])

    assert len(duckdb_queries) >= 1
    assert len(postgres_queries) >= 1
