"""Combined pushdown scenarios (projection + agg + join + limit)."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_document,
    find_alias_expression,
    find_in_select,
    group_column_names,
    join_table_names,
    select_column_names,
    unwrap_parens,
)


def test_projection_predicate_aggregation_join_limit(single_source_env):
    """Checks sum/group/filter/limit stay remote with the correct join target."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region "
        "HAVING SUM(P.base_price) > 50 "
        "LIMIT 5"
    )
    document = explain_document(runtime, sql)
    queries = document["queries"]
    assert len(queries) == 1
    query_ast = queries[0]["query"]
    projection = select_column_names(query_ast)
    assert projection == ["region", "total_cost"]
    aggregates = find_in_select(query_ast, lambda node: isinstance(node, exp.Sum))
    assert len(aggregates) == 1
    child = aggregates[0].this
    assert isinstance(child, exp.Column)
    assert child.table == "P"
    assert query_ast.args.get("group") is not None
    names = group_column_names(query_ast)
    assert "region" in names
    join_tables = join_table_names(query_ast)
    assert "products" in join_tables


def test_join_with_computed_expression_group_by(single_source_env):
    """Validates computed SUM uses the expected multiplication expression once."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price * O.quantity) AS revenue "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region"
    )
    document = explain_document(runtime, sql)
    query_ast = document["queries"][0]["query"]
    projection = select_column_names(query_ast)
    assert projection == ["region", "revenue"]
    aggregates = find_in_select(query_ast, lambda node: isinstance(node, exp.Sum))
    assert len(aggregates) == 1
    expression = unwrap_parens(aggregates[0].this)
    assert isinstance(expression, exp.Mul)
    assert isinstance(expression.left, exp.Column)
    assert isinstance(expression.right, exp.Column)
    assert query_ast.args.get("group") is not None


def test_join_with_order_by_after_aggregation(single_source_env):
    """Ensures SUM + ORDER BY keep the join remote with a single aggregate."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region "
        "ORDER BY total_cost DESC"
    )
    document = explain_document(runtime, sql)
    query_ast = document["queries"][0]["query"]
    projection = select_column_names(query_ast)
    assert projection == ["region", "total_cost"]
    aggregates = find_in_select(query_ast, lambda node: isinstance(node, exp.Sum))
    assert len(aggregates) == 1
    join_nodes = query_ast.args.get("joins") or []
    assert join_nodes, "join should remain remote"


def test_join_distinct_stays_remote(single_source_env):
    """Checks DISTINCT joins still plan a single remote join query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT O.region, P.category "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id"
    )
    document = explain_document(runtime, sql)
    assert len(document["queries"]) == 1
    query_ast = document["queries"][0]["query"]
    join_tables = join_table_names(query_ast)
    assert "products" in join_tables
    assert set(select_column_names(query_ast)) == {
        "region",
        "product_id",
        "category",
        "id",
    }


def test_join_with_case_expression_and_limit(single_source_env):
    """Ensures CASE + SUM alias stays remote and limit remains pushed."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(CASE WHEN P.category = 'clothing' THEN P.base_price ELSE 0 END) AS clothing_spend "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region "
        "LIMIT 10"
    )
    document = explain_document(runtime, sql)
    query_ast = document["queries"][0]["query"]
    projection = select_column_names(query_ast)
    assert projection == ["region", "clothing_spend"]
    target = find_alias_expression(query_ast, "clothing_spend")
    assert target is not None
    assert isinstance(target, exp.Sum)
    assert isinstance(target.this, exp.Case)
