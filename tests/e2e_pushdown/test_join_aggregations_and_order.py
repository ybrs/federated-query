"""Join pushdown combinations: aggregations, HAVING, order/limit."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    explain_document,
    find_in_select,
    group_column_names,
    select_column_names,
)


def test_join_aggregation_group_by_pushdown(single_source_env):
    """Ensures grouped join pushdown keeps SUM(base_price) grouped by region once."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = group_column_names(ast)
    assert "region" in names
    projection = select_column_names(ast)
    assert projection == ["region", "total_cost"]
    aggregates = find_in_select(ast, lambda node: isinstance(node, exp.Sum))
    assert len(aggregates) == 1


def test_join_aggregation_with_having(single_source_env):
    """Validates SUM(base_price) with HAVING clause stays remote with one aggregate."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region "
        "HAVING SUM(P.base_price) > 50"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    projection = select_column_names(ast)
    assert projection == ["region", "total_cost"]
    aggregates = find_in_select(ast, lambda node: isinstance(node, exp.Sum))
    assert len(aggregates) == 1


def test_join_with_order_and_limit_still_remote(single_source_env):
    """Checks order+limit queries keep the join remote and respect join kind."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "ORDER BY O.order_id LIMIT 5"
    )
    ast = explain_datasource_query(runtime, sql)
    joins = ast.args.get("joins") or []
    assert joins, "expected remote join SQL"
    join = joins[0]
    kind = join.args.get("kind") or join.args.get("side")
    if kind is not None:
        normalized = str(getattr(kind, "value", kind)).upper()
        assert normalized in ("INNER", "LEFT", "RIGHT")


def test_join_aggregation_multiple_group_columns(single_source_env):
    """Confirms grouped join lists both region and category exactly once."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, P.category, SUM(P.base_price) "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region, P.category"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = group_column_names(ast)
    assert "region" in names
    assert "category" in names
    projection = select_column_names(ast)
    assert projection[0] == "region"
    assert projection[1] == "category"


def test_join_having_with_alias_adds_filter(single_source_env):
    """Ensures alias-based HAVING is applied post-aggregation without remote WHERE."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region "
        "HAVING total_cost > 25"
    )
    document = explain_document(runtime, sql)
    query = document["queries"][0]["query"]
    assert query.args.get("where") is None


def test_join_order_by_multiple_columns(single_source_env):
    """Validates ORDER BY with multiple columns still plans a remote join."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, P.category "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "ORDER BY O.region DESC, P.category ASC LIMIT 3"
    )
    document = explain_document(runtime, sql)
    query = document["queries"][0]["query"]
    joins = query.args.get("joins") or []
    assert joins, "expected remote join for order/limit query"

