"""Combined pushdown scenarios (projection + agg + join + limit)."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import build_runtime


def _explain_document(runtime, sql: str):
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    return document


def _collect_select_sql(select_ast: exp.Select):
    fragments = []
    for expression in select_ast.expressions:
        fragments.append(expression.sql())
    return fragments


def test_projection_predicate_aggregation_join_limit(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region "
        "HAVING SUM(P.base_price) > 50 "
        "LIMIT 5"
    )
    document = _explain_document(runtime, sql)
    queries = document["queries"]
    assert len(queries) == 1
    query_ast = queries[0]["query"]
    fragments = _collect_select_sql(query_ast)
    assert any("SUM(" in fragment for fragment in fragments)
    assert query_ast.args.get("group") is not None
    assert any("PhysicalLimit" in line for line in document["plan"])


def test_join_with_computed_expression_group_by(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price * O.quantity) AS revenue "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region"
    )
    document = _explain_document(runtime, sql)
    query_ast = document["queries"][0]["query"]
    fragments = _collect_select_sql(query_ast)
    assert any("base_price * O.quantity" in fragment for fragment in fragments)
    assert query_ast.args.get("group") is not None


def test_join_with_order_by_after_aggregation(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P ON O.product_id = P.id "
        "GROUP BY O.region "
        "ORDER BY total_cost DESC"
    )
    document = _explain_document(runtime, sql)
    query_ast = document["queries"][0]["query"]
    fragments = _collect_select_sql(query_ast)
    assert any("SUM(" in fragment for fragment in fragments)
    join_nodes = query_ast.args.get("joins") or []
    assert join_nodes, "join should remain remote"


def test_join_distinct_stays_remote(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT O.region, P.category "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id"
    )
    document = _explain_document(runtime, sql)
    assert len(document["queries"]) == 1
    assert any("PhysicalRemoteJoin" in line for line in document["plan"])


def test_join_with_case_expression_and_limit(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(CASE WHEN P.category = 'clothing' THEN P.base_price ELSE 0 END) AS clothing_spend "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region "
        "LIMIT 10"
    )
    document = _explain_document(runtime, sql)
    fragments = _collect_select_sql(document["queries"][0]["query"])
    assert any("CASE" in fragment for fragment in fragments)
