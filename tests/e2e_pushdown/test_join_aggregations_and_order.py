"""Join pushdown combinations: aggregations, HAVING, order/limit."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
)


def _collect_select_sql(ast: exp.Select):
    fragments = []
    for expression in ast.expressions:
        fragments.append(expression.sql())
    return fragments


def test_join_aggregation_group_by_pushdown(single_source_env):
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
    fragments = _collect_select_sql(ast)
    assert any("SUM(" in fragment for fragment in fragments)


def test_join_aggregation_with_having(single_source_env):
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
    fragments = _collect_select_sql(ast)
    assert any("SUM(" in fragment for fragment in fragments)


def test_join_with_order_and_limit_still_remote(single_source_env):
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
    names = [expr.sql() for expr in group_clause.expressions or []]
    assert any("region" in name for name in names)
    assert any("category" in name for name in names)


def test_join_having_with_alias_adds_filter(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, SUM(P.base_price) AS total_cost "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "GROUP BY O.region "
        "HAVING total_cost > 25"
    )
    document = _explain_document(runtime, sql)
    assert any("PhysicalFilter" in line for line in document["plan"])


def test_join_order_by_multiple_columns(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, P.category "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "ORDER BY O.region DESC, P.category ASC LIMIT 3"
    )
    document = _explain_document(runtime, sql)
    assert any("PhysicalLimit" in line for line in document["plan"])


def _explain_document(runtime, sql: str):
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    return document
