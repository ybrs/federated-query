"""Two-table join pushdown tests for single datasource queries."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    from_table_name,
    join_table_names,
)


def _explain_document(runtime, sql: str):
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    return document


def _get_join_node(select_ast: exp.Select) -> exp.Join:
    joins = select_ast.args.get("joins") or []
    assert joins, "expected remote join SQL"
    return joins[0]


def _normalize_join_kind(join_expr: exp.Join) -> str:
    kind = join_expr.args.get("kind")
    if kind is not None:
        if hasattr(kind, "value"):
            return str(kind.value).upper()
        return str(kind).upper()
    side = join_expr.args.get("side")
    if side is not None:
        return str(side).upper()
    return "INNER"


def _assert_join_type(select_ast: exp.Select, expected: str) -> None:
    join = _get_join_node(select_ast)
    actual = _normalize_join_kind(join)
    assert actual == expected


def test_inner_join_pushdown_select_star(single_source_env):
    """Verifies basic INNER JOIN stays remote between orders and products."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT * FROM duckdb_primary.main.orders O "
        "INNER JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "INNER")


def test_inner_join_with_filters(single_source_env):
    """Ensures joins stay remote even when additional filters are present."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.region, P.category "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "WHERE O.region = 'EU' AND P.category = 'clothing'"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "INNER")


def test_inner_join_with_limit(single_source_env):
    """Ensures joins remain remote for LIMIT queries even if limit executes later."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id "
        "LIMIT 5"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "INNER")


def test_left_join_pushdown(single_source_env):
    """Checks LEFT JOIN preference remains remote."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "LEFT JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "LEFT")


def test_right_join_pushdown(single_source_env):
    """Checks RIGHT JOIN preference remains remote."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT P.name, O.order_id "
        "FROM duckdb_primary.main.products P "
        "RIGHT JOIN duckdb_primary.main.orders O "
        "ON O.product_id = P.id"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "RIGHT")


def test_inner_join_comma_syntax(single_source_env):
    """Validates comma joins split into two remote scans without joins."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O, duckdb_primary.main.products P "
        "WHERE O.product_id = P.id"
    )
    document = _explain_document(runtime, sql)
    queries = document["queries"]
    assert len(queries) == 2
    seen_tables = []
    for entry in queries:
        select_ast = entry["query"]
        joins = select_ast.args.get("joins") or []
        assert not joins
        seen_tables.append(from_table_name(select_ast))
    assert len(seen_tables) == 2
    assert set(seen_tables) == {"orders", "products"}


def test_inner_join_non_equi_falls_back(single_source_env):
    """Ensures non-equi joins break into separate scans with no join nodes."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id > P.id"
    )
    document = _explain_document(runtime, sql)
    queries = document["queries"]
    assert len(queries) == 2
    seen_tables = []
    for entry in queries:
        select_ast = entry["query"]
        joins = select_ast.args.get("joins") or []
        assert not joins
        seen_tables.append(from_table_name(select_ast))
    assert len(seen_tables) == 2
    assert set(seen_tables) == {"orders", "products"}


def test_cross_join_generates_multiple_queries(single_source_env):
    """Ensures CROSS JOIN falls back to independent table scans."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id, P.name "
        "FROM duckdb_primary.main.orders O "
        "CROSS JOIN duckdb_primary.main.products P"
    )
    document = _explain_document(runtime, sql)
    queries = document["queries"]
    assert len(queries) == 2
    seen_tables = []
    for entry in queries:
        select_ast = entry["query"]
        joins = select_ast.args.get("joins") or []
        assert not joins
        seen_tables.append(from_table_name(select_ast))
    assert len(seen_tables) == 2
    assert set(seen_tables) == {"orders", "products"}


def test_left_join_filter_on_right_side(single_source_env):
    """Validates IS NULL filters on right table stay local (no remote WHERE)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "LEFT JOIN duckdb_primary.main.products products "
        "ON O.product_id = products.id "
        "WHERE products.name IS NULL"
    )
    document = _explain_document(runtime, sql)
    first_query = document["queries"][0]["query"]
    joins = first_query.args.get("joins") or []
    assert joins, "expected remote join"
    join_tables = join_table_names(first_query)
    assert "products" in join_tables
    where_clause = first_query.args.get("where")
    assert where_clause is None


def test_right_join_with_limit(single_source_env):
    """Ensures RIGHT JOIN pushdown remains remote even with LIMIT."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT P.name, O.order_id "
        "FROM duckdb_primary.main.products P "
        "RIGHT JOIN duckdb_primary.main.orders O "
        "ON O.product_id = P.id "
        "LIMIT 10"
    )
    ast = explain_datasource_query(runtime, sql)
    _assert_join_type(ast, "RIGHT")
