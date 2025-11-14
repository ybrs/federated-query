"""Single-table aggregation pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    unwrap_parens,
)


def _collect_expression_sql(select_ast: exp.Select):
    sql_fragments = []
    for expression in select_ast.expressions:
        sql_fragments.append(expression.sql())
    return sql_fragments


def test_count_star_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT COUNT(*) FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    expressions = _collect_expression_sql(ast)
    assert "COUNT(*)" in expressions
    group_clause = ast.args.get("group")
    assert group_clause is None


def test_multiple_aggregates_with_aliases(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT SUM(quantity) AS total_qty, AVG(price) AS avg_price "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)
    expressions = _collect_expression_sql(ast)
    expected = ["SUM(quantity)", "AVG(price)"]
    index = 0
    while index < len(expected):
        assert expected[index] in expressions
        index += 1


def test_group_by_single_column(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, COUNT(*) AS cnt "
        "FROM duckdb_primary.main.orders GROUP BY region"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    group_expressions = group_clause.expressions or []
    names = []
    for expression in group_expressions:
        names.append(expression.sql())
    assert "region" in names


def test_group_by_multiple_columns(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, status, SUM(quantity) "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region, status"
    )
    ast = explain_datasource_query(runtime, sql)
    group_clause = ast.args.get("group")
    assert group_clause is not None
    names = []
    for expression in group_clause.expressions or []:
        names.append(expression.sql())
    assert "region" in names
    assert "status" in names


def test_having_clause_translated_to_remote_filter(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT region, SUM(quantity) AS total_qty "
        "FROM duckdb_primary.main.orders "
        "GROUP BY region HAVING SUM(quantity) > 10"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    assert predicate.left.name.lower() == "total_qty"


def test_min_max_with_limit(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT MIN(price) AS min_price, MAX(price) AS max_price "
        "FROM duckdb_primary.main.orders LIMIT 2 OFFSET 1"
    )
    ast = explain_datasource_query(runtime, sql)
    expressions = _collect_expression_sql(ast)
    assert "MIN(price)" in expressions
    assert "MAX(price)" in expressions
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    offset_clause = ast.args.get("offset")
    assert offset_clause is not None
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 1
