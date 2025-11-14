"""Phase 1 pushdown tests: single-table selects and predicates."""

from typing import List, Tuple

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
    unwrap_parens,
)


def _collect_projection_names(select_ast: exp.Select) -> List[str]:
    names: List[str] = []
    for expression in select_ast.expressions:
        if hasattr(expression, "alias_or_name") and expression.alias_or_name:
            names.append(expression.alias_or_name)
        else:
            names.append(expression.sql())
    return names


def test_select_all_no_filter(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    assert isinstance(ast, exp.Select)
    where_clause = ast.args.get("where")
    assert where_clause is None
    projection = _collect_projection_names(ast)
    assert len(projection) == 1
    assert projection[0] == "*"


def test_projection_subset_columns(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id, region FROM duckdb_primary.main.orders"
    ast = explain_datasource_query(runtime, sql)
    projection = _collect_projection_names(ast)
    assert projection == ["order_id", "region"]


def test_numeric_predicate_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE quantity > 4"
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.GT)
    assert predicate.left.name.lower() == "quantity"


def test_string_predicate_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT * FROM duckdb_primary.main.orders WHERE status = 'processing'"
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    assert predicate.left.name.lower() == "status"
    literal = predicate.right
    assert isinstance(literal, exp.Literal)
    assert literal.this == "processing"


def test_boolean_predicate_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = "SELECT id FROM duckdb_primary.main.products WHERE active"
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Column)
    assert predicate.name.lower() == "active"


def test_compound_and_predicate(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' AND status = 'processing'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
    left = unwrap_parens(predicate.left)
    right = unwrap_parens(predicate.right)
    assert isinstance(left, exp.EQ)
    assert isinstance(right, exp.EQ)


def test_or_predicate(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' OR region = 'APAC'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Or)


def test_in_list_predicate(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE status IN ('processing', 'shipped')"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.In)
    assert predicate.this.name.lower() == "status"


def test_between_predicate(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE quantity BETWEEN 2 AND 6"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Between)
    assert predicate.this.name.lower() == "quantity"


def test_like_predicate(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT name FROM duckdb_primary.main.products "
        "WHERE name LIKE 'c%'"
    )
    ast = explain_datasource_query(runtime, sql)
    where_clause = ast.args.get("where")
    assert where_clause is not None
    predicate = unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.Like)


def test_limit_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' LIMIT 3"
    )
    ast = explain_datasource_query(runtime, sql)
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 3


def test_limit_offset_pushdown(single_source_env):
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id FROM duckdb_primary.main.orders "
        "LIMIT 2 OFFSET 1"
    )
    ast = explain_datasource_query(runtime, sql)
    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    offset_clause = ast.args.get("offset")
    assert offset_clause is not None
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 1
