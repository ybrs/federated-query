"""LIMIT and OFFSET edge cases pushdown tests."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_datasource_query,
)


def test_limit_zero(single_source_env):
    """Verifies LIMIT 0 pushes to datasource."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders LIMIT 0"
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 0


def test_very_large_limit(single_source_env):
    """Validates very large LIMIT value (1000000) pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders LIMIT 1000000"
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 1000000


def test_very_large_offset(single_source_env):
    """Checks very large OFFSET value pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders LIMIT 10 OFFSET 999999"
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 10

    offset_clause = ast.args.get("offset")
    assert isinstance(offset_clause, exp.Offset)
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 999999


def test_offset_without_limit(single_source_env):
    """Ensures OFFSET without LIMIT pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders OFFSET 10"
    ast = explain_datasource_query(runtime, sql)

    offset_clause = ast.args.get("offset")
    assert isinstance(offset_clause, exp.Offset)
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 10

    limit_clause = ast.args.get("limit")
    assert limit_clause is None


def test_offset_larger_than_table(single_source_env):
    """Validates OFFSET larger than table size pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders LIMIT 10 OFFSET 10000000"
    ast = explain_datasource_query(runtime, sql)

    offset_clause = ast.args.get("offset")
    assert isinstance(offset_clause, exp.Offset)
    offset_value = int(offset_clause.expression.this)
    assert offset_value == 10000000

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 10


def test_limit_one_optimization(single_source_env):
    """Checks LIMIT 1 optimization opportunity pushes correctly."""
    runtime = build_runtime(single_source_env)
    sql = "SELECT order_id FROM duckdb_primary.main.orders WHERE price > 100 LIMIT 1"
    ast = explain_datasource_query(runtime, sql)

    limit_clause = ast.args.get("limit")
    assert isinstance(limit_clause, exp.Limit)
    limit_value = int(limit_clause.expression.this)
    assert limit_value == 1

    where_clause = ast.args.get("where")
    assert where_clause is not None
