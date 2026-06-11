"""Section 7: edge cases and negative pushdown scenarios."""

import pytest

from tests.e2e_pushdown.helpers import (
    build_runtime,
    explain_document,
)


def _expect_explain_failure(runtime, sql: str, message: str) -> None:
    """Execute an EXPLAIN statement and assert it raises ValueError."""
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    with pytest.raises(ValueError, match=message):
        runtime.execute(statement)


def test_window_function_not_supported(single_source_env):
    """Ensures window functions surface a parser error instead of silent pushdown."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY order_id) AS rn "
        "FROM duckdb_primary.main.orders"
    )
    _expect_explain_failure(runtime, sql, "Window")


def test_scalar_subquery_in_projection_supported(single_source_env):
    """A scalar subquery in the projection decorrelates into its own scan.

    Scalar subqueries are supported now (they did not used to be): the
    uncorrelated aggregate becomes a separate remote query joined onto the
    outer scan, rather than raising a "not supported" error.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT (SELECT MAX(quantity) FROM duckdb_primary.main.orders) AS max_qty "
        "FROM duckdb_primary.main.orders"
    )
    document = explain_document(runtime, sql)
    queries = document.get("queries", [])
    assert len(queries) == 2


def test_join_condition_with_or_falls_back(single_source_env):
    """Confirms non-equi OR join predicates fall back to independent scans."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id OR O.region = P.category"
    )
    document = explain_document(runtime, sql)
    queries = document.get("queries", [])
    assert len(queries) == 2, "unsupported OR join should not stay remote"
    for entry in queries:
        select_ast = entry["query"]
        joins = select_ast.args.get("joins") or []
        assert not joins
