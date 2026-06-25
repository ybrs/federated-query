"""Section 7: edge cases and negative pushdown scenarios."""

import pytest

from sqlglot import exp

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
    """A single-source scalar subquery in the projection pushes as one query.

    The decorrelated scalar is re-correlated and inlined into the projection, so
    the whole statement ships to the source as a single remote query carrying
    the scalar subquery, rather than a separate remote query joined locally.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT (SELECT MAX(quantity) FROM duckdb_primary.main.orders) AS max_qty "
        "FROM duckdb_primary.main.orders"
    )
    document = explain_document(runtime, sql)
    queries = document.get("queries", [])
    assert len(queries) == 1
    assert queries[0]["query"].find(exp.Subquery) is not None


def test_join_condition_with_or_pushes_down(single_source_env):
    """A same-source join with an OR condition pushes as one remote query."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT O.order_id "
        "FROM duckdb_primary.main.orders O "
        "JOIN duckdb_primary.main.products P "
        "ON O.product_id = P.id OR O.region = P.category"
    )
    document = explain_document(runtime, sql)
    queries = document.get("queries", [])
    assert len(queries) == 1, "single-source OR join should push down"
    joins = queries[0]["query"].args.get("joins") or []
    assert len(joins) == 1
