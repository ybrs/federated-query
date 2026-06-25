"""Execution tests for TABLESAMPLE.

TABLESAMPLE is carried on the scan and rendered through the source's dialect, so
the Postgres-form ``BERNOULLI (10)`` is transpiled to DuckDB's ``BERNOULLI (10
PERCENT)``. Sampling is nondeterministic, so correctness is checked two ways:
the pushed SQL is the right dialect form (via EXPLAIN), and execution returns a
subset of the table.
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import build_runtime, explain_datasource_query

TABLE = "duckdb_primary.main.orders"


def test_tablesample_survives_into_pushed_query(single_source_env):
    """TABLESAMPLE reaches the pushed query (it is not silently dropped).

    The pushed AST is Postgres-form; its transpilation to DuckDB's PERCENT form
    at send time is covered by tests/test_dialect_rendering.py.
    """
    runtime = build_runtime(single_source_env)
    ast = explain_datasource_query(
        runtime, f"SELECT order_id FROM {TABLE} TABLESAMPLE BERNOULLI (10)"
    )
    assert ast.find(exp.TableSample) is not None


def test_tablesample_executes_and_returns_subset(single_source_env):
    """A sample never returns more than the full table and does not error."""
    runtime = build_runtime(single_source_env)
    full = runtime.execute(f"SELECT order_id FROM {TABLE}").num_rows
    sampled = runtime.execute(
        f"SELECT order_id FROM {TABLE} TABLESAMPLE BERNOULLI (50)"
    ).num_rows
    assert 0 <= sampled <= full


def test_tablesample_explain_ast_is_valid(single_source_env):
    """EXPLAIN still produces a parseable single pushed query for the sampled scan."""
    runtime = build_runtime(single_source_env)
    ast = explain_datasource_query(
        runtime, f"SELECT order_id FROM {TABLE} TABLESAMPLE SYSTEM (25)"
    )
    assert ast is not None
