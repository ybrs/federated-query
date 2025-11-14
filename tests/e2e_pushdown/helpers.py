"""Shared utilities for pushdown end-to-end tests."""

from sqlglot import exp

from federated_query.cli.fedq import FedQRuntime
from federated_query.config import ExecutorConfig


def build_runtime(env) -> FedQRuntime:
    """Create a FedQ runtime for the provided environment."""
    runtime = FedQRuntime(env.catalog, ExecutorConfig())
    return runtime


def explain_datasource_query(runtime: FedQRuntime, sql: str) -> exp.Expression:
    """Return the remote query AST produced by EXPLAIN (FORMAT JSON)."""
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    queries = document.get("queries", [])
    assert len(queries) == 1
    entry = queries[0]
    return entry["query"]


def unwrap_parens(expression):
    """Remove redundant Paren wrappers from sqlglot expressions."""
    current = expression
    while isinstance(current, exp.Paren):
        current = current.this
    return current
