"""Foundation tests for EXPLAIN (FORMAT JSON) query capture."""

from sqlglot import exp

from federated_query.cli.fedq import FedQRuntime
from federated_query.config import ExecutorConfig


def _build_runtime(env):
    runtime = FedQRuntime(env.catalog, ExecutorConfig())
    return runtime


def _run_explain_json(runtime: FedQRuntime, sql: str):
    document = runtime.execute(sql)
    assert isinstance(document, dict)
    return document


def _single_query_entry(document):
    queries = document.get("queries", [])
    assert len(queries) == 1
    entry = queries[0]
    return entry["datasource_name"], entry["query"]


def _unwrap_parens(expression):
    current = expression
    while isinstance(current, exp.Paren):
        current = current.this
    return current


def test_explain_json_tracks_simple_predicate(single_source_env):
    runtime = _build_runtime(single_source_env)
    sql = (
        "EXPLAIN (FORMAT JSON) "
        "SELECT * FROM duckdb_primary.main.orders WHERE status = 'processing'"
    )
    document = _run_explain_json(runtime, sql)
    datasource, query_ast = _single_query_entry(document)
    assert datasource == "duckdb_primary"
    where_clause = query_ast.args.get("where")
    assert where_clause is not None
    predicate = _unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.EQ)
    column = predicate.left
    assert isinstance(column, exp.Column)
    assert column.name.lower() == "status"
    literal = predicate.right
    assert isinstance(literal, exp.Literal)
    assert literal.this == "processing"


def test_explain_json_tracks_compound_predicate(single_source_env):
    runtime = _build_runtime(single_source_env)
    sql = (
        "EXPLAIN (FORMAT JSON) "
        "SELECT * FROM duckdb_primary.main.orders "
        "WHERE region = 'EU' AND quantity > 3"
    )
    document = _run_explain_json(runtime, sql)
    datasource, query_ast = _single_query_entry(document)
    assert datasource == "duckdb_primary"
    where_clause = query_ast.args.get("where")
    assert where_clause is not None
    predicate = _unwrap_parens(where_clause.this)
    assert isinstance(predicate, exp.And)
    left = predicate.left
    assert isinstance(_unwrap_parens(left), exp.EQ)
    right = predicate.right
    assert isinstance(_unwrap_parens(right), exp.GT)
