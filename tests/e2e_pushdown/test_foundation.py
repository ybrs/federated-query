"""Foundation tests for EXPLAIN remote-query capture."""

from sqlglot import exp

from tests.e2e_pushdown.helpers import build_runtime, explain_document


def _build_runtime(env):
    """Instantiate a Rust-engine runtime for the provided environment."""
    return build_runtime(env)


def _run_explain_json(runtime, sql: str):
    """Return the remote-query document for the (EXPLAIN-prefixed) statement."""
    inner = sql
    prefix = "EXPLAIN (FORMAT JSON) "
    if inner.startswith(prefix):
        inner = inner[len(prefix) :]
    return explain_document(runtime, inner)


def _single_query_entry(document):
    """Extract the single datasource/query pair from an explain document."""
    queries = document.get("queries", [])
    assert len(queries) == 1
    entry = queries[0]
    return entry["datasource_name"], entry["query"]


def _unwrap_parens(expression):
    """Strip sqlglot Paren wrappers so predicate inspection is easier."""
    current = expression
    while isinstance(current, exp.Paren):
        current = current.this
    return current


def test_explain_json_tracks_simple_predicate(single_source_env):
    """Ensures EXPLAIN JSON captures datasource plus a single equality predicate."""
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
    """Confirms AND predicates appear exactly once with both child comparisons."""
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
