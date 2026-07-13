"""Shared utilities for pushdown end-to-end tests."""

from typing import Callable, Dict, List, Optional

import sqlglot
from sqlglot import exp

from tests.rust_runtime import RustRuntime


def build_runtime(env) -> RustRuntime:
    """Create a Rust-engine runtime for the provided environment."""
    runtime = RustRuntime(env.source_pairs())
    return runtime


def explain_datasource_query(runtime: RustRuntime, sql: str) -> exp.Expression:
    """Return the single remote-query AST from the engine's EXPLAIN plan.

    Asserts exactly one datasource query is emitted (the pushdown contract) and
    parses its rendered SQL as a DuckDB-dialect AST.
    """
    queries = runtime.explain_queries(sql)
    assert len(queries) == 1
    _datasource, sql_text = queries[0]
    return sqlglot.parse_one(sql_text, dialect="duckdb")


def explain_document(runtime: RustRuntime, sql: str) -> Dict[str, object]:
    """Return a document mapping ``queries`` to the engine's remote queries.

    Each entry mirrors the old JSON shape: ``datasource_name`` plus the parsed
    ``query`` AST, so callers that iterated the document keep working.
    """
    queries = runtime.explain_queries(sql)
    entries: List[Dict[str, object]] = []
    for datasource, sql_text in queries:
        entry: Dict[str, object] = {}
        entry["datasource_name"] = datasource
        entry["query"] = sqlglot.parse_one(sql_text, dialect="duckdb")
        entries.append(entry)
    document: Dict[str, object] = {}
    document["queries"] = entries
    return document


def unwrap_parens(expression):
    """Remove redundant Paren wrappers from sqlglot expressions."""
    current = expression
    while isinstance(current, exp.Paren):
        current = current.this
    return current


def is_func(node, name: str) -> bool:
    """Whether a node is a function call of ``name`` (case-insensitive).

    The single SQL emitter lowers function calls to ``exp.Anonymous`` (verbatim
    names) - the engine's canonical Postgres form, shared by every pushed query;
    the dialect transpile boundary (to_source_sql) normalizes them to typed nodes
    on execution. Tests therefore match by NAME, not by a specific sqlglot class.
    """
    target = name.upper()
    if isinstance(node, exp.Anonymous):
        return str(node.this).upper() == target
    if isinstance(node, exp.Func):
        return node.sql_name().upper() == target
    return False


def find_in_select(
    select_ast: exp.Select, predicate: Callable[[exp.Expression], bool]
) -> List[exp.Expression]:
    """Return all projection expressions that match the predicate."""
    matches: List[exp.Expression] = []
    for expression in select_ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
        node = unwrap_parens(node)
        if predicate(node):
            matches.append(node)
    return matches


def select_column_names(select_ast: exp.Select) -> List[str]:
    """Collect the projection names from a SELECT expression."""
    names: List[str] = []
    for expression in select_ast.expressions:
        alias = getattr(expression, "alias", None)
        if alias:
            names.append(str(alias))
            continue
        if isinstance(expression, exp.Column):
            names.append(expression.name)
            continue
        if isinstance(expression, exp.Star):
            names.append("*")
            continue
        names.append(expression.sql())
    return names


def from_table_name(select_ast: exp.Select) -> str:
    """Return the base table referenced by the FROM clause."""
    from_clause = select_ast.args.get("from_")
    assert from_clause is not None
    table = from_clause.this
    assert isinstance(table, exp.Table)
    return table.name


def join_table_names(select_ast: exp.Select) -> List[str]:
    """Return the table names referenced by JOIN clauses."""
    names: List[str] = []
    joins = select_ast.args.get("joins") or []
    for join in joins:
        table = join.this
        if isinstance(table, exp.Table):
            names.append(table.name)
    return names


def group_column_names(select_ast: exp.Select) -> List[str]:
    """Collect column names referenced by GROUP BY."""
    names: List[str] = []
    group_clause = select_ast.args.get("group")
    if group_clause is None:
        return names
    expressions = group_clause.expressions or []
    for expression in expressions:
        alias = getattr(expression, "alias_or_name", None)
        if alias:
            names.append(alias)
            continue
        if isinstance(expression, exp.Column):
            names.append(expression.name)
    return names


def find_alias_expression(
    select_ast: exp.Select, alias: str
) -> Optional[exp.Expression]:
    """Return the expression referenced by the provided alias."""
    for expression in select_ast.expressions:
        alias_name = getattr(expression, "alias", None)
        if alias_name == alias and isinstance(expression, exp.Alias):
            return unwrap_parens(expression.this)
    return None


def datasource_query_map(document: Dict[str, object]) -> Dict[str, exp.Select]:
    """Map datasource names to their captured SELECT ASTs."""
    mapping: Dict[str, exp.Select] = {}
    queries = document.get("queries", [])
    for entry in queries:
        datasource = entry["datasource_name"]
        mapping[str(datasource)] = entry["query"]
    return mapping
