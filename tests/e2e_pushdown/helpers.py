"""Shared utilities for pushdown end-to-end tests."""

from typing import Callable, Dict, List, Optional

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


def explain_document(runtime: FedQRuntime, sql: str) -> Dict[str, object]:
    """Execute EXPLAIN (FORMAT JSON) and return the document."""
    statement = f"EXPLAIN (FORMAT JSON) {sql}"
    document = runtime.execute(statement)
    assert isinstance(document, dict)
    return document


def unwrap_parens(expression):
    """Remove redundant Paren wrappers from sqlglot expressions."""
    current = expression
    while isinstance(current, exp.Paren):
        current = current.this
    return current


def find_in_select(
    select_ast: exp.Select, predicate: Callable[[exp.Expression], bool]
) -> List[exp.Expression]:
    """Return all projection expressions that match the predicate."""
    matches: List[exp.Expression] = []
    for expression in select_ast.expressions:
        node = expression
        if isinstance(expression, exp.Alias):
            node = expression.this
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
    from_clause = select_ast.args.get("from")
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
            return expression.this
    return None


def datasource_query_map(document: Dict[str, object]) -> Dict[str, exp.Select]:
    """Map datasource names to their captured SELECT ASTs."""
    mapping: Dict[str, exp.Select] = {}
    queries = document.get("queries", [])
    for entry in queries:
        datasource = entry["datasource_name"]
        mapping[str(datasource)] = entry["query"]
    return mapping
