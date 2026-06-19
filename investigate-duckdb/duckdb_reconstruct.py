#!/usr/bin/env python
"""Reconstruct readable SQL from DuckDB optimized logical EXPLAIN JSON."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import click
import duckdb
import sqlglot
from prompt_toolkit import PromptSession


DUCKDB_SOURCE_ROOT = Path(__file__).resolve().parent / "duckdb-src"
LOGICAL_HEADER = (
    DUCKDB_SOURCE_ROOT
    / "src/include/duckdb/common/enums/logical_operator_type.hpp"
)
LOGICAL_STRINGS = DUCKDB_SOURCE_ROOT / "src/common/enums/logical_operator_type.cpp"


class PlanReconstructionError(RuntimeError):
    """Raised when an optimized DuckDB plan cannot be reconstructed safely."""


@dataclass(frozen=True)
class PlanNode:
    """Holds one node from DuckDB EXPLAIN JSON."""

    name: str
    children: tuple["PlanNode", ...]
    extra_info: dict[str, Any]


@dataclass(frozen=True)
class RelationSql:
    """Holds reconstructed SQL fragments for a relation subtree."""

    from_sql: str
    filters: tuple[str, ...]
    aliases: tuple[str, ...]
    mapping: tuple[str, ...]


@dataclass(frozen=True)
class DuckDBNodeRegistry:
    """Holds source-derived DuckDB logical node names and handler groups."""

    logical_names: frozenset[str]
    supported_names: frozenset[str]
    unsupported_names: frozenset[str]

    def assert_classified(self) -> None:
        """Raise when a source-derived logical node has no handler decision."""
        classified_names = set()
        classified_names.update(self.supported_names)
        classified_names.update(self.unsupported_names)
        missing_names = self.logical_names.difference(classified_names)
        if missing_names:
            raise PlanReconstructionError(
                "Unclassified DuckDB logical nodes: "
                + ", ".join(sorted(missing_names))
            )

    def validate_node(self, node: PlanNode) -> None:
        """Raise when a plan node is neither supported nor a dynamic scan."""
        if node.name in self.supported_names:
            return
        if node.name in self.unsupported_names:
            raise PlanReconstructionError(f"Unsupported DuckDB node: {node.name}")
        if is_scan_node(node):
            return
        raise PlanReconstructionError(f"Unknown DuckDB plan node: {node.name}")


class SqlSurface:
    """Provides alias and clause text recovered from the original SQL."""

    def __init__(self, sql: str, connection: duckdb.DuckDBPyConnection):
        """Parse SQL and load table/alias metadata for reconstruction."""
        self.sql = sql
        self.expression = sqlglot.parse_one(sql, read="duckdb")
        self.connection = connection
        self.alias_to_table: dict[str, str] = {}
        self.table_to_alias: dict[str, str] = {}
        self.table_columns: dict[str, set[str]] = {}
        self._load_table_aliases()
        self._load_table_columns()

    def projection_sql(self) -> str:
        """Return the original SELECT projection list."""
        expressions = self.expression.args.get("expressions") or []
        projection_parts = []
        for expression in expressions:
            projection_parts.append(expression.sql(dialect="duckdb"))
        return ", ".join(projection_parts)

    def order_sql(self) -> Optional[str]:
        """Return original ORDER BY text when present."""
        order = self.expression.args.get("order")
        if not order:
            return None
        return order.sql(dialect="duckdb")

    def limit_sql(self) -> Optional[str]:
        """Return original LIMIT text when present."""
        limit = self.expression.args.get("limit")
        if not limit:
            return None
        return limit.sql(dialect="duckdb")

    def alias_for_table(self, plan_table: str) -> str:
        """Return a display alias for a DuckDB plan table name."""
        table_name = plan_table.split(".")[-1]
        return self.table_to_alias.get(table_name, table_name)

    def display_table(self, plan_table: str) -> str:
        """Return table SQL with alias when the original query used one."""
        table_name = plan_table.split(".")[-1]
        alias = self.table_to_alias.get(table_name)
        if alias and alias != table_name:
            return f"{table_name} AS {alias}"
        return table_name

    def qualify_column(self, column_name: str, aliases: tuple[str, ...]) -> str:
        """Qualify a column using the first matching table alias."""
        if "." in column_name:
            return column_name
        for alias in aliases:
            table_name = self.alias_to_table.get(alias, alias)
            columns = self.table_columns.get(table_name, set())
            if column_name in columns:
                return f"{alias}.{column_name}"
        return column_name

    def _load_table_aliases(self) -> None:
        """Load table aliases from the original SQL AST."""
        for table in self.expression.find_all(sqlglot.expressions.Table):
            table_name = table.name
            alias = table.alias_or_name
            self.alias_to_table[alias] = table_name
            self.table_to_alias[table_name] = alias

    def _load_table_columns(self) -> None:
        """Load referenced table columns from DuckDB catalog metadata."""
        for table_name in self.table_to_alias:
            rows = self.connection.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            columns = set()
            for row in rows:
                columns.add(str(row[1]))
            self.table_columns[table_name] = columns


def read_logical_node_registry() -> DuckDBNodeRegistry:
    """Read DuckDB source and return the classified logical node registry."""
    enum_names = read_logical_enum_names(LOGICAL_HEADER)
    string_names = read_logical_string_names(LOGICAL_STRINGS)
    logical_names = set()
    for enum_name in enum_names:
        rendered_name = string_names.get(enum_name)
        if rendered_name:
            logical_names.add(rendered_name)
    supported_names = set(query_supported_node_names())
    unsupported_names = set(explicitly_unsupported_node_names())
    registry = DuckDBNodeRegistry(
        frozenset(logical_names),
        frozenset(supported_names),
        frozenset(unsupported_names),
    )
    registry.assert_classified()
    return registry


def read_logical_enum_names(header_path: Path) -> tuple[str, ...]:
    """Read logical enum names from DuckDB's checked-in source header."""
    text = header_path.read_text()
    enum_body = text.split("enum class LogicalOperatorType", 1)[1]
    enum_body = enum_body.split("{", 1)[1].split("};", 1)[0]
    names = []
    for line in enum_body.splitlines():
        cleaned_line = line.split("//", 1)[0].strip().rstrip(",")
        if cleaned_line:
            names.append(cleaned_line.split("=", 1)[0].strip())
    return tuple(names)


def read_logical_string_names(source_path: Path) -> dict[str, str]:
    """Read LogicalOperatorType to rendered-name mappings from DuckDB source."""
    text = source_path.read_text()
    pattern = r"case LogicalOperatorType::([A-Z0-9_]+):\s*return \"([^\"]+)\";"
    matches = re.findall(pattern, text)
    names = {}
    for enum_name, rendered_name in matches:
        names[enum_name] = rendered_name
    return names


def query_supported_node_names() -> tuple[str, ...]:
    """Return logical node names this script can reconstruct."""
    return (
        "GET",
        "PROJECTION",
        "FILTER",
        "AGGREGATE",
        "LIMIT",
        "ORDER_BY",
        "TOP_N",
        "DISTINCT",
        "DELIM_GET",
        "EMPTY_RESULT",
        "DUMMY_SCAN",
        "CTE_SCAN",
        "JOIN",
        "DELIM_JOIN",
        "COMPARISON_JOIN",
        "ANY_JOIN",
        "CROSS_PRODUCT",
        "POSITIONAL_JOIN",
        "ASOF_JOIN",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "REC_CTE",
        "CTE",
    )


def explicitly_unsupported_node_names() -> tuple[str, ...]:
    """Return logical node names that are known but intentionally unsupported."""
    return (
        "WINDOW",
        "UNNEST",
        "COPY_TO_FILE",
        "SAMPLE",
        "PIVOT",
        "COPY_DATABASE",
        "CHUNK_GET",
        "EXPRESSION_GET",
        "DEPENDENT_JOIN",
        "INSERT",
        "DELETE",
        "UPDATE",
        "MERGE_INTO",
        "ALTER",
        "CREATE_TABLE",
        "CREATE_INDEX",
        "CREATE_SEQUENCE",
        "CREATE_VIEW",
        "CREATE_SCHEMA",
        "CREATE_MACRO",
        "DROP",
        "PRAGMA",
        "TRANSACTION",
        "CREATE_TYPE",
        "ATTACH",
        "DETACH",
        "CREATE_TRIGGER",
        "EXPLAIN",
        "PREPARE",
        "EXECUTE",
        "EXPORT",
        "VACUUM",
        "SET",
        "LOAD",
        "RESET",
        "UPDATE_EXTENSIONS",
        "CONNECT",
        "DISCONNECT",
        "CREATE_SECRET",
        "CUSTOM_OP",
    )


def explain_optimized_json(
    connection: duckdb.DuckDBPyConnection, sql: str
) -> PlanNode:
    """Run DuckDB optimized JSON EXPLAIN and return its root plan node."""
    connection.execute("SET explain_output = 'optimized_only'")
    row = connection.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    plan_items = json.loads(row[1])
    return parse_plan_node(plan_items[0])


def parse_plan_node(raw_node: dict[str, Any]) -> PlanNode:
    """Convert one raw DuckDB JSON object into a PlanNode."""
    children = []
    for child in raw_node.get("children", []):
        children.append(parse_plan_node(child))
    return PlanNode(
        name=str(raw_node["name"]),
        children=tuple(children),
        extra_info=dict(raw_node.get("extra_info", {})),
    )


def is_scan_node(node: PlanNode) -> bool:
    """Return true when a dynamic LogicalGet node looks like a scan."""
    if "Table" in node.extra_info:
        return True
    return node.extra_info.get("Type", "").endswith("Scan")


def reconstruct_sql(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    include_raw_json: bool,
    quiet: bool,
) -> str:
    """Return reconstructed SQL and optional verbose reconstruction details."""
    registry = read_logical_node_registry()
    surface = SqlSurface(sql, connection)
    root = explain_optimized_json(connection, sql)
    validate_tree(root, registry)
    relation = reconstruct_relation(root, surface, registry)
    sql_text = format_select_sql(surface, relation)
    if quiet:
        return sql_text
    return format_verbose_output(root, relation, sql_text, include_raw_json)


def validate_tree(node: PlanNode, registry: DuckDBNodeRegistry) -> None:
    """Validate every node against the source-derived registry."""
    registry.validate_node(node)
    for child in node.children:
        validate_tree(child, registry)


def reconstruct_relation(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a relation subtree from a supported DuckDB plan node."""
    if is_scan_node(node):
        return reconstruct_scan(node, surface)
    handlers = relation_handlers()
    handler = handlers.get(node.name)
    if not handler:
        raise PlanReconstructionError(f"No relation handler for {node.name}")
    return handler(node, surface, registry)


def relation_handlers() -> dict[str, Any]:
    """Return node-name to reconstruction-function dispatch mapping."""
    return {
        "PROJECTION": reconstruct_unary_passthrough,
        "FILTER": reconstruct_filter,
        "DELIM_JOIN": reconstruct_join,
        "COMPARISON_JOIN": reconstruct_join,
        "JOIN": reconstruct_join,
        "ANY_JOIN": reconstruct_join,
        "CROSS_PRODUCT": reconstruct_cross_product,
        "AGGREGATE": reconstruct_unary_passthrough,
        "DISTINCT": reconstruct_unary_passthrough,
        "LIMIT": reconstruct_unary_passthrough,
        "ORDER_BY": reconstruct_unary_passthrough,
        "TOP_N": reconstruct_unary_passthrough,
        "EMPTY_RESULT": reconstruct_empty_result,
        "DUMMY_SCAN": reconstruct_empty_result,
        "DELIM_GET": reconstruct_delim_get,
    }


def reconstruct_scan(node: PlanNode, surface: SqlSurface) -> RelationSql:
    """Reconstruct a base scan and pushed filters."""
    plan_table = str(node.extra_info.get("Table", node.name))
    alias = surface.alias_for_table(plan_table)
    filters = normalize_filters(node.extra_info.get("Filters", ""))
    qualified_filters = qualify_filters(filters, surface, (alias,))
    mapping = (f"{node.name} -> FROM {surface.display_table(plan_table)}",)
    return RelationSql(
        surface.display_table(plan_table),
        qualified_filters,
        (alias,),
        mapping,
    )


def reconstruct_unary_passthrough(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a unary node by preserving its child relation."""
    child = only_child(node)
    relation = reconstruct_relation(child, surface, registry)
    mapping = relation.mapping + (f"{node.name} -> preserved in SELECT shape",)
    return RelationSql(relation.from_sql, relation.filters, relation.aliases, mapping)


def reconstruct_filter(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a filter node by adding its expressions as predicates."""
    child = only_child(node)
    relation = reconstruct_relation(child, surface, registry)
    filters = normalize_filters(node.extra_info.get("Expressions", ""))
    qualified = qualify_filters(filters, surface, relation.aliases)
    mapping = relation.mapping + (f"FILTER -> WHERE {' AND '.join(qualified)}",)
    return RelationSql(
        relation.from_sql,
        relation.filters + qualified,
        relation.aliases,
        mapping,
    )


def reconstruct_cross_product(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a cross product between two relation children."""
    left, right = two_children(node)
    left_relation = reconstruct_relation(left, surface, registry)
    right_relation = reconstruct_relation(right, surface, registry)
    from_sql = f"{left_relation.from_sql} CROSS JOIN {right_relation.from_sql}"
    filters = left_relation.filters + right_relation.filters
    aliases = left_relation.aliases + right_relation.aliases
    mapping = left_relation.mapping + right_relation.mapping + ("CROSS_PRODUCT -> CROSS JOIN",)
    return RelationSql(from_sql, filters, aliases, mapping)


def reconstruct_join(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a comparison or delim join."""
    left, right = two_children(node)
    left_relation = reconstruct_relation(left, surface, registry)
    right_relation = reconstruct_relation(right, surface, registry)
    join_keyword = join_type_keyword(node)
    condition = best_join_condition(node, left_relation, right_relation, surface)
    from_sql = join_from_sql(left_relation, right_relation, join_keyword, condition)
    filters = left_relation.filters + right_relation.filters
    aliases = left_relation.aliases + right_relation.aliases
    mapping = join_mapping(left_relation, right_relation, node, join_keyword, condition)
    return RelationSql(from_sql, filters, aliases, mapping)


def reconstruct_empty_result(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct an empty result placeholder."""
    return RelationSql("(SELECT * WHERE FALSE)", tuple(), tuple(), (f"{node.name} -> empty result",))


def reconstruct_delim_get(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a delim get placeholder used inside decorrelated plans."""
    return RelationSql("", tuple(), tuple(), ("DELIM_GET -> correlated input",))


def best_join_condition(
    node: PlanNode,
    left_relation: RelationSql,
    right_relation: RelationSql,
    surface: SqlSurface,
) -> str:
    """Return the most readable condition for a join node."""
    inner_condition = find_inner_join_condition(node)
    if inner_condition and node.name == "DELIM_JOIN":
        return qualify_binary_condition(
            inner_condition,
            right_relation.aliases,
            left_relation.aliases,
            surface,
        )
    condition = first_extra_value(node.extra_info.get("Conditions", ""))
    return qualify_binary_condition(
        condition,
        left_relation.aliases,
        right_relation.aliases,
        surface,
    )


def find_inner_join_condition(node: PlanNode) -> Optional[str]:
    """Find a nested comparison join condition inside a delim join RHS."""
    for child in node.children:
        if child.name == "COMPARISON_JOIN":
            return first_extra_value(child.extra_info.get("Conditions", ""))
        nested_condition = find_inner_join_condition(child)
        if nested_condition:
            return nested_condition
    return None


def qualify_binary_condition(
    condition: str,
    left_aliases: tuple[str, ...],
    right_aliases: tuple[str, ...],
    surface: SqlSurface,
) -> str:
    """Qualify a simple binary condition using left and right relation aliases."""
    clean_condition = strip_wrapping_parentheses(condition)
    operator = find_binary_operator(clean_condition)
    if not operator:
        return qualify_expression(clean_condition, surface, left_aliases + right_aliases)
    left_text, right_text = split_binary_condition(clean_condition, operator)
    left_sql = surface.qualify_column(left_text.strip(), left_aliases)
    right_sql = surface.qualify_column(right_text.strip(), right_aliases)
    return f"{left_sql} {operator} {right_sql}"


def find_binary_operator(condition: str) -> Optional[str]:
    """Return the supported binary operator found in condition text."""
    for operator in (" IS NOT DISTINCT FROM ", " = ", "=", " > ", " < "):
        if operator in condition:
            return operator.strip()
    return None


def split_binary_condition(condition: str, operator: str) -> tuple[str, str]:
    """Split a binary condition around its operator."""
    if operator == "=":
        parts = condition.split("=", 1)
        return parts[0], parts[1]
    parts = condition.split(f" {operator} ", 1)
    return parts[0], parts[1]


def strip_wrapping_parentheses(text: str) -> str:
    """Remove one pair of wrapping parentheses from an expression string."""
    stripped_text = text.strip()
    if stripped_text.startswith("(") and stripped_text.endswith(")"):
        return stripped_text[1:-1].strip()
    return stripped_text


def join_type_keyword(node: PlanNode) -> str:
    """Return the SQL join keyword for a DuckDB join node."""
    join_type = str(node.extra_info.get("Join Type", "INNER")).upper()
    mapping = {
        "INNER": "JOIN",
        "LEFT": "LEFT JOIN",
        "RIGHT": "RIGHT JOIN",
        "OUTER": "FULL JOIN",
        "FULL": "FULL JOIN",
        "SEMI": "SEMI JOIN",
        "ANTI": "ANTI JOIN",
        "MARK": "MARK JOIN",
    }
    return mapping.get(join_type, f"{join_type} JOIN")


def join_from_sql(
    left_relation: RelationSql,
    right_relation: RelationSql,
    join_keyword: str,
    condition: str,
) -> str:
    """Format a SQL FROM clause containing one join."""
    if not right_relation.from_sql:
        return left_relation.from_sql
    if join_keyword == "CROSS JOIN":
        return f"{left_relation.from_sql} CROSS JOIN {right_relation.from_sql}"
    return f"{left_relation.from_sql}\n{join_keyword} {right_relation.from_sql}\n  ON {condition}"


def join_mapping(
    left_relation: RelationSql,
    right_relation: RelationSql,
    node: PlanNode,
    join_keyword: str,
    condition: str,
) -> tuple[str, ...]:
    """Return verbose mapping entries for a reconstructed join."""
    mapping = left_relation.mapping + right_relation.mapping
    return mapping + (f"{node.name} -> {join_keyword} ON {condition}",)


def normalize_filters(raw_filters: Any) -> tuple[str, ...]:
    """Normalize DuckDB filter extra_info into a tuple of predicate strings."""
    if not raw_filters:
        return tuple()
    if isinstance(raw_filters, list):
        return normalize_filter_list(raw_filters)
    return normalize_filter_text(str(raw_filters))


def normalize_filter_list(raw_filters: list[Any]) -> tuple[str, ...]:
    """Normalize a list of DuckDB filter strings."""
    filters = []
    for raw_filter in raw_filters:
        filters.extend(normalize_filter_text(str(raw_filter)))
    return tuple(filters)


def normalize_filter_text(raw_filters: str) -> tuple[str, ...]:
    """Normalize a newline-separated DuckDB filter string."""
    filters = []
    for filter_text in raw_filters.splitlines():
        stripped_filter = strip_wrapping_parentheses(filter_text)
        if stripped_filter and stripped_filter != "SUBQUERY":
            filters.append(stripped_filter)
    return tuple(filters)


def qualify_filters(
    filters: tuple[str, ...], surface: SqlSurface, aliases: tuple[str, ...]
) -> tuple[str, ...]:
    """Qualify all filter strings using relation aliases."""
    qualified_filters = []
    for filter_text in filters:
        qualified_filters.append(qualify_expression(filter_text, surface, aliases))
    return tuple(qualified_filters)


def qualify_expression(
    expression: str, surface: SqlSurface, aliases: tuple[str, ...]
) -> str:
    """Qualify unqualified column names in simple expression text."""
    result = expression
    column_names = collect_columns_for_aliases(surface, aliases)
    for column_name in sorted(column_names, key=len, reverse=True):
        qualified_name = surface.qualify_column(column_name, aliases)
        result = replace_unqualified_identifier(result, column_name, qualified_name)
    return result


def collect_columns_for_aliases(
    surface: SqlSurface, aliases: tuple[str, ...]
) -> set[str]:
    """Return all known columns for the supplied aliases."""
    column_names = set()
    for alias in aliases:
        table_name = surface.alias_to_table.get(alias, alias)
        columns = surface.table_columns.get(table_name, set())
        for column_name in columns:
            column_names.add(column_name)
    return column_names


def replace_unqualified_identifier(text: str, name: str, replacement: str) -> str:
    """Replace an unqualified identifier without touching qualified names."""
    pattern = rf"(?<![.\w]){re.escape(name)}(?![\w])"
    return re.sub(pattern, replacement, text)


def first_extra_value(value: Any) -> str:
    """Return the first textual value from a DuckDB extra_info field."""
    if isinstance(value, list):
        if not value:
            return ""
        return str(value[0])
    return str(value)


def only_child(node: PlanNode) -> PlanNode:
    """Return the only child of a unary node."""
    if len(node.children) != 1:
        raise PlanReconstructionError(f"{node.name} expected one child")
    return node.children[0]


def two_children(node: PlanNode) -> tuple[PlanNode, PlanNode]:
    """Return the two children of a binary node."""
    if len(node.children) != 2:
        raise PlanReconstructionError(f"{node.name} expected two children")
    return node.children[0], node.children[1]


def format_select_sql(surface: SqlSurface, relation: RelationSql) -> str:
    """Format the final reconstructed SELECT statement."""
    lines = [f"SELECT {surface.projection_sql()}", f"FROM {relation.from_sql}"]
    if relation.filters:
        lines.append("WHERE " + " AND ".join(relation.filters))
    order_sql = surface.order_sql()
    if order_sql:
        lines.append(order_sql)
    limit_sql = surface.limit_sql()
    if limit_sql:
        lines.append(limit_sql)
    return "\n".join(lines) + ";"


def format_verbose_output(
    root: PlanNode, relation: RelationSql, sql_text: str, include_raw_json: bool
) -> str:
    """Format verbose reconstruction output for the CLI and REPL."""
    lines = ["Reconstruction mapping:"]
    for mapping_line in relation.mapping:
        lines.append(f"- {mapping_line}")
    lines.append("")
    lines.append("Reconstructed SQL:")
    lines.append(sql_text)
    if include_raw_json:
        lines.append("")
        lines.append("Optimized JSON:")
        lines.append(json.dumps(plan_node_to_json(root), indent=2))
    return "\n".join(lines)


def plan_node_to_json(node: PlanNode) -> dict[str, Any]:
    """Convert a PlanNode back into JSON-serializable data."""
    children = []
    for child in node.children:
        children.append(plan_node_to_json(child))
    return {"name": node.name, "children": children, "extra_info": node.extra_info}


def apply_setup_sql(connection: duckdb.DuckDBPyConnection, setup_sql: str) -> None:
    """Execute setup SQL on the active DuckDB connection."""
    if setup_sql.strip():
        connection.execute(setup_sql)


def read_optional_file(path: Optional[str]) -> str:
    """Read a file path option when supplied."""
    if not path:
        return ""
    return Path(path).read_text()


def combine_setup_sql(setup: str, setup_file: Optional[str]) -> str:
    """Combine inline and file-based setup SQL."""
    setup_sql = ""
    file_sql = read_optional_file(setup_file)
    if file_sql:
        setup_sql += file_sql + "\n"
    if setup:
        setup_sql += setup
    return setup_sql


def read_query_sql(sql: str, sql_file: Optional[str]) -> str:
    """Return SQL from either an inline string or file path."""
    if sql:
        return sql
    if sql_file:
        return Path(sql_file).read_text()
    raise click.ClickException("Provide --sql or --sql-file")


@click.group()
def cli() -> None:
    """Run the DuckDB optimized SQL reconstructor."""


@cli.command()
@click.option("--database", default=":memory:", show_default=True)
@click.option("--setup", default="")
@click.option("--setup-file")
@click.option("--sql", default="")
@click.option("--sql-file")
@click.option("--json", "include_raw_json", is_flag=True)
@click.option("--quiet", is_flag=True)
def render(
    database: str,
    setup: str,
    setup_file: Optional[str],
    sql: str,
    sql_file: Optional[str],
    include_raw_json: bool,
    quiet: bool,
) -> None:
    """Render one SQL query from DuckDB optimized JSON."""
    connection = duckdb.connect(database)
    setup_sql = combine_setup_sql(setup, setup_file)
    query_sql = read_query_sql(sql, sql_file)
    apply_setup_sql(connection, setup_sql)
    click.echo(reconstruct_sql(connection, query_sql, include_raw_json, quiet))


@cli.command()
@click.option("--database", default=":memory:", show_default=True)
@click.option("--setup", default="")
@click.option("--setup-file")
@click.option("--json", "include_raw_json", is_flag=True)
@click.option("--quiet", is_flag=True)
def repl(
    database: str,
    setup: str,
    setup_file: Optional[str],
    include_raw_json: bool,
    quiet: bool,
) -> None:
    """Start a multiline REPL for optimized SQL reconstruction."""
    connection = duckdb.connect(database)
    apply_setup_sql(connection, combine_setup_sql(setup, setup_file))
    session = PromptSession(multiline=True)
    run_repl(session, connection, include_raw_json, quiet)


def run_repl(
    session: PromptSession,
    connection: duckdb.DuckDBPyConnection,
    include_raw_json: bool,
    quiet: bool,
) -> None:
    """Read SQL statements and print reconstructed output until quit."""
    while True:
        sql = session.prompt("duckdb-reconstruct> ").strip()
        if sql in (":quit", ":exit"):
            return
        if not sql:
            continue
        click.echo(reconstruct_sql(connection, sql, include_raw_json, quiet))


if __name__ == "__main__":
    cli()
