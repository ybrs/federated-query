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
    output_columns: tuple[str, ...] = tuple()


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


@dataclass
class AutoSchemaTable:
    """Holds best-effort table and column definitions for auto schema mode."""

    name: str
    columns: dict[str, str]


class SqlSurface:
    """Provides alias and clause text recovered from the original SQL."""

    def __init__(self, sql: str, connection: duckdb.DuckDBPyConnection):
        """Parse SQL and load table/alias metadata for reconstruction."""
        self.sql = sql
        self.expression = sqlglot.parse_one(sql, read="duckdb")
        self.connection = connection
        self.cte_names = collect_cte_names(self.expression)
        self.cte_output_names = collect_cte_output_names(self.expression)
        self.cte_base_tables = collect_cte_base_tables(self.expression)
        self.cte_join_names = collect_cte_join_names(self.expression, self.cte_names)
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

    def decorrelated_join_condition(self) -> Optional[str]:
        """Return the first original equality predicate across table aliases."""
        for equality in self.expression.find_all(sqlglot.expressions.EQ):
            condition = self._equality_sql_if_cross_alias(equality)
            if condition:
                return condition
        return None

    def aggregate_alias_for_relation(
        self, relation: RelationSql
    ) -> tuple[Optional[str], tuple[str, ...]]:
        """Return a CTE alias and output names for an aggregate relation."""
        for cte_name in self.cte_join_names:
            base_tables = self.cte_base_tables.get(cte_name, set())
            if relation_uses_any_table(relation, base_tables):
                return cte_name, self.cte_output_names.get(cte_name, tuple())
        return None, tuple()

    def inlined_cte_relation(self, relation: RelationSql) -> Optional[RelationSql]:
        """Return a relation aliased as its original CTE name when inlined."""
        if relation_is_aggregate_derived(relation):
            return None
        for cte_name in self.cte_join_names:
            base_tables = self.cte_base_tables.get(cte_name, set())
            if relation_uses_any_table(relation, base_tables):
                return alias_relation_as_cte(relation, cte_name)
        return None

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
            if table.name in self.cte_names:
                continue
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

    def _equality_sql_if_cross_alias(
        self, equality: sqlglot.expressions.EQ
    ) -> Optional[str]:
        """Return equality SQL when both sides reference different aliases."""
        left = equality.left
        right = equality.right
        if not isinstance(left, sqlglot.expressions.Column):
            return None
        if not isinstance(right, sqlglot.expressions.Column):
            return None
        if left.table and right.table and left.table != right.table:
            return f"{left.sql(dialect='duckdb')} = {right.sql(dialect='duckdb')}"
        return None


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


def apply_auto_schema(connection: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Create missing tables and columns needed to bind one SQL statement."""
    expression = sqlglot.parse_one(sql, read="duckdb")
    tables, alias_to_table = collect_auto_schema_tables(expression)
    cte_names = collect_cte_names(expression)
    collect_auto_schema_columns(expression, tables, alias_to_table, cte_names)
    infer_auto_schema_types(expression, tables, alias_to_table, cte_names)
    ensure_auto_schema_tables(connection, tables)


def collect_cte_names(expression: sqlglot.expressions.Expression) -> set[str]:
    """Collect CTE names that should not be treated as base tables."""
    cte_names = set()
    for cte in expression.find_all(sqlglot.expressions.CTE):
        cte_names.add(cte.alias_or_name)
    return cte_names


def collect_cte_output_names(
    expression: sqlglot.expressions.Expression,
) -> dict[str, tuple[str, ...]]:
    """Collect output names for each CTE body."""
    output_names = {}
    for cte in expression.find_all(sqlglot.expressions.CTE):
        names = []
        for select_expression in cte.this.expressions:
            names.append(select_expression.alias_or_name)
        output_names[cte.alias_or_name] = tuple(names)
    return output_names


def collect_cte_base_tables(
    expression: sqlglot.expressions.Expression,
) -> dict[str, set[str]]:
    """Collect base table names referenced inside each CTE body."""
    cte_base_tables = {}
    for cte in expression.find_all(sqlglot.expressions.CTE):
        base_tables = set()
        for table in cte.this.find_all(sqlglot.expressions.Table):
            base_tables.add(table.name)
        cte_base_tables[cte.alias_or_name] = base_tables
    return cte_base_tables


def collect_cte_join_names(
    expression: sqlglot.expressions.Expression, cte_names: set[str]
) -> tuple[str, ...]:
    """Collect CTE names used directly as join relations."""
    join_names = []
    for join in expression.find_all(sqlglot.expressions.Join):
        relation = join.this
        if isinstance(relation, sqlglot.expressions.Table):
            if relation.name in cte_names:
                join_names.append(relation.name)
    return tuple(join_names)


def collect_auto_schema_tables(
    expression: sqlglot.expressions.Expression,
) -> tuple[dict[str, AutoSchemaTable], dict[str, str]]:
    """Collect table names and aliases from a SQL expression."""
    tables: dict[str, AutoSchemaTable] = {}
    alias_to_table: dict[str, str] = {}
    cte_names = collect_cte_names(expression)
    for table in expression.find_all(sqlglot.expressions.Table):
        if table.name in cte_names:
            continue
        table_name = table.name
        tables[table_name] = AutoSchemaTable(table_name, {})
        alias_to_table[table.alias_or_name] = table_name
    return tables, alias_to_table


def collect_auto_schema_columns(
    expression: sqlglot.expressions.Expression,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Collect referenced columns and assign default types."""
    for column in expression.find_all(sqlglot.expressions.Column):
        add_cte_projected_column_to_base_tables(column, expression, tables, cte_names)
        table_names = resolve_auto_schema_column_tables(
            column, tables, alias_to_table, cte_names
        )
        for table_name in table_names:
            tables[table_name].columns.setdefault(column.name, "BIGINT")
    add_dummy_columns_for_star_only_tables(tables)


def add_cte_projected_column_to_base_tables(
    column: sqlglot.expressions.Column,
    expression: sqlglot.expressions.Expression,
    tables: dict[str, AutoSchemaTable],
    cte_names: set[str],
) -> None:
    """Propagate cte.column references through simple star CTE bodies."""
    if not column.table:
        return
    if column.table not in cte_names:
        return
    base_tables = collect_cte_base_tables(expression).get(column.table, set())
    if len(base_tables) != 1:
        return
    for base_table in base_tables:
        if base_table in tables:
            tables[base_table].columns.setdefault(column.name, "BIGINT")


def resolve_auto_schema_column_tables(
    column: sqlglot.expressions.Column,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> tuple[str, ...]:
    """Resolve a column reference to one or more auto schema tables."""
    if column.table:
        table_name = alias_to_table.get(column.table, column.table)
        if table_name in cte_names:
            return tuple()
        if table_name not in tables:
            return tuple()
        return (table_name,)
    if len(tables) == 1:
        for table_name in tables:
            return (table_name,)
    table_names = []
    for table_name in tables:
        table_names.append(table_name)
    return tuple(table_names)


def add_dummy_columns_for_star_only_tables(
    tables: dict[str, AutoSchemaTable]
) -> None:
    """Add one column to tables that are only referenced through stars."""
    for table in tables.values():
        if not table.columns:
            table.columns["_auto_col"] = "BIGINT"


def infer_auto_schema_types(
    expression: sqlglot.expressions.Expression,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Infer simple column types from nearby SQL literals."""
    for comparison in comparison_expressions(expression):
        infer_comparison_types(comparison, tables, alias_to_table, cte_names)
    for in_expression in expression.find_all(sqlglot.expressions.In):
        infer_in_expression_types(in_expression, tables, alias_to_table, cte_names)


def comparison_expressions(
    expression: sqlglot.expressions.Expression,
) -> tuple[sqlglot.expressions.Expression, ...]:
    """Return comparison expressions that can imply column types."""
    comparisons = []
    comparison_types = (
        sqlglot.expressions.EQ,
        sqlglot.expressions.NEQ,
        sqlglot.expressions.GT,
        sqlglot.expressions.GTE,
        sqlglot.expressions.LT,
        sqlglot.expressions.LTE,
    )
    for comparison_type in comparison_types:
        for comparison in expression.find_all(comparison_type):
            comparisons.append(comparison)
    return tuple(comparisons)


def infer_comparison_types(
    comparison: sqlglot.expressions.Expression,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Infer types for columns compared directly with literals."""
    infer_column_type_from_pair(
        comparison.left, comparison.right, tables, alias_to_table, cte_names
    )
    infer_column_type_from_pair(
        comparison.right, comparison.left, tables, alias_to_table, cte_names
    )


def infer_in_expression_types(
    expression: sqlglot.expressions.In,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Infer a column type from literal values in an IN expression."""
    column = expression.this
    if not isinstance(column, sqlglot.expressions.Column):
        return
    for value in expression.expressions:
        inferred_type = infer_literal_type(value)
        if inferred_type:
            set_auto_schema_column_type(
                column, inferred_type, tables, alias_to_table, cte_names
            )


def infer_column_type_from_pair(
    column_candidate: sqlglot.expressions.Expression,
    literal_candidate: sqlglot.expressions.Expression,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Infer one column type from a column/literal expression pair."""
    if not isinstance(column_candidate, sqlglot.expressions.Column):
        return
    inferred_type = infer_literal_type(literal_candidate)
    if inferred_type:
        set_auto_schema_column_type(
            column_candidate, inferred_type, tables, alias_to_table, cte_names
        )


def infer_literal_type(expression: sqlglot.expressions.Expression) -> Optional[str]:
    """Infer a DuckDB type from a literal-like SQL expression."""
    if isinstance(expression, sqlglot.expressions.Literal):
        return infer_basic_literal_type(expression)
    if isinstance(expression, sqlglot.expressions.Boolean):
        return "BOOLEAN"
    if isinstance(expression, sqlglot.expressions.Date):
        return "DATE"
    return None


def infer_basic_literal_type(expression: sqlglot.expressions.Literal) -> str:
    """Infer a DuckDB type from a sqlglot literal."""
    if expression.is_string:
        return "VARCHAR"
    if "." in expression.this:
        return "DOUBLE"
    return "BIGINT"


def set_auto_schema_column_type(
    column: sqlglot.expressions.Column,
    inferred_type: str,
    tables: dict[str, AutoSchemaTable],
    alias_to_table: dict[str, str],
    cte_names: set[str],
) -> None:
    """Set an inferred type for every resolved column table."""
    table_names = resolve_auto_schema_column_tables(
        column, tables, alias_to_table, cte_names
    )
    for table_name in table_names:
        table = tables[table_name]
        current_type = table.columns.get(column.name, "BIGINT")
        table.columns[column.name] = wider_auto_schema_type(current_type, inferred_type)


def wider_auto_schema_type(current_type: str, inferred_type: str) -> str:
    """Return the safer type when multiple inferences disagree."""
    precedence = {"BIGINT": 1, "DOUBLE": 2, "DATE": 3, "BOOLEAN": 3, "VARCHAR": 4}
    if precedence[inferred_type] > precedence[current_type]:
        return inferred_type
    return current_type


def ensure_auto_schema_tables(
    connection: duckdb.DuckDBPyConnection,
    tables: dict[str, AutoSchemaTable],
) -> None:
    """Create missing tables and add missing columns for auto schema mode."""
    for table in tables.values():
        if auto_schema_table_exists(connection, table.name):
            ensure_auto_schema_columns(connection, table)
        else:
            create_auto_schema_table(connection, table)
            seed_auto_schema_table(connection, table)


def auto_schema_table_exists(
    connection: duckdb.DuckDBPyConnection, table_name: str
) -> bool:
    """Return true when a table exists in the active DuckDB connection."""
    rows = connection.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return rows[0] > 0


def ensure_auto_schema_columns(
    connection: duckdb.DuckDBPyConnection, table: AutoSchemaTable
) -> None:
    """Add missing columns to an existing auto schema table."""
    existing_columns = read_existing_columns(connection, table.name)
    for column_name, column_type in table.columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE {quote_identifier(table.name)} "
                f"ADD COLUMN {quote_identifier(column_name)} {column_type}"
            )


def read_existing_columns(
    connection: duckdb.DuckDBPyConnection, table_name: str
) -> set[str]:
    """Return existing column names for a table."""
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns = set()
    for row in rows:
        columns.add(str(row[1]))
    return columns


def create_auto_schema_table(
    connection: duckdb.DuckDBPyConnection, table: AutoSchemaTable
) -> None:
    """Create a best-effort table for auto schema mode."""
    column_sql = format_auto_schema_columns(table)
    connection.execute(f"CREATE TABLE {quote_identifier(table.name)} ({column_sql})")


def format_auto_schema_columns(table: AutoSchemaTable) -> str:
    """Format column definitions for CREATE TABLE."""
    column_parts = []
    for column_name, column_type in table.columns.items():
        column_parts.append(f"{quote_identifier(column_name)} {column_type}")
    return ", ".join(column_parts)


def seed_auto_schema_table(
    connection: duckdb.DuckDBPyConnection, table: AutoSchemaTable
) -> None:
    """Insert rows into auto-created tables so optimizer plans stay meaningful."""
    column_names = []
    values = []
    for column_name, column_type in table.columns.items():
        column_names.append(quote_identifier(column_name))
        values.append(auto_schema_value_sql(column_type))
    column_sql = ", ".join(column_names)
    value_sql = ", ".join(values)
    for _ in range(20):
        connection.execute(
            f"INSERT INTO {quote_identifier(table.name)} ({column_sql}) "
            f"VALUES ({value_sql})"
        )


def auto_schema_value_sql(column_type: str) -> str:
    """Return a literal value for one auto schema column type."""
    if column_type == "VARCHAR":
        return "'x'"
    if column_type == "DOUBLE":
        return "150.0"
    if column_type == "DATE":
        return "DATE '2026-01-01'"
    if column_type == "BOOLEAN":
        return "TRUE"
    return "150"


def quote_identifier(identifier: str) -> str:
    """Quote a DuckDB identifier."""
    escaped_identifier = identifier.replace('"', '""')
    return f'"{escaped_identifier}"'


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
    auto_schema: bool = False,
) -> str:
    """Return reconstructed SQL and optional verbose reconstruction details."""
    registry = read_logical_node_registry()
    if auto_schema:
        apply_auto_schema(connection, sql)
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
        "AGGREGATE": reconstruct_aggregate,
        "DISTINCT": reconstruct_unary_passthrough,
        "LIMIT": reconstruct_unary_passthrough,
        "ORDER_BY": reconstruct_unary_passthrough,
        "TOP_N": reconstruct_unary_passthrough,
        "EMPTY_RESULT": reconstruct_empty_result,
        "DUMMY_SCAN": reconstruct_dummy_scan,
        "DELIM_GET": reconstruct_delim_get,
    }


def reconstruct_scan(node: PlanNode, surface: SqlSurface) -> RelationSql:
    """Reconstruct a base scan and pushed filters."""
    plan_table = str(node.extra_info.get("Table", node.name))
    alias = surface.alias_for_table(plan_table)
    table_name = plan_table.split(".")[-1]
    filters = normalize_filters(node.extra_info.get("Filters", ""))
    qualified_filters = qualify_filters(filters, surface, (alias,))
    mapping = (f"{node.name} -> FROM {surface.display_table(plan_table)}",)
    return RelationSql(
        surface.display_table(plan_table),
        qualified_filters,
        (alias,),
        mapping,
        tuple(surface.table_columns.get(table_name, tuple())),
    )


def reconstruct_unary_passthrough(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a unary node by preserving its child relation."""
    child = only_child(node)
    relation = reconstruct_relation(child, surface, registry)
    mapping = relation.mapping + (f"{node.name} -> preserved in SELECT shape",)
    return RelationSql(
        relation.from_sql,
        relation.filters,
        relation.aliases,
        mapping,
        relation.output_columns,
    )


def reconstruct_filter(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a filter node by adding its expressions as predicates."""
    child = only_child(node)
    relation = reconstruct_relation(child, surface, registry)
    filters = normalize_filters(node.extra_info.get("Expressions", ""))
    if relation_is_aggregate_derived(relation):
        return add_having_to_aggregate_relation(relation, filters)
    qualified = qualify_filters(filters, surface, relation.aliases)
    mapping = relation.mapping + (f"FILTER -> WHERE {' AND '.join(qualified)}",)
    return RelationSql(
        relation.from_sql,
        relation.filters + qualified,
        relation.aliases,
        mapping,
        relation.output_columns,
    )


def relation_is_aggregate_derived(relation: RelationSql) -> bool:
    """Return true when a relation was produced from an aggregate node."""
    for mapping_line in relation.mapping:
        if mapping_line.startswith("AGGREGATE -> derived table"):
            return True
    return False


def add_having_to_aggregate_relation(
    relation: RelationSql, filters: tuple[str, ...]
) -> RelationSql:
    """Fold a filter above an aggregate node into the derived table HAVING."""
    if not filters:
        return relation
    having_sql = " AND ".join(filters)
    from_sql = relation.from_sql.replace(") AS ", f"\nHAVING {having_sql}) AS ", 1)
    mapping = relation.mapping + (f"FILTER -> HAVING {having_sql}",)
    return RelationSql(
        from_sql,
        relation.filters,
        relation.aliases,
        mapping,
        relation.output_columns,
    )


def reconstruct_aggregate(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct an aggregate node as a derived relation."""
    child = only_child(node)
    child_relation = reconstruct_relation(child, surface, registry)
    groups = normalize_filters(node.extra_info.get("Groups", ""))
    aggregates = normalize_filters(node.extra_info.get("Expressions", ""))
    alias, output_names = surface.aggregate_alias_for_relation(child_relation)
    select_parts = aggregate_select_parts(groups, aggregates, output_names)
    output_columns = aggregate_output_columns(groups, aggregates, output_names)
    from_sql = aggregate_from_sql(child_relation, select_parts, groups)
    relation_alias = alias or "__duckdb_agg"
    mapping = child_relation.mapping + (
        f"AGGREGATE -> derived table {relation_alias}",
    )
    return RelationSql(
        f"({from_sql}) AS {relation_alias}",
        tuple(),
        (relation_alias,),
        mapping,
        output_columns,
    )


def aggregate_output_columns(
    groups: tuple[str, ...],
    aggregates: tuple[str, ...],
    output_names: tuple[str, ...],
) -> tuple[str, ...]:
    """Return output column names for an aggregate derived relation."""
    if output_names:
        return output_names
    output_columns = []
    for group in groups:
        output_columns.append(group)
    for aggregate in aggregates:
        output_columns.append(aggregate)
    return tuple(output_columns)


def aggregate_select_parts(
    groups: tuple[str, ...],
    aggregates: tuple[str, ...],
    output_names: tuple[str, ...],
) -> tuple[str, ...]:
    """Return SELECT expressions for an aggregate derived relation."""
    select_parts = []
    for group in groups:
        select_parts.append(group)
    for aggregate in aggregates:
        select_parts.append(aggregate_with_output_alias(aggregate, select_parts, output_names))
    return tuple(select_parts)


def aggregate_with_output_alias(
    aggregate: str, select_parts: list[str], output_names: tuple[str, ...]
) -> str:
    """Attach an output alias to an aggregate expression when available."""
    output_index = len(select_parts)
    if output_index >= len(output_names):
        return aggregate
    output_name = output_names[output_index]
    if not output_name or output_name == aggregate:
        return aggregate
    return f"{aggregate} AS {output_name}"


def aggregate_from_sql(
    child_relation: RelationSql,
    select_parts: tuple[str, ...],
    groups: tuple[str, ...],
) -> str:
    """Format a derived aggregate SQL query."""
    lines = [f"SELECT {', '.join(select_parts)}", f"FROM {child_relation.from_sql}"]
    if child_relation.filters:
        lines.append("WHERE " + " AND ".join(child_relation.filters))
    if groups:
        lines.append("GROUP BY " + ", ".join(groups))
    return "\n".join(lines)


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
    right_relation = replace_inlined_cte_relation(surface, right_relation)
    empty_relation = reconstruct_empty_join(node, left_relation, right_relation)
    if empty_relation:
        return empty_relation
    join_keyword = join_type_keyword(node)
    condition = best_join_condition(node, left_relation, right_relation, surface)
    from_sql = join_from_sql(left_relation, right_relation, join_keyword, condition)
    filters = left_relation.filters + right_relation.filters
    aliases = left_relation.aliases + right_relation.aliases
    mapping = join_mapping(left_relation, right_relation, node, join_keyword, condition)
    return RelationSql(from_sql, filters, aliases, mapping)


def replace_inlined_cte_relation(
    surface: SqlSurface, right_relation: RelationSql
) -> RelationSql:
    """Alias an inlined CTE relation with its original CTE name."""
    cte_relation = surface.inlined_cte_relation(right_relation)
    if cte_relation:
        return cte_relation
    return right_relation


def alias_relation_as_cte(relation: RelationSql, cte_name: str) -> RelationSql:
    """Return a relation displayed under an inlined CTE alias."""
    from_sql = add_alias_to_relation_sql(relation.from_sql, cte_name)
    mapping = relation.mapping + (f"CTE {cte_name} -> inlined relation alias",)
    return RelationSql(
        from_sql,
        relation.filters,
        (cte_name,),
        mapping,
        relation.output_columns,
    )


def add_alias_to_relation_sql(from_sql: str, alias: str) -> str:
    """Add a SQL alias to a relation fragment."""
    if " AS " in from_sql:
        return from_sql
    if "\n" in from_sql or from_sql.startswith("("):
        return f"({from_sql}) AS {alias}"
    return f"{from_sql} AS {alias}"


def reconstruct_empty_join(
    node: PlanNode, left_relation: RelationSql, right_relation: RelationSql
) -> Optional[RelationSql]:
    """Reconstruct joins where one optimized side is EMPTY_RESULT."""
    join_type = str(node.extra_info.get("Join Type", "INNER")).upper()
    if relation_is_empty(left_relation):
        return empty_join_result(node, left_relation, right_relation)
    if not relation_is_empty(right_relation):
        return None
    if join_type == "ANTI":
        mapping = left_relation.mapping + right_relation.mapping
        return RelationSql(left_relation.from_sql, left_relation.filters, left_relation.aliases, mapping)
    if join_type == "SEMI":
        return left_relation_with_false_filter(node, left_relation, right_relation)
    if join_type == "INNER":
        return empty_join_result(node, left_relation, right_relation)
    return None


def relation_is_empty(relation: RelationSql) -> bool:
    """Return true when a relation represents DuckDB EMPTY_RESULT."""
    return relation.from_sql == "(SELECT * WHERE FALSE)"


def relation_uses_any_table(relation: RelationSql, table_names: set[str]) -> bool:
    """Return true when relation SQL references any supplied table name."""
    for table_name in table_names:
        if re.search(rf"\b{re.escape(table_name)}\b", relation.from_sql):
            return True
    return False


def empty_join_result(
    node: PlanNode, left_relation: RelationSql, right_relation: RelationSql
) -> RelationSql:
    """Return an empty relation for joins that cannot produce rows."""
    mapping = left_relation.mapping + right_relation.mapping
    mapping += (f"{node.name} -> empty result because one join side is empty",)
    return RelationSql("(SELECT * WHERE FALSE)", tuple(), tuple(), mapping)


def left_relation_with_false_filter(
    node: PlanNode, left_relation: RelationSql, right_relation: RelationSql
) -> RelationSql:
    """Return the left relation with a false predicate for empty SEMI joins."""
    mapping = left_relation.mapping + right_relation.mapping
    mapping += (f"{node.name} -> WHERE FALSE because SEMI JOIN RHS is empty",)
    return RelationSql(
        left_relation.from_sql,
        left_relation.filters + ("FALSE",),
        left_relation.aliases,
        mapping,
    )


def reconstruct_empty_result(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct an empty result placeholder."""
    return RelationSql("(SELECT * WHERE FALSE)", tuple(), tuple(), (f"{node.name} -> empty result",))


def reconstruct_dummy_scan(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a dummy scan as a SELECT without a FROM clause."""
    return RelationSql("", tuple(), tuple(), ("DUMMY_SCAN -> no FROM",))


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
    original_condition = original_condition_for_semantic_join(node, surface)
    if original_condition:
        return original_condition
    inner_condition = find_inner_join_condition(node)
    if inner_condition and node.name == "DELIM_JOIN":
        return qualify_binary_condition(
            inner_condition,
            right_relation.aliases,
            left_relation.aliases,
            surface,
        )
    condition = first_extra_value(node.extra_info.get("Conditions", ""))
    return qualify_join_condition(condition, left_relation, right_relation, surface)


def qualify_join_condition(
    condition: str,
    left_relation: RelationSql,
    right_relation: RelationSql,
    surface: SqlSurface,
) -> str:
    """Qualify a binary join condition using relation metadata."""
    clean_condition = strip_wrapping_parentheses(condition)
    operator = find_binary_operator(clean_condition)
    if not operator:
        aliases = left_relation.aliases + right_relation.aliases
        return qualify_expression(clean_condition, surface, aliases)
    left_text, right_text = split_binary_condition(clean_condition, operator)
    left_sql = resolve_condition_term(left_text.strip(), left_relation, surface)
    right_sql = resolve_condition_term(right_text.strip(), right_relation, surface)
    return f"{left_sql} {operator} {right_sql}"


def resolve_condition_term(
    term: str, relation: RelationSql, surface: SqlSurface
) -> str:
    """Resolve one side of a join condition against a relation."""
    slot_column = resolve_slot_reference(term, relation)
    if slot_column:
        return slot_column
    return surface.qualify_column(term, relation.aliases)


def resolve_slot_reference(term: str, relation: RelationSql) -> Optional[str]:
    """Resolve a DuckDB #N slot reference against relation output columns."""
    if not term.startswith("#"):
        return None
    slot_text = term[1:]
    if not slot_text.isdigit():
        return None
    slot_index = int(slot_text)
    if slot_index >= len(relation.output_columns):
        return None
    if not relation.aliases:
        return relation.output_columns[slot_index]
    return f"{relation.aliases[0]}.{relation.output_columns[slot_index]}"


def original_condition_for_semantic_join(
    node: PlanNode, surface: SqlSurface
) -> Optional[str]:
    """Return original cross-relation predicate for semantic joins."""
    join_type = str(node.extra_info.get("Join Type", "")).upper()
    if join_type not in ("INNER", "LEFT", "RIGHT", "FULL", "SEMI", "ANTI"):
        return None
    return surface.decorrelated_join_condition()


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
    lines = [f"SELECT {surface.projection_sql()}"]
    if relation.from_sql:
        lines.append(f"FROM {relation.from_sql}")
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
@click.option("--auto-schema", is_flag=True)
def render(
    database: str,
    setup: str,
    setup_file: Optional[str],
    sql: str,
    sql_file: Optional[str],
    include_raw_json: bool,
    quiet: bool,
    auto_schema: bool,
) -> None:
    """Render one SQL query from DuckDB optimized JSON."""
    connection = duckdb.connect(database)
    setup_sql = combine_setup_sql(setup, setup_file)
    query_sql = read_query_sql(sql, sql_file)
    apply_setup_sql(connection, setup_sql)
    click.echo(
        reconstruct_sql(connection, query_sql, include_raw_json, quiet, auto_schema)
    )


@cli.command()
@click.option("--database", default=":memory:", show_default=True)
@click.option("--setup", default="")
@click.option("--setup-file")
@click.option("--json", "include_raw_json", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.option("--auto-schema", is_flag=True)
def repl(
    database: str,
    setup: str,
    setup_file: Optional[str],
    include_raw_json: bool,
    quiet: bool,
    auto_schema: bool,
) -> None:
    """Start a multiline REPL for optimized SQL reconstruction."""
    connection = duckdb.connect(database)
    apply_setup_sql(connection, combine_setup_sql(setup, setup_file))
    session = PromptSession()
    run_repl(session, connection, include_raw_json, quiet, auto_schema)


def run_repl(
    session: PromptSession,
    connection: duckdb.DuckDBPyConnection,
    include_raw_json: bool,
    quiet: bool,
    auto_schema: bool = False,
) -> None:
    """Read SQL lines until semicolon and print reconstructed output."""
    pending_lines: list[str] = []
    while True:
        prompt_text = repl_prompt(pending_lines)
        line = session.prompt(prompt_text).strip()
        if should_exit_repl(line, pending_lines):
            return
        if not line:
            continue
        pending_lines.append(line)
        if line.endswith(";"):
            execute_repl_statement(
                "\n".join(pending_lines),
                connection,
                include_raw_json,
                quiet,
                auto_schema,
            )
            pending_lines = []


def repl_prompt(pending_lines: list[str]) -> str:
    """Return the primary or continuation REPL prompt."""
    if pending_lines:
        return "              ...> "
    return "duckdb-reconstruct> "


def should_exit_repl(line: str, pending_lines: list[str]) -> bool:
    """Return true when the user requested REPL exit."""
    if pending_lines:
        return False
    return line in (":quit", ":exit")


def execute_repl_statement(
    sql: str,
    connection: duckdb.DuckDBPyConnection,
    include_raw_json: bool,
    quiet: bool,
    auto_schema: bool = False,
) -> None:
    """Execute one REPL statement and print either output or an error."""
    try:
        output = reconstruct_sql(connection, sql, include_raw_json, quiet, auto_schema)
    except Exception as error:
        click.echo(f"ERROR: {error}")
        return
    click.echo(output)


if __name__ == "__main__":
    cli()
