#!/usr/bin/env python
"""Reconstruct readable SQL from DuckDB optimized logical EXPLAIN JSON."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import click
import duckdb
import sqlglot
from pygments import highlight
from pygments.formatters import TerminalFormatter
from pygments.lexers import JsonLexer, SqlLexer
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory

DUCKDB_SOURCE_ROOT = Path(__file__).resolve().parent / "duckdb-src"
LOGICAL_HEADER = (
    DUCKDB_SOURCE_ROOT / "src/include/duckdb/common/enums/logical_operator_type.hpp"
)
LOGICAL_STRINGS = DUCKDB_SOURCE_ROOT / "src/common/enums/logical_operator_type.cpp"
PLAN_DUMP_SOURCE = Path(__file__).resolve().parent / "duckdb_plan_dump.cpp"
PLAN_DUMP_BINARY = Path(__file__).resolve().parent / "duckdb_plan_dump"
DUCKDB_LIBRARY = DUCKDB_SOURCE_ROOT / "build/release/src/libduckdb.so"


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
    prelude_sql: tuple[str, ...] = tuple()
    projection_sql: Optional[str] = None
    complete_sql: Optional[str] = None


@dataclass(frozen=True)
class DuckDBNodeRegistry:
    """Holds source-derived DuckDB logical node names."""

    logical_names: frozenset[str]
    supported_names: frozenset[str]

    def assert_classified(self) -> None:
        """Raise when the supported set does not cover DuckDB source nodes."""
        missing_names = self.logical_names.difference(self.supported_names)
        if missing_names:
            raise PlanReconstructionError(
                "Unclassified DuckDB logical nodes: " + ", ".join(sorted(missing_names))
            )

    def validate_node(self, node: PlanNode) -> None:
        """Raise when a plan node is neither source-derived nor a dynamic scan."""
        if node.name in self.supported_names:
            return
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
        self.cte_join_aliases = collect_cte_join_aliases(
            self.expression, self.cte_names
        )
        self.root_subquery_view = collect_root_subquery_view(self.expression)
        self.alias_to_table: dict[str, str] = {}
        self.table_to_alias: dict[str, str] = {}
        self.table_aliases: dict[str, tuple[str, ...]] = {}
        self.table_scan_counts: dict[str, int] = {}
        self.table_columns: dict[str, set[str]] = {}
        self.generated_relation_counts: dict[str, int] = {}
        self._load_table_aliases()
        self._load_table_columns()

    def projection_sql(self) -> str:
        """Return the original SELECT projection list."""
        expressions = self.expression.args.get("expressions") or []
        projection_parts = []
        for expression in expressions:
            projection_parts.append(expression.sql(dialect="duckdb"))
        return ", ".join(projection_parts)

    def final_projection_sql(self, relation: RelationSql) -> str:
        """Return final projection SQL without scalar subquery syntax."""
        expressions = self.expression.args.get("expressions") or []
        projection_parts = []
        for expression in expressions:
            projection_parts.append(self.final_projection_part(expression, relation))
        return ", ".join(projection_parts)

    def final_projection_part(
        self, expression: sqlglot.expressions.Expression, relation: RelationSql
    ) -> str:
        """Return one final projection expression for reconstructed SQL."""
        subquery = expression.find(sqlglot.expressions.Subquery)
        if not subquery:
            return self.simple_projection_part(expression, relation)
        projected_sql = scalar_subquery_projection_sql(subquery)
        if not projected_sql:
            raise PlanReconstructionError(
                "Cannot reconstruct scalar subquery projection"
            )
        return scalar_projection_with_alias(expression, projected_sql)

    def simple_projection_part(
        self, expression: sqlglot.expressions.Expression, relation: RelationSql
    ) -> str:
        """Return one non-subquery projection expression."""
        if not isinstance(expression, sqlglot.expressions.Column):
            return expression.sql(dialect="duckdb")
        if expression.table:
            return expression.sql(dialect="duckdb")
        if len(relation.aliases) < 2:
            return expression.sql(dialect="duckdb")
        return self.qualify_column(expression.name, relation.aliases)

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

    def scalar_subquery_filter_sql(self) -> Optional[str]:
        """Return a simple scalar subquery comparison as a joined filter."""
        for comparison in scalar_subquery_comparisons(self.expression):
            replacement = scalar_subquery_comparison_sql(comparison)
            if replacement:
                return replacement
        return None

    def scalar_subquery_output_name(self) -> Optional[str]:
        """Return the projected output name for the first scalar subquery."""
        for subquery in self.expression.find_all(sqlglot.expressions.Subquery):
            output_name = scalar_subquery_output_name(subquery)
            if output_name:
                return output_name
        return None

    def aggregate_alias_for_relation(
        self, relation: RelationSql
    ) -> tuple[Optional[str], tuple[str, ...]]:
        """Return a CTE alias and output names for an aggregate relation."""
        for cte_name in self.cte_join_names:
            base_tables = self.cte_base_tables.get(cte_name, set())
            if relation_uses_any_table(relation, base_tables):
                alias = self.cte_join_aliases.get(cte_name, cte_name)
                return alias, self.cte_output_names.get(cte_name, tuple())
        return None, tuple()

    def scalar_aggregate_output_names(
        self, groups: tuple[str, ...], aggregates: tuple[str, ...]
    ) -> tuple[str, ...]:
        """Return output names for scalar subquery aggregate rewrites."""
        projection_name = scalar_ordered_limit_projection_name(self.expression)
        if not projection_name:
            return tuple()
        if len(aggregates) != 1:
            return tuple()
        output_names = []
        for group in groups:
            output_names.append(group)
        output_names.append(projection_name)
        return tuple(output_names)

    def generated_relation_alias(self, prefix: str) -> str:
        """Return a unique generated relation alias for this query."""
        count = self.generated_relation_counts.get(prefix, 0) + 1
        self.generated_relation_counts[prefix] = count
        if count == 1:
            return prefix
        return f"{prefix}_{count}"

    def inlined_cte_relation(self, relation: RelationSql) -> Optional[RelationSql]:
        """Return a relation aliased as its original CTE name when inlined."""
        if relation_is_aggregate_derived(relation):
            return None
        for cte_name in self.cte_join_names:
            base_tables = self.cte_base_tables.get(cte_name, set())
            if relation_uses_any_table(relation, base_tables):
                alias = self.cte_join_aliases.get(cte_name, cte_name)
                return alias_relation_as_cte(relation, alias)
        return None

    def alias_for_table(self, plan_table: str) -> str:
        """Return a display alias for a DuckDB plan table name."""
        table_name = plan_table.split(".")[-1]
        return self.table_to_alias.get(table_name, table_name)

    def scan_display(self, plan_table: str) -> tuple[str, str]:
        """Return display SQL and alias for the next scan of a plan table."""
        table_name = plan_table.split(".")[-1]
        alias = self.next_scan_alias(table_name)
        if alias != table_name:
            return f"{table_name} AS {alias}", alias
        return table_name, alias

    def next_scan_alias(self, table_name: str) -> str:
        """Return the original alias for the next scan of a table."""
        aliases = self.table_aliases.get(table_name, tuple())
        position = self.table_scan_counts.get(table_name, 0)
        self.table_scan_counts[table_name] = position + 1
        if position < len(aliases):
            return aliases[position]
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
            self._append_table_alias(table_name, alias)

    def _append_table_alias(self, table_name: str, alias: str) -> None:
        """Append one original table alias to scan-order metadata."""
        aliases = []
        for existing_alias in self.table_aliases.get(table_name, tuple()):
            aliases.append(existing_alias)
        aliases.append(alias)
        self.table_aliases[table_name] = tuple(aliases)

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
    supported_names = set(logical_names)
    registry = DuckDBNodeRegistry(
        frozenset(logical_names),
        frozenset(supported_names),
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
        "WINDOW",
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


def explain_optimized_json(connection: duckdb.DuckDBPyConnection, sql: str) -> PlanNode:
    """Run DuckDB optimized JSON EXPLAIN and return its root plan node."""
    connection.execute("SET explain_output = 'optimized_only'")
    row = connection.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()
    plan_items = json.loads(row[1])
    return parse_plan_node(plan_items[0])


def extract_optimized_json(database: str, setup_sql: str, sql: str) -> PlanNode:
    """Run the C++ ExtractPlan helper and return its enriched root node."""
    ensure_plan_dump_helper()
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        setup_path = temp_path / "setup.sql"
        sql_path = temp_path / "query.sql"
        setup_path.write_text(setup_sql)
        sql_path.write_text(sql)
        output = run_plan_dump_helper(database, setup_path, sql_path)
    plan_items = json.loads(output)
    return parse_plan_node(plan_items[0])


def ensure_plan_dump_helper() -> None:
    """Build the C++ ExtractPlan helper when the binary is missing."""
    if PLAN_DUMP_BINARY.exists():
        return
    compile_plan_dump_helper()


def compile_plan_dump_helper() -> None:
    """Compile the C++ ExtractPlan helper against the local DuckDB build."""
    command = plan_dump_compile_command()
    subprocess.run(command, check=True)


def plan_dump_compile_command() -> list[str]:
    """Return the compiler invocation for the local DuckDB helper."""
    include_paths = plan_dump_include_paths()
    command = ["g++", "-std=c++17", str(PLAN_DUMP_SOURCE)]
    for include_path in include_paths:
        command.extend(["-I", str(include_path)])
    command.extend(["-L", str(DUCKDB_LIBRARY.parent), "-lduckdb"])
    command.extend([f"-Wl,-rpath,{DUCKDB_LIBRARY.parent}", "-o", str(PLAN_DUMP_BINARY)])
    return command


def plan_dump_include_paths() -> tuple[Path, ...]:
    """Return DuckDB source include paths needed by the helper."""
    return (
        DUCKDB_SOURCE_ROOT / "src/include",
        DUCKDB_SOURCE_ROOT / "third_party/fmt/include",
        DUCKDB_SOURCE_ROOT / "third_party/utf8proc/include",
        DUCKDB_SOURCE_ROOT / "third_party/yyjson/include",
        DUCKDB_SOURCE_ROOT / "third_party/fastpforlib",
        DUCKDB_SOURCE_ROOT / "third_party/fast_float",
        DUCKDB_SOURCE_ROOT / "third_party/re2",
        DUCKDB_SOURCE_ROOT / "third_party/miniz",
        DUCKDB_SOURCE_ROOT / "third_party/concurrentqueue",
        DUCKDB_SOURCE_ROOT / "third_party/pcg",
    )


def run_plan_dump_helper(database: str, setup_path: Path, sql_path: Path) -> str:
    """Execute the C++ ExtractPlan helper and return stdout."""
    command = [
        str(PLAN_DUMP_BINARY),
        "--database",
        database,
        "--setup-file",
        str(setup_path),
        "--sql-file",
        str(sql_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise PlanReconstructionError(result.stderr.strip())
    return result.stdout


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
    return expand_cte_base_tables(cte_base_tables)


def expand_cte_base_tables(cte_base_tables: dict[str, set[str]]) -> dict[str, set[str]]:
    """Expand CTE-to-CTE references into physical base table names."""
    expanded = {}
    for cte_name in cte_base_tables:
        expanded[cte_name] = expand_one_cte_base_table(cte_name, cte_base_tables, set())
    return expanded


def expand_one_cte_base_table(
    cte_name: str, cte_base_tables: dict[str, set[str]], seen: set[str]
) -> set[str]:
    """Return physical base tables reachable from one CTE."""
    if cte_name in seen:
        return set()
    seen.add(cte_name)
    expanded = set()
    for table_name in cte_base_tables.get(cte_name, set()):
        if table_name in cte_base_tables:
            expanded.update(
                expand_one_cte_base_table(table_name, cte_base_tables, seen)
            )
        else:
            expanded.add(table_name)
    return expanded


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


def collect_cte_join_aliases(
    expression: sqlglot.expressions.Expression, cte_names: set[str]
) -> dict[str, str]:
    """Collect aliases used when CTEs appear as join relations."""
    aliases = {}
    for join in expression.find_all(sqlglot.expressions.Join):
        relation = join.this
        if isinstance(relation, sqlglot.expressions.Table):
            if relation.name in cte_names:
                aliases[relation.name] = relation.alias_or_name
    return aliases


def collect_root_subquery_view(
    expression: sqlglot.expressions.Expression,
) -> Optional[tuple[str, str]]:
    """Return a temporary-view display for a root FROM subquery."""
    from_expression = expression.args.get("from_")
    if not from_expression:
        return None
    relation = from_expression.this
    if not isinstance(relation, sqlglot.expressions.Subquery):
        return None
    view_name = relation.alias_or_name or "__subquery_1"
    return view_name, relation.this.sql(dialect="duckdb")


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


def add_dummy_columns_for_star_only_tables(tables: dict[str, AutoSchemaTable]) -> None:
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


def scalar_subquery_comparisons(
    expression: sqlglot.expressions.Expression,
) -> tuple[sqlglot.expressions.Expression, ...]:
    """Return comparisons that contain a scalar subquery operand."""
    comparisons = []
    for comparison in comparison_expressions(expression):
        if expression_contains_subquery(comparison.left):
            comparisons.append(comparison)
        elif expression_contains_subquery(comparison.right):
            comparisons.append(comparison)
    return tuple(comparisons)


def scalar_ordered_limit_projection_name(
    expression: sqlglot.expressions.Expression,
) -> Optional[str]:
    """Return projection name from a scalar ORDER BY LIMIT subquery."""
    for subquery in expression.find_all(sqlglot.expressions.Subquery):
        projection_name = ordered_limit_projection_name(subquery)
        if projection_name:
            return projection_name
    return None


def ordered_limit_projection_name(
    subquery: sqlglot.expressions.Subquery,
) -> Optional[str]:
    """Return one projection name when a subquery has ORDER BY and LIMIT."""
    select = subquery.this
    if not isinstance(select, sqlglot.expressions.Select):
        return None
    if not select.args.get("order") or not select.args.get("limit"):
        return None
    expressions = select.args.get("expressions") or []
    if len(expressions) != 1:
        return None
    return expressions[0].alias_or_name


def expression_contains_subquery(expression: sqlglot.expressions.Expression) -> bool:
    """Return true when an expression is or contains a subquery."""
    if isinstance(expression, sqlglot.expressions.Subquery):
        return True
    return expression.find(sqlglot.expressions.Subquery) is not None


def scalar_subquery_comparison_sql(
    comparison: sqlglot.expressions.Expression,
) -> Optional[str]:
    """Return SQL for a scalar subquery comparison after decorrelation."""
    operator = comparison_sql_operator(comparison)
    if not operator:
        return None
    left = scalar_operand_sql(comparison.left)
    right = scalar_operand_sql(comparison.right)
    if not left or not right:
        return None
    return f"{left} {operator} {right}"


def comparison_sql_operator(
    comparison: sqlglot.expressions.Expression,
) -> Optional[str]:
    """Return the SQL operator for a comparison expression."""
    operator_by_type = {
        sqlglot.expressions.EQ: "=",
        sqlglot.expressions.NEQ: "!=",
        sqlglot.expressions.GT: ">",
        sqlglot.expressions.GTE: ">=",
        sqlglot.expressions.LT: "<",
        sqlglot.expressions.LTE: "<=",
    }
    for expression_type, operator in operator_by_type.items():
        if isinstance(comparison, expression_type):
            return operator
    return None


def scalar_operand_sql(expression: sqlglot.expressions.Expression) -> Optional[str]:
    """Return SQL for a scalar comparison operand."""
    subquery = expression.find(sqlglot.expressions.Subquery)
    if isinstance(expression, sqlglot.expressions.Subquery):
        subquery = expression
    if subquery:
        return scalar_subquery_projection_sql(subquery)
    return expression.sql(dialect="duckdb")


def scalar_subquery_projection_sql(
    subquery: sqlglot.expressions.Subquery,
) -> Optional[str]:
    """Return the selected scalar expression from a subquery."""
    select = subquery.this
    if not isinstance(select, sqlglot.expressions.Select):
        return None
    expressions = select.args.get("expressions") or []
    if len(expressions) != 1:
        return None
    if select.args.get("order") and select.args.get("limit"):
        return f"__duckdb_agg.{expressions[0].alias_or_name}"
    return qualify_scalar_subquery_projection(expressions[0], select)


def scalar_subquery_output_name(
    subquery: sqlglot.expressions.Subquery,
) -> Optional[str]:
    """Return the output name selected by a scalar subquery."""
    select = subquery.this
    if not isinstance(select, sqlglot.expressions.Select):
        return None
    expressions = select.args.get("expressions") or []
    if len(expressions) != 1:
        return None
    return expressions[0].alias_or_name


def qualify_scalar_subquery_projection(
    expression: sqlglot.expressions.Expression,
    select: sqlglot.expressions.Select,
) -> str:
    """Qualify an unqualified scalar subquery projection with its table alias."""
    if not isinstance(expression, sqlglot.expressions.Column):
        return expression.sql(dialect="duckdb")
    if expression.table:
        return expression.sql(dialect="duckdb")
    table = select.find(sqlglot.expressions.Table)
    if not table:
        return expression.sql(dialect="duckdb")
    return f"{table.alias_or_name}.{expression.name}"


def scalar_projection_with_alias(
    expression: sqlglot.expressions.Expression, projected_sql: str
) -> str:
    """Return scalar projection SQL with the original output alias."""
    alias = expression.alias
    if not alias:
        return projected_sql
    if alias == projected_sql:
        return projected_sql
    return f"{projected_sql} AS {alias}"


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
    root: Optional[PlanNode] = None,
) -> str:
    """Return reconstructed SQL and optional verbose reconstruction details."""
    registry = read_logical_node_registry()
    if auto_schema:
        apply_auto_schema(connection, sql)
    surface = SqlSurface(sql, connection)
    if not root:
        root = root_plan_for_surface(connection, sql, surface)
    validate_tree(root, registry)
    root_subquery_relation = reconstruct_root_subquery(root, surface, registry)
    if root_subquery_relation:
        return format_reconstruction(
            root, root_subquery_relation, include_raw_json, quiet, surface
        )
    relation = reconstruct_relation(root, surface, registry)
    return format_reconstruction(root, relation, include_raw_json, quiet, surface)


def reject_unreconstructable_root_subquery(surface: SqlSurface) -> None:
    """Reject root FROM subqueries when DuckDB JSON drops their expressions."""
    if surface.root_subquery_view:
        raise PlanReconstructionError(
            "Cannot reconstruct root FROM subquery from DuckDB optimized JSON: "
            "EXPLAIN exposes only projected output names, not the inner "
            "projection expressions."
        )


def root_plan_for_surface(
    connection: duckdb.DuckDBPyConnection, sql: str, surface: SqlSurface
) -> PlanNode:
    """Return the best available optimized plan for one parsed SQL surface."""
    if surface.root_subquery_view:
        setup_sql = schema_setup_sql(connection, surface)
        return extract_optimized_json(":memory:", setup_sql, sql)
    return explain_optimized_json(connection, sql)


def schema_setup_sql(connection: duckdb.DuckDBPyConnection, surface: SqlSurface) -> str:
    """Return CREATE TABLE statements for tables used by the current query."""
    statements = []
    for table_name in surface.table_to_alias:
        statements.append(create_table_sql_from_catalog(connection, table_name))
    return "\n".join(statements)


def create_table_sql_from_catalog(
    connection: duckdb.DuckDBPyConnection, table_name: str
) -> str:
    """Return a CREATE TABLE statement for one DuckDB catalog table."""
    rows = connection.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    column_sql = catalog_column_sql(rows)
    return f"CREATE TABLE {quote_identifier(table_name)} ({column_sql});"


def catalog_column_sql(rows: list[Any]) -> str:
    """Return column definitions from PRAGMA table_info rows."""
    column_parts = []
    for row in rows:
        column_name = quote_identifier(str(row[1]))
        column_type = str(row[2])
        column_parts.append(f"{column_name} {column_type}")
    return ", ".join(column_parts)


def reconstruct_root_subquery(
    root: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> Optional[RelationSql]:
    """Reconstruct a root derived-table projection using enriched plan data."""
    if not surface.root_subquery_view:
        return None
    inner_projection = root_derived_inner_projection(root)
    if not inner_projection:
        raise PlanReconstructionError("Root FROM subquery has no inner projection")
    child = only_child(inner_projection)
    relation = reconstruct_relation(child, surface, registry)
    projection_sql = derived_projection_sql(root, inner_projection)
    mapping = relation.mapping + (
        "ROOT_SUBQUERY -> collapsed using ExtractPlan projection expressions",
    )
    return RelationSql(
        relation.from_sql,
        relation.filters,
        relation.aliases,
        mapping,
        relation.output_columns,
        relation.prelude_sql,
        projection_sql,
    )


def root_derived_inner_projection(root: PlanNode) -> Optional[PlanNode]:
    """Return the inner projection node for a root derived table."""
    if root.name != "PROJECTION":
        return None
    child = only_child(root)
    if child.name != "PROJECTION":
        return None
    return child


def derived_projection_sql(root: PlanNode, inner_projection: PlanNode) -> str:
    """Return SELECT text for a collapsed root derived-table projection."""
    root_expressions = normalize_filters(root.extra_info.get("Expressions", ""))
    inner_names = normalize_filters(
        inner_projection.extra_info.get("Expression Names", "")
    )
    inner_expressions = normalize_filters(
        inner_projection.extra_info.get("Expressions", "")
    )
    if tuple(root_expressions) != tuple(inner_names):
        raise PlanReconstructionError(
            "Root FROM subquery projection is not a simple alias projection"
        )
    return ", ".join(aliased_projection_parts(inner_expressions, inner_names))


def aliased_projection_parts(
    expressions: tuple[str, ...], names: tuple[str, ...]
) -> tuple[str, ...]:
    """Return projection expressions with output aliases preserved."""
    if len(expressions) != len(names):
        raise PlanReconstructionError("Projection expression/name count mismatch")
    parts = []
    for expression, name in zip(expressions, names):
        parts.append(aliased_projection_part(expression, name))
    return tuple(parts)


def aliased_projection_part(expression: str, name: str) -> str:
    """Return one projection expression with an alias when needed."""
    if not name or expression == name:
        return expression
    return f"{expression} AS {name}"


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
        "WINDOW": reconstruct_window,
        "DISTINCT": reconstruct_unary_passthrough,
        "LIMIT": reconstruct_unary_passthrough,
        "ORDER_BY": reconstruct_unary_passthrough,
        "TOP_N": reconstruct_unary_passthrough,
        "EMPTY_RESULT": reconstruct_empty_result,
        "DUMMY_SCAN": reconstruct_dummy_scan,
        "DELIM_GET": reconstruct_delim_get,
        "UNION": reconstruct_set_operation,
        "EXCEPT": reconstruct_set_operation,
        "INTERSECT": reconstruct_set_operation,
    }


def reconstruct_scan(node: PlanNode, surface: SqlSurface) -> RelationSql:
    """Reconstruct a base scan and pushed filters."""
    plan_table = str(node.extra_info.get("Table", node.name))
    table_name = plan_table.split(".")[-1]
    display_table, alias = surface.scan_display(plan_table)
    filters = normalize_filters(node.extra_info.get("Filters", ""))
    qualified_filters = qualify_filters(filters, surface, (alias,))
    mapping = (f"{node.name} -> FROM {display_table}",)
    return RelationSql(
        display_table,
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
    projection_sql = projection_sql_for_unary_node(node, relation)
    output_columns = output_columns_for_unary_node(node, relation)
    return RelationSql(
        relation.from_sql,
        relation.filters,
        relation.aliases,
        mapping,
        output_columns,
        relation.prelude_sql,
        projection_sql or relation.projection_sql,
        relation.complete_sql,
    )


def output_columns_for_unary_node(node: PlanNode, relation: RelationSql) -> tuple[str, ...]:
    """Return output columns for a unary logical node."""
    if node.name != "PROJECTION":
        return relation.output_columns
    projections = normalize_plan_terms(node.extra_info.get("Expressions", ""))
    if not projections:
        return relation.output_columns
    return projection_output_columns(projections, relation)


def projection_output_columns(
    projections: tuple[str, ...], relation: RelationSql
) -> tuple[str, ...]:
    """Return output column names for a projection node."""
    output_columns = []
    for index, projection in enumerate(projections):
        output_columns.append(projection_output_column(projection, index, relation))
    return tuple(output_columns)


def projection_output_column(
    projection: str, index: int, relation: RelationSql
) -> str:
    """Return one projection output column name."""
    slot_column = resolve_slot_reference(projection, relation)
    if slot_column:
        return slot_column.split(".")[-1]
    if index < len(relation.output_columns):
        return output_column_for_projection(projection, index, relation)
    return projection.split(".")[-1]


def projection_sql_for_unary_node(
    node: PlanNode, relation: RelationSql
) -> Optional[str]:
    """Return projection SQL when a node exposes materialized output columns."""
    if node.name != "PROJECTION":
        return None
    if not relation_is_prelude_aggregate(relation):
        return None
    projections = normalize_filters(node.extra_info.get("Expressions", ""))
    return projection_sql_from_relation_outputs(projections, relation)


def projection_sql_from_relation_outputs(
    projections: tuple[str, ...], relation: RelationSql
) -> Optional[str]:
    """Map projection expressions to columns of a materialized relation."""
    if not projections:
        return None
    if len(projections) > len(relation.output_columns):
        return None
    parts = []
    for index, projection in enumerate(projections):
        column_name = output_column_for_projection(projection, index, relation)
        parts.append(f"{relation.aliases[0]}.{column_name}")
    return ", ".join(parts)


def output_column_for_projection(
    projection: str, index: int, relation: RelationSql
) -> str:
    """Return the materialized output column for one projection expression."""
    aggregate_name = safe_output_name(projection, "agg")
    if aggregate_name in relation.output_columns:
        return aggregate_name
    group_name = safe_output_name(projection, "group")
    if group_name in relation.output_columns:
        return group_name
    return relation.output_columns[index]


def reconstruct_filter(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a filter node by adding its expressions as predicates."""
    child = only_child(node)
    relation = reconstruct_relation(child, surface, registry)
    filters = normalize_filters(node.extra_info.get("Expressions", ""))
    filters = replace_scalar_subquery_filters(filters, surface, relation)
    if relation_is_aggregate_derived(relation):
        return add_having_to_aggregate_relation(relation, filters)
    qualified = qualify_filters(filters, surface, relation.aliases)
    qualified = qualify_relation_output_filters(qualified, relation)
    mapping = relation.mapping + (f"FILTER -> WHERE {' AND '.join(qualified)}",)
    return RelationSql(
        relation.from_sql,
        relation.filters + qualified,
        relation.aliases,
        mapping,
        relation.output_columns,
        relation.prelude_sql,
    )


def qualify_relation_output_filters(
    filters: tuple[str, ...], relation: RelationSql
) -> tuple[str, ...]:
    """Qualify filter terms that reference materialized relation outputs."""
    if not relation.aliases:
        return filters
    qualified_filters = []
    for filter_text in filters:
        qualified_filters.append(qualify_relation_output_filter(filter_text, relation))
    return tuple(qualified_filters)


def qualify_relation_output_filter(filter_text: str, relation: RelationSql) -> str:
    """Qualify one filter against materialized relation output columns."""
    result = filter_text
    alias = relation.aliases[-1]
    for column_name in sorted(relation.output_columns, key=len, reverse=True):
        result = replace_unqualified_identifier(
            result, column_name, f"{alias}.{column_name}"
        )
    return result


def replace_scalar_subquery_filters(
    filters: tuple[str, ...], surface: SqlSurface, relation: RelationSql
) -> tuple[str, ...]:
    """Replace DuckDB SUBQUERY placeholders with joined relation columns."""
    if not filter_contains_subquery_placeholder(filters):
        return filters
    replacement = scalar_subquery_relation_reference(surface, relation)
    replaced_filters = []
    for filter_text in filters:
        replaced_filters.append(filter_text.replace("SUBQUERY", replacement))
    return tuple(replaced_filters)


def scalar_subquery_relation_reference(
    surface: SqlSurface, relation: RelationSql
) -> str:
    """Return the relation column that represents a scalar subquery value."""
    output_name = surface.scalar_subquery_output_name()
    if not output_name:
        raise PlanReconstructionError(
            "Cannot reconstruct scalar subquery filter from DuckDB optimized JSON"
        )
    for alias in reversed(relation.aliases):
        if relation_has_output_column(relation, alias, output_name):
            return f"{alias}.{output_name}"
    raise PlanReconstructionError(
        "Cannot map scalar subquery output to reconstructed relation"
    )


def relation_has_output_column(
    relation: RelationSql, alias: str, output_name: str
) -> bool:
    """Return true when a relation alias exposes an output column."""
    if alias == relation.aliases[-1] and output_name in relation.output_columns:
        return True
    return False


def filter_contains_subquery_placeholder(filters: tuple[str, ...]) -> bool:
    """Return true when a filter contains DuckDB's SUBQUERY placeholder."""
    for filter_text in filters:
        if "SUBQUERY" in filter_text:
            return True
    return False


def relation_is_aggregate_derived(relation: RelationSql) -> bool:
    """Return true when a relation was produced from an aggregate node."""
    if relation_is_prelude_aggregate(relation):
        return True
    if not relation.from_sql.startswith("("):
        return False
    return ") AS " in relation.from_sql


def add_having_to_aggregate_relation(
    relation: RelationSql, filters: tuple[str, ...]
) -> RelationSql:
    """Fold a filter above an aggregate node into the derived table HAVING."""
    if not filters:
        return relation
    having_sql = " AND ".join(filters)
    from_sql = relation.from_sql
    prelude_sql = relation.prelude_sql
    if relation_is_prelude_aggregate(relation):
        prelude_sql = add_having_to_last_prelude(prelude_sql, having_sql)
    else:
        from_sql = add_having_before_relation_alias(relation.from_sql, having_sql)
    mapping = relation.mapping + (f"FILTER -> HAVING {having_sql}",)
    return RelationSql(
        from_sql,
        relation.filters,
        relation.aliases,
        mapping,
        relation.output_columns,
        prelude_sql,
    )


def relation_is_prelude_aggregate(relation: RelationSql) -> bool:
    """Return true when an aggregate relation is displayed as a prelude view."""
    if not relation.prelude_sql:
        return False
    if relation.from_sql not in relation.aliases:
        return False
    for mapping_line in relation.mapping:
        if mapping_line.startswith("AGGREGATE "):
            return True
    return False


def add_having_to_last_prelude(
    prelude_sql: tuple[str, ...], having_sql: str
) -> tuple[str, ...]:
    """Add HAVING to the final prelude CREATE VIEW statement."""
    if not prelude_sql:
        raise PlanReconstructionError("Aggregate relation has no prelude SQL")
    updated_prelude = []
    for prelude in prelude_sql[:-1]:
        updated_prelude.append(prelude)
    updated_prelude.append(add_having_before_semicolon(prelude_sql[-1], having_sql))
    return tuple(updated_prelude)


def add_having_before_semicolon(sql: str, having_sql: str) -> str:
    """Insert HAVING before the semicolon of a CREATE VIEW statement."""
    stripped_sql = sql.rstrip()
    if not stripped_sql.endswith(";"):
        raise PlanReconstructionError("Prelude aggregate SQL must end with semicolon")
    return f"{stripped_sql[:-1]}\nHAVING {having_sql};"


def add_having_before_relation_alias(from_sql: str, having_sql: str) -> str:
    """Insert HAVING before the outer alias of a derived aggregate relation."""
    marker = ") AS "
    if marker not in from_sql:
        raise PlanReconstructionError("Aggregate relation has no outer alias")
    query_sql, alias_sql = from_sql.rsplit(marker, 1)
    return f"{query_sql}\nHAVING {having_sql}) AS {alias_sql}"


def reconstruct_window(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a logical window node as a temporary view."""
    child = only_child(node)
    child_relation = reconstruct_relation(child, surface, registry)
    window_expressions = normalize_plan_terms(node.extra_info.get("Expressions", ""))
    output_columns = window_output_columns(child_relation, window_expressions)
    select_parts = window_select_parts(child_relation, window_expressions)
    window_sql = window_relation_sql(child_relation, select_parts)
    relation_alias = surface.generated_relation_alias("__duckdb_window")
    prelude_sql = child_relation.prelude_sql + (
        f"CREATE TEMPORARY VIEW {relation_alias} AS\n{window_sql};",
    )
    mapping = child_relation.mapping + (f"WINDOW -> temporary view {relation_alias}",)
    return RelationSql(
        relation_alias,
        tuple(),
        (relation_alias,),
        mapping,
        output_columns,
        prelude_sql,
    )


def window_output_columns(
    child_relation: RelationSql, window_expressions: tuple[str, ...]
) -> tuple[str, ...]:
    """Return output columns for a window relation."""
    output_columns = []
    for column_name in child_relation.output_columns:
        output_columns.append(column_name)
    for expression in window_expressions:
        output_columns.append(window_expression_alias(expression))
    return tuple(output_columns)


def window_expression_alias(expression: str) -> str:
    """Return the exposed alias for a DuckDB window expression."""
    if "ROW_NUMBER()" in expression:
        return "limit_rownum"
    return safe_output_name(expression, "window")


def window_select_parts(
    child_relation: RelationSql, window_expressions: tuple[str, ...]
) -> tuple[str, ...]:
    """Return SELECT expressions for a window temporary view."""
    select_parts = []
    for column_name in child_relation.output_columns:
        select_parts.append(column_name)
    for expression in window_expressions:
        alias = window_expression_alias(expression)
        select_parts.append(f"{expression} AS {alias}")
    return tuple(select_parts)


def window_relation_sql(
    child_relation: RelationSql, select_parts: tuple[str, ...]
) -> str:
    """Format SQL for a window temporary view."""
    lines = [f"SELECT {', '.join(select_parts)}", f"FROM {child_relation.from_sql}"]
    if child_relation.filters:
        lines.append("WHERE " + " AND ".join(child_relation.filters))
    return "\n".join(lines)


def reconstruct_aggregate(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct an aggregate node as a derived relation."""
    child = only_child(node)
    child_relation = reconstruct_relation(child, surface, registry)
    raw_groups = normalize_plan_terms(node.extra_info.get("Groups", ""))
    raw_aggregates = normalize_plan_terms(node.extra_info.get("Expressions", ""))
    alias, output_names = surface.aggregate_alias_for_relation(child_relation)
    if not output_names:
        output_names = surface.scalar_aggregate_output_names(raw_groups, raw_aggregates)
    groups = resolve_aggregate_terms(raw_groups, child_relation)
    aggregates = resolve_aggregate_terms(raw_aggregates, child_relation)
    output_names = aggregate_safe_output_names(groups, aggregates, output_names)
    select_parts = aggregate_select_parts(groups, aggregates, output_names)
    output_columns = aggregate_output_columns(groups, aggregates, output_names)
    from_sql = aggregate_from_sql(child_relation, select_parts, groups)
    relation_alias = alias or surface.generated_relation_alias("__duckdb_agg")
    mapping = child_relation.mapping + (f"AGGREGATE -> derived table {relation_alias}",)
    return aggregate_prelude_relation(
        child_relation, from_sql, relation_alias, mapping, output_columns
    )


def aggregate_prelude_relation(
    child_relation: RelationSql,
    aggregate_sql: str,
    relation_alias: str,
    mapping: tuple[str, ...],
    output_columns: tuple[str, ...],
) -> RelationSql:
    """Return a named aggregate relation displayed as a temporary view."""
    prelude_sql = child_relation.prelude_sql + (
        f"CREATE TEMPORARY VIEW {relation_alias} AS\n{aggregate_sql};",
    )
    return RelationSql(
        relation_alias,
        tuple(),
        (relation_alias,),
        mapping + (f"AGGREGATE {relation_alias} -> temporary view",),
        output_columns,
        prelude_sql,
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


def aggregate_safe_output_names(
    groups: tuple[str, ...],
    aggregates: tuple[str, ...],
    output_names: tuple[str, ...],
) -> tuple[str, ...]:
    """Return safe output names for aggregate relation columns."""
    if output_names:
        return output_names
    names = []
    for group in groups:
        names.append(safe_output_name(group, "group"))
    for aggregate in aggregates:
        names.append(safe_output_name(aggregate, "agg"))
    return tuple(names)


def safe_output_name(expression: str, prefix: str) -> str:
    """Return expression as a column name or a generated safe alias."""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", expression):
        return expression
    digest = hashlib.sha1(expression.encode("utf-8")).hexdigest()[:8]
    return f"__duckdb_{prefix}_{digest}"


def resolve_aggregate_terms(
    terms: tuple[str, ...], child_relation: RelationSql
) -> tuple[str, ...]:
    """Resolve DuckDB slot references inside aggregate expressions."""
    resolved_terms = []
    for term in terms:
        resolved_terms.append(resolve_aggregate_term(term, child_relation))
    return tuple(resolved_terms)


def resolve_aggregate_term(term: str, child_relation: RelationSql) -> str:
    """Resolve one aggregate term against its child relation."""
    if term == "SUBQUERY":
        return resolve_subquery_group_term(child_relation)
    return resolve_slot_references_in_expression(term, child_relation)


def resolve_subquery_group_term(child_relation: RelationSql) -> str:
    """Resolve DuckDB's SUBQUERY group placeholder to a child output column."""
    if not child_relation.aliases:
        raise PlanReconstructionError("SUBQUERY group has no child relation alias")
    if not child_relation.output_columns:
        raise PlanReconstructionError("SUBQUERY group has no child output column")
    return f"{child_relation.aliases[-1]}.{child_relation.output_columns[0]}"


def resolve_slot_references_in_expression(
    expression: str, relation: RelationSql
) -> str:
    """Replace all #N references in one expression with relation columns."""
    result = expression
    for slot_index in range(len(relation.output_columns)):
        replacement = resolve_slot_reference(f"#{slot_index}", relation)
        if replacement:
            result = result.replace(f"#{slot_index}", replacement)
    return result


def aggregate_select_parts(
    groups: tuple[str, ...],
    aggregates: tuple[str, ...],
    output_names: tuple[str, ...],
) -> tuple[str, ...]:
    """Return SELECT expressions for an aggregate derived relation."""
    select_parts = []
    for group in groups:
        select_parts.append(group_with_output_alias(group, select_parts, output_names))
    for aggregate in aggregates:
        select_parts.append(
            aggregate_with_output_alias(aggregate, select_parts, output_names)
        )
    return tuple(select_parts)


def group_with_output_alias(
    group: str, select_parts: list[str], output_names: tuple[str, ...]
) -> str:
    """Attach an output alias to a group expression when needed."""
    output_index = len(select_parts)
    if output_index >= len(output_names):
        return group
    output_name = output_names[output_index]
    if not output_name or output_name == group:
        return group
    return f"{group} AS {output_name}"


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
    mapping = (
        left_relation.mapping
        + right_relation.mapping
        + ("CROSS_PRODUCT -> CROSS JOIN",)
    )
    prelude_sql = left_relation.prelude_sql + right_relation.prelude_sql
    return RelationSql(
        from_sql,
        filters,
        aliases,
        mapping,
        right_relation.output_columns,
        prelude_sql,
    )


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
    prelude_sql = left_relation.prelude_sql + right_relation.prelude_sql
    return RelationSql(
        from_sql,
        filters,
        aliases,
        mapping,
        right_relation.output_columns,
        prelude_sql,
    )


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
    prelude_sql = relation.prelude_sql + (
        f"CREATE TEMPORARY VIEW {cte_name} AS SELECT * FROM {relation.from_sql};",
    )
    mapping = relation.mapping + (
        f"CTE {cte_name} -> displayed as temporary view over optimized relation",
    )
    return RelationSql(
        cte_name,
        relation.filters,
        (cte_name,),
        mapping,
        relation.output_columns,
        prelude_sql,
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
        if join_type != "INNER":
            raise PlanReconstructionError(
                f"Cannot reconstruct {join_type} JOIN with EMPTY_RESULT left side"
            )
        return empty_join_result(node, left_relation, right_relation)
    if not relation_is_empty(right_relation):
        return None
    if join_type == "ANTI":
        mapping = left_relation.mapping + right_relation.mapping
        return RelationSql(
            left_relation.from_sql,
            left_relation.filters,
            left_relation.aliases,
            mapping,
        )
    if join_type == "SEMI":
        return left_relation_with_false_filter(node, left_relation, right_relation)
    if join_type == "INNER":
        return empty_join_result(node, left_relation, right_relation)
    raise PlanReconstructionError(
        f"Cannot reconstruct {join_type} JOIN with EMPTY_RESULT right side"
    )


def relation_is_empty(relation: RelationSql) -> bool:
    """Return true when a relation represents DuckDB EMPTY_RESULT."""
    return relation.from_sql == empty_result_sql()


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
    return RelationSql(empty_result_sql(), tuple(), tuple(), mapping)


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
    return RelationSql(
        empty_result_sql(), tuple(), tuple(), (f"{node.name} -> empty result",)
    )


def empty_result_sql() -> str:
    """Return a valid SQL relation that produces no rows."""
    return "(SELECT * FROM (VALUES (1)) AS __empty(_empty) WHERE FALSE)"


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


def reconstruct_set_operation(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> RelationSql:
    """Reconstruct a UNION, EXCEPT, or INTERSECT relation."""
    if len(node.children) < 2:
        raise PlanReconstructionError(f"{node.name} expected at least two children")
    branch_sql = []
    mapping = []
    for child in node.children:
        branch_text, branch_relation = reconstruct_set_branch(child, surface, registry)
        branch_sql.append(branch_text)
        mapping.extend(branch_relation.mapping)
    operator = set_operation_keyword(node, surface)
    complete_sql = f"\n{operator}\n".join(branch_sql)
    mapping.append(f"{node.name} -> {operator}")
    return RelationSql(
        "",
        tuple(),
        tuple(),
        tuple(mapping),
        tuple(),
        tuple(),
        None,
        complete_sql,
    )


def reconstruct_set_branch(
    node: PlanNode, surface: SqlSurface, registry: DuckDBNodeRegistry
) -> tuple[str, RelationSql]:
    """Return SELECT SQL and reconstructed relation for one set-operation branch."""
    projection_node = set_branch_projection_node(node)
    relation_node = only_child(projection_node)
    relation = reconstruct_relation(relation_node, surface, registry)
    projections = normalize_filters(projection_node.extra_info.get("Expressions", ""))
    if not projections:
        raise PlanReconstructionError("Set operation branch has no projections")
    lines = [f"SELECT {', '.join(projections)}"]
    if relation.from_sql:
        lines.append(f"FROM {relation.from_sql}")
    if relation.filters:
        lines.append("WHERE " + " AND ".join(relation.filters))
    return "\n".join(lines), relation


def set_branch_projection_node(node: PlanNode) -> PlanNode:
    """Return the projection node that defines a set-operation branch output."""
    if node.name != "PROJECTION":
        raise PlanReconstructionError(
            f"Set operation branch expected PROJECTION, got {node.name}"
        )
    return node


def set_operation_keyword(node: PlanNode, surface: SqlSurface) -> str:
    """Return the SQL keyword for a set operation."""
    sql_lower = surface.sql.lower()
    if node.name == "UNION" and re.search(r"\bunion\s+all\b", sql_lower):
        return "UNION ALL"
    return node.name


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
    if inner_condition and should_use_inner_delim_condition(node):
        return qualify_binary_condition(
            inner_condition,
            right_relation.aliases,
            left_relation.aliases,
            surface,
        )
    condition = first_extra_value(node.extra_info.get("Conditions", ""))
    return qualify_join_condition(condition, left_relation, right_relation, surface)


def should_use_inner_delim_condition(node: PlanNode) -> bool:
    """Return true when a DELIM_JOIN should show the nested join condition."""
    if node.name != "DELIM_JOIN":
        return False
    join_type = str(node.extra_info.get("Join Type", "")).upper()
    return join_type != "SINGLE"


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
    output_column = resolve_output_column(term, relation)
    if output_column:
        return output_column
    return surface.qualify_column(term, relation.aliases)


def resolve_output_column(term: str, relation: RelationSql) -> Optional[str]:
    """Resolve a named term against relation output columns."""
    if term not in relation.output_columns:
        return None
    if not relation.aliases:
        return term
    return f"{relation.aliases[0]}.{term}"


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
        return qualify_expression(
            clean_condition, surface, left_aliases + right_aliases
        )
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
        "SINGLE": "JOIN",
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


def normalize_plan_terms(raw_terms: Any) -> tuple[str, ...]:
    """Normalize DuckDB non-filter extra_info into expression terms."""
    if not raw_terms:
        return tuple()
    if isinstance(raw_terms, list):
        return normalize_plan_term_list(raw_terms)
    return normalize_plan_term_text(str(raw_terms))


def normalize_plan_term_list(raw_terms: list[Any]) -> tuple[str, ...]:
    """Normalize a list of DuckDB expression terms."""
    terms = []
    for raw_term in raw_terms:
        terms.extend(normalize_plan_term_text(str(raw_term)))
    return tuple(terms)


def normalize_plan_term_text(raw_terms: str) -> tuple[str, ...]:
    """Normalize newline-separated DuckDB expression terms."""
    terms = []
    for term_text in raw_terms.splitlines():
        stripped_term = strip_wrapping_parentheses(term_text)
        if stripped_term:
            terms.append(stripped_term)
    return tuple(terms)


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
    lines = []
    for prelude_sql in relation.prelude_sql:
        lines.append(prelude_sql)
    if relation.prelude_sql:
        lines.append("")
    if relation.complete_sql:
        lines.append(relation.complete_sql)
        return "\n".join(lines) + ";"
    from_sql = relation.from_sql
    projection_sql = relation.projection_sql or surface.final_projection_sql(relation)
    lines.append(f"SELECT {projection_sql}")
    if from_sql:
        lines.append(f"FROM {from_sql}")
    if relation.filters:
        lines.append("WHERE " + " AND ".join(relation.filters))
    order_sql = surface.order_sql()
    if order_sql:
        lines.append(order_sql)
    limit_sql = surface.limit_sql()
    if limit_sql:
        lines.append(limit_sql)
    return "\n".join(lines) + ";"


def format_reconstruction(
    root: PlanNode,
    relation: RelationSql,
    include_raw_json: bool,
    quiet: bool,
    surface: SqlSurface,
) -> str:
    """Format quiet or verbose reconstruction output."""
    sql_text = format_select_sql(surface, relation)
    if quiet:
        return sql_text
    return format_verbose_output(root, relation, sql_text, include_raw_json)


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
        reconstruct_sql(
            connection,
            query_sql,
            include_raw_json,
            quiet,
            auto_schema,
        )
    )


@cli.command()
@click.option("--database", default=":memory:", show_default=True)
@click.option("--setup", default="")
@click.option("--setup-file")
@click.option("--json", "include_raw_json", is_flag=True)
@click.option("--quiet", is_flag=True)
@click.option("--auto-schema", is_flag=True)
@click.option(
    "--history-file",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Path for persistent REPL history.",
)
def repl(
    database: str,
    setup: str,
    setup_file: Optional[str],
    include_raw_json: bool,
    quiet: bool,
    auto_schema: bool,
    history_file: Optional[Path],
) -> None:
    """Start a multiline REPL for optimized SQL reconstruction."""
    connection = duckdb.connect(database)
    setup_sql = combine_setup_sql(setup, setup_file)
    apply_setup_sql(connection, setup_sql)
    session = create_repl_session(history_file)
    run_repl(session, connection, include_raw_json, quiet, auto_schema)


def create_repl_session(history_file: Optional[Path] = None) -> PromptSession:
    """Create a prompt-toolkit session with persistent history."""
    resolved_history_file = history_file or default_repl_history_file()
    ensure_history_directory(resolved_history_file)
    return PromptSession(history=FileHistory(str(resolved_history_file)))


def default_repl_history_file() -> Path:
    """Return the default persistent REPL history file path."""
    return Path.home() / ".duckdb_reconstruct_history"


def ensure_history_directory(history_file: Path) -> None:
    """Create the parent directory for the REPL history file."""
    history_file.parent.mkdir(parents=True, exist_ok=True)


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
        click.secho(f"ERROR: {error}", fg="red", err=True, color=True)
        return
    click.echo(color_repl_output(output, quiet, include_raw_json), color=True)


def color_repl_output(output: str, quiet: bool, include_raw_json: bool) -> str:
    """Return colored REPL output while preserving render command output."""
    if quiet:
        return color_sql(output)
    return color_verbose_repl_output(output, include_raw_json)


def color_verbose_repl_output(output: str, include_raw_json: bool) -> str:
    """Color verbose REPL output sections."""
    lines = output.splitlines()
    colored_lines = []
    section = "mapping"
    for line in lines:
        section = next_color_section(line, section, include_raw_json)
        colored_lines.append(color_verbose_line(line, section))
    return "\n".join(colored_lines)


def next_color_section(line: str, section: str, include_raw_json: bool) -> str:
    """Return the current output section for color decisions."""
    if line == "Reconstructed SQL:":
        return "sql_header"
    if include_raw_json and line == "Optimized JSON:":
        return "json_header"
    if section == "sql_header":
        return "sql"
    if section == "json_header":
        return "json"
    return section


def color_verbose_line(line: str, section: str) -> str:
    """Color one verbose output line."""
    if line in ("Reconstruction mapping:", "Reconstructed SQL:", "Optimized JSON:"):
        return click.style(line, fg="cyan", bold=True)
    if section == "mapping" and line.startswith("- "):
        return click.style(line, fg="bright_black")
    if section == "sql":
        return color_sql(line)
    if section == "json":
        return color_json(line)
    return line


def color_sql(sql: str) -> str:
    """Color SQL text for terminal output."""
    return highlight(sql, SqlLexer(), TerminalFormatter()).rstrip("\n")


def color_json(json_text: str) -> str:
    """Color JSON text for terminal output."""
    return highlight(json_text, JsonLexer(), TerminalFormatter()).rstrip("\n")


if __name__ == "__main__":
    cli()
