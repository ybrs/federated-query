"""Query preprocessing stage that expands SELECT * projections."""

from __future__ import annotations

from typing import List, Optional, Tuple, Union

import pyarrow as pa
import sqlglot
from sqlglot import exp

from ..model import StateModel

from ..catalog import Catalog
from ..catalog.schema import Table
from ..parser.dialect import FedQPostgres
from ..parser.errors import UnsupportedSQLError
from .query_context import ColumnMapping, QueryContext
from .query_executor import QueryExecutor, QueryProcessor


class StarExpansionError(Exception):
    """Raised when SELECT * cannot be expanded."""

    pass


class _SelectSource(StateModel):
    """Metadata for a table referenced in a SELECT."""

    datasource: str
    schema_name: str
    table_name: str
    alias: Optional[str]
    sql_qualifier: str
    internal_prefix: str
    columns: List[str]

    def has_column(self, column: str) -> bool:
        """Check if this table defines a column."""
        for existing in self.columns:
            if existing == column:
                return True
        return False

    def matches(self, identifier: str) -> bool:
        """Check if identifier refers to this source."""
        candidates: List[str] = []
        if self.alias:
            candidates.append(self.alias)
        candidates.append(self.table_name)
        qualified = f"{self.schema_name}.{self.table_name}"
        candidates.append(qualified)
        candidates.append(self.sql_qualifier)
        for value in candidates:
            if identifier == value:
                return True
        return False


class QueryPreprocessor:
    """Expands SELECT * and captures column metadata."""

    def __init__(self, catalog: Catalog, dialect=FedQPostgres):
        """Initialize with catalog metadata and SQL dialect."""
        self.catalog = catalog
        self.dialect = dialect

    def preprocess(self, sql: str, context: QueryContext) -> str:
        """Rewrite SQL and update context with projection metadata."""
        ast = sqlglot.parse_one(sql, dialect=self.dialect)
        self._rewrite_expression_tree(ast, context, is_root=True)
        return ast.sql(dialect=self.dialect)

    def _rewrite_expression_tree(
        self,
        node: exp.Expression,
        context: QueryContext,
        is_root: bool,
    ) -> None:
        """Traverse AST and rewrite every SELECT."""
        if isinstance(node, exp.Select):
            self._rewrite_pivot(node)
            self._inline_named_windows(node)
            self._rewrite_select(node, context, is_root)
        for key, value in node.args.items():
            child_is_root = self._child_is_root(node, key, is_root)
            self._visit_child(key, value, context, child_is_root)

    def _visit_child(
        self,
        key: str,
        value: Union[exp.Expression, List[exp.Expression]],
        context: QueryContext,
        is_root: bool,
    ) -> None:
        """Visit child expressions."""
        if isinstance(value, exp.Expression):
            self._rewrite_expression_tree(value, context, is_root=is_root)
            return
        if isinstance(value, list):
            for child in value:
                if isinstance(child, exp.Expression):
                    self._rewrite_expression_tree(child, context, is_root=False)

    def _child_is_root(
        self,
        parent: exp.Expression,
        arg_key: str,
        parent_is_root: bool,
    ) -> bool:
        """Determine if child should inherit root status."""
        if not parent_is_root:
            return False
        if isinstance(parent, exp.With) and arg_key == "this":
            return True
        if isinstance(parent, exp.Describe) and arg_key == "this":
            return True
        set_operations = (exp.Union, exp.Except, exp.Intersect)
        if isinstance(parent, set_operations) and arg_key == "this":
            return True
        if isinstance(parent, exp.Paren) and arg_key == "this":
            return True
        return False

    def _rewrite_select(
        self,
        select: exp.Select,
        context: QueryContext,
        is_root: bool,
    ) -> None:
        """Rewrite a SELECT node."""
        if not self._select_requires_expansion(select):
            return
        sources = self._collect_sources(select)
        expanded: List[exp.Expression] = []
        metadata: List[ColumnMapping] = []
        for expression in list(select.expressions):
            replacements, entries = self._rewrite_projection(expression, sources)
            for replacement in replacements:
                expanded.append(replacement)
            if is_root:
                for entry in entries:
                    metadata.append(entry)
        select.set("expressions", expanded)
        if is_root:
            self._replace_context_columns(context, metadata)

    def _rewrite_projection(
        self,
        expression: exp.Expression,
        sources: List[_SelectSource],
    ) -> Tuple[List[exp.Expression], List[ColumnMapping]]:
        """Rewrite an individual SELECT expression."""
        if isinstance(expression, exp.Star):
            return self._expand_wildcard(None, sources, expression)
        if isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star):
            qualifier = self._identifier_to_str(expression.table)
            return self._expand_wildcard(qualifier, sources, expression.this)
        replacements: List[exp.Expression] = [expression]
        metadata = self._column_mappings_for_expression(expression, sources)
        return replacements, metadata

    def _rewrite_pivot(self, select: exp.Select) -> None:
        """Expand a single-aggregate static PIVOT into conditional aggregation.

        PIVOT is not expressible in the engine's Postgres internal SQL, so it is
        rewritten to portable ``agg(CASE WHEN k = value THEN x END)`` that every
        source supports. Shapes the rewrite cannot handle fail fast.
        """
        pivot = self._single_pivot(select)
        if pivot is None:
            return
        self._reject_unsupported_pivot(select, pivot)
        table = select.args["from_"].this
        columns = self._build_source(table).columns
        self._apply_pivot_rewrite(select, table, pivot, columns)

    def _single_pivot(self, select: exp.Select):
        """Return the FROM table's pivot node, or None when there is none."""
        from_clause = select.args.get("from_")
        if from_clause is None:
            return None
        table = from_clause.this
        if not isinstance(table, exp.Table):
            return None
        pivots = table.args.get("pivots")
        if not pivots:
            return None
        return pivots[0]

    def _reject_unsupported_pivot(self, select: exp.Select, pivot: exp.Pivot) -> None:
        """Fail fast on pivot shapes the conditional-aggregation rewrite cannot do."""
        if pivot.args.get("unpivot"):
            raise UnsupportedSQLError("UNPIVOT is not supported")
        if len(pivot.expressions) != 1:
            raise UnsupportedSQLError("PIVOT with multiple aggregates is not supported")
        self._reject_non_star_pivot(select)
        self._reject_unsupported_pivot_field(pivot)

    def _reject_non_star_pivot(self, select: exp.Select) -> None:
        """PIVOT requires SELECT * over a single table (no joins)."""
        if select.args.get("joins") or not self._is_star_select(select):
            raise UnsupportedSQLError("PIVOT requires SELECT * over a single table")

    def _reject_unsupported_pivot_field(self, pivot: exp.Pivot) -> None:
        """Require a static IN list of literals and a column aggregate argument."""
        self._reject_non_static_in(pivot.args["fields"][0])
        agg_input = self._pivot_agg_func(pivot.expressions[0]).this
        if not isinstance(agg_input, exp.Column):
            raise UnsupportedSQLError("PIVOT aggregate must be over a single column")

    def _reject_non_static_in(self, field: exp.Expression) -> None:
        """The pivot's FOR ... IN must be a static list of literal values."""
        if not isinstance(field, exp.In) or not field.expressions:
            raise UnsupportedSQLError("PIVOT requires a static IN value list")
        self._reject_non_literal_values(field.expressions)

    def _reject_non_literal_values(self, values) -> None:
        """Require every pivot IN value to be a literal (no subquery/expression)."""
        for value in values:
            if not isinstance(value, exp.Literal):
                raise UnsupportedSQLError("PIVOT IN values must be literals")

    def _is_star_select(self, select: exp.Select) -> bool:
        """True when the projection is a single bare ``*``."""
        expressions = select.expressions
        return len(expressions) == 1 and isinstance(expressions[0], exp.Star)

    def _pivot_agg_func(self, aggregate: exp.Expression) -> exp.Expression:
        """Return the aggregate function node, unwrapping an output alias."""
        if isinstance(aggregate, exp.Alias):
            return aggregate.this
        return aggregate

    def _apply_pivot_rewrite(self, select, table, pivot, columns) -> None:
        """Replace SELECT * ... PIVOT(...) with conditional-aggregation SQL."""
        field = pivot.args["fields"][0]
        agg_func = self._pivot_agg_func(pivot.expressions[0])
        group_cols = self._pivot_group_columns(columns, field.this.name, agg_func)
        projections = self._pivot_projections(group_cols, pivot, field, agg_func)
        table.set("pivots", None)
        select.set("expressions", projections)
        keys = []
        for column in group_cols:
            keys.append(exp.column(column, quoted=True))
        select.set("group", exp.Group(expressions=keys))

    def _pivot_group_columns(self, columns, pivot_col, agg_func) -> List[str]:
        """Columns that stay grouped: all table columns minus pivot/value columns."""
        consumed = {pivot_col.lower()}
        for column in agg_func.find_all(exp.Column):
            consumed.add(column.name.lower())
        group_cols = []
        for column in columns:
            if column.lower() not in consumed:
                group_cols.append(column)
        return group_cols

    def _pivot_projections(self, group_cols, pivot, field, agg_func):
        """Group columns plus one conditional aggregate per IN value."""
        projections = []
        for column in group_cols:
            projections.append(exp.column(column, quoted=True))
        names = self._pivot_output_names(pivot, field)
        for value, out_name in zip(field.expressions, names):
            projections.append(
                self._pivot_case_aggregate(field.this, value, agg_func, out_name)
            )
        return projections

    def _pivot_output_names(self, pivot: exp.Pivot, field: exp.In) -> List[str]:
        """Output column names sqlglot computed, or the IN value text as fallback."""
        names = []
        for column in pivot.args.get("columns") or []:
            names.append(column.name)
        if len(names) == len(field.expressions):
            return names
        names = []
        for value in field.expressions:
            names.append(value.name)
        return names

    def _pivot_case_aggregate(self, pivot_col, value, agg_func, out_name):
        """Build ``agg(CASE WHEN pivot_col = value THEN <arg> END) AS out_name``."""
        condition = exp.EQ(this=pivot_col.copy(), expression=value.copy())
        case = exp.Case(ifs=[exp.If(this=condition, true=agg_func.this.copy())])
        new_agg = agg_func.__class__(this=case)
        return exp.alias_(new_agg, out_name)

    def _inline_named_windows(self, select: exp.Select) -> None:
        """Inline named windows (WINDOW w AS (...)) into their OVER w references.

        After inlining, every window reference carries its own spec and the
        WINDOW clause is removed, so the parser sees only explicit OVER (...).
        """
        named = select.args.get("windows")
        if not named:
            return
        definitions = self._named_window_map(named)
        for window in select.find_all(exp.Window):
            self._resolve_window_reference(window, definitions)
        select.set("windows", None)

    def _named_window_map(self, named: List[exp.Window]) -> dict:
        """Map each WINDOW name to its definition node."""
        definitions = {}
        for definition in named:
            definitions[definition.this.name.lower()] = definition
        return definitions

    def _resolve_window_reference(self, window: exp.Window, definitions: dict) -> None:
        """Merge a named window's spec into an ``OVER name`` reference."""
        alias = window.args.get("alias")
        if alias is None:
            return
        definition = definitions.get(alias.name.lower())
        if definition is None:
            raise StarExpansionError(f"Unknown window name '{alias.name}'")
        self._merge_window_definition(window, definition)

    def _merge_window_definition(
        self, window: exp.Window, definition: exp.Window
    ) -> None:
        """Adopt the definition's partition/order/frame, keeping reference extras."""
        window.set("alias", None)
        self._inherit_window_arg(window, definition, "partition_by")
        self._inherit_window_arg(window, definition, "order")
        self._inherit_window_arg(window, definition, "spec")

    def _inherit_window_arg(
        self, window: exp.Window, definition: exp.Window, key: str
    ) -> None:
        """Copy one window-spec arg from the definition if the reference lacks it."""
        if window.args.get(key):
            return
        source = definition.args.get(key)
        if source is None:
            return
        window.set(key, self._copy_window_arg(source))

    def _copy_window_arg(self, source):
        """Copy a window-spec arg, which may be a node or a list of nodes."""
        if isinstance(source, list):
            copied = []
            for item in source:
                copied.append(item.copy())
            return copied
        return source.copy()

    def _expand_wildcard(
        self,
        qualifier: Optional[str],
        sources: List[_SelectSource],
        star: exp.Star,
    ) -> Tuple[List[exp.Expression], List[ColumnMapping]]:
        """Expand '*' or alias.* into explicit columns, honoring EXCLUDE/REPLACE.

        EXCLUDE/EXCEPT drops the listed columns; REPLACE substitutes a column's
        output with the given ``expr AS name`` expression, keeping its position.
        """
        targets = self._targets_for_wildcard(qualifier, sources)
        excluded = self._excluded_column_names(star)
        replacements = self._replacement_expressions(star)
        columns: List[exp.Expression] = []
        metadata: List[ColumnMapping] = []
        for source in targets:
            for column in source.columns:
                if column.lower() in excluded:
                    continue
                self._append_wildcard_column(
                    source, column, replacements, columns, metadata
                )
        return columns, metadata

    def _append_wildcard_column(self, source, column, replacements, columns, metadata):
        """Append one expanded column, or its REPLACE expression, plus metadata."""
        replacement = replacements.get(column.lower())
        if replacement is not None:
            columns.append(replacement.copy())
            return
        columns.append(self._build_column_expression(source, column))
        metadata.append(
            ColumnMapping(
                internal_name=self._build_internal_name(source, column),
                visible_name=column,
            )
        )

    def _excluded_column_names(self, star: exp.Star) -> set:
        """Return the lowercased column names listed in a star's EXCEPT/EXCLUDE."""
        names = set()
        for column in star.args.get("except_") or []:
            names.add(column.name.lower())
        return names

    def _replacement_expressions(self, star: exp.Star) -> dict:
        """Map a lowercased column name to its REPLACE ``expr AS name`` node."""
        mapping = {}
        for alias in star.args.get("replace") or []:
            mapping[alias.alias.lower()] = alias
        return mapping

    def _targets_for_wildcard(
        self,
        qualifier: Optional[str],
        sources: List[_SelectSource],
    ) -> List[_SelectSource]:
        """Select sources referenced by a wildcard."""
        if qualifier is None:
            return sources
        matched: List[_SelectSource] = []
        for source in sources:
            if source.matches(qualifier):
                matched.append(source)
        if not matched:
            raise StarExpansionError(
                f"Unknown table alias '{qualifier}' in star projection"
            )
        return matched

    def _column_mappings_for_expression(
        self,
        expression: exp.Expression,
        sources: List[_SelectSource],
    ) -> List[ColumnMapping]:
        """Build metadata for explicit columns."""
        alias_name = None
        target_expr = expression
        if isinstance(expression, exp.Alias):
            alias_name = self._identifier_to_str(expression.alias)
            target_expr = expression.this
        column_expr = self._extract_column_expression(target_expr)
        if column_expr is None:
            return []
        mapping = self._build_column_mapping(column_expr, alias_name, sources)
        if mapping:
            return [mapping]
        return []

    def _extract_column_expression(
        self,
        expression: exp.Expression,
    ) -> Optional[exp.Column]:
        """Extract Column from expression when possible."""
        if isinstance(expression, exp.Column):
            return expression
        return None

    def _build_column_mapping(
        self,
        column_expr: exp.Column,
        alias_name: Optional[str],
        sources: List[_SelectSource],
    ) -> Optional[ColumnMapping]:
        """Create ColumnMapping for a single column expression."""
        column_name = self._identifier_to_str(column_expr.this)
        qualifier = self._identifier_to_str(column_expr.table)
        source = self._resolve_source_for_column(qualifier, column_name, sources)
        if source is None:
            return None
        visible = column_name
        if alias_name:
            visible = alias_name
        internal = self._build_internal_name(source, column_name)
        return ColumnMapping(
            internal_name=internal,
            visible_name=visible,
            alias=alias_name,
        )

    def _resolve_source_for_column(
        self,
        qualifier: Optional[str],
        column_name: str,
        sources: List[_SelectSource],
    ) -> Optional[_SelectSource]:
        """Find source for a column reference."""
        if qualifier:
            for source in sources:
                if source.matches(qualifier):
                    return source
            return None
        matches: List[_SelectSource] = []
        for source in sources:
            if source.has_column(column_name):
                matches.append(source)
        if len(matches) == 1:
            return matches[0]
        return None

    def _collect_sources(self, select: exp.Select) -> List[_SelectSource]:
        """Collect table metadata needed for expansion."""
        sources: List[_SelectSource] = []
        from_clause = select.args.get("from_")
        if from_clause is None:
            return sources
        sources.append(self._build_source(from_clause.this))
        joins = select.args.get("joins")
        if joins:
            for join in joins:
                sources.append(self._build_source(join.this))
        return sources

    def _build_source(self, table_expr: exp.Expression) -> _SelectSource:
        """Build metadata for a FROM/JOIN table."""
        if not isinstance(table_expr, exp.Table):
            raise StarExpansionError("Star expansion only supports base tables")
        datasource, schema_name, table_name = self._extract_table_parts(table_expr)
        table = self.catalog.get_table(datasource, schema_name, table_name)
        if table is None:
            qualified = f"{datasource}.{schema_name}.{table_name}"
            raise StarExpansionError(f"Missing catalog metadata for {qualified}")
        alias = self._parse_alias(table_expr)
        sql_qualifier = self._sql_qualifier(datasource, schema_name, table_name, alias)
        internal_prefix = self._internal_prefix(
            datasource, schema_name, table_name, alias
        )
        columns = self._column_names(table)
        return _SelectSource(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            alias=alias,
            sql_qualifier=sql_qualifier,
            internal_prefix=internal_prefix,
            columns=columns,
        )

    def _parse_alias(self, table_expr: exp.Table) -> Optional[str]:
        """Parse table alias if present."""
        alias = table_expr.alias
        if alias:
            return str(alias)
        return None

    def _sql_qualifier(
        self,
        datasource: str,
        schema_name: str,
        table_name: str,
        alias: Optional[str],
    ) -> str:
        """Determine SQL qualifier used in rewritten columns."""
        if alias:
            return alias
        return table_name

    def _internal_prefix(
        self,
        datasource: str,
        schema_name: str,
        table_name: str,
        alias: Optional[str],
    ) -> str:
        """Determine prefix for internal names."""
        if alias:
            return f"{datasource}.{alias}"
        return f"{datasource}.{schema_name}.{table_name}"

    def _column_names(self, table: Table) -> List[str]:
        """List column names from catalog metadata."""
        names: List[str] = []
        for column in table.columns:
            names.append(column.name)
        if not names:
            raise StarExpansionError(f"Table {table.name} has no column metadata")
        return names

    def _extract_table_parts(self, table_expr: exp.Table) -> Tuple[str, str, str]:
        """Extract datasource, schema, and table name."""
        parts: List[str] = []
        if table_expr.catalog:
            parts.append(str(table_expr.catalog))
        if table_expr.db:
            parts.append(str(table_expr.db))
        parts.append(table_expr.name)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return parts[0], parts[1], table_expr.name
        datasource = "default"
        schema_name = "public"
        return datasource, schema_name, parts[0]

    def _replace_context_columns(
        self,
        context: QueryContext,
        metadata: List[ColumnMapping],
    ) -> None:
        """Replace context column metadata."""
        context.columns = []
        for entry in metadata:
            context.add_column(entry)

    def _identifier_to_str(self, identifier: Optional[exp.Expression]) -> Optional[str]:
        """Convert Identifier or None to string."""
        if identifier is None:
            return None
        return str(identifier)

    def _build_internal_name(
        self,
        source: _SelectSource,
        column_name: str,
    ) -> str:
        """Build internal column name used inside the engine."""
        return f"{source.internal_prefix}.{column_name}"

    def _select_requires_expansion(self, select: exp.Select) -> bool:
        """Check if select contains projection stars."""
        for expression in select.expressions:
            if self._is_projection_star(expression):
                return True
        return False

    def _is_projection_star(self, expression: exp.Expression) -> bool:
        """Check if expression is '*' or alias.*."""
        if isinstance(expression, exp.Star):
            return True
        if isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star):
            return True
        return False

    def _build_column_expression(
        self,
        source: _SelectSource,
        column_name: str,
    ) -> exp.Column:
        """Create a Column expression bound to a table qualifier.

        A column whose name is a reserved word (e.g. ``select``) must be
        quoted, otherwise the rewritten SQL fails to re-parse.
        """
        table_identifier = exp.to_identifier(source.sql_qualifier)
        column_identifier = exp.to_identifier(
            column_name, quoted=self._is_reserved_word(column_name)
        )
        return exp.Column(this=column_identifier, table=table_identifier)

    def _is_reserved_word(self, name: str) -> bool:
        """Whether a bare name must be quoted to survive a re-parse.

        Checked by re-parsing ``SELECT <name>``: a truly reserved word (e.g.
        ``select``) fails or does not yield a plain column, so it needs
        quoting; ordinary names like ``name`` parse cleanly and do not.
        """
        from sqlglot.errors import ParseError

        try:
            parsed = sqlglot.parse_one(f"SELECT {name}", dialect=self.dialect)
        except ParseError:
            return True
        selected = parsed.expressions[0] if parsed.expressions else None
        return not (
            isinstance(selected, exp.Column) and selected.name.upper() == name.upper()
        )


class StarExpansionProcessor(QueryProcessor):
    """Middleware that performs star expansion and column renaming."""

    def __init__(self, catalog: Catalog, dialect=FedQPostgres):
        """Initialize processor with catalog metadata."""
        self.preprocessor = QueryPreprocessor(catalog, dialect=dialect)

    def before_execution(self, executor: QueryExecutor) -> None:
        """Rewrite SQL before parsing."""
        context = executor.query_context
        rewritten = self.preprocessor.preprocess(context.rewritten_sql, context)
        context.rewritten_sql = rewritten

    def after_execution(
        self,
        executor: QueryExecutor,
        result: Union[pa.Table, dict],
    ) -> Union[pa.Table, dict]:
        """Rename columns back to user-visible names."""
        if not isinstance(result, pa.Table):
            return result
        context = executor.query_context
        if not context.columns:
            return result
        if result.num_columns != len(context.columns):
            return result
        names: List[str] = []
        for mapping in context.columns:
            names.append(mapping.visible_name)
        return result.rename_columns(names)
