"""Query preprocessing stage that expands SELECT * projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import pyarrow as pa
import sqlglot
from sqlglot import exp

from ..catalog import Catalog
from ..catalog.schema import Table
from .query_context import ColumnMapping, QueryContext
from .query_executor import QueryExecutor, QueryProcessor


class StarExpansionError(Exception):
    """Raised when SELECT * cannot be expanded."""

    pass


@dataclass
class _SelectSource:
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

    def __init__(self, catalog: Catalog, dialect: str = "postgres"):
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
            self._rewrite_select(node, context, is_root)
        for value in node.args.values():
            self._visit_child(value, context)

    def _visit_child(self, value: Union[exp.Expression, List[exp.Expression]], context: QueryContext) -> None:
        """Visit child expressions."""
        if isinstance(value, exp.Expression):
            self._rewrite_expression_tree(value, context, is_root=False)
            return
        if isinstance(value, list):
            for child in value:
                if isinstance(child, exp.Expression):
                    self._rewrite_expression_tree(child, context, is_root=False)

    def _rewrite_select(
        self,
        select: exp.Select,
        context: QueryContext,
        is_root: bool,
    ) -> None:
        """Rewrite a SELECT node."""
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
            return self._expand_wildcard(None, sources)
        if isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star):
            qualifier = self._identifier_to_str(expression.table)
            return self._expand_wildcard(qualifier, sources)
        replacements: List[exp.Expression] = [expression]
        metadata = self._column_mappings_for_expression(expression, sources)
        return replacements, metadata

    def _expand_wildcard(
        self,
        qualifier: Optional[str],
        sources: List[_SelectSource],
    ) -> Tuple[List[exp.Expression], List[ColumnMapping]]:
        """Expand '*' or alias.* into explicit columns."""
        targets = self._targets_for_wildcard(qualifier, sources)
        replacements: List[exp.Expression] = []
        metadata: List[ColumnMapping] = []
        for source in targets:
            for column in source.columns:
                column_expr = self._build_column_expression(source, column)
                replacements.append(column_expr)
                mapping = ColumnMapping(
                    internal_name=self._build_internal_name(source, column),
                    visible_name=column,
                )
                metadata.append(mapping)
        return replacements, metadata

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
            raise StarExpansionError(f"Unknown table alias '{qualifier}' in star projection")
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
        from_clause = select.args.get("from")
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
        internal_prefix = self._internal_prefix(datasource, schema_name, table_name, alias)
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

    def _build_column_expression(
        self,
        source: _SelectSource,
        column_name: str,
    ) -> exp.Column:
        """Create a Column expression bound to a table qualifier."""
        table_identifier = exp.to_identifier(source.sql_qualifier)
        column_identifier = exp.to_identifier(column_name)
        return exp.Column(this=column_identifier, table=table_identifier)


class StarExpansionProcessor(QueryProcessor):
    """Middleware that performs star expansion and column renaming."""

    def __init__(self, catalog: Catalog, dialect: str = "postgres"):
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
