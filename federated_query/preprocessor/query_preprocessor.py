"""Query preprocessing before parsing/binding.

Current responsibilities:
1. Expand SELECT * and alias.* into explicit column lists using catalog metadata
2. Strictly validate that all FROM/JOIN sources are concrete catalog tables with aliases

Raises StarExpansionError when rewrite requirements are not met.
"""

from typing import Dict, List, Optional, Tuple
import sqlglot
from sqlglot import exp

from ..catalog.catalog import Catalog


class StarExpansionError(ValueError):
    """Raised when SELECT * expansion cannot resolve column metadata."""


class QueryPreprocessor:
    """Apply SQL rewrites before the parser builds logical plans."""

    def __init__(self, catalog: Catalog, dialect: str = "postgres"):
        self.catalog = catalog
        self.dialect = dialect

    def preprocess(self, sql: str) -> str:
        """Rewrite SQL before parsing.

        Currently only expands SELECT * expressions using catalog metadata.
        Raises StarExpansionError when expansion is impossible.
        """
        ast = sqlglot.parse_one(sql, dialect=self.dialect)
        if not self._contains_star(ast):
            return sql
        rewritten = self._expand_select_stars(ast)
        return rewritten.sql(dialect=self.dialect)

    def _contains_star(self, ast: exp.Expression) -> bool:
        for select in ast.find_all(exp.Select):
            if self._select_contains_star(select):
                return True
        return False

    def _expand_select_stars(self, ast: exp.Expression) -> exp.Expression:
        rewritten = ast.copy()
        for select in rewritten.find_all(exp.Select):
            if not self._select_contains_star(select):
                continue
            table_order, alias_map = self._collect_table_metadata(select)
            if len(table_order) == 0:
                continue
            expressions = self._rewrite_select_expressions(
                select.expressions,
                table_order,
                alias_map,
            )
            select.set("expressions", expressions)
        return rewritten

    def _select_contains_star(self, select: exp.Select) -> bool:
        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                return True
            if self._is_qualified_star(expr):
                return True
        return False

    def _collect_table_metadata(
        self,
        select: exp.Select,
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        table_order: List[str] = []
        alias_map: Dict[str, List[str]] = {}
        sources = self._iter_table_expressions(select)
        for table_expr in sources:
            alias = self._extract_table_alias(table_expr)
            if alias is None:
                raise StarExpansionError("SELECT * requires every table to have an alias")
            columns: List[str] = []
            if isinstance(table_expr, exp.Table):
                parts = self._extract_table_parts(table_expr)
                columns = self._lookup_table_columns(parts)
            elif isinstance(table_expr, exp.Subquery):
                message = f"SELECT * expansion does not support derived tables (alias '{alias}')"
                raise StarExpansionError(message)
            else:
                message = (
                    "SELECT * expansion requires catalog tables; found "
                    f"{type(table_expr).__name__}"
                )
                raise StarExpansionError(message)
            table_order.append(alias)
            alias_map[alias] = columns
        return table_order, alias_map

    def _iter_table_expressions(self, select: exp.Select) -> List[exp.Expression]:
        tables: List[exp.Expression] = []
        from_clause = select.args.get("from")
        if from_clause:
            main_table = getattr(from_clause, "this", None)
            if isinstance(main_table, (exp.Table, exp.Subquery)):
                tables.append(main_table)
            for expr in from_clause.expressions or []:
                if isinstance(expr, (exp.Table, exp.Subquery)):
                    tables.append(expr)
        joins = select.args.get("joins") or []
        for join in joins:
            table_expr = getattr(join, "this", None)
            if isinstance(table_expr, (exp.Table, exp.Subquery)):
                tables.append(table_expr)
        return tables

    def _extract_table_alias(self, table_expr: exp.Expression) -> Optional[str]:
        alias = getattr(table_expr, "alias", None)
        if alias:
            return str(alias)
        alias_or_name = getattr(table_expr, "alias_or_name", None)
        if alias_or_name is not None:
            return str(alias_or_name)
        if isinstance(table_expr, exp.Subquery):
            return str(table_expr)
        return None

    def _extract_table_parts(self, table_expr: exp.Table) -> Tuple[str, str, str]:
        parts: List[str] = []
        if table_expr.catalog:
            parts.append(table_expr.catalog)
        if table_expr.db:
            parts.append(table_expr.db)
        name = table_expr.name
        if name:
            parts.append(name)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "default", parts[0], parts[1]
        return "default", "public", parts[0]

    def _lookup_table_columns(
        self,
        parts: Tuple[str, str, str],
    ) -> List[str]:
        datasource, schema_name, table_name = parts
        table = self.catalog.get_table(datasource, schema_name, table_name)
        if table is None:
            message = (
                "SELECT * expansion requires metadata for "
                f"{datasource}.{schema_name}.{table_name}"
            )
            raise StarExpansionError(message)
        names: List[str] = []
        for column in table.columns:
            names.append(column.name)
        return names

    def _rewrite_select_expressions(
        self,
        expressions: List[exp.Expression],
        table_order: List[str],
        alias_map: Dict[str, List[str]],
    ) -> List[exp.Expression]:
        rewritten: List[exp.Expression] = []
        for expr in expressions:
            replacements = self._expand_select_expression(
                expr,
                table_order,
                alias_map,
            )
            for item in replacements:
                rewritten.append(item)
        return rewritten

    def _expand_select_expression(
        self,
        expr: exp.Expression,
        table_order: List[str],
        alias_map: Dict[str, List[str]],
    ) -> List[exp.Expression]:
        if isinstance(expr, exp.Star):
            return self._expand_unqualified_star(table_order, alias_map)
        if self._is_qualified_star(expr):
            alias = self._column_identifier(expr.table)
            return self._expand_qualified_star(alias, alias_map)
        return [expr]

    def _expand_unqualified_star(
        self,
        table_order: List[str],
        alias_map: Dict[str, List[str]],
    ) -> List[exp.Expression]:
        columns: List[exp.Expression] = []
        for alias in table_order:
            names = alias_map.get(alias, [])
            expanded = self._build_column_list(alias, names)
            for col in expanded:
                columns.append(col)
        return columns

    def _expand_qualified_star(
        self,
        alias: Optional[str],
        alias_map: Dict[str, List[str]],
    ) -> List[exp.Expression]:
        if alias is None:
            raise StarExpansionError("Qualified star missing alias")
        if alias not in alias_map:
            message = f"Alias '{alias}' not found for SELECT * expansion"
            raise StarExpansionError(message)
        return self._build_column_list(alias, alias_map[alias])

    def _build_column_list(
        self,
        alias: str,
        column_names: List[str],
    ) -> List[exp.Expression]:
        columns: List[exp.Expression] = []
        for name in column_names:
            column_expr = exp.Column(
                this=exp.Identifier(this=name, quoted=True),
                table=exp.Identifier(this=alias, quoted=False),
            )
            columns.append(column_expr)
        return columns

    def _is_qualified_star(self, expr: exp.Expression) -> bool:
        if not isinstance(expr, exp.Column):
            return False
        return isinstance(expr.this, exp.Star)

    def _column_identifier(self, identifier) -> Optional[str]:
        if identifier is None:
            return None
        if hasattr(identifier, "this"):
            return identifier.this
        return str(identifier)
