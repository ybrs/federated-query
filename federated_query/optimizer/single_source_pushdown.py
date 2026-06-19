"""Render a pushable single-source logical subtree as one remote SQL query.

When an entire subtree (projection / aggregate / join-tree / filtered scans)
targets a single data source, the whole thing can be sent to that source as one
flat ``SELECT`` instead of fetching each table and joining locally. This module
detects such subtrees and builds the remote query; anything it cannot render
(cross-source, unsupported node, unsafe filter) is declined so the planner falls
back to local execution.
"""

from typing import Dict, List, Optional, Tuple

from ..catalog.catalog import Catalog
from ..datasources.base import DataSourceCapability
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Join,
    JoinType,
    Aggregate,
    Sort,
    Limit,
)
from ..plan.expressions import (
    ColumnRef,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    Cast,
    Expression,
)
from ..plan.physical import PhysicalRemoteQuery

_JOIN_KEYWORDS = {
    JoinType.INNER: "INNER JOIN",
    JoinType.LEFT: "LEFT JOIN",
    JoinType.RIGHT: "RIGHT JOIN",
    JoinType.FULL: "FULL OUTER JOIN",
}


class _PushContext:
    """Mutable accumulator for the clauses of one remote query."""

    def __init__(self) -> None:
        self.datasource: Optional[str] = None
        self.select_items: List[str] = []
        self.output_names: List[str] = []
        self.from_sql: Optional[str] = None
        self.joins: List[str] = []
        self.where_terms: List[str] = []
        self.group_sql: Optional[str] = None
        self.order_sql: Optional[str] = None
        self.limit: Optional[int] = None
        self.offset: int = 0
        self.distinct: bool = False
        self.has_join: bool = False
        self.has_computed: bool = False
        self.has_aggregate: bool = False
        # (table qualifier, column) -> unique output name, so an operator above
        # a pushed multi-table query can resolve a qualified reference even when
        # two source columns share a name (e.g. both tables expose ``id``).
        self.column_aliases: Dict[Tuple[Optional[str], str], str] = {}
        # Leaf scans, in FROM order, used to expand ``*`` when no projection
        # supplied an explicit (and unique) column list.
        self.scans: List[Scan] = []


class SingleSourcePushdown:
    """Builds a single remote query for a same-source subtree, when possible."""

    def __init__(self, catalog: Catalog) -> None:
        """Store the catalog used to resolve data source connections."""
        self.catalog = catalog

    def try_build(self, node: LogicalPlanNode) -> Optional[PhysicalRemoteQuery]:
        """Return a remote query for a pushable same-source subtree, or None.

        Fires for any subtree that contains a join (G1) or a computed,
        non-aggregate projection (G2 — e.g. ``UPPER(x)``, ``a || b``); plain
        single-table scans keep using the existing scan-pushdown path.
        """
        context = _PushContext()
        if not self._absorb(node, context):
            return None
        if not self._should_push(context):
            return None
        if not context.select_items and not self._expand_star_select(context):
            return None
        return self._finish(context)

    def _expand_star_select(self, context: _PushContext) -> bool:
        """Build a unique-aliased SELECT list from each scan's needed columns.

        Used when no projection supplied the columns (a bare ``SELECT *`` over a
        join). Emitting ``*`` would yield duplicate column names that a parent
        operator cannot resolve, so the columns are listed explicitly with
        unique aliases and recorded for qualified resolution. Each scan already
        carries the columns projection pushdown found necessary, so only those
        are emitted (not the whole table) — a true ``*`` falls back to the
        catalog.
        """
        seen: set = set()
        for scan in context.scans:
            names = self._scan_output_columns(scan)
            if names is None:
                return False
            self._expand_scan_columns(scan, names, seen, context)
        return True

    def _scan_output_columns(self, scan) -> Optional[List[str]]:
        """Columns a scan must emit: its pruned list, or all when it is ``*``."""
        if scan.columns and "*" not in scan.columns:
            return scan.columns
        table = self.catalog.get_table(
            scan.datasource, scan.schema_name, scan.table_name
        )
        if table is None:
            return None
        names: List[str] = []
        for column in table.columns:
            names.append(column.name)
        return names

    def _expand_scan_columns(self, scan, names: List[str], seen: set, context) -> None:
        """Append a scan's named columns as unique-aliased SELECT items."""
        alias = scan.alias if scan.alias else scan.table_name
        for name in names:
            unique = self._unique_name(name, seen)
            seen.add(unique)
            context.output_names.append(unique)
            context.select_items.append(f'{alias}."{name}" AS "{unique}"')
            context.column_aliases[(alias, name)] = unique

    def _should_push(self, context: _PushContext) -> bool:
        """Whether this subtree is worth replacing with one remote query."""
        if context.has_join:
            return True
        return context.has_computed and not context.has_aggregate

    def _finish(self, context: _PushContext) -> Optional[PhysicalRemoteQuery]:
        """Resolve the connection, render SQL, and build the physical node."""
        connection = self.catalog.get_datasource(context.datasource)
        if connection is None:
            return None
        if not connection.supports_capability(DataSourceCapability.JOINS):
            return None
        sql = self._render(context)
        ast = connection.parse_query(sql)
        return PhysicalRemoteQuery(
            datasource=context.datasource,
            datasource_connection=connection,
            query_ast=ast,
            output_names=context.output_names,
            column_alias_map=context.column_aliases,
        )

    def _absorb(self, node: LogicalPlanNode, context: _PushContext) -> bool:
        """Fold one plan node's clause into the context; recurse into its input."""
        if isinstance(node, Limit):
            return self._absorb_limit(node, context)
        if isinstance(node, Sort):
            return self._absorb_sort(node, context)
        if isinstance(node, Projection):
            return self._absorb_projection(node, context)
        if isinstance(node, Aggregate):
            return self._absorb_aggregate(node, context)
        if isinstance(node, Filter):
            return self._absorb_filter(node, context)
        if isinstance(node, (Join, Scan)):
            return self._absorb_from(node, context)
        return False

    def _absorb_limit(self, limit: Limit, context: _PushContext) -> bool:
        """Record LIMIT / OFFSET, then descend."""
        context.limit = limit.limit
        context.offset = limit.offset
        return self._absorb(limit.input, context)

    def _absorb_sort(self, sort: Sort, context: _PushContext) -> bool:
        """Record the ORDER BY clause, then descend."""
        context.order_sql = self._render_order(sort)
        return self._absorb(sort.input, context)

    def _absorb_projection(self, projection: Projection, context: _PushContext) -> bool:
        """Record the SELECT list (with aliases), then descend.

        An unexpanded ``*`` projection is declined: the preprocessor normally
        expands stars before planning, so one surviving here cannot be rendered
        faithfully (``* AS "*"`` is invalid SQL) and is left to local execution.
        """
        context.distinct = projection.distinct
        if self._has_star(projection.expressions):
            return False
        if self._has_computed_expression(projection.expressions):
            context.has_computed = True
        self._set_select(projection.expressions, projection.aliases, context)
        return self._absorb(projection.input, context)

    def _has_star(self, expressions) -> bool:
        """Whether any projection item is an unexpanded ``*`` reference."""
        for expression in expressions:
            if isinstance(expression, ColumnRef) and expression.column == "*":
                return True
        return False

    def _has_computed_expression(self, expressions) -> bool:
        """Whether any projection item is more than a bare column reference."""
        for expression in expressions:
            if not isinstance(expression, ColumnRef):
                return True
        return False

    def _absorb_aggregate(self, aggregate: Aggregate, context: _PushContext) -> bool:
        """Record GROUP BY and the aggregate SELECT list, then descend."""
        context.has_aggregate = True
        if not context.select_items:
            self._set_select(aggregate.aggregates, aggregate.output_names, context)
        context.group_sql = self._render_group_by(aggregate)
        return self._absorb(aggregate.input, context)

    def _absorb_filter(self, filter_node: Filter, context: _PushContext) -> bool:
        """Push a WHERE-level filter into the remote query; leave HAVING local.

        A filter directly above an Aggregate is a HAVING clause; the convention
        for join pushdown is to apply it locally (the binder rewrites it to the
        aggregate's output alias, which is not a remotely resolvable WHERE), so
        we decline here and let the planner keep it as a local filter above the
        pushed aggregate. Any other filter is a WHERE predicate and is rendered
        in the remote query.
        """
        if isinstance(filter_node.input, Aggregate):
            return False
        context.where_terms.append(filter_node.predicate.to_sql())
        return self._absorb(filter_node.input, context)

    def _absorb_from(self, node: LogicalPlanNode, context: _PushContext) -> bool:
        """Build the FROM clause from a left-deep join tree or a base scan."""
        if isinstance(node, Scan):
            return self._absorb_base_scan(node, context)
        if isinstance(node, Join):
            return self._absorb_join(node, context)
        return False

    def _absorb_base_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Set the leftmost FROM source and collect its filter."""
        if not self._claim_scan(scan, context):
            return False
        context.from_sql = self._scan_ref(scan)
        return True

    def _absorb_join(self, join: Join, context: _PushContext) -> bool:
        """Add one join (right side must be a base scan in a left-deep tree)."""
        if join.join_type in (JoinType.SEMI, JoinType.ANTI):
            return self._absorb_semi_anti_join(join, context)
        keyword = _JOIN_KEYWORDS.get(join.join_type)
        if keyword is None or not self._join_is_pushable(join):
            return False
        if not isinstance(join.right, Scan):
            return False
        if not self._absorb_from(join.left, context):
            return False
        if not self._claim_scan(join.right, context):
            return False
        context.joins.append(self._render_join_clause(join, keyword))
        context.has_join = True
        return True

    def _join_is_pushable(self, join: Join) -> bool:
        """A join pushes when it has an ON condition, or is NATURAL/USING."""
        return join.condition is not None or join.natural or join.using is not None

    def _render_join_clause(self, join: Join, keyword: str) -> str:
        """Render one join clause using ON, USING, or NATURAL syntax."""
        right_ref = self._scan_ref(join.right)
        if join.natural:
            return f"NATURAL {keyword} {right_ref}"
        if join.using is not None:
            columns = ", ".join(join.using)
            return f"{keyword} {right_ref} USING ({columns})"
        return f"{keyword} {right_ref} ON {join.condition.to_sql()}"

    def _absorb_semi_anti_join(self, join: Join, context: _PushContext) -> bool:
        """Render a decorrelated SEMI/ANTI join as a ``[NOT] EXISTS`` predicate.

        Decorrelation turns EXISTS/IN/ANY into a SEMI join (ANTI for the negated
        forms) whose right side is a key projection. Re-correlating it back to an
        ``EXISTS`` subquery lets the whole thing push to one source as one query.
        Only fires at the outer-query level: a SEMI join nested under an
        aggregate keeps running in the merge engine (re-rendering it there would
        strand the aggregate's output columns).
        """
        if context.has_aggregate:
            return False
        if not self._absorb_from(join.left, context):
            return False
        subquery = self._render_existence_subquery(join, context)
        if subquery is None:
            return False
        prefix = "NOT " if join.join_type == JoinType.ANTI else ""
        context.where_terms.append(f"{prefix}EXISTS ({subquery})")
        context.has_join = True
        return True

    def _render_existence_subquery(
        self, join: Join, context: _PushContext
    ) -> Optional[str]:
        """Build the ``SELECT 1 FROM ... WHERE ...`` body of the EXISTS subquery.

        Outer columns are qualified with the outer alias. Only when the subquery
        scans the *same* alias as the outer table is it given a fresh alias (and
        its columns re-qualified) so a self-referential subquery keeps the two
        sides distinct; otherwise the existing aliases already disambiguate.
        """
        right = join.right
        if not isinstance(right, Projection):
            return None
        if not self._is_simple_subquery_relation(right.input):
            return None
        outer_alias = self._sole_outer_alias(context)
        scan = self._subquery_scan(right.input)
        if outer_alias is None or scan.datasource != context.datasource:
            return None
        orig = scan.alias if scan.alias else scan.table_name
        inner_alias = orig
        if orig == outer_alias:
            inner_alias = self._fresh_subquery_alias(scan.table_name, outer_alias)
        return self._compose_exists_select(
            join, right, scan, orig, inner_alias, outer_alias
        )

    def _compose_exists_select(
        self, join, projection, scan, orig, inner_alias, outer_alias
    ) -> str:
        """Assemble the EXISTS body from the re-aliased scan and correlation."""
        key_map = self._build_key_map(projection, orig, inner_alias)
        condition = self._substitute_keys(join.condition, key_map, outer_alias)
        terms = self._inner_where_terms(join.right.input, orig, inner_alias)
        terms.append(condition.to_sql())
        from_sql = f'"{scan.schema_name}"."{scan.table_name}" AS {inner_alias}'
        return f"SELECT 1 FROM {from_sql} WHERE {' AND '.join(terms)}"

    def _subquery_scan(self, node: LogicalPlanNode) -> Scan:
        """The base scan of a simple subquery body (unwrapping a Filter)."""
        if isinstance(node, Filter):
            return node.input
        return node

    def _fresh_subquery_alias(self, table_name: str, outer_alias: str) -> str:
        """A subquery alias distinct from the outer alias to avoid collisions."""
        candidate = f"{table_name}_sq"
        if candidate == outer_alias:
            return f"{table_name}_sq2"
        return candidate

    def _inner_where_terms(
        self, node: LogicalPlanNode, orig: str, inner_alias: str
    ) -> List[str]:
        """The subquery's own filters, re-qualified to its fresh alias."""
        terms: List[str] = []
        if isinstance(node, Filter):
            terms.append(self._requalify(node.predicate, orig, inner_alias).to_sql())
            node = node.input
        if node.filters is not None:
            terms.append(self._requalify(node.filters, orig, inner_alias).to_sql())
        return terms

    def _requalify(self, expr: Expression, orig: str, inner_alias: str) -> Expression:
        """Re-qualify a subquery-local expression's columns to its scan alias."""
        return self._map_columns(
            expr, lambda col: self._requalify_column(col, orig, inner_alias)
        )

    def _requalify_column(
        self, column: ColumnRef, orig: str, inner_alias: str
    ) -> Expression:
        """Re-qualify one column that belongs to the subquery's own scan."""
        if column.table == orig or column.table is None:
            return ColumnRef(table=inner_alias, column=column.column)
        return column

    def _map_columns(self, expr: Expression, leaf) -> Expression:
        """Rebuild an expression, applying ``leaf`` to every ColumnRef it holds."""
        if isinstance(expr, ColumnRef):
            return leaf(expr)
        if isinstance(expr, BinaryOp):
            left = self._map_columns(expr.left, leaf)
            right = self._map_columns(expr.right, leaf)
            return BinaryOp(op=expr.op, left=left, right=right)
        if isinstance(expr, UnaryOp):
            return UnaryOp(op=expr.op, operand=self._map_columns(expr.operand, leaf))
        return self._map_compound_columns(expr, leaf)

    def _map_compound_columns(self, expr: Expression, leaf) -> Expression:
        """Map columns inside a Cast or function-call wrapper."""
        if isinstance(expr, Cast):
            inner = self._map_columns(expr.expr, leaf)
            return Cast(
                expr=inner, target_type=expr.target_type, data_type=expr.data_type
            )
        if isinstance(expr, FunctionCall):
            args: List[Expression] = []
            for arg in expr.args:
                args.append(self._map_columns(arg, leaf))
            return FunctionCall(
                function_name=expr.function_name,
                args=args,
                is_aggregate=expr.is_aggregate,
                distinct=expr.distinct,
            )
        return expr

    def _sole_outer_alias(self, context: _PushContext) -> Optional[str]:
        """The single outer scan's alias, used to qualify correlation columns.

        Returns None when the outer side has several scans, where an unqualified
        correlation column cannot be safely attributed to one of them.
        """
        if len(context.scans) != 1:
            return None
        scan = context.scans[0]
        return scan.alias if scan.alias else scan.table_name

    def _is_simple_subquery_relation(self, node: LogicalPlanNode) -> bool:
        """Whether a subquery body is a single plain scan we can re-render safely.

        Restricts EXISTS rendering to ``Scan`` or ``Filter`` over ``Scan`` with
        no aggregate/group; aggregates, joins, or further nesting inside the
        subquery are declined so they keep running in the merge engine.
        """
        if isinstance(node, Filter):
            node = node.input
        return isinstance(node, Scan) and not node.group_by and not node.aggregates

    def _build_key_map(
        self, projection: Projection, orig: str, inner_alias: str
    ) -> Dict[str, Expression]:
        """Map each key alias to its source expression, re-qualified to the alias."""
        mapping: Dict[str, Expression] = {}
        for index in range(len(projection.aliases)):
            expr = self._requalify(projection.expressions[index], orig, inner_alias)
            mapping[projection.aliases[index]] = expr
        return mapping

    def _substitute_keys(
        self, expr: Expression, key_map: Dict[str, Expression], outer_alias: str
    ) -> Expression:
        """Rewrite a correlation condition for use inside the EXISTS subquery.

        A synthetic key alias becomes its source expression; any other bare
        column is an outer reference and is qualified with ``outer_alias``.
        """
        return self._map_columns(
            expr, lambda col: self._substitute_column(col, key_map, outer_alias)
        )

    def _substitute_column(
        self, column: ColumnRef, key_map: Dict[str, Expression], outer_alias: str
    ) -> Expression:
        """Resolve one column ref to a key definition or a qualified outer ref."""
        if column.table is not None:
            return column
        if column.column in key_map:
            return key_map[column.column]
        return ColumnRef(table=outer_alias, column=column.column)

    def _claim_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Confirm the scan shares the subtree's data source and collect filters."""
        if scan.group_by or scan.aggregates:
            return False
        if context.datasource is None:
            context.datasource = scan.datasource
        elif context.datasource != scan.datasource:
            return False
        if scan.filters is not None:
            context.where_terms.append(scan.filters.to_sql())
        context.scans.append(scan)
        return True

    def _set_select(self, expressions, names, context: _PushContext) -> None:
        """Render the SELECT list with unique output names per column.

        Output names are made unique (``id``, ``id_1`` ...) so a result with
        columns from several tables never has two fields of the same name —
        otherwise a parent operator could not address one of them and the
        connector could not decode them. Each column reference also records a
        (qualifier, column) -> unique name entry for qualified resolution.
        """
        seen: set = set()
        items: List[str] = []
        output_names: List[str] = []
        for index in range(len(expressions)):
            expression = expressions[index]
            base = self._base_name(expression, names, index)
            unique = self._unique_name(base, seen)
            seen.add(unique)
            output_names.append(unique)
            items.append(f'{expression.to_sql()} AS "{unique}"')
            self._record_alias(expression, unique, context)
        context.select_items = items
        context.output_names = output_names

    def _base_name(self, expression, names, index: int) -> str:
        """The desired (pre-dedup) output name for a projection item."""
        if names and index < len(names) and names[index]:
            return names[index]
        return expression.to_sql()

    def _unique_name(self, base: str, seen: set) -> str:
        """Return ``base`` or the first free ``base_N`` not already used."""
        if base not in seen:
            return base
        suffix = 1
        while f"{base}_{suffix}" in seen:
            suffix += 1
        return f"{base}_{suffix}"

    def _record_alias(self, expression, unique: str, context: _PushContext) -> None:
        """Map a column reference's (qualifier, column) to its output name."""
        if isinstance(expression, ColumnRef):
            context.column_aliases[(expression.table, expression.column)] = unique

    def _scan_ref(self, scan: Scan) -> str:
        """Render a table reference with its alias for the FROM clause."""
        reference = f'"{scan.schema_name}"."{scan.table_name}"'
        alias = scan.alias if scan.alias else scan.table_name
        return f"{reference} AS {alias}"

    def _render_order(self, sort: Sort) -> str:
        """Render the ORDER BY items with direction and NULLS handling."""
        items: List[str] = []
        for index in range(len(sort.sort_keys)):
            items.append(self._order_item(sort, index))
        return ", ".join(items)

    def _order_item(self, sort: Sort, index: int) -> str:
        """Render a single ORDER BY key with ASC/DESC and NULLS placement."""
        item = sort.sort_keys[index].to_sql()
        if sort.ascending and not sort.ascending[index]:
            item = f"{item} DESC"
        return self._with_nulls(item, sort, index)

    def _with_nulls(self, item: str, sort: Sort, index: int) -> str:
        """Append NULLS FIRST/LAST to an ORDER BY item when specified."""
        if not sort.nulls_order or index >= len(sort.nulls_order):
            return item
        spec = sort.nulls_order[index]
        if not spec:
            return item
        return f"{item} NULLS {spec}"

    def _render_group_by(self, aggregate: Aggregate) -> Optional[str]:
        """Render the GROUP BY clause, or None when the aggregate is global."""
        if not aggregate.group_by:
            return None
        parts: List[str] = []
        for expression in aggregate.group_by:
            parts.append(expression.to_sql())
        return ", ".join(parts)

    def _render(self, context: _PushContext) -> str:
        """Assemble the full remote SELECT statement from the context."""
        select_kw = "SELECT DISTINCT" if context.distinct else "SELECT"
        select_list = ", ".join(context.select_items) if context.select_items else "*"
        from_sql = self._from_clause(context)
        query = f"{select_kw} {select_list} FROM {from_sql}"
        query = self._append_where(query, context)
        query = self._append_group_by(query, context)
        return self._append_order_limit(query, context)

    def _from_clause(self, context: _PushContext) -> str:
        """Join the base source and its join chain into a FROM clause."""
        parts = [context.from_sql or ""]
        for join_sql in context.joins:
            parts.append(join_sql)
        return " ".join(parts)

    def _append_where(self, query: str, context: _PushContext) -> str:
        """Append a combined top-level WHERE from collected scan filters."""
        if not context.where_terms:
            return query
        predicate = " AND ".join(context.where_terms)
        return f"{query} WHERE {predicate}"

    def _append_group_by(self, query: str, context: _PushContext) -> str:
        """Append the GROUP BY clause when present."""
        if context.group_sql:
            query = f"{query} GROUP BY {context.group_sql}"
        return query

    def _append_order_limit(self, query: str, context: _PushContext) -> str:
        """Append ORDER BY, LIMIT, and OFFSET clauses when present."""
        if context.order_sql:
            query = f"{query} ORDER BY {context.order_sql}"
        if context.limit is not None:
            query = f"{query} LIMIT {context.limit}"
        if context.offset:
            query = f"{query} OFFSET {context.offset}"
        return query
