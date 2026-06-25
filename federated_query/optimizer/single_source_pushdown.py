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
    LateralJoin,
    SubqueryScan,
)
from ..plan.expressions import ColumnRef, Expression, FunctionCall
from ..plan.physical import PhysicalRemoteQuery
from .decorrelation import (
    _split_conjuncts,
    _rebuild_expression,
    _expression_column_refs,
)

_JOIN_KEYWORDS = {
    JoinType.INNER: "INNER JOIN",
    JoinType.LEFT: "LEFT JOIN",
    JoinType.RIGHT: "RIGHT JOIN",
    JoinType.FULL: "FULL OUTER JOIN",
    JoinType.SEMI: "SEMI JOIN",
    JoinType.ANTI: "ANTI JOIN",
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
        self.having_terms: List[str] = []
        self.group_sql: Optional[str] = None
        self.order_sql: Optional[str] = None
        self.limit: Optional[int] = None
        self.offset: int = 0
        self.distinct: bool = False
        self.has_join: bool = False
        self.has_computed: bool = False
        self.has_aggregate: bool = False
        # True once a derived table (``FROM (SELECT ...) AS t``) is the FROM
        # source, which is worth pushing as one query even without a join.
        self.has_subquery_relation: bool = False
        # Number of derived-table relations emitted so far, used to give each a
        # unique relation alias (``subq_0``, ``subq_1`` ...).
        self.derived_count: int = 0
        # True once a column-contributing join (LEFT/INNER) absorbs a derived
        # relation, so a bare ``*`` cannot be faithfully expanded from the base
        # scans alone and the whole subtree declines to push.
        self.has_derived_columns: bool = False
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

        Declines when a column-contributing join pulled in a derived relation:
        its synthetic columns are not among the base scans, so a base-scan-only
        expansion would silently drop them.
        """
        if context.has_derived_columns:
            return False
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
        if context.has_join or context.has_subquery_relation:
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
        if isinstance(node, LateralJoin):
            return self._absorb_lateral_join(node, context)
        if isinstance(node, (Join, Scan, SubqueryScan)):
            return self._absorb_from(node, context)
        return False

    def _absorb_lateral_join(self, node: LateralJoin, context: _PushContext) -> bool:
        """Render a dependent join as ``LEFT JOIN LATERAL (...) ON TRUE``.

        The right side renders as a derived relation that keeps its outer
        column references; in LATERAL scope the source (or the DuckDB merge
        engine) evaluates it per left row. Same-source only — a cross-source
        right side fails the data-source check and declines to push.
        """
        if not self._absorb_from(node.left, context):
            return False
        right_ref = self._render_relation(node.right, context)
        if right_ref is None:
            return False
        keyword = _JOIN_KEYWORDS[node.join_type]
        context.joins.append(f"{keyword} LATERAL {right_ref} ON TRUE")
        context.has_join = True
        context.has_derived_columns = True
        return True

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
        if self._is_columns_over_aggregate(projection):
            return self._absorb_aggregate_projection(projection, context)
        if self._has_computed_expression(projection.expressions):
            context.has_computed = True
        self._set_select(projection.expressions, projection.aliases, context)
        return self._absorb(projection.input, context)

    def _absorb_aggregate_projection(
        self, projection: Projection, context: _PushContext
    ) -> bool:
        """Render a projection that selects (and often renames) an aggregate's outputs.

        Decorrelation projects an aggregate's outputs under synthetic names
        (``AVG(amount)`` re-aliased to ``__subq_0_v0``). Each selected column is
        replaced by the aggregate's real expression and emitted under the
        projection's alias, so the relation renders ``AVG(amount) AS __subq_0_v0``
        rather than selecting a non-existent base column. The aggregate child
        then renders its own GROUP BY and source.
        """
        resolved = self._resolve_against_aggregate(
            projection.expressions, projection.input
        )
        self._set_select(resolved, projection.aliases, context)
        return self._absorb(projection.input, context)

    def _resolve_against_aggregate(self, expressions, child):
        """Map each projected column to its aggregate child's source expression."""
        outputs = self._aggregate_outputs(child)
        aggregates = child.aggregates
        resolved = []
        for expression in expressions:
            resolved.append(aggregates[outputs.index(expression.column)])
        return resolved

    def _is_columns_over_aggregate(self, projection: Projection) -> bool:
        """Whether a projection selects only column refs of an aggregate child."""
        outputs = self._aggregate_outputs(projection.input)
        if outputs is None:
            return False
        return self._all_output_refs(projection.expressions, outputs)

    def _aggregate_outputs(self, node: LogicalPlanNode):
        """Output names of an aggregate-producing child (Aggregate or agg scan)."""
        if isinstance(node, Aggregate):
            return node.output_names
        if isinstance(node, Scan) and node.aggregates:
            return node.output_names
        return None

    def _all_output_refs(self, expressions, outputs) -> bool:
        """Whether every item is a bare column ref naming an aggregate output."""
        for expression in expressions:
            if not self._is_output_ref(expression, outputs):
                return False
        return True

    def _is_output_ref(self, expression, outputs) -> bool:
        """Whether expression is a column ref to one of the aggregate outputs."""
        return isinstance(expression, ColumnRef) and expression.column in outputs

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
        """Build the FROM clause from a join tree, base scan, or derived table."""
        if isinstance(node, Scan):
            return self._absorb_base_scan(node, context)
        if isinstance(node, SubqueryScan):
            return self._absorb_subquery_scan_base(node, context)
        if isinstance(node, Join):
            return self._absorb_join(node, context)
        return False

    def _absorb_subquery_scan_base(
        self, node: SubqueryScan, context: _PushContext
    ) -> bool:
        """Set a derived table as the FROM source: ``(SELECT ...) AS alias``."""
        rendered = self._render_subquery_scan(node, context)
        if rendered is None:
            return False
        context.from_sql = rendered
        context.has_subquery_relation = True
        return True

    def _absorb_base_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Set the leftmost FROM source and collect its filter."""
        if scan.aggregates or scan.group_by:
            return self._absorb_aggregate_scan(scan, context)
        if not self._claim_scan(scan, context):
            return False
        context.from_sql = self._scan_ref(scan)
        self._absorb_scan_modifiers(scan, context)
        return True

    def _absorb_scan_modifiers(self, scan: Scan, context: _PushContext) -> None:
        """Render an ORDER BY / LIMIT the optimizer folded onto a base scan.

        A folded sort or row-limit leaves no Sort/Limit node, so it is read
        straight off the scan; an explicit node above already populated these,
        so a clause that is present is never overwritten.
        """
        self._adopt_scan_order(scan, context)
        self._adopt_scan_limit(scan, context)

    def _adopt_scan_order(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded ORDER BY unless a Sort node already set one."""
        if scan.order_by_keys and context.order_sql is None:
            context.order_sql = self._scan_order_sql(scan)

    def _adopt_scan_limit(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded LIMIT unless a Limit node already set one."""
        if scan.limit is not None and context.limit is None:
            context.limit = scan.limit

    def _absorb_aggregate_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Render a scan carrying folded aggregates/GROUP BY as the FROM source.

        The optimizer collapses a single-table aggregate sub-relation (such as a
        scalar subquery's keyed aggregate) into one scan; here its aggregates
        become the SELECT list and its grouping the GROUP BY, so the relation
        pushes as ordinary aggregate SQL rather than a re-correlated subquery.
        """
        if not self._claim_source(scan, context):
            return False
        if not context.select_items:
            self._set_select(scan.aggregates, scan.output_names, context)
        self._split_scan_filter(scan, context)
        context.group_sql = self._render_group_by(scan)
        context.from_sql = self._scan_ref(scan)
        context.has_aggregate = True
        return True

    def _split_scan_filter(self, scan: Scan, context: _PushContext) -> None:
        """Route a folded aggregate-scan filter into WHERE or HAVING.

        The optimizer folds both WHERE and HAVING into one scan filter.
        A conjunct that references an aggregate output is a HAVING term; its
        synthetic output references (e.g. ``__subq_0_h1``, which decorrelation
        hoisted out of ``COUNT(*)``) are substituted back to the aggregate
        expression so the source sees ``HAVING COUNT(*) > 10``.
        """
        if scan.filters is None:
            return
        aggregate_map = self._aggregate_expr_map(scan)
        for conjunct in _split_conjuncts(scan.filters):
            self._route_conjunct(conjunct, aggregate_map, context)

    def _route_conjunct(self, conjunct, aggregate_map, context: _PushContext) -> None:
        """Place one conjunct in HAVING (aggregate refs resolved) or in WHERE."""
        if self._references_aggregate(conjunct, aggregate_map):
            resolved = self._substitute_aggregate_refs(conjunct, aggregate_map)
            context.having_terms.append(resolved.to_sql())
        else:
            context.where_terms.append(conjunct.to_sql())

    def _aggregate_expr_map(self, scan: Scan) -> Dict[str, Expression]:
        """Map each aggregate-function output name to its aggregate expression."""
        mapping: Dict[str, Expression] = {}
        for index in range(len(scan.output_names)):
            expression = scan.aggregates[index]
            if isinstance(expression, FunctionCall) and expression.is_aggregate:
                mapping[scan.output_names[index]] = expression
        return mapping

    def _references_aggregate(self, expr: Expression, aggregate_map) -> bool:
        """Whether an expression references any aggregate-output column."""
        for ref in _expression_column_refs(expr):
            if ref.column in aggregate_map:
                return True
        return False

    def _substitute_aggregate_refs(self, expr: Expression, aggregate_map) -> Expression:
        """Replace aggregate-output column refs with their aggregate expressions."""
        if isinstance(expr, ColumnRef):
            return aggregate_map.get(expr.column, expr)
        return _rebuild_expression(
            expr, lambda child: self._substitute_aggregate_refs(child, aggregate_map)
        )

    def _absorb_join(self, join: Join, context: _PushContext) -> bool:
        """Add one join to a left-deep tree, rendering its right relation.

        Every join type — including the SEMI/ANTI/LEFT joins that decorrelation
        produces from EXISTS/IN/scalar subqueries — pushes as a real SQL join.
        The right side is rendered as a relation: a base scan stays a table
        reference, a projected sub-relation becomes a derived table. No subquery
        expression is ever re-created.
        """
        keyword = _JOIN_KEYWORDS.get(join.join_type)
        if keyword is None or not self._join_is_pushable(join):
            return False
        if not self._absorb_from(join.left, context):
            return False
        right_ref = self._render_relation(join.right, context)
        if right_ref is None:
            return False
        if self._contributes_columns(join):
            context.has_derived_columns = True
        context.joins.append(self._render_join_clause(join, keyword, right_ref))
        context.has_join = True
        return True

    def _join_is_pushable(self, join: Join) -> bool:
        """A join pushes when it has an ON condition, or is NATURAL/USING."""
        return join.condition is not None or join.natural or join.using is not None

    def _contributes_columns(self, join: Join) -> bool:
        """Whether a join adds its right relation's columns to a ``*`` projection.

        SEMI/ANTI joins are existence filters and contribute no columns; any
        other join exposes the right side, and when that side is a derived
        relation its synthetic columns escape base-scan star expansion.
        """
        if join.join_type in (JoinType.SEMI, JoinType.ANTI):
            return False
        return self._is_derived_relation(join.right)

    def _is_derived_relation(self, node: LogicalPlanNode) -> bool:
        """Whether a node renders as a derived table rather than a base table ref."""
        if isinstance(node, Scan):
            return bool(node.aggregates or node.group_by)
        return True

    def _render_relation(
        self, node: LogicalPlanNode, context: _PushContext
    ) -> Optional[str]:
        """Render a join's right side as a FROM/JOIN relation reference.

        A base scan renders as its table reference; anything projected (the
        decorrelated key/value relation, or a user derived table) renders as a
        parenthesized derived table with a fresh alias.
        """
        if isinstance(node, SubqueryScan):
            return self._render_subquery_scan(node, context)
        if not self._is_derived_relation(node):
            if not self._claim_scan(node, context):
                return None
            return self._scan_ref(node)
        return self._render_derived(node, context)

    def _render_subquery_scan(
        self, node: SubqueryScan, context: _PushContext
    ) -> Optional[str]:
        """Render a derived table ``(SELECT ...) AS alias`` keeping its own alias.

        Unlike a synthetic decorrelation relation, a ``SubqueryScan`` carries a
        user-visible alias that outer columns reference (``o.amount``), so the
        alias is preserved instead of a generated ``subq_N``.
        """
        inner = _PushContext()
        if not self._absorb(node.input, inner) or not inner.select_items:
            return None
        if context.datasource is not None and inner.datasource != context.datasource:
            return None
        if context.datasource is None:
            context.datasource = inner.datasource
        return f"({self._render(inner)}) AS {node.alias}"

    def _render_derived(
        self, node: LogicalPlanNode, context: _PushContext
    ) -> Optional[str]:
        """Render a projected sub-relation as ``(SELECT ...) AS alias``.

        The sub-relation is rendered by an independent push context, so it
        carries its own SELECT/WHERE/GROUP BY; it must resolve to the same data
        source as the enclosing query. Its projection aliases (the synthetic
        decorrelation keys) are preserved verbatim so the join condition above
        still resolves them.
        """
        inner = _PushContext()
        if not self._absorb(node, inner) or not inner.select_items:
            return None
        if context.datasource is not None and inner.datasource != context.datasource:
            return None
        if context.datasource is None:
            context.datasource = inner.datasource
        alias = self._derived_alias(context)
        return f"({self._render(inner)}) AS {alias}"

    def _derived_alias(self, context: _PushContext) -> str:
        """Return a unique relation alias for the next derived table."""
        alias = f"subq_{context.derived_count}"
        context.derived_count += 1
        return alias

    def _render_join_clause(self, join: Join, keyword: str, right_ref: str) -> str:
        """Render one join clause using ON, USING, or NATURAL syntax."""
        if join.natural:
            return f"NATURAL {keyword} {right_ref}"
        if join.using is not None:
            columns = ", ".join(join.using)
            return f"{keyword} {right_ref} USING ({columns})"
        return f"{keyword} {right_ref} ON {join.condition.to_sql()}"

    def _claim_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Confirm a plain (non-aggregate) scan shares the data source."""
        if scan.group_by or scan.aggregates:
            return False
        if not self._claim_source(scan, context):
            return False
        self._claim_scan_filters(scan, context)
        return True

    def _claim_scan_filters(self, scan: Scan, context: _PushContext) -> None:
        """Collect a plain scan's filters as WHERE terms."""
        if scan.filters is not None:
            context.where_terms.append(scan.filters.to_sql())

    def _claim_source(self, scan: Scan, context: _PushContext) -> bool:
        """Bind the scan's data source; reject a cross-source scan."""
        if context.datasource is None:
            context.datasource = scan.datasource
        elif context.datasource != scan.datasource:
            return False
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
        """Render a Sort node's ORDER BY items with direction and NULLS."""
        return self._render_order_keys(sort.sort_keys, sort.ascending, sort.nulls_order)

    def _scan_order_sql(self, scan: Scan) -> str:
        """Render an ORDER BY the optimizer folded onto a scan."""
        return self._render_order_keys(
            scan.order_by_keys, scan.order_by_ascending, scan.order_by_nulls
        )

    def _render_order_keys(self, keys, ascending, nulls) -> str:
        """Render ORDER BY items from key, direction, and NULLS-order lists."""
        items: List[str] = []
        for index in range(len(keys)):
            items.append(self._order_key_item(keys, ascending, nulls, index))
        return ", ".join(items)

    def _order_key_item(self, keys, ascending, nulls, index: int) -> str:
        """Render one ORDER BY key with ASC/DESC and NULLS placement."""
        item = keys[index].to_sql()
        if ascending and not ascending[index]:
            item = f"{item} DESC"
        return self._order_key_nulls(item, nulls, index)

    def _order_key_nulls(self, item: str, nulls, index: int) -> str:
        """Append NULLS FIRST/LAST to an ORDER BY item when specified."""
        if not nulls or index >= len(nulls) or not nulls[index]:
            return item
        return f"{item} NULLS {nulls[index]}"

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
        query = self._append_having(query, context)
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

    def _append_having(self, query: str, context: _PushContext) -> str:
        """Append the HAVING clause from collected post-aggregate terms."""
        if not context.having_terms:
            return query
        predicate = " AND ".join(context.having_terms)
        return f"{query} HAVING {predicate}"

    def _append_order_limit(self, query: str, context: _PushContext) -> str:
        """Append ORDER BY, LIMIT, and OFFSET clauses when present."""
        if context.order_sql:
            query = f"{query} ORDER BY {context.order_sql}"
        if context.limit is not None:
            query = f"{query} LIMIT {context.limit}"
        if context.offset:
            query = f"{query} OFFSET {context.offset}"
        return query
