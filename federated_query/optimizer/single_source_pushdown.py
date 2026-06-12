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
from ..plan.expressions import ColumnRef
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
        """Build a unique-aliased SELECT list from every scanned column.

        Used when no projection supplied the columns (a bare ``SELECT *`` over a
        join). Emitting ``*`` would yield duplicate column names that a parent
        operator cannot resolve, so each table's columns are listed explicitly
        with unique aliases and recorded for qualified resolution.
        """
        seen: set = set()
        for scan in context.scans:
            table = self.catalog.get_table(
                scan.datasource, scan.schema_name, scan.table_name
            )
            if table is None:
                return False
            self._expand_scan_columns(scan, table, seen, context)
        return True

    def _expand_scan_columns(self, scan, table, seen: set, context) -> None:
        """Append one scan's columns as unique-aliased SELECT items."""
        alias = scan.alias if scan.alias else scan.table_name
        for column in table.columns:
            unique = self._unique_name(column.name, seen)
            seen.add(unique)
            context.output_names.append(unique)
            context.select_items.append(f'{alias}."{column.name}" AS "{unique}"')
            context.column_aliases[(alias, column.name)] = unique

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
        """Record the SELECT list (with aliases), then descend."""
        context.distinct = projection.distinct
        if self._has_computed_expression(projection.expressions):
            context.has_computed = True
        self._set_select(projection.expressions, projection.aliases, context)
        return self._absorb(projection.input, context)

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
        keyword = _JOIN_KEYWORDS.get(join.join_type)
        if keyword is None or join.condition is None:
            return False
        if not isinstance(join.right, Scan):
            return False
        if not self._absorb_from(join.left, context):
            return False
        if not self._claim_scan(join.right, context):
            return False
        condition_sql = join.condition.to_sql()
        right_ref = self._scan_ref(join.right)
        context.joins.append(f"{keyword} {right_ref} ON {condition_sql}")
        context.has_join = True
        return True

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
