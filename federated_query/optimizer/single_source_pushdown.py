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
    SetOperation,
    SetOpKind,
    CTE,
    CTERef,
    Values,
)
from sqlglot import exp

from ..plan.expressions import ColumnRef, Expression
from ..plan.physical import PhysicalRemoteQuery
from ..plan.emit import clauses, CANONICAL_SOURCE_RESOLVER
from ..plan.emit.expressions import expression_to_ast
from ..plan.expressions import aggregate_output_map, split_where_having


def _source_ast(expr: Expression) -> exp.Expression:
    """Lower an engine expression to its canonical Postgres-form sqlglot AST."""
    return expression_to_ast(expr, CANONICAL_SOURCE_RESOLVER)


def _combine_and(terms) -> Optional[exp.Expression]:
    """AND a list of predicate ASTs into one expression, or None when empty."""
    if not terms:
        return None
    return exp.and_(*terms)

# Engine join type -> sqlglot exp.Join (kind, side), mirroring how sqlglot's own
# parser represents each keyword so the rendered SQL is identical to the prior
# parse-back. SEMI/ANTI render to WHERE [NOT] EXISTS in Postgres form, exactly as
# the parsed "SEMI/ANTI JOIN" string did.
_JOIN_KIND_SIDE = {
    JoinType.INNER: ("INNER", None),
    JoinType.LEFT: (None, "LEFT"),
    JoinType.RIGHT: (None, "RIGHT"),
    JoinType.FULL: ("OUTER", "FULL"),
    JoinType.SEMI: ("SEMI", None),
    JoinType.ANTI: ("ANTI", None),
}

# Engine set-operation kind -> sqlglot node; distinct=False renders ``... ALL``.
_SET_OP_EXP = {
    SetOpKind.UNION: exp.Union,
    SetOpKind.INTERSECT: exp.Intersect,
    SetOpKind.EXCEPT: exp.Except,
}


def same_source(left: Optional[str], right: Optional[str]) -> bool:
    """Whether two datasource names identify the same source.

    A datasource name maps 1:1 to a connection in the catalog, so the name is the
    authoritative identity for "can these run together as one source?". This is
    the single definition of that decision: the pushdown builder and the physical
    planner both use it instead of each re-deciding (one by name, one by
    connection object). A ``None`` name (no source claimed yet) matches nothing.
    """
    return left is not None and left == right


class _PushContext:
    """Mutable accumulator for the clauses of one remote query."""

    def __init__(self) -> None:
        self.datasource: Optional[str] = None
        # SELECT items, FROM relation, join chain, WHERE/HAVING predicates, and
        # GROUP BY / ORDER BY are sqlglot AST nodes (built via the one emitter),
        # assembled once by clauses.assemble_select - the same skeleton builder a
        # single-table scan uses. No SQL string is hand-crafted or parsed back.
        self.select_items: List[exp.Expression] = []
        self.output_names: List[str] = []
        self.from_node: Optional[exp.Expression] = None
        self.joins: List[exp.Join] = []
        self.where_terms: List[exp.Expression] = []
        self.having_terms: List[exp.Expression] = []
        self.group_node: Optional[exp.Group] = None
        self.order_node: Optional[exp.Order] = None
        self.limit: Optional[int] = None
        self.offset: int = 0
        self.distinct: bool = False
        self.distinct_on: Optional[List[Expression]] = None
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
        # Optional {id(scan): registered name} map used when rendering a
        # cross-source LATERAL right side for the merge engine: base scans
        # render as their registered Arrow relation name, not ``schema.table``.
        self.scan_names: Optional[Dict[int, str]] = None
        # True inside a derived-relation sub-render, where a set operation may
        # be wrapped as ``(... UNION ...) AS u``. A top-level set operation
        # (the query body) is left to the planner's set-operation path.
        self.in_derived: bool = False
        # CTE definitions to emit as a leading WITH clause, in declaration
        # order: each is (name, column_names, body_ast).
        self.ctes: List[Tuple[str, Optional[List[str]], exp.Select]] = []
        # CTE names defined by an enclosing WITH (so a body or derived sub-render
        # can reference an earlier sibling, or itself when recursive) without
        # re-emitting their definitions here.
        self.visible_ctes: List[str] = []
        # True once a CTE (WITH) wraps the query, which makes the whole subtree
        # worth pushing as one remote statement even without a join.
        self.has_cte: bool = False
        # True when any CTE in the WITH is recursive (renders WITH RECURSIVE).
        self.has_recursive_cte: bool = False


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

    def render_correlated_sql(self, node: LogicalPlanNode, scan_names) -> Optional[str]:
        """Render a single-source subtree to SQL with base scans named for the
        merge engine.

        ``scan_names`` maps ``id(scan)`` to the registered Arrow relation name a
        scan should render as (instead of ``"schema"."table"``). Outer column
        references (the LATERAL correlation) are kept verbatim. Returns the SQL
        text, or None when the subtree is not renderable as one query.
        """
        context = _PushContext()
        context.scan_names = scan_names
        if not self._absorb(node, context):
            return None
        return self._render(context).sql(dialect="postgres")

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
            context.select_items.append(
                clauses.aliased_item(exp.column(name, table=alias, quoted=True), unique)
            )
            context.column_aliases[(alias, name)] = unique

    def _should_push(self, context: _PushContext) -> bool:
        """Whether this subtree is worth replacing with one remote query."""
        if context.has_cte:
            return True
        if context.has_join or context.has_subquery_relation:
            return True
        if context.distinct_on is not None:
            # DISTINCT ON is only correct at the source (ORDER BY picks the row);
            # the local pyarrow path cannot do it, so always push it.
            return True
        return context.has_computed and not context.has_aggregate

    def _finish(self, context: _PushContext) -> Optional[PhysicalRemoteQuery]:
        """Resolve the connection, render SQL, and build the physical node."""
        datasource = self._resolve_datasource(context)
        if datasource is None:
            return None
        connection = self.catalog.get_datasource(datasource)
        if connection is None:
            return None
        if not connection.supports_capability(DataSourceCapability.JOINS):
            return None
        return PhysicalRemoteQuery(
            datasource=context.datasource,
            datasource_connection=connection,
            query_ast=self._render(context),
            output_names=context.output_names,
            column_alias_map=context.column_aliases,
        )

    def _resolve_datasource(self, context: _PushContext) -> Optional[str]:
        """Pick the target source, defaulting a pure-computation CTE.

        A recursive or constant-only CTE has no table scan, so no source was
        claimed; when exactly one source exists it owns the computation.
        """
        if context.datasource is not None:
            return context.datasource
        if not context.has_cte or len(self.catalog.datasources) != 1:
            return None
        only = next(iter(self.catalog.datasources))
        context.datasource = only
        return only

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
        if isinstance(node, CTE):
            return self._absorb_cte(node, context)
        if isinstance(node, (Join, Scan, SubqueryScan, SetOperation, CTERef)):
            return self._absorb_from(node, context)
        return False

    def _absorb_cte(self, node: CTE, context: _PushContext) -> bool:
        """Render a CTE body, record it as a WITH definition, then descend.

        The body becomes ``name [(cols)] AS (<sql>)`` in a leading WITH clause;
        the child query is folded normally and any reference to the CTE renders
        as the bare CTE name. The source executes the CTE (multi-reference
        materialization and, for ``WITH RECURSIVE``, the fixpoint).
        """
        if node.recursive:
            context.visible_ctes.append(node.name)
        body_sql = self._render_cte_body(node, context)
        if body_sql is None:
            return False
        context.ctes.append((node.name, node.column_names, body_sql))
        context.has_cte = True
        if node.recursive:
            context.has_recursive_cte = True
        return self._absorb(node.child, context)

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
        kind, side = _JOIN_KIND_SIDE[node.join_type]
        context.joins.append(
            exp.Join(
                this=exp.Lateral(this=right_ref), kind=kind, side=side, on=exp.true()
            )
        )
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
        context.order_node = clauses.order_by(
            sort.sort_keys, sort.ascending, sort.nulls_order, CANONICAL_SOURCE_RESOLVER
        )
        return self._absorb(sort.input, context)

    def _absorb_projection(self, projection: Projection, context: _PushContext) -> bool:
        """Record the SELECT list (with aliases), then descend.

        An unexpanded ``*`` projection is declined: the preprocessor normally
        expands stars before planning, so one surviving here cannot be rendered
        faithfully (``* AS "*"`` is invalid SQL) and is left to local execution.
        """
        context.distinct = projection.distinct
        context.distinct_on = projection.distinct_on
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
        """Map each projected column to its aggregate child's source expression.

        ``outputs`` are the aggregate's SQL result-column names, unique within
        the one relation, so ``index`` resolves each projected name to exactly
        one aggregate; a name that is not an output raises (caller guards with
        _is_columns_over_aggregate, so a miss here is a planner bug).
        """
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
        context.group_node = clauses.group_by(
            aggregate.group_by, aggregate.grouping_sets, CANONICAL_SOURCE_RESOLVER
        )
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
        context.where_terms.append(_source_ast(filter_node.predicate))
        return self._absorb(filter_node.input, context)

    def _absorb_from(self, node: LogicalPlanNode, context: _PushContext) -> bool:
        """Build the FROM clause from a join tree, base scan, or derived table."""
        if isinstance(node, Scan):
            return self._absorb_base_scan(node, context)
        if isinstance(node, SubqueryScan):
            return self._absorb_subquery_scan_base(node, context)
        if isinstance(node, SetOperation):
            return self._absorb_set_operation_base(node, context)
        if isinstance(node, CTERef):
            return self._absorb_cte_ref_base(node, context)
        if isinstance(node, Join):
            return self._absorb_join(node, context)
        return False

    def _absorb_cte_ref_base(self, node: CTERef, context: _PushContext) -> bool:
        """Set a CTE reference as the FROM source: the bare CTE name.

        Only a CTE defined within this same pushed WITH can render as a bare
        name; a reference to a CTE materialized elsewhere (a cross-source CTE
        the planner produces separately) declines so the subtree plans
        structurally and reads the materialized relation instead.
        """
        if not self._cte_defined(node.name, context):
            return False
        context.from_node = self._cte_ref_sql(node)
        return True

    def _cte_defined(self, name: str, context: _PushContext) -> bool:
        """Whether a CTE name is defined here or in an enclosing WITH."""
        if name in context.visible_ctes:
            return True
        for cte_name, _, _ in context.ctes:
            if cte_name == name:
                return True
        return False

    def _visible_names(self, context: _PushContext) -> List[str]:
        """CTE names a sub-render may reference: enclosing plus locally defined."""
        names = list(context.visible_ctes)
        for cte_name, _, _ in context.ctes:
            names.append(cte_name)
        return names

    def _cte_ref_sql(self, node: CTERef) -> exp.Table:
        """Build a CTE reference (its bare name), keeping a distinct alias."""
        table = exp.Table(this=exp.to_identifier(node.name))
        if node.alias and node.alias != node.name:
            table.set(
                "alias", exp.TableAlias(this=exp.to_identifier(node.alias, quoted=True))
            )
        return table

    def _absorb_set_operation_base(
        self, node: SetOperation, context: _PushContext
    ) -> bool:
        """Set a UNION/INTERSECT/EXCEPT as the FROM source: ``(... UNION ...) AS u``.

        Only inside a derived sub-render; a top-level set operation (the query
        body) is declined so the planner renders it as a bare ``UNION`` rather
        than ``SELECT * FROM (...)``.
        """
        if not context.in_derived:
            return False
        rendered = self._render_set_operation(node, context)
        if rendered is None:
            return False
        context.from_node = rendered
        context.has_subquery_relation = True
        return True

    def _render_set_operation(
        self, node: SetOperation, context: _PushContext
    ) -> Optional[exp.Subquery]:
        """Build a set operation as an aliased derived relation of its branches."""
        left = self._render_branch(node.left, context)
        right = self._render_branch(node.right, context)
        if left is None or right is None:
            return None
        alias = self._derived_alias(context)
        return self._derived_table(self._set_op_ast(node, left, right), alias)

    def _set_op_ast(
        self, node: SetOperation, left: exp.Expression, right: exp.Expression
    ) -> exp.Expression:
        """Build a UNION/INTERSECT/EXCEPT AST; ``distinct`` False renders ALL."""
        return _SET_OP_EXP[node.kind](
            this=left, expression=right, distinct=node.distinct
        )

    def _render_branch(
        self, node: LogicalPlanNode, context: _PushContext
    ) -> Optional[exp.Expression]:
        """Build one set-operation branch as a standalone SELECT, same source."""
        if isinstance(node, Values):
            return self._render_values_branch(node)
        inner = _PushContext()
        inner.scan_names = context.scan_names
        inner.in_derived = True
        inner.visible_ctes = self._visible_names(context)
        if not self._absorb(node, inner) or not inner.select_items:
            return None
        if not self._branch_source_compatible(inner, context):
            return None
        return self._render(inner)

    def _branch_source_compatible(
        self, inner: _PushContext, context: _PushContext
    ) -> bool:
        """Confirm a branch shares the source, adopting it when one is unset.

        A branch that references only CTEs has no scan and so no source of its
        own; it is compatible with any enclosing source.
        """
        if inner.datasource is None:
            return True
        if context.datasource is not None and not same_source(
            inner.datasource, context.datasource
        ):
            return False
        context.datasource = inner.datasource
        return True

    def _render_values_branch(self, node: Values) -> Optional[exp.Select]:
        """Build a single constant row as a FROM-less ``SELECT`` branch.

        Used for the base case of a recursive CTE (``SELECT 1``); a trailing
        alias is added only when it differs from the rendered expression.
        """
        if len(node.rows) != 1:
            return None
        items = []
        for expr, name in zip(node.rows[0], node.output_names):
            ast = _source_ast(expr)
            if name and name != expr.to_sql():
                ast = clauses.aliased_item(ast, name)
            items.append(ast)
        return exp.Select(expressions=items)

    def _render_cte_body(
        self, node: CTE, context: _PushContext
    ) -> Optional[exp.Expression]:
        """Build a CTE's body (the SELECT / set operation inside ``name AS (...)``)."""
        body = node.cte_plan
        if isinstance(body, SetOperation):
            return self._render_cte_set_body(body, context)
        return self._render_branch(body, context)

    def _render_cte_set_body(
        self, node: SetOperation, context: _PushContext
    ) -> Optional[exp.Expression]:
        """Build a set-operation CTE body (unwrapped, no derived-table alias)."""
        left = self._render_branch(node.left, context)
        right = self._render_branch(node.right, context)
        if left is None or right is None:
            return None
        return self._set_op_ast(node, left, right)

    def _absorb_subquery_scan_base(
        self, node: SubqueryScan, context: _PushContext
    ) -> bool:
        """Set a derived table as the FROM source: ``(SELECT ...) AS alias``."""
        rendered = self._render_subquery_scan(node, context)
        if rendered is None:
            return False
        context.from_node = rendered
        context.has_subquery_relation = True
        return True

    def _absorb_base_scan(self, scan: Scan, context: _PushContext) -> bool:
        """Set the leftmost FROM source and collect its filter."""
        if scan.aggregates or scan.group_by:
            return self._absorb_aggregate_scan(scan, context)
        if not self._claim_scan(scan, context):
            return False
        context.from_node = self._scan_ref(scan, context)
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
        self._adopt_scan_offset(scan, context)
        self._adopt_scan_distinct(scan, context)

    def _adopt_scan_order(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded ORDER BY unless a Sort node already set one."""
        if scan.order_by_keys and context.order_node is None:
            context.order_node = clauses.order_by(
                scan.order_by_keys,
                scan.order_by_ascending,
                scan.order_by_nulls,
                CANONICAL_SOURCE_RESOLVER,
            )

    def _adopt_scan_limit(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded LIMIT unless a Limit node already set one."""
        if scan.limit is not None and context.limit is None:
            context.limit = scan.limit

    def _adopt_scan_offset(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded OFFSET unless a Limit node already set one."""
        if scan.offset and context.offset == 0:
            context.offset = scan.offset

    def _adopt_scan_distinct(self, scan: Scan, context: _PushContext) -> None:
        """Adopt a scan's folded DISTINCT unless already set above."""
        if scan.distinct and not context.distinct:
            context.distinct = True

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
        context.group_node = clauses.group_by(
            scan.group_by, scan.grouping_sets, CANONICAL_SOURCE_RESOLVER
        )
        context.from_node = self._scan_ref(scan, context)
        context.has_aggregate = True
        return True

    def _split_scan_filter(self, scan: Scan, context: _PushContext) -> None:
        """Route a folded aggregate-scan filter into WHERE / HAVING.

        Delegates to the shared split_where_having (the single source of truth);
        the resulting predicates are stringified into the push context.
        """
        if scan.filters is None:
            return
        output_map = aggregate_output_map(scan.output_names, scan.aggregates)
        where_pred, having_pred = split_where_having(scan.filters, output_map)
        if where_pred is not None:
            context.where_terms.append(_source_ast(where_pred))
        if having_pred is not None:
            context.having_terms.append(_source_ast(having_pred))

    def _absorb_join(self, join: Join, context: _PushContext) -> bool:
        """Add one join to a left-deep tree, rendering its right relation.

        Every join type — including the SEMI/ANTI/LEFT joins that decorrelation
        produces from EXISTS/IN/scalar subqueries — pushes as a real SQL join.
        The right side is rendered as a relation: a base scan stays a table
        reference, a projected sub-relation becomes a derived table. No subquery
        expression is ever re-created.
        """
        if join.join_type not in _JOIN_KIND_SIDE or not self._join_is_pushable(join):
            return False
        if not self._absorb_from(join.left, context):
            return False
        right_ref = self._render_relation(join.right, context)
        if right_ref is None:
            return False
        if self._contributes_columns(join):
            context.has_derived_columns = True
        context.joins.append(self._render_join_clause(join, right_ref))
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
    ) -> Optional[exp.Expression]:
        """Build a join's right side as a FROM/JOIN relation AST.

        A base scan builds its table reference; anything projected (the
        decorrelated key/value relation, or a user derived table) builds a
        subquery (derived table) with a fresh alias.
        """
        if isinstance(node, SubqueryScan):
            return self._render_subquery_scan(node, context)
        if isinstance(node, CTERef):
            if not self._cte_defined(node.name, context):
                return None
            return self._cte_ref_sql(node)
        if not self._is_derived_relation(node):
            if not self._claim_scan(node, context):
                return None
            return self._scan_ref(node, context)
        return self._render_derived(node, context)

    def _render_subquery_scan(
        self, node: SubqueryScan, context: _PushContext
    ) -> Optional[exp.Subquery]:
        """Build a derived table ``(SELECT ...) AS alias`` keeping its own alias.

        Unlike a synthetic decorrelation relation, a ``SubqueryScan`` carries a
        user-visible alias that outer columns reference (``o.amount``), so the
        alias is preserved instead of a generated ``subq_N``.
        """
        inner = _PushContext()
        inner.scan_names = context.scan_names
        inner.in_derived = True
        inner.visible_ctes = self._visible_names(context)
        if not self._absorb(node.input, inner) or not inner.select_items:
            return None
        if context.datasource is not None and inner.datasource != context.datasource:
            return None
        if context.datasource is None:
            context.datasource = inner.datasource
        return self._derived_table(self._render(inner), node.alias)

    def _render_derived(
        self, node: LogicalPlanNode, context: _PushContext
    ) -> Optional[exp.Subquery]:
        """Build a projected sub-relation as ``(SELECT ...) AS alias``.

        The sub-relation is rendered by an independent push context, so it
        carries its own SELECT/WHERE/GROUP BY; it must resolve to the same data
        source as the enclosing query. Its projection aliases (the synthetic
        decorrelation keys) are preserved verbatim so the join condition above
        still resolves them.
        """
        inner = _PushContext()
        inner.scan_names = context.scan_names
        inner.in_derived = True
        inner.visible_ctes = self._visible_names(context)
        if not self._absorb(node, inner) or not inner.select_items:
            return None
        if context.datasource is not None and inner.datasource != context.datasource:
            return None
        if context.datasource is None:
            context.datasource = inner.datasource
        return self._derived_table(self._render(inner), self._derived_alias(context))

    def _derived_table(self, body: exp.Select, alias: str) -> exp.Subquery:
        """Wrap a sub-SELECT as an aliased derived table ``(<body>) AS alias``."""
        return exp.Subquery(
            this=body,
            alias=exp.TableAlias(this=exp.to_identifier(alias, quoted=True)),
        )

    def _derived_alias(self, context: _PushContext) -> str:
        """Return a unique relation alias for the next derived table."""
        alias = f"subq_{context.derived_count}"
        context.derived_count += 1
        return alias

    def _render_join_clause(
        self, join: Join, right_ref: exp.Expression
    ) -> exp.Join:
        """Build one join clause AST using ON, USING, or NATURAL."""
        kind, side = _JOIN_KIND_SIDE[join.join_type]
        if join.natural:
            return exp.Join(this=right_ref, kind=kind, side=side, method="NATURAL")
        if join.using is not None:
            using = []
            for column in join.using:
                using.append(exp.to_identifier(column, quoted=True))
            return exp.Join(this=right_ref, kind=kind, side=side, using=using)
        return exp.Join(
            this=right_ref, kind=kind, side=side, on=_source_ast(join.condition)
        )

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
            context.where_terms.append(_source_ast(scan.filters))

    def _claim_source(self, scan: Scan, context: _PushContext) -> bool:
        """Bind the scan's data source; reject a cross-source scan when pushing.

        In merge-engine rendering mode (``scan_names`` set) every scan becomes a
        registered Arrow relation, so scans from several sources are allowed.
        """
        if not self._source_compatible(scan, context):
            return False
        if context.datasource is None:
            context.datasource = scan.datasource
        context.scans.append(scan)
        return True

    def _source_compatible(self, scan: Scan, context: _PushContext) -> bool:
        """Whether a scan may join this push (always true when merge-rendering)."""
        if context.scan_names is not None:
            return True
        return context.datasource is None or same_source(
            context.datasource, scan.datasource
        )

    def _set_select(self, expressions, names, context: _PushContext) -> None:
        """Render the SELECT list with unique output names per column.

        Output names are made unique (``id``, ``id_1`` ...) so a result with
        columns from several tables never has two fields of the same name —
        otherwise a parent operator could not address one of them and the
        connector could not decode them. Each column reference also records a
        (qualifier, column) -> unique name entry for qualified resolution.
        """
        seen: set = set()
        items: List[exp.Expression] = []
        output_names: List[str] = []
        for index in range(len(expressions)):
            expression = expressions[index]
            base = self._base_name(expression, names, index)
            unique = self._unique_name(base, seen)
            seen.add(unique)
            output_names.append(unique)
            items.append(clauses.aliased_item(_source_ast(expression), unique))
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

    def _scan_ref(self, scan: Scan, context: _PushContext) -> exp.Table:
        """Build the FROM table reference (with alias) for a base scan.

        When the context carries a scan-name map (cross-source LATERAL rendering
        for the merge engine), a base scan references its registered Arrow
        relation name instead of ``"schema"."table"``.
        """
        from ..plan.physical import _parse_table_sample

        registered = None
        if context.scan_names is not None:
            registered = context.scan_names.get(id(scan))
        if registered is not None:
            table = exp.Table(this=exp.to_identifier(registered, quoted=True))
        else:
            table = exp.Table(
                this=exp.to_identifier(scan.table_name, quoted=True),
                db=exp.to_identifier(scan.schema_name, quoted=True),
            )
        alias = scan.alias if scan.alias else scan.table_name
        table.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
        if registered is None and scan.sample:
            table.set("sample", _parse_table_sample(scan.sample))
        return table

    def _render(self, context: _PushContext) -> exp.Select:
        """Assemble the remote SELECT AST via the one shared skeleton builder.

        The same ``clauses.assemble_select`` a single-table scan uses; this is
        only the N>1 case (a join / derived / CTE / set-op subtree). Every piece
        in the context is already a sqlglot node, so no SQL string is built or
        parsed back.
        """
        select = clauses.assemble_select(
            context.from_node,
            context.select_items if context.select_items else [exp.Star()],
            joins=context.joins,
            where=_combine_and(context.where_terms),
            group=context.group_node,
            having=_combine_and(context.having_terms),
            distinct=context.distinct,
            distinct_on=self._distinct_on_asts(context),
            order=context.order_node,
            limit=context.limit,
            offset=context.offset,
        )
        return self._with_node(select, context)

    def _distinct_on_asts(self, context: _PushContext):
        """The DISTINCT ON key ASTs, or None when there is no DISTINCT ON."""
        if context.distinct_on is None:
            return None
        keys = []
        for key in context.distinct_on:
            keys.append(_source_ast(key))
        return keys

    def _with_node(self, select: exp.Select, context: _PushContext) -> exp.Select:
        """Attach a leading WITH clause from the collected CTE definitions."""
        if not context.ctes:
            return select
        cte_nodes = []
        for name, columns, body in context.ctes:
            cte_nodes.append(self._cte_node(name, columns, body))
        select.set(
            "with_",
            exp.With(expressions=cte_nodes, recursive=context.has_recursive_cte),
        )
        return select

    def _cte_node(self, name, columns, body: exp.Expression) -> exp.CTE:
        """Build one ``name [(cols)] AS (<body>)`` CTE definition."""
        alias = exp.TableAlias(this=exp.to_identifier(name))
        if columns:
            cols = []
            for column in columns:
                cols.append(exp.to_identifier(column))
            alias.set("columns", cols)
        return exp.CTE(this=body, alias=alias)
