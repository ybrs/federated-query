"""Physical planner converts logical plans to physical plans."""

from typing import Dict, Optional, TYPE_CHECKING
from ..catalog.catalog import Catalog
from ..datasources.base import DataSource, DataSourceCapability
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Limit,
    Join,
    Aggregate,
    Explain,
    Sort,
    JoinType,
    CTE,
    CTERef,
    Union,
    SetOperation,
    SetOpKind,
    Values,
    SubqueryScan,
    SingleRowGuard,
    GroupedLimit,
    LateralJoin,
)
from ..plan.physical import (
    PhysicalPlanNode,
    PhysicalScan,
    PhysicalProjection,
    PhysicalWindow,
    PhysicalFilter,
    PhysicalLimit,
    PhysicalHashJoin,
    PhysicalRemoteJoin,
    PhysicalNestedLoopJoin,
    PhysicalHashAggregate,
    PhysicalExplain,
    PhysicalSort,
    PhysicalUnion,
    PhysicalRemoteSetOp,
    PhysicalSetOperation,
    PhysicalValues,
    PhysicalSingleRowGuard,
    PhysicalGroupedLimit,
    PhysicalLateralJoin,
    PhysicalCTE,
    PhysicalCTEScan,
    PhysicalCTEMergeQuery,
    PhysicalAliasedRelation,
)
from ..plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    Literal,
    Expression,
    split_conjuncts as _split_and,
)
from .single_source_pushdown import SingleSourcePushdown, same_source
from ..parser.errors import UnsupportedSQLError
from typing import List, Tuple
from ..model import StateModel

# Comparison operators a LATERAL correlation can use to derive a domain filter.
_COMPARISONS = {
    BinaryOpType.EQ,
    BinaryOpType.LT,
    BinaryOpType.LTE,
    BinaryOpType.GT,
    BinaryOpType.GTE,
}
# Operator flip when the inner column is on the right of the comparison.
_FLIP_OP = {
    BinaryOpType.EQ: BinaryOpType.EQ,
    BinaryOpType.LT: BinaryOpType.GT,
    BinaryOpType.LTE: BinaryOpType.GTE,
    BinaryOpType.GT: BinaryOpType.LT,
    BinaryOpType.GTE: BinaryOpType.LTE,
}


if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


class _JoinSides(StateModel):
    """Relation qualifiers and bare column names exposed by each join side."""

    left_aliases: set
    right_aliases: set
    left_names: set
    right_names: set

    @classmethod
    def create(
        cls,
        *,
        left_aliases: set,
        right_aliases: set,
        left_names: set,
        right_names: set,
    ) -> "_JoinSides":
        """Sanctioned fresh-construction path for _JoinSides.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left_aliases=left_aliases,
            right_aliases=right_aliases,
            left_names=left_names,
            right_names=right_names,
        )


class PhysicalPlanner:
    """Converts logical plans to physical plans."""

    def __init__(self, catalog: Catalog):
        """Initialize physical planner.

        Args:
            catalog: Catalog for looking up data sources
        """
        self.catalog = catalog
        self.single_source = SingleSourcePushdown(catalog)
        # CTE name -> its materializing producer, registered while a cross-source
        # CTE's child is planned so each CTERef resolves to a shared scan.
        self._cte_producers: Dict[str, PhysicalCTE] = {}

    def plan(
        self,
        logical_plan: LogicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> PhysicalPlanNode:
        """Convert logical plan to physical plan.

        Args:
            logical_plan: Logical plan to convert
            query_executor: Optional executor reference for context sharing

        Returns:
            Physical plan ready for execution
        """
        return self._plan_node(logical_plan)

    def _plan_node(self, node: LogicalPlanNode) -> PhysicalPlanNode:
        """Plan a single node."""
        remote = self.single_source.try_build(node)
        if remote is not None:
            return remote
        if isinstance(node, Scan):
            return self._plan_scan(node)
        if isinstance(node, Filter):
            return self._plan_filter(node)
        if isinstance(node, Projection):
            return self._plan_projection(node)
        if isinstance(node, Limit):
            return self._plan_limit(node)
        if isinstance(node, Sort):
            return self._plan_sort(node)
        if isinstance(node, Join):
            return self._plan_join(node)
        if isinstance(node, Aggregate):
            return self._plan_aggregate(node)
        if isinstance(node, Explain):
            return self._plan_explain(node)
        if isinstance(node, CTE):
            return self._plan_cte(node)
        if isinstance(node, CTERef):
            return self._plan_cte_ref(node)

        return self._plan_decorrelation_node(node)

    def _plan_decorrelation_node(self, node: LogicalPlanNode) -> PhysicalPlanNode:
        """Plan nodes produced by subquery decorrelation."""
        if isinstance(node, SetOperation):
            return self._plan_set_operation(node)
        if isinstance(node, Union):
            inputs = []
            for child in node.inputs:
                inputs.append(self._plan_node(child))
            # Concatenate the planned branch streams at execution, deduplicating
            # rows when the operation requested DISTINCT.
            # Lowered from a decorrelation-produced logical Union over its inputs.
            return PhysicalUnion.create(inputs=inputs, distinct=node.distinct)
        if isinstance(node, Values):
            # Emit the literal row tuples directly as an in-memory relation with
            # the given output column names.
            # Lowered from a logical Values row set produced by decorrelation.
            return PhysicalValues.create(rows=node.rows, output_names=node.output_names)
        if isinstance(node, SubqueryScan):
            # Re-expose the input relation under a new correlation name so parent
            # references resolve against the aliased scope at execution.
            # Lowered from a logical SubqueryScan wrapping its input plan.
            return PhysicalAliasedRelation.create(
                input=self._plan_node(node.input), alias=node.alias
            )
        if isinstance(node, LateralJoin):
            return self._plan_lateral_join(node)
        return self._plan_guard_node(node)

    def _plan_lateral_join(self, node: LateralJoin) -> PhysicalPlanNode:
        """Plan a cross-source LATERAL as a merge-engine dependent join.

        Same-source laterals were already pushed as one remote query by
        single-source pushdown. Here the two sides live on different sources:
        the left and the right's base relation are materialized into Arrow and
        the LATERAL runs in the in-memory DuckDB, with the base reduced to the
        left's correlation domain (a dynamic filter) before transfer.
        """
        base_scans = self._collect_base_scans(node.right)
        if len(base_scans) != 1:
            raise ValueError(
                "Cross-source LATERAL with multiple base relations is not "
                "supported yet"
            )
        base = base_scans[0]
        base_name = "lat_b0"
        right_sql = self.single_source.render_correlated_sql(
            node.right, {id(base): base_name}
        )
        if right_sql is None:
            raise ValueError("Cross-source LATERAL right side is not renderable")
        outer_alias = self._lateral_outer_alias(node)
        base_alias = base.alias if base.alias else base.table_name
        # For each left row, run the rendered LATERAL query over the base
        # relation reduced to the correlation domain, then join the results.
        # Lowered from a cross-source logical LateralJoin (left plus right base).
        return PhysicalLateralJoin.create(
            left=self._plan_node(node.left),
            left_name="lat_left",
            left_alias=outer_alias,
            base_scan=self._plan_node(base),
            base_name=base_name,
            lateral_sql=self._lateral_sql(node, outer_alias, right_sql),
            output_names=node.schema(),
            join_type=node.join_type,
            correlations=self._lateral_correlations(
                node.right, base_alias, outer_alias
            ),
        )

    def _lateral_outer_alias(self, node: LateralJoin) -> str:
        """The left relation's alias, referenced by the right's correlation."""
        scan = self._leftmost_scan(node.left)
        if scan is None:
            return "lat_left"
        return scan.alias if scan.alias else scan.table_name

    def _leftmost_scan(self, node: LogicalPlanNode) -> Optional[Scan]:
        """The leftmost base scan of a subtree."""
        if isinstance(node, Scan):
            return node
        children = node.children()
        if not children:
            return None
        return self._leftmost_scan(children[0])

    def _lateral_sql(self, node: LateralJoin, outer_alias: str, right_sql: str) -> str:
        """Build the merge-engine LATERAL SQL over the registered relations."""
        items: List[str] = []
        for column in node.left.schema():
            items.append(f'{outer_alias}."{column}"')
        for column in node.right.schema():
            items.append(f'sub."{column}"')
        keyword = "LEFT JOIN" if node.join_type == JoinType.LEFT else "INNER JOIN"
        return (
            f"SELECT {', '.join(items)} FROM lat_left AS {outer_alias} "
            f"{keyword} LATERAL ({right_sql}) AS sub ON TRUE"
        )

    def _lateral_correlations(
        self, right: LogicalPlanNode, base_alias: str, outer_alias: str
    ) -> List[Tuple[str, BinaryOpType, str]]:
        """Extract (inner col, op, outer col) terms for the dynamic filter."""
        terms: List[Tuple[str, BinaryOpType, str]] = []
        for predicate in self._lateral_predicates(right):
            term = self._correlation_term(predicate, base_alias, outer_alias)
            if term is not None:
                terms.append(term)
        return terms

    def _lateral_predicates(self, node: LogicalPlanNode) -> List[Expression]:
        """All conjuncts of every Filter in a subtree."""
        predicates: List[Expression] = []
        if isinstance(node, Filter):
            predicates.extend(_split_and(node.predicate))
        for child in node.children():
            predicates.extend(self._lateral_predicates(child))
        return predicates

    def _correlation_term(self, predicate, base_alias, outer_alias):
        """Decompose ``inner OP outer`` into a normalized (inner, op, outer)."""
        if not isinstance(predicate, BinaryOp) or predicate.op not in _COMPARISONS:
            return None
        left, right, op = predicate.left, predicate.right, predicate.op
        if self._is_ref(left, base_alias) and self._is_ref(right, outer_alias):
            return (left.column, op, right.column)
        if self._is_ref(left, outer_alias) and self._is_ref(right, base_alias):
            return (right.column, _FLIP_OP[op], left.column)
        return None

    def _is_ref(self, expr, alias: str) -> bool:
        """Whether expr is a column ref qualified by the given alias."""
        return isinstance(expr, ColumnRef) and expr.table == alias

    def _plan_guard_node(self, node: LogicalPlanNode) -> PhysicalPlanNode:
        """Plan cardinality guard and per-key limit nodes."""
        if isinstance(node, SingleRowGuard):
            # Enforce at most one input row per key group at execution, raising
            # when a scalar-subquery source returns more than one.
            # Lowered from a logical SingleRowGuard over its input.
            return PhysicalSingleRowGuard.create(
                input=self._plan_node(node.input), keys=node.keys
            )
        if isinstance(node, GroupedLimit):
            # Keep only the first rows within each key group at execution,
            # ordered by the per-group sort keys.
            # Lowered from a logical GroupedLimit over its input.
            return PhysicalGroupedLimit.create(
                input=self._plan_node(node.input),
                keys=node.keys,
                limit=node.limit,
                order_by_keys=node.order_by_keys,
                order_by_ascending=node.order_by_ascending,
                order_by_nulls=node.order_by_nulls,
            )
        raise ValueError(f"Unsupported logical plan node: {type(node)}")

    def _plan_scan(self, scan: Scan) -> PhysicalScan:
        """Plan a scan node."""
        datasource = self.catalog.get_datasource(scan.datasource)
        if datasource is None:
            raise ValueError(f"Data source not found: {scan.datasource}")

        # Push the projected columns, filters and any aggregation down to the
        # owning source and stream the matching rows back as Arrow batches.
        # Lowered from a logical Scan bound to a resolved datasource connection.
        return PhysicalScan.create(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=scan.filters,
            datasource_connection=datasource,
            group_by=scan.group_by,
            grouping_sets=scan.grouping_sets,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            alias=scan.alias,
            sample=scan.sample,
            limit=scan.limit,
            offset=scan.offset,
            order_by_keys=scan.order_by_keys,
            order_by_ascending=scan.order_by_ascending,
            order_by_nulls=scan.order_by_nulls,
            distinct=scan.distinct,
        )

    def _plan_filter(self, filter_node: Filter) -> PhysicalFilter:
        """Plan a filter node."""
        input_plan = self._plan_node(filter_node.input)
        # Evaluate the predicate against each input row at execution and drop
        # the rows that do not satisfy it.
        # Lowered from a logical Filter over its planned input.
        return PhysicalFilter.create(input=input_plan, predicate=filter_node.predicate)

    def _plan_projection(self, projection: Projection) -> PhysicalPlanNode:
        """Plan a projection; window-bearing ones run in the merge engine."""
        input_plan = self._plan_node(projection.input)
        if self._projection_has_window(projection):
            # DISTINCT (incl. DISTINCT ON) over a window is rejected at parse by
            # _reject_distinct_window, so a window projection is never distinct
            # here - no distinct handling needed on this path.
            # Evaluate the window functions over their partitions and frames in
            # the merge engine, appending the computed columns to each row.
            # Lowered from a logical Projection that carries window expressions.
            return PhysicalWindow.create(
                input=input_plan,
                expressions=projection.expressions,
                output_names=projection.aliases,
            )
        if projection.distinct:
            self._propagate_distinct(input_plan)
        # Evaluate the projection expressions per input row at execution,
        # optionally deduplicating on the DISTINCT (ON) keys.
        # Lowered from a logical Projection over its planned input.
        return PhysicalProjection.create(
            input=input_plan,
            expressions=projection.expressions,
            output_names=projection.aliases,
            distinct=projection.distinct,
            distinct_on=projection.distinct_on,
        )

    def _projection_has_window(self, projection: Projection) -> bool:
        """Whether any projection expression contains a window function.

        Recurses the expression tree so a window nested inside an expression
        (e.g. ``row_number() OVER (...) + 1``) still selects PhysicalWindow.
        """
        for expr in projection.expressions:
            if self._expression_has_window(expr):
                return True
        return False

    def _expression_has_window(self, expr) -> bool:
        """Whether an expression tree contains a WindowExpr at any depth."""
        from ..plan.expressions import WindowExpr, expression_children

        if isinstance(expr, WindowExpr):
            return True
        for child in expression_children(expr):
            if self._expression_has_window(child):
                return True
        return False

    def _propagate_distinct(self, node: PhysicalPlanNode) -> None:
        """Mark the lowest scan-like node to emit DISTINCT."""
        if isinstance(node, PhysicalScan):
            node.distinct = True
            return
        if isinstance(node, PhysicalRemoteJoin):
            node.distinct = True
            return
        child = getattr(node, "input", None)
        if isinstance(child, PhysicalPlanNode):
            self._propagate_distinct(child)

    def _plan_limit(self, limit: Limit) -> PhysicalPlanNode:
        """Plan a limit node, folding it into a pushed-down set operation."""
        input_plan = self._plan_node(limit.input)
        if isinstance(input_plan, PhysicalRemoteSetOp):
            input_plan.limit = limit.limit
            input_plan.offset = limit.offset
            return input_plan
        # Stop pulling from the input once offset rows are skipped and limit
        # rows have been emitted at execution.
        # Lowered from a logical Limit that could not fold into a pushed set op.
        return PhysicalLimit.create(
            input=input_plan, limit=limit.limit, offset=limit.offset
        )

    def _plan_sort(self, sort: Sort) -> PhysicalPlanNode:
        """Plan a sort node."""
        input_plan = self._plan_node(sort.input)

        if isinstance(input_plan, PhysicalScan):
            input_plan.order_by_keys = sort.sort_keys
            input_plan.order_by_ascending = sort.ascending
            input_plan.order_by_nulls = sort.nulls_order
            return input_plan

        if isinstance(input_plan, PhysicalRemoteJoin):
            input_plan.order_by_keys = sort.sort_keys
            input_plan.order_by_ascending = sort.ascending
            input_plan.order_by_nulls = sort.nulls_order
            return input_plan

        if isinstance(input_plan, PhysicalRemoteSetOp):
            input_plan.order_by_keys = sort.sort_keys
            input_plan.order_by_ascending = sort.ascending
            input_plan.order_by_nulls = sort.nulls_order
            return input_plan

        # Materialize the input and order it by the sort keys at execution,
        # honoring each key's direction and null placement.
        # Lowered from a logical Sort whose input is not a pushdown target.
        return PhysicalSort.create(
            input=input_plan,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _plan_aggregate(self, aggregate: Aggregate) -> PhysicalPlanNode:
        """Plan an aggregate node."""
        input_plan = self._plan_node(aggregate.input)
        if isinstance(input_plan, PhysicalRemoteJoin):
            return self._plan_remote_join_aggregate(aggregate, input_plan)
        # Build a hash table keyed by the grouping columns and accumulate the
        # aggregate states per group at execution.
        # Lowered from a logical Aggregate whose input is not a remote join.
        return PhysicalHashAggregate.create(
            input=input_plan,
            group_by=aggregate.group_by,
            aggregates=aggregate.aggregates,
            output_names=aggregate.output_names,
            grouping_sets=aggregate.grouping_sets,
        )

    def _plan_remote_join_aggregate(
        self, aggregate: Aggregate, remote_join: PhysicalRemoteJoin
    ) -> PhysicalRemoteJoin:
        """Fold an aggregate onto a remote join so both push as one remote query.

        Uses model_copy so the join's own fields (distinct/order_by_*) survive;
        re-listing fields would silently drop any omitted one.
        """
        return remote_join.model_copy(
            update={
                "group_by": aggregate.group_by,
                "grouping_sets": aggregate.grouping_sets,
                "aggregates": aggregate.aggregates,
                "output_names": aggregate.output_names,
            }
        )

    def _plan_explain(self, explain: Explain) -> PhysicalExplain:
        """Plan an explain node."""
        child_plan = self._plan_node(explain.input)
        # Render the planned child tree as EXPLAIN output in the requested
        # format at execution instead of running it.
        # Lowered from a logical Explain wrapping its input plan.
        return PhysicalExplain.create(child=child_plan, format=explain.format)

    def _plan_cte(self, cte: CTE) -> PhysicalPlanNode:
        """Plan a CTE that single-source pushdown declined (it spans sources).

        A same-source CTE is pushed whole as a remote ``WITH`` before this
        point. A recursive cross-source CTE runs entirely in the merge engine
        (the fixpoint needs one engine); a non-recursive one materializes its
        body once and feeds every reference.
        """
        if cte.recursive:
            return self._plan_recursive_cte_merge(cte)
        return self._plan_cross_source_cte(cte)

    def _plan_cross_source_cte(self, cte: CTE) -> PhysicalPlanNode:
        """Materialize the body once; plan the child with the name in scope."""
        # Materialize the CTE body once at execution and expose it under its
        # name so every reference reads the same buffered result.
        # Lowered from a non-recursive cross-source logical CTE's body plan.
        producer = PhysicalCTE.create(
            name=cte.name,
            body=self._plan_node(cte.cte_plan),
            column_names=cte.column_names,
        )
        saved = self._cte_producers.get(cte.name)
        self._cte_producers[cte.name] = producer
        child_plan = self._plan_node(cte.child)
        self._restore_cte_producer(cte.name, saved)
        return child_plan

    def _restore_cte_producer(self, name: str, saved: Optional[PhysicalCTE]) -> None:
        """Restore the producer registry after planning a CTE's child."""
        if saved is None:
            del self._cte_producers[name]
        else:
            self._cte_producers[name] = saved

    def _plan_cte_ref(self, node: CTERef) -> PhysicalPlanNode:
        """Resolve a CTE reference to a scan over its materialized producer."""
        producer = self._cte_producers.get(node.name)
        if producer is None:
            raise ValueError(f"CTE '{node.name}' is not in scope")
        # Read the already-materialized CTE producer's buffered rows under the
        # reference's alias at execution.
        # Lowered from a logical CTERef resolved to its in-scope producer.
        return PhysicalCTEScan.create(producer=producer, alias=node.alias)

    def _plan_recursive_cte_merge(self, cte: CTE) -> PhysicalPlanNode:
        """Run a cross-source recursive CTE wholly in the merge engine.

        Every base source relation in the CTE is materialized into Arrow under a
        generated name; the rendered ``WITH RECURSIVE`` references those names so
        the in-memory DuckDB runs the recursion locally.
        """
        scans = self._collect_base_scans(cte)
        scan_names = {}
        inputs = {}
        for index in range(len(scans)):
            register_name = f"cte_src_{index}"
            scan_names[id(scans[index])] = register_name
            inputs[register_name] = self._plan_node(scans[index])
        sql = self.single_source.render_correlated_sql(cte, scan_names)
        if sql is None:
            raise ValueError(
                f"Recursive CTE '{cte.name}' is not renderable for the merge engine"
            )
        # Run the rendered WITH RECURSIVE over the materialized base relations
        # in the in-memory merge engine so the fixpoint iterates in one engine.
        # Lowered from a recursive cross-source logical CTE.
        return PhysicalCTEMergeQuery.create(
            sql=sql, inputs=inputs, output_names=cte.schema()
        )

    def _collect_base_scans(self, node: LogicalPlanNode) -> List[Scan]:
        """Collect every base ``Scan`` in a subtree (CTE references excluded)."""
        if isinstance(node, Scan):
            return [node]
        scans: List[Scan] = []
        for child in node.children():
            scans.extend(self._collect_base_scans(child))
        return scans

    def _plan_join(self, join: Join) -> PhysicalPlanNode:
        """Plan a join: try remote pushdown, then a hash join, else nested loop."""
        left_plan = self._plan_node(join.left)
        right_plan = self._plan_node(join.right)

        remote = self._try_plan_remote_join(join, left_plan, right_plan)
        if remote:
            return remote
        if join.condition is None and (join.natural or join.using):
            # Same-source NATURAL/USING is rendered as-is by the remote path
            # above. Reaching here means a cross-source NATURAL/USING join whose
            # name-match equality the merge engine cannot resolve; building a
            # conditionless nested loop would be a silent Cartesian product.
            raise UnsupportedSQLError(
                "cross-source NATURAL/USING join is not supported; "
                "use an explicit ON condition"
            )
        hash_join = self._plan_hash_join_or_none(join, left_plan, right_plan)
        if hash_join:
            return hash_join
        return self._plan_nested_loop(join, left_plan, right_plan)

    def _plan_hash_join_or_none(
        self, join: Join, left_plan: PhysicalPlanNode, right_plan: PhysicalPlanNode
    ) -> Optional[PhysicalPlanNode]:
        """Build a hash join when the condition yields equi-join keys, else None."""
        if join.condition is None:
            return None
        join_keys = self._extract_join_keys(join.condition)
        if not join_keys:
            return None
        left_keys, right_keys = self._orient_join_keys(join_keys, join)
        # Build a hash table on one side's equi-keys and probe it with the
        # other side at execution, emitting matched (and outer) rows.
        # Lowered from a logical Join whose condition yields equi-join keys.
        hash_join = PhysicalHashJoin.create(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            left_keys=left_keys,
            right_keys=right_keys,
            build_side=self._choose_build_side(join.join_type, left_plan, right_plan),
            estimated_rows=join.estimated_rows,
            estimate_defaults=join.estimate_defaults,
        )
        self._mark_dynamic_filter(hash_join)
        return hash_join

    def _plan_nested_loop(
        self, join: Join, left_plan: PhysicalPlanNode, right_plan: PhysicalPlanNode
    ) -> PhysicalNestedLoopJoin:
        """Fall back to a nested-loop join (no equi-keys, or no join condition)."""
        # Evaluate the join condition for every left/right row pair at
        # execution, the general fallback when no equi-keys exist.
        # Lowered from a logical Join with no usable equi-join keys.
        return PhysicalNestedLoopJoin.create(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
            estimated_rows=join.estimated_rows,
            estimate_defaults=join.estimate_defaults,
        )

    def _choose_build_side(
        self,
        join_type: JoinType,
        left_plan: PhysicalPlanNode,
        right_plan: PhysicalPlanNode,
    ) -> str:
        """Build from the side with a selective equality filter (likely small).

        That side becomes the hash-table build input and the other becomes the
        for SEMI/ANTI/outer joins the build side is fixed by semantics (the
        right input is the inner/built side). Defaults to the right input when
        neither (or both) side has such a filter.
        """
        if join_type != JoinType.INNER:
            return "right"
        if self._has_selective_filter(left_plan) and not self._has_selective_filter(
            right_plan
        ):
            return "left"
        return "right"

    def _has_selective_filter(self, plan: PhysicalPlanNode) -> bool:
        """Whether the plan's underlying scan filters on a column = literal."""
        scan = self._find_scan(plan)
        if scan is None:
            return False
        return self._filter_has_literal_equality(scan.filters)

    def _find_scan(self, plan: PhysicalPlanNode) -> Optional[PhysicalScan]:
        """Descend single-child wrappers to the underlying scan, if any."""
        current = plan
        while True:
            if isinstance(current, PhysicalScan):
                return current
            children = current.children()
            if len(children) != 1:
                return None
            current = children[0]

    def _filter_has_literal_equality(self, predicate) -> bool:
        """Whether a predicate contains a ``column = literal`` conjunct."""
        if not isinstance(predicate, BinaryOp):
            return False
        if predicate.op == BinaryOpType.EQ:
            return self._is_column_literal_eq(predicate)
        if predicate.op == BinaryOpType.AND:
            return self._and_has_literal_equality(predicate)
        return False

    def _and_has_literal_equality(self, predicate: BinaryOp) -> bool:
        """Recurse into both sides of an AND for a column = literal conjunct."""
        if self._filter_has_literal_equality(predicate.left):
            return True
        return self._filter_has_literal_equality(predicate.right)

    def _is_column_literal_eq(self, predicate: BinaryOp) -> bool:
        """Whether an equality compares a column reference to a literal."""
        return {type(predicate.left), type(predicate.right)} == {ColumnRef, Literal}

    def _mark_dynamic_filter(self, hash_join: PhysicalHashJoin) -> None:
        """Mark the probe-side scan that will receive a runtime IN filter.

        The probe is the side that is not the build side. Marking lets EXPLAIN
        show the dynamic filter even though its values are only known at
        execution time. Only INNER joins with a single-column key qualify.
        """
        if hash_join.join_type != JoinType.INNER:
            return
        probe, probe_keys = self._probe_side(hash_join)
        if len(probe_keys) != 1:
            return
        if isinstance(probe, PhysicalScan):
            probe.dynamic_filter_keys = list(probe_keys)

    def _probe_side(
        self, hash_join: PhysicalHashJoin
    ) -> Tuple[PhysicalPlanNode, List[ColumnRef]]:
        if hash_join.build_side == "right":
            return hash_join.left, hash_join.left_keys
        return hash_join.right, hash_join.right_keys

    def _orient_join_keys(
        self,
        join_keys: Tuple[List[ColumnRef], List[ColumnRef]],
        join: Join,
    ) -> Tuple[List[ColumnRef], List[ColumnRef]]:
        """Assign each equality's columns to the join side that has them.

        Join conditions do not guarantee that the equality's left
        expression references the join's left input (decorrelated
        conditions often have the opposite orientation). Each pair is
        checked against the logical inputs' output column names and
        swapped when needed; pairs that cannot be placed by name keep
        their original orientation.
        """
        # Bundle each input's relation aliases and output column names so every
        # equality's columns can be assigned to the correct join side.
        # Built from the logical Join's left and right relation metadata.
        sides = _JoinSides.create(
            left_aliases=self._relation_aliases(join.left),
            right_aliases=self._relation_aliases(join.right),
            left_names=set(join.left.schema()),
            right_names=set(join.right.schema()),
        )
        left_keys: List[ColumnRef] = []
        right_keys: List[ColumnRef] = []
        for index in range(len(join_keys[0])):
            first, second = self._orient_key_pair(
                join_keys[0][index], join_keys[1][index], sides
            )
            left_keys.append(first)
            right_keys.append(second)
        return left_keys, right_keys

    def _orient_key_pair(
        self,
        first: ColumnRef,
        second: ColumnRef,
        sides: "_JoinSides",
    ) -> Tuple[ColumnRef, ColumnRef]:
        """Place one equality's columns onto the (left, right) sides.

        Table qualifiers are authoritative: when both inputs expose the same
        bare column names, only the alias disambiguates which side a column
        belongs to. Unqualified columns (e.g. decorrelation-renamed keys)
        fall back to matching by bare column name.
        """
        by_alias = self._orient_by_alias(first, second, sides)
        if by_alias is not None:
            return by_alias
        return self._orient_by_name(first, second, sides)

    def _orient_by_alias(
        self, first: ColumnRef, second: ColumnRef, sides: "_JoinSides"
    ) -> Optional[Tuple[ColumnRef, ColumnRef]]:
        """Orient using the columns' table qualifiers, if both are known."""
        if first.table in sides.left_aliases and second.table in sides.right_aliases:
            return first, second
        if first.table in sides.right_aliases and second.table in sides.left_aliases:
            return second, first
        return None

    def _orient_by_name(
        self, first: ColumnRef, second: ColumnRef, sides: "_JoinSides"
    ) -> Tuple[ColumnRef, ColumnRef]:
        """Orient using bare column names when qualifiers do not resolve.

        Raises when neither column resolves to a side: the planner cannot tell
        which key belongs to which input, and keeping the original order would
        key the hash join on mismatched columns (wrong or empty results).
        """
        if first.column in sides.left_names and second.column in sides.right_names:
            return first, second
        if first.column in sides.right_names and second.column in sides.left_names:
            return second, first
        raise ValueError(
            f"cannot orient join keys '{first.column}' / '{second.column}' to a "
            "join side; neither resolves by qualifier or by column name"
        )

    def _relation_aliases(self, plan) -> set:
        """Collect the relation aliases/names a plan subtree exposes."""
        aliases: set = set()
        self._collect_relation_aliases(plan, aliases)
        return aliases

    def _collect_relation_aliases(self, plan, aliases: set) -> None:
        """Recursively gather the relation qualifiers a subtree EXPOSES.

        Stops at a SubqueryScan boundary - its alias is exposed, its internals
        are not (SQL scoping). This is deliberately NOT decorrelation's
        _collect_inner_aliases, which recurses into a subquery to find every
        inner name for correlation analysis; the two answer different questions.
        """
        if isinstance(plan, Scan):
            aliases.add(plan.alias if plan.alias else plan.table_name)
            return
        if isinstance(plan, SubqueryScan):
            aliases.add(plan.alias)
            return
        for child in plan.children():
            self._collect_relation_aliases(child, aliases)

    def _try_plan_remote_join(
        self, join: Join, left_plan: PhysicalPlanNode, right_plan: PhysicalPlanNode
    ) -> Optional[PhysicalPlanNode]:
        """Build a single-source PhysicalRemoteJoin when both sides allow it.

        Carries either side's pushed ORDER BY onto the remote join; returns None
        when the join is not a same-source candidate (see _is_remote_join_candidate).
        """
        if not self._is_remote_join_candidate(join, left_plan, right_plan):
            return None
        order_keys = None
        order_asc = None
        order_nulls = None
        if isinstance(left_plan, PhysicalScan) and left_plan.order_by_keys:
            order_keys = left_plan.order_by_keys
            order_asc = left_plan.order_by_ascending
            order_nulls = left_plan.order_by_nulls
        if (
            isinstance(right_plan, PhysicalScan)
            and right_plan.order_by_keys
            and order_keys is None
        ):
            order_keys = right_plan.order_by_keys
            order_asc = right_plan.order_by_ascending
            order_nulls = right_plan.order_by_nulls
        # Render both scans and their ON condition as one SQL join sent to the
        # shared source, streaming only the joined result back.
        # Lowered from a same-source logical Join over two plain scans.
        return PhysicalRemoteJoin.create(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
            datasource_connection=left_plan.datasource_connection,
            order_by_keys=order_keys,
            order_by_ascending=order_asc,
            order_by_nulls=order_nulls,
        )

    def _is_remote_join_candidate(
        self, join: Join, left_plan: PhysicalPlanNode, right_plan: PhysicalPlanNode
    ) -> bool:
        """Whether a join can run on one source: INNER/LEFT/RIGHT with a condition
        and both sides plain scans on the same datasource and connection."""
        allowed = {JoinType.INNER, JoinType.LEFT, JoinType.RIGHT}
        if join.join_type not in allowed or join.condition is None:
            return False
        if not isinstance(left_plan, PhysicalScan):
            return False
        if not isinstance(right_plan, PhysicalScan):
            return False
        if not same_source(left_plan.datasource, right_plan.datasource):
            return False
        ds = left_plan.datasource_connection
        if ds is None:
            return False
        if not ds.supports_capability(DataSourceCapability.JOINS):
            return False
        if left_plan.group_by or left_plan.aggregates:
            return False
        if right_plan.group_by or right_plan.aggregates:
            return False
        return self._extract_join_keys(join.condition) is not None

    def _extract_join_keys(
        self, condition: Optional[BinaryOp]
    ) -> Optional[Tuple[List[ColumnRef], List[ColumnRef]]]:
        """Extract equi-join keys from condition.

        Returns:
            Tuple of (left_keys, right_keys) if this is an equi-join,
            None otherwise
        """
        if condition is None:
            return None

        if not isinstance(condition, BinaryOp):
            return None

        if condition.op == BinaryOpType.EQ:
            left_expr = condition.left
            right_expr = condition.right

            if isinstance(left_expr, ColumnRef) and isinstance(right_expr, ColumnRef):
                return ([left_expr], [right_expr])

        if condition.op == BinaryOpType.AND:
            left_keys = self._extract_join_keys(condition.left)
            right_keys = self._extract_join_keys(condition.right)

            if left_keys and right_keys:
                return (
                    left_keys[0] + right_keys[0],
                    left_keys[1] + right_keys[1],
                )

        return None

    def _plan_set_operation(self, set_op: SetOperation) -> PhysicalPlanNode:
        """Plan a set operation, pushing it down when both branches share a source."""
        left = self._plan_node(set_op.left)
        right = self._plan_node(set_op.right)
        remote = self._try_plan_remote_set_op(set_op, left, right)
        if remote is not None:
            return remote
        return self._plan_local_set_op(set_op, left, right)

    def _try_plan_remote_set_op(
        self,
        set_op: SetOperation,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
    ) -> Optional[PhysicalPlanNode]:
        """Build a single remote set operation when both branches push together."""
        left_branch = self._as_pushable_set_branch(left)
        right_branch = self._as_pushable_set_branch(right)
        if left_branch is None or right_branch is None:
            return None
        if not same_source(left_branch.datasource, right_branch.datasource):
            return None
        connection = left_branch.datasource_connection
        if connection is None:
            return None
        # Send both branches to the shared source as one UNION/INTERSECT/EXCEPT
        # query at execution and stream the combined result back.
        # Lowered from a logical SetOperation whose branches share one source.
        return PhysicalRemoteSetOp.create(
            left=left_branch,
            right=right_branch,
            kind=set_op.kind,
            distinct=set_op.distinct,
            datasource=left_branch.datasource,
            datasource_connection=connection,
        )

    def _as_pushable_set_branch(
        self, node: PhysicalPlanNode
    ) -> Optional[PhysicalPlanNode]:
        """Return a single-source branch (scan or nested remote set op) or None."""
        if isinstance(node, (PhysicalScan, PhysicalRemoteSetOp)):
            return node
        if isinstance(node, PhysicalProjection):
            return self._collapse_projection_scan(node)
        return None

    def _collapse_projection_scan(
        self, projection: PhysicalProjection
    ) -> Optional[PhysicalScan]:
        """Fold a plain-column projection over a scan into one scan.

        A set-operation branch plans to a projection over a scan that
        over-selects filter columns. When the projection only renames nothing
        and selects bare columns, those columns become the scan's output and
        the projection disappears. Computed or aliased projections are left
        for local execution (G2 covers their pushdown).
        """
        scan = projection.input
        if not isinstance(scan, PhysicalScan):
            return None
        if scan.group_by or scan.aggregates:
            return None
        columns = self._bare_projection_columns(projection)
        if columns is None:
            return None
        return self._scan_with_columns(scan, columns)

    def _bare_projection_columns(
        self, projection: PhysicalProjection
    ) -> Optional[List[str]]:
        """Return projected column names when every item is an unaliased column."""
        names = projection.output_names or []
        columns: List[str] = []
        for index in range(len(projection.expressions)):
            expression = projection.expressions[index]
            if not isinstance(expression, ColumnRef):
                return None
            if index < len(names) and names[index] != expression.column:
                return None
            columns.append(expression.column)
        return columns

    def _scan_with_columns(
        self, scan: PhysicalScan, columns: List[str]
    ) -> PhysicalScan:
        """Clone a scan, replacing its projected column list.

        Uses model_copy so every other field (including sample/grouping_sets)
        is preserved; re-listing fields would silently drop any omitted one.
        """
        return scan.model_copy(update={"columns": columns})

    def _plan_local_set_op(
        self,
        set_op: SetOperation,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
    ) -> PhysicalPlanNode:
        """Evaluate a set operation locally when it cannot be pushed down."""
        if set_op.kind == SetOpKind.UNION:
            # Concatenate both branch streams locally at execution, applying
            # DISTINCT deduplication for UNION (not UNION ALL).
            # Lowered from a logical UNION SetOperation that cannot be pushed down.
            return PhysicalUnion.create(inputs=[left, right], distinct=set_op.distinct)
        # Compute the INTERSECT/EXCEPT of the two branch streams locally at
        # execution, deduplicating per the operation's DISTINCT flag.
        # Lowered from a logical SetOperation that cannot be pushed down.
        return PhysicalSetOperation.create(
            left=left, right=right, kind=set_op.kind, distinct=set_op.distinct
        )

    def __repr__(self) -> str:
        return "PhysicalPlanner()"
