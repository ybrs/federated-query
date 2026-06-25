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
)
from ..plan.expressions import BinaryOp, BinaryOpType, ColumnRef, Literal, Expression
from .single_source_pushdown import SingleSourcePushdown
from typing import List, Tuple
from dataclasses import dataclass

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


def _split_and(expr: Expression) -> List[Expression]:
    """Flatten a predicate into its top-level AND conjuncts."""
    if isinstance(expr, BinaryOp) and expr.op == BinaryOpType.AND:
        return _split_and(expr.left) + _split_and(expr.right)
    return [expr]


if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


@dataclass(frozen=True)
class _JoinSides:
    """Relation qualifiers and bare column names exposed by each join side."""

    left_aliases: set
    right_aliases: set
    left_names: set
    right_names: set


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
            return PhysicalUnion(inputs=inputs, distinct=node.distinct)
        if isinstance(node, Values):
            return PhysicalValues(rows=node.rows, output_names=node.output_names)
        if isinstance(node, SubqueryScan):
            return self._plan_node(node.input)
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
        base_scans = self._lateral_base_scans(node.right)
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
        return PhysicalLateralJoin(
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

    def _lateral_base_scans(self, node: LogicalPlanNode) -> List[Scan]:
        """Collect the base scans of a LATERAL right side."""
        if isinstance(node, Scan):
            return [node]
        scans: List[Scan] = []
        for child in node.children():
            scans.extend(self._lateral_base_scans(child))
        return scans

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
            return PhysicalSingleRowGuard(
                input=self._plan_node(node.input), keys=node.keys
            )
        if isinstance(node, GroupedLimit):
            return PhysicalGroupedLimit(
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

        return PhysicalScan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=scan.filters,
            datasource_connection=datasource,
            group_by=scan.group_by,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            alias=scan.alias,
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
        return PhysicalFilter(input=input_plan, predicate=filter_node.predicate)

    def _plan_projection(self, projection: Projection) -> PhysicalProjection:
        """Plan a projection node."""
        input_plan = self._plan_node(projection.input)
        if projection.distinct:
            self._propagate_distinct(input_plan)
        return PhysicalProjection(
            input=input_plan,
            expressions=projection.expressions,
            output_names=projection.aliases,
            distinct=projection.distinct,
        )

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
        return PhysicalLimit(input=input_plan, limit=limit.limit, offset=limit.offset)

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

        return PhysicalSort(
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
        return PhysicalHashAggregate(
            input=input_plan,
            group_by=aggregate.group_by,
            aggregates=aggregate.aggregates,
            output_names=aggregate.output_names,
        )

    def _plan_remote_join_aggregate(
        self, aggregate: Aggregate, remote_join: PhysicalRemoteJoin
    ) -> PhysicalRemoteJoin:
        """Fold an aggregate onto a remote join so both push as one remote query."""
        return PhysicalRemoteJoin(
            left=remote_join.left,
            right=remote_join.right,
            join_type=remote_join.join_type,
            condition=remote_join.condition,
            datasource_connection=remote_join.datasource_connection,
            group_by=aggregate.group_by,
            aggregates=aggregate.aggregates,
            output_names=aggregate.output_names,
        )

    def _plan_explain(self, explain: Explain) -> PhysicalExplain:
        """Plan an explain node."""
        child_plan = self._plan_node(explain.input)
        return PhysicalExplain(child=child_plan, format=explain.format)

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
        producer = PhysicalCTE(
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
        return PhysicalCTEScan(producer=producer, alias=node.alias)

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
        return PhysicalCTEMergeQuery(sql=sql, inputs=inputs, output_names=cte.schema())

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
        hash_join = PhysicalHashJoin(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            left_keys=left_keys,
            right_keys=right_keys,
            build_side=self._choose_build_side(join.join_type, left_plan, right_plan),
        )
        self._mark_dynamic_filter(hash_join)
        return hash_join

    def _plan_nested_loop(
        self, join: Join, left_plan: PhysicalPlanNode, right_plan: PhysicalPlanNode
    ) -> PhysicalNestedLoopJoin:
        """Fall back to a nested-loop join (no equi-keys, or no join condition)."""
        return PhysicalNestedLoopJoin(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
        )

    def _choose_build_side(
        self,
        join_type: JoinType,
        left_plan: PhysicalPlanNode,
        right_plan: PhysicalPlanNode,
    ) -> str:
        """Build from the side with a selective equality filter (likely small).

        That side becomes the hash-table build input and the other becomes the
        probe that the dynamic filter reduces. Only INNER joins are eligible —
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

        return PhysicalNestedLoopJoin(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
        )

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
        """Return the (probe node, probe keys) — the input that is not built."""
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
        sides = _JoinSides(
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
        """Orient using bare column names when qualifiers do not resolve."""
        if first.column in sides.left_names and second.column in sides.right_names:
            return first, second
        if first.column in sides.right_names and second.column in sides.left_names:
            return second, first
        return first, second

    def _relation_aliases(self, plan) -> set:
        """Collect the relation aliases/names a plan subtree exposes."""
        aliases: set = set()
        self._collect_relation_aliases(plan, aliases)
        return aliases

    def _collect_relation_aliases(self, plan, aliases: set) -> None:
        """Recursively gather Scan/SubqueryScan qualifiers from a subtree."""
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
        return PhysicalRemoteJoin(
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
        if left_plan.datasource != right_plan.datasource:
            return False
        ds = left_plan.datasource_connection
        if ds is None or ds != right_plan.datasource_connection:
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
        if left_branch.datasource != right_branch.datasource:
            return None
        connection = left_branch.datasource_connection
        if connection is None or connection != right_branch.datasource_connection:
            return None
        return PhysicalRemoteSetOp(
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
        """Clone a scan, replacing its projected column list."""
        return PhysicalScan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=columns,
            filters=scan.filters,
            datasource_connection=scan.datasource_connection,
            group_by=scan.group_by,
            aggregates=scan.aggregates,
            output_names=scan.output_names,
            alias=scan.alias,
            limit=scan.limit,
            offset=scan.offset,
            order_by_keys=scan.order_by_keys,
            order_by_ascending=scan.order_by_ascending,
            order_by_nulls=scan.order_by_nulls,
            distinct=scan.distinct,
        )

    def _plan_local_set_op(
        self,
        set_op: SetOperation,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
    ) -> PhysicalPlanNode:
        """Evaluate a set operation locally when it cannot be pushed down."""
        if set_op.kind == SetOpKind.UNION:
            return PhysicalUnion(inputs=[left, right], distinct=set_op.distinct)
        return PhysicalSetOperation(
            left=left, right=right, kind=set_op.kind, distinct=set_op.distinct
        )

    def __repr__(self) -> str:
        return "PhysicalPlanner()"
