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
)
from ..plan.expressions import BinaryOp, BinaryOpType, ColumnRef
from typing import List, Tuple

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


class PhysicalPlanner:
    """Converts logical plans to physical plans."""

    def __init__(self, catalog: Catalog):
        """Initialize physical planner.

        Args:
            catalog: Catalog for looking up data sources
        """
        self.catalog = catalog

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

    def _plan_limit(self, limit: Limit) -> PhysicalLimit:
        """Plan a limit node."""
        input_plan = self._plan_node(limit.input)
        return PhysicalLimit(
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

        return PhysicalSort(
            input=input_plan,
            sort_keys=sort.sort_keys,
            ascending=sort.ascending,
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
        self,
        aggregate: Aggregate,
        remote_join: PhysicalRemoteJoin
    ) -> PhysicalRemoteJoin:
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
        """Plan a CTE by planning its child; execution layer should inline."""
        cte_plan = self._plan_node(cte.cte_plan)
        child_plan = self._plan_node(cte.child)
        # For now, ignore the cte name and return the child plan.
        # Physical execution layer currently lacks explicit CTE support.
        return child_plan

    def _plan_join(self, join: Join) -> PhysicalPlanNode:
        """Plan a join node."""
        left_plan = self._plan_node(join.left)
        right_plan = self._plan_node(join.right)

        remote = self._try_plan_remote_join(join, left_plan, right_plan)
        if remote:
            return remote

        if join.condition is None:
            return PhysicalNestedLoopJoin(
                left=left_plan,
                right=right_plan,
                join_type=join.join_type,
                condition=None,
            )

        join_keys = self._extract_join_keys(join.condition)
        if join_keys:
            left_keys, right_keys = join_keys
            return PhysicalHashJoin(
                left=left_plan,
                right=right_plan,
                join_type=join.join_type,
                left_keys=left_keys,
                right_keys=right_keys,
                build_side="right",
            )

        return PhysicalNestedLoopJoin(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
        )

    def _try_plan_remote_join(
        self,
        join: Join,
        left_plan: PhysicalPlanNode,
        right_plan: PhysicalPlanNode
    ) -> Optional[PhysicalPlanNode]:
        if not self._is_remote_join_candidate(join, left_plan, right_plan):
            return None
        order_keys = None
        order_asc = None
        order_nulls = None
        if isinstance(left_plan, PhysicalScan) and left_plan.order_by_keys:
            order_keys = left_plan.order_by_keys
            order_asc = left_plan.order_by_ascending
            order_nulls = left_plan.order_by_nulls
        if isinstance(right_plan, PhysicalScan) and right_plan.order_by_keys and order_keys is None:
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
        self,
        join: Join,
        left_plan: PhysicalPlanNode,
        right_plan: PhysicalPlanNode
    ) -> bool:
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

    def __repr__(self) -> str:
        return "PhysicalPlanner()"
