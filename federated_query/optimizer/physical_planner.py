"""Physical planner converts logical plans to physical plans."""

from typing import Dict
from ..catalog.catalog import Catalog
from ..datasources.base import DataSource, DataSourceCapability
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Limit,
    Join,
    Aggregate,
    Explain,
    JoinType,
)
from ..plan.physical import (
    PhysicalPlanNode,
    PhysicalScan,
    PhysicalProject,
    PhysicalFilter,
    PhysicalLimit,
    PhysicalHashJoin,
    PhysicalRemoteJoin,
    PhysicalNestedLoopJoin,
    PhysicalHashAggregate,
    PhysicalExplain,
)
from ..plan.expressions import BinaryOp, BinaryOpType, ColumnRef
from typing import List, Tuple, Optional


class PhysicalPlanner:
    """Converts logical plans to physical plans."""

    def __init__(self, catalog: Catalog):
        """Initialize physical planner.

        Args:
            catalog: Catalog for looking up data sources
        """
        self.catalog = catalog

    def plan(self, logical_plan: LogicalPlanNode) -> PhysicalPlanNode:
        """Convert logical plan to physical plan.

        Args:
            logical_plan: Logical plan to convert

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
        if isinstance(node, Project):
            return self._plan_project(node)
        if isinstance(node, Limit):
            return self._plan_limit(node)
        if isinstance(node, Join):
            return self._plan_join(node)
        if isinstance(node, Aggregate):
            return self._plan_aggregate(node)
        if isinstance(node, Explain):
            return self._plan_explain(node)

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
        )

    def _plan_filter(self, filter_node: Filter) -> PhysicalFilter:
        """Plan a filter node."""
        input_plan = self._plan_node(filter_node.input)
        return PhysicalFilter(input=input_plan, predicate=filter_node.predicate)

    def _plan_project(self, project: Project) -> PhysicalProject:
        """Plan a project node."""
        input_plan = self._plan_node(project.input)
        return PhysicalProject(
            input=input_plan,
            expressions=project.expressions,
            output_names=project.aliases,
        )

    def _plan_limit(self, limit: Limit) -> PhysicalLimit:
        """Plan a limit node."""
        input_plan = self._plan_node(limit.input)
        return PhysicalLimit(
            input=input_plan, limit=limit.limit, offset=limit.offset
        )

    def _plan_aggregate(self, aggregate: Aggregate) -> PhysicalHashAggregate:
        """Plan an aggregate node."""
        input_plan = self._plan_node(aggregate.input)
        return PhysicalHashAggregate(
            input=input_plan,
            group_by=aggregate.group_by,
            aggregates=aggregate.aggregates,
            output_names=aggregate.output_names,
        )

    def _plan_explain(self, explain: Explain) -> PhysicalExplain:
        """Plan an explain node."""
        child_plan = self._plan_node(explain.input)
        return PhysicalExplain(child=child_plan)

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
        return PhysicalRemoteJoin(
            left=left_plan,
            right=right_plan,
            join_type=join.join_type,
            condition=join.condition,
            datasource_connection=left_plan.datasource_connection,
        )

    def _is_remote_join_candidate(
        self,
        join: Join,
        left_plan: PhysicalPlanNode,
        right_plan: PhysicalPlanNode
    ) -> bool:
        if join.join_type != JoinType.INNER or join.condition is None:
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
