"""Cost model for query optimization."""

from typing import Dict, Any, Optional
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Join,
    Aggregate,
    Limit,
    Sort,
    Union,
    JoinType,
)
from ..plan.physical import PhysicalPlanNode
from ..plan.expressions import (
    Expression,
    BinaryOp,
    UnaryOp,
    ColumnRef,
    Literal,
    BinaryOpType,
    UnaryOpType,
)
from ..config.config import CostConfig
from ..optimizer.statistics import StatisticsCollector
from ..datasources.base import TableStatistics


class CostModel:
    """Cost model for estimating query execution cost."""

    def __init__(
        self,
        config: CostConfig,
        stats_collector: Optional[StatisticsCollector] = None
    ):
        """Initialize cost model.

        Args:
            config: Cost model configuration
            stats_collector: Statistics collector for cardinality info
        """
        self.config = config
        self.stats_collector = stats_collector

    def estimate_cardinality(self, plan: LogicalPlanNode) -> int:
        """Estimate output cardinality of a plan.

        Args:
            plan: Logical plan node

        Returns:
            Estimated number of output rows
        """
        if isinstance(plan, Scan):
            return self._estimate_scan_cardinality(plan)
        if isinstance(plan, Filter):
            return self._estimate_filter_cardinality(plan)
        if isinstance(plan, Project):
            return self._estimate_project_cardinality(plan)
        if isinstance(plan, Join):
            return self._estimate_join_cardinality(plan)
        if isinstance(plan, Aggregate):
            return self._estimate_aggregate_cardinality(plan)
        if isinstance(plan, Limit):
            return self._estimate_limit_cardinality(plan)

        return 1000

    def _estimate_scan_cardinality(self, scan: Scan) -> int:
        """Estimate cardinality of a scan node."""
        if not self.stats_collector:
            return 1000

        stats = self.stats_collector.get_table_statistics(
            scan.datasource,
            scan.schema_name,
            scan.table_name
        )
        if not stats:
            return 1000

        base_card = stats.row_count
        if scan.filters:
            selectivity = self.estimate_selectivity(
                scan.filters,
                stats
            )
            return max(1, int(base_card * selectivity))

        return base_card

    def _estimate_filter_cardinality(self, filter_node: Filter) -> int:
        """Estimate cardinality after filtering."""
        input_card = self.estimate_cardinality(filter_node.input)
        selectivity = self.estimate_selectivity(
            filter_node.predicate,
            None
        )
        return max(1, int(input_card * selectivity))

    def _estimate_project_cardinality(self, project: Project) -> int:
        """Estimate cardinality of projection (same as input)."""
        return self.estimate_cardinality(project.input)

    def _estimate_join_cardinality(self, join: Join) -> int:
        """Estimate cardinality of a join."""
        left_card = self.estimate_cardinality(join.left)
        right_card = self.estimate_cardinality(join.right)
        return self._compute_join_cardinality(
            join.join_type,
            left_card,
            right_card,
            join.condition
        )

    def _compute_join_cardinality(
        self,
        join_type: JoinType,
        left_card: int,
        right_card: int,
        condition: Optional[Expression]
    ) -> int:
        """Compute join cardinality based on type and condition."""
        if join_type == JoinType.CROSS:
            return left_card * right_card

        if not condition:
            return left_card * right_card

        selectivity = self.estimate_selectivity(condition, None)
        base_card = left_card * right_card

        if join_type == JoinType.INNER:
            return max(1, int(base_card * selectivity))
        if join_type == JoinType.LEFT:
            return max(left_card, int(base_card * selectivity))
        if join_type == JoinType.RIGHT:
            return max(right_card, int(base_card * selectivity))
        if join_type == JoinType.FULL:
            inner = int(base_card * selectivity)
            return left_card + right_card - inner

        return int(base_card * selectivity)

    def _estimate_aggregate_cardinality(self, agg: Aggregate) -> int:
        """Estimate cardinality after aggregation."""
        if not agg.group_by:
            return 1

        input_card = self.estimate_cardinality(agg.input)
        num_groups = max(1, input_card // 10)
        return min(input_card, num_groups)

    def _estimate_limit_cardinality(self, limit: Limit) -> int:
        """Estimate cardinality with limit."""
        input_card = self.estimate_cardinality(limit.input)
        total_limit = limit.offset + limit.limit
        return min(input_card, total_limit)

    def estimate_selectivity(
        self,
        predicate: Expression,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity of a predicate.

        Args:
            predicate: Filter predicate expression
            stats: Table statistics if available

        Returns:
            Estimated selectivity (0.0 to 1.0)
        """
        if isinstance(predicate, BinaryOp):
            return self._estimate_binary_op_selectivity(
                predicate,
                stats
            )
        if isinstance(predicate, UnaryOp):
            return self._estimate_unary_op_selectivity(
                predicate,
                stats
            )

        return 0.1

    def _estimate_binary_op_selectivity(
        self,
        binop: BinaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for binary operations."""
        if binop.op == BinaryOpType.AND:
            return self._estimate_and_selectivity(binop, stats)
        if binop.op == BinaryOpType.OR:
            return self._estimate_or_selectivity(binop, stats)
        if binop.op == BinaryOpType.EQ:
            return self._estimate_equality_selectivity(binop, stats)
        if binop.op in (BinaryOpType.LT, BinaryOpType.LTE, BinaryOpType.GT, BinaryOpType.GTE):
            return 0.33
        if binop.op == BinaryOpType.NEQ:
            eq_sel = self._estimate_equality_selectivity(binop, stats)
            return 1.0 - eq_sel
        if binop.op == BinaryOpType.LIKE:
            return 0.1

        return 0.1

    def _estimate_and_selectivity(
        self,
        binop: BinaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for AND (product of operands)."""
        left_sel = self.estimate_selectivity(binop.left, stats)
        right_sel = self.estimate_selectivity(binop.right, stats)
        return left_sel * right_sel

    def _estimate_or_selectivity(
        self,
        binop: BinaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for OR."""
        left_sel = self.estimate_selectivity(binop.left, stats)
        right_sel = self.estimate_selectivity(binop.right, stats)
        return 1.0 - ((1.0 - left_sel) * (1.0 - right_sel))

    def _estimate_equality_selectivity(
        self,
        binop: BinaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for equality comparison."""
        if not stats:
            return 0.1

        col_ref = self._extract_column_ref(binop)
        if not col_ref:
            return 0.1

        col_name = col_ref.column
        if col_name not in stats.column_stats:
            return 0.1

        col_stats = stats.column_stats[col_name]
        if col_stats.num_distinct == 0:
            return 0.0

        return min(1.0, 1.0 / col_stats.num_distinct)

    def _extract_column_ref(self, binop: BinaryOp) -> Optional[ColumnRef]:
        """Extract column reference from binary operation."""
        if isinstance(binop.left, ColumnRef):
            return binop.left
        if isinstance(binop.right, ColumnRef):
            return binop.right
        return None

    def _estimate_unary_op_selectivity(
        self,
        unop: UnaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for unary operations."""
        if unop.op == UnaryOpType.NOT:
            inner_sel = self.estimate_selectivity(unop.operand, stats)
            return 1.0 - inner_sel
        if unop.op == UnaryOpType.IS_NULL:
            return self._estimate_is_null_selectivity(unop, stats)
        if unop.op == UnaryOpType.IS_NOT_NULL:
            null_sel = self._estimate_is_null_selectivity(unop, stats)
            return 1.0 - null_sel

        return 0.1

    def _estimate_is_null_selectivity(
        self,
        unop: UnaryOp,
        stats: Optional[TableStatistics]
    ) -> float:
        """Estimate selectivity for IS NULL."""
        if not stats or not isinstance(unop.operand, ColumnRef):
            return 0.05

        col_name = unop.operand.column
        if col_name not in stats.column_stats:
            return 0.05

        return stats.column_stats[col_name].null_fraction

    def __repr__(self) -> str:
        return "CostModel()"
