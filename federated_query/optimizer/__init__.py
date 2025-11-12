"""Query optimizer."""

from .rules import (
    OptimizationRule,
    RuleBasedOptimizer,
    ExpressionSimplificationRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    JoinReorderingRule,
)
from .cost import CostModel
from .statistics import StatisticsCollector
from .physical_planner import PhysicalPlanner
from .expression_rewriter import (
    ExpressionRewriter,
    ConstantFoldingRewriter,
    ExpressionSimplificationRewriter,
    CompositeExpressionRewriter,
)

__all__ = [
    "OptimizationRule",
    "RuleBasedOptimizer",
    "ExpressionSimplificationRule",
    "PredicatePushdownRule",
    "ProjectionPushdownRule",
    "JoinReorderingRule",
    "CostModel",
    "StatisticsCollector",
    "PhysicalPlanner",
    "ExpressionRewriter",
    "ConstantFoldingRewriter",
    "ExpressionSimplificationRewriter",
    "CompositeExpressionRewriter",
]
