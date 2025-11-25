"""Query optimizer."""

from .rules import (
    OptimizationRule,
    RuleBasedOptimizer,
    ExpressionSimplificationRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    AggregatePushdownRule,
    OrderByPushdownRule,
    LimitPushdownRule,
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
from .decorrelation import Decorrelator, DecorrelationError

__all__ = [
    "OptimizationRule",
    "RuleBasedOptimizer",
    "ExpressionSimplificationRule",
    "PredicatePushdownRule",
    "ProjectionPushdownRule",
    "AggregatePushdownRule",
    "OrderByPushdownRule",
    "LimitPushdownRule",
    "JoinReorderingRule",
    "CostModel",
    "StatisticsCollector",
    "PhysicalPlanner",
    "ExpressionRewriter",
    "ConstantFoldingRewriter",
    "ExpressionSimplificationRewriter",
    "CompositeExpressionRewriter",
    "Decorrelator",
    "DecorrelationError",
]
