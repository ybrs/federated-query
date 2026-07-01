"""Query optimizer."""

from .rules import (
    OptimizationRule,
    RuleBasedOptimizer,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    AggregatePushdownRule,
    OrderByPushdownRule,
    LimitPushdownRule,
)
from .cost import CostModel
from .statistics import StatisticsCollector
from .physical_planner import PhysicalPlanner
from .decorrelation import Decorrelator, DecorrelationError

__all__ = [
    "OptimizationRule",
    "RuleBasedOptimizer",
    "PredicatePushdownRule",
    "ProjectionPushdownRule",
    "AggregatePushdownRule",
    "OrderByPushdownRule",
    "LimitPushdownRule",
    "CostModel",
    "StatisticsCollector",
    "PhysicalPlanner",
    "Decorrelator",
    "DecorrelationError",
]
