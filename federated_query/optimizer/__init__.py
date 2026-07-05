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
from .factory import build_optimizer
from .join_ordering import JoinOrderingRule

__all__ = [
    "OptimizationRule",
    "RuleBasedOptimizer",
    "PredicatePushdownRule",
    "ProjectionPushdownRule",
    "AggregatePushdownRule",
    "OrderByPushdownRule",
    "LimitPushdownRule",
    "JoinOrderingRule",
    "CostModel",
    "StatisticsCollector",
    "PhysicalPlanner",
    "Decorrelator",
    "DecorrelationError",
    "build_optimizer",
]
