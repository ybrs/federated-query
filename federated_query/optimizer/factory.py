"""The one place the optimizer's rule stack is assembled from configuration.

Every runtime (CLI, benchmarks, tests that want the standard stack) builds its
RuleBasedOptimizer here, so the OptimizerConfig flags are honored everywhere
and a rule can never be silently forgotten at one construction site.
"""

from ..catalog.catalog import Catalog
from ..config.config import CostConfig, OptimizerConfig
from .cost import CostModel
from .join_ordering import JoinOrderingRule
from .rules import (
    AggregatePushdownRule,
    LimitPushdownRule,
    OrderByPushdownRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    RuleBasedOptimizer,
)
from .statistics import StatisticsCollector


def build_optimizer(
    catalog: Catalog, optimizer_config: OptimizerConfig, cost_config: CostConfig
) -> RuleBasedOptimizer:
    """The standard rule stack, honoring the optimizer configuration.

    JoinOrdering registers immediately after PredicatePushdown: it reads the
    folded equi conditions and embedded scan filters pushdown produces, and
    must run before projection pushdown prunes columns.
    """
    optimizer = RuleBasedOptimizer(catalog)
    if optimizer_config.enable_predicate_pushdown:
        optimizer.add_rule(PredicatePushdownRule())
    if optimizer_config.enable_join_reordering:
        optimizer.add_rule(_join_ordering_rule(catalog, optimizer_config, cost_config))
    if optimizer_config.enable_projection_pushdown:
        optimizer.add_rule(ProjectionPushdownRule())
    optimizer.add_rule(AggregatePushdownRule())
    optimizer.add_rule(OrderByPushdownRule())
    optimizer.add_rule(LimitPushdownRule())
    return optimizer


def _join_ordering_rule(
    catalog: Catalog, optimizer_config: OptimizerConfig, cost_config: CostConfig
) -> JoinOrderingRule:
    """The cost-based join-ordering rule over a session-cached statistics
    collector: statistics are fetched from the sources' catalogs lazily per
    column and cached for the life of this optimizer."""
    cost_model = CostModel(cost_config, StatisticsCollector(catalog))
    return JoinOrderingRule(cost_model, optimizer_config.max_join_reorder_size)
