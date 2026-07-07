"""The one place the optimizer's rule stack is assembled from configuration.

Every runtime (CLI, benchmarks, tests that want the standard stack) builds its
RuleBasedOptimizer here, so the OptimizerConfig flags are honored everywhere
and a rule can never be silently forgotten at one construction site.
"""

from ..catalog.catalog import Catalog
from ..config.config import CostConfig, OptimizerConfig
from .cost import CostModel
from .cte_union_filter import CTEUnionFilterPushdownRule
from .join_ordering import JoinOrderingRule
from .rules import (
    AggregatePushdownRule,
    LimitPushdownRule,
    OrderByPushdownRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    RuleBasedOptimizer,
    SemiJoinPushdownRule,
)
from .statistics import StatisticsCollector


def build_cost_model(catalog: Catalog, cost_config: CostConfig) -> CostModel:
    """The session's ONE cost model over a session-cached statistics
    collector: statistics are fetched from the sources' catalogs lazily per
    column and cached for the life of the session. Shared by the join-ordering
    rule and the physical planner so every size-sensitive decision reads the
    same numbers."""
    return CostModel(cost_config, StatisticsCollector(catalog))


def build_optimizer(
    catalog: Catalog,
    optimizer_config: OptimizerConfig,
    cost_config: CostConfig,
    cost_model: CostModel = None,
) -> RuleBasedOptimizer:
    """The standard rule stack, honoring the optimizer configuration.

    JoinOrdering registers immediately after PredicatePushdown: it reads the
    folded equi conditions and embedded scan filters pushdown produces, and
    must run before projection pushdown prunes columns. Callers that also
    build a PhysicalPlanner pass the shared cost model here.
    """
    if cost_model is None:
        cost_model = build_cost_model(catalog, cost_config)
    optimizer = RuleBasedOptimizer(catalog)
    if optimizer_config.enable_predicate_pushdown:
        # Before the ordinary pushdown: this rule consumes the per-consumer
        # filters pushdown places above the CTE refs, and inserts the union
        # filter the same pushdown then sinks to the body's base scans.
        optimizer.add_rule(CTEUnionFilterPushdownRule())
        optimizer.add_rule(PredicatePushdownRule())
    # Before join ordering: pushing a selective SEMI/ANTI join down to the
    # relation it filters changes the region the reorderer then sees (a
    # reduced input instead of a top-level existential filter).
    optimizer.add_rule(SemiJoinPushdownRule())
    if optimizer_config.enable_join_reordering:
        optimizer.add_rule(
            JoinOrderingRule(cost_model, optimizer_config.max_join_reorder_size)
        )
    if optimizer_config.enable_projection_pushdown:
        optimizer.add_rule(ProjectionPushdownRule())
    optimizer.add_rule(AggregatePushdownRule())
    optimizer.add_rule(OrderByPushdownRule())
    optimizer.add_rule(LimitPushdownRule())
    return optimizer
