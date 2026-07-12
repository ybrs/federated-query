//! fq-optimize: the logical optimizer - the rewrite rules and the join-ordering
//! DP, plus the ESTIMATION FOUNDATION they depend on: the cost model, the
//! statistics collector, the estimate-defaults helpers, the byte-critical
//! subplan signature, and the join-graph extraction.
//!
//! Ports `optimizer/{rules,join_ordering,cost,statistics,estimate_defaults,
//! subplan_signature,join_graph}.py`.

pub mod cost;
pub mod error;
pub mod estimate_defaults;
pub mod join_graph;
pub mod pushdown;
pub mod rules;
pub mod statistics;
pub mod subplan_signature;

pub use cost::CostModel;
pub use error::{EstimateError, OptimizeError};
pub use estimate_defaults::{
    add_rows, cap_composite_denom, combine_defaults, max_known_ndv, min_rows, mul_groups,
    useless_key_reduction, CardinalityEstimate, TRANSFER_WEIGHT, USELESS_KEYS_NDV_FRACTION,
};
pub use join_graph::{
    extract_region, is_region_join, JoinAtom, JoinConjunct, JoinGraphError, JoinRegion,
};
pub use rules::{
    build_optimizer, AggregatePushdown, CTEUnionFilterPushdown, EagerAggregation, JoinOrdering,
    LimitPushdown, OptimizationRule, OrderByPushdown, PredicatePushdown, ProjectionPushdown,
    RuleBasedOptimizer, SemiJoinPushdown,
};
pub use statistics::StatisticsCollector;
pub use subplan_signature::{
    group_column_names, python_class_name, scan_predicate_template, subplan_signature,
};
