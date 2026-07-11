//! fq-optimize: the logical optimizer. Milestone 1 = the ESTIMATION FOUNDATION
//! that the rewrite rules and join-ordering DP (later milestones) depend on:
//! the cost model, the statistics collector, the estimate-defaults helpers, the
//! byte-critical subplan signature, and the join-graph extraction.
//!
//! Ports `optimizer/{cost,statistics,estimate_defaults,subplan_signature,
//! join_graph}.py`. The rewrite rules and join-ordering land in later milestones.

pub mod cost;
pub mod error;
pub mod estimate_defaults;
pub mod join_graph;
pub mod pushdown;
pub mod statistics;
pub mod subplan_signature;

pub use cost::CostModel;
pub use error::EstimateError;
pub use estimate_defaults::{
    add_rows, cap_composite_denom, combine_defaults, max_known_ndv, min_rows, mul_groups,
    useless_key_reduction, CardinalityEstimate, TRANSFER_WEIGHT, USELESS_KEYS_NDV_FRACTION,
};
pub use join_graph::{
    extract_region, is_region_join, JoinAtom, JoinConjunct, JoinGraphError, JoinRegion,
};
pub use statistics::StatisticsCollector;
pub use subplan_signature::{
    group_column_names, python_class_name, scan_predicate_template, subplan_signature,
};
