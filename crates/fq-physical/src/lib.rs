//! fq-physical: optimized `LogicalPlan` -> `PhysicalPlan`.
//!
//! Three peer modules: the physical `planner`, plus the `single_source` and
//! `dim_shipping` pushdown rules the planner tries FIRST for every node. Ports
//! `optimizer/{physical_planner,single_source_pushdown,dim_shipping}.py`.

pub mod dim_shipping;
pub mod error;
pub mod planner;
mod relation_sql;
pub mod single_source;
pub mod steps;

pub use dim_shipping::ShipThresholds;
pub use error::PhysicalError;
pub use planner::PhysicalPlanner;
pub use steps::{
    build_steps, render_remote_set_op, render_scan_sql, BuiltSteps, Fragment, Observation,
    ScanSpec, Step, StepError,
};
