//! fq-physical: optimized `LogicalPlan` -> `PhysicalPlan`.
//!
//! Three peer modules: the physical `planner` (this milestone), plus the
//! `single_source` and `dim_shipping` pushdown rules the planner tries FIRST for
//! every node (filled in later milestones; stubbed to decline here). Ports
//! `optimizer/{physical_planner,single_source_pushdown,dim_shipping}.py`.

pub mod dim_shipping;
pub mod error;
pub mod planner;
pub mod single_source;

pub use error::PhysicalError;
pub use planner::PhysicalPlanner;
