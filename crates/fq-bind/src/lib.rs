//! fq-bind: name resolution and typing for a `LogicalPlan`.
//!
//! Resolves every table/column reference against the catalog, sets each column's
//! qualifier and `DataType`, and raises on an invalid reference (a bogus
//! qualifier or typo'd column fails here, before any source is touched). Ports
//! `parser/binder.py`.
//!
//! Covers the query shapes fq-parse produces - base tables, joins, derived
//! tables, CTEs, set operations, aggregates, subquery expressions (correlated),
//! HAVING/ORDER-BY aliases and positional ordinals - closing SQL -> bound plan.
//! The HAVING aggregate-call hoist is implemented; the same hoist for ORDER BY
//! aggregate calls absent from SELECT, and WITH RECURSIVE, are not.

pub mod binder;
pub mod error;
pub mod expr;

pub use binder::Binder;
pub use error::BindError;

use fq_catalog::Catalog;
use fq_plan::LogicalPlan;

/// Bind a logical plan against `catalog`, returning the fully resolved plan.
pub fn bind(catalog: &Catalog, plan: LogicalPlan) -> Result<LogicalPlan, BindError> {
    Binder::new(catalog).bind(plan)
}
