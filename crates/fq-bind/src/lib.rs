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
//! The HAVING and ORDER BY aggregate-call hoists are implemented; WITH
//! RECURSIVE is not.

pub mod binder;
pub mod error;
pub mod expr;
mod nested;

pub use binder::Binder;
pub use error::BindError;

use fq_catalog::Catalog;
use fq_common::Principal;
use fq_plan::LogicalPlan;

/// The bind-time authorization hook. The binder consults it in the ONE resolver
/// (`resolve_scan_table`), AFTER the catalog resolves a base table's real
/// qualifier, asking whether the current principal (or PUBLIC) holds SELECT at
/// the table's datasource, schema, or table level. A denial raises a non-leaking
/// not-found; the implementor is responsible for the server-side audit line.
pub trait Authorizer {
    /// Whether the principal may SELECT the fully-qualified base table. The
    /// datasource/schema/table are the table's REAL qualifier from the catalog,
    /// never the raw (possibly bare) scan reference.
    fn authorize_select(&self, datasource: &str, schema: &str, table: &str) -> bool;
}

/// Bind a logical plan against `catalog` with NO enforcement (an implicit
/// superuser): the embedded process and every test bind through here. The wire
/// server uses [`bind_as`] with the authenticated principal.
pub fn bind(catalog: &Catalog, plan: LogicalPlan) -> Result<LogicalPlan, BindError> {
    guard_depth(&plan)?;
    let bound = Binder::new(catalog).bind(plan)?;
    guard_depth(&bound)?;
    Ok(bound)
}

/// Refuse a plan deeper than `fq_common::MAX_PLAN_DEPTH`.
///
/// Both entry points check the plan they are HANDED and the plan they RETURN.
/// The input check is what protects the binder's own recursion, which runs one
/// frame chain per plan level before any output exists; the output check
/// protects every later pass, over the plan the binder actually produced -
/// hoists and CTE inlining make it a different plan from the input.
///
/// The measure counts plan nodes and crosses subquery boundaries, and it walks
/// iteratively: a recursive measure would overflow on the plans this rejects.
fn guard_depth(plan: &LogicalPlan) -> Result<(), BindError> {
    let depth = plan.max_depth();
    if depth > fq_common::MAX_PLAN_DEPTH {
        return Err(BindError::PlanTooDeep {
            depth,
            limit: fq_common::MAX_PLAN_DEPTH,
        });
    }
    Ok(())
}

/// Bind a logical plan as `principal`, authorizing every base-table reference
/// through `authorizer`. A superuser principal short-circuits the check (its
/// queries pay nothing); a normal principal's unauthorized reference raises a
/// non-leaking `TableNotFound`.
pub fn bind_as(
    catalog: &Catalog,
    plan: LogicalPlan,
    principal: &Principal,
    authorizer: &dyn Authorizer,
) -> Result<LogicalPlan, BindError> {
    guard_depth(&plan)?;
    let bound = Binder::with_principal(catalog, principal.superuser, authorizer).bind(plan)?;
    guard_depth(&bound)?;
    Ok(bound)
}

mod json;
