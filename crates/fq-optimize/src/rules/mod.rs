//! The rule-based fixpoint driver and the six pushdown rules. Ports
//! `optimizer/{rules,factory}.py` (the pushdown subset).
//!
//! Each rule is a stateless recursive walker over `LogicalPlan` (owned in, owned
//! out): it uses `LogicalPlan::try_map_children` for the generic recurse and
//! clone-and-set (struct update, re-wrapped into the enum) for targeted rewrites.
//! A rule NEVER returns `None`; it returns a (possibly identical) plan, and the
//! driver detects change by structural `!=` (fq-plan derives `PartialEq`). Only
//! `ProjectionPushdown` can raise; every other rule declines by returning the
//! plan unchanged.

mod aggregate;
mod driver;
mod limit;
mod order_by;
mod predicate;
mod projection;
mod semi_join;

use fq_plan::logical::LogicalPlan;

use crate::error::OptimizeError;

pub use aggregate::AggregatePushdown;
pub use driver::{build_optimizer, RuleBasedOptimizer};
pub use limit::LimitPushdown;
pub use order_by::OrderByPushdown;
pub use predicate::PredicatePushdown;
pub use projection::ProjectionPushdown;
pub use semi_join::SemiJoinPushdown;

/// An optimization rule: a whole-plan rewrite the driver applies in a fixpoint
/// loop. Ports the `OptimizationRule` ABC. Takes the plan BY VALUE (rules rebuild
/// via `try_map_children`, which consumes the plan); the driver keeps a clone of
/// the pre-rule plan for change detection.
pub trait OptimizationRule {
    /// This rule's identifier, used in the `validate_scope` stage label.
    fn name(&self) -> &'static str;

    /// Apply this rule, returning the possibly-identical rewritten plan.
    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError>;
}

/// The branch plans of a binary `SetOperation` or an n-ary `Union`. Ports
/// `_set_operation_branches`.
fn set_operation_branches(node: &LogicalPlan) -> Vec<LogicalPlan> {
    match node {
        LogicalPlan::Union(union) => union.inputs.clone(),
        LogicalPlan::SetOperation(set_op) => {
            vec![(*set_op.left).clone(), (*set_op.right).clone()]
        }
        _ => Vec::new(),
    }
}

/// Rebuild a set operation with new branch plans (kind/flags preserved). Ports
/// `_with_set_operation_branches`. Only called for `Union`/`SetOperation`, always
/// with a branch count matching the node's arity.
fn with_set_operation_branches(node: LogicalPlan, branches: Vec<LogicalPlan>) -> LogicalPlan {
    match node {
        LogicalPlan::Union(mut union) => {
            union.inputs = branches;
            LogicalPlan::Union(union)
        }
        LogicalPlan::SetOperation(mut set_op) => {
            let mut branch_iter = branches.into_iter();
            set_op.left = Box::new(
                branch_iter
                    .next()
                    .expect("set operation needs a left branch"),
            );
            set_op.right = Box::new(
                branch_iter
                    .next()
                    .expect("set operation needs a right branch"),
            );
            LogicalPlan::SetOperation(set_op)
        }
        other => other,
    }
}

/// The variant name of a plan node (Python's `type(plan).__name__`), for the one
/// pruner raise site.
fn plan_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::SetOperation(_) => "SetOperation",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::CteRef(_) => "CteRef",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::SubqueryScan(_) => "SubqueryScan",
        LogicalPlan::SingleRowGuard(_) => "SingleRowGuard",
        LogicalPlan::GroupedLimit(_) => "GroupedLimit",
        LogicalPlan::LateralJoin(_) => "LateralJoin",
    }
}
