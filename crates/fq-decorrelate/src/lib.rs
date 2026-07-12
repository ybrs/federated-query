//! fq-decorrelate: rewrite every subquery EXPRESSION in a bound logical plan into
//! a set-based join, so NO correlated/bounded subquery survives into execution.
//! Ports `optimizer/decorrelation.py`.
//!
//! The engine ALWAYS decorrelates. A flat join plan is what the rest of the
//! engine already knows how to optimize, push to a single source, or split across
//! sources - a decorrelated EXISTS "is just a join". Physical subquery planning is
//! a last resort, ideally never reached.
//!
//! This crate implements the flattenable path (EXISTS/NOT EXISTS, IN/NOT IN, op
//! ANY/ALL become SEMI/ANTI joins), the scalar LEFT-join path (with SingleRowGuard
//! placement and correlated-LIMIT GroupedLimit), the OR boolean-flag / domain-union
//! path, the correlated sort-key rewrite, and the Neumann-Kemper dependent join
//! (with a LATERAL fallback) for a non-flattenable scalar correlation - all sharing
//! the correlation-stripping/key-exposure machinery. Two post-passes then run: an
//! assertion that no subquery expression survives, and `scope::validate_scope`
//! (the well-scoped-plan guard that raises if any qualified reference escaped its
//! relation's scope). The one remaining seam fails loudly with `Unsupported`: a
//! subquery in a join condition.

pub mod error;
pub mod helpers;

mod boolean;
mod dependent;
mod disjunctive;
mod filter;
mod prepare;
pub mod scope;
mod value;

pub use error::DecorrelationError;

use fq_common::DataType;
use fq_plan::expr::{Expr, LiteralValue};
use fq_plan::logical::{Aggregate, Join, Projection, Values};
use fq_plan::LogicalPlan;

use crate::boolean::any_has_subquery;
use crate::helpers::expression_has_subquery;

/// The crate-internal result type; every unsupported/invalid shape returns Err.
pub(crate) type Result<T> = std::result::Result<T, DecorrelationError>;

/// Decorrelate a bound logical plan: remove every subquery expression, turning it
/// into joins. Raises on any shape this milestone does not yet flatten, and on any
/// subquery expression that survives (a loud internal invariant).
pub fn decorrelate(plan: LogicalPlan) -> std::result::Result<LogicalPlan, DecorrelationError> {
    let mut decorrelator = Decorrelator::new();
    let rewritten = decorrelator.rewrite_plan(plan)?;
    raise_if_subquery_expression(&rewritten)?;
    scope::validate_scope(&rewritten, "after decorrelation")?;
    Ok(rewritten)
}

/// Decorrelates subqueries in bound logical plans. Holds the deterministic
/// subquery name counter (one `__subq_{n}` prefix per prepared subquery).
pub(crate) struct Decorrelator {
    counter: usize,
}

impl Decorrelator {
    /// Initialize the deterministic subquery name counter.
    fn new() -> Self {
        Self { counter: 0 }
    }

    /// Allocate the next deterministic subquery name prefix.
    pub(crate) fn next_prefix(&mut self) -> String {
        let prefix = format!("__subq_{}", self.counter);
        self.counter += 1;
        prefix
    }

    /// Rewrite one plan node, dispatching by type. Recurses into children as it
    /// goes (top-down), so nested subqueries below are already flat when a node is
    /// processed.
    pub(crate) fn rewrite_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => self.rewrite_filter(node),
            LogicalPlan::Projection(node) => self.rewrite_projection(node),
            LogicalPlan::Sort(node) => self.rewrite_sort(node),
            LogicalPlan::Join(node) => self.rewrite_join(node),
            LogicalPlan::Values(node) => self.rewrite_values(node),
            LogicalPlan::LateralJoin(node) => self.rewrite_lateral_join(node),
            other => self.rewrite_rest(other),
        }
    }

    /// Rewrite a join: recurse into both sides. A subquery in a join condition is a
    /// later milestone.
    fn rewrite_join(&mut self, node: Join) -> Result<LogicalPlan> {
        let left = self.rewrite_plan(*node.left)?;
        let right = self.rewrite_plan(*node.right)?;
        if node.condition.as_ref().is_some_and(expression_has_subquery) {
            return Err(DecorrelationError::Unsupported(
                "subquery in a join condition decorrelation not yet ported".to_string(),
            ));
        }
        Ok(LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            ..node
        }))
    }

    /// Rewrite remaining node types by recursing into children. An Aggregate first
    /// rejects subqueries in grouping or aggregate positions (unsupported).
    fn rewrite_rest(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if let LogicalPlan::Aggregate(Aggregate {
            group_by,
            aggregates,
            ..
        }) = &plan
        {
            reject_subqueries_in(group_by, "GROUP BY")?;
            reject_subqueries_in(aggregates, "aggregate expressions")?;
        }
        plan.try_map_children(|child| self.rewrite_plan(child))
    }

    /// Decorrelate subqueries inside a FROM-less SELECT's Values row. `SELECT
    /// EXISTS (...)` parses to a one-row Values whose expression is a subquery: the
    /// row acts as the single outer row that each subquery join hangs off, and the
    /// rewritten expressions are re-projected under the original output names.
    fn rewrite_values(&mut self, node: Values) -> Result<LogicalPlan> {
        if !node.rows.iter().flatten().any(expression_has_subquery) {
            return Ok(LogicalPlan::Values(node));
        }
        if node.rows.len() != 1 {
            return Err(DecorrelationError::Unsupported(
                "Multi-row VALUES with subqueries is not supported".to_string(),
            ));
        }
        // Thread joins onto a clean one-row placeholder, not the original Values:
        // the original still carries the subquery expressions and would re-introduce
        // them as a join input.
        let mut plan = LogicalPlan::Values(Values {
            rows: vec![vec![Expr::Literal {
                value: LiteralValue::Integer(1),
                data_type: DataType::Integer,
            }]],
            output_names: vec!["__exists_base".to_string()],
        });
        let mut expressions = Vec::new();
        let row = node.rows.into_iter().next().expect("exactly one row");
        for expr in row {
            let (rewritten, next) = self.rewrite_value_expr(expr, plan)?;
            plan = next;
            expressions.push(rewritten);
        }
        // Fresh projection re-exposing the rewritten Values-row expressions under the
        // original output names - no base to copy from. Field list is complete.
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(plan),
            expressions,
            aliases: node.output_names,
            distinct: false,
            distinct_on: None,
        }))
    }
}

/// Reject subqueries in an expression list (GROUP BY / aggregate positions).
fn reject_subqueries_in(expressions: &[fq_plan::Expr], position: &str) -> Result<()> {
    if any_has_subquery(expressions) {
        return Err(DecorrelationError::Unsupported(format!(
            "Subqueries in {position} are not supported"
        )));
    }
    Ok(())
}

/// Walk every node's direct expressions; any surviving subquery expression is a
/// loud internal invariant violation (decorrelation must remove all of them).
fn raise_if_subquery_expression(plan: &LogicalPlan) -> Result<()> {
    for expr in plan.direct_expressions() {
        if expression_has_subquery(expr) {
            return Err(DecorrelationError::Invariant(format!(
                "a subquery expression ({}) survived decorrelation",
                expr.variant_label()
            )));
        }
    }
    for child in plan.children() {
        raise_if_subquery_expression(child)?;
    }
    Ok(())
}
