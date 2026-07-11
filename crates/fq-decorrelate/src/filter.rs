//! The filter path: thread boolean subquery conjuncts into SEMI/ANTI joins around
//! the running plan, keep non-subquery conjuncts as a residual filter, and restore
//! a HAVING filter's schema. Ports `_rewrite_filter` and its conjunct helpers.

use fq_plan::expr::{split_conjuncts, split_disjuncts, Expr};
use fq_plan::logical::{Filter, Join, JoinType, LogicalPlan, Projection};

use crate::boolean::{constant_exists_value, negated_subquery_conjunct};
use crate::helpers::{
    and_join, bool_literal, expression_has_subquery, is_subquery_node, unqualified_col,
};
use crate::value::tighten_scalar_equality;
use crate::{Decorrelator, Result};

impl Decorrelator {
    /// Rewrite a filter: joins for boolean subquery conjuncts. A HAVING (a filter
    /// over an aggregate) is re-projected back to the aggregate's columns; a WHERE
    /// filter is left as the join-threaded plan.
    pub(crate) fn rewrite_filter(&mut self, node: Filter) -> Result<LogicalPlan> {
        let input_plan = self.rewrite_plan(*node.input)?;
        if !expression_has_subquery(&node.predicate) {
            return Ok(LogicalPlan::Filter(Filter {
                input: Box::new(input_plan),
                predicate: node.predicate,
            }));
        }
        let input_is_aggregate = matches!(input_plan, LogicalPlan::Aggregate(_));
        let original_schema = input_plan.schema();
        let rewritten = self.rewrite_filter_predicate(input_plan, &node.predicate)?;
        if input_is_aggregate {
            return Ok(restore_filter_schema(rewritten, &original_schema));
        }
        Ok(rewritten)
    }

    /// Rewrite a subquery-bearing predicate into joins (AND) or a disjunctive
    /// rewrite (OR). A top-level OR routes to the domain-union / flag path.
    fn rewrite_filter_predicate(
        &mut self,
        input_plan: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan> {
        let disjuncts = split_disjuncts(predicate);
        if disjuncts.len() > 1 {
            let owned: Vec<Expr> = disjuncts.into_iter().cloned().collect();
            return self.expand_or(input_plan, owned);
        }
        self.rewrite_filter_conjuncts(input_plan, predicate)
    }

    /// Apply each conjunct as a join or a residual filter predicate.
    fn rewrite_filter_conjuncts(
        &mut self,
        input_plan: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan> {
        let mut plan = input_plan;
        let mut kept: Vec<Expr> = Vec::new();
        for conjunct in split_conjuncts(predicate) {
            let (next_plan, residual) = self.apply_conjunct(plan, conjunct.clone())?;
            plan = next_plan;
            if let Some(residual) = residual {
                kept.push(residual);
            }
        }
        if kept.is_empty() {
            return Ok(plan);
        }
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(plan),
            predicate: and_join(kept)?,
        }))
    }

    /// Turn one conjunct into a join (consumed) or keep it as a residual predicate.
    fn apply_conjunct(
        &mut self,
        plan: LogicalPlan,
        conjunct: Expr,
    ) -> Result<(LogicalPlan, Option<Expr>)> {
        if let Some(value) = constant_exists_value(&conjunct) {
            // A constant boolean residual for an always-true/false EXISTS over a
            // global aggregate; it stays as a filter predicate, no join required.
            return Ok((plan, Some(bool_literal(value))));
        }
        if let Some(negated) = negated_subquery_conjunct(&conjunct)? {
            return self.apply_conjunct(plan, negated);
        }
        if is_subquery_node(&conjunct) {
            let (right, condition, positive) = self.semi_anti_parts(conjunct)?;
            let join_type = if positive {
                JoinType::Semi
            } else {
                JoinType::Anti
            };
            let join = LogicalPlan::Join(Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type,
                condition,
                natural: false,
                using: None,
                estimated_rows: None,
                estimate_defaults: None,
            });
            return Ok((join, None));
        }
        if !expression_has_subquery(&conjunct) {
            return Ok((plan, Some(conjunct)));
        }
        // An OR of positive same-key existentials collapses to one domain-union
        // SEMI join, consuming the whole conjunct with no residual predicate.
        if let Some((outer_expr, parts)) = self.or_conjunct_domain_parts(&conjunct)? {
            let joined = self.build_domain_semi(plan, outer_expr, parts);
            return Ok((joined, None));
        }
        // Otherwise a scalar subquery embedded in the predicate: thread its LEFT
        // join in, then tighten a residual `col = subq_v0` equality to an INNER
        // equi join (a cross join plus filter would hide it from semi-join reduction).
        let (rewritten, plan) = self.rewrite_value_expr(conjunct, plan)?;
        if let Some(tightened) = tighten_scalar_equality(&rewritten, &plan) {
            return Ok((tightened, None));
        }
        Ok((plan, Some(rewritten)))
    }
}

/// Re-project a subquery-joined filter back to its pre-join columns. A SEMI/ANTI
/// join preserves the left schema, so this is a no-op on the boolean path; it only
/// bites when a scalar subquery in a HAVING added a value column.
fn restore_filter_schema(plan: LogicalPlan, names: &[String]) -> LogicalPlan {
    if plan.schema() == names {
        return plan;
    }
    let mut expressions = Vec::with_capacity(names.len());
    for name in names {
        expressions.push(unqualified_col(name));
    }
    LogicalPlan::Projection(Projection {
        input: Box::new(plan),
        expressions,
        aliases: names.to_vec(),
        distinct: false,
        distinct_on: None,
    })
}
