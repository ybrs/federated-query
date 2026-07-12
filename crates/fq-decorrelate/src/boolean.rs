//! The boolean-subquery path: EXISTS/IN/quantified comparisons become the
//! (right side, condition, positive?) parts a SEMI/ANTI join is built from, plus
//! the NOT-pushdown and constant-EXISTS handling. Ports the `_semi_anti_parts`
//! family from `decorrelation.py`.

use fq_plan::expr::{BinaryOpType, Expr, Quantifier, UnaryOpType};
use fq_plan::logical::{Aggregate, LogicalPlan, Projection};

use crate::error::DecorrelationError;
use crate::helpers::{
    and_join, binary, expression_has_subquery, is_subquery_node, null_check, qualified_col,
};
use crate::prepare::{PreparedSubquery, SubqueryPreparer};
use crate::{Decorrelator, Result};

impl Decorrelator {
    /// Create a preparer with a fresh deterministic name prefix.
    pub(crate) fn preparer(&mut self) -> SubqueryPreparer<'_> {
        let prefix = self.next_prefix();
        SubqueryPreparer::new(self, prefix)
    }

    /// Build (right side, condition, positive?) for a boolean subquery. `positive`
    /// selects SEMI (true) vs ANTI (false).
    pub(crate) fn semi_anti_parts(
        &mut self,
        expr: Expr,
    ) -> Result<(LogicalPlan, Option<Expr>, bool)> {
        match expr {
            Expr::Exists { subquery, negated } => {
                let prepared = self.preparer().prepare_exists(*subquery)?;
                Ok((prepared.plan, prepared.condition, !negated))
            }
            Expr::InSubquery {
                value,
                subquery,
                negated,
            } => self.in_parts(*value, *subquery, negated),
            Expr::QuantifiedComparison {
                operator,
                quantifier,
                left,
                subquery,
            } => self.quantified_parts(operator, quantifier, *left, *subquery),
            other => Err(DecorrelationError::Unsupported(format!(
                "Unsupported boolean subquery expression: {}",
                other.variant_label()
            ))),
        }
    }

    /// Build the join parts for IN / NOT IN. IN matches with plain equality (NULLs
    /// never match); NOT IN treats a NULL on either side as a match so the ANTI
    /// join drops rows whose result would be UNKNOWN.
    fn in_parts(
        &mut self,
        value: Expr,
        subquery: LogicalPlan,
        negated: bool,
    ) -> Result<(LogicalPlan, Option<Expr>, bool)> {
        let prepared = self.preparer().prepare_values(subquery)?;
        let items = in_value_items(value, &prepared)?;
        let mut comparisons = Vec::with_capacity(items.len());
        for (item, value_name) in items.into_iter().zip(prepared.values.iter()) {
            let value_ref = qualified_col(&prepared.alias, value_name);
            comparisons.push(match_term(item, value_ref, negated));
        }
        let condition = combine_condition(comparisons, prepared.condition)?;
        Ok((prepared.plan, Some(condition), !negated))
    }

    /// Build the join parts for op ANY/SOME/ALL. ANY keeps rows with at least one
    /// satisfying comparison (SEMI); ALL keeps rows with no violation (ANTI), where
    /// a NULL on either side counts as a violation.
    fn quantified_parts(
        &mut self,
        operator: BinaryOpType,
        quantifier: Quantifier,
        left: Expr,
        subquery: LogicalPlan,
    ) -> Result<(LogicalPlan, Option<Expr>, bool)> {
        let prepared = self.preparer().prepare_values(subquery)?;
        if prepared.values.len() != 1 {
            return Err(DecorrelationError::Unsupported(
                "Quantified comparison subquery must return one column".to_string(),
            ));
        }
        let value_ref = qualified_col(&prepared.alias, &prepared.values[0]);
        // `left` and `value_ref` each feed both the comparison and (for ALL) the
        // violation term below, so the comparison gets its own copy of each.
        let comparison = binary(operator, left.clone(), value_ref.clone());
        match quantifier {
            Quantifier::Any | Quantifier::Some => {
                let condition = combine_condition(vec![comparison], prepared.condition)?;
                Ok((prepared.plan, Some(condition), true))
            }
            Quantifier::All => {
                let violation = violation_term(comparison, left, value_ref);
                let condition = combine_condition(vec![violation], prepared.condition)?;
                Ok((prepared.plan, Some(condition), false))
            }
        }
    }
}

/// Resolve an EXISTS over an unconditional global aggregate to a constant: it
/// yields one row for every outer row, so EXISTS is always TRUE (NOT EXISTS always
/// FALSE). Key-widening would instead drop non-matching outer rows.
pub(crate) fn constant_exists_value(expr: &Expr) -> Option<bool> {
    if let Expr::Exists { subquery, negated } = expr {
        if is_unconditional_global_aggregate(subquery) {
            return Some(!negated);
        }
    }
    None
}

/// Push a `NOT` around a subquery predicate into the node itself, returning the
/// negated node to re-dispatch; None when the conjunct is not `NOT (subquery)`.
pub(crate) fn negated_subquery_conjunct(conjunct: &Expr) -> Result<Option<Expr>> {
    if let Expr::UnaryOp {
        op: UnaryOpType::Not,
        operand,
    } = conjunct
    {
        if is_subquery_node(operand) {
            return Ok(Some(negate_subquery_predicate(operand)?));
        }
    }
    Ok(None)
}

/// The outer-side value items of an IN, validated against subquery arity.
fn in_value_items(value: Expr, prepared: &PreparedSubquery) -> Result<Vec<Expr>> {
    let items = match value {
        Expr::Tuple { items } => items,
        other => vec![other],
    };
    if items.len() != prepared.values.len() {
        return Err(DecorrelationError::Unsupported(format!(
            "IN subquery returns {} columns but {} values are compared",
            prepared.values.len(),
            items.len()
        )));
    }
    Ok(items)
}

/// An equality term; NULL-aware variants also match on NULL sides.
fn match_term(outer_value: Expr, inner_ref: Expr, null_aware: bool) -> Expr {
    let equality = binary(BinaryOpType::Eq, outer_value.clone(), inner_ref.clone());
    if !null_aware {
        return equality;
    }
    let with_outer_null = binary(BinaryOpType::Or, equality, null_check(outer_value));
    binary(BinaryOpType::Or, with_outer_null, null_check(inner_ref))
}

/// A row violating `x op ALL`: the comparison fails or is UNKNOWN.
fn violation_term(comparison: Expr, left: Expr, value_ref: Expr) -> Expr {
    // Fresh node wrapping the comparison in NOT - there is no base node to copy
    // from. Field list (op/operand) is the complete UnaryOp variant.
    let negated = Expr::UnaryOp {
        op: UnaryOpType::Not,
        operand: Box::new(comparison),
    };
    let with_left_null = binary(BinaryOpType::Or, negated, null_check(left));
    binary(BinaryOpType::Or, with_left_null, null_check(value_ref))
}

/// Combine value comparisons with the correlation conjuncts.
fn combine_condition(comparisons: Vec<Expr>, correlation: Option<Expr>) -> Result<Expr> {
    let mut conjuncts = comparisons;
    if let Some(correlation) = correlation {
        conjuncts.push(correlation);
    }
    and_join(conjuncts)
}

/// The logical negation of a boolean subquery predicate.
fn negate_subquery_predicate(node: &Expr) -> Result<Expr> {
    match node {
        Expr::Exists { .. } | Expr::InSubquery { .. } => {
            // Clone-and-mutate: own a copy of the borrowed node and flip ONLY its
            // `negated` flag, leaving the subquery (and IN value) untouched. update!/
            // ..base cannot target an enum variant, so &mut on the clone is the idiom.
            let mut negated_node = node.clone();
            match &mut negated_node {
                Expr::Exists { negated, .. } | Expr::InSubquery { negated, .. } => {
                    *negated = !*negated;
                }
                _ => {
                    return Err(DecorrelationError::Invariant(
                        "negate_subquery_predicate matched a non-EXISTS/IN node".to_string(),
                    ))
                }
            }
            Ok(negated_node)
        }
        Expr::QuantifiedComparison {
            operator,
            quantifier,
            left,
            subquery,
        } => negate_quantified(*operator, *quantifier, left, subquery),
        other => Err(DecorrelationError::Unsupported(format!(
            "Cannot negate subquery predicate: {}",
            other.variant_label()
        ))),
    }
}

/// Negate `x op ANY/ALL S` by De Morgan (flip operator and quantifier).
fn negate_quantified(
    operator: BinaryOpType,
    quantifier: Quantifier,
    left: &Expr,
    subquery: &LogicalPlan,
) -> Result<Expr> {
    let Some(negated_op) = negated_binary_op(operator) else {
        return Err(DecorrelationError::Unsupported(format!(
            "Cannot negate quantified operator: {}",
            operator.value()
        )));
    };
    let flipped = match quantifier {
        Quantifier::Any | Quantifier::Some => Quantifier::All,
        Quantifier::All => Quantifier::Any,
    };
    // De Morgan negation built from decomposed parts (no original node in scope):
    // `left` is borrowed, so clone it into an owned child; `subquery` likewise.
    let left = Box::new(left.clone());
    let subquery = Box::new(subquery.clone());
    // Fresh node: field list (operator/quantifier/left/subquery) is the complete
    // QuantifiedComparison variant; there is no base to copy from.
    Ok(Expr::QuantifiedComparison {
        operator: negated_op,
        quantifier: flipped,
        left,
        subquery,
    })
}

/// The logical negation of a comparison operator (Eq/Neq/Lt/Lte/Gt/Gte); None for
/// any other operator (e.g. LIKE), which cannot be pushed through a quantifier.
fn negated_binary_op(op: BinaryOpType) -> Option<BinaryOpType> {
    match op {
        BinaryOpType::Eq => Some(BinaryOpType::Neq),
        BinaryOpType::Neq => Some(BinaryOpType::Eq),
        BinaryOpType::Lt => Some(BinaryOpType::Gte),
        BinaryOpType::Lte => Some(BinaryOpType::Gt),
        BinaryOpType::Gt => Some(BinaryOpType::Lte),
        BinaryOpType::Gte => Some(BinaryOpType::Lt),
        _ => None,
    }
}

/// True when a subquery always yields exactly one row: a global (ungrouped)
/// Aggregate, optionally under a bare Projection. A HAVING/other top node could
/// drop the row, so only a bare/projected ungrouped Aggregate qualifies.
fn is_unconditional_global_aggregate(plan: &LogicalPlan) -> bool {
    let node = match plan {
        LogicalPlan::Projection(Projection { input, .. }) => input.as_ref(),
        other => other,
    };
    matches!(node, LogicalPlan::Aggregate(Aggregate { group_by, .. }) if group_by.is_empty())
}

/// Whether any expression in a slice contains a subquery node.
pub(crate) fn any_has_subquery(expressions: &[Expr]) -> bool {
    expressions.iter().any(expression_has_subquery)
}
