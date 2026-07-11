//! A well-scoped-plan validator: the loud safety net that re-checks, after
//! decorrelation, that every qualified column reference resolves to a relation
//! its operator's inputs actually expose. The binder establishes this on the
//! original tree; a rewrite can break it on the rewritten tree and nothing else
//! re-checks. Ports `optimizer/scope_validator.py`.
//!
//! Scope is qualifier-level (does the table/alias exist in scope?), not
//! column-level (the binder already checked columns). Qualifier-level is the
//! strongest low-false-positive signal and catches the item-1 failure mode: a
//! SEMI/join condition referencing a relation introduced only by its parent,
//! which would silently return wrong rows. `validate_scope` raises on the first
//! non-empty result so a mis-scoped plan fails at the layer that built it.
//!
//! Deliberately permissive on UNqualified refs (`table = None`): decorrelation
//! legitimately mints inner-physical output names that a `SubqueryScan` boundary
//! re-qualifies, matching the Python validator. A separate guard
//! (`expose_predicate_refs`) rejects an unqualified ref that would escape into a
//! join condition.

use std::collections::HashSet;

use fq_plan::expr::column_refs;
use fq_plan::logical::{JoinType, LogicalPlan};

use crate::error::DecorrelationError;

/// Raise if any qualified reference is out of scope. The stage label names where
/// the mis-scoped plan was produced, so the failure points at the transform that
/// broke the invariant.
pub fn validate_scope(plan: &LogicalPlan, stage: &str) -> Result<(), DecorrelationError> {
    let mut violations = Vec::new();
    find_scope_violations(plan, &HashSet::new(), &mut violations);
    if violations.is_empty() {
        return Ok(());
    }
    Err(DecorrelationError::Scope(format!(
        "{stage}: {}",
        violations.join("; ")
    )))
}

/// Collect every qualified reference whose qualifier is out of scope at the node
/// that uses it, walking children with the outer scope each one sees.
fn find_scope_violations(
    plan: &LogicalPlan,
    outer: &HashSet<String>,
    violations: &mut Vec<String>,
) {
    let mut available = available_qualifiers(plan);
    available.extend(outer.iter().cloned());
    for expr in plan.direct_expressions() {
        for col in column_refs(expr) {
            if let Some(table) = &col.table {
                if !available.contains(table) {
                    violations.push(format!(
                        "{} references {table}.{}; in scope: {}",
                        plan_label(plan),
                        col.column,
                        sorted_list(&available)
                    ));
                }
            }
        }
    }
    for (child, child_outer) in child_scopes(plan, outer) {
        find_scope_violations(child, &child_outer, violations);
    }
}

/// Relation qualifiers a node exposes to its PARENT. Exhaustive: a new node forces
/// a new arm.
fn output_qualifiers(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan(scan) => single(
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table_name.clone()),
        ),
        LogicalPlan::SubqueryScan(node) => single(node.alias.clone()),
        LogicalPlan::CteRef(node) => {
            single(node.alias.clone().unwrap_or_else(|| node.name.clone()))
        }
        LogicalPlan::Values(_) => HashSet::new(),
        LogicalPlan::Filter(node) => output_qualifiers(&node.input),
        LogicalPlan::Projection(node) => output_qualifiers(&node.input),
        LogicalPlan::Aggregate(node) => output_qualifiers(&node.input),
        LogicalPlan::Sort(node) => output_qualifiers(&node.input),
        LogicalPlan::Limit(node) => output_qualifiers(&node.input),
        LogicalPlan::GroupedLimit(node) => output_qualifiers(&node.input),
        LogicalPlan::SingleRowGuard(node) => output_qualifiers(&node.input),
        LogicalPlan::Explain(node) => output_qualifiers(&node.input),
        LogicalPlan::Cte(node) => output_qualifiers(&node.child),
        LogicalPlan::Join(node) => join_output_qualifiers(node),
        LogicalPlan::LateralJoin(node) => union_of(
            &output_qualifiers(&node.left),
            &output_qualifiers(&node.right),
        ),
        LogicalPlan::SetOperation(node) => output_qualifiers(&node.left),
        LogicalPlan::Union(node) => union_output_qualifiers(node),
    }
}

/// A join exposes both sides, except SEMI/ANTI which expose only the left.
fn join_output_qualifiers(node: &fq_plan::logical::Join) -> HashSet<String> {
    let left = output_qualifiers(&node.left);
    if matches!(node.join_type, JoinType::Semi | JoinType::Anti) {
        return left;
    }
    union_of(&left, &output_qualifiers(&node.right))
}

/// A union exposes the qualifiers of all its branches.
fn union_output_qualifiers(node: &fq_plan::logical::Union) -> HashSet<String> {
    let mut qualifiers = HashSet::new();
    for branch in &node.inputs {
        qualifiers.extend(output_qualifiers(branch));
    }
    qualifiers
}

/// Relation qualifiers visible to a node's OWN expressions (not its parent's). A
/// join condition sees both sides; a scan's filters see the scan; every other
/// expression-bearing node sees its single input. Exhaustive.
fn available_qualifiers(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan(scan) => single(
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table_name.clone()),
        ),
        LogicalPlan::Join(node) => union_of(
            &output_qualifiers(&node.left),
            &output_qualifiers(&node.right),
        ),
        LogicalPlan::LateralJoin(node) => union_of(
            &output_qualifiers(&node.left),
            &output_qualifiers(&node.right),
        ),
        LogicalPlan::Filter(node) => output_qualifiers(&node.input),
        LogicalPlan::Projection(node) => output_qualifiers(&node.input),
        LogicalPlan::Aggregate(node) => output_qualifiers(&node.input),
        LogicalPlan::Sort(node) => output_qualifiers(&node.input),
        LogicalPlan::Limit(node) => output_qualifiers(&node.input),
        LogicalPlan::GroupedLimit(node) => output_qualifiers(&node.input),
        LogicalPlan::SingleRowGuard(node) => output_qualifiers(&node.input),
        LogicalPlan::Explain(node) => output_qualifiers(&node.input),
        LogicalPlan::Union(_)
        | LogicalPlan::SetOperation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Cte(_)
        | LogicalPlan::CteRef(_)
        | LogicalPlan::SubqueryScan(_) => HashSet::new(),
    }
}

/// Each child paired with the outer scope visible inside it. A LATERAL join's
/// right side may correlate to its left, so the left's output qualifiers are
/// added to the right child's outer scope; every other child sees the same outer
/// scope as its parent.
fn child_scopes<'a>(
    plan: &'a LogicalPlan,
    outer: &HashSet<String>,
) -> Vec<(&'a LogicalPlan, HashSet<String>)> {
    if let LogicalPlan::LateralJoin(node) = plan {
        let mut right_outer = outer.clone();
        right_outer.extend(output_qualifiers(&node.left));
        return vec![
            (node.left.as_ref(), outer.clone()),
            (node.right.as_ref(), right_outer),
        ];
    }
    plan.children()
        .into_iter()
        .map(|child| (child, outer.clone()))
        .collect()
}

/// A single-element qualifier set.
fn single(qualifier: String) -> HashSet<String> {
    let mut set = HashSet::new();
    set.insert(qualifier);
    set
}

/// The union of two qualifier sets.
fn union_of(left: &HashSet<String>, right: &HashSet<String>) -> HashSet<String> {
    let mut set = left.clone();
    set.extend(right.iter().cloned());
    set
}

/// A stable, sorted rendering of a qualifier set for an error message.
fn sorted_list(qualifiers: &HashSet<String>) -> String {
    let mut names: Vec<&String> = qualifiers.iter().collect();
    names.sort();
    let rendered: Vec<String> = names.into_iter().map(ToString::to_string).collect();
    format!("[{}]", rendered.join(", "))
}

/// A node-kind label for a scope-violation message.
fn plan_label(plan: &LogicalPlan) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
    use fq_plan::logical::{Filter, Join, Projection, Scan};

    /// A qualified integer column reference for tests.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    /// A base-table scan aliased to `alias` for tests.
    fn scan(alias: &str, columns: &[&str]) -> LogicalPlan {
        let mut node = Scan::new(
            "ds",
            "public",
            "t",
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = Some(alias.to_string());
        LogicalPlan::Scan(Box::new(node))
    }

    /// An `x = 1` predicate over a qualified column for tests.
    fn eq_one(table: &str, name: &str) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col(table, name)),
            right: Box::new(Expr::Literal {
                value: LiteralValue::Integer(1),
                data_type: DataType::Integer,
            }),
        }
    }

    #[test]
    fn well_scoped_filter_passes() {
        let plan = LogicalPlan::Filter(Filter {
            input: Box::new(scan("o", &["x"])),
            predicate: eq_one("o", "x"),
        });
        assert!(validate_scope(&plan, "test").is_ok());
    }

    #[test]
    fn out_of_scope_qualifier_raises() {
        // The filter references `p.x`, but only `o` is in scope below it.
        let plan = LogicalPlan::Filter(Filter {
            input: Box::new(scan("o", &["x"])),
            predicate: eq_one("p", "x"),
        });
        let err = validate_scope(&plan, "test").expect_err("p is out of scope");
        assert!(matches!(err, DecorrelationError::Scope(m) if m.contains("p.x")));
    }

    #[test]
    fn parent_referencing_a_semi_hidden_relation_raises() {
        // The canonical item-1 case: a SEMI join exposes only its left (`o`), so a
        // parent projection referencing the right relation (`p`) is out of scope.
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("o", &["k"])),
            right: Box::new(scan("p", &["k"])),
            join_type: JoinType::Semi,
            condition: Some(Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("o", "k")),
                right: Box::new(col("p", "k")),
            }),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let plan = LogicalPlan::Projection(Projection {
            input: Box::new(join),
            expressions: vec![col("p", "k")],
            aliases: vec!["k".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let err = validate_scope(&plan, "test").expect_err("p is hidden by the SEMI join");
        assert!(matches!(err, DecorrelationError::Scope(m) if m.contains("p.k")));
    }

    #[test]
    fn semi_join_condition_may_reference_both_sides() {
        // The join's OWN condition sees both sides, so `o.k = p.k` is in scope even
        // though the SEMI join exposes only the left to its parent.
        let plan = LogicalPlan::Join(Join {
            left: Box::new(scan("o", &["k"])),
            right: Box::new(scan("p", &["k"])),
            join_type: JoinType::Semi,
            condition: Some(Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("o", "k")),
                right: Box::new(col("p", "k")),
            }),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert!(validate_scope(&plan, "test").is_ok());
    }
}
