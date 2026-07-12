//! `SemiJoinPushdown`: commute a selective SEMI/ANTI join below the INNER join it
//! sits on, onto the one side its condition references. Ports
//! `SemiJoinPushdownRule`.
//!
//! Correctness: `SEMI(A JOIN B, p) == SEMI(A, p) JOIN B` and
//! `ANTI(A JOIN B, p) == ANTI(A, p) JOIN B` whenever `p` references only `A`. The
//! rule fires only for that provable shape (plain INNER host, one-sided
//! condition); everything else is left untouched.

use std::collections::HashSet;

use fq_plan::expr::{column_refs, ColumnRef};
use fq_plan::logical::{Join, JoinType, LogicalPlan};

use crate::error::OptimizeError;
use crate::pushdown::available_columns;

use super::OptimizationRule;

/// Push a SEMI/ANTI join below the INNER join it sits on.
pub struct SemiJoinPushdown;

impl OptimizationRule for SemiJoinPushdown {
    fn name(&self) -> &'static str {
        "SemiJoinPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        rewrite(plan)
    }
}

/// One of the two sides of the inner join a semi may commute onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Side {
    Left,
    Right,
}

/// The region a single condition column belongs to.
#[derive(Debug, Clone, Copy)]
enum Region {
    Left,
    Right,
    Subquery,
}

/// Try to push a semi/anti join here, then recurse (so a pushed join keeps
/// descending toward the relation its condition references). Ports `_rewrite`.
fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    let pushed = match plan {
        LogicalPlan::Join(join) => try_push(join),
        other => other,
    };
    pushed.try_map_children(rewrite)
}

/// Rewrite `SEMI/ANTI(INNER(X, Y), subq)` to push the semi onto the one inner
/// side its condition references, or return the join unchanged. Ports
/// `_try_push`.
fn try_push(join: Join) -> LogicalPlan {
    if !matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
        return LogicalPlan::Join(join);
    }
    if !is_plain_inner(&join.left) {
        return LogicalPlan::Join(join);
    }
    match condition_side(&join) {
        Some(side) => push_to_side(join, side),
        None => LogicalPlan::Join(join),
    }
}

/// A host we may commute a semi through: a plain INNER join, never a
/// NATURAL/USING one (its condition is implicit and must not be split). Ports
/// `_is_plain_inner`.
fn is_plain_inner(node: &LogicalPlan) -> bool {
    let LogicalPlan::Join(inner) = node else {
        return false;
    };
    inner.join_type == JoinType::Inner && !(inner.natural || inner.using.is_some())
}

/// Which single inner side the semi condition references, or None (both, neither,
/// or an unresolvable column - unsafe). Ports `_condition_side` /
/// `_classify_condition`.
fn condition_side(join: &Join) -> Option<Side> {
    let condition = join.condition.as_ref()?;
    let LogicalPlan::Join(inner) = join.left.as_ref() else {
        return None;
    };
    let left_cols = available_columns(&inner.left);
    let right_cols = available_columns(&inner.right);
    let subq_cols = available_columns(&join.right);

    let mut hits: HashSet<Side> = HashSet::new();
    for reference in column_refs(condition) {
        match classify_ref(reference, &left_cols, &right_cols, &subq_cols)? {
            Region::Left => {
                hits.insert(Side::Left);
            }
            Region::Right => {
                hits.insert(Side::Right);
            }
            Region::Subquery => {}
        }
    }
    if hits.len() == 1 {
        hits.into_iter().next()
    } else {
        None
    }
}

/// Place one column ref in exactly one region, else None (ambiguous or
/// unresolvable references make the push unsafe). Ports `_classify_ref`.
fn classify_ref(
    reference: &ColumnRef,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
    subq_cols: &HashSet<String>,
) -> Option<Region> {
    let mut keys: HashSet<String> = HashSet::new();
    keys.insert(reference.column.clone());
    if let Some(table) = &reference.table {
        if !table.is_empty() {
            keys.insert(format!("{table}.{}", reference.column));
        }
    }
    let mut regions: Vec<Region> = Vec::new();
    if !keys.is_disjoint(left_cols) {
        regions.push(Region::Left);
    }
    if !keys.is_disjoint(right_cols) {
        regions.push(Region::Right);
    }
    if !keys.is_disjoint(subq_cols) {
        regions.push(Region::Subquery);
    }
    match regions.as_slice() {
        [single] => Some(*single),
        _ => None,
    }
}

/// Rebuild `INNER(X, Y)` with the semi/anti join wrapped around the named side;
/// the inner join's type/condition/other side are otherwise untouched. Ports
/// `_push_to_side` / `_wrap_semi`.
fn push_to_side(mut semi: Join, side: Side) -> LogicalPlan {
    // Pull the inner join out of the semi's left. The wrapped SEMI/ANTI is the SAME
    // node as `semi` with only its `left` re-pointed at one of the inner join's
    // inputs, so we keep `semi` owned and preserve its type/condition/other side and
    // the estimated_rows/estimate_defaults STAMPS by identity (re-listing them risked
    // resetting a stamp).
    let mut inner = match *semi.left {
        LogicalPlan::Join(inner) => inner,
        other => {
            // Guarded unreachable in practice (the caller checked is_plain_inner);
            // restore the semi's left and return it unchanged (fields preserved by
            // identity) rather than risk a wrong rewrite.
            semi.left = Box::new(other);
            return LogicalPlan::Join(semi);
        }
    };
    match side {
        Side::Left => {
            let target = *inner.left;
            // In-place: the semi wraps the inner join's LEFT input; only semi.left
            // changes, every other semi field (incl. the stamps) is preserved.
            semi.left = Box::new(target);
            inner.left = Box::new(LogicalPlan::Join(semi));
        }
        Side::Right => {
            let target = *inner.right;
            // In-place: the semi wraps the inner join's RIGHT input; only semi.left
            // changes, every other semi field (incl. the stamps) is preserved.
            semi.left = Box::new(target);
            inner.right = Box::new(LogicalPlan::Join(semi));
        }
    }
    LogicalPlan::Join(inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, Expr};
    use fq_plan::logical::Scan;

    /// A scan reading `columns` from `table`.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(Box::new(Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        )))
    }

    /// A qualified column reference.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    /// An equality between two columns.
    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// A join node with the given type and condition over two inputs.
    fn join(
        join_type: JoinType,
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option<Expr>,
    ) -> Join {
        Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        }
    }

    #[test]
    fn semi_condition_on_left_commutes_onto_left_input() {
        // SEMI( INNER(orders, lineitem), subq ) with a condition on orders (left).
        // Distinct bare column names per relation keep the classification exact.
        let inner = join(
            JoinType::Inner,
            scan("orders", &["orderkey"]),
            scan("lineitem", &["linekey"]),
            Some(eq(col("orders", "orderkey"), col("lineitem", "linekey"))),
        );
        let semi = join(
            JoinType::Semi,
            LogicalPlan::Join(inner),
            scan("subq", &["subkey"]),
            Some(eq(col("orders", "orderkey"), col("subq", "subkey"))),
        );
        let result = SemiJoinPushdown.apply(LogicalPlan::Join(semi)).unwrap();
        // The INNER join is now on top; its left is the semi over orders.
        let LogicalPlan::Join(top) = result else {
            panic!("inner join surfaces to the top");
        };
        assert_eq!(top.join_type, JoinType::Inner);
        assert!(matches!(&*top.left, LogicalPlan::Join(j) if j.join_type == JoinType::Semi));
    }

    #[test]
    fn semi_condition_on_right_commutes_onto_right_input() {
        let inner = join(
            JoinType::Inner,
            scan("orders", &["orderkey"]),
            scan("lineitem", &["linekey"]),
            Some(eq(col("orders", "orderkey"), col("lineitem", "linekey"))),
        );
        let semi = join(
            JoinType::Semi,
            LogicalPlan::Join(inner),
            scan("subq", &["subkey"]),
            Some(eq(col("lineitem", "linekey"), col("subq", "subkey"))),
        );
        let LogicalPlan::Join(top) = SemiJoinPushdown.apply(LogicalPlan::Join(semi)).unwrap()
        else {
            panic!("inner join surfaces to the top");
        };
        assert!(matches!(&*top.right, LogicalPlan::Join(j) if j.join_type == JoinType::Semi));
    }

    #[test]
    fn semi_condition_spanning_both_sides_declines() {
        let inner = join(
            JoinType::Inner,
            scan("orders", &["orderkey"]),
            scan("lineitem", &["linekey"]),
            Some(eq(col("orders", "orderkey"), col("lineitem", "linekey"))),
        );
        // Condition references BOTH inner sides -> unsafe -> unchanged.
        let semi = join(
            JoinType::Semi,
            LogicalPlan::Join(inner),
            scan("subq", &["subkey"]),
            Some(eq(col("orders", "orderkey"), col("lineitem", "linekey"))),
        );
        let original = LogicalPlan::Join(semi);
        assert_eq!(
            SemiJoinPushdown.apply(original.clone()).unwrap(),
            original,
            "a both-sided condition is left untouched"
        );
    }

    #[test]
    fn semi_over_non_inner_host_declines() {
        // The host is a LEFT join, not a plain INNER -> unchanged.
        let host = join(
            JoinType::Left,
            scan("orders", &["orderkey"]),
            scan("lineitem", &["linekey"]),
            Some(eq(col("orders", "orderkey"), col("lineitem", "linekey"))),
        );
        let semi = join(
            JoinType::Semi,
            LogicalPlan::Join(host),
            scan("subq", &["subkey"]),
            Some(eq(col("orders", "orderkey"), col("subq", "subkey"))),
        );
        let original = LogicalPlan::Join(semi);
        assert_eq!(SemiJoinPushdown.apply(original.clone()).unwrap(), original);
    }
}
