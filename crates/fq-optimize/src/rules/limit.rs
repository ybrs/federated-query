//! `LimitPushdown`: push LIMIT/OFFSET into scans (as folded metadata) when safe.
//! Ports `LimitPushdownRule`.

use fq_plan::logical::{Limit, LogicalPlan, Projection, Scan, Sort};

use crate::error::OptimizeError;

use super::OptimizationRule;

/// Push LIMIT operators closer to data sources when safe.
pub struct LimitPushdown;

impl OptimizationRule for LimitPushdown {
    fn name(&self) -> &'static str {
        "LimitPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        rewrite_plan(plan)
    }
}

/// Recursively rewrite the plan, pushing limits when safe. A Limit is pushed
/// downward; the handled inner nodes just recurse; `SetOperation` and
/// `SingleRowGuard` (absent here) bottom out unchanged. Ports `_rewrite_plan`.
fn rewrite_plan(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Limit(limit) => push_limit_node(limit),
        recurse @ (LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::SubqueryScan(_)
        | LogicalPlan::Cte(_)) => recurse.try_map_children(rewrite_plan),
        other => Ok(other),
    }
}

/// Push a single Limit downward when safe. Ports `_push_limit_node`.
fn push_limit_node(limit: Limit) -> Result<LogicalPlan, OptimizeError> {
    let Limit {
        input,
        limit: limit_value,
        offset,
    } = limit;
    let input_node = rewrite_plan(*input)?;
    match input_node {
        LogicalPlan::Projection(projection) => {
            Ok(push_through_projection(projection, limit_value, offset))
        }
        LogicalPlan::Sort(sort) => push_limit_with_sort(sort, limit_value, offset),
        LogicalPlan::Scan(scan) => {
            let scan_with_limit = apply_limit_to_scan(*scan, limit_value, offset);
            // The offset is now folded into the scan, so the outer Limit zeroes it.
            Ok(wrap_limit(
                LogicalPlan::Scan(Box::new(scan_with_limit)),
                limit_value,
                0,
            ))
        }
        other => Ok(wrap_limit(other, limit_value, offset)),
    }
}

/// Whether a DISTINCT projection blocks pushing a LIMIT below it: safe over a
/// single Scan (renders as one `SELECT DISTINCT ... LIMIT`), never over anything
/// else (the DISTINCT runs locally and a LIMIT beneath would drop distinct rows).
/// Ports `_distinct_blocks_pushdown`.
fn distinct_blocks_pushdown(projection: &Projection) -> bool {
    if !(projection.distinct || projection.distinct_on.is_some()) {
        return false;
    }
    !matches!(&*projection.input, LogicalPlan::Scan(_))
}

/// Move a limit below a projection. The OFFSET is consumed by the scan only when
/// the child IS a scan; otherwise the outer Limit retains it (zeroing an offset
/// that was never pushed would corrupt pagination). Ports
/// `_push_through_projection`.
fn push_through_projection(
    mut projection: Projection,
    limit_value: Option<u64>,
    offset: u64,
) -> LogicalPlan {
    if distinct_blocks_pushdown(&projection) {
        return wrap_limit(LogicalPlan::Projection(projection), limit_value, offset);
    }
    let child_is_scan = matches!(&*projection.input, LogicalPlan::Scan(_));
    let pushed_child = apply_limit_metadata(*projection.input, limit_value, offset);
    let retained_offset = if child_is_scan { 0 } else { offset };
    let limited = wrap_limit(pushed_child, limit_value, retained_offset);
    // In-place on the owned node: only the input is re-boxed (to the limited child);
    // expressions, aliases, distinct, distinct_on are preserved by identity.
    projection.input = Box::new(limited);
    LogicalPlan::Projection(projection)
}

/// Move a limit below a sort, folding order + limit into a scan when the sort's
/// input is one. Ports `_push_limit_with_sort`.
fn push_limit_with_sort(
    mut sort: Sort,
    limit_value: Option<u64>,
    offset: u64,
) -> Result<LogicalPlan, OptimizeError> {
    let sorted_input = rewrite_plan(*sort.input)?;
    match sorted_input {
        LogicalPlan::Scan(mut scan) => {
            scan.order_by_keys = Some(sort.sort_keys);
            scan.order_by_ascending = Some(sort.ascending);
            scan.order_by_nulls = sort.nulls_order;
            scan.limit = limit_value;
            scan.offset = offset;
            Ok(wrap_limit(LogicalPlan::Scan(scan), limit_value, 0))
        }
        other => {
            // In-place on the owned node: only the input is re-boxed (to the
            // rewritten child); sort_keys, ascending, nulls_order preserved by
            // identity.
            sort.input = Box::new(other);
            Ok(wrap_limit(LogicalPlan::Sort(sort), limit_value, offset))
        }
    }
}

/// Attach limit/offset metadata to a scan while keeping plan shape; any other
/// node passes through. Ports `_apply_limit_metadata`.
fn apply_limit_metadata(node: LogicalPlan, limit_value: Option<u64>, offset: u64) -> LogicalPlan {
    match node {
        LogicalPlan::Scan(scan) => {
            LogicalPlan::Scan(Box::new(apply_limit_to_scan(*scan, limit_value, offset)))
        }
        other => other,
    }
}

/// Fold limit/offset onto a scan, keeping the TIGHTER existing limit and
/// accumulating offsets. Ports `_apply_limit_to_scan`.
fn apply_limit_to_scan(mut scan: Scan, limit_value: Option<u64>, offset_value: u64) -> Scan {
    if scan.limit == limit_value && scan.offset == offset_value {
        return scan;
    }
    let scan_limit_tighter = match (scan.limit, limit_value) {
        (Some(_), None) => true,
        (Some(existing), Some(incoming)) => existing < incoming,
        (None, _) => false,
    };
    let (effective_limit, effective_offset) = if scan_limit_tighter {
        (scan.limit, scan.offset + offset_value)
    } else if scan.offset != 0 {
        (limit_value, scan.offset + offset_value)
    } else {
        (limit_value, offset_value)
    };
    scan.limit = effective_limit;
    scan.offset = effective_offset;
    scan
}

/// A Limit over `input` with the given cap and offset.
fn wrap_limit(input: LogicalPlan, limit: Option<u64>, offset: u64) -> LogicalPlan {
    // A genuinely fresh Limit wrapper built from the three arguments; there is no
    // base node to copy from, and {input, limit, offset} is the complete Limit
    // field set (Limit carries no estimate/stamp fields).
    LogicalPlan::Limit(Limit {
        input: Box::new(input),
        limit,
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{ColumnRef, Expr, NullsOrder};

    /// A scan reading `columns` from `table`.
    fn scan(table: &str, columns: &[&str]) -> Scan {
        Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        )
    }

    /// A qualified column reference.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    #[test]
    fn limit_over_scan_folds_into_scan_and_zeroes_outer_offset() {
        let plan = wrap_limit(LogicalPlan::Scan(Box::new(scan("t", &["a"]))), Some(10), 5);
        let LogicalPlan::Limit(limit) = LimitPushdown.apply(plan).unwrap() else {
            panic!("expected a Limit at the root");
        };
        assert_eq!(limit.offset, 0, "offset was folded into the scan");
        let LogicalPlan::Scan(inner) = *limit.input else {
            panic!("Limit input should be the scan");
        };
        assert_eq!(inner.limit, Some(10));
        assert_eq!(inner.offset, 5);
    }

    #[test]
    fn limit_through_projection_over_non_scan_retains_offset() {
        // Limit over Projection over Filter(Scan): the child is not a scan, so
        // the offset must NOT be zeroed (nothing was pushed to a source).
        let filter = LogicalPlan::Filter(fq_plan::logical::Filter {
            input: Box::new(LogicalPlan::Scan(Box::new(scan("t", &["a"])))),
            predicate: col("t", "a"),
        });
        let projection = Projection {
            input: Box::new(filter),
            expressions: vec![col("t", "a")],
            aliases: vec!["a".to_string()],
            distinct: false,
            distinct_on: None,
        };
        let plan = wrap_limit(LogicalPlan::Projection(projection), Some(3), 7);
        let LogicalPlan::Projection(result) = LimitPushdown.apply(plan).unwrap() else {
            panic!("projection stays on top");
        };
        let LogicalPlan::Limit(limit) = *result.input else {
            panic!("Limit sinks below the projection");
        };
        assert_eq!(limit.offset, 7, "offset retained over a non-scan child");
        assert_eq!(limit.limit, Some(3));
    }

    #[test]
    fn distinct_projection_over_join_keeps_limit_above() {
        let join = LogicalPlan::Join(fq_plan::logical::Join {
            left: Box::new(LogicalPlan::Scan(Box::new(scan("l", &["id"])))),
            right: Box::new(LogicalPlan::Scan(Box::new(scan("r", &["id"])))),
            join_type: fq_plan::logical::JoinType::Inner,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let projection = Projection {
            input: Box::new(join),
            expressions: vec![col("l", "id")],
            aliases: vec!["id".to_string()],
            distinct: true,
            distinct_on: None,
        };
        let plan = wrap_limit(LogicalPlan::Projection(projection), Some(5), 0);
        // DISTINCT over a join: the Limit stays above the projection unchanged.
        let LogicalPlan::Limit(limit) = LimitPushdown.apply(plan).unwrap() else {
            panic!("Limit must stay at the root over a DISTINCT-over-join");
        };
        assert!(matches!(*limit.input, LogicalPlan::Projection(_)));
    }

    #[test]
    fn sort_then_limit_becomes_scan_top_n() {
        let sort = LogicalPlan::Sort(Sort {
            input: Box::new(LogicalPlan::Scan(Box::new(scan("t", &["id"])))),
            sort_keys: vec![col("t", "id")],
            ascending: vec![true],
            nulls_order: Some(vec![Some(NullsOrder::Last)]),
        });
        let plan = wrap_limit(sort, Some(10), 0);
        let LogicalPlan::Limit(limit) = LimitPushdown.apply(plan).unwrap() else {
            panic!("outer Limit remains");
        };
        let LogicalPlan::Scan(inner) = *limit.input else {
            panic!("Sort+Limit fold into the scan");
        };
        assert_eq!(inner.order_by_keys, Some(vec![col("t", "id")]));
        assert_eq!(inner.limit, Some(10));
    }

    #[test]
    fn tighter_existing_scan_limit_wins_and_offsets_accumulate() {
        let mut base = scan("t", &["a"]);
        base.limit = Some(5);
        base.offset = 2;
        // Incoming looser limit 100, offset 3: keep the tighter 5, add offsets.
        let tightened = apply_limit_to_scan(base, Some(100), 3);
        assert_eq!(tightened.limit, Some(5));
        assert_eq!(tightened.offset, 5);
    }

    #[test]
    fn apply_is_idempotent() {
        let plan = wrap_limit(LogicalPlan::Scan(Box::new(scan("t", &["a"]))), Some(10), 5);
        let once = LimitPushdown.apply(plan).unwrap();
        let twice = LimitPushdown.apply(once.clone()).unwrap();
        assert_eq!(once, twice, "apply twice must equal apply once");
    }
}
