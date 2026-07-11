//! `AggregatePushdown`: fold a GROUP BY over a single scan (optionally through a
//! WHERE filter) onto that scan, so the whole aggregate runs at the source. Ports
//! `AggregatePushdownRule`.

use fq_plan::expr::{and_expressions, Expr};
use fq_plan::logical::{Aggregate, Filter, LogicalPlan, Scan};

use crate::error::OptimizeError;

use super::OptimizationRule;

/// Push aggregates to data sources when the whole GROUP BY sits over one scan.
pub struct AggregatePushdown;

impl OptimizationRule for AggregatePushdown {
    fn name(&self) -> &'static str {
        "AggregatePushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        push_aggregate(plan)
    }
}

/// Recursively push aggregates down the plan tree. Ports `_push_aggregate`.
fn push_aggregate(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => try_push_aggregate(aggregate),
        other => recurse_node(other),
    }
}

/// Try to fold an aggregate onto its input scan (directly, or through a WHERE
/// filter over a scan). Otherwise the aggregate cannot fold, but its INPUT may
/// still hold a foldable aggregate (a HAVING subquery under the main aggregate),
/// so recurse. Ports `_try_push_aggregate`.
fn try_push_aggregate(aggregate: Aggregate) -> Result<LogicalPlan, OptimizeError> {
    let Aggregate {
        input,
        group_by,
        aggregates,
        output_names,
        grouping_sets,
    } = aggregate;
    let clauses = AggregateClauses {
        group_by,
        aggregates,
        output_names,
        grouping_sets,
    };
    match *input {
        LogicalPlan::Scan(scan) => Ok(push_to_scan(*scan, None, clauses)),
        LogicalPlan::Filter(filter) => {
            let Filter {
                input: filter_input,
                predicate,
            } = filter;
            match *filter_input {
                LogicalPlan::Scan(scan) => Ok(push_to_scan(*scan, Some(predicate), clauses)),
                other_inner => {
                    let rebuilt_filter = LogicalPlan::Filter(Filter {
                        input: Box::new(other_inner),
                        predicate,
                    });
                    recurse_aggregate(rebuilt_filter, clauses)
                }
            }
        }
        other => recurse_aggregate(other, clauses),
    }
}

/// The grouping clauses an aggregate carries, moved as a unit when folding onto a
/// scan or rebuilding an un-foldable aggregate.
struct AggregateClauses {
    group_by: Vec<Expr>,
    aggregates: Vec<Expr>,
    output_names: Vec<String>,
    grouping_sets: Option<Vec<Vec<Expr>>>,
}

/// Rebuild an aggregate over `input` and recurse into it (the input may still
/// hold a foldable aggregate deeper down).
fn recurse_aggregate(
    input: LogicalPlan,
    clauses: AggregateClauses,
) -> Result<LogicalPlan, OptimizeError> {
    let aggregate = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(input),
        group_by: clauses.group_by,
        aggregates: clauses.aggregates,
        output_names: clauses.output_names,
        grouping_sets: clauses.grouping_sets,
    });
    aggregate.try_map_children(push_aggregate)
}

/// Fold the aggregate's clauses (and any WHERE predicate) onto a scan. On the
/// scan the clauses become the OUTPUT shape; the filters merge None-safely. Ports
/// `_push_to_scan`.
fn push_to_scan(
    mut scan: Scan,
    filter_expr: Option<Expr>,
    clauses: AggregateClauses,
) -> LogicalPlan {
    scan.filters = and_expressions(scan.filters.take(), filter_expr);
    scan.group_by = Some(clauses.group_by);
    scan.grouping_sets = clauses.grouping_sets;
    scan.aggregates = Some(clauses.aggregates);
    scan.output_names = Some(clauses.output_names);
    LogicalPlan::Scan(Box::new(scan))
}

/// Recurse into a non-aggregate node's children. `SetOperation` rewrites both
/// branches (via `try_map_children`); `Union` is intentionally NOT recursed
/// (faithful to Python). Ports `_recurse_node`.
fn recurse_node(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        recurse @ (LogicalPlan::SetOperation(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::SubqueryScan(_)
        | LogicalPlan::Cte(_)) => recurse.try_map_children(push_aggregate),
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, LiteralValue};

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

    /// SUM(table.column) as an aggregate call.
    fn sum(table: &str, name: &str) -> Expr {
        Expr::FunctionCall {
            function_name: "SUM".to_string(),
            args: vec![col(table, name)],
            is_aggregate: true,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
        }
    }

    /// An aggregate GROUP BY region over `table.region` summing `table.amount`.
    fn grouped_aggregate(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(Aggregate {
            input: Box::new(input),
            group_by: vec![col("sales", "region")],
            aggregates: vec![sum("sales", "amount")],
            output_names: vec!["region".to_string(), "amount".to_string()],
            grouping_sets: None,
        })
    }

    #[test]
    fn aggregate_over_scan_folds_onto_scan() {
        let plan = grouped_aggregate(scan("sales", &["region", "amount"]));
        let LogicalPlan::Scan(scan) = AggregatePushdown.apply(plan).unwrap() else {
            panic!("the aggregate collapses onto the scan");
        };
        assert_eq!(scan.group_by, Some(vec![col("sales", "region")]));
        assert_eq!(scan.aggregates, Some(vec![sum("sales", "amount")]));
        assert_eq!(
            scan.output_names,
            Some(vec!["region".to_string(), "amount".to_string()])
        );
    }

    #[test]
    fn aggregate_over_filter_over_scan_merges_the_where() {
        let predicate = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col("sales", "year")),
            right: Box::new(Expr::Literal {
                value: LiteralValue::Integer(2000),
                data_type: DataType::Integer,
            }),
        };
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan("sales", &["region", "amount", "year"])),
            predicate: predicate.clone(),
        });
        let plan = grouped_aggregate(filter);
        let LogicalPlan::Scan(scan) = AggregatePushdown.apply(plan).unwrap() else {
            panic!("the aggregate + WHERE collapse onto the scan");
        };
        assert_eq!(scan.filters, Some(predicate));
        assert_eq!(scan.group_by, Some(vec![col("sales", "region")]));
    }

    #[test]
    fn aggregate_over_join_recurses_but_does_not_fold() {
        let join = LogicalPlan::Join(fq_plan::logical::Join {
            left: Box::new(scan("l", &["id"])),
            right: Box::new(scan("r", &["id"])),
            join_type: fq_plan::logical::JoinType::Inner,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let plan = grouped_aggregate(join);
        // The aggregate over a join cannot fold; it stays an Aggregate.
        assert!(matches!(
            AggregatePushdown.apply(plan).unwrap(),
            LogicalPlan::Aggregate(_)
        ));
    }

    #[test]
    fn apply_is_idempotent() {
        let plan = grouped_aggregate(scan("sales", &["region", "amount"]));
        let once = AggregatePushdown.apply(plan).unwrap();
        let twice = AggregatePushdown.apply(once.clone()).unwrap();
        assert_eq!(once, twice);
    }
}
