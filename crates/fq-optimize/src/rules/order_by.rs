//! `OrderByPushdown`: dissolve a Sort into scan order metadata when the order is
//! preserved down to the source (through projections/aggregates/unions), keeping
//! the Sort above operators that reorder rows (joins, filters). Ports
//! `OrderByPushdownRule`.

use std::collections::HashSet;

use fq_plan::expr::{and_expressions, Expr, NullsOrder};
use fq_plan::logical::{Aggregate, Filter, LogicalPlan, Projection, Scan, Sort, Union};

use crate::error::OptimizeError;
use crate::pushdown::{available_columns, column_key_set};

use super::OptimizationRule;

/// Push ORDER BY clauses to data sources when safe.
pub struct OrderByPushdown;

impl OptimizationRule for OrderByPushdown {
    fn name(&self) -> &'static str {
        "OrderByPushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        push_order_by(plan)
    }
}

/// The order metadata a Sort carries, moved as a unit onto a scan or a
/// reconstructed Sort.
#[derive(Clone)]
struct OrderKeys {
    keys: Vec<Expr>,
    ascending: Vec<bool>,
    nulls: Option<Vec<Option<NullsOrder>>>,
}

impl OrderKeys {
    /// A Sort over `input` carrying this order.
    fn into_sort(self, input: LogicalPlan) -> LogicalPlan {
        // A fresh Sort built from this consumed OrderKeys bundle plus the input;
        // OrderKeys mirrors exactly the Sort's order fields, so {input, sort_keys,
        // ascending, nulls_order} is the complete field set (Sort has no stamps).
        LogicalPlan::Sort(Sort {
            input: Box::new(input),
            sort_keys: self.keys,
            ascending: self.ascending,
            nulls_order: self.nulls,
        })
    }

    /// A scan with this order folded into its order metadata.
    fn into_scan(self, mut scan: Scan) -> LogicalPlan {
        scan.order_by_keys = Some(self.keys);
        scan.order_by_ascending = Some(self.ascending);
        scan.order_by_nulls = self.nulls;
        LogicalPlan::Scan(Box::new(scan))
    }
}

/// Recursively push ORDER BY down the plan tree. Ports `_push_order_by`.
fn push_order_by(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Sort(sort) => try_push_sort(sort),
        other => recurse_node(other),
    }
}

/// Try to push a sort into its input. A non-column sort key keeps the Sort but
/// still dissolves order metadata into any scan underneath. Ports
/// `_try_push_sort`.
fn try_push_sort(sort: Sort) -> Result<LogicalPlan, OptimizeError> {
    let Sort {
        input,
        sort_keys,
        ascending,
        nulls_order,
    } = sort;
    let order = OrderKeys {
        keys: sort_keys,
        ascending,
        nulls: nulls_order,
    };
    if !sort_keys_are_columns(&order.keys) {
        let pushed_child = attach_order_metadata(*input, &order);
        return Ok(order.into_sort(pushed_child));
    }
    match *input {
        LogicalPlan::Projection(projection) => push_through_projection(projection, order),
        LogicalPlan::Aggregate(aggregate) => push_through_aggregate(aggregate, order),
        LogicalPlan::Union(union) => push_through_union(union, order),
        LogicalPlan::Scan(scan) => Ok(order.into_scan(*scan)),
        // Filter / Join keep the Sort above (a filter breaks a top-N; a join
        // reorders rows); any other input bottoms out with the Sort in place.
        other => Ok(order.into_sort(other)),
    }
}

/// Whether every sort key is a plain column reference. Ports
/// `_sort_keys_are_columns`.
fn sort_keys_are_columns(sort_keys: &[Expr]) -> bool {
    sort_keys.iter().all(|key| matches!(key, Expr::Column(_)))
}

/// Dissolve order metadata into the scans under a subtree of pass-throughs (used
/// when the sort keys are not plain columns, so the Sort itself stays). Ports
/// `_attach_order_metadata`.
fn attach_order_metadata(node: LogicalPlan, order: &OrderKeys) -> LogicalPlan {
    match node {
        LogicalPlan::Scan(scan) => order.clone().into_scan(*scan),
        LogicalPlan::Projection(mut projection) => {
            let input = *projection.input;
            projection.input = Box::new(attach_order_metadata(input, order));
            LogicalPlan::Projection(projection)
        }
        LogicalPlan::Filter(mut filter) => {
            let input = *filter.input;
            filter.input = Box::new(attach_order_metadata(input, order));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Limit(mut limit) => {
            let input = *limit.input;
            limit.input = Box::new(attach_order_metadata(input, order));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::Aggregate(mut aggregate) => {
            let input = *aggregate.input;
            aggregate.input = Box::new(attach_order_metadata(input, order));
            LogicalPlan::Aggregate(aggregate)
        }
        LogicalPlan::Sort(mut sort) => {
            let input = *sort.input;
            sort.input = Box::new(attach_order_metadata(input, order));
            LogicalPlan::Sort(sort)
        }
        other => other,
    }
}

/// Push ORDER BY through a projection, rewriting alias sort keys back to the
/// input expressions. If the keys cannot all be resolved to the input, the Sort
/// stays above. Ports `_push_through_projection`.
fn push_through_projection(
    mut projection: Projection,
    order: OrderKeys,
) -> Result<LogicalPlan, OptimizeError> {
    let Some(rewritten_keys) = rewrite_sort_keys_for_projection(&order.keys, &projection) else {
        return Ok(order.into_sort(LogicalPlan::Projection(projection)));
    };
    // The rewritten sort keeps the original ascending/nulls, only the keys and
    // input change (it now sits directly over the projection's input).
    let inner_order = OrderKeys {
        keys: rewritten_keys,
        ascending: order.ascending.clone(),
        nulls: order.nulls.clone(),
    };
    let pushed_child = push_order_by(inner_order.into_sort(*projection.input))?;
    match pushed_child {
        LogicalPlan::Sort(child_sort) => {
            // The order could not descend; reattach the projection over the sort's
            // input (in-place: expressions/aliases/distinct/distinct_on preserved by
            // identity) and put the ORIGINAL sort back on top of the projection.
            projection.input = child_sort.input;
            Ok(order.into_sort(LogicalPlan::Projection(projection)))
        }
        other => {
            // In-place on the owned node: only the input is re-boxed to the pushed
            // child; every other projection field is preserved by identity.
            projection.input = Box::new(other);
            Ok(LogicalPlan::Projection(projection))
        }
    }
}

/// Rewrite projection sort keys to input expressions when aliases are used;
/// None if any key cannot be resolved to the input. Ports
/// `_rewrite_sort_keys_for_projection`.
fn rewrite_sort_keys_for_projection(
    sort_keys: &[Expr],
    projection: &Projection,
) -> Option<Vec<Expr>> {
    let alias_map = build_projection_alias_map(projection);
    let available = available_columns(&projection.input);
    let mut rewritten = Vec::with_capacity(sort_keys.len());
    for key in sort_keys {
        match key {
            Expr::Column(_) => {
                let mapped = map_projection_sort_column(key, &alias_map, &available)?;
                rewritten.push(mapped);
            }
            other => rewritten.push(other.clone()),
        }
    }
    Some(rewritten)
}

/// A positional map from each projection alias to its expression. Ports
/// `_build_projection_alias_map`.
fn build_projection_alias_map(projection: &Projection) -> Vec<(String, Expr)> {
    let mut alias_map = Vec::with_capacity(projection.aliases.len());
    for (alias, expr) in projection.aliases.iter().zip(projection.expressions.iter()) {
        alias_map.push((alias.clone(), expr.clone()));
    }
    alias_map
}

/// Map a projection sort column to an input expression: an unqualified alias
/// rewrites to its underlying expression; a column already exposed by the input
/// stays; otherwise None. Ports `_map_projection_sort_column`.
fn map_projection_sort_column(
    key: &Expr,
    alias_map: &[(String, Expr)],
    available: &HashSet<String>,
) -> Option<Expr> {
    let Expr::Column(column) = key else {
        return Some(key.clone());
    };
    if column.table.is_none() {
        if let Some((_, expr)) = alias_map.iter().find(|(alias, _)| alias == &column.column) {
            return Some(expr.clone());
        }
    }
    if let Some(table) = &column.table {
        if available.contains(&format!("{table}.{}", column.column)) {
            return Some(key.clone());
        }
    }
    if available.contains(&column.column) {
        return Some(key.clone());
    }
    None
}

/// Push ORDER BY through an aggregate when its keys are a subset of both the
/// aggregate output and the GROUP BY keys. Ports `_push_through_aggregate`.
fn push_through_aggregate(
    mut aggregate: Aggregate,
    order: OrderKeys,
) -> Result<LogicalPlan, OptimizeError> {
    let sort_cols = column_key_set(&order.keys);
    let output_cols: HashSet<String> = aggregate.output_names.iter().cloned().collect();
    if !sort_cols.is_subset(&output_cols) {
        return Ok(order.into_sort(LogicalPlan::Aggregate(aggregate)));
    }
    let group_cols = column_key_set(&aggregate.group_by);
    if !sort_cols.is_subset(&group_cols) {
        return Ok(order.into_sort(LogicalPlan::Aggregate(aggregate)));
    }
    let pushed_child = push_sort_into_child(order.clone(), *aggregate.input)?;
    // In-place on the owned node: only the input is re-boxed to the pushed child;
    // group_by, aggregates, output_names, grouping_sets are preserved by identity.
    aggregate.input = Box::new(pushed_child);
    Ok(order.into_sort(LogicalPlan::Aggregate(aggregate)))
}

/// Propagate sort metadata into union inputs while keeping the top Sort. Ports
/// `_push_through_union`.
fn push_through_union(mut union: Union, order: OrderKeys) -> Result<LogicalPlan, OptimizeError> {
    let mut new_inputs = Vec::with_capacity(union.inputs.len());
    // Take the inputs out to rewrite them; the union node itself is reused below
    // (its `distinct` preserved by identity, not re-listed).
    for child in std::mem::take(&mut union.inputs) {
        let pushed = push_sort_into_child(order.clone(), child)?;
        match pushed {
            LogicalPlan::Sort(sort) => new_inputs.push(*sort.input),
            other => new_inputs.push(other),
        }
    }
    union.inputs = new_inputs;
    Ok(order.into_sort(LogicalPlan::Union(union)))
}

/// Push an order into a child and return the result (order dissolved into scan
/// metadata, or a kept Sort where it could not descend). Must NOT strip a kept
/// Sort. Ports `_push_sort_into_child`.
fn push_sort_into_child(
    order: OrderKeys,
    child: LogicalPlan,
) -> Result<LogicalPlan, OptimizeError> {
    push_order_by(order.into_sort(child))
}

/// Recurse into a node's children. Filter has a special HAVING-onto-scan arm;
/// `SetOperation` rewrites both branches (via `try_map_children`). Ports
/// `_recurse_node`.
fn recurse_node(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Filter(filter) => recurse_filter(filter),
        recurse @ (LogicalPlan::SetOperation(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::SubqueryScan(_)
        | LogicalPlan::Cte(_)) => recurse.try_map_children(push_order_by),
        other => Ok(other),
    }
}

/// Recurse into a Filter, folding a HAVING predicate onto an aggregate-bearing
/// scan the order pushdown just produced. Ports `_recurse_filter`.
fn recurse_filter(mut filter: Filter) -> Result<LogicalPlan, OptimizeError> {
    let before = (*filter.input).clone();
    let new_input = push_order_by(*filter.input)?;
    if new_input == before {
        // Unchanged: reattach the child (in-place) and keep the filter as-is; its
        // predicate is preserved by identity.
        filter.input = Box::new(new_input);
        return Ok(LogicalPlan::Filter(filter));
    }
    match new_input {
        LogicalPlan::Scan(mut scan) if scan.aggregates.is_some() => {
            scan.filters = and_expressions(scan.filters.take(), Some(filter.predicate));
            Ok(LogicalPlan::Scan(scan))
        }
        other => {
            // In-place on the owned node: only the input is re-boxed to the pushed
            // child; the predicate is preserved by identity.
            filter.input = Box::new(other);
            Ok(LogicalPlan::Filter(filter))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::ColumnRef;

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

    /// An unqualified column reference (used for a projection alias key).
    fn alias_col(name: &str) -> Expr {
        Expr::Column(ColumnRef::new(None, name, Some(DataType::Integer)))
    }

    /// A Sort over `input` ascending by `keys`.
    fn sort(input: LogicalPlan, keys: Vec<Expr>) -> LogicalPlan {
        let ascending = vec![true; keys.len()];
        LogicalPlan::Sort(Sort {
            input: Box::new(input),
            sort_keys: keys,
            ascending,
            nulls_order: None,
        })
    }

    #[test]
    fn sort_over_scan_dissolves_into_scan_metadata() {
        let plan = sort(scan("orders", &["ts"]), vec![col("orders", "ts")]);
        let LogicalPlan::Scan(scan) = OrderByPushdown.apply(plan).unwrap() else {
            panic!("the sort dissolves into the scan");
        };
        assert_eq!(scan.order_by_keys, Some(vec![col("orders", "ts")]));
        assert_eq!(scan.order_by_ascending, Some(vec![true]));
    }

    #[test]
    fn sort_over_projection_alias_maps_back_and_pushes_to_scan() {
        // SELECT o.ts AS t FROM orders o ORDER BY t
        let projection = LogicalPlan::Projection(Projection {
            input: Box::new(scan("orders", &["ts"])),
            expressions: vec![col("orders", "ts")],
            aliases: vec!["t".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let plan = sort(projection, vec![alias_col("t")]);
        let LogicalPlan::Projection(projection) = OrderByPushdown.apply(plan).unwrap() else {
            panic!("the projection stays, the sort dissolves under it");
        };
        let LogicalPlan::Scan(scan) = *projection.input else {
            panic!("the sort dissolved into the scan under the projection");
        };
        assert_eq!(scan.order_by_keys, Some(vec![col("orders", "ts")]));
    }

    #[test]
    fn sort_over_filter_stays_above() {
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan("orders", &["ts", "region"])),
            predicate: col("orders", "region"),
        });
        let plan = sort(filter, vec![col("orders", "ts")]);
        // A filter between Sort and a possible LIMIT breaks the top-N; Sort stays.
        assert!(matches!(
            OrderByPushdown.apply(plan).unwrap(),
            LogicalPlan::Sort(_)
        ));
    }

    #[test]
    fn sort_over_join_stays_above() {
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
        let plan = sort(join, vec![col("l", "id")]);
        assert!(matches!(
            OrderByPushdown.apply(plan).unwrap(),
            LogicalPlan::Sort(_)
        ));
    }

    #[test]
    fn sort_over_aggregate_non_group_key_stays_above() {
        // Sort key is not a GROUP BY column -> Sort stays above the aggregate.
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(scan("sales", &["region", "amount"])),
            group_by: vec![col("sales", "region")],
            aggregates: vec![col("sales", "amount")],
            output_names: vec!["region".to_string(), "amount".to_string()],
            grouping_sets: None,
        });
        let plan = sort(aggregate, vec![alias_col("amount")]);
        assert!(matches!(
            OrderByPushdown.apply(plan).unwrap(),
            LogicalPlan::Sort(_)
        ));
    }

    #[test]
    fn apply_is_idempotent() {
        let plan = sort(scan("orders", &["ts"]), vec![col("orders", "ts")]);
        let once = OrderByPushdown.apply(plan).unwrap();
        let twice = OrderByPushdown.apply(once.clone()).unwrap();
        assert_eq!(once, twice);
    }
}
