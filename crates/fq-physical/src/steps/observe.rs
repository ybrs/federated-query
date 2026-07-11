//! Observation provenance recorders: what a binding's engine-measured output-row
//! count MEANS for the learned-stats catalog (a base table's rows, a column's NDV, a
//! group count, a predicate's output). Kept OUT of the steps (a write-path concern);
//! returned in `BuiltSteps.observations`. Ports `_record_*` of `executor/rust_ir.py`.

use fq_optimize::scan_predicate_template;
use fq_plan::logical::Scan as LogicalScan;
use fq_plan::physical::{
    GroupObservation, PhysicalHashAggregate, PhysicalPlan, PhysicalRemoteQuery, PhysicalScan,
};
use fq_plan::Expr;

use super::reduction::originating_relation;
use super::types::Observation;
use super::{node_id, Ctx};

/// Record what a base scan's measured output rows mean for the catalog. A pushed
/// remote island -> its group provenance; a filtered scan -> its predicate template;
/// a plain single-table GROUP BY -> the group count; any other grouped/aggregate
/// scan -> nothing; a plain unfiltered ungrouped scan -> the table's base row count.
/// Ports `_record_scan_observation`.
pub(super) fn record_scan_observation(ctx: &mut Ctx<'_>, node: &PhysicalPlan, binding: &str) {
    if let PhysicalPlan::RemoteQuery(remote) = node {
        record_island_group(ctx, remote, binding);
        return;
    }
    let PhysicalPlan::Scan(scan) = node else {
        return;
    };
    if scan.filters.is_some() {
        record_predicate_observation(ctx, node, scan, binding);
        return;
    }
    if plain_grouped(scan) {
        record_group_observation(ctx, scan, binding);
        return;
    }
    if scan_folds_grouping(scan) {
        return;
    }
    ctx.observations.insert(
        binding.to_string(),
        Observation::TableRows {
            datasource: scan.datasource.clone(),
            schema: scan.schema_name.clone(),
            table: scan.table_name.clone(),
        },
    );
}

/// A shipped island's group provenance -> the measured group count. Ports
/// `_record_island_group`.
fn record_island_group(ctx: &mut Ctx<'_>, remote: &PhysicalRemoteQuery, binding: &str) {
    if let Some(observation) = &remote.group_observation {
        ctx.observations
            .insert(binding.to_string(), group_observation(observation));
    }
}

/// A filtered plain scan's measured output, keyed by its constant-neutral template.
/// Excluded: a grouped/aggregated scan and a REDUCED scan (an injected IN list
/// shrinks output below what the predicate keeps). Ports `_record_predicate_observation`.
fn record_predicate_observation(
    ctx: &mut Ctx<'_>,
    node: &PhysicalPlan,
    scan: &PhysicalScan,
    binding: &str,
) {
    if scan_folds_grouping(scan) {
        return;
    }
    if scan.dynamic_filter_keys.is_some() || ctx.injected.contains_key(&node_id(node)) {
        return;
    }
    let Some(template) = scan_predicate_template(&logical_scan_for_template(scan)) else {
        return;
    };
    ctx.observations.insert(
        binding.to_string(),
        Observation::Predicate {
            datasource: scan.datasource.clone(),
            schema: scan.schema_name.clone(),
            table: scan.table_name.clone(),
            template,
        },
    );
}

/// A pushed single-table GROUP BY: the output rows are the group count for
/// (table, group columns). Only plain-column keys. Ports `_record_group_observation`.
fn record_group_observation(ctx: &mut Ctx<'_>, scan: &PhysicalScan, binding: &str) {
    let Some(columns) = plain_group_columns(scan) else {
        return;
    };
    ctx.observations.insert(
        binding.to_string(),
        Observation::Group {
            subject: format!(
                "{}.{}.{}",
                scan.datasource, scan.schema_name, scan.table_name
            ),
            columns,
        },
    );
}

/// A coordinator aggregate's group provenance -> the measured group count. Ports
/// `_record_group_signature`.
pub(super) fn record_group_signature(
    ctx: &mut Ctx<'_>,
    node: &PhysicalHashAggregate,
    binding: &str,
) {
    if let Some(observation) = &node.group_observation {
        ctx.observations
            .insert(binding.to_string(), group_observation(observation));
    }
}

/// A collect_distinct's rows are a base COLUMN's NDV - only when the collected key
/// IS a column of an UNREDUCED base scan. Ports `_record_ndv_observation`.
pub(super) fn record_ndv_observation(
    ctx: &mut Ctx<'_>,
    build_child: &PhysicalPlan,
    key_expr: &Expr,
    keys_binding: &str,
) {
    let Some(node) = ndv_base_node(ctx, build_child, key_expr) else {
        return;
    };
    let (PhysicalPlan::Scan(scan), Expr::Column(col)) = (node, key_expr) else {
        return;
    };
    ctx.observations.insert(
        keys_binding.to_string(),
        Observation::ColumnNdv {
            datasource: scan.datasource.clone(),
            schema: scan.schema_name.clone(),
            table: scan.table_name.clone(),
            column: col.column.clone(),
        },
    );
}

/// The base scan node whose real column the collected key IS, or None. A scan with a
/// filter/aggregate, or one reduced by an injected filter, is excluded. Ports
/// `_ndv_base_scan`.
fn ndv_base_node<'a>(
    ctx: &Ctx<'_>,
    build_child: &'a PhysicalPlan,
    key_expr: &Expr,
) -> Option<&'a PhysicalPlan> {
    let node = if matches!(build_child, PhysicalPlan::Scan(_)) {
        build_child
    } else {
        originating_relation(build_child, key_expr)?
    };
    let PhysicalPlan::Scan(scan) = node else {
        return None;
    };
    if scan.filters.is_some() || scan_folds_grouping(scan) {
        return None;
    }
    if scan.dynamic_filter_keys.is_some() || ctx.injected.contains_key(&node_id(node)) {
        return None;
    }
    if !node.column_aliases().contains_key(&key_pair_of(key_expr)) {
        return None;
    }
    Some(node)
}

/// A `GroupObservation` stamp as an `Observation::Group`.
fn group_observation(observation: &GroupObservation) -> Observation {
    Observation::Group {
        subject: observation.subject.clone(),
        columns: observation.columns.clone(),
    }
}

/// A logical scan carrying only what `scan_predicate_template` reads (filter, alias,
/// table identity) - the small adapter from a physical scan. The template neutralizes
/// the qualifier and drops constants, so the physical columns/clauses are irrelevant.
fn logical_scan_for_template(scan: &PhysicalScan) -> LogicalScan {
    let mut logical = LogicalScan::new(
        scan.datasource.clone(),
        scan.schema_name.clone(),
        scan.table_name.clone(),
        scan.columns.clone(),
    );
    logical.filters.clone_from(&scan.filters);
    logical.alias.clone_from(&scan.alias);
    logical
}

/// The group-by column names when every key is a plain (non-star) column, else None.
/// Ports `_plain_group_columns`.
fn plain_group_columns(scan: &PhysicalScan) -> Option<Vec<String>> {
    let mut columns = Vec::new();
    for key in scan.group_by.as_deref().unwrap_or(&[]) {
        match key {
            Expr::Column(col) if col.column != "*" => columns.push(col.column.clone()),
            _ => return None,
        }
    }
    Some(columns)
}

/// Whether a scan has a plain single-table GROUP BY (group keys, no GROUPING SETS).
fn plain_grouped(scan: &PhysicalScan) -> bool {
    scan.group_by.as_ref().is_some_and(|keys| !keys.is_empty())
        && scan.grouping_sets.as_ref().is_none_or(Vec::is_empty)
}

/// Whether a scan folds any aggregate/grouping shape.
fn scan_folds_grouping(scan: &PhysicalScan) -> bool {
    scan.aggregates
        .as_ref()
        .is_some_and(|aggs| !aggs.is_empty())
        || scan.group_by.as_ref().is_some_and(|keys| !keys.is_empty())
        || scan
            .grouping_sets
            .as_ref()
            .is_some_and(|sets| !sets.is_empty())
}

/// The (qualifier, column) pair of a column-reference key.
fn key_pair_of(key: &Expr) -> (Option<String>, String) {
    match key {
        Expr::Column(col) => (col.table.clone(), col.column.clone()),
        _ => (None, String::new()),
    }
}
