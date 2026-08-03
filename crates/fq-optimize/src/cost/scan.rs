//! Scan estimation: base rows, the four-tier filter-rows ladder, pushed-down
//! grouping, and filter value caps. Ports the `_estimate_scan_tracked` family of
//! `optimizer/cost.py`.

use std::collections::HashSet;

use fq_catalog::datasource::TableStatistics;
use fq_plan::expr::{split_conjuncts, BinaryOpType, Expr};
use fq_plan::logical::Scan;

use super::selectivity::tracked_selectivity;
use super::{key_name, scan_target, CostModel};
use crate::error::Result;
use crate::estimate_defaults::{min_rows, mul_groups, CardinalityEstimate};
use crate::pushdown::bare_names;
use crate::subplan_signature::scan_predicate_template;

impl CostModel {
    /// Scan estimate: source row count x filter selectivity, then pushed-down
    /// grouping when the scan carries a remote aggregate. An UNKNOWN base row
    /// count stays unknown through the filter, but a pushed GROUP BY may still
    /// bound the output by its keys' NDV product.
    pub(super) fn estimate_scan(&self, scan: &Scan) -> Result<CardinalityEstimate> {
        let stats = self.scan_stats(scan)?;
        let (mut rows, mut defaults) = tracked_base_rows(scan, stats.as_ref());
        if scan.filters.is_some() {
            if let Some(base_rows) = rows {
                let (filtered, filter_defaults) =
                    self.scan_filter_rows(scan, stats.as_ref(), base_rows)?;
                rows = Some(filtered);
                defaults.extend(filter_defaults);
            }
        }
        if let Some(group_by) = &scan.group_by {
            if !group_by.is_empty() {
                return Ok(tracked_scan_groups(scan, stats.as_ref(), rows, defaults));
            }
        }
        Ok(CardinalityEstimate::of(rows, defaults))
    }

    /// A standalone Filter: input estimate x tracked selectivity, with NO table
    /// statistics in scope (predicates that could use stats were folded into
    /// their Scan by pushdown). An unknown selectivity reduces nothing.
    pub(super) fn estimate_filter(
        &mut self,
        node: &fq_plan::logical::Filter,
    ) -> Result<CardinalityEstimate> {
        let input = self.estimate(&node.input)?;
        let (selectivity, defaults) = tracked_selectivity(&node.predicate, None, "filter");
        let rows = input.rows.map(|rows| filtered_rows(rows, selectivity));
        Ok(CardinalityEstimate::of(
            rows,
            crate::estimate_defaults::combine_defaults(&[&input], &defaults),
        ))
    }

    /// The scan's table statistics covering its filter and group columns.
    fn scan_stats(&self, scan: &Scan) -> Result<Option<TableStatistics>> {
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        stats.get_table_statistics(
            &scan.datasource,
            &scan.schema_name,
            &scan.table_name,
            &scan_needed_columns(scan),
        )
    }

    /// (filtered rows, gap notes) for a scan's pushed predicate - the four-tier
    /// ladder: the engine's own complete pricing; else a LEARNED template output
    /// (a measurement of this shape, consulted BEFORE the source planner); else
    /// the SOURCE PLANNER's estimate; else the no-reduction bound with gaps.
    fn scan_filter_rows(
        &self,
        scan: &Scan,
        stats: Option<&TableStatistics>,
        rows: u64,
    ) -> Result<(u64, Vec<String>)> {
        let filters = scan
            .filters
            .as_ref()
            .expect("caller checked filters is Some");
        let (selectivity, gaps) = tracked_selectivity(filters, stats, &scan_target(scan));
        if gaps.is_empty() {
            return Ok((filtered_rows(rows, selectivity), Vec::new()));
        }
        if let Some(learned) = self.learned_predicate_rows(scan)? {
            return Ok((clamp_to_base(learned, rows), Vec::new()));
        }
        if let Some(planner_rows) = self.source_scan_estimate(scan)? {
            return Ok((clamp_to_base(planner_rows, rows), Vec::new()));
        }
        Ok((filtered_rows(rows, selectivity), gaps))
    }

    /// The catalog's MEASURED output rows for this scan's filter template, or
    /// `None`. Consulted only when the engine's own pricing left gaps (fill, never
    /// override) and BEFORE the source planner.
    fn learned_predicate_rows(&self, scan: &Scan) -> Result<Option<i64>> {
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        let Some(catalog) = stats.stats_catalog() else {
            return Ok(None);
        };
        let Some(template) = scan_predicate_template(scan) else {
            return Ok(None);
        };
        Ok(catalog.predicate_output_rows(
            &scan.datasource,
            &scan.schema_name,
            &scan.table_name,
            &template,
            "",
            stats.learned_ttl_seconds(),
        )?)
    }

    /// The source planner's row estimate for this filtered scan. Only a PLAIN scan
    /// asks: a pushed-aggregate scan's filter is a HAVING over aggregate OUTPUT
    /// names, which do not exist as stored columns (the probe would be invalid).
    fn source_scan_estimate(&self, scan: &Scan) -> Result<Option<i64>> {
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        if non_empty(scan.group_by.as_deref())
            || non_empty(scan.aggregates.as_deref())
            || has_grouping_sets(scan)
        {
            return Ok(None);
        }
        stats.scan_planner_estimate(scan)
    }
}

/// The columns a scan's estimate reads stats for: filter + group keys, restricted
/// to REAL base columns (a HAVING over aggregate outputs is not a stored column).
fn scan_needed_columns(scan: &Scan) -> Vec<String> {
    let mut needed: HashSet<String> = HashSet::new();
    if let Some(filters) = &scan.filters {
        needed.extend(bare_names(filters));
    }
    if let Some(group_by) = &scan.group_by {
        for key in group_by {
            needed.extend(bare_names(key));
        }
    }
    let columns: HashSet<&String> = scan.columns.iter().collect();
    let mut out: Vec<String> = needed
        .into_iter()
        .filter(|name| columns.contains(name))
        .collect();
    out.sort();
    out
}

/// The raw table row count: a measurement or honestly UNKNOWN (`None`), never a
/// fabricated constant.
fn tracked_base_rows(scan: &Scan, stats: Option<&TableStatistics>) -> (Option<u64>, Vec<String>) {
    if let Some(rows) = stats
        .and_then(|stats| stats.row_count)
        .and_then(|count| u64::try_from(count).ok())
    {
        return (Some(rows), Vec::new());
    }
    (None, vec![format!("row_count({})", scan_target(scan))])
}

/// A scan with a pushed-down GROUP BY outputs its group count: the product of its
/// keys' NDVs bounded by the (filtered) input rows.
fn tracked_scan_groups(
    scan: &Scan,
    stats: Option<&TableStatistics>,
    rows: Option<u64>,
    mut defaults: Vec<String>,
) -> CardinalityEstimate {
    let mut groups = Some(1u64);
    for key in scan.group_by.as_ref().expect("caller checked group_by") {
        let (ndv, key_defaults) = scan_key_ndv(key, stats, rows, scan);
        groups = mul_groups(groups, ndv);
        defaults.extend(key_defaults);
    }
    CardinalityEstimate::of(min_rows(rows, groups), defaults)
}

/// One pushed group key's NDV from the scan's own statistics, or UNKNOWN with its
/// gap recorded. The scan's own filter caps the NDV exactly.
fn scan_key_ndv(
    key: &Expr,
    stats: Option<&TableStatistics>,
    rows: Option<u64>,
    scan: &Scan,
) -> (Option<u64>, Vec<String>) {
    let gap = || {
        (
            None,
            vec![format!(
                "group_ndv({}.{})",
                scan_target(scan),
                key_name(key)
            )],
        )
    };
    let Expr::Column(column) = key else {
        return gap();
    };
    let Some(num_distinct) = stats
        .and_then(|stats| stats.column_stats.get(&column.column))
        .and_then(|col_stats| col_stats.num_distinct)
        .and_then(|ndv| u64::try_from(ndv).ok())
    else {
        return gap();
    };
    let capped = min_rows(
        Some(num_distinct),
        filter_value_cap(scan.filters.as_ref(), &column.column),
    );
    (min_rows(capped, rows).map(|value| value.max(1)), Vec::new())
}

/// Rows after a filter whose selectivity may be UNKNOWN: an unknown predicate
/// reduces nothing (ceiling 1.0), so the input rows stand as the bound.
fn filtered_rows(rows: u64, selectivity: Option<f64>) -> u64 {
    match selectivity {
        None => rows,
        Some(selectivity) => ((rows as f64 * selectivity) as u64).max(1),
    }
}

/// Clamp a learned/planner measurement to the base table (a filtered scan cannot
/// outgrow it), floored at 1. A negative measurement (a contract violation) reads
/// as the floor.
fn clamp_to_base(measured: i64, base_rows: u64) -> u64 {
    u64::try_from(measured).unwrap_or(0).min(base_rows).max(1)
}

/// How many distinct values a filter allows one column, or `None` (no exact
/// bound). Derived from the predicate itself - a logical fact, no provenance gap.
pub(super) fn filter_value_cap(filters: Option<&Expr>, column: &str) -> Option<u64> {
    let filters = filters?;
    let mut cap = None;
    for conjunct in split_conjuncts(filters) {
        cap = min_rows(cap, conjunct_value_cap(conjunct, column));
    }
    cap
}

/// One conjunct's exact value bound for a column, or `None`. `column = literal`
/// allows ONE; `column IN (v1..vn)` allows at most n.
fn conjunct_value_cap(conjunct: &Expr, column: &str) -> Option<u64> {
    match conjunct {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left,
            right,
        } => {
            if is_column_named(left, column) && matches!(**right, Expr::Literal { .. }) {
                return Some(1);
            }
            if is_column_named(right, column) && matches!(**left, Expr::Literal { .. }) {
                return Some(1);
            }
            None
        }
        Expr::InList { value, options } if is_column_named(value, column) => {
            Some(options.len() as u64)
        }
        _ => None,
    }
}

/// Whether an expression is a column reference with the given name.
fn is_column_named(expr: &Expr, column: &str) -> bool {
    matches!(expr, Expr::Column(reference) if reference.column == column)
}

/// Whether an optional expression slice is present and non-empty.
fn non_empty(items: Option<&[Expr]>) -> bool {
    items.is_some_and(|items| !items.is_empty())
}

/// Whether a scan carries pushed grouping sets.
fn has_grouping_sets(scan: &Scan) -> bool {
    scan.grouping_sets
        .as_ref()
        .is_some_and(|sets| !sets.is_empty())
}
