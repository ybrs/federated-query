//! Aggregate estimation: the global-aggregate single row, the learned group
//! count, and the group-key NDV product. Ports the `_estimate_aggregate_tracked`
//! family of `optimizer/cost.py`.

use fq_plan::expr::Expr;
use fq_plan::logical::{Aggregate, LogicalPlan};

use super::scan::filter_value_cap;
use super::{key_name, ndv_u64, CostModel};
use crate::error::Result;
use crate::estimate_defaults::{combine_defaults, min_rows, mul_groups, CardinalityEstimate};
use crate::subplan_signature::{group_column_names, subplan_signature};

impl CostModel {
    /// Group count = product of the group keys' NDVs, clamped by input. A global
    /// aggregate is exactly one row; a MEASURED group count (exact) replaces the
    /// NDV product and stands even over an unknown input.
    pub(super) fn estimate_aggregate(&mut self, agg: &Aggregate) -> Result<CardinalityEstimate> {
        let input = self.estimate(&agg.input)?;
        if agg.group_by.is_empty() {
            return Ok(CardinalityEstimate::of(Some(1), input.defaults_used));
        }
        if let Some(learned) = self.learned_group_count(agg)? {
            let learned = u64::try_from(learned).unwrap_or(0);
            return Ok(CardinalityEstimate::of(
                min_rows(input.rows, Some(learned)),
                input.defaults_used,
            ));
        }
        let (groups, defaults) = self.tracked_group_count(&agg.group_by, &agg.input, input.rows)?;
        Ok(CardinalityEstimate::of(
            min_rows(input.rows, groups),
            combine_defaults(&[&input], &defaults),
        ))
    }

    /// The catalog's MEASURED group count for a plain GROUP BY, or `None`. The
    /// subject matches the write side: a single UNFILTERED base table by its name,
    /// anything else by its input's SUBPLAN SIGNATURE.
    fn learned_group_count(&self, agg: &Aggregate) -> Result<Option<i64>> {
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        let Some(catalog) = stats.stats_catalog() else {
            return Ok(None);
        };
        let Some(columns) = group_column_names(&agg.group_by) else {
            return Ok(None);
        };
        let subject = group_subject(&agg.input);
        Ok(catalog.group_count(&subject, &columns, stats.learned_ttl_seconds())?)
    }

    /// The product of the group keys' NDVs over the input subtree; a key with no
    /// NDV makes the product UNKNOWN.
    fn tracked_group_count(
        &self,
        keys: &[Expr],
        input: &LogicalPlan,
        input_rows: Option<u64>,
    ) -> Result<(Option<u64>, Vec<String>)> {
        let mut groups = Some(1u64);
        let mut defaults = Vec::new();
        for key in keys {
            let (ndv, key_defaults) = self.group_key_ndv(key, input, input_rows)?;
            groups = mul_groups(groups, ndv);
            defaults.extend(key_defaults);
        }
        Ok((groups, defaults))
    }

    /// One group key's NDV, or UNKNOWN with its gap recorded. The owner's own
    /// FILTER caps the NDV exactly (after `d_year IN (2001, 2002)` the column holds
    /// at most TWO values).
    fn group_key_ndv(
        &self,
        key: &Expr,
        input: &LogicalPlan,
        input_rows: Option<u64>,
    ) -> Result<(Option<u64>, Vec<String>)> {
        if let Expr::Column(column) = key {
            if let Some(qualifier) = column.table.as_deref() {
                if let Some(owner) = self.find_relation(input, qualifier)? {
                    let ndv = ndv_u64(self.owner_column_ndv(owner, &column.column)?);
                    let ndv = min_rows(ndv, owner_filter_value_cap(owner, &column.column));
                    if let Some(ndv) = ndv {
                        let bounded = min_rows(Some(ndv), input_rows).map(|value| value.max(1));
                        return Ok((bounded, Vec::new()));
                    }
                }
            }
        }
        Ok((None, vec![format!("group_ndv({})", key_name(key))]))
    }
}

/// The catalog subject for an aggregate's input: a single UNFILTERED base table's
/// name, else the input's subplan signature.
fn group_subject(input: &LogicalPlan) -> String {
    if let LogicalPlan::Scan(scan) = input {
        if scan.filters.is_none() {
            return format!(
                "{}.{}.{}",
                scan.datasource, scan.schema_name, scan.table_name
            );
        }
    }
    subplan_signature(input)
}

/// The value cap a SCAN owner's pushed filter puts on one column, or `None` when
/// the owner is not a filtered scan.
fn owner_filter_value_cap(owner: &LogicalPlan, column: &str) -> Option<u64> {
    match owner {
        LogicalPlan::Scan(scan) => filter_value_cap(scan.filters.as_ref(), column),
        _ => None,
    }
}
