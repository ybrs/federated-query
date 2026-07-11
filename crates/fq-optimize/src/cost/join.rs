//! Join estimation: the NDV cardinality formula, the composite-key cap, the
//! type clamps, and the SEMI/ANTI occupancy. Ports the `_estimate_join_tracked`
//! family of `optimizer/cost.py`.

use fq_plan::expr::{split_conjuncts, BinaryOpType, ColumnRef, Expr};
use fq_plan::logical::{Join, JoinType};

use super::selectivity::tracked_selectivity;
use super::{ndv_u64, scan_target, CostModel};
use crate::error::{EstimateError, Result};
use crate::estimate_defaults::{
    apply_conjunct_term, cap_composite_denom, combine_defaults, max_known_ndv, min_rows,
    CardinalityEstimate,
};

impl CostModel {
    /// Join estimate: the NDV formula over equi keys, clamped by type. A join over
    /// an UNKNOWN side is unknown (a product with an unbounded factor has no
    /// bound); a conditionless / CROSS join is the full cross product.
    pub(super) fn estimate_join(&mut self, join: &Join) -> Result<CardinalityEstimate> {
        let left = self.estimate(&join.left)?;
        let right = self.estimate(&join.right)?;
        let (Some(left_rows), Some(right_rows)) = (left.rows, right.rows) else {
            return Ok(CardinalityEstimate::unknown(combine_defaults(
                &[&left, &right],
                &[],
            )));
        };
        if join.condition.is_none() || join.join_type == JoinType::Cross {
            return Ok(CardinalityEstimate::known(
                left_rows.saturating_mul(right_rows),
                combine_defaults(&[&left, &right], &[]),
            ));
        }
        let (inner, defaults) = self.tracked_inner_rows(join, left_rows, right_rows)?;
        let rows = clamp_join_rows(join.join_type, inner, left_rows, right_rows);
        Ok(CardinalityEstimate::known(
            rows,
            combine_defaults(&[&left, &right], &defaults),
        ))
    }

    /// Inner-join rows: cross product / capped key NDV x non-equi selectivity. The
    /// equi-key denominator (product of per-column NDVs) assumes independence and
    /// over-counts a composite key, so it is capped at the smaller side's rows.
    fn tracked_inner_rows(
        &self,
        join: &Join,
        left_rows: u64,
        right_rows: u64,
    ) -> Result<(u64, Vec<String>)> {
        let (denom, selectivity, equi_count, defaults) =
            self.conjunct_terms(join, left_rows, right_rows)?;
        let denom = cap_composite_denom(denom, equi_count, left_rows, right_rows);
        let rows = (left_rows as f64) * (right_rows as f64) * selectivity / denom;
        Ok(((rows as u64).max(1), defaults))
    }

    /// Accumulate the equi-key NDV denominator (product), the non-equi
    /// selectivity, and the equi-conjunct COUNT across the join's conjuncts.
    fn conjunct_terms(
        &self,
        join: &Join,
        left_rows: u64,
        right_rows: u64,
    ) -> Result<(f64, f64, u32, Vec<String>)> {
        let condition = join.condition.as_ref().expect("caller checked condition");
        let mut denom = 1.0;
        let mut selectivity = 1.0;
        let mut equi_count = 0;
        let mut defaults = Vec::new();
        for conjunct in split_conjuncts(condition) {
            let (is_equi, value, conjunct_defaults) =
                self.conjunct_term(join, conjunct, left_rows, right_rows)?;
            apply_conjunct_term(
                is_equi,
                value,
                &mut denom,
                &mut selectivity,
                &mut equi_count,
            );
            defaults.extend(conjunct_defaults);
        }
        Ok((denom, selectivity, equi_count, defaults))
    }

    /// (is_equi, value, defaults): for a cross-side equi key the value is its NDV
    /// (a denominator term); for anything else it is the tracked selectivity (a
    /// multiplier). Either value may be `None` (UNKNOWN).
    fn conjunct_term(
        &self,
        join: &Join,
        conjunct: &Expr,
        left_rows: u64,
        right_rows: u64,
    ) -> Result<(bool, Option<f64>, Vec<String>)> {
        let Some((left_ref, right_ref)) = self.equi_key_pair(join, conjunct)? else {
            let (selectivity, defaults) = tracked_selectivity(conjunct, None, "join");
            return Ok((false, selectivity, defaults));
        };
        let (left_ndv, left_defaults) = self.side_key_ndv(&join.left, left_ref, left_rows)?;
        let (right_ndv, mut defaults) = self.side_key_ndv(&join.right, right_ref, right_rows)?;
        let mut merged = left_defaults;
        merged.append(&mut defaults);
        let value = max_known_ndv(left_ndv, right_ndv).map(|ndv| ndv as f64);
        Ok((true, value, merged))
    }

    /// (left_ref, right_ref) for a column=column equality ORIENTED to the join's
    /// sides; `None` for anything else.
    fn equi_key_pair<'a>(
        &self,
        join: &Join,
        conjunct: &'a Expr,
    ) -> Result<Option<(&'a ColumnRef, &'a ColumnRef)>> {
        let Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left,
            right,
        } = conjunct
        else {
            return Ok(None);
        };
        let (Expr::Column(left), Expr::Column(right)) = (&**left, &**right) else {
            return Ok(None);
        };
        self.orient_pair(join, left, right)
    }

    /// Assign two column refs to the join's sides by their qualifiers.
    fn orient_pair<'a>(
        &self,
        join: &Join,
        first: &'a ColumnRef,
        second: &'a ColumnRef,
    ) -> Result<Option<(&'a ColumnRef, &'a ColumnRef)>> {
        require_qualified(first)?;
        require_qualified(second)?;
        if self.resolves_in(&join.left, first)? && self.resolves_in(&join.right, second)? {
            return Ok(Some((first, second)));
        }
        if self.resolves_in(&join.left, second)? && self.resolves_in(&join.right, first)? {
            return Ok(Some((second, first)));
        }
        Ok(None)
    }

    /// Whether the ref's qualifier names a relation inside this subtree.
    fn resolves_in(
        &self,
        node: &fq_plan::logical::LogicalPlan,
        reference: &ColumnRef,
    ) -> Result<bool> {
        let qualifier = reference.table.as_deref().unwrap_or_default();
        Ok(self.find_relation(node, qualifier)?.is_some())
    }

    /// A join key's NDV on its side, clamped to the side's row estimate (a
    /// filtered side cannot hold more distinct keys than rows). An unknown NDV
    /// stays UNKNOWN with its gap recorded.
    fn side_key_ndv(
        &self,
        side: &fq_plan::logical::LogicalPlan,
        reference: &ColumnRef,
        side_rows: u64,
    ) -> Result<(Option<u64>, Vec<String>)> {
        let qualifier = reference.table.as_deref().unwrap_or_default();
        let owner = self.find_relation(side, qualifier)?.ok_or_else(|| {
            EstimateError::NoOwningRelation {
                relation: qualifier.to_string(),
                column: reference.column.clone(),
            }
        })?;
        match ndv_u64(self.owner_column_ndv(owner, &reference.column)?) {
            None => Ok((
                None,
                vec![format!("ndv({})", owner_target(owner, reference))],
            )),
            Some(ndv) => Ok((
                min_rows(Some(ndv), Some(side_rows)).map(|value| value.max(1)),
                Vec::new(),
            )),
        }
    }
}

/// An unqualified column in a join condition is an upstream bug: every
/// post-binder reference must carry its relation qualifier.
fn require_qualified(reference: &ColumnRef) -> Result<()> {
    if reference.table.as_deref().unwrap_or_default().is_empty() {
        return Err(EstimateError::UnqualifiedInCondition(
            reference.column.clone(),
        ));
    }
    Ok(())
}

/// The provenance name of a join key's owning relation and column.
fn owner_target(owner: &fq_plan::logical::LogicalPlan, reference: &ColumnRef) -> String {
    match owner {
        fq_plan::logical::LogicalPlan::Scan(scan) => {
            format!("{}.{}", scan_target(scan), reference.column)
        }
        _ => format!(
            "{}.{}",
            reference.table.as_deref().unwrap_or_default(),
            reference.column
        ),
    }
}

/// Join-type bounds over the inner-join estimate. EXHAUSTIVE over `JoinType`.
fn clamp_join_rows(join_type: JoinType, inner: u64, left_rows: u64, right_rows: u64) -> u64 {
    match join_type {
        // Cross is priced by the conditionless branch; a conditioned CROSS
        // reaching here prices as its inner-join estimate, like Inner.
        JoinType::Inner | JoinType::Cross => inner,
        JoinType::Left => left_rows.max(inner),
        JoinType::Right => right_rows.max(inner),
        JoinType::Full => left_rows
            .saturating_add(right_rows)
            .saturating_sub(inner)
            .max(left_rows)
            .max(right_rows),
        JoinType::Semi => semi_matched_rows(inner, left_rows).max(1),
        JoinType::Anti => left_rows
            .saturating_sub(semi_matched_rows(inner, left_rows))
            .max(1),
    }
}

/// Expected DISTINCT left rows with at least one match, for a semi/anti join. The
/// occupancy estimate `left * (1 - e^-fanout)` never saturates to `left` (which
/// `min(left, inner)` would, spuriously emptying an ANTI join).
fn semi_matched_rows(inner: u64, left_rows: u64) -> u64 {
    if left_rows == 0 {
        return 0;
    }
    let fanout = inner as f64 / left_rows as f64;
    (left_rows as f64 * (1.0 - (-fanout).exp())) as u64
}
