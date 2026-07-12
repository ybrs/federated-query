//! The provenance-carrying cardinality estimate and its combination helpers.
//! Ports `optimizer/estimate_defaults.py`.
//!
//! A statistic feeding an estimate is either a MEASUREMENT with provenance or it
//! is UNKNOWN (`None`) - never a fabricated constant. `rows: None` PROPAGATES; a
//! missing NDV or an inestimable predicate contributes its honest BOUND (no
//! reduction: a selectivity's ceiling is 1.0, a denominator's floor is 1). Each
//! gap is recorded in `defaults_used`.
//!
//! `orientation_rows` / `larger_estimated_side` (Python priced physical nodes
//! via `getattr` reflection) live with their consumer in
//! `fq_physical::planner::orient`, not here.

use std::collections::HashSet;

/// Relative cost of one row CROSSING A SOURCE BOUNDARY versus one row of
/// coordinator join output (C_out). The join-order enumerator adds
/// `TRANSFER_WEIGHT * rows_shipped` to a candidate; carried here for that
/// consumer (join ordering).
pub const TRANSFER_WEIGHT: f64 = 1.0;

/// A dynamic filter is refused when its expected semi-join selectivity is at
/// least this fraction (such a filter keeps nearly every probe row, so applying
/// it is pure overhead). Consumed by join ordering.
pub const USELESS_KEYS_NDV_FRACTION: f64 = 0.8;

/// A row-count estimate plus the provenance of every statistics gap behind it.
///
/// `rows` is `None` when the estimate is UNKNOWN; unknown propagates to every
/// estimate derived from this one. A non-None `rows` may still rest on gaps
/// named in `defaults_used`, so a decision gate can refuse to trust a gap-fed
/// estimate.
#[derive(Debug, Clone, PartialEq)]
pub struct CardinalityEstimate {
    pub rows: Option<u64>,
    pub defaults_used: Vec<String>,
}

impl CardinalityEstimate {
    /// A known estimate with its provenance.
    pub fn known(rows: u64, defaults_used: Vec<String>) -> Self {
        Self {
            rows: Some(rows),
            defaults_used,
        }
    }

    /// An UNKNOWN estimate carrying the provenance of the gaps that made it so.
    pub fn unknown(defaults_used: Vec<String>) -> Self {
        Self {
            rows: None,
            defaults_used,
        }
    }

    /// An estimate whose rows may be known or unknown, with its provenance.
    pub fn of(rows: Option<u64>, defaults_used: Vec<String>) -> Self {
        Self {
            rows,
            defaults_used,
        }
    }
}

/// Union the provenance of parent estimates plus new defaults, in first-
/// occurrence order and without duplicates, for a derived estimate.
pub fn combine_defaults(parents: &[&CardinalityEstimate], extra: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    let mut seen = HashSet::new();
    for parent in parents {
        extend_unique(&mut merged, &mut seen, &parent.defaults_used);
    }
    extend_unique(&mut merged, &mut seen, extra);
    merged
}

/// Append entries not seen yet, tracking them in the seen set.
fn extend_unique(merged: &mut Vec<String>, seen: &mut HashSet<String>, entries: &[String]) {
    for entry in entries {
        if seen.insert(entry.clone()) {
            merged.push(entry.clone());
        }
    }
}

/// A sum of row counts where UNKNOWN (`None`) propagates: a sum with an
/// unbounded term has no bound.
pub fn add_rows(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        _ => None,
    }
}

/// The smaller of two row counts, IGNORING an UNKNOWN side: a minimum is still
/// bounded by whichever side is known; both unknown is unknown. NOTE this is not
/// `Option::min`, which would drop the known bound when one side is `None`.
pub fn min_rows(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, right) => right,
        (left, None) => left,
        (Some(left), Some(right)) => Some(left.min(right)),
    }
}

/// A group-count product where UNKNOWN (`None`) propagates: a product with an
/// unknown factor has no value.
pub fn mul_groups(groups: Option<u64>, ndv: Option<u64>) -> Option<u64> {
    match (groups, ndv) {
        (Some(groups), Some(ndv)) => Some(groups.saturating_mul(ndv)),
        _ => None,
    }
}

/// The larger of two join-key NDVs, IGNORING an UNKNOWN (`None`) side: the known
/// side's domain is a valid denominator on its own (the FK-containment
/// assumption). Both unknown is unknown.
pub fn max_known_ndv(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (None, right) => right,
        (left, None) => left,
        (Some(left), Some(right)) => Some(left.max(right)),
    }
}

/// Fold one join conjunct into the running estimate terms. An equi key
/// multiplies the NDV denominator and counts toward the composite cap; a
/// non-equi conjunct multiplies the selectivity. A `None` value is an UNKNOWN
/// statistic: it contributes its no-reduction bound (denominator floor 1,
/// selectivity ceiling 1.0), but an unknown equi key still COUNTS (it is a key).
pub fn apply_conjunct_term(
    is_equi: bool,
    value: Option<f64>,
    denom: &mut f64,
    selectivity: &mut f64,
    equi_count: &mut u32,
) {
    if is_equi {
        if let Some(value) = value {
            *denom *= value;
        }
        *equi_count += 1;
        return;
    }
    if let Some(value) = value {
        *selectivity *= value;
    }
}

/// Cap a COMPOSITE key's NDV denominator at the smaller side's rows.
///
/// The denominator is the product of per-column NDVs, which assumes the columns
/// are independent. For a SINGLE key that product is just the key's NDV and is
/// left alone (it can legitimately exceed `min(rows)`). For a MULTI-column key
/// the product over-counts the distinct combinations, which can never exceed the
/// smaller side's rows, so it is capped there.
pub fn cap_composite_denom(denom: f64, equi_count: u32, left_rows: u64, right_rows: u64) -> f64 {
    if equi_count < 2 {
        return denom;
    }
    denom.min((left_rows.min(right_rows).max(1)) as f64)
}

/// True when a planned key reduction provably filters (almost) nothing.
///
/// An unknown build NDV abstains (`false`, reduce-by-default); an unknown probe
/// NDV falls back to the build domain alone (FK containment). Consumed by join
/// ordering.
pub fn useless_key_reduction(
    build_keys_ndv: Option<u64>,
    build_rows: Option<u64>,
    probe_column_ndv: Option<u64>,
) -> bool {
    let Some(build_keys_ndv) = build_keys_ndv else {
        return false;
    };
    // A filtered build cannot donate more distinct keys than it has rows.
    let expected_keys = min_rows(Some(build_keys_ndv), build_rows).unwrap_or(build_keys_ndv);
    let domain = build_keys_ndv.max(probe_column_ndv.unwrap_or(0));
    (expected_keys as f64) >= (domain as f64) * USELESS_KEYS_NDV_FRACTION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_rows_propagates_unknown() {
        assert_eq!(add_rows(Some(3), Some(4)), Some(7));
        assert_eq!(add_rows(Some(3), None), None);
        assert_eq!(add_rows(None, None), None);
    }

    #[test]
    fn min_rows_keeps_the_known_side() {
        // The asymmetry: a None side is IGNORED, not treated as zero/min.
        assert_eq!(min_rows(Some(3), None), Some(3));
        assert_eq!(min_rows(None, Some(4)), Some(4));
        assert_eq!(min_rows(Some(3), Some(4)), Some(3));
        assert_eq!(min_rows(None, None), None);
    }

    #[test]
    fn mul_groups_propagates_unknown() {
        assert_eq!(mul_groups(Some(2), Some(5)), Some(10));
        assert_eq!(mul_groups(Some(2), None), None);
    }

    #[test]
    fn max_known_ndv_ignores_none() {
        assert_eq!(max_known_ndv(Some(3), None), Some(3));
        assert_eq!(max_known_ndv(None, Some(4)), Some(4));
        assert_eq!(max_known_ndv(Some(3), Some(4)), Some(4));
        assert_eq!(max_known_ndv(None, None), None);
    }

    fn close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-9
    }

    #[test]
    fn cap_composite_denom_untouched_for_single_key() {
        // equi_count < 2: a single key's NDV may exceed min(rows); left alone.
        assert!(close(cap_composite_denom(50_000.0, 1, 100, 200), 50_000.0));
    }

    #[test]
    fn cap_composite_denom_caps_multi_column_key() {
        // equi_count >= 2: the independence product is capped at min(rows).
        assert!(close(cap_composite_denom(50_000.0, 2, 100, 200), 100.0));
        // A denom already below min(rows) is left as-is.
        assert!(close(cap_composite_denom(30.0, 2, 100, 200), 30.0));
    }

    #[test]
    fn apply_conjunct_term_all_four_cases() {
        let mut denom = 1.0;
        let mut sel = 1.0;
        let mut equi = 0;
        // equi + known NDV: multiplies denom, counts.
        apply_conjunct_term(true, Some(10.0), &mut denom, &mut sel, &mut equi);
        assert!(close(denom, 10.0) && close(sel, 1.0) && equi == 1);
        // equi + unknown: counts, denom unchanged (floor 1).
        apply_conjunct_term(true, None, &mut denom, &mut sel, &mut equi);
        assert!(close(denom, 10.0) && close(sel, 1.0) && equi == 2);
        // non-equi + known selectivity: multiplies selectivity, no count.
        apply_conjunct_term(false, Some(0.5), &mut denom, &mut sel, &mut equi);
        assert!(close(denom, 10.0) && close(sel, 0.5) && equi == 2);
        // non-equi + unknown: no change (ceiling 1.0).
        apply_conjunct_term(false, None, &mut denom, &mut sel, &mut equi);
        assert!(close(denom, 10.0) && close(sel, 0.5) && equi == 2);
    }

    #[test]
    fn useless_key_reduction_abstains_and_fires() {
        // Unknown build NDV abstains (reduce-by-default).
        assert!(!useless_key_reduction(None, Some(100), Some(100)));
        // Build NDV covers >= 80% of the probe domain -> useless.
        assert!(useless_key_reduction(Some(90), Some(100), Some(100)));
        // Build NDV is a small fraction of the probe domain -> reduces.
        assert!(!useless_key_reduction(Some(10), Some(100), Some(100)));
    }

    #[test]
    fn combine_defaults_first_occurrence_dedup() {
        let a = CardinalityEstimate::known(1, vec!["x".into(), "y".into()]);
        let b = CardinalityEstimate::known(1, vec!["y".into(), "z".into()]);
        let merged = combine_defaults(&[&a, &b], &["x".into(), "w".into()]);
        assert_eq!(merged, vec!["x", "y", "z", "w"]);
    }
}
