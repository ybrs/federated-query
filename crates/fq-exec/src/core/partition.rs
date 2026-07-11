// Imported fedqrs engine core - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! Scan-partitioning and selectivity math for the read strategies.
//!
//! Pure functions (no IO): the connector feeds them page counts and stats and
//! acts on the result. Kept here so they can be unit-tested without a database.

use arrow::array::{Array, Float64Array, RecordBatch};

/// Split `[0, pages)` into `partitions` half-open ctid page ranges. The last
/// range extends to the max page so rows added since the last ANALYZE are still
/// covered. A zero page count (never analyzed) yields a single full-table range.
pub fn ctid_ranges(pages: u32, partitions: usize) -> Vec<(u32, u32)> {
    if pages == 0 {
        return vec![(0, u32::MAX)];
    }
    let partitions = (partitions.max(1) as u32).min(pages);
    let chunk = (pages / partitions).max(1);
    let mut ranges = Vec::new();
    let mut lo = 0u32;
    while lo < pages {
        let hi = lo.saturating_add(chunk);
        ranges.push((lo, hi));
        lo = hi;
    }
    if let Some(last) = ranges.last_mut() {
        last.1 = u32::MAX;
    }
    ranges
}

/// Estimate the fraction of a table `num_keys` distinct filter values select,
/// from a two-column result of `reltuples::float8, n_distinct::float8`. Returns
/// None when stats are missing/degenerate (caller then avoids the full-scan path).
pub fn selectivity_from_stats(batches: &[RecordBatch], num_keys: usize) -> Option<f64> {
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let reltuples = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()?
        .value(0);
    let ndist_col = batch.column(1).as_any().downcast_ref::<Float64Array>()?;
    if ndist_col.is_null(0) {
        return None;
    }
    let n_distinct = ndist_col.value(0);
    // n_distinct: > 0 is an absolute count; < 0 is the negative fraction of rows.
    let distinct = if n_distinct > 0.0 {
        n_distinct
    } else if n_distinct < 0.0 && reltuples > 0.0 {
        -n_distinct * reltuples
    } else {
        return None;
    };
    if distinct <= 0.0 {
        return None;
    }
    Some((num_keys as f64 / distinct).min(1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn ranges_cover_and_do_not_overlap() {
        let ranges = ctid_ranges(1000, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].0, 0);
        // half-open and contiguous: each hi is the next lo
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0);
        }
        // last extends to the max page
        assert_eq!(ranges.last().unwrap().1, u32::MAX);
    }

    #[test]
    fn unanalyzed_table_is_one_range() {
        assert_eq!(ctid_ranges(0, 8), vec![(0, u32::MAX)]);
    }

    #[test]
    fn partitions_capped_at_page_count() {
        // 3 pages cannot be split into 8 ranges
        assert_eq!(ctid_ranges(3, 8).len(), 3);
    }

    fn stats(reltuples: f64, n_distinct: Option<f64>) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("reltuples", DataType::Float64, false),
            Field::new("n_distinct", DataType::Float64, true),
        ]));
        let rel = Float64Array::from(vec![reltuples]);
        let nd = Float64Array::from(vec![n_distinct]);
        vec![RecordBatch::try_new(schema, vec![Arc::new(rel), Arc::new(nd)]).unwrap()]
    }

    #[test]
    fn selectivity_absolute_ndistinct() {
        // n_distinct = 100000 absolute; 10000 keys -> 10%
        let f = selectivity_from_stats(&stats(3_000_000.0, Some(100_000.0)), 10_000).unwrap();
        assert!((f - 0.1).abs() < 1e-9);
    }

    #[test]
    fn selectivity_negative_ndistinct_is_row_fraction() {
        // n_distinct = -0.2 => distinct = 0.2 * 600000 = 120000; 72000 keys -> 0.6
        let f = selectivity_from_stats(&stats(600_000.0, Some(-0.2)), 72_000).unwrap();
        assert!((f - 0.6).abs() < 1e-6);
    }

    #[test]
    fn selectivity_capped_at_one() {
        let f = selectivity_from_stats(&stats(1000.0, Some(10.0)), 1_000_000).unwrap();
        assert_eq!(f, 1.0);
    }

    #[test]
    fn selectivity_none_without_stats() {
        assert!(selectivity_from_stats(&stats(1000.0, None), 100).is_none());
    }
}
