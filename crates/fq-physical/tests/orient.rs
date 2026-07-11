//! Orientation-size unit tests. Translate the physical-planner-owned cases of
//! `tests/test_reduction_gate.py` (build-side + orientation), which exercise
//! `orientation_rows` / `larger_estimated_side` over hand-built physical nodes.

use std::collections::BTreeMap;

use fq_physical::planner::orient::{larger_estimated_side, orientation_rows, Side};
use fq_plan::physical::{PhysicalPlan, PhysicalRemoteQuery, PhysicalScan};

/// A plain injectable scan carrying a cost estimate.
fn scan(table: &str, alias: &str, rows: Option<u64>) -> PhysicalPlan {
    PhysicalPlan::Scan(Box::new(PhysicalScan {
        datasource: "duck".to_string(),
        schema_name: "main".to_string(),
        table_name: table.to_string(),
        columns: vec!["k".to_string()],
        filters: None,
        sample: None,
        group_by: None,
        grouping_sets: None,
        aggregates: None,
        output_names: None,
        alias: Some(alias.to_string()),
        limit: None,
        offset: 0,
        order_by_keys: None,
        order_by_ascending: None,
        order_by_nulls: None,
        distinct: false,
        dynamic_filter_keys: None,
        estimated_rows: rows,
        column_ndv: None,
        seeded_schema: None,
    }))
}

/// A collapsed remote island with a max-base floor (`estimated_rows`) and an
/// optional real output estimate (`output_estimated_rows`).
fn island(output_est: Option<u64>, est: Option<u64>) -> PhysicalPlan {
    PhysicalPlan::RemoteQuery(Box::new(PhysicalRemoteQuery {
        datasource: "duck".to_string(),
        sql: "SELECT 1".to_string(),
        output_names: vec!["k".to_string()],
        column_alias_map: BTreeMap::new(),
        estimated_rows: est,
        output_estimated_rows: output_est,
        column_ndv: None,
        seeded_schema: None,
        group_observation: None,
    }))
}

#[test]
fn scan_orientation_size_is_its_estimate() {
    assert_eq!(orientation_rows(&scan("a", "a", Some(1000))), Some(1000));
    assert_eq!(orientation_rows(&scan("a", "a", None)), None);
}

#[test]
fn remote_orientation_prefers_output_estimate_over_floor() {
    // A real output estimate wins over the max-base floor.
    assert_eq!(
        orientation_rows(&island(Some(400), Some(6_000_000))),
        Some(400)
    );
    // Absent output estimate falls back to the floor (the max-base bound).
    assert_eq!(
        orientation_rows(&island(None, Some(6_000_000))),
        Some(6_000_000)
    );
    assert_eq!(orientation_rows(&island(None, None)), None);
}

#[test]
fn larger_side_uses_the_remote_output_estimate() {
    // The q11-style case: a 150k scan vs an island whose OUTPUT is only 400 rows
    // -> the scan is the larger side.
    let customer = scan("customer", "c", Some(150_000));
    let small_island = island(Some(400), Some(6_000_000));
    assert_eq!(
        larger_estimated_side(&customer, &small_island),
        Some(Side::Left)
    );

    // With no output estimate the island sizes by its 6M floor -> island larger.
    let customer = scan("customer", "c", Some(150_000));
    let unestimated_island = island(None, Some(6_000_000));
    assert_eq!(
        larger_estimated_side(&customer, &unestimated_island),
        Some(Side::Right)
    );
}

#[test]
fn larger_side_abstains_on_tie_or_unknown() {
    // Equal estimates (both defaulted) must not pick a side by cardinality.
    let a = scan("a", "a", Some(1000));
    let b = scan("b", "b", Some(1000));
    assert_eq!(larger_estimated_side(&a, &b), None);
    // One side unknown -> abstain.
    let a = scan("a", "a", Some(1000));
    let b = scan("b", "b", None);
    assert_eq!(larger_estimated_side(&a, &b), None);
}
