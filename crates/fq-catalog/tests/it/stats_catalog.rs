//! Translation of the direct-StatsCatalog half of `tests/test_stats_catalog.py`:
//! upsert self-healing, TTL reads, roundtrips, the freshness contract.
//!
//! The other tests in that file exercise OTHER crates (see the stats_catalog.rs
//! port note): `_persist_observations` / `_plain_group_columns`
//! (fq-physical/fq-runtime) and the CostModel/StatisticsCollector overlay
//! (fq-optimize). They translate with those crates.
//!
//! `test_unknown_field_raises` retires: the upsert field is a compile-time enum,
//! so an invalid column is unrepresentable (see the port note).

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, TimeZone, Utc};
use fq_catalog::{Clock, StatsCatalog};

/// A unique throwaway on-disk catalog path (removed by `TempCatalog` on drop).
struct TempPath {
    path: String,
}

impl TempPath {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir()
            .join(format!("fq_stats_test_{tag}.sqlite"))
            .to_string_lossy()
            .into_owned();
        // Start clean: an earlier run's file would carry stale rows.
        let _ = std::fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        // WAL leaves -wal/-shm siblings; remove all three.
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path));
        let _ = std::fs::remove_file(format!("{}-shm", self.path));
    }
}

/// A controllable clock returning RFC3339 stamps, so tests set freshness without
/// sleeping (the Python `_Clock`).
struct TestClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl TestClock {
    fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(start)),
        }
    }

    fn clock(&self) -> Clock {
        let now = Arc::clone(&self.now);
        Arc::new(move || now.lock().unwrap().to_rfc3339())
    }

    fn advance(&self, seconds: i64) {
        let mut guard = self.now.lock().unwrap();
        *guard += Duration::seconds(seconds);
    }
}

fn cols(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

#[test]
fn test_table_rows_roundtrip() {
    let temp = TempPath::new("table_rows_roundtrip");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    catalog
        .record_table_rows("pg", "public", "store_sales", 28_800_991)
        .unwrap();
    assert_eq!(
        catalog
            .table_rows("pg", "public", "store_sales", None)
            .unwrap(),
        Some(28_800_991)
    );
}

#[test]
fn test_absent_reads_return_none() {
    let temp = TempPath::new("absent_reads");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    assert_eq!(
        catalog.table_rows("pg", "public", "nope", None).unwrap(),
        None
    );
    assert_eq!(
        catalog
            .column_ndv("pg", "public", "nope", "c", None)
            .unwrap(),
        None
    );
    assert_eq!(
        catalog
            .predicate_selectivity("pg", "public", "nope", "x = $1", "", None)
            .unwrap(),
        None
    );
}

#[test]
fn test_column_ndv_roundtrip() {
    let temp = TempPath::new("column_ndv_roundtrip");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    catalog
        .record_table_rows("pg", "public", "warehouse", 10)
        .unwrap();
    catalog
        .record_column_ndv("pg", "public", "warehouse", "w_warehouse_sk", 10)
        .unwrap();
    assert_eq!(
        catalog
            .table_rows("pg", "public", "warehouse", None)
            .unwrap(),
        Some(10)
    );
    assert_eq!(
        catalog
            .column_ndv("pg", "public", "warehouse", "w_warehouse_sk", None)
            .unwrap(),
        Some(10)
    );
}

#[test]
fn test_upsert_self_heals() {
    let temp = TempPath::new("upsert_self_heals");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    catalog
        .record_table_rows("duck", "main", "catalog_sales", 100)
        .unwrap();
    catalog
        .record_table_rows("duck", "main", "catalog_sales", 14_401_261)
        .unwrap();
    assert_eq!(
        catalog
            .table_rows("duck", "main", "catalog_sales", None)
            .unwrap(),
        Some(14_401_261)
    );
    // Upsert bumped observation_count rather than inserting a duplicate.
    assert_eq!(
        catalog
            .table_observation_count("duck", "main", "catalog_sales")
            .unwrap(),
        Some(2)
    );
}

#[test]
fn test_predicate_selectivity_roundtrip() {
    let temp = TempPath::new("predicate_selectivity");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    catalog
        .record_predicate(
            "pg",
            "public",
            "date_dim",
            "d_month_seq BETWEEN $1 AND $2",
            Some(73049),
            366,
            "",
        )
        .unwrap();
    let selectivity = catalog
        .predicate_selectivity(
            "pg",
            "public",
            "date_dim",
            "d_month_seq BETWEEN $1 AND $2",
            "",
            None,
        )
        .unwrap()
        .expect("recorded");
    assert!((selectivity - 366.0 / 73049.0).abs() < 1e-12);
}

#[test]
fn test_ttl_expires_stale_reads() {
    let temp = TempPath::new("ttl_expires");
    let clock = TestClock::new(Utc.with_ymd_and_hms(2026, 7, 9, 0, 0, 0).unwrap());
    let catalog = StatsCatalog::open_with_clock(&temp.path, clock.clock()).unwrap();
    catalog
        .record_table_rows("pg", "public", "store_sales", 28_800_991)
        .unwrap();

    clock.advance(30);
    assert_eq!(
        catalog
            .table_rows("pg", "public", "store_sales", Some(60))
            .unwrap(),
        Some(28_800_991)
    );

    clock.advance(120);
    assert_eq!(
        catalog
            .table_rows("pg", "public", "store_sales", Some(60))
            .unwrap(),
        None
    );

    // No TTL: the value is always served, however old.
    assert_eq!(
        catalog
            .table_rows("pg", "public", "store_sales", None)
            .unwrap(),
        Some(28_800_991)
    );
}

#[test]
fn test_persists_across_reopen() {
    let temp = TempPath::new("persists_reopen");
    {
        let catalog = StatsCatalog::open(&temp.path).unwrap();
        catalog
            .record_table_rows("pg", "public", "item", 102_000)
            .unwrap();
    }
    let reopened = StatsCatalog::open(&temp.path).unwrap();
    assert_eq!(
        reopened.table_rows("pg", "public", "item", None).unwrap(),
        Some(102_000)
    );
}

#[test]
fn test_group_count_roundtrip_order_independent() {
    let temp = TempPath::new("group_count");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    catalog
        .record_group(
            "duck.main.store_sales",
            &cols(&["i_item_sk", "d_date"]),
            13_800_000,
            None,
        )
        .unwrap();
    // Read with the columns in the opposite order -> same key.
    assert_eq!(
        catalog
            .group_count(
                "duck.main.store_sales",
                &cols(&["d_date", "i_item_sk"]),
                None
            )
            .unwrap(),
        Some(13_800_000)
    );
    // A different key set is absent.
    assert_eq!(
        catalog
            .group_count("duck.main.store_sales", &cols(&["i_item_sk"]), None)
            .unwrap(),
        None
    );
}

#[test]
fn test_predicate_output_rows_roundtrip() {
    let temp = TempPath::new("predicate_output_rows");
    let catalog = StatsCatalog::open(&temp.path).unwrap();
    // input_rows unknown (None): the measured output is still recorded directly.
    catalog
        .record_predicate(
            "pg",
            "public",
            "part",
            "LIKE(pg.public.part.p_name)",
            None,
            10664,
            "",
        )
        .unwrap();
    assert_eq!(
        catalog
            .predicate_output_rows(
                "pg",
                "public",
                "part",
                "LIKE(pg.public.part.p_name)",
                "",
                None
            )
            .unwrap(),
        Some(10664)
    );
    // The selectivity ratio is honestly absent when the base was unknown.
    assert_eq!(
        catalog
            .predicate_selectivity(
                "pg",
                "public",
                "part",
                "LIKE(pg.public.part.p_name)",
                "",
                None
            )
            .unwrap(),
        None
    );
}

#[test]
fn test_group_key_matches_python_json_dumps() {
    // Byte-compat guard: the key format must match Python json.dumps(sorted(...)),
    // including the ", " element separator, or a shared catalog would miss.
    assert_eq!(
        fq_catalog::group_key(&cols(&["i_item_sk", "d_date"])),
        "[\"d_date\", \"i_item_sk\"]"
    );
}
