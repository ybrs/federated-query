//! A config-driven `Runtime` runs several real
//! queries against a bundled DuckDB fixture IN ONE SESSION and returns correct
//! Arrow rows under the user-visible SELECT column names.
//!
//! Running multiple queries through one `Runtime` proves the DuckDB file-lock is
//! handled: the catalog's read-only metadata/stats handle and the exec-plane
//! read-only handle coexist on the same file for the whole session.
//!
//! Gated: the whole test early-returns if the fixture is absent (mirrors the
//! fq-connectors DB-backed tests), so a checkout without the benchmark data
//! still passes `cargo test`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The region + nation subset the assertions read: TPC-H's 5 regions and 25
/// nations (5 per region), with the exact names and keys the queries expect
/// (region 0 = AFRICA holds ALGERIA/ETHIOPIA/KENYA/MOROCCO/MOZAMBIQUE; the max
/// nationkey is 24). Only the three columns the queries touch are seeded.
const SEED_SQL: &str = "\
    CREATE TABLE region (r_regionkey INTEGER, r_name VARCHAR); \
    INSERT INTO region VALUES \
        (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST'); \
    CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER); \
    INSERT INTO nation VALUES \
        (0,'ALGERIA',0),(1,'ARGENTINA',1),(2,'BRAZIL',1),(3,'CANADA',1),(4,'EGYPT',4), \
        (5,'ETHIOPIA',0),(6,'FRANCE',3),(7,'GERMANY',3),(8,'INDIA',2),(9,'INDONESIA',2), \
        (10,'IRAN',4),(11,'IRAQ',4),(12,'JAPAN',2),(13,'JORDAN',4),(14,'KENYA',0), \
        (15,'MOROCCO',0),(16,'MOZAMBIQUE',0),(17,'PERU',1),(18,'CHINA',2),(19,'ROMANIA',3), \
        (20,'SAUDI ARABIA',4),(21,'VIETNAM',2),(22,'RUSSIA',3),(23,'UNITED KINGDOM',3), \
        (24,'UNITED STATES',1);";

/// Seed a fresh DuckDB file with the region/nation subset and return its path.
/// DuckDB opens a file read-write (single writer per file per process), so every
/// caller builds its OWN file at a unique temp path: parallel test binaries never
/// contend on a shared file, and no checked-in database is ever opened.
fn seed_fixture() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "fq_seed_region_nation_{}_{}.duckdb",
        std::process::id(),
        id
    ));
    // A leftover file from a crashed prior run would make CREATE TABLE fail; start
    // from a clean path.
    let _ = std::fs::remove_file(&path);
    let source =
        DuckDbSource::open("seed", path.to_str().expect("temp path is valid UTF-8")).expect("open");
    source.execute_batch(SEED_SQL).expect("seed region/nation");
    drop(source);
    path
}

/// Build a single-DuckDB-source `Config` in code, pointing at `path` under the
/// datasource name `duck` (the name the `duck.main.*` table references resolve).
fn duck_config(path: &str) -> Config {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "duck".to_string(),
        DataSourceConfig {
            name: "duck".to_string(),
            ty: "duckdb".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        server: ServerConfig::default(),
        accelerator: fq_common::AcceleratorConfig::default(),
        catalog: fq_common::CatalogConfig::default(),
        events: fq_common::EventsConfig::default(),
        source_path: None,
    }
}

/// The user-visible column names of a result schema, in order.
fn column_names(schema: &SchemaRef) -> Vec<String> {
    let mut names = Vec::new();
    for field in schema.fields() {
        names.push(field.name().clone());
    }
    names
}

/// Flatten `(Int32, Utf8)` result rows in batch order.
fn int_string_rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = downcast_i32(batch, 0);
        let names = downcast_str(batch, 1);
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), names.value(row).to_string()));
        }
    }
    rows
}

/// Flatten `(Int32, Int64)` result rows in batch order.
fn int_count_rows(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = downcast_i32(batch, 0);
        let counts = downcast_i64(batch, 1);
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), counts.value(row)));
        }
    }
    rows
}

/// Flatten a single Utf8 column's values in batch order.
fn string_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let names = downcast_str(batch, 0);
        for row in 0..batch.num_rows() {
            rows.push(names.value(row).to_string());
        }
    }
    rows
}

/// Downcast one column to `Int32Array` (panics loudly on a type mismatch - a
/// wrong result type is a bug, not a soft skip).
fn downcast_i32(batch: &RecordBatch, index: usize) -> &Int32Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("column is Int32")
}

/// Downcast one column to `Int64Array`.
fn downcast_i64(batch: &RecordBatch, index: usize) -> &Int64Array {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column is Int64")
}

/// Downcast one column to `StringArray`.
fn downcast_str(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is Utf8")
}

#[test]
fn runs_several_queries_in_one_session_on_real_duckdb() {
    let path = seed_fixture();
    let path = path.to_str().expect("fixture path is valid UTF-8");
    let runtime = Runtime::from_config(&duck_config(path)).expect("from_config");

    // 1. Projection + filter + ORDER BY + LIMIT: the three lowest regions.
    let (schema, batches) = runtime
        .execute(
            "SELECT r_regionkey, r_name FROM duck.main.region \
             WHERE r_regionkey < 4 ORDER BY r_regionkey LIMIT 3",
        )
        .expect("projection query");
    assert_eq!(column_names(&schema), vec!["r_regionkey", "r_name"]);
    assert_eq!(
        int_string_rows(&batches),
        vec![
            (0, "AFRICA".to_string()),
            (1, "AMERICA".to_string()),
            (2, "ASIA".to_string()),
        ]
    );

    // 2. GROUP BY with an aggregate alias: five regions, five nations each.
    let (schema, batches) = runtime
        .execute(
            "SELECT n_regionkey, count(*) AS c FROM duck.main.nation \
             GROUP BY n_regionkey ORDER BY n_regionkey",
        )
        .expect("group-by query");
    assert_eq!(column_names(&schema), vec!["n_regionkey", "c"]);
    assert_eq!(
        int_count_rows(&batches),
        vec![(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)]
    );

    // 3. Two-table JOIN (nation JOIN region), filtered to AFRICA.
    let (schema, batches) = runtime
        .execute(
            "SELECT n.n_name FROM duck.main.nation n \
             JOIN duck.main.region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AFRICA' ORDER BY n.n_name",
        )
        .expect("join query");
    assert_eq!(column_names(&schema), vec!["n_name"]);
    assert_eq!(
        string_rows(&batches),
        vec!["ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE"]
    );

    // 4. Scalar aggregate over the whole table: 25 nations, max key 24.
    let (schema, batches) = runtime
        .execute("SELECT count(*) AS total, max(n_nationkey) AS mx FROM duck.main.nation")
        .expect("aggregate query");
    assert_eq!(column_names(&schema), vec!["total", "mx"]);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = &batches[0];
    assert_eq!(downcast_i64(batch, 0).value(0), 25);
    assert_eq!(downcast_i32(batch, 1).value(0), 24);
}

#[test]
fn explain_returns_a_plan_without_executing() {
    let path = seed_fixture();
    let path = path.to_str().expect("fixture path is valid UTF-8");
    let runtime = Runtime::from_config(&duck_config(path)).expect("from_config");

    let (schema, batches) = runtime
        .execute("EXPLAIN SELECT r_regionkey FROM duck.main.region WHERE r_regionkey < 2")
        .expect("explain query");
    // A single `plan` text column; the rows are the physical-plan tree lines.
    assert_eq!(column_names(&schema), vec!["plan"]);
    let lines = string_rows(&batches);
    assert!(!lines.is_empty(), "EXPLAIN produced no plan lines");
    let joined = lines.join("\n");
    assert!(
        joined.contains("region"),
        "EXPLAIN plan should name the scanned table, got:\n{joined}"
    );
}

#[test]
fn explain_scan_line_carries_the_effective_pushed_sql() {
    let path = seed_fixture();
    let path = path.to_str().expect("fixture path is valid UTF-8");
    let runtime = Runtime::from_config(&duck_config(path)).expect("from_config");

    let (_schema, batches) = runtime
        .execute("EXPLAIN SELECT r_regionkey FROM duck.main.region WHERE r_regionkey < 2")
        .expect("explain query");
    let lines = string_rows(&batches);
    let joined = lines.join("\n");
    // A plain single-table scan renders as `Scan [<ds>] :: <SQL>`, carrying the
    // effective source SELECT with the folded WHERE, so the pushdown suites can
    // assert on the exact SQL sent to the source.
    let scan_line = lines
        .iter()
        .find(|line| line.trim_start().starts_with("Scan ["))
        .unwrap_or_else(|| panic!("no `Scan [ds] :: SQL` line in EXPLAIN, got:\n{joined}"));
    assert!(
        scan_line.contains("Scan [duck] ::"),
        "scan line should tag the datasource, got:\n{scan_line}"
    );
    assert!(
        scan_line.contains("SELECT") && scan_line.contains("region"),
        "scan line should carry the rendered SELECT over the table, got:\n{scan_line}"
    );
    assert!(
        scan_line.contains("WHERE") && scan_line.contains("r_regionkey"),
        "scan line should fold the WHERE into the pushed SQL, got:\n{scan_line}"
    );
}

#[test]
fn planning_budget_kill_reports_the_stage_timings() {
    let path = seed_fixture();
    let path = path.to_str().expect("fixture path is valid UTF-8");
    // A zero budget is exceeded the moment the clock starts: every plan MUST be
    // killed, and the kill MUST report where the time went.
    let mut config = duck_config(path);
    config.optimizer.planning_budget_ms = 0;
    let runtime = Runtime::from_config(&config).expect("from_config");

    let error = runtime
        .execute("SELECT r_regionkey FROM duck.main.region")
        .expect_err("a blown planning budget must kill the query");
    let message = error.to_string();
    assert!(
        message.contains("planning budget exceeded"),
        "kill must self-identify, got: {message}"
    );
    assert!(
        message.contains("parse"),
        "kill must report per-stage timings, got: {message}"
    );
}

#[test]
fn invalid_query_raises_not_returns_rows() {
    let path = seed_fixture();
    let path = path.to_str().expect("fixture path is valid UTF-8");
    let runtime = Runtime::from_config(&duck_config(path)).expect("from_config");

    // A column the table does not have MUST raise at bind, never return rows.
    let result = runtime.execute("SELECT no_such_column FROM duck.main.region");
    assert!(
        result.is_err(),
        "an invalid column reference must raise, not manufacture a result"
    );
}
