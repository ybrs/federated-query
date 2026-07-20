//! Concurrency of the session-scoped data plane at the `Runtime` level: many
//! runtimes (each its own session) over ONE shared DuckDB file, driven from
//! their own threads at the same time, and a runtime dropping while others run.
//!
//! These pin the guarantees the session plane makes for the supported usage -
//! ONE `Runtime` per driving thread, N runtimes in a process: every session's
//! reads resolve to its own registered source, the process-wide session-keyed
//! maps stay coherent under concurrent access, and pruning one dropped session
//! never disturbs a concurrently running one.
//!
//! DuckDB only: a real Postgres source needs the ADBC driver and a live server;
//! the Python stress suite (`tests/e2e_federated/test_concurrency.py`) covers the
//! federated Postgres path and connection-boundedness after drops.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The region + nation subset the assertions read (TPC-H's 5 regions, 25
/// nations, 5 per region). Only the columns the queries touch are seeded.
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

/// Seed a fresh DuckDB file at a unique temp path and return it. The read-write
/// seeding handle is dropped before any runtime opens the file read-only.
fn seed_fixture() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "fq_conc_region_nation_{}_{}.duckdb",
        std::process::id(),
        id
    ));
    let _ = std::fs::remove_file(&path);
    let source =
        DuckDbSource::open("seed", path.to_str().expect("temp path is valid UTF-8")).expect("open");
    source.execute_batch(SEED_SQL).expect("seed region/nation");
    drop(source);
    path
}

/// Build a single-DuckDB-source `Config` pointing at `path` under the datasource
/// name `duck`. Each concurrent runtime builds its own from the same path.
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
        source_path: None,
    }
}

/// The AFRICA nations, the join query's expected rows. Used as the correctness
/// oracle every concurrent session must reproduce.
fn africa_nations() -> Vec<String> {
    let mut names = Vec::new();
    for name in ["ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE"] {
        names.push(name.to_string());
    }
    names
}

/// Run the AFRICA join on `runtime` and return the nation names in order.
fn run_africa_join(runtime: &Runtime) -> Vec<String> {
    let (_schema, batches) = runtime
        .execute(
            "SELECT n.n_name FROM duck.main.nation n \
             JOIN duck.main.region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AFRICA' ORDER BY n.n_name",
        )
        .expect("join query");
    string_rows(&batches)
}

/// Run the per-region count and return (regionkey, count) rows in order.
fn run_region_counts(runtime: &Runtime) -> Vec<(i32, i64)> {
    let (_schema, batches) = runtime
        .execute(
            "SELECT n_regionkey, count(*) AS c FROM duck.main.nation \
             GROUP BY n_regionkey ORDER BY n_regionkey",
        )
        .expect("group-by query");
    int_count_rows(&batches)
}

/// Flatten a single Utf8 column's values in batch order.
fn string_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column is Utf8");
        for row in 0..batch.num_rows() {
            rows.push(names.value(row).to_string());
        }
    }
    rows
}

/// Flatten `(Int32, Int64)` rows in batch order.
fn int_count_rows(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column is Int32");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column is Int64");
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), counts.value(row)));
        }
    }
    rows
}

#[test]
fn concurrent_own_runtimes_over_one_duckdb_file_stay_correct() {
    // N threads, each its OWN Runtime (its own session) over the SAME seeded
    // DuckDB file, all running queries at once. The session-keyed data-plane
    // maps (registry, duck base cache) and the shared prefetch pools must serve
    // every session correctly and in isolation - a name collision or a torn
    // shared cursor would show up as wrong rows or a panic under contention.
    let path = seed_fixture();
    let path = Arc::new(
        path.to_str()
            .expect("fixture path is valid UTF-8")
            .to_string(),
    );
    let threads = 6;
    let iterations = 4;
    let expected = africa_nations();
    let region_counts = vec![(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)];
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let path = Arc::clone(&path);
        let barrier = Arc::clone(&barrier);
        let expected = expected.clone();
        let region_counts = region_counts.clone();
        handles.push(std::thread::spawn(move || {
            let runtime = Runtime::from_config(&duck_config(&path)).expect("from_config");
            // Release all sessions into the data plane at the same instant so
            // their reads genuinely overlap on the shared maps and pools.
            barrier.wait();
            for _ in 0..iterations {
                assert_eq!(run_africa_join(&runtime), expected);
                assert_eq!(run_region_counts(&runtime), region_counts);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker thread panicked");
    }
    let _ = std::fs::remove_file(&*path);
}

#[test]
fn concurrent_from_config_over_one_shared_config_all_build() {
    // The fedq-server case: several client connections each build a Runtime from
    // the SAME config at once. A config with a source_path opens the shared learned-
    // stats SQLite during from_config, so six concurrent builds contend on ONE fresh
    // stats file - every build must succeed, none may inherit a stats-open race.
    let path = seed_fixture();
    let path = path
        .to_str()
        .expect("fixture path is valid UTF-8")
        .to_string();

    // A fresh config source_path (unique via the fixture stem) whose sibling
    // <stem>.stats.sqlite the builds open and race on; remove any prior run's stats
    // file so this open is a genuinely fresh one.
    let config_path = std::path::Path::new(&path).with_extension("cfg.yaml");
    let stats_path = config_path.with_file_name(format!(
        "{}.stats.sqlite",
        config_path.file_stem().unwrap().to_string_lossy()
    ));
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", stats_path.to_string_lossy()));
    }

    let mut config = duck_config(&path);
    config.source_path = Some(config_path.to_string_lossy().into_owned());
    let config = Arc::new(config);

    let threads = 6;
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let config = Arc::clone(&config);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            // Build all runtimes at once so their stats-catalog opens truly overlap.
            barrier.wait();
            Runtime::from_config(&config).expect("concurrent from_config must build");
        }));
    }
    for handle in handles {
        handle.join().expect("builder thread panicked");
    }

    let _ = std::fs::remove_file(&path);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", stats_path.to_string_lossy()));
    }
}

#[test]
fn dropping_one_runtime_does_not_disturb_a_concurrently_running_one() {
    // A long-lived runtime runs a query loop while short-lived runtimes over the
    // SAME file are built and dropped underneath it. Each drop prunes its own
    // session (registry, caches, and a prune broadcast to both pools); the
    // survivor's results must stay correct throughout - proof that a prune scoped
    // to one session never reaches another live session's state.
    let path = seed_fixture();
    let path = path
        .to_str()
        .expect("fixture path is valid UTF-8")
        .to_string();
    let survivor = Runtime::from_config(&duck_config(&path)).expect("survivor from_config");
    let expected = africa_nations();

    let churn_path = path.clone();
    let churn = std::thread::spawn(move || {
        for _ in 0..20 {
            let runtime =
                Runtime::from_config(&duck_config(&churn_path)).expect("churn from_config");
            // Run one real query so the session opened its data-plane connections
            // before it is dropped and pruned.
            let _ = run_region_counts(&runtime);
            drop(runtime);
        }
    });

    for _ in 0..40 {
        assert_eq!(run_africa_join(&survivor), expected);
    }
    churn.join().expect("churn thread panicked");
    // The survivor still reads correctly after every other session has been
    // pruned out from under it.
    assert_eq!(run_africa_join(&survivor), expected);
    let _ = std::fs::remove_file(&path);
}
