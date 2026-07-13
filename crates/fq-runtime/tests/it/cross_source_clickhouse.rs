//! Cross-source proof: a `Runtime` with a DuckDB source AND a ClickHouse source
//! runs a JOIN ACROSS the two, exercising the federated path with the new
//! ClickHouse connector (HTTP read -> Arrow, merged and reduced by the
//! coordinator).
//!
//! Gated (all early-return, never fail the suite on absence):
//!   * the DuckDB fixture must build,
//!   * ClickHouse must be reachable over HTTP (default :8123).
//!
//! The ClickHouse dimension is created by the test (via the connector) in a
//! uniquely-named database BEFORE the runtime loads metadata, and dropped at the
//! end.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{RecordBatch, StringArray};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::{ClickHouseSource, DuckDbSource};
use fq_runtime::Runtime;
use serde_yaml::Value;

const NATION_SEED_SQL: &str = "\
    CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER); \
    INSERT INTO nation VALUES \
        (0,'ALGERIA',0),(1,'ARGENTINA',1),(5,'ETHIOPIA',0),(14,'KENYA',0), \
        (15,'MOROCCO',0),(16,'MOZAMBIQUE',0),(18,'CHINA',2),(6,'FRANCE',3);";

fn clickhouse_url() -> String {
    std::env::var("FQ_TEST_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123/".to_string())
}

/// Seed a fresh DuckDB file with the nation subset; each caller builds its own
/// unique path (single writer per file per process).
fn seed_duck_fixture() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("fq_ch_nation_{}_{}.duckdb", std::process::id(), id));
    let _ = std::fs::remove_file(&path);
    let source =
        DuckDbSource::open("seed", path.to_str().expect("temp path is valid UTF-8")).expect("open");
    source.execute_batch(NATION_SEED_SQL).expect("seed nation");
    drop(source);
    path
}

/// The host:port the register path parses out of the endpoint URL.
fn host_port() -> (String, u16) {
    let url = clickhouse_url();
    let rest = url.trim_start_matches("http://").trim_end_matches('/');
    let mut parts = rest.splitn(2, ':');
    let host = parts.next().unwrap_or("127.0.0.1").to_string();
    let port = parts.next().and_then(|p| p.parse().ok()).unwrap_or(8123);
    (host, port)
}

fn duck_params(path: &str) -> BTreeMap<String, Value> {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    params
}

fn clickhouse_params(database: &str) -> BTreeMap<String, Value> {
    let (host, port) = host_port();
    let mut params = BTreeMap::new();
    params.insert("host".to_string(), Value::String(host));
    params.insert("port".to_string(), Value::Number(port.into()));
    params.insert(
        "schemas".to_string(),
        Value::Sequence(vec![Value::String(database.to_string())]),
    );
    params
}

fn cross_config(duck_path: &str, database: &str) -> Config {
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "duck".to_string(),
        DataSourceConfig {
            name: "duck".to_string(),
            ty: "duckdb".to_string(),
            config: duck_params(duck_path),
            capabilities: Vec::new(),
        },
    );
    datasources.insert(
        "ch".to_string(),
        DataSourceConfig {
            name: "ch".to_string(),
            ty: "clickhouse".to_string(),
            config: clickhouse_params(database),
            capabilities: Vec::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        server: ServerConfig::default(),
        source_path: None,
    }
}

fn string_pair_rows(batches: &[RecordBatch]) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let left = downcast_str(batch, 0);
        let right = downcast_str(batch, 1);
        for row in 0..batch.num_rows() {
            rows.push((left.value(row).to_string(), right.value(row).to_string()));
        }
    }
    rows
}

fn downcast_str(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is Utf8")
}

#[test]
fn joins_duckdb_nation_against_clickhouse_region() {
    let duck_path = seed_duck_fixture();
    let database = format!("fq_ch_x_{}", std::process::id());
    let Ok(setup) =
        ClickHouseSource::connect("ch", clickhouse_url(), None, None, vec![database.clone()])
    else {
        eprintln!("skipping cross_source_clickhouse: clickhouse unreachable");
        return;
    };
    setup
        .execute(&format!("CREATE DATABASE IF NOT EXISTS \"{database}\""))
        .expect("create db");
    setup
        .execute(&format!(
            "CREATE TABLE \"{database}\".region (r_regionkey UInt32, r_name String) \
             ENGINE = MergeTree ORDER BY r_regionkey"
        ))
        .expect("create region");
    setup
        .execute(&format!(
            "INSERT INTO \"{database}\".region VALUES \
             (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST')"
        ))
        .expect("seed region");

    let duck_path_str = duck_path.to_str().expect("fixture path is valid UTF-8");
    let runtime =
        Runtime::from_config(&cross_config(duck_path_str, &database)).expect("from_config");

    let sql = format!(
        "SELECT n.n_name, r.r_name FROM duck.main.nation n \
         JOIN ch.{database}.region r ON n.n_regionkey = r.r_regionkey \
         WHERE r.r_name = 'AFRICA' ORDER BY n.n_name"
    );
    let (schema, batches) = runtime.execute(&sql).expect("cross-source join");

    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(names, vec!["n_name", "r_name"]);
    assert_eq!(
        string_pair_rows(&batches),
        vec![
            ("ALGERIA".to_string(), "AFRICA".to_string()),
            ("ETHIOPIA".to_string(), "AFRICA".to_string()),
            ("KENYA".to_string(), "AFRICA".to_string()),
            ("MOROCCO".to_string(), "AFRICA".to_string()),
            ("MOZAMBIQUE".to_string(), "AFRICA".to_string()),
        ]
    );

    let _ = setup.execute(&format!("DROP DATABASE IF EXISTS \"{database}\""));
    let _ = std::fs::remove_file(&duck_path);
}
