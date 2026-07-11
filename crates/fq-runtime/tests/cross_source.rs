//! MILESTONE C step 3a cross-source proof: a `Runtime` with a DuckDB source AND
//! a Postgres source runs a JOIN ACROSS the two sources, exercising the engine's
//! federated path (per-source reads merged and reduced by the coordinator).
//!
//! Gated three ways (all early-return, never fail the suite on absence):
//!   * the DuckDB fixture must exist,
//!   * Postgres must be reachable on :5432 (mirrors the fq-connectors gating),
//!   * the ADBC Postgres driver shared library must be present (the exec-plane
//!     Postgres reads go over ADBC).
//!
//! The Postgres dimension fixture (`fq_rt_region`) is created by the test itself
//! (via the connector's setup helper) BEFORE the runtime loads metadata, and
//! dropped at the end.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{RecordBatch, StringArray};
use fq_common::{Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig};
use fq_connectors::PostgresSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The bundled DuckDB fixture (TPC-H SF 0.01).
fn duck_fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/tpch/data/tpch_sf0.01.duckdb")
}

/// The libpq connection string for the local trust-auth Postgres.
fn pg_conn_str() -> String {
    "host=localhost port=5432 user=postgres dbname=postgres".to_string()
}

/// The first ADBC Postgres driver shared library found among known install
/// locations, or `None` (the test then skips - the exec plane needs it).
fn adbc_driver_path() -> Option<String> {
    let candidates = [
        "/workspace/federated-query/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
        "/workspace/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
        "/workspace/venv-fedq/lib/python3.13/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    ];
    for candidate in candidates {
        if PathBuf::from(candidate).exists() {
            return Some(candidate.to_string());
        }
    }
    None
}

/// Build a duckdb string param map for one path.
fn duck_params(path: &str) -> BTreeMap<String, Value> {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    params
}

/// Build a postgres param map wiring the local trust-auth connection plus the
/// ADBC driver path the exec plane reads over.
fn pg_params(adbc_driver: &str) -> BTreeMap<String, Value> {
    let mut params = BTreeMap::new();
    params.insert("host".to_string(), Value::String("localhost".to_string()));
    params.insert("port".to_string(), Value::Number(5432.into()));
    params.insert("user".to_string(), Value::String("postgres".to_string()));
    params.insert(
        "database".to_string(),
        Value::String("postgres".to_string()),
    );
    params.insert(
        "schemas".to_string(),
        Value::Sequence(vec![Value::String("public".to_string())]),
    );
    params.insert(
        "adbc_driver".to_string(),
        Value::String(adbc_driver.to_string()),
    );
    params
}

/// Assemble a two-source (`duck` + `pg`) config.
fn cross_config(duck_path: &str, adbc_driver: &str) -> Config {
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
        "pg".to_string(),
        DataSourceConfig {
            name: "pg".to_string(),
            ty: "postgres".to_string(),
            config: pg_params(adbc_driver),
            capabilities: Vec::new(),
        },
    );
    Config {
        datasources,
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        source_path: None,
    }
}

/// Flatten two Utf8 columns into `(String, String)` rows in batch order.
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

/// Downcast one column to `StringArray` (panics on a type mismatch).
fn downcast_str(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is Utf8")
}

/// Create the Postgres dimension fixture, replacing any prior copy.
fn create_pg_fixture(source: &PostgresSource) {
    source
        .execute(
            "DROP TABLE IF EXISTS fq_rt_region; \
             CREATE TABLE fq_rt_region (r_regionkey integer, r_name text); \
             INSERT INTO fq_rt_region VALUES \
             (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST'); \
             ANALYZE fq_rt_region;",
        )
        .expect("create pg fixture");
}

#[test]
fn joins_duckdb_nation_against_postgres_region() {
    let duck_path = duck_fixture_path();
    if !duck_path.exists() {
        eprintln!("skipping cross_source: duckdb fixture absent");
        return;
    }
    let Some(adbc_driver) = adbc_driver_path() else {
        eprintln!("skipping cross_source: ADBC postgres driver not found");
        return;
    };
    // Connect for setup; if Postgres is unreachable, skip (mirrors fq-connectors).
    let Ok(setup) = PostgresSource::connect("pg", &pg_conn_str(), vec!["public".to_string()])
    else {
        eprintln!("skipping cross_source: postgres unreachable on :5432");
        return;
    };
    create_pg_fixture(&setup);
    drop(setup);

    let duck_path = duck_path.to_str().expect("fixture path is valid UTF-8");
    let runtime =
        Runtime::from_config(&cross_config(duck_path, &adbc_driver)).expect("from_config");

    // Cross-source join: nation (DuckDB) x region (Postgres), filtered to AFRICA.
    let (schema, batches) = runtime
        .execute(
            "SELECT n.n_name, r.r_name FROM duck.main.nation n \
             JOIN pg.public.fq_rt_region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AFRICA' ORDER BY n.n_name",
        )
        .expect("cross-source join");

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

    // Clean up the fixture.
    if let Ok(cleanup) = PostgresSource::connect("pg", &pg_conn_str(), vec!["public".to_string()]) {
        let _ = cleanup.execute("DROP TABLE IF EXISTS fq_rt_region;");
    }
}
