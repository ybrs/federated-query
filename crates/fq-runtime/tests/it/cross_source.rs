//! Cross-source proof: a `Runtime` with a DuckDB source AND
//! a Postgres source runs a JOIN ACROSS the two sources, exercising the engine's
//! federated path (per-source reads merged and reduced by the coordinator).
//!
//! The test uses the repository test server selected by `PGPORT`; any connection
//! failure fails the test. The ADBC Postgres driver is also required because the
//! exec plane reads Postgres over ADBC.
//!
//! The Postgres dimension fixture (`fq_rt_region`) is created by the test itself
//! (via the connector's setup helper) BEFORE the runtime loads metadata, and
//! dropped at the end.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use arrow::array::{RecordBatch, StringArray};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::{DuckDbSource, PostgresSource};
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The nation subset the join reads: TPC-H's 25 nations (5 per region), with the
/// real names and region keys. The join filters to region 0 (AFRICA), whose five
/// nations are ALGERIA, ETHIOPIA, KENYA, MOROCCO, MOZAMBIQUE. Region lives on the
/// Postgres side (see `create_pg_fixture`), so only nation is seeded here.
const NATION_SEED_SQL: &str = "\
    CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER); \
    INSERT INTO nation VALUES \
        (0,'ALGERIA',0),(1,'ARGENTINA',1),(2,'BRAZIL',1),(3,'CANADA',1),(4,'EGYPT',4), \
        (5,'ETHIOPIA',0),(6,'FRANCE',3),(7,'GERMANY',3),(8,'INDIA',2),(9,'INDONESIA',2), \
        (10,'IRAN',4),(11,'IRAQ',4),(12,'JAPAN',2),(13,'JORDAN',4),(14,'KENYA',0), \
        (15,'MOROCCO',0),(16,'MOZAMBIQUE',0),(17,'PERU',1),(18,'CHINA',2),(19,'ROMANIA',3), \
        (20,'SAUDI ARABIA',4),(21,'VIETNAM',2),(22,'RUSSIA',3),(23,'UNITED KINGDOM',3), \
        (24,'UNITED STATES',1);";

/// Seed a fresh DuckDB file with the nation subset and return its path. DuckDB
/// opens a file read-write (single writer per file per process) and this test
/// ships temp tables INTO the fact source, so every caller builds its OWN file at
/// a unique temp path: parallel test binaries never contend, and no checked-in
/// database is ever opened.
fn seed_duck_fixture() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "fq_seed_nation_{}_{}.duckdb",
        std::process::id(),
        id
    ));
    // A leftover file from a crashed prior run would make CREATE TABLE fail; start
    // from a clean path.
    let _ = std::fs::remove_file(&path);
    let source =
        DuckDbSource::open("seed", path.to_str().expect("temp path is valid UTF-8")).expect("open");
    source.execute_batch(NATION_SEED_SQL).expect("seed nation");
    drop(source);
    path
}

/// The libpq connection string for the selected test PostgreSQL server.
fn pg_conn_str() -> String {
    std::env::var("FQ_TEST_PG_URL").unwrap_or_else(|_| {
        let port = std::env::var("PGPORT").expect("PGPORT must select test PostgreSQL");
        format!("host=localhost port={port} user=postgres dbname=postgres")
    })
}

/// Prints the directory holding the installed `adbc_driver_postgresql` package.
const ADBC_PACKAGE_DIR_PROGRAM: &str =
    "import adbc_driver_postgresql, os; print(os.path.dirname(adbc_driver_postgresql.__file__))";

/// Return the Python interpreter from the active virtual environment.
fn python_interpreter() -> String {
    let venv = std::env::var("VIRTUAL_ENV").expect("VIRTUAL_ENV must select Python");
    assert!(!venv.is_empty(), "VIRTUAL_ENV must not be empty");
    format!("{venv}/bin/python3")
}

/// The ADBC Postgres driver shipped with the active Python environment.
///
/// Resolved by asking the interpreter where its `adbc_driver_postgresql`
/// package lives. Every failure here panics rather than returning an absence
/// for the caller to skip on: the exec plane reads Postgres over ADBC, so
/// without the driver this test proves nothing, and a green result would claim
/// cross-source coverage the run never had.
fn adbc_driver_path() -> String {
    let interpreter = python_interpreter();
    let output = Command::new(&interpreter)
        .args(["-c", ADBC_PACKAGE_DIR_PROGRAM])
        .output()
        .unwrap_or_else(|error| panic!("cannot run {interpreter} to locate ADBC: {error}"));
    assert!(
        output.status.success(),
        "{interpreter} cannot import adbc_driver_postgresql: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let package_dir = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let driver = PathBuf::from(package_dir).join("libadbc_driver_postgresql.so");
    assert!(
        driver.exists(),
        "no ADBC driver library at {}",
        driver.display()
    );
    driver.to_string_lossy().into_owned()
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
    let port = std::env::var("PGPORT")
        .expect("PGPORT must select test PostgreSQL")
        .parse::<u16>()
        .expect("PGPORT must be a valid port number");
    params.insert("port".to_string(), Value::Number(port.into()));
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
            change_keys: BTreeMap::new(),
        },
    );
    datasources.insert(
        "pg".to_string(),
        DataSourceConfig {
            name: "pg".to_string(),
            ty: "postgres".to_string(),
            config: pg_params(adbc_driver),
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
        diagnostics: fq_common::DiagnosticsConfig::default(),
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
    let duck_path = seed_duck_fixture();
    let adbc_driver = adbc_driver_path();
    // Connect to the required test service before creating its fixture.
    let setup = PostgresSource::connect("pg", &pg_conn_str(), vec!["public".to_string()])
        .expect("connect to test PostgreSQL");
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
    let cleanup = PostgresSource::connect("pg", &pg_conn_str(), vec!["public".to_string()])
        .expect("reconnect to test PostgreSQL for cleanup");
    cleanup
        .execute("DROP TABLE IF EXISTS fq_rt_region;")
        .expect("drop PostgreSQL fixture");
}

/// Keep JSON projections and predicates on the coordinator when PostgreSQL lacks the function.
#[test]
fn postgres_json_extraction_matches_native_semantics() {
    let duck = seed_duck_fixture();
    let driver = adbc_driver_path();
    let setup =
        PostgresSource::connect("json_setup", &pg_conn_str(), vec!["public".into()]).unwrap();
    setup.execute("DROP TABLE IF EXISTS fq_rt_json_region; CREATE TABLE fq_rt_json_region(r_regionkey integer,r_name text); INSERT INTO fq_rt_json_region VALUES(0,'AFRICA'),(1,'EUROPE'); ANALYZE fq_rt_json_region;").unwrap();
    let runtime = Runtime::from_config(&cross_config(duck.to_str().unwrap(), &driver)).unwrap();
    assert_postgres_json_queries(&runtime);
    drop(runtime);
    setup.execute("DROP TABLE fq_rt_json_region").unwrap();
}

/// Compare projection, WHERE, GROUP BY and ORDER BY without asking PostgreSQL to run JSON SQL.
fn assert_postgres_json_queries(runtime: &Runtime) {
    let extract = "json_extract_string('\"' || r.r_name || '\"', '$')";
    let table = "pg.public.fq_rt_json_region AS r";
    let queries = [
        (format!("SELECT {extract} AS name FROM {table} WHERE r.r_regionkey=0"), "AFRICA"),
        (format!("SELECT r.r_name FROM {table} WHERE {extract}='AFRICA'"), "AFRICA"),
        (format!("SELECT {extract} AS name FROM {table} GROUP BY {extract} ORDER BY {extract} LIMIT 1"), "AFRICA"),
        (format!("SELECT r.r_name FROM {table} ORDER BY {extract} DESC LIMIT 1"), "EUROPE"),
        (format!("SELECT {extract} AS name, ROW_NUMBER() OVER (ORDER BY r.r_name) AS seq FROM {table} LIMIT 1"), "AFRICA"),
        (format!("SELECT json_extract_path_text('\"' || r.r_name || '\"', '$') AS name FROM {table} WHERE r.r_regionkey=0"), "AFRICA"),
    ];
    for (sql, expected) in queries {
        assert_postgres_json_query(runtime, &sql, expected);
    }
}

/// Require the decoded result and row count to survive coordinator placement.
fn assert_postgres_json_query(runtime: &Runtime, sql: &str, expected: &str) {
    let (_, batches) = runtime
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), expected, "{sql}");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}
