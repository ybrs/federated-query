//! End-to-end: a config-driven `Runtime` over a Parquet directory source runs a
//! real query (exact rows out of the footer-planned + DataFusion-executed scan),
//! and a `Runtime` over a DuckDB source AND a Parquet source joins ACROSS the two.
//!
//! The Parquet directory is written by the test (the parquet arrow writer), so no
//! checked-in data is needed. The DuckDB half seeds its own temp file. Neither
//! source needs a server, so this suite is not gated (unlike the Postgres
//! cross-source test); the DuckDB fixture write is the only external dependency.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use parquet::arrow::ArrowWriter;
use serde_yaml::Value;

/// The nation subset the DuckDB side seeds (5 nations per region, region 0 =
/// AFRICA holds ALGERIA/ETHIOPIA/KENYA/MOROCCO/MOZAMBIQUE); only the columns the
/// queries touch are seeded.
const NATION_SEED_SQL: &str = "\
    CREATE TABLE nation (n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER); \
    INSERT INTO nation VALUES \
        (0,'ALGERIA',0),(1,'ARGENTINA',1),(2,'BRAZIL',1),(3,'CANADA',1),(4,'EGYPT',4), \
        (5,'ETHIOPIA',0),(6,'FRANCE',3),(7,'GERMANY',3),(8,'INDIA',2),(9,'INDONESIA',2), \
        (10,'IRAN',4),(11,'IRAQ',4),(12,'JAPAN',2),(13,'JORDAN',4),(14,'KENYA',0), \
        (15,'MOROCCO',0),(16,'MOZAMBIQUE',0),(17,'PERU',1),(18,'CHINA',2),(19,'ROMANIA',3), \
        (20,'SAUDI ARABIA',4),(21,'VIETNAM',2),(22,'RUSSIA',3),(23,'UNITED KINGDOM',3), \
        (24,'UNITED STATES',1);";

/// Seed a fresh DuckDB file with the nation subset and return its path (a unique
/// temp path per caller: parallel test binaries never contend, no checked-in db).
fn seed_duck_fixture() -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("fq_pq_nation_{}_{}.duckdb", std::process::id(), id));
    let _ = std::fs::remove_file(&path);
    let source =
        DuckDbSource::open("seed", path.to_str().expect("temp path is valid UTF-8")).expect("open");
    source.execute_batch(NATION_SEED_SQL).expect("seed nation");
    drop(source);
    path
}

/// A fresh empty temp directory for one test's Parquet files.
fn temp_parquet_dir(tag: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fq_pq_rt_{tag}_{}_{}", std::process::id(), id));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp parquet dir");
    dir
}

/// Write the region fixture as `<dir>/region.parquet`: TPC-H's 5 regions, an
/// INTEGER key and a VARCHAR name.
fn write_region_parquet(dir: &Path) {
    let key: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![
        "AFRICA",
        "AMERICA",
        "ASIA",
        "EUROPE",
        "MIDDLE EAST",
    ]));
    let schema = Schema::new(vec![
        Field::new("r_regionkey", ArrowDataType::Int32, false),
        Field::new("r_name", ArrowDataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![key, name]).expect("region batch");
    let file = File::create(dir.join("region.parquet")).expect("create region.parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write region");
    writer.close().expect("close writer");
}

/// A duckdb string-param map for one path.
fn duck_params(path: &str) -> BTreeMap<String, Value> {
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), Value::String(path.to_string()));
    params
}

/// A parquet param map wiring the directory of `<table>.parquet` files.
fn parquet_params(dir: &str) -> BTreeMap<String, Value> {
    let mut params = BTreeMap::new();
    params.insert("dir".to_string(), Value::String(dir.to_string()));
    params
}

/// Assemble a single Parquet-source `Config` under the datasource name `pq`.
fn parquet_config(dir: &str) -> Config {
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "pq".to_string(),
        DataSourceConfig {
            name: "pq".to_string(),
            ty: "parquet".to_string(),
            config: parquet_params(dir),
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    base_config(datasources)
}

/// Assemble a two-source (`nduck` + `pq`) `Config`. The DuckDB source is named
/// `nduck` (not `duck`): the fq-exec datasource registry is process-global and
/// keyed by NAME, and the `it` binary runs every suite's tests in one process, so
/// a `duck` here (nation only) would race the `duckdb_runtime` suite's `duck`
/// (region + nation) and clobber it. A distinct name keeps the suites isolated.
fn cross_config(duck_path: &str, parquet_dir: &str) -> Config {
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "nduck".to_string(),
        DataSourceConfig {
            name: "nduck".to_string(),
            ty: "duckdb".to_string(),
            config: duck_params(duck_path),
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    datasources.insert(
        "pq".to_string(),
        DataSourceConfig {
            name: "pq".to_string(),
            ty: "parquet".to_string(),
            config: parquet_params(parquet_dir),
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    base_config(datasources)
}

/// Wrap a datasource map in a default-everything-else `Config`.
fn base_config(datasources: BTreeMap<String, DataSourceConfig>) -> Config {
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

/// Downcast one column to `StringArray` (panics loudly on a type mismatch).
fn downcast_str(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column is Utf8")
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

#[test]
fn runs_a_filtered_projected_query_over_a_parquet_source() {
    let dir = temp_parquet_dir("single");
    write_region_parquet(&dir);
    let runtime =
        Runtime::from_config(&parquet_config(dir.to_str().unwrap())).expect("from_config");

    // Projection + filter + ORDER BY: the two lowest regions by name.
    let (schema, batches) = runtime
        .execute(
            "SELECT r_name FROM pq.main.region \
             WHERE r_regionkey < 2 ORDER BY r_name",
        )
        .expect("parquet query");
    assert_eq!(column_names(&schema), vec!["r_name"]);
    assert_eq!(string_rows(&batches), vec!["AFRICA", "AMERICA"]);
}

#[test]
fn counts_exact_rows_out_of_a_parquet_source() {
    let dir = temp_parquet_dir("count");
    write_region_parquet(&dir);
    let runtime =
        Runtime::from_config(&parquet_config(dir.to_str().unwrap())).expect("from_config");

    let (schema, batches) = runtime
        .execute("SELECT count(*) AS c FROM pq.main.region")
        .expect("count query");
    assert_eq!(column_names(&schema), vec!["c"]);
    let total: i64 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0);
    assert_eq!(total, 5);
}

#[test]
fn joins_duckdb_nation_against_parquet_region() {
    let duck_path = seed_duck_fixture();
    let dir = temp_parquet_dir("join");
    write_region_parquet(&dir);
    let duck_path = duck_path.to_str().expect("fixture path is valid UTF-8");
    let runtime =
        Runtime::from_config(&cross_config(duck_path, dir.to_str().unwrap())).expect("from_config");

    // Cross-source join: nation (DuckDB) x region (Parquet), filtered to AFRICA.
    let (schema, batches) = runtime
        .execute(
            "SELECT n.n_name, r.r_name FROM nduck.main.nation n \
             JOIN pq.main.region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AFRICA' ORDER BY n.n_name",
        )
        .expect("cross-source join");
    assert_eq!(column_names(&schema), vec!["n_name", "r_name"]);
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
}

#[test]
fn invalid_column_over_parquet_raises_not_returns_rows() {
    let dir = temp_parquet_dir("invalid");
    write_region_parquet(&dir);
    let runtime =
        Runtime::from_config(&parquet_config(dir.to_str().unwrap())).expect("from_config");

    // A column the parquet table does not have MUST raise at bind, never scan.
    let result = runtime.execute("SELECT no_such_column FROM pq.main.region");
    assert!(
        result.is_err(),
        "an invalid column reference must raise, not manufacture a result"
    );
}

/// Assemble a single-FILE Parquet-source `Config`: datasource `name` reading the
/// one `.parquet` file at `path` (the `file` param, not `dir`).
fn parquet_file_config(name: &str, path: &str) -> Config {
    let mut params = BTreeMap::new();
    params.insert("file".to_string(), Value::String(path.to_string()));
    let mut datasources = BTreeMap::new();
    datasources.insert(
        name.to_string(),
        DataSourceConfig {
            name: name.to_string(),
            ty: "parquet".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    base_config(datasources)
}

/// Flatten one Utf8 column (by index) across batches, in batch order.
fn string_rows_at(batches: &[RecordBatch], index: usize) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = downcast_str(batch, index);
        for row in 0..batch.num_rows() {
            rows.push(column.value(row).to_string());
        }
    }
    rows
}

#[test]
fn a_single_file_source_is_one_table_named_after_the_datasource() {
    let dir = temp_parquet_dir("file");
    write_region_parquet(&dir);
    let file = dir.join("region.parquet");
    let runtime = Runtime::from_config(&parquet_file_config("ev", file.to_str().unwrap()))
        .expect("from_config");

    // The bare datasource name resolves as a table reference, star-expanded
    // from catalog metadata and executed through DataFusion.
    let (schema, batches) = runtime
        .execute("SELECT * FROM ev ORDER BY r_regionkey LIMIT 2")
        .expect("bare-name select");
    assert_eq!(column_names(&schema), vec!["r_regionkey", "r_name"]);
    assert_eq!(string_rows_at(&batches, 1), vec!["AFRICA", "AMERICA"]);

    // The fully qualified form names the same table.
    let (_, qualified) = runtime
        .execute("SELECT r_name FROM ev.main.ev ORDER BY r_regionkey")
        .expect("qualified select");
    assert_eq!(string_rows(&qualified).len(), 5);
}

#[test]
fn show_tables_and_describe_surface_a_single_file_table() {
    let dir = temp_parquet_dir("intro");
    write_region_parquet(&dir);
    let file = dir.join("region.parquet");
    let runtime = Runtime::from_config(&parquet_file_config("evd", file.to_str().unwrap()))
        .expect("from_config");

    let (schema, batches) = runtime.execute("SHOW TABLES").expect("show tables");
    assert_eq!(
        column_names(&schema),
        vec!["datasource", "schema", "table"]
    );
    assert_eq!(string_rows_at(&batches, 0), vec!["evd"]);
    assert_eq!(string_rows_at(&batches, 1), vec!["main"]);
    assert_eq!(string_rows_at(&batches, 2), vec!["evd"]);

    let (schema, batches) = runtime.execute("DESCRIBE evd").expect("describe");
    assert_eq!(column_names(&schema), vec!["column", "type", "nullable"]);
    assert_eq!(
        string_rows_at(&batches, 0),
        vec!["r_regionkey", "r_name"]
    );
    assert_eq!(string_rows_at(&batches, 1), vec!["INTEGER", "VARCHAR"]);
}

#[test]
fn show_tables_narrows_to_one_datasource_and_an_unknown_one_raises() {
    let duck_path = seed_duck_fixture();
    let dir = temp_parquet_dir("narrow");
    write_region_parquet(&dir);
    let duck_path = duck_path.to_str().expect("fixture path is valid UTF-8");
    let runtime =
        Runtime::from_config(&cross_config(duck_path, dir.to_str().unwrap())).expect("from_config");

    let (_, batches) = runtime
        .execute("SHOW TABLES FROM pq")
        .expect("show tables from");
    assert_eq!(string_rows_at(&batches, 0), vec!["pq"]);
    assert_eq!(string_rows_at(&batches, 2), vec!["region"]);

    let error = runtime.execute("SHOW TABLES FROM nope").unwrap_err();
    assert!(
        format!("{error}").contains("'nope' does not exist"),
        "unexpected error: {error}"
    );
}

#[test]
fn describe_resolves_qualified_references_and_an_unknown_table_raises() {
    let dir = temp_parquet_dir("descq");
    write_region_parquet(&dir);
    let runtime =
        Runtime::from_config(&parquet_config(dir.to_str().unwrap())).expect("from_config");

    // Every qualification level resolves to the same table.
    for sql in ["DESCRIBE region", "DESCRIBE main.region", "DESCRIBE pq.main.region"] {
        let (_, batches) = runtime.execute(sql).expect(sql);
        assert_eq!(
            string_rows_at(&batches, 0),
            vec!["r_regionkey", "r_name"],
            "{sql}"
        );
    }

    let error = runtime.execute("DESCRIBE pq.main.nope").unwrap_err();
    assert!(
        format!("{error}").contains("not found in the catalog"),
        "unexpected error: {error}"
    );
}
