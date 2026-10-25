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
        diagnostics: fq_common::DiagnosticsConfig::default(),
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
    assert_eq!(column_names(&schema), vec!["datasource", "schema", "table"]);
    assert_eq!(string_rows_at(&batches, 0), vec!["evd"]);
    assert_eq!(string_rows_at(&batches, 1), vec!["main"]);
    assert_eq!(string_rows_at(&batches, 2), vec!["evd"]);

    let (schema, batches) = runtime.execute("DESCRIBE evd").expect("describe");
    assert_eq!(column_names(&schema), vec!["column", "type", "nullable"]);
    assert_eq!(string_rows_at(&batches, 0), vec!["r_regionkey", "r_name"]);
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
    for sql in [
        "DESCRIBE region",
        "DESCRIBE main.region",
        "DESCRIBE pq.main.region",
    ] {
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

#[test]
fn an_aggregate_only_in_order_by_makes_the_query_grouped() {
    // `ORDER BY count(*)` with no GROUP BY and no aggregate in the SELECT list is
    // valid SQL: the aggregate groups the whole input into ONE row. The five
    // fixture regions must therefore come back as a single row, not five - an
    // aggregate sort key over an ungrouped scan is not an aggregate query.
    let dir = temp_parquet_dir("order_by_agg");
    write_region_parquet(&dir);
    let runtime =
        Runtime::from_config(&parquet_config(dir.to_str().unwrap())).expect("from_config");

    for sql in [
        "SELECT 1 AS one FROM pq.main.region ORDER BY count(*)",
        "SELECT 1 AS one FROM pq.main.region ORDER BY count(*) + 1",
        "SELECT 1 AS one FROM pq.main.region ORDER BY count(*) DESC",
    ] {
        let (_, batches) = runtime
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql} must run: {error}"));
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "{sql} groups the whole input into one row");
    }

    // The five rows are still five when nothing groups them, so the aggregate
    // gate did not start swallowing ordinary ORDER BY queries.
    let (_, batches) = runtime
        .execute("SELECT r_name FROM pq.main.region ORDER BY r_name")
        .expect("plain order by");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 5, "a plain ORDER BY still returns every row");
}

#[test]
fn an_unknown_datasource_parameter_raises() {
    // A misspelled key must not be ignored. `host` falls back to localhost when
    // absent, so accepting `hostt` silently points the engine at a different
    // machine than the config names - a wrong answer sourced from the wrong
    // database, with nothing to read in a log.
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "hostt".to_string(),
        Value::String("prod-db.invalid".to_string()),
    );
    params.insert("user".to_string(), Value::String("postgres".to_string()));
    params.insert("database".to_string(), Value::String("test_db".to_string()));
    let mut sources = BTreeMap::new();
    sources.insert(
        "pg".to_string(),
        DataSourceConfig {
            name: "pg".to_string(),
            ty: "postgresql".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    let error = Runtime::from_config(&base_config(sources))
        .err()
        .expect("a misspelled postgres parameter must raise");
    let message = error.to_string();
    assert!(
        message.contains("hostt"),
        "the raise names the offending key: {message}"
    );

    // Same for a parquet source, whose real keys are `dir` and `file`.
    let dir = temp_parquet_dir("unknown_key_raises");
    write_region_parquet(&dir);
    let mut pq = parquet_params(dir.to_str().unwrap());
    pq.insert("cache_sizee".to_string(), Value::String("999".to_string()));
    let mut sources2 = BTreeMap::new();
    sources2.insert(
        "pq".to_string(),
        DataSourceConfig {
            name: "pq".to_string(),
            ty: "parquet".to_string(),
            config: pq,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    let error = Runtime::from_config(&base_config(sources2))
        .err()
        .expect("an unknown parquet parameter must raise");
    assert!(
        error.to_string().contains("cache_sizee"),
        "the raise names the offending key: {error}"
    );
}

#[test]
fn every_real_datasource_parameter_is_still_accepted() {
    // The guard must not reject a legitimate config: the parquet source the
    // other tests in this file use has to keep working.
    let dir = temp_parquet_dir("known_keys_ok");
    write_region_parquet(&dir);
    Runtime::from_config(&parquet_config(dir.to_str().unwrap()))
        .expect("a config using only real keys still builds");
}

#[test]
fn a_programmatic_config_can_be_given_a_learned_catalog() {
    // A config built in memory has no file path, so no catalog path derives from
    // it and every estimate falls back to default constants. FEDQ_STATS_CATALOG
    // names one explicitly. Observable because opening the catalog CREATES the
    // sqlite file at that path.
    let dir = temp_parquet_dir("stats_override");
    write_region_parquet(&dir);
    let catalog_path = dir.join("explicit.stats.sqlite");
    assert!(!catalog_path.exists(), "starts absent");

    let mut config = parquet_config(dir.to_str().unwrap());
    // A programmatic config: no file behind it.
    config.source_path = None;

    // SAFETY: set and restored within this test; no other test reads this var.
    unsafe { std::env::set_var("FEDQ_STATS_CATALOG", &catalog_path) };
    let built = Runtime::from_config(&config);
    unsafe { std::env::remove_var("FEDQ_STATS_CATALOG") };
    built.expect("runtime builds with an explicit catalog");

    assert!(
        catalog_path.exists(),
        "the named catalog was opened at {}",
        catalog_path.display()
    );
}

/// Write `<dir>/<name>.parquet` with five rows whose `r_regionkey` starts at
/// `base` and whose `r_name` is `R<key>`. Used to build multi-file fixtures.
fn write_region_range(dir: &Path, name: &str, base: i32) {
    let keys: Vec<i32> = (base..base + 5).collect();
    let names: Vec<String> = keys.iter().map(|key| format!("R{key}")).collect();
    let key: ArrayRef = Arc::new(Int32Array::from(keys));
    let name_col: ArrayRef = Arc::new(StringArray::from(names));
    let schema = Schema::new(vec![
        Field::new("r_regionkey", ArrowDataType::Int32, false),
        Field::new("r_name", ArrowDataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![key, name_col]).expect("range batch");
    let file = File::create(dir.join(format!("{name}.parquet"))).expect("create range file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write range");
    writer.close().expect("close writer");
}

/// Write `<dir>/part-0.parquet` (keys 0..4) and `<dir>/part-1.parquet` (keys
/// 5..9): two files, disjoint keys, one merged table.
fn write_two_region_files(dir: &Path) {
    write_region_range(dir, "part-0", 0);
    write_region_range(dir, "part-1", 5);
}

/// Write part-0 WITH and part-1 WITHOUT `r_name`, for the union-by-name case: a
/// glob over both yields a table whose `r_name` is NULL for part-1's five rows.
fn write_schema_evolution_files(dir: &Path) {
    write_region_range(dir, "part-0", 0);
    let key: ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 7, 8, 9]));
    let schema = Schema::new(vec![Field::new("r_regionkey", ArrowDataType::Int32, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![key]).expect("key batch");
    let file = File::create(dir.join("part-1.parquet")).expect("create part-1");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write part-1");
    writer.close().expect("close writer");
}

/// A parquet-source `Config` with one shape parameter under `key` (a directory
/// with just that source).
fn parquet_shape_config(name: &str, key: &str, value: Value) -> Config {
    let mut params = BTreeMap::new();
    params.insert(key.to_string(), value);
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

/// Flatten one Int32 column (by index) across batches.
fn int32_values(batches: &[RecordBatch], index: usize) -> Vec<i32> {
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column is Int32");
        for row in 0..batch.num_rows() {
            values.push(column.value(row));
        }
    }
    values
}

/// The single i64 count of a one-row result.
fn count_value(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

#[test]
fn a_glob_source_exposes_one_table_over_every_match() {
    let dir = temp_parquet_dir("glob");
    write_two_region_files(&dir);
    let pattern = dir.join("part-*.parquet");
    let config = parquet_shape_config(
        "ge",
        "glob",
        Value::String(pattern.to_string_lossy().to_string()),
    );
    let runtime = Runtime::from_config(&config).expect("from_config");

    // One table named after the datasource, ten rows across the two files.
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM ge")
        .expect("count");
    assert_eq!(count_value(&batches), 10);

    // A pushed filter + ORDER BY runs across both files.
    let (_, batches) = runtime
        .execute("SELECT r_regionkey FROM ge WHERE r_regionkey >= 8 ORDER BY r_regionkey")
        .expect("filtered");
    assert_eq!(int32_values(&batches, 0), vec![8, 9]);
}

#[test]
fn a_files_list_source_exposes_one_table() {
    let dir = temp_parquet_dir("files");
    write_two_region_files(&dir);
    let list = vec![
        Value::String(dir.join("part-0.parquet").to_string_lossy().to_string()),
        Value::String(dir.join("part-1.parquet").to_string_lossy().to_string()),
    ];
    let runtime = Runtime::from_config(&parquet_shape_config("fl", "files", Value::Sequence(list)))
        .expect("from_config");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM fl")
        .expect("count");
    assert_eq!(count_value(&batches), 10);
}

#[test]
fn a_manifest_source_exposes_one_table() {
    let dir = temp_parquet_dir("manifest");
    write_two_region_files(&dir);
    let manifest = dir.join("files.txt");
    std::fs::write(&manifest, "# files\npart-0.parquet\npart-1.parquet\n").expect("manifest");
    let config = parquet_shape_config(
        "mf",
        "manifest",
        Value::String(manifest.to_string_lossy().to_string()),
    );
    let runtime = Runtime::from_config(&config).expect("from_config");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM mf")
        .expect("count");
    assert_eq!(count_value(&batches), 10);
}

#[test]
fn a_column_absent_from_one_file_reads_as_null() {
    let dir = temp_parquet_dir("union_null");
    write_schema_evolution_files(&dir);
    let pattern = dir.join("part-*.parquet");
    let config = parquet_shape_config(
        "se",
        "glob",
        Value::String(pattern.to_string_lossy().to_string()),
    );
    let runtime = Runtime::from_config(&config).expect("from_config");

    // part-0 has r_name, part-1 does not: its five rows read as NULL.
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM se WHERE r_name IS NULL")
        .expect("null count");
    assert_eq!(count_value(&batches), 5);
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM se WHERE r_name IS NOT NULL")
        .expect("not-null count");
    assert_eq!(count_value(&batches), 5);
}

#[test]
fn zero_or_two_parquet_shapes_raise() {
    let dir = temp_parquet_dir("shape_err");
    write_region_parquet(&dir);

    // Two shapes: `dir` and `glob` together.
    let mut params = parquet_params(dir.to_str().unwrap());
    params.insert(
        "glob".to_string(),
        Value::String(dir.join("*.parquet").to_string_lossy().to_string()),
    );
    let mut sources = BTreeMap::new();
    sources.insert(
        "two".to_string(),
        DataSourceConfig {
            name: "two".to_string(),
            ty: "parquet".to_string(),
            config: params,
            capabilities: Vec::new(),
            change_keys: BTreeMap::new(),
        },
    );
    let error = Runtime::from_config(&base_config(sources))
        .err()
        .expect("two shapes must raise");
    assert!(error.to_string().contains("exactly one"), "{error}");

    // Zero shapes: an empty parquet block.
    let empty = parquet_shape_config("zero", "dir", Value::String(String::new()));
    let mut config = empty;
    if let Some(source) = config.datasources.get_mut("zero") {
        source.config.clear();
    }
    let error = Runtime::from_config(&config)
        .err()
        .expect("zero shapes must raise");
    assert!(error.to_string().contains("exactly one"), "{error}");
}

#[test]
fn ddl_registers_parquet_sources_by_glob_manifest_and_files() {
    let dir = temp_parquet_dir("ddl_multi");
    write_two_region_files(&dir);
    let manifest = dir.join("m.txt");
    std::fs::write(&manifest, "part-0.parquet\npart-1.parquet\n").expect("manifest");

    // A config with a source path so dynamic sources can persist.
    let mut config = base_config(BTreeMap::new());
    config.source_path = Some(dir.join("config.yaml").to_string_lossy().to_string());
    let runtime = Runtime::from_config(&config).expect("from_config");

    let glob_sql = format!(
        "CREATE DATASOURCE dg TYPE parquet WITH (glob = '{}')",
        dir.join("part-*.parquet").display()
    );
    runtime.execute(&glob_sql).expect("create glob");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM dg")
        .expect("count dg");
    assert_eq!(count_value(&batches), 10);

    let manifest_sql = format!(
        "CREATE DATASOURCE dm TYPE parquet WITH (manifest = '{}')",
        manifest.display()
    );
    runtime.execute(&manifest_sql).expect("create manifest");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM dm")
        .expect("count dm");
    assert_eq!(count_value(&batches), 10);

    // The DDL `files` form: a JSON array of paths.
    let json = format!(
        "[\"{}\",\"{}\"]",
        dir.join("part-0.parquet").display(),
        dir.join("part-1.parquet").display()
    );
    let files_sql = format!("CREATE DATASOURCE df TYPE parquet WITH (files = '{json}')");
    runtime.execute(&files_sql).expect("create files json");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS c FROM df")
        .expect("count df");
    assert_eq!(count_value(&batches), 10);
}
