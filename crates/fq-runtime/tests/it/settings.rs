//! The settings surface end to end through `Runtime::execute`: `SHOW SETTINGS`
//! shape, a `SET`/`RESET` roundtrip that actually changes planning behavior, the
//! static-setting raise, the unknown-name raise, and the source column.

use std::collections::BTreeMap;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// A datasource-free config: enough to build a runtime and run the settings
/// surface and constant `SELECT`s (no source data is needed).
fn empty_config() -> Config {
    Config {
        datasources: BTreeMap::new(),
        optimizer: OptimizerConfig::default(),
        executor: ExecutorConfig::default(),
        cost: CostConfig::default(),
        server: ServerConfig::default(),
        accelerator: fq_common::AcceleratorConfig::default(),
        catalog: fq_common::CatalogConfig::default(),
        source_path: None,
    }
}

/// Downcast a result column to its string values (the SHOW result is all text).
fn text_column(batch: &RecordBatch, index: usize) -> Vec<String> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("SHOW column is Utf8");
    let mut values = Vec::new();
    for row in 0..batch.num_rows() {
        values.push(array.value(row).to_string());
    }
    values
}

/// A SHOW result row keyed by setting name: (value, default, source, mutable).
type ShowRow = (String, String, String, String);

/// Index a SHOW result by setting name into (value, default, source, mutable).
fn show_index(schema: &SchemaRef, batches: &[RecordBatch]) -> BTreeMap<String, ShowRow> {
    assert_eq!(
        column_names(schema),
        vec![
            "name",
            "value",
            "default",
            "source",
            "mutable",
            "description"
        ],
        "SHOW returns the six settings columns"
    );
    let mut index = BTreeMap::new();
    for batch in batches {
        let names = text_column(batch, 0);
        let values = text_column(batch, 1);
        let defaults = text_column(batch, 2);
        let sources = text_column(batch, 3);
        let mutables = text_column(batch, 4);
        for row in 0..batch.num_rows() {
            index.insert(
                names[row].clone(),
                (
                    values[row].clone(),
                    defaults[row].clone(),
                    sources[row].clone(),
                    mutables[row].clone(),
                ),
            );
        }
    }
    index
}

/// The column names of a schema, in order.
fn column_names(schema: &SchemaRef) -> Vec<String> {
    let mut names = Vec::new();
    for field in schema.fields() {
        names.push(field.name().clone());
    }
    names
}

#[test]
fn show_settings_returns_the_six_columns_and_covers_every_area() {
    let runtime = Runtime::from_config(&empty_config()).expect("build runtime");
    let (schema, batches) = runtime.execute("SHOW SETTINGS").expect("show settings");
    let index = show_index(&schema, &batches);

    // A setting from each area is present with the right default and mutability.
    let (value, default, source, mutable) = &index["optimizer.planning_budget_ms"];
    assert_eq!(value, "100");
    assert_eq!(default, "100");
    assert_eq!(source, "default");
    assert_eq!(mutable, "session");

    let (_, _, _, mutable) = &index["exec.parallel_partitions"];
    assert_eq!(mutable, "static", "an exec constant is static");
    let (_, _, _, mutable) = &index["env.dim_shipping"];
    assert_eq!(mutable, "static", "an env flag is static");
    assert!(index.contains_key("cost.network_rtt_ms"));
    assert!(index.contains_key("executor.batch_size"));
    assert!(index.contains_key("server.users"));
}

#[test]
fn show_setting_shows_one_and_unknown_raises_with_a_suggestion() {
    let runtime = Runtime::from_config(&empty_config()).expect("build runtime");
    let (_, batches) = runtime
        .execute("SHOW SETTING optimizer.ship_local_floor")
        .expect("show one setting");
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 1, "SHOW SETTING <name> returns exactly one row");

    let error = runtime
        .execute("SHOW SETTING optimizer.ship_local_flor")
        .expect_err("an unknown setting raises");
    let message = error.to_string();
    assert!(message.contains("unknown setting"), "{message}");
    assert!(
        message.contains("optimizer.ship_local_floor"),
        "suggests the nearest name: {message}"
    );
}

#[test]
fn set_planning_budget_changes_live_planning_and_reset_restores_it() {
    let runtime = Runtime::from_config(&empty_config()).expect("build runtime");
    // A constant SELECT plans fine under the default 100ms budget.
    runtime
        .execute("EXPLAIN SELECT 1")
        .expect("plans under the default budget");

    // Lowering the budget to 0 on the LIVE runtime makes the very next plan blow
    // the budget - proof the SET reached the planner, not just the SHOW table.
    runtime
        .execute("SET optimizer.planning_budget_ms = 0")
        .expect("set the budget");
    let killed = runtime
        .execute("EXPLAIN SELECT 1")
        .expect_err("a zero budget kills the plan");
    assert!(
        killed.to_string().contains("planning budget exceeded"),
        "{killed}"
    );

    // SHOW reflects the change with source = set.
    let (schema, batches) = runtime
        .execute("SHOW SETTING optimizer.planning_budget_ms")
        .expect("show the changed setting");
    let (value, _default, source, _mutable) =
        show_index(&schema, &batches)["optimizer.planning_budget_ms"].clone();
    assert_eq!(value, "0");
    assert_eq!(source, "set");

    // RESET restores the default value AND the source, and planning works again.
    runtime
        .execute("RESET optimizer.planning_budget_ms")
        .expect("reset the budget");
    let (schema, batches) = runtime
        .execute("SHOW SETTING optimizer.planning_budget_ms")
        .expect("show the reset setting");
    let (value, default, source, _mutable) =
        show_index(&schema, &batches)["optimizer.planning_budget_ms"].clone();
    assert_eq!(value, default, "RESET restores the default value");
    assert_eq!(source, "default", "RESET restores the default source");
    runtime
        .execute("EXPLAIN SELECT 1")
        .expect("plans again after RESET");
}

#[test]
fn set_on_a_static_setting_raises() {
    let runtime = Runtime::from_config(&empty_config()).expect("build runtime");
    let error = runtime
        .execute("SET exec.parallel_partitions = 4")
        .expect_err("a static setting cannot be set");
    let message = error.to_string();
    assert!(message.contains("static"), "{message}");
}

#[test]
fn set_type_checks_the_value() {
    let runtime = Runtime::from_config(&empty_config()).expect("build runtime");
    let error = runtime
        .execute("SET optimizer.planning_budget_ms = fast")
        .expect_err("a non-numeric value is rejected");
    assert!(error.to_string().contains("unsigned integer"), "{error}");
    // A negative for an unsigned setting is out of range, not silently wrapped.
    let error = runtime
        .execute("SET optimizer.ship_local_floor = -5")
        .expect_err("a negative value is rejected");
    assert!(error.to_string().contains("unsigned integer"), "{error}");
}

/// A minimal single-column DuckDB fixture for the plan-text behavioral check.
const REGION_SEED: &str = "\
    CREATE TABLE region (r_regionkey INTEGER, r_name VARCHAR); \
    INSERT INTO region VALUES (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA');";

/// Seed a fresh DuckDB file with the region fixture and return a config over it.
fn duck_region_config() -> (Config, std::path::PathBuf) {
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "fq_settings_region_{}_{}.duckdb",
        std::process::id(),
        id
    ));
    let _ = std::fs::remove_file(&path);
    let source = DuckDbSource::open("duck", path.to_str().expect("utf8 path")).expect("open duck");
    source.execute_batch(REGION_SEED).expect("seed region");
    drop(source);

    let mut params = BTreeMap::new();
    params.insert(
        "path".to_string(),
        Value::String(path.display().to_string()),
    );
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
    let mut config = empty_config();
    config.datasources = datasources;
    (config, path)
}

/// The single-column EXPLAIN text of a query, joined into one string.
fn explain_text(runtime: &Runtime, sql: &str) -> String {
    let (_, batches) = runtime.execute(sql).expect("explain");
    let mut lines = Vec::new();
    for batch in &batches {
        for line in text_column(batch, 0) {
            lines.push(line);
        }
    }
    lines.join("\n")
}

#[test]
fn set_predicate_pushdown_changes_the_plan_text() {
    let (config, path) = duck_region_config();
    let runtime = Runtime::from_config(&config).expect("build runtime");
    let query = "EXPLAIN SELECT r_name FROM duck.main.region WHERE r_regionkey > 1";

    let with_pushdown = explain_text(&runtime, query);
    runtime
        .execute("SET optimizer.enable_predicate_pushdown = false")
        .expect("disable predicate pushdown");
    let without_pushdown = explain_text(&runtime, query);
    assert_ne!(
        with_pushdown, without_pushdown,
        "disabling predicate pushdown changes the physical plan"
    );

    // RESET ALL restores every override; the plan text returns to the pushed form.
    runtime.execute("RESET ALL").expect("reset all");
    assert_eq!(explain_text(&runtime, query), with_pushdown);
    let _ = std::fs::remove_file(&path);
}
