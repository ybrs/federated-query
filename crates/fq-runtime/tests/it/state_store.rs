//! The engine state store as a fatal dependency: a runtime whose
//! `<config-stem>.stats.sqlite` is truncated, deleted or replaced underneath it
//! raises ONCE, naming the file, and then refuses every later call with that
//! same error instead of serving work it has no state for.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_runtime::{Runtime, RuntimeError};
use parquet::arrow::ArrowWriter;
use serde_yaml::Value;

/// A fresh temp sandbox for one test's parquet fixture and engine state.
fn sandbox(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("fq_state_store_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create sandbox dir");
    dir
}

/// Write the one-column fixture the queries in this file read.
fn write_fixture(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "country",
        ArrowDataType::Utf8,
        false,
    )]));
    let columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["US", "DE"]))];
    let batch = RecordBatch::try_new(schema, columns).expect("fixture batch");
    let file = File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// A config over the fixture with a source path, so the runtime has a state
/// store beside it (that store is what these tests break).
fn config_for(dir: &Path, file: &Path) -> Config {
    let mut params = BTreeMap::new();
    params.insert(
        "file".to_string(),
        Value::String(file.to_string_lossy().to_string()),
    );
    let mut datasources = BTreeMap::new();
    datasources.insert(
        "src".to_string(),
        DataSourceConfig {
            name: "src".to_string(),
            ty: "parquet".to_string(),
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
        diagnostics: fq_common::DiagnosticsConfig::default(),
        source_path: Some(dir.join("config.yaml").to_string_lossy().to_string()),
    }
}

/// The state store path a config derives: `<config-stem>.stats.sqlite`.
fn store_path(dir: &Path) -> PathBuf {
    dir.join("config.stats.sqlite")
}

/// Build a runtime over a fresh sandbox and prove it serves a query, returning
/// the sandbox, the runtime, and the state store it opened.
fn live_runtime(tag: &str) -> (PathBuf, Runtime, PathBuf) {
    let dir = sandbox(tag);
    let fixture = dir.join("fixture.parquet");
    write_fixture(&fixture);
    let runtime = Runtime::from_config(&config_for(&dir, &fixture)).expect("from_config");
    runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect("a healthy runtime serves the query");
    let store = store_path(&dir);
    assert!(store.exists(), "the runtime opened {}", store.display());
    (dir, runtime, store)
}

/// Assert an error is the fatal store loss, naming the store and saying so.
fn assert_fatal(error: &RuntimeError, store: &Path) {
    assert!(
        matches!(error, RuntimeError::StateStoreLost(_)),
        "expected the fatal store loss, got: {error}"
    );
    let message = error.to_string();
    assert!(
        message.starts_with("FATAL:"),
        "the message says it is fatal: {message}"
    );
    assert!(
        message.contains(&store.display().to_string()),
        "the message names the store: {message}"
    );
}

#[test]
fn a_store_overwritten_mid_life_is_fatal_and_poisons_the_runtime() {
    let (_dir, runtime, store) = live_runtime("overwritten");
    let intact = std::fs::read(&store).expect("read the intact store");

    // The file is overwritten in place with content that is not a database -
    // the same inode, so only the lost SQLite header reveals it.
    File::create(&store)
        .expect("open the store for overwrite")
        .write_all(&[b'x'; 4096])
        .expect("overwrite the store");

    let first = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("the query is refused");
    assert_fatal(&first, &store);

    // Putting the bytes back does NOT revive the runtime: everything written
    // while the store was gone is lost, so the runtime stays poisoned and the
    // next call fails with the identical error rather than succeeding.
    std::fs::write(&store, &intact).expect("restore the store bytes");
    let second = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("and so is the next one");
    assert_fatal(&second, &store);
    assert_eq!(
        first.to_string(),
        second.to_string(),
        "the runtime keeps reporting the one fatal loss"
    );

    // Every entry point refuses, not just execute.
    let described = runtime
        .describe("SELECT count(*) AS n FROM src.main.src")
        .expect_err("describe refuses too");
    assert_fatal(&described, &store);
    let principal = runtime
        .set_principal("someone")
        .expect_err("set_principal refuses too");
    assert_fatal(&principal, &store);
}

#[test]
fn a_truncated_store_is_fatal() {
    let (_dir, runtime, store) = live_runtime("truncated");
    // Truncation to nothing: too short to hold a SQLite header.
    File::create(&store).expect("truncate the store");

    let error = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("the query is refused");
    assert_fatal(&error, &store);
}

#[test]
fn a_deleted_store_is_fatal() {
    let (_dir, runtime, store) = live_runtime("deleted");
    // The runtime's open connections keep the unlinked inode alive, so SQLite
    // itself would never notice this.
    std::fs::remove_file(&store).expect("delete the store");

    let error = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("the query is refused");
    assert_fatal(&error, &store);
}

#[test]
fn a_store_replaced_by_another_database_is_fatal() {
    let (dir, runtime, store) = live_runtime("replaced");
    // A byte-identical copy moved into place: a perfectly valid SQLite database
    // at the same path, but a different file, so the runtime's open connections
    // no longer address what the path names.
    let replacement = dir.join("replacement.sqlite");
    std::fs::copy(&store, &replacement).expect("copy the store");
    std::fs::remove_file(&store).expect("remove the original");
    std::fs::rename(&replacement, &store).expect("move the copy into place");

    let error = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("the query is refused");
    assert_fatal(&error, &store);
}

#[test]
fn a_fresh_config_with_no_store_yet_still_works() {
    // A MISSING store at open time means "create it" - the ordinary first run
    // of a new config, and not a loss.
    let dir = sandbox("fresh");
    let fixture = dir.join("fixture.parquet");
    write_fixture(&fixture);
    let store = store_path(&dir);
    assert!(!store.exists(), "no store exists before the first runtime");

    let runtime = Runtime::from_config(&config_for(&dir, &fixture))
        .expect("a config with no store yet builds");
    let (_, batches) = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect("and serves queries");
    assert_eq!(batches[0].num_rows(), 1);
    assert!(
        store.exists(),
        "the store was created at {}",
        store.display()
    );

    // A second runtime over the same config reopens the store it created.
    let reopened = Runtime::from_config(&config_for(&dir, &fixture)).expect("reopen");
    reopened
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect("the reopened runtime serves queries");
}

#[test]
fn a_failed_query_is_not_a_fatal_store_loss() {
    let (_dir, runtime, store) = live_runtime("distinguishable");

    // An ordinary invalid query raises its own error and leaves the runtime
    // serving: a caller can tell "this query failed" from "this is finished".
    let query_error = runtime
        .execute("SELECT count(*) FROM src.main.no_such_table")
        .expect_err("an unknown table raises");
    assert!(
        !matches!(query_error, RuntimeError::StateStoreLost(_)),
        "an invalid query is not a store loss: {query_error}"
    );
    assert!(
        !query_error.to_string().starts_with("FATAL:"),
        "an invalid query does not claim to be fatal: {query_error}"
    );
    runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect("the runtime still serves after a failed query");

    // The same runtime, once its store is gone, reports the fatal loss.
    std::fs::remove_file(&store).expect("delete the store");
    let fatal = runtime
        .execute("SELECT count(*) AS n FROM src.main.src")
        .expect_err("the query is refused");
    assert_fatal(&fatal, &store);
}
