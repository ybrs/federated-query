//! Exercise native JSON projections, durable SELECT builds and bounded file manifests.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};
use fq_runtime::Runtime;
use parquet::arrow::ArrowWriter;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

/// Keep each fixture's config, manifest, source and event store under a unique root.
fn sandbox() -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "fq_native_ingestion_{}_{}",
        std::process::id(),
        nonce
    ));
    std::fs::create_dir(&root).unwrap();
    root
}

/// Represent two tied source events in reverse UUID order and one independent actor.
fn fixture_batch() -> RecordBatch {
    let mut columns = actor_columns();
    columns.extend(order_columns());
    columns.push((
        "encoded",
        Arc::new(StringArray::from(vec![
            r#""second""#,
            r#""first""#,
            r#""first""#,
        ])),
    ));
    let fields: Vec<_> = columns
        .iter()
        .map(|(name, column)| {
            Field::new(*name, column.data_type().clone(), column.null_count() > 0)
        })
        .collect();
    let arrays = columns.into_iter().map(|(_, column)| column).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// Include a missing user id to exercise the device fallback.
fn actor_columns() -> Vec<(&'static str, ArrayRef)> {
    vec![
        ("actor", Arc::new(StringArray::from(vec!["a", "a", "b"]))),
        (
            "user_id",
            Arc::new(StringArray::from(vec![Some("a"), Some("a"), None])),
        ),
        (
            "device_id",
            Arc::new(StringArray::from(vec!["device-a", "device-a", "b"])),
        ),
    ]
}

/// Keep source counters tied while the UUIDs discriminate the equal timestamps.
fn order_columns() -> Vec<(&'static str, ArrayRef)> {
    vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![1_000_000, 1_000_000, 2_000_000])),
        ),
        ("event_id", Arc::new(Int64Array::from(vec![22, 22, 1]))),
        ("uuid", Arc::new(StringArray::from(vec!["b", "a", "c"]))),
    ]
}

/// Write one physical Parquet file with the exact fixture schema.
fn write_source(root: &Path) -> PathBuf {
    let path = root.join("source.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let batch = fixture_batch();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    path
}

/// Wire an explicit file manifest using a small event build budget.
fn config(root: &Path, files: &[PathBuf]) -> fq_common::Config {
    let mut manifest = String::new();
    for path in files {
        writeln!(manifest, "{}", path.display()).unwrap();
    }
    std::fs::write(root.join("files.txt"), manifest).unwrap();
    let yaml = format!("datasources:\n  raw:\n    type: parquet\n    manifest: '{}'\nevents:\n  build_memory_bytes: 1048576\n  build_threads: 2\n", root.join("files.txt").display());
    let path = root.join("config.yaml");
    std::fs::write(&path, yaml).unwrap();
    fq_common::load_config(path.to_str().unwrap()).unwrap()
}

/// Decode event names and let the builder order tied timestamps by source counter then UUID.
fn create_sql() -> &'static str {
    "CREATE EVENT DATASET native_events AS (SELECT COALESCE(r.user_id,r.device_id) AS actor_id, r.ts AS event_time, json_extract_string(r.encoded, '$') AS event_name, r.event_id AS event_id, r.uuid AS uuid FROM raw.main.raw AS r) ACTOR actor_id TIME event_time EVENT event_name TIEBREAK (event_id, uuid) PROPERTIES () WITH (time_unit='us')"
}

/// Require ordinary SQL CREATE, reopen, REBUILD and indexed paths to preserve ordering.
#[test]
fn native_select_build_reopens_and_rebuilds_with_uuid_fallback() {
    let root = sandbox();
    let source = write_source(&root);
    let config = config(&root, &[source]);
    let runtime = Runtime::from_config(&config).unwrap();
    runtime.execute(create_sql()).unwrap();
    assert_path_order(&runtime);
    drop(runtime);
    let reopened = Runtime::from_config(&config).unwrap();
    assert_path_order(&reopened);
    reopened
        .execute("REBUILD EVENT DATASET native_events")
        .unwrap();
    assert_path_order(&reopened);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

/// The lower UUID event must precede the higher UUID despite reversed input row order.
fn assert_path_order(runtime: &Runtime) {
    let (_, batches) = runtime
        .execute("PATHS ON native_events STARTING AT 'first' DEPTH 2 TOP 5")
        .unwrap();
    let text = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(text.contains("second"), "{text}");
    let (_, batches) = runtime.execute("SHOW EVENT DATASETS").unwrap();
    let counts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 3);
}

/// A source counter collision must remain a loud failure when no UUID fallback is declared.
#[test]
fn source_event_id_alone_rejects_ambiguous_order() {
    let root = sandbox();
    let source = write_source(&root);
    let runtime = Runtime::from_config(&config(&root, &[source])).unwrap();
    let sql = "CREATE EVENT DATASET ambiguous FROM raw.main.raw ACTOR actor TIME ts EVENT encoded TIEBREAK event_id PROPERTIES () WITH (time_unit='us')";
    assert!(runtime
        .execute(sql)
        .unwrap_err()
        .to_string()
        .contains("equal tiebreak"));
    let (_, rows) = runtime.execute("SHOW EVENT DATASETS").unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Invalid JSON paths and arity must raise even when no input row survives a filter.
#[test]
fn invalid_json_queries_raise_before_an_empty_result() {
    let root = sandbox();
    let source = write_source(&root);
    let runtime = Runtime::from_config(&config(&root, &[source])).unwrap();
    for projection in [
        "json_extract_string(r.encoded,'$[')",
        "json_extract_string(r.encoded)",
        "json_extract_string(r.encoded, ARRAY['$',NULL])",
        "json_extract_string(r.ts,'$')",
    ] {
        assert!(runtime
            .execute(&format!(
                "SELECT {projection} FROM raw.main.raw AS r WHERE r.ts<0"
            ))
            .is_err());
    }
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

/// Exercise the actual CLI with more file paths than its inherited descriptor allowance.
#[test]
fn manifests_work_under_a_small_descriptor_limit() {
    let root = sandbox();
    let source = write_source(&root);
    let mut files = Vec::new();
    for index in 0..256 {
        let path = root.join(format!("part{index}.parquet"));
        std::fs::hard_link(&source, &path).unwrap();
        files.push(path);
    }
    config(&root, &files);
    assert_limited_cli(&root);
    std::fs::remove_dir_all(root).unwrap();
}

/// An unbuilt CLI raises, and descriptor exhaustion cannot be mistaken for a passed check.
fn assert_limited_cli(root: &Path) {
    let output = limited_cli(root).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(rows[0]["rows"], 768);
}

/// Locate the release artifact that the complete suite builds before its tests.
fn cli_binary() -> PathBuf {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let binary = workspace.join("target/release/fedq");
    assert!(
        binary.is_file(),
        "build the release CLI before running the suite"
    );
    binary
}

/// Apply the descriptor limit only to the child, leaving the suite's other tests alone.
fn limited_cli(root: &Path) -> Command {
    let mut command = Command::new("bash");
    command.args(["-c", "ulimit -n 64; exec \"$@\"", "native-manifest-check"]);
    command.arg(cli_binary());
    command.arg("--config").arg(root.join("config.yaml"));
    command.args([
        "--command",
        "SELECT count(*) AS rows FROM raw.main.raw",
        "--format",
        "json",
    ]);
    command
}
