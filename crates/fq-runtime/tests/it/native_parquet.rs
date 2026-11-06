//! Exercise native JSON validation and bounded file manifests.

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use fq_runtime::Runtime;
use parquet::arrow::ArrowWriter;
use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

/// Keep each fixture's config, manifest and source under a unique root.
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

/// Provide JSON strings and timestamps for source validation and manifest scans.
fn fixture_batch() -> RecordBatch {
    let timestamps: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
    let encoded: ArrayRef = Arc::new(StringArray::from(vec![
        r#""second""#,
        r#""first""#,
        r#""first""#,
    ]));
    RecordBatch::try_from_iter(vec![("ts", timestamps), ("encoded", encoded)]).unwrap()
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

/// Wire an explicit file manifest into a persisted config.
fn config(root: &Path, files: &[PathBuf]) -> fq_common::Config {
    let mut manifest = String::new();
    for path in files {
        writeln!(manifest, "{}", path.display()).unwrap();
    }
    std::fs::write(root.join("files.txt"), manifest).unwrap();
    let yaml = format!(
        "datasources:\n  raw:\n    type: parquet\n    manifest: '{}'\n",
        root.join("files.txt").display()
    );
    let path = root.join("config.yaml");
    std::fs::write(&path, yaml).unwrap();
    fq_common::load_config(path.to_str().unwrap()).unwrap()
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
