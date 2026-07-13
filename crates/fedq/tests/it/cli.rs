//! End-to-end: run the compiled fedq binary one-shot against a tiny seeded
//! DuckDB config and assert the rendered stdout and the process exit status.
//!
//! A valid query prints the requested format (table, csv, json) and exits 0; an
//! invalid query (a column the table does not have) prints an error to stderr
//! and exits nonzero. The binary is located through `CARGO_BIN_EXE_fedq`, which
//! cargo sets to the freshly built binary for this crate's integration tests.

use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};

use fq_connectors::DuckDbSource;

/// The seed table the queries read: three fruit rows with an integer id and a
/// string name.
const SEED_SQL: &str = "\
    CREATE TABLE items (id INTEGER, name VARCHAR); \
    INSERT INTO items VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry');";

/// A unique temp path per call, so parallel tests never share a DuckDB file or a
/// config file.
fn temp_path(suffix: &str) -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir()
        .join(format!("fedq_cli_{pid}_{id}_{suffix}"))
        .to_str()
        .expect("temp path is valid UTF-8")
        .to_owned()
}

/// Seed a fresh DuckDB fixture and write a config pointing at it under the
/// datasource name `shop`, returning the config file path. The seeding handle is
/// closed before the binary opens the file.
fn seeded_config() -> String {
    let db_path = temp_path("db.duckdb");
    let _ = std::fs::remove_file(&db_path);
    let source = DuckDbSource::open("seed", &db_path).expect("open seed duckdb");
    source.execute_batch(SEED_SQL).expect("seed items");
    drop(source);
    let config_path = temp_path("config.yaml");
    let config = format!("datasources:\n  shop:\n    type: duckdb\n    path: {db_path}\n");
    std::fs::write(&config_path, config).expect("write config");
    config_path
}

/// Run the fedq binary one-shot with `--command sql` in `format`, returning
/// (success, stdout, stderr).
fn run_command(config: &str, sql: &str, format: &str) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_fedq"))
        .args(["--config", config, "--format", format, "--command", sql])
        .output()
        .expect("run fedq binary");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf-8"),
        String::from_utf8(output.stderr).expect("stderr is utf-8"),
    )
}

#[test]
fn table_format_prints_aligned_rows() {
    let config = seeded_config();
    let (ok, stdout, stderr) = run_command(
        &config,
        "SELECT id, name FROM shop.main.items ORDER BY id",
        "table",
    );
    assert!(ok, "expected success, stderr:\n{stderr}");
    assert!(
        stdout.contains("id") && stdout.contains("name"),
        "header:\n{stdout}"
    );
    assert!(stdout.contains("apple"), "row apple missing:\n{stdout}");
    assert!(stdout.contains("cherry"), "row cherry missing:\n{stdout}");
    // The aligned grid draws bordered rows.
    assert!(stdout.contains("+---"), "no grid border:\n{stdout}");
}

#[test]
fn csv_format_prints_header_and_rows() {
    let config = seeded_config();
    let (ok, stdout, stderr) = run_command(
        &config,
        "SELECT id, name FROM shop.main.items ORDER BY id",
        "csv",
    );
    assert!(ok, "expected success, stderr:\n{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "id,name");
    assert_eq!(lines[1], "1,apple");
    assert_eq!(lines[2], "2,banana");
    assert_eq!(lines[3], "3,cherry");
}

#[test]
fn json_format_prints_typed_rows() {
    let config = seeded_config();
    let (ok, stdout, stderr) = run_command(
        &config,
        "SELECT id, name FROM shop.main.items WHERE id = 1",
        "json",
    );
    assert!(ok, "expected success, stderr:\n{stderr}");
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid json");
    let rows = parsed.as_array().expect("array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], serde_json::Value::from(1));
    assert_eq!(rows[0]["name"], serde_json::Value::from("apple"));
}

#[test]
fn invalid_query_exits_nonzero_with_stderr() {
    let config = seeded_config();
    let (ok, stdout, stderr) = run_command(
        &config,
        "SELECT no_such_column FROM shop.main.items",
        "table",
    );
    assert!(!ok, "invalid query must fail; stdout:\n{stdout}");
    assert!(!stderr.is_empty(), "an error must be reported to stderr");
    assert!(
        stdout.is_empty(),
        "no rows should print for an invalid query:\n{stdout}"
    );
}

#[test]
fn missing_config_exits_nonzero() {
    let (ok, _stdout, stderr) = run_command("/nonexistent/fedq-config.yaml", "SELECT 1", "table");
    assert!(!ok, "a missing config must fail");
    assert!(
        !stderr.is_empty(),
        "the config failure must be reported to stderr"
    );
}
