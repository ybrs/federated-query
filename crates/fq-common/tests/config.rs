//! Translation of `tests/test_config.py` (243 lines) into the fq-common crate.
//!
//! Every Python test is carried over. Two Rust-only additions pin the loudness
//! that `test_state_model.py` used to own on the Python side: an unknown key in
//! the optimizer section must raise (the `deny_unknown_fields` analogue of
//! `extra="forbid"`).

use std::path::{Path, PathBuf};

use fq_common::{load_config, ConfigError};

/// Write `contents` to a uniquely named temp file and return its path. Distinct
/// names per test keep parallel test threads from colliding (Rust std has no
/// NamedTemporaryFile).
fn write_temp(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("fq_common_test_{name}.yaml"));
    std::fs::write(&path, contents).expect("write temp config");
    path
}

/// Path to the repository's example config, relative to this crate's manifest.
fn example_config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/example_config.yaml")
}

#[test]
// Exact f64 compare is intended: the YAML literal and the expected literal are
// the same value, so they are bit-identical (matches the Python test's `== 0.01`).
#[allow(clippy::float_cmp)]
fn test_load_example_config() {
    let path = example_config_path();
    if !path.exists() {
        // Mirrors the Python skip when the example config is absent.
        eprintln!("Example config not found; skipping");
        return;
    }
    let config = load_config(path.to_str().unwrap()).expect("load example config");

    // Data sources present.
    assert!(config.datasources.contains_key("postgres_prod"));
    assert!(config.datasources.contains_key("local_duckdb"));

    // PostgreSQL config.
    let pg = &config.datasources["postgres_prod"];
    assert_eq!(pg.ty, "postgresql");
    assert_eq!(pg.config["host"].as_str(), Some("localhost"));
    assert_eq!(pg.config["port"].as_u64(), Some(5432));
    assert_eq!(pg.config["database"].as_str(), Some("analytics"));

    // DuckDB config.
    let duck = &config.datasources["local_duckdb"];
    assert_eq!(duck.ty, "duckdb");
    assert!(duck.config.contains_key("path"));

    // Optimizer config.
    assert!(config.optimizer.enable_predicate_pushdown);
    assert!(config.optimizer.enable_projection_pushdown);
    assert!(config.optimizer.enable_join_reordering);
    assert_eq!(config.optimizer.max_join_reorder_size, 10);
    assert_eq!(config.optimizer.planning_budget_ms, 100);

    // Executor config.
    assert_eq!(config.executor.max_memory_mb, 2048);
    assert_eq!(config.executor.batch_size, 10000);
    assert_eq!(config.executor.max_threads, 8);

    // Cost config.
    assert_eq!(config.cost.cpu_tuple_cost, 0.01);
    assert_eq!(config.cost.io_page_cost, 1.0);
}

#[test]
// Exact f64 compare is intended (see test_load_example_config).
#[allow(clippy::float_cmp)]
fn test_load_minimal_config() {
    let minimal = "
datasources:
  test_pg:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test
";
    let path = write_temp("minimal", minimal);
    let config = load_config(path.to_str().unwrap()).expect("load minimal config");

    assert!(config.datasources.contains_key("test_pg"));
    assert_eq!(config.datasources["test_pg"].ty, "postgresql");

    // Defaults applied.
    assert!(config.optimizer.enable_predicate_pushdown);
    assert_eq!(config.executor.max_memory_mb, 1024);
    assert_eq!(config.cost.cpu_tuple_cost, 0.01);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_missing_config_file() {
    let result = load_config("nonexistent_config.yaml");
    assert!(matches!(result, Err(ConfigError::FileNotFound(_))));
}

#[test]
fn test_unknown_top_level_section_raises() {
    // A misspelled top-level section must raise, not be silently ignored.
    let config_yaml = "
datasources:
  test_pg:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test
optimizr:
  enable_predicate_pushdown: false
";
    let path = write_temp("unknown_section", config_yaml);
    let error = load_config(path.to_str().unwrap()).expect_err("must raise");
    match error {
        ConfigError::UnknownSection(message) => assert!(message.contains("optimizr")),
        other => panic!("expected UnknownSection, got {other:?}"),
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn test_config_with_capabilities() {
    let config_yaml = "
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test
    capabilities:
      - aggregations
      - joins
      - window_functions
";
    let path = write_temp("capabilities", config_yaml);
    let config = load_config(path.to_str().unwrap()).expect("load config");
    let ds = &config.datasources["test_db"];

    assert!(ds.capabilities.contains(&"aggregations".to_string()));
    assert!(ds.capabilities.contains(&"joins".to_string()));
    assert!(ds.capabilities.contains(&"window_functions".to_string()));

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_multiple_datasources() {
    let config_yaml = "
datasources:
  pg1:
    type: postgresql
    host: host1
    database: db1
    user: user1
    password: pass1

  pg2:
    type: postgresql
    host: host2
    database: db2
    user: user2
    password: pass2

  duck1:
    type: duckdb
    path: /path/to/db.duckdb
";
    let path = write_temp("multiple", config_yaml);
    let config = load_config(path.to_str().unwrap()).expect("load config");

    assert_eq!(config.datasources.len(), 3);
    assert!(config.datasources.contains_key("pg1"));
    assert!(config.datasources.contains_key("pg2"));
    assert!(config.datasources.contains_key("duck1"));

    assert_eq!(config.datasources["pg1"].ty, "postgresql");
    assert_eq!(config.datasources["pg2"].ty, "postgresql");
    assert_eq!(config.datasources["duck1"].ty, "duckdb");

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_optimizer_config_override() {
    let config_yaml = "
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test

optimizer:
  enable_predicate_pushdown: false
  enable_join_reordering: false
  max_join_reorder_size: 20
  planning_budget_ms: 250
";
    let path = write_temp("optimizer_override", config_yaml);
    let config = load_config(path.to_str().unwrap()).expect("load config");

    assert!(!config.optimizer.enable_predicate_pushdown);
    assert!(!config.optimizer.enable_join_reordering);
    assert_eq!(config.optimizer.max_join_reorder_size, 20);
    assert_eq!(config.optimizer.planning_budget_ms, 250);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_executor_config_override() {
    let config_yaml = "
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test

executor:
  max_memory_mb: 4096
  batch_size: 50000
  max_threads: 16
";
    let path = write_temp("executor_override", config_yaml);
    let config = load_config(path.to_str().unwrap()).expect("load config");

    assert_eq!(config.executor.max_memory_mb, 4096);
    assert_eq!(config.executor.batch_size, 50000);
    assert_eq!(config.executor.max_threads, 16);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_unknown_optimizer_key_raises() {
    // Rust-only pin (replaces test_state_model.py's extra="forbid" loudness):
    // an unknown key inside a section must raise via serde deny_unknown_fields,
    // never be silently dropped.
    let config_yaml = "
datasources:
  test_db:
    type: postgresql
    host: localhost
    database: test
    user: test
    password: test

optimizer:
  enable_predicate_pushdown: true
  not_a_real_flag: true
";
    let path = write_temp("unknown_optimizer_key", config_yaml);
    let result = load_config(path.to_str().unwrap());
    assert!(result.is_err(), "unknown optimizer key must raise");
    std::fs::remove_file(&path).ok();
}

#[test]
fn test_source_path_recorded() {
    // Rust-only pin: the loaded config records the YAML path so the learned-stats
    // catalog can default next to it (config.py sets source_path).
    let path = write_temp("source_path", "datasources: {}\n");
    let config = load_config(path.to_str().unwrap()).expect("load config");
    assert_eq!(config.source_path.as_deref(), Some(path.to_str().unwrap()));
    std::fs::remove_file(&path).ok();
}
