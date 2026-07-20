//! Runtime datasource DDL end to end over real DuckDB sources: CREATE validates
//! and persists a source and installs it into the session; a query reads it;
//! SHOW DATASOURCES lists it with its params fail-closed redacted; DROP removes
//! it (a later reference raises); a NEW runtime over the same config sees a
//! persisted source; a YAML/persisted name collision aborts construction; a
//! concurrent create of one name makes the loser raise; a persisted source that
//! no longer connects is scoped-unavailable (an unrelated query works, a
//! reference raises the stored error).
//!
//! Every test builds its own temp config dir and uniquely named DuckDB files, so
//! parallel tests never share a store or clobber the process-wide exec-plane
//! registry.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, RecordBatch, StringArray};
use fq_accel::{Accelerator, DynamicDatasource};
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// A per-test sandbox: a temp dir that holds seeded DuckDB files and the config
/// path the persisted dynamic-datasource store hangs off.
struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    /// Create a fresh empty sandbox dir under a unique temp path.
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_dyn_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        Self { dir }
    }

    /// Seed a DuckDB file holding one two-column table with `rows` and return
    /// its absolute path. The file is closed before it is returned so the
    /// runtime can take the single read-write instance.
    fn seed_duck(&self, file: &str, table: &str, rows: &[(i32, &str)]) -> String {
        let path = self.dir.join(file);
        let path_str = path.to_string_lossy().to_string();
        let source = DuckDbSource::open("seed", &path_str).expect("open seed duckdb");
        source
            .execute_batch(&format!("CREATE TABLE {table} (id INTEGER, name VARCHAR)"))
            .expect("create table");
        for (id, name) in rows {
            source
                .execute_batch(&format!("INSERT INTO {table} VALUES ({id}, '{name}')"))
                .expect("insert row");
        }
        drop(source);
        path_str
    }

    /// A config with the given bootstrap datasources plus a `source_path` inside
    /// the sandbox (which gives the runtime a persisted datasource store).
    fn config(&self, datasources: BTreeMap<String, DataSourceConfig>) -> Config {
        Config {
            datasources,
            optimizer: OptimizerConfig::default(),
            executor: ExecutorConfig::default(),
            cost: CostConfig::default(),
            server: ServerConfig::default(),
            accelerator: fq_common::AcceleratorConfig::default(),
            catalog: fq_common::CatalogConfig::default(),
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over a config with no bootstrap datasources.
    fn empty_runtime(&self) -> Runtime {
        Runtime::from_config(&self.config(BTreeMap::new())).expect("from_config")
    }

    /// The accelerator over this sandbox's store, for asserting/seeding the
    /// persisted dynamic-datasource table directly.
    fn accelerator(&self) -> Accelerator {
        Accelerator::open(&self.dir.join("config.yaml")).expect("open accelerator")
    }
}

/// A DuckDB bootstrap `DataSourceConfig` reading `path`.
fn duck_bootstrap(name: &str, path: &str) -> DataSourceConfig {
    let mut config = BTreeMap::new();
    config.insert("path".to_string(), Value::String(path.to_string()));
    DataSourceConfig {
        name: name.to_string(),
        ty: "duckdb".to_string(),
        config,
        capabilities: Vec::new(),
        change_keys: BTreeMap::new(),
    }
}

/// Flatten a single Utf8 result column in batch order.
fn string_column(batches: &[RecordBatch], column: usize) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column is Utf8");
        for row in 0..batch.num_rows() {
            out.push(array.value(row).to_string());
        }
    }
    out
}

/// Whether any cell of a result set contains `needle`.
fn any_cell_contains(batches: &[RecordBatch], needle: &str) -> bool {
    for batch in batches {
        for column in 0..batch.num_columns() {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("cell is Utf8");
            for row in 0..batch.num_rows() {
                if array.value(row).contains(needle) {
                    return true;
                }
            }
        }
    }
    false
}

#[test]
fn create_query_show_drop_lifecycle() {
    let sandbox = Sandbox::new("lifecycle");
    let path = sandbox.seed_duck("live.duckdb", "t", &[(1, "a"), (2, "b")]);
    let runtime = sandbox.empty_runtime();

    runtime
        .execute(&format!(
            "CREATE DATASOURCE live TYPE duckdb WITH (path = '{path}')"
        ))
        .expect("create datasource");

    let (_schema, rows) = runtime
        .execute("SELECT name FROM live.main.t ORDER BY id")
        .expect("query the new source");
    assert_eq!(
        string_column(&rows, 0),
        vec!["a".to_string(), "b".to_string()]
    );

    let (_schema, shown) = runtime.execute("SHOW DATASOURCES").expect("show");
    assert_eq!(string_column(&shown, 0), vec!["live".to_string()]);
    assert_eq!(string_column(&shown, 2), vec!["dynamic".to_string()]);
    assert!(string_column(&shown, 3)[0].contains("path="));

    runtime
        .execute("DROP DATASOURCE live")
        .expect("drop datasource");
    let after = runtime.execute("SELECT name FROM live.main.t");
    assert!(after.is_err(), "a reference after DROP must raise");
}

#[test]
fn new_runtime_sees_persisted_source() {
    let sandbox = Sandbox::new("persist");
    let path = sandbox.seed_duck("p.duckdb", "t", &[(7, "seven")]);
    {
        let creator = sandbox.empty_runtime();
        creator
            .execute(&format!(
                "CREATE DATASOURCE p TYPE duckdb WITH (path = '{path}')"
            ))
            .expect("create datasource");
    }
    // A fresh runtime over the same config picks the source up at from_config.
    let reader = sandbox.empty_runtime();
    let (_schema, rows) = reader
        .execute("SELECT name FROM p.main.t")
        .expect("query persisted source from a new runtime");
    assert_eq!(string_column(&rows, 0), vec!["seven".to_string()]);
}

#[test]
fn bootstrap_collision_raises() {
    let sandbox = Sandbox::new("collision");
    let path = sandbox.seed_duck("c.duckdb", "t", &[(1, "a")]);
    {
        let creator = sandbox.empty_runtime();
        creator
            .execute(&format!(
                "CREATE DATASOURCE dup TYPE duckdb WITH (path = '{path}')"
            ))
            .expect("create datasource");
    }
    // A YAML block now also declares `dup`: the effective source is ambiguous,
    // so construction refuses rather than pick one silently.
    let mut datasources = BTreeMap::new();
    datasources.insert("dup".to_string(), duck_bootstrap("dup", &path));
    let error = Runtime::from_config(&sandbox.config(datasources));
    assert!(error.is_err(), "a YAML/persisted name collision must raise");
}

#[test]
fn concurrent_create_loser_raises() {
    let sandbox = Sandbox::new("race");
    let path = sandbox.seed_duck("r.duckdb", "t", &[(1, "a")]);
    let first = sandbox.empty_runtime();
    let second = sandbox.empty_runtime();
    let create = format!("CREATE DATASOURCE only TYPE duckdb WITH (path = '{path}')");
    first.execute(&create).expect("first create wins");
    // The second runtime validated its own connection, then lost the INSERT OR
    // IGNORE race; it raises rather than silently no-op.
    let loser = second.execute(&create);
    assert!(
        loser.is_err(),
        "the losing create must raise already-exists"
    );
}

#[test]
fn drop_bootstrap_source_raises() {
    let sandbox = Sandbox::new("dropboot");
    let path = sandbox.seed_duck("b.duckdb", "t", &[(1, "a")]);
    let mut datasources = BTreeMap::new();
    datasources.insert("boot".to_string(), duck_bootstrap("boot", &path));
    let runtime = Runtime::from_config(&sandbox.config(datasources)).expect("from_config");
    let error = runtime.execute("DROP DATASOURCE boot");
    assert!(error.is_err(), "dropping a bootstrap source must raise");
}

#[test]
fn drop_missing_source_raises() {
    let sandbox = Sandbox::new("dropmiss");
    let runtime = sandbox.empty_runtime();
    let error = runtime.execute("DROP DATASOURCE nope");
    assert!(error.is_err(), "dropping an unknown source must raise");
}

#[test]
fn create_validation_failure_persists_nothing() {
    let sandbox = Sandbox::new("badcreate");
    let bad = sandbox.dir.join("does_not_exist.duckdb");
    // Point at a path that is not a DuckDB database: validation fails, so the
    // row must not persist and the source must not install.
    std::fs::write(&bad, b"not a duckdb file").expect("write junk file");
    let runtime = sandbox.empty_runtime();
    let created = runtime.execute(&format!(
        "CREATE DATASOURCE bad TYPE duckdb WITH (path = '{}')",
        bad.to_string_lossy()
    ));
    assert!(created.is_err(), "a source that fails to open must raise");
    assert!(
        sandbox
            .accelerator()
            .list_datasources()
            .expect("list")
            .is_empty(),
        "a failed create persists nothing"
    );
}

#[test]
fn show_redacts_a_persisted_password() {
    let sandbox = Sandbox::new("redact");
    // Seed a persisted postgres row (with a password) directly, so SHOW renders
    // it without connecting. The password must never appear in the output.
    let mut params = BTreeMap::new();
    params.insert("host".to_string(), "db.example".to_string());
    params.insert("port".to_string(), "5432".to_string());
    params.insert("database".to_string(), "sales".to_string());
    params.insert("schemas".to_string(), "public".to_string());
    params.insert("user".to_string(), "sam".to_string());
    params.insert("password".to_string(), "s3cr3t-token".to_string());
    let row = DynamicDatasource {
        name: "warehouse".to_string(),
        kind: "postgres".to_string(),
        params,
        created_at: String::new(),
    };
    assert!(sandbox
        .accelerator()
        .insert_datasource(&row)
        .expect("insert"));

    // The dead host makes the source unavailable at construction, which does not
    // abort the runtime; SHOW still lists it from the persisted table.
    let runtime = sandbox.empty_runtime();
    let (_schema, shown) = runtime.execute("SHOW DATASOURCES").expect("show");
    assert!(
        !any_cell_contains(&shown, "s3cr3t-token"),
        "SHOW must never render a password"
    );
    let summary = &string_column(&shown, 3)[0];
    assert!(summary.contains("host=db.example"));
    assert!(summary.contains("database=sales"));
}

#[test]
fn scoped_unavailable_defers_the_raise_to_the_reference() {
    let sandbox = Sandbox::new("unavail");
    let good = sandbox.seed_duck("good.duckdb", "t", &[(1, "ok")]);
    // A persisted dynamic source whose path is not a DuckDB database: it fails
    // to load at construction and is marked unavailable, not aborting the run.
    let junk = sandbox.dir.join("junk.duckdb");
    std::fs::write(&junk, b"garbage").expect("write junk");
    let mut params = BTreeMap::new();
    params.insert("path".to_string(), junk.to_string_lossy().to_string());
    let dead = DynamicDatasource {
        name: "dead".to_string(),
        kind: "duckdb".to_string(),
        params,
        created_at: String::new(),
    };
    assert!(sandbox
        .accelerator()
        .insert_datasource(&dead)
        .expect("insert"));

    let mut datasources = BTreeMap::new();
    datasources.insert("live".to_string(), duck_bootstrap("live", &good));
    let runtime = Runtime::from_config(&sandbox.config(datasources)).expect("from_config");

    // An unrelated query works: the dead source blocks nothing.
    let (_schema, rows) = runtime
        .execute("SELECT name FROM live.main.t")
        .expect("unrelated query works");
    assert_eq!(string_column(&rows, 0), vec!["ok".to_string()]);

    // A reference to the dead source raises the stored connector error.
    let error = runtime.execute("SELECT id FROM dead.main.t");
    assert!(
        error.is_err(),
        "referencing an unavailable source must raise"
    );
}

#[test]
fn drop_purges_learned_stats() {
    use fq_catalog::StatsCatalog;

    let sandbox = Sandbox::new("purge");
    let path = sandbox.seed_duck("s.duckdb", "t", &[(1, "a")]);
    let runtime = sandbox.empty_runtime();
    runtime
        .execute(&format!(
            "CREATE DATASOURCE learn TYPE duckdb WITH (path = '{path}')"
        ))
        .expect("create datasource");

    // Record a learned row for the source, then assert DROP removes it.
    let stats_path = sandbox.dir.join("config.stats.sqlite");
    let stats = StatsCatalog::open(&stats_path.to_string_lossy()).expect("open stats");
    stats
        .record_table_rows("learn", "main", "t", 42)
        .expect("record");
    assert_eq!(
        stats.table_rows("learn", "main", "t", None).expect("read"),
        Some(42)
    );

    runtime.execute("DROP DATASOURCE learn").expect("drop");
    let reopened = StatsCatalog::open(&stats_path.to_string_lossy()).expect("reopen stats");
    assert_eq!(
        reopened
            .table_rows("learn", "main", "t", None)
            .expect("read"),
        None,
        "DROP purges the source's learned rows"
    );
}
