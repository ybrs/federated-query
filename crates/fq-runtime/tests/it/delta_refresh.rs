//! Change-key delta refresh end to end over a real DuckDB source: a monotonic
//! declaration turns REFRESH into a delta append (new rows only, existing
//! chunk files untouched), a primary-key declaration into a merge (only
//! affected chunks rewritten), matching source tokens into a no-op that writes
//! nothing, and everything else into a whole re-pull whose status row names
//! the reason. After EVERY refresh path the identity gate holds: reading the
//! view equals running its stored SELECT fresh.
//!
//! Each test builds its own temp config dir and uniquely named DuckDB
//! datasource; the store is inspected through a second `Accelerator` handle on
//! the same config path (SQLite WAL admits concurrent readers).

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use fq_accel::Accelerator;
use fq_common::{
    ChangeKey, Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_exec::connectors;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The seed: an append-style events table and a mutable customers table.
const SEED_SQL: &str = "\
    CREATE TABLE events (id BIGINT, happened_at TIMESTAMP, payload VARCHAR); \
    INSERT INTO events VALUES \
        (1, TIMESTAMP '2026-01-01 08:00:00', 'alpha'), \
        (2, TIMESTAMP '2026-01-02 09:30:00', 'beta'), \
        (3, TIMESTAMP '2026-01-03 10:45:00', 'gamma'); \
    CREATE TABLE customers (c_id BIGINT, c_name VARCHAR); \
    INSERT INTO customers VALUES (1, 'ada'), (2, 'bob'), (3, 'cyd');";

/// A per-test sandbox with change-key declarations on its one DuckDB source.
struct Sandbox {
    dir: PathBuf,
    datasource: String,
    change_keys: BTreeMap<String, ChangeKey>,
}

impl Sandbox {
    /// Seed a fresh sandbox; `change_keys` declares how the tables change.
    fn new(tag: &str, change_keys: &[(&str, ChangeKey)]) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_delta_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        let duck_path = dir.join("data.duckdb");
        let source = DuckDbSource::open("seed", duck_path.to_str().expect("utf-8 path"))
            .expect("open seed duckdb");
        source.execute_batch(SEED_SQL).expect("seed tables");
        drop(source);
        let mut keys = BTreeMap::new();
        for (table, key) in change_keys {
            keys.insert((*table).to_string(), key.clone());
        }
        Self {
            dir,
            datasource: format!("{tag}_duck"),
            change_keys: keys,
        }
    }

    /// The sandbox's config: one DuckDB datasource with the declarations, plus
    /// a `source_path` inside the sandbox dir (which is what gives the runtime
    /// a materialized-view store).
    fn config(&self) -> Config {
        let mut params = BTreeMap::new();
        params.insert(
            "path".to_string(),
            Value::String(self.dir.join("data.duckdb").to_string_lossy().to_string()),
        );
        let mut datasources = BTreeMap::new();
        datasources.insert(
            self.datasource.clone(),
            DataSourceConfig {
                name: self.datasource.clone(),
                ty: "duckdb".to_string(),
                config: params,
                capabilities: Vec::new(),
                change_keys: self.change_keys.clone(),
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
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over this sandbox's config.
    fn runtime(&self) -> Runtime {
        Runtime::from_config(&self.config()).expect("from_config")
    }

    /// Run a mutation through the exec plane's shared DuckDB instance (the
    /// runtime holds the file's single read-write instance). The datasource
    /// registry is session-keyed, so the mutation runs inside the runtime's
    /// session to reach the same instance the runtime reads.
    fn mutate(&self, runtime: &Runtime, sql: &str) {
        let _scope = connectors::SessionScope::enter(runtime.exec_session());
        connectors::fetch(&self.datasource, sql).expect("mutate source");
    }

    /// A registry handle on the same store, for chunk-list assertions.
    fn accel(&self) -> Accelerator {
        Accelerator::open(&self.dir.join("config.yaml")).expect("open store")
    }
}

/// The single status string of a DDL result.
fn status_of(result: &(arrow::datatypes::SchemaRef, Vec<RecordBatch>)) -> String {
    assert_eq!(result.0.field(0).name(), "status");
    let column = result.1[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status is Utf8");
    assert_eq!(column.len(), 1);
    column.value(0).to_string()
}

/// Flatten `(Int64, Utf8)` result rows in batch order.
fn rows_of(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 is Int64");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 1 is Utf8");
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), names.value(row).to_string()));
        }
    }
    rows
}

/// The identity gate: reading the view equals running `select` fresh, both
/// under the same ORDER BY.
fn assert_identity(runtime: &Runtime, view: &str, select: &str, order_by: &str) {
    let (_, from_view) = runtime
        .execute(&format!("SELECT * FROM {view} ORDER BY {order_by}"))
        .expect("read view");
    let (_, fresh) = runtime
        .execute(&format!("{select} ORDER BY {order_by}"))
        .expect("run select fresh");
    assert_eq!(rows_of(&from_view), rows_of(&fresh));
}

/// The monotonic declaration on events.id.
fn monotonic_id() -> Vec<(&'static str, ChangeKey)> {
    vec![(
        "main.events",
        ChangeKey::Monotonic {
            column: "id".to_string(),
        },
    )]
}

#[test]
fn append_delta_pulls_only_new_rows_and_keeps_old_chunks() {
    let sandbox = Sandbox::new("append", &monotonic_id());
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let select = format!("SELECT id, payload FROM {ds}.main.events");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW ev AS {select}"))
        .expect("create");
    let created = sandbox.accel().view("ev").expect("view");
    assert_eq!(created.measured_rows, 3);
    // CREATE stored the watermark of the pulled rows.
    let state = created.change_key.clone().expect("append state stored");
    assert_eq!(state.column, "id");
    assert_eq!(state.watermark, Some(fq_accel::Watermark::Int(3)));
    assert!(!created.source_tokens.is_empty());

    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (4, TIMESTAMP '2026-01-04 11:00:00', 'delta')",
    );
    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (5, TIMESTAMP '2026-01-05 12:00:00', 'edges')",
    );

    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW ev")
        .expect("refresh");
    assert_eq!(
        status_of(&refreshed),
        "REFRESH MATERIALIZED VIEW (delta append: 2 rows)"
    );

    // The old generation-0 chunk is still listed first and untouched; the two
    // delta rows landed as a NEW generation-1 chunk after it.
    let after = sandbox.accel().view("ev").expect("view");
    assert_eq!(after.chunk_list.len(), created.chunk_list.len() + 1);
    assert_eq!(
        &after.chunk_list[..created.chunk_list.len()],
        &created.chunk_list[..]
    );
    assert!(after
        .chunk_list
        .last()
        .expect("new chunk")
        .starts_with("chunk-1-"));
    assert_eq!(after.measured_rows, 5);
    let advanced = after.change_key.expect("state advanced");
    assert_eq!(advanced.watermark, Some(fq_accel::Watermark::Int(5)));

    assert_identity(&runtime, "ev", &select, "id");
}

#[test]
fn timestamp_watermarks_append_through_the_engine() {
    // The watermark column is a TIMESTAMP exposed under an alias: the delta
    // filter renders a TIMESTAMP literal and compares on the aliased output.
    let sandbox = Sandbox::new(
        "tswm",
        &[(
            "main.events",
            ChangeKey::Monotonic {
                column: "happened_at".to_string(),
            },
        )],
    );
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let select =
        format!("SELECT id, payload, happened_at AS seen_at FROM {ds}.main.events WHERE id <> 2");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW recent AS {select}"))
        .expect("create");

    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (9, TIMESTAMP '2026-02-01 07:15:30', 'later')",
    );
    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW recent")
        .expect("refresh");
    assert_eq!(
        status_of(&refreshed),
        "REFRESH MATERIALIZED VIEW (delta append: 1 rows)"
    );
    assert_identity(&runtime, "recent", &select, "id");
}

#[test]
fn unchanged_source_tokens_make_refresh_a_no_op_that_writes_nothing() {
    let sandbox = Sandbox::new("noop", &monotonic_id());
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW ev AS SELECT id, payload FROM {ds}.main.events"
        ))
        .expect("create");
    let before = sandbox.accel().view("ev").expect("view");

    // No source change since CREATE: the tokens match and the pull is skipped.
    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW ev")
        .expect("refresh");
    assert_eq!(
        status_of(&refreshed),
        "REFRESH MATERIALIZED VIEW (no-op: source tokens unchanged)"
    );
    let after = sandbox.accel().view("ev").expect("view");
    // NOTHING was written: chunk list, timestamps, tokens, and state are
    // byte-identical to the pre-refresh row.
    assert_eq!(after, before);

    // A real change moves the token; the next refresh pulls, and the one
    // after that no-ops again on the tokens the pull stored.
    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (4, TIMESTAMP '2026-01-04 11:00:00', 'delta')",
    );
    let pulled = runtime
        .execute("REFRESH MATERIALIZED VIEW ev")
        .expect("refresh after change");
    assert!(status_of(&pulled).contains("delta append"), "{pulled:?}");
    let again = runtime
        .execute("REFRESH MATERIALIZED VIEW ev")
        .expect("refresh again");
    assert_eq!(
        status_of(&again),
        "REFRESH MATERIALIZED VIEW (no-op: source tokens unchanged)"
    );
}

#[test]
fn primary_key_merge_rewrites_only_affected_chunks() {
    let sandbox = Sandbox::new(
        "merge",
        &[(
            "main.customers",
            ChangeKey::PrimaryKey {
                column: "c_id".to_string(),
            },
        )],
    );
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let select = format!("SELECT c_id, c_name FROM {ds}.main.customers");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW cust AS {select}"))
        .expect("create");
    let created = sandbox.accel().view("cust").expect("view");
    // A merge-mode view carries no watermark state.
    assert_eq!(created.change_key, None);

    // Update one row, delete one, insert one.
    sandbox.mutate(
        &runtime,
        "UPDATE main.customers SET c_name = 'ADA' WHERE c_id = 1",
    );
    sandbox.mutate(&runtime, "DELETE FROM main.customers WHERE c_id = 2");
    sandbox.mutate(&runtime, "INSERT INTO main.customers VALUES (4, 'dan')");

    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW cust")
        .expect("refresh");
    // The single stored chunk holds changed keys, so it rewrites; the new key
    // appends. (The multi-chunk only-affected proof lives in fq-accel's
    // integration suite, where chunk sizes are controllable.)
    assert_eq!(
        status_of(&refreshed),
        "REFRESH MATERIALIZED VIEW (merge: 1 chunks rewritten, 0 kept, 1 appended)"
    );
    let after = sandbox.accel().view("cust").expect("view");
    assert_eq!(after.measured_rows, 3);
    assert!(after
        .chunk_list
        .iter()
        .all(|chunk| chunk.starts_with("chunk-1-")));
    assert_identity(&runtime, "cust", &select, "c_id");

    // A second refresh with no change is a token no-op.
    let noop = runtime
        .execute("REFRESH MATERIALIZED VIEW cust")
        .expect("refresh again");
    assert_eq!(
        status_of(&noop),
        "REFRESH MATERIALIZED VIEW (no-op: source tokens unchanged)"
    );
}

#[test]
fn a_non_admissible_view_falls_back_to_whole_re_pull_and_says_why() {
    // BOTH tables declare change keys, so the fallback reason is the SHAPE.
    let sandbox = Sandbox::new(
        "shape",
        &[
            (
                "main.events",
                ChangeKey::Monotonic {
                    column: "id".to_string(),
                },
            ),
            (
                "main.customers",
                ChangeKey::PrimaryKey {
                    column: "c_id".to_string(),
                },
            ),
        ],
    );
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let select = format!(
        "SELECT e.id, c.c_name FROM {ds}.main.events e \
         JOIN {ds}.main.customers c ON e.id = c.c_id"
    );
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW joined AS {select}"))
        .expect("create");
    sandbox.mutate(&runtime, "INSERT INTO main.customers VALUES (4, 'dan')");
    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (4, TIMESTAMP '2026-01-04 11:00:00', 'delta')",
    );

    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW joined")
        .expect("refresh");
    let status = status_of(&refreshed);
    assert!(
        status.starts_with("REFRESH MATERIALIZED VIEW (whole re-pull:"),
        "{status}"
    );
    assert!(status.contains("Join node"), "{status}");
    assert_identity(&runtime, "joined", &select, "id");
}

#[test]
fn a_projection_that_drops_the_change_key_falls_back_and_says_why() {
    let sandbox = Sandbox::new("dropped", &monotonic_id());
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    // `id` is declared monotonic but the view does not expose it.
    let select = format!("SELECT id AS c0, payload FROM {ds}.main.events");
    let dropped = format!("SELECT payload AS p1, payload AS p2 FROM {ds}.main.events");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW exposed AS {select}"))
        .expect("create exposed");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW hidden AS {dropped}"))
        .expect("create hidden");
    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (4, TIMESTAMP '2026-01-04 11:00:00', 'delta')",
    );

    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW hidden")
        .expect("refresh");
    let status = status_of(&refreshed);
    assert!(status.contains("whole re-pull"), "{status}");
    assert!(status.contains("not exposed"), "{status}");

    // The aliased sibling still deltas: the watermark tracks the OUTPUT name.
    let delta = runtime
        .execute("REFRESH MATERIALIZED VIEW exposed")
        .expect("refresh exposed");
    assert_eq!(
        status_of(&delta),
        "REFRESH MATERIALIZED VIEW (delta append: 1 rows)"
    );
    assert_identity(&runtime, "exposed", &select, "c0");
}

#[test]
fn a_change_key_declared_after_creation_still_deltas_via_the_stored_chunks() {
    // CREATE under a config WITHOUT declarations (no watermark state stored),
    // then refresh under a config WITH the declaration: the watermark is
    // re-derived from the stored chunks and the refresh appends.
    let mut sandbox = Sandbox::new("latekey", &[]);
    {
        let runtime = sandbox.runtime();
        let ds = &sandbox.datasource;
        runtime
            .execute(&format!(
                "CREATE MATERIALIZED VIEW ev AS SELECT id, payload FROM {ds}.main.events"
            ))
            .expect("create");
        assert_eq!(sandbox.accel().view("ev").expect("view").change_key, None);
    }
    sandbox.change_keys.insert(
        "main.events".to_string(),
        ChangeKey::Monotonic {
            column: "id".to_string(),
        },
    );
    let runtime = sandbox.runtime();
    sandbox.mutate(
        &runtime,
        "INSERT INTO main.events VALUES (4, TIMESTAMP '2026-01-04 11:00:00', 'delta')",
    );
    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW ev")
        .expect("refresh");
    assert_eq!(
        status_of(&refreshed),
        "REFRESH MATERIALIZED VIEW (delta append: 1 rows)"
    );
    let ds = &sandbox.datasource;
    assert_identity(
        &runtime,
        "ev",
        &format!("SELECT id, payload FROM {ds}.main.events"),
        "id",
    );
}

#[test]
fn change_key_declarations_are_validated_at_load() {
    let unknown_table = Sandbox::new(
        "valtable",
        &[(
            "main.ghost",
            ChangeKey::Monotonic {
                column: "id".to_string(),
            },
        )],
    );
    let error = Runtime::from_config(&unknown_table.config())
        .map(|_| ())
        .unwrap_err();
    assert!(format!("{error}").contains("unknown table"), "{error}");

    let unknown_column = Sandbox::new(
        "valcolumn",
        &[(
            "main.events",
            ChangeKey::PrimaryKey {
                column: "ghost".to_string(),
            },
        )],
    );
    let error = Runtime::from_config(&unknown_column.config())
        .map(|_| ())
        .unwrap_err();
    assert!(format!("{error}").contains("unknown column"), "{error}");
}
