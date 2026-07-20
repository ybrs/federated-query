//! Materialized views end to end over a real DuckDB source: CREATE persists
//! the defining SELECT's rows and the view serves them as a relation; serving
//! trusts the last pull until an explicit REFRESH re-pulls the whole view;
//! DROP removes the relation and its chunks; a NEW runtime over the same
//! config sees the view (the store is durable next to the config); EXPLAIN
//! labels the read `MaterializedScan`. The identity gate: rows read from the
//! view equal running the stored SELECT directly.
//!
//! Every test builds its own temp config dir and uniquely named DuckDB
//! datasource, so parallel tests never share a store or clobber the
//! process-wide exec-plane registry.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_exec::connectors;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The region subset the suite reads: keys 0..4 with their TPC-H names.
const SEED_SQL: &str = "\
    CREATE TABLE region (r_regionkey INTEGER, r_name VARCHAR); \
    INSERT INTO region VALUES \
        (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST');";

/// A per-test sandbox: a temp dir holding the seeded DuckDB file and the
/// config path the store hangs off (`<dir>/config.stats.sqlite`,
/// `<dir>/config.mv/`). The datasource name is `<tag>_duck`, unique per test.
struct Sandbox {
    dir: PathBuf,
    datasource: String,
}

impl Sandbox {
    /// Seed a fresh sandbox under a unique temp dir.
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_mv_{tag}_{}", std::process::id()));
        // A leftover dir from a crashed prior run would make CREATE TABLE and
        // duplicate-view checks misfire; start clean.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        let duck_path = dir.join("data.duckdb");
        let source = DuckDbSource::open("seed", duck_path.to_str().expect("utf-8 path"))
            .expect("open seed duckdb");
        source.execute_batch(SEED_SQL).expect("seed region");
        drop(source);
        Self {
            dir,
            datasource: format!("{tag}_duck"),
        }
    }

    /// The sandbox's config: one DuckDB datasource plus a `source_path` inside
    /// the sandbox dir, which is what gives the runtime a materialized-view
    /// store (it lives next to the config path).
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
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over this sandbox's config.
    fn runtime(&self) -> Runtime {
        Runtime::from_config(&self.config()).expect("from_config")
    }

    /// Append one region row through the exec plane's shared DuckDB instance
    /// (the runtime holds the file's single read-write instance, so a second
    /// standalone open would fight its lock).
    fn insert_region(&self, runtime: &Runtime, key: i32, name: &str) {
        let _scope = connectors::SessionScope::enter(runtime.exec_session());
        connectors::fetch(
            &self.datasource,
            &format!("INSERT INTO main.region VALUES ({key}, '{name}')"),
        )
        .expect("insert region row");
    }

    /// The store root the runtime derives from this sandbox's config path.
    fn store_root(&self) -> PathBuf {
        self.dir.join("config.mv")
    }
}

/// Flatten `(Int32, Utf8)` result rows in batch order.
fn int_string_rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column 0 is Int32");
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

/// Flatten a single Utf8 column in batch order (status tables, EXPLAIN rows).
fn string_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column 0 is Utf8");
        for row in 0..batch.num_rows() {
            rows.push(column.value(row).to_string());
        }
    }
    rows
}

/// Assert a DDL result is the one-row `status` table carrying `tag`.
fn assert_status(result: &(SchemaRef, Vec<RecordBatch>), tag: &str) {
    assert_eq!(result.0.field(0).name(), "status");
    assert_eq!(string_rows(&result.1), vec![tag.to_string()]);
}

#[test]
fn create_serves_the_stored_rows_and_matches_the_select() {
    let sandbox = Sandbox::new("identity");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let select = format!("SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3");

    let created = runtime
        .execute(&format!("CREATE MATERIALIZED VIEW low_regions AS {select}"))
        .expect("create");
    assert_status(&created, "CREATE MATERIALIZED VIEW");

    // The Phase A identity gate: reading the view returns exactly the rows the
    // stored SELECT produces when run directly (same values, same order under
    // the shared ORDER BY).
    let (schema, from_view) = runtime
        .execute("SELECT r_regionkey, r_name FROM low_regions ORDER BY r_regionkey")
        .expect("select from view");
    let (_, direct) = runtime
        .execute(&format!("{select} ORDER BY r_regionkey"))
        .expect("select direct");
    assert_eq!(schema.field(0).name(), "r_regionkey");
    assert_eq!(schema.field(1).name(), "r_name");
    assert_eq!(int_string_rows(&from_view), int_string_rows(&direct));
    assert_eq!(int_string_rows(&from_view).len(), 3);
}

#[test]
fn serving_trusts_the_last_pull_until_refresh() {
    let sandbox = Sandbox::new("freshness");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW fresh_regions AS \
             SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3"
        ))
        .expect("create");

    // Mutate the SOURCE: a new row that matches the view's filter.
    sandbox.insert_region(&runtime, 2, "ATLANTIS");

    // The view still serves the LAST PULL (three rows) while a direct query
    // sees four: nothing on the query path checks the source.
    let (_, stale) = runtime
        .execute("SELECT r_regionkey, r_name FROM fresh_regions ORDER BY r_regionkey, r_name")
        .expect("stale read");
    assert_eq!(int_string_rows(&stale).len(), 3);
    let (_, direct) = runtime
        .execute(&format!(
            "SELECT r_regionkey, r_name FROM {ds}.main.region \
             WHERE r_regionkey < 3 ORDER BY r_regionkey, r_name"
        ))
        .expect("direct read");
    assert_eq!(int_string_rows(&direct).len(), 4);

    // REFRESH re-pulls the whole view (this sandbox declares no change keys,
    // and the status row says exactly that); it now serves the new row too.
    let refreshed = runtime
        .execute("REFRESH MATERIALIZED VIEW fresh_regions")
        .expect("refresh");
    let status = string_rows(&refreshed.1);
    assert_eq!(status.len(), 1);
    assert!(
        status[0].starts_with("REFRESH MATERIALIZED VIEW (whole re-pull:"),
        "{status:?}"
    );
    assert!(status[0].contains("no declared change key"), "{status:?}");
    let (_, current) = runtime
        .execute("SELECT r_regionkey, r_name FROM fresh_regions ORDER BY r_regionkey, r_name")
        .expect("fresh read");
    assert_eq!(int_string_rows(&current), int_string_rows(&direct));
}

#[test]
fn drop_removes_the_relation_and_its_chunks() {
    let sandbox = Sandbox::new("drop");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW doomed AS SELECT r_name FROM {ds}.main.region"
        ))
        .expect("create");
    let chunk_dirs = || -> usize {
        std::fs::read_dir(sandbox.store_root()).map_or(0, std::iter::Iterator::count)
    };
    assert_eq!(chunk_dirs(), 1);

    let dropped = runtime
        .execute("DROP MATERIALIZED VIEW doomed")
        .expect("drop");
    assert_status(&dropped, "DROP MATERIALIZED VIEW");

    // The relation no longer resolves: the binder raises loudly on it.
    let error = runtime.execute("SELECT r_name FROM doomed").unwrap_err();
    assert!(format!("{error}").contains("doomed"), "{error}");
    // And its chunk directory is gone from the store.
    assert_eq!(chunk_dirs(), 0);
}

#[test]
fn a_new_runtime_over_the_same_config_sees_the_view() {
    let sandbox = Sandbox::new("restart");
    let ds = &sandbox.datasource;
    {
        let runtime = sandbox.runtime();
        runtime
            .execute(&format!(
                "CREATE MATERIALIZED VIEW durable AS \
                 SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 2"
            ))
            .expect("create");
    }
    // A FRESH runtime (same config) loads the view from the durable store.
    let runtime = sandbox.runtime();
    let (_, rows) = runtime
        .execute("SELECT r_regionkey, r_name FROM durable ORDER BY r_regionkey")
        .expect("select after restart");
    assert_eq!(
        int_string_rows(&rows),
        vec![(0, "AFRICA".to_string()), (1, "AMERICA".to_string())]
    );
}

#[test]
fn explain_labels_the_view_read_materialized_scan() {
    let sandbox = Sandbox::new("explain");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW labeled AS SELECT r_regionkey, r_name FROM {ds}.main.region"
        ))
        .expect("create");
    let (_, plan) = runtime
        .execute("EXPLAIN SELECT r_name FROM labeled WHERE r_regionkey = 1")
        .expect("explain");
    let lines = string_rows(&plan);
    assert!(
        lines
            .iter()
            .any(|line| line.contains("MaterializedScan [labeled]")),
        "plan lines: {lines:?}"
    );
}

#[test]
fn a_view_joins_with_a_source_table() {
    let sandbox = Sandbox::new("join");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW keys_mv AS \
             SELECT r_regionkey FROM {ds}.main.region WHERE r_regionkey < 2"
        ))
        .expect("create");
    // The view participates in federation like any relation: a cross-source
    // join between the store and the DuckDB source.
    let (_, rows) = runtime
        .execute(&format!(
            "SELECT m.r_regionkey, r.r_name FROM keys_mv m \
             JOIN {ds}.main.region r ON m.r_regionkey = r.r_regionkey \
             ORDER BY m.r_regionkey"
        ))
        .expect("join view with source");
    assert_eq!(
        int_string_rows(&rows),
        vec![(0, "AFRICA".to_string()), (1, "AMERICA".to_string())]
    );
}

#[test]
fn two_views_join_inside_the_store() {
    let sandbox = Sandbox::new("twoviews");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW left_mv AS \
             SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3"
        ))
        .expect("create left");
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW right_mv AS \
             SELECT r_regionkey FROM {ds}.main.region WHERE r_regionkey >= 1"
        ))
        .expect("create right");
    // Both relations live on the ONE store datasource, so the planner may
    // push the whole join down as a single DataFusion query over the chunks.
    let (_, rows) = runtime
        .execute(
            "SELECT l.r_regionkey, l.r_name FROM left_mv l \
             JOIN right_mv r ON l.r_regionkey = r.r_regionkey \
             ORDER BY l.r_regionkey",
        )
        .expect("join two views");
    assert_eq!(
        int_string_rows(&rows),
        vec![(1, "AMERICA".to_string()), (2, "ASIA".to_string())]
    );
}

#[test]
fn name_collisions_and_unknown_views_raise() {
    let sandbox = Sandbox::new("collide");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW taken AS SELECT r_name FROM {ds}.main.region"
        ))
        .expect("create");

    // A second view under the same name raises; nothing is replaced.
    let duplicate = runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW taken AS SELECT r_regionkey FROM {ds}.main.region"
        ))
        .unwrap_err();
    assert!(
        format!("{duplicate}").contains("already exists"),
        "{duplicate}"
    );

    // A view shadowing an existing SOURCE table raises too: bare names resolve
    // across every schema, so the collision would make `region` ambiguous.
    let shadows = runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW region AS SELECT r_name FROM {ds}.main.region"
        ))
        .unwrap_err();
    assert!(format!("{shadows}").contains("already exists"), "{shadows}");

    // REFRESH / DROP of a view that does not exist raise loudly.
    let refresh = runtime
        .execute("REFRESH MATERIALIZED VIEW ghost")
        .unwrap_err();
    assert!(format!("{refresh}").contains("does not exist"), "{refresh}");
    let drop = runtime.execute("DROP MATERIALIZED VIEW ghost").unwrap_err();
    assert!(format!("{drop}").contains("does not exist"), "{drop}");
}

#[test]
fn ddl_without_a_config_path_raises() {
    let sandbox = Sandbox::new("pathless");
    let mut config = sandbox.config();
    // A programmatically built config has no file path, so there is no place
    // for a store; the DDL must say so instead of writing anywhere.
    config.source_path = None;
    let runtime = Runtime::from_config(&config).expect("from_config");
    let error = runtime
        .execute("CREATE MATERIALIZED VIEW nowhere AS SELECT 1 AS one")
        .unwrap_err();
    assert!(
        format!("{error}").contains("no materialized-view store"),
        "{error}"
    );
}

#[test]
fn describe_resolves_ddl_to_the_status_column() {
    let sandbox = Sandbox::new("describe");
    let runtime = sandbox.runtime();
    let columns = runtime
        .describe("CREATE MATERIALIZED VIEW later AS SELECT 1 AS one")
        .expect("describe ddl");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].0, "status");
}

#[test]
fn an_aggregating_view_serves_filtered_reads() {
    let sandbox = Sandbox::new("agg");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    // The view body aggregates; reads over the view push their own filter and
    // grouping into the store scan.
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW region_stats AS \
             SELECT r_regionkey, count(*) AS c FROM {ds}.main.region GROUP BY r_regionkey"
        ))
        .expect("create");
    let (schema, rows) = runtime
        .execute(
            "SELECT r_regionkey, c FROM region_stats WHERE r_regionkey >= 3 \
             ORDER BY r_regionkey",
        )
        .expect("filtered read over the view");
    assert_eq!(schema.field(1).name(), "c");
    let mut flattened = Vec::new();
    for batch in &rows {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 keys");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64 counts");
        for row in 0..batch.num_rows() {
            flattened.push((keys.value(row), counts.value(row)));
        }
    }
    assert_eq!(flattened, vec![(3, 1), (4, 1)]);
}
