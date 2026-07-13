//! Event views end to end over a real DuckDB source: CREATE EVENT VIEW maps a
//! SELECT onto the event roles and persists a sorted materialization; FUNNEL
//! and SEGMENT run their kernels over the stored chunks with hand-computable
//! results; freshness is the materialized-view contract (analyses see the
//! last pull until REFRESH EVENT VIEW); contract violations and cross-form
//! DDL raise loudly.
//!
//! Every test builds its own temp config dir and uniquely named DuckDB
//! datasource, so parallel tests never share a store or clobber the
//! process-wide exec-plane registry.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_exec::connectors;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// A hand-verifiable event fixture (all on 2026-01-01 except one u1 'view'):
/// against a signup -> activate -> purchase funnel with a 2 HOURS window,
/// u1 completes; u2 activates too late; u3's early purchase cannot follow its
/// activate; u4 never signs up; u5 completes only from its second signup
/// (re-entry); u6's same-instant activate is a tie and never advances.
/// Rows are inserted out of order on purpose: the BUILD must sort them.
const SEED_SQL: &str = "\
    CREATE TABLE events (user_id INTEGER, ts TIMESTAMP, name VARCHAR, device VARCHAR); \
    INSERT INTO events VALUES \
        (5, '2026-01-01 13:30:00', 'purchase', 'ios'), \
        (1, '2026-01-01 10:00:00', 'signup',   'ios'), \
        (1, '2026-01-01 10:30:00', 'activate', 'ios'), \
        (1, '2026-01-01 11:00:00', 'purchase', 'ios'), \
        (2, '2026-01-01 10:00:00', 'signup',   'web'), \
        (2, '2026-01-01 13:00:00', 'activate', 'web'), \
        (3, '2026-01-01 10:00:00', 'signup',   'web'), \
        (3, '2026-01-01 10:20:00', 'purchase', 'web'), \
        (3, '2026-01-01 10:40:00', 'activate', 'web'), \
        (4, '2026-01-01 10:00:00', 'activate', 'ios'), \
        (4, '2026-01-01 10:30:00', 'purchase', 'ios'), \
        (5, '2026-01-01 10:00:00', 'signup',   'ios'), \
        (5, '2026-01-01 12:30:00', 'signup',   'ios'), \
        (5, '2026-01-01 13:00:00', 'activate', 'ios'), \
        (6, '2026-01-01 10:00:00', 'signup',   'web'), \
        (6, '2026-01-01 10:00:00', 'activate', 'web'), \
        (1, '2026-01-02 09:00:00', 'view',     'ios');";

/// The event-view DDL over the seeded table, parameterized by datasource.
fn create_view_sql(datasource: &str) -> String {
    format!(
        "CREATE EVENT VIEW ev ENTITY user_id TIMESTAMP ts EVENT name \
         AS SELECT user_id, ts, name, device FROM {datasource}.main.events"
    )
}

/// The three-step funnel the fixture is hand-computed against.
const FUNNEL_SQL: &str = "FUNNEL OVER ev STEPS ('signup', 'activate', 'purchase') WITHIN 2 HOURS";

/// A per-test sandbox: a temp dir holding the seeded DuckDB file and the
/// config path the stores hang off. The datasource name is `ev_<tag>_duck` -
/// the `ev_` prefix keeps it unique in the PROCESS-WIDE exec-plane registry,
/// which every integration suite in this binary shares.
struct Sandbox {
    dir: PathBuf,
    datasource: String,
}

impl Sandbox {
    /// Seed a fresh sandbox under a unique temp dir.
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_ev_{tag}_{}", std::process::id()));
        // A leftover dir from a crashed prior run would make CREATE TABLE and
        // duplicate-view checks misfire; start clean.
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        let duck_path = dir.join("data.duckdb");
        let source = DuckDbSource::open("seed", duck_path.to_str().expect("utf-8 path"))
            .expect("open seed duckdb");
        source.execute_batch(SEED_SQL).expect("seed events");
        drop(source);
        Self {
            dir,
            datasource: format!("ev_{tag}_duck"),
        }
    }

    /// The sandbox's config: one DuckDB datasource plus a `source_path` inside
    /// the sandbox dir, which gives the runtime its view stores.
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
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over this sandbox's config.
    fn runtime(&self) -> Runtime {
        Runtime::from_config(&self.config()).expect("from_config")
    }

    /// Append one event row through the exec plane's shared DuckDB instance
    /// (the runtime holds the file's single read-write instance).
    fn insert_event(&self, user: i32, timestamp: &str, name: &str) {
        connectors::fetch(
            &self.datasource,
            &format!("INSERT INTO main.events VALUES ({user}, '{timestamp}', '{name}', 'web')"),
        )
        .expect("insert event row");
    }
}

/// One flattened funnel result row: (step, event, entities, from_start,
/// from_previous).
type FunnelRow = (i64, String, i64, Option<f64>, Option<f64>);

/// Flatten a funnel result to per-step rows.
fn funnel_rows(batches: &[RecordBatch]) -> Vec<FunnelRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let steps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("step Int64");
        let events = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("event Utf8");
        let entities = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("entities Int64");
        let from_start = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("from_start Float64");
        let from_previous = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("from_previous Float64");
        for row in 0..batch.num_rows() {
            rows.push((
                steps.value(row),
                events.value(row).to_string(),
                entities.value(row),
                from_start.is_valid(row).then(|| from_start.value(row)),
                from_previous
                    .is_valid(row)
                    .then(|| from_previous.value(row)),
            ));
        }
    }
    rows
}

/// Flatten a single Utf8 column in batch order (status tables).
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
fn the_view_serves_exactly_the_defining_selects_rows() {
    let sandbox = Sandbox::new("identity");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let created = runtime.execute(&create_view_sql(ds)).expect("create");
    assert_status(&created, "CREATE EVENT VIEW");

    // The identity gate: the sort/partition contract changes physical order
    // only, never content - under a shared total ORDER BY the view's rows
    // equal the defining SELECT's rows exactly.
    let order = "ORDER BY user_id, ts, name";
    let (_, from_view) = runtime
        .execute(&format!("SELECT user_id, name, device FROM ev {order}"))
        .expect("select from view");
    let (_, direct) = runtime
        .execute(&format!(
            "SELECT user_id, name, device FROM {ds}.main.events {order}"
        ))
        .expect("select direct");
    let flatten = |batches: &[RecordBatch]| -> Vec<String> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let users = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .expect("Int32 users");
                let names = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 names");
                let devices = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 devices");
                rows.push(format!(
                    "{}|{}|{}",
                    users.value(row),
                    names.value(row),
                    devices.value(row)
                ));
            }
        }
        rows
    };
    let view_rows = flatten(&from_view);
    assert_eq!(view_rows, flatten(&direct));
    assert_eq!(view_rows.len(), 17);
}

#[test]
fn funnel_counts_match_the_hand_computed_fixture() {
    let sandbox = Sandbox::new("funnel");
    let runtime = sandbox.runtime();
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("create");
    let (schema, batches) = runtime.execute(FUNNEL_SQL).expect("funnel");
    assert_eq!(schema.field(0).name(), "step");
    // Step 1: u1,u2,u3,u5,u6 = 5; step 2: u1,u3,u5 = 3; step 3: u1,u5 = 2.
    assert_eq!(
        funnel_rows(&batches),
        vec![
            (1, "signup".to_string(), 5, Some(1.0), None),
            (2, "activate".to_string(), 3, Some(0.6), Some(0.6)),
            (3, "purchase".to_string(), 2, Some(0.4), Some(2.0 / 3.0)),
        ]
    );
}

#[test]
fn analyses_see_the_last_pull_until_refresh() {
    let sandbox = Sandbox::new("freshness");
    let runtime = sandbox.runtime();
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("create");

    // Mutate the SOURCE: u7 signs up and activates within the window.
    sandbox.insert_event(7, "2026-01-03 10:00:00", "signup");
    sandbox.insert_event(7, "2026-01-03 11:00:00", "activate");

    // The funnel still serves the LAST PULL: nothing on the analysis path
    // checks the source.
    let (_, stale) = runtime.execute(FUNNEL_SQL).expect("stale funnel");
    assert_eq!(funnel_rows(&stale)[0].2, 5);
    assert_eq!(funnel_rows(&stale)[1].2, 3);

    // REFRESH re-pulls, re-validates, and re-sorts; u7 now counts.
    let refreshed = runtime
        .execute("REFRESH EVENT VIEW ev")
        .expect("refresh event view");
    assert_status(&refreshed, "REFRESH EVENT VIEW");
    let (_, fresh) = runtime.execute(FUNNEL_SQL).expect("fresh funnel");
    assert_eq!(funnel_rows(&fresh)[0].2, 6);
    assert_eq!(funnel_rows(&fresh)[1].2, 4);
    assert_eq!(funnel_rows(&fresh)[2].2, 2);
}

#[test]
fn segment_counts_events_and_entities_per_day() {
    let sandbox = Sandbox::new("segment");
    let runtime = sandbox.runtime();
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("create");

    // 16 events on 2026-01-01 and one 'view' on 2026-01-02.
    let (schema, batches) = runtime
        .execute("SEGMENT OVER ev MEASURE EVENTS BY DAY")
        .expect("segment events");
    assert_eq!(schema.field(0).name(), "bucket");
    let values = |batches: &[RecordBatch]| -> Vec<(i64, i64)> {
        let mut rows = Vec::new();
        for batch in batches {
            let buckets = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .expect("bucket timestamps");
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 values");
            for row in 0..batch.num_rows() {
                rows.push((buckets.value(row), counts.value(row)));
            }
        }
        rows
    };
    let events = values(&batches);
    assert_eq!(events.len(), 2);
    assert_eq!((events[0].1, events[1].1), (16, 1));
    // The buckets are consecutive UTC days.
    assert_eq!(events[1].0 - events[0].0, 86_400 * 1_000_000);

    // Distinct entities: six users on day one, only u1 on day two.
    let (_, batches) = runtime
        .execute("SEGMENT OVER ev MEASURE ENTITIES BY DAY")
        .expect("segment entities");
    let entities = values(&batches);
    assert_eq!((entities[0].1, entities[1].1), (6, 1));

    // The event filter measures one name: six signup EVENTS on day one (u5
    // signs up twice), against five signup ENTITIES.
    let (_, batches) = runtime
        .execute("SEGMENT OVER ev MEASURE EVENTS EVENT 'signup' BY DAY")
        .expect("segment filtered");
    assert_eq!(values(&batches), vec![(events[0].0, 6)]);
    let (_, batches) = runtime
        .execute("SEGMENT OVER ev MEASURE ENTITIES EVENT 'signup' BY DAY")
        .expect("segment filtered entities");
    assert_eq!(values(&batches), vec![(events[0].0, 5)]);
}

#[test]
fn drop_removes_the_relation_and_frees_the_name() {
    let sandbox = Sandbox::new("drop");
    let runtime = sandbox.runtime();
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("create");

    let dropped = runtime.execute("DROP EVENT VIEW ev").expect("drop");
    assert_status(&dropped, "DROP EVENT VIEW");

    // Neither the relation nor the analyses resolve it any more.
    assert!(runtime.execute("SELECT user_id FROM ev").is_err());
    let funnel = runtime.execute(FUNNEL_SQL).unwrap_err();
    assert!(format!("{funnel}").contains("does not exist"), "{funnel}");

    // The name is free again: a re-create succeeds.
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("re-create after drop");
}

#[test]
fn a_new_runtime_over_the_same_config_sees_the_event_view() {
    let sandbox = Sandbox::new("restart");
    {
        let runtime = sandbox.runtime();
        runtime
            .execute(&create_view_sql(&sandbox.datasource))
            .expect("create");
    }
    // A FRESH runtime (same config) finds the roles and chunks durably.
    let runtime = sandbox.runtime();
    let (_, batches) = runtime.execute(FUNNEL_SQL).expect("funnel after restart");
    assert_eq!(funnel_rows(&batches)[0].2, 5);
}

#[test]
fn a_null_role_value_fails_the_create_and_persists_nothing() {
    let sandbox = Sandbox::new("nullrole");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    sandbox.insert_event(8, "2026-01-01 10:00:00", "signup");
    connectors::fetch(
        ds,
        "INSERT INTO main.events VALUES (8, '2026-01-01 11:00:00', NULL, 'web')",
    )
    .expect("insert a null-named event");

    let error = runtime.execute(&create_view_sql(ds)).unwrap_err();
    let text = format!("{error}");
    assert!(text.contains("contract") && text.contains("name"), "{text}");

    // Nothing persisted: the name did not come into existence.
    assert!(runtime.execute("SELECT user_id FROM ev").is_err());

    // A defining SELECT that filters the violation out builds fine.
    runtime
        .execute(&format!(
            "CREATE EVENT VIEW ev ENTITY user_id TIMESTAMP ts EVENT name \
             AS SELECT user_id, ts, name FROM {ds}.main.events WHERE name IS NOT NULL"
        ))
        .expect("create with the filter");
}

#[test]
fn bad_role_declarations_raise() {
    let sandbox = Sandbox::new("badroles");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;

    // A role naming a column the SELECT does not produce.
    let missing = runtime
        .execute(&format!(
            "CREATE EVENT VIEW ev ENTITY uzer_id TIMESTAMP ts EVENT name \
             AS SELECT user_id, ts, name FROM {ds}.main.events"
        ))
        .unwrap_err();
    assert!(format!("{missing}").contains("uzer_id"), "{missing}");

    // A role naming a column of the wrong type (TIMESTAMP over text).
    let mistyped = runtime
        .execute(&format!(
            "CREATE EVENT VIEW ev ENTITY user_id TIMESTAMP device EVENT name \
             AS SELECT user_id, ts, name, device FROM {ds}.main.events"
        ))
        .unwrap_err();
    assert!(
        format!("{mistyped}").contains("TIMESTAMP or DATE"),
        "{mistyped}"
    );
}

#[test]
fn event_and_materialized_ddl_refuse_each_others_views() {
    let sandbox = Sandbox::new("crossddl");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&create_view_sql(ds))
        .expect("create event view");
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW plain_mv AS SELECT user_id FROM {ds}.main.events"
        ))
        .expect("create plain mv");

    // The MATERIALIZED forms refuse an event view (they would skip the sort
    // contract / orphan the roles).
    for sql in ["REFRESH MATERIALIZED VIEW ev", "DROP MATERIALIZED VIEW ev"] {
        let error = runtime.execute(sql).unwrap_err();
        assert!(format!("{error}").contains("is an event view"), "{error}");
    }

    // The EVENT forms refuse a plain materialized view: it is not an event view.
    for sql in ["REFRESH EVENT VIEW plain_mv", "DROP EVENT VIEW plain_mv"] {
        let error = runtime.execute(sql).unwrap_err();
        assert!(format!("{error}").contains("does not exist"), "{error}");
    }

    // FUNNEL over the plain view raises the same way.
    let funnel = runtime
        .execute("FUNNEL OVER plain_mv STEPS ('a', 'b') WITHIN 1 DAY")
        .unwrap_err();
    assert!(format!("{funnel}").contains("does not exist"), "{funnel}");

    // A name that already resolves (the event view) cannot be re-created.
    let collision = runtime.execute(&create_view_sql(ds)).unwrap_err();
    assert!(
        format!("{collision}").contains("already exists"),
        "{collision}"
    );
}

#[test]
fn describe_matches_the_executed_analysis_schemas() {
    let sandbox = Sandbox::new("describe");
    let runtime = sandbox.runtime();
    runtime
        .execute(&create_view_sql(&sandbox.datasource))
        .expect("create");

    // The described columns equal the executed result's column names, in order.
    let described = runtime.describe(FUNNEL_SQL).expect("describe funnel");
    let (schema, _) = runtime.execute(FUNNEL_SQL).expect("run funnel");
    let mut described_names = Vec::new();
    for (name, _) in &described {
        described_names.push(name.clone());
    }
    let mut executed_names = Vec::new();
    for field in schema.fields() {
        executed_names.push(field.name().clone());
    }
    assert_eq!(described_names, executed_names);

    let segment_sql = "SEGMENT OVER ev MEASURE EVENTS BY DAY";
    let described = runtime.describe(segment_sql).expect("describe segment");
    let (schema, _) = runtime.execute(segment_sql).expect("run segment");
    assert_eq!(described.len(), schema.fields().len());
    assert_eq!(described[0].0, schema.field(0).name().as_str());

    // Event DDL describes to the status column; EXPLAIN of an analysis raises.
    let ddl = runtime
        .describe("DROP EVENT VIEW ev")
        .expect("describe ddl");
    assert_eq!(ddl[0].0, "status");
    let explain = runtime
        .execute(&format!("EXPLAIN {FUNNEL_SQL}"))
        .unwrap_err();
    assert!(
        format!("{explain}").contains("no plan to explain"),
        "{explain}"
    );
}

#[test]
fn paths_raises_naming_the_unimplemented_surface() {
    let sandbox = Sandbox::new("paths");
    let runtime = sandbox.runtime();
    let error = runtime
        .execute("PATHS OVER ev MAX DEPTH 5 TOP 10")
        .unwrap_err();
    assert!(
        format!("{error}").contains("path analysis is not implemented"),
        "{error}"
    );
}
