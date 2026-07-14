//! Automatic materialized-view substitution end to end over a real DuckDB
//! source: a query whose subtree matches a registered view's definition reads
//! the view's chunks in place of recomputing, cost-gated, behind a live kill
//! switch, with the substitution surfaced in EXPLAIN and benefit counters that
//! advance and persist. The identity gate: the substituted answer equals the
//! non-substituted answer.
//!
//! Every test builds its own temp config dir and uniquely named DuckDB
//! datasource, so parallel tests never share a store or clobber the
//! process-wide exec-plane registry.

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use fq_common::{
    Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig,
};
use fq_connectors::DuckDbSource;
use fq_runtime::Runtime;
use serde_yaml::Value;

/// The region subset the suite reads: keys 0..4 with their TPC-H names.
const SEED_SQL: &str = "\
    CREATE TABLE region (r_regionkey INTEGER, r_name VARCHAR); \
    INSERT INTO region VALUES \
        (0,'AFRICA'),(1,'AMERICA'),(2,'ASIA'),(3,'EUROPE'),(4,'MIDDLE EAST');";

/// A per-test sandbox: a temp dir holding the seeded DuckDB file and the config
/// path the store hangs off. The datasource name is `sub_<tag>_duck`, unique in
/// the process-wide exec-plane registry every suite in this binary shares.
struct Sandbox {
    dir: PathBuf,
    datasource: String,
}

/// A hand-verifiable event fixture: six rows, four on device 'ios'. The event
/// view over the 'ios' subset reduces 6 base rows to 4, so the cost gate clears.
const EVENTS_SEED: &str = "\
    CREATE TABLE events (user_id INTEGER, ts TIMESTAMP, name VARCHAR, device VARCHAR); \
    INSERT INTO events VALUES \
        (1,'2026-01-01 10:00:00','signup',  'ios'), \
        (1,'2026-01-01 10:30:00','activate','ios'), \
        (2,'2026-01-01 11:00:00','signup',  'web'), \
        (2,'2026-01-01 11:30:00','purchase','web'), \
        (3,'2026-01-01 12:00:00','signup',  'ios'), \
        (3,'2026-01-01 12:30:00','activate','ios');";

impl Sandbox {
    /// Seed a fresh sandbox with the region fixture.
    fn new(tag: &str) -> Self {
        Self::with_seed(tag, SEED_SQL)
    }

    /// Seed a fresh sandbox under a unique temp dir with the given DDL.
    fn with_seed(tag: &str, seed_sql: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("fq_sub_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sandbox dir");
        let duck_path = dir.join("data.duckdb");
        let source = DuckDbSource::open("seed", duck_path.to_str().expect("utf-8 path"))
            .expect("open seed duckdb");
        source.execute_batch(seed_sql).expect("seed fixture");
        drop(source);
        Self {
            dir,
            datasource: format!("sub_{tag}_duck"),
        }
    }

    /// The sandbox's config: one DuckDB datasource plus a `source_path` inside
    /// the sandbox dir, which gives the runtime a materialized-view store.
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
            source_path: Some(self.dir.join("config.yaml").to_string_lossy().to_string()),
        }
    }

    /// A runtime over this sandbox's config.
    fn runtime(&self) -> Runtime {
        Runtime::from_config(&self.config()).expect("from_config")
    }
}

/// The EXPLAIN plan lines of a query.
fn explain_lines(runtime: &Runtime, sql: &str) -> Vec<String> {
    let (_, batches) = runtime
        .execute(&format!("EXPLAIN {sql}"))
        .expect("explain query");
    string_column(&batches, 0)
}

/// Whether any plan line reads the materialized view named `view`.
fn substitutes(lines: &[String], view: &str) -> bool {
    lines
        .iter()
        .any(|line| line.contains(&format!("MaterializedScan [{view}]")))
}

/// Flatten one Utf8 column in batch order.
fn string_column(batches: &[RecordBatch], index: usize) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        for row in 0..batch.num_rows() {
            out.push(column.value(row).to_string());
        }
    }
    out
}

/// The single Int64 scalar of a one-row, one-column result.
fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    let batch = batches.first().expect("one batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .value(0)
}

/// Flatten `(Int32, Utf8)` result rows in batch order.
fn int_string_rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 column 0");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column 1");
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), names.value(row).to_string()));
        }
    }
    rows
}

#[test]
fn substitution_fires_and_matches_the_unsubstituted_answer() {
    let sandbox = Sandbox::new("fires");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    // An aggregating view: 5 base rows collapse to 1, so the cost gate (base
    // input 5 > cached read 1) clears.
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW region_count AS \
             SELECT count(*) AS c FROM {ds}.main.region"
        ))
        .expect("create");
    let query = format!("SELECT count(*) AS c FROM {ds}.main.region");

    // Substitution ON (default): the plan reads the view, not the source.
    let on_plan = explain_lines(&runtime, &query);
    assert!(
        substitutes(&on_plan, "region_count"),
        "expected a MaterializedScan; plan: {on_plan:?}"
    );
    let (on_schema, on_rows) = runtime.execute(&query).expect("run substituted");

    // Substitution OFF: the plan recomputes against the source.
    runtime
        .execute("SET accelerator.enable_substitution = false")
        .expect("disable substitution");
    let off_plan = explain_lines(&runtime, &query);
    assert!(
        !substitutes(&off_plan, "region_count"),
        "expected no substitution when off; plan: {off_plan:?}"
    );
    assert!(
        off_plan
            .iter()
            .any(|line| line.contains(&format!("Scan [{ds}]"))),
        "expected a source scan when off; plan: {off_plan:?}"
    );
    let (off_schema, off_rows) = runtime.execute(&query).expect("run unsubstituted");

    // The identity contract: same schema, same value.
    assert_eq!(on_schema.field(0).name(), off_schema.field(0).name());
    assert_eq!(scalar_i64(&on_rows), 5);
    assert_eq!(scalar_i64(&on_rows), scalar_i64(&off_rows));
}

#[test]
fn substitution_preserves_the_row_set() {
    let sandbox = Sandbox::new("identity");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    // A row-reducing filtered view (3 of 5 rows), so the gate clears. The query
    // is exactly the definition (no outer ORDER BY): substitution reads the
    // view chunks as a SET, which is the same set the recompute produces.
    let definition =
        format!("SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3");
    runtime
        .execute(&format!("CREATE MATERIALIZED VIEW low AS {definition}"))
        .expect("create");

    let (_, on_rows) = runtime.execute(&definition).expect("substituted");
    assert!(substitutes(&explain_lines(&runtime, &definition), "low"));

    runtime
        .execute("SET accelerator.enable_substitution = false")
        .expect("disable");
    let (_, off_rows) = runtime.execute(&definition).expect("unsubstituted");
    assert!(!substitutes(&explain_lines(&runtime, &definition), "low"));

    // Row order is unspecified for an unordered query (both ON and OFF), so the
    // identity contract is the ROW SET; a query's own ORDER BY (applied above a
    // substituted scan) fixes order in the ordinary way.
    let mut on_sorted = int_string_rows(&on_rows);
    let mut off_sorted = int_string_rows(&off_rows);
    on_sorted.sort();
    off_sorted.sort();
    assert_eq!(on_sorted, off_sorted);
    assert_eq!(on_sorted.len(), 3);
}

#[test]
fn a_mismatch_declines_to_substitute() {
    let sandbox = Sandbox::new("mismatch");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW low AS \
             SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3"
        ))
        .expect("create");

    // The EXACT definition matches (positive control; gate clears at 5 > 3).
    let exact = format!("SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 3");
    assert!(substitutes(&explain_lines(&runtime, &exact), "low"));

    // A different CONSTANT is a different shape - no match.
    let other_constant =
        format!("SELECT r_regionkey, r_name FROM {ds}.main.region WHERE r_regionkey < 2");
    assert!(!substitutes(
        &explain_lines(&runtime, &other_constant),
        "low"
    ));

    // A different COLUMN SET - no match.
    let fewer_columns = format!("SELECT r_regionkey FROM {ds}.main.region WHERE r_regionkey < 3");
    assert!(!substitutes(
        &explain_lines(&runtime, &fewer_columns),
        "low"
    ));

    // An EXTRA filter conjunct - no match.
    let extra_filter = format!(
        "SELECT r_regionkey, r_name FROM {ds}.main.region \
         WHERE r_regionkey < 3 AND r_name <> 'ZZZ'"
    );
    assert!(!substitutes(&explain_lines(&runtime, &extra_filter), "low"));
}

#[test]
fn the_cost_gate_declines_a_trivial_passthrough() {
    let sandbox = Sandbox::new("gate");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    // A view whose output equals its whole base table: recompute reads the same
    // 5 rows the cache would, so the gate (5 > 5 is false) declines - a fresh
    // source scan is no more expensive than opening the cache.
    let definition = format!("SELECT r_regionkey, r_name FROM {ds}.main.region");
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW all_regions AS {definition}"
        ))
        .expect("create");

    let plan = explain_lines(&runtime, &definition);
    assert!(
        !substitutes(&plan, "all_regions"),
        "the cost gate should decline a trivial passthrough; plan: {plan:?}"
    );
    // And the query still answers correctly, from the source.
    let (_, rows) = runtime.execute(&definition).expect("run");
    assert_eq!(int_string_rows(&rows).len(), 5);
}

#[test]
fn an_order_by_limit_view_declines_to_substitute() {
    // An ORDER BY ... LIMIT view collapses rows (5 base -> 3 stored), so the cost
    // gate would clear. It must still decline: after optimization the whole query
    // is rooted in a Projection over Limit over a Scan carrying the folded ORDER
    // BY / LIMIT, so ordering semantics live INSIDE the matched subtree, not at
    // its root. A chunk read is an unordered set, so serving the view would drop
    // the sort. The guard must walk the whole subtree and decline.
    let sandbox = Sandbox::new("orderlimit");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let definition = format!(
        "SELECT r_regionkey, r_name FROM {ds}.main.region ORDER BY r_regionkey DESC LIMIT 3"
    );
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW top_regions AS {definition}"
        ))
        .expect("create");

    // The structural pin: the ordered/limited shape does NOT substitute. The cold
    // plan shows the Limit and the folded ORDER BY the recompute preserves.
    let plan = explain_lines(&runtime, &definition);
    assert!(
        !substitutes(&plan, "top_regions"),
        "an ORDER BY + LIMIT view must not substitute; plan: {plan:?}"
    );
    assert!(
        plan.iter().any(|line| line.contains("Limit")),
        "expected the cold plan to keep the Limit; plan: {plan:?}"
    );
    assert!(
        plan.iter()
            .any(|line| line.contains(&format!("Scan [{ds}]"))),
        "expected a source scan when declined; plan: {plan:?}"
    );

    // The answer is ordered, so the identity contract includes ORDER: the served
    // rows equal the recompute rows position by position, keys descending.
    let (_, on_rows) = runtime.execute(&definition).expect("substitution on");
    runtime
        .execute("SET accelerator.enable_substitution = false")
        .expect("disable");
    assert!(!substitutes(
        &explain_lines(&runtime, &definition),
        "top_regions"
    ));
    let (_, off_rows) = runtime.execute(&definition).expect("substitution off");

    let on_ordered = int_string_rows(&on_rows);
    let off_ordered = int_string_rows(&off_rows);
    assert_eq!(on_ordered, off_ordered);
    assert_eq!(
        on_ordered,
        vec![
            (4, "MIDDLE EAST".to_string()),
            (3, "EUROPE".to_string()),
            (2, "ASIA".to_string()),
        ]
    );
}

#[test]
fn the_kill_switch_toggles_substitution_live() {
    let sandbox = Sandbox::new("killswitch");
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    runtime
        .execute(&format!(
            "CREATE MATERIALIZED VIEW region_count AS \
             SELECT count(*) AS c FROM {ds}.main.region"
        ))
        .expect("create");
    let query = format!("SELECT count(*) AS c FROM {ds}.main.region");

    assert!(substitutes(
        &explain_lines(&runtime, &query),
        "region_count"
    ));
    runtime
        .execute("SET accelerator.enable_substitution = false")
        .expect("disable");
    assert!(!substitutes(
        &explain_lines(&runtime, &query),
        "region_count"
    ));
    // RESET restores the default (on) on the live runtime.
    runtime
        .execute("RESET accelerator.enable_substitution")
        .expect("reset");
    assert!(substitutes(
        &explain_lines(&runtime, &query),
        "region_count"
    ));
}

/// The row for `view` in a `SHOW MATERIALIZED VIEWS` result:
/// (name, rows, bytes, use_count, cost_saved).
fn show_row(runtime: &Runtime, view: &str) -> (String, i64, i64, i64, f64) {
    let (schema, batches) = runtime
        .execute("SHOW MATERIALIZED VIEWS")
        .expect("show materialized views");
    assert_eq!(
        column_names(&schema),
        vec![
            "name",
            "rows",
            "bytes",
            "created",
            "refreshed",
            "use_count",
            "cost_saved"
        ]
    );
    let batch = batches.first().expect("one batch");
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let rows = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let bytes = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let use_counts = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let cost_saved = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for row in 0..batch.num_rows() {
        if names.value(row) == view {
            return (
                view.to_string(),
                rows.value(row),
                bytes.value(row),
                use_counts.value(row),
                cost_saved.value(row),
            );
        }
    }
    panic!("view {view} not in SHOW MATERIALIZED VIEWS");
}

/// The field names of a result schema.
fn column_names(schema: &SchemaRef) -> Vec<String> {
    let mut names = Vec::new();
    for field in schema.fields() {
        names.push(field.name().clone());
    }
    names
}

#[test]
fn benefit_counters_advance_and_persist_across_runtimes() {
    let sandbox = Sandbox::new("benefit");
    let ds = sandbox.datasource.clone();
    let query = format!("SELECT count(*) AS c FROM {ds}.main.region");
    {
        let runtime = sandbox.runtime();
        runtime
            .execute(&format!(
                "CREATE MATERIALIZED VIEW region_count AS \
                 SELECT count(*) AS c FROM {ds}.main.region"
            ))
            .expect("create");

        // Fresh view: no substitutions recorded yet.
        let (_, rows, _bytes, use_count, cost_saved) = show_row(&runtime, "region_count");
        assert_eq!(rows, 1);
        assert_eq!(use_count, 0);
        assert!(cost_saved.abs() < 1e-9);

        // Two served substitutions advance use_count and accrue the estimated
        // saving (base 5 - cached read 1 = 4 per reuse).
        runtime.execute(&query).expect("first substituted run");
        runtime.execute(&query).expect("second substituted run");
        let (_, _, _, use_count, cost_saved) = show_row(&runtime, "region_count");
        assert_eq!(use_count, 2);
        assert!((cost_saved - 8.0).abs() < 1e-9);

        // An EXPLAIN plans but does not serve, so it records no benefit.
        let _ = explain_lines(&runtime, &query);
        assert_eq!(show_row(&runtime, "region_count").3, 2);
    }
    // A NEW runtime over the same store sees the persisted counters.
    let runtime = sandbox.runtime();
    let (_, _, _, use_count, cost_saved) = show_row(&runtime, "region_count");
    assert_eq!(use_count, 2);
    assert!((cost_saved - 8.0).abs() < 1e-9);
}

/// Flatten `(user_id, name)` from a `(user_id, ts, name)` event result - columns
/// 0 (Int32) and 2 (Utf8), skipping the timestamp.
fn user_name_rows(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let users = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 user_id");
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 name");
        for row in 0..batch.num_rows() {
            rows.push((users.value(row), names.value(row).to_string()));
        }
    }
    rows
}

#[test]
fn an_event_view_is_a_substitution_candidate() {
    // Event views are stored as materialized views (plus role metadata), so they
    // are substitution candidates like any other view. Their chunks are sorted
    // by (entity, timestamp) - a permutation of the definition's rows, i.e. the
    // same SET - so substituting a matching plain SELECT is sound: the row set is
    // preserved, and an outer ORDER BY (above the substituted scan) fixes order.
    let sandbox = Sandbox::with_seed("eventview", EVENTS_SEED);
    let runtime = sandbox.runtime();
    let ds = &sandbox.datasource;
    let definition = format!("SELECT user_id, ts, name FROM {ds}.main.events WHERE device = 'ios'");
    runtime
        .execute(&format!(
            "CREATE EVENT VIEW ios_ev ENTITY user_id TIMESTAMP ts EVENT name AS {definition}"
        ))
        .expect("create event view");

    // The event view's definition matches and the gate clears (6 base > 4 read).
    assert!(substitutes(&explain_lines(&runtime, &definition), "ios_ev"));
    let (_, on_rows) = runtime.execute(&definition).expect("substituted");

    runtime
        .execute("SET accelerator.enable_substitution = false")
        .expect("disable");
    assert!(!substitutes(
        &explain_lines(&runtime, &definition),
        "ios_ev"
    ));
    let (_, off_rows) = runtime.execute(&definition).expect("unsubstituted");

    let mut on_sorted = user_name_rows(&on_rows);
    let mut off_sorted = user_name_rows(&off_rows);
    on_sorted.sort();
    off_sorted.sort();
    assert_eq!(on_sorted, off_sorted);
    assert_eq!(
        on_sorted,
        vec![
            (1, "activate".to_string()),
            (1, "signup".to_string()),
            (3, "activate".to_string()),
            (3, "signup".to_string()),
        ]
    );
}
