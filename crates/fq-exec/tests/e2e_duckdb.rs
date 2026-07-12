//! End-to-end proof: the WHOLE Rust pipeline executes a query end to end against
//! a real DuckDB fixture and returns correct Arrow rows.
//!
//! Flow: build a catalog from a real `.duckdb` file (fq-connectors `DuckDbSource`
//! metadata), drive `parse -> bind -> decorrelate -> optimize -> plan` on that
//! catalog, register the same file as a native source in fq-exec, then run
//! `execute_plan` and assert the returned schema + rows.
//!
//! Gated: the whole test early-returns if the fixture is absent (mirrors the
//! fq-connectors DB-backed tests), so a checkout without the benchmark data still
//! passes `cargo test`.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use fq_catalog::Catalog;
use fq_common::{CostConfig, OptimizerConfig};
use fq_connectors::DuckDbSource;
use fq_exec::{connectors, execute_plan};
use fq_optimize::{build_optimizer, CostModel};
use fq_parse::parse_with_catalog;
use fq_physical::PhysicalPlanner;
use fq_plan::physical::PhysicalPlan;

/// The smallest bundled DuckDB fixture (TPC-H at SF 0.01: region has 5 rows,
/// nation 25). Absolute, relative to this crate's manifest.
fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/tpch/data/tpch_sf0.01.duckdb")
}

/// Drive parse -> bind -> decorrelate -> optimize into an optimized logical plan.
fn optimize(catalog: &Catalog, sql: &str) -> fq_plan::logical::LogicalPlan {
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    let decorrelated = fq_decorrelate::decorrelate(bound).expect("decorrelate");
    let cost_model = CostModel::new(CostConfig::default(), None);
    let optimizer = build_optimizer(&OptimizerConfig::default(), cost_model);
    optimizer.optimize(decorrelated).expect("optimize")
}

/// Plan one query end to end into a physical tree against the shared catalog.
fn plan_sql(catalog: &Arc<Catalog>, sql: &str) -> PhysicalPlan {
    let logical = optimize(catalog, sql);
    let mut planner = PhysicalPlanner::new(Arc::clone(catalog), None);
    planner.plan(&logical).expect("plan")
}

/// Flatten every batch's two columns into `(i32, String)` rows, in batch order.
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

/// Flatten every batch's two columns into `(i32, i64)` rows, in batch order.
fn int_count_rows(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column 0 is Int32");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 1 is Int64");
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), counts.value(row)));
        }
    }
    rows
}

#[test]
fn runs_projection_filter_limit_and_group_by_on_real_duckdb() {
    let path = fixture_path();
    if !path.exists() {
        eprintln!("skipping e2e_duckdb: fixture {} absent", path.display());
        return;
    }
    let path = path
        .to_str()
        .expect("fixture path is valid UTF-8")
        .to_string();

    // Two queries: a projection+filter+ORDER BY+LIMIT, and a small GROUP BY. Both
    // planned WHILE the DuckDbSource holds the file (planning reads metadata and
    // stats); the source is then dropped so the engine can open the same file.
    let sql_projection = "SELECT r_regionkey, r_name FROM duck.main.region \
         WHERE r_regionkey < 4 ORDER BY r_regionkey LIMIT 3";
    let sql_group_by = "SELECT n_regionkey, count(*) AS c FROM duck.main.nation \
         GROUP BY n_regionkey ORDER BY n_regionkey";

    let (plan_projection, plan_group_by) = {
        let source = DuckDbSource::open("duck", &path).expect("open fixture");
        let mut catalog = Catalog::new();
        catalog.register_datasource(Arc::new(source));
        catalog.load_metadata().expect("load metadata");
        let catalog = Arc::new(catalog);
        (
            plan_sql(&catalog, sql_projection),
            plan_sql(&catalog, sql_group_by),
        )
        // catalog (and the DuckDbSource connection) drops here, releasing the
        // file lock before fq-exec opens the same database read-write.
    };

    // Register the same file as the native execution source, under the name the
    // physical scans carry ("duck").
    connectors::register(
        "duck".to_string(),
        connectors::spec_from_kind("duckdb", Some(path.clone()), None).expect("spec"),
    );

    // Projection + filter + limit: exactly the three lowest regions, in order.
    let (_schema, batches, _observations) =
        execute_plan(&plan_projection).expect("execute projection");
    let rows = int_string_rows(&batches);
    assert_eq!(
        rows,
        vec![
            (0, "AFRICA".to_string()),
            (1, "AMERICA".to_string()),
            (2, "ASIA".to_string()),
        ],
        "projection+filter+limit returned the wrong rows"
    );

    // GROUP BY: five regions, five nations each (standard TPC-H distribution).
    let (_schema, batches, _observations) = execute_plan(&plan_group_by).expect("execute group by");
    let rows = int_count_rows(&batches);
    assert_eq!(
        rows,
        vec![(0, 5), (1, 5), (2, 5), (3, 5), (4, 5)],
        "group-by counts per region are wrong"
    );
}
