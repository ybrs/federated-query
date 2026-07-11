//! Step-building behavioral tests at the Vec<Step> / Fragment level. Real SQL is
//! driven parse -> bind -> decorrelate -> optimize -> plan -> build_steps, then the
//! produced steps/fragments are asserted. Translates the structural half of
//! `tests/test_ship_emit.py`, `tests/e2e_pushdown/test_injected_scan_cse.py`, and
//! the node-dispatch coverage.

use std::sync::Arc;

use fq_catalog::datasource::{DataSource, DataSourceCapability, TableMetadata, TableStatistics};
use fq_catalog::{Catalog, CatalogError, Column, Schema, Table};
use fq_common::{CostConfig, DataType, OptimizerConfig};
use fq_optimize::{build_optimizer, CostModel};
use fq_parse::parse_with_catalog;
use fq_physical::steps::{build_steps, Fragment, JoinKind, Step};
use fq_physical::PhysicalPlanner;
use fq_plan::logical::LogicalPlan;
use fq_plan::physical::{DatasourceKind, PhysicalPlan, PhysicalScan, PhysicalShipment};

// --------------------------------------------------------------------------
// Catalog + mock datasource
// --------------------------------------------------------------------------

/// A minimal metadata-only source; capabilities are configurable.
struct MockSource {
    name: String,
    capabilities: Vec<DataSourceCapability>,
}

impl DataSource for MockSource {
    fn name(&self) -> &str {
        &self.name
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        self.capabilities.clone()
    }
    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    fn list_tables(&self, _schema: &str) -> Result<Vec<String>, CatalogError> {
        Ok(vec![])
    }
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata, CatalogError> {
        Ok(TableMetadata {
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            columns: vec![],
            row_count: None,
            size_bytes: None,
        })
    }
    fn get_table_statistics(
        &self,
        _schema: &str,
        _table: &str,
        _columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        Ok(None)
    }
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        fq_catalog::datasource::map_native_type_default(type_str)
    }
}

/// A two-source catalog: `pg.public` (orders, customer) and `duck.main`
/// (lineitem, part). Both sources support JOINS.
fn catalog() -> Arc<Catalog> {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("cust_id", DataType::Integer, true),
            Column::new("region", DataType::Varchar, true),
            Column::new("amount", DataType::Double, false),
        ],
    );
    let customer = Table::new(
        "customer",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("nation", DataType::Varchar, true),
        ],
    );
    let lineitem = Table::new(
        "lineitem",
        vec![
            Column::new("l_orderkey", DataType::Integer, false),
            Column::new("l_quantity", DataType::Double, false),
        ],
    );
    let part = Table::new(
        "part",
        vec![
            Column::new("p_partkey", DataType::Integer, false),
            Column::new("p_name", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, customer]),
    );
    catalog.insert_schema(
        "duck",
        "main",
        Schema::with_tables("main", "duck", vec![lineitem, part]),
    );
    catalog.register_datasource(Arc::new(MockSource {
        name: "pg".to_string(),
        capabilities: vec![DataSourceCapability::Joins],
    }));
    catalog.register_datasource(Arc::new(MockSource {
        name: "duck".to_string(),
        capabilities: vec![DataSourceCapability::Joins],
    }));
    Arc::new(catalog)
}

/// Parse -> bind -> decorrelate -> optimize a query against the shared catalog.
fn optimize(catalog: &Catalog, sql: &str) -> LogicalPlan {
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    let decorrelated = fq_decorrelate::decorrelate(bound).expect("decorrelate");
    let cost_model = CostModel::new(CostConfig::default(), None);
    let optimizer = build_optimizer(&OptimizerConfig::default(), cost_model);
    optimizer.optimize(decorrelated).expect("optimize")
}

/// Plan a query end to end, returning the physical tree.
fn plan_sql(catalog: &Arc<Catalog>, sql: &str) -> PhysicalPlan {
    let logical = optimize(catalog, sql);
    let mut planner = PhysicalPlanner::new(Arc::clone(catalog), None);
    planner.plan(&logical).expect("plan")
}

/// Steps for a query end to end.
fn steps_of(sql: &str) -> Vec<Step> {
    let catalog = catalog();
    let plan = plan_sql(&catalog, sql);
    build_steps(&plan).expect("build_steps").steps
}

/// The final `Return` step's input binding.
fn return_input(steps: &[Step]) -> String {
    match steps.last() {
        Some(Step::Return { input }) => input.clone(),
        other => panic!("last step is not Return: {other:?}"),
    }
}

/// The count of a given step kind.
fn count_source_scans(steps: &[Step]) -> usize {
    steps
        .iter()
        .filter(|step| matches!(step, Step::SourceScan { .. }))
        .count()
}

fn count_merges(steps: &[Step]) -> usize {
    steps
        .iter()
        .filter(|step| matches!(step, Step::Merge { .. }))
        .count()
}

// --------------------------------------------------------------------------
// tests
// --------------------------------------------------------------------------

#[test]
fn single_table_scan_emits_source_scan_then_return() {
    let steps = steps_of("SELECT id, cust_id FROM pg.public.orders");
    // Exactly one source read, and the plan terminates in a Return whose input is a
    // real producing binding (either the scan itself or a projection over it).
    assert_eq!(count_source_scans(&steps), 1);
    assert!(matches!(steps[0], Step::SourceScan { .. }));
    let final_binding = return_input(&steps);
    let produced = steps
        .iter()
        .any(|step| step.binding() == Some(&final_binding));
    assert!(
        produced,
        "Return input {final_binding} has no producer: {steps:?}"
    );
}

#[test]
fn cross_source_join_reads_both_sides_and_merges() {
    // orders on pg, lineitem on duck: a cross-source hash join.
    let steps = steps_of(
        "SELECT o.id, l.l_quantity FROM pg.public.orders o \
         JOIN duck.main.lineitem l ON o.id = l.l_orderkey",
    );
    // Two source scans (one per source) and at least one merge (the join).
    assert!(count_source_scans(&steps) >= 2, "steps: {steps:?}");
    assert!(count_merges(&steps) >= 1, "steps: {steps:?}");
}

#[test]
fn cross_source_join_fragment_is_a_hash_join() {
    let catalog = catalog();
    let plan = plan_sql(
        &catalog,
        "SELECT o.id, l.l_quantity FROM pg.public.orders o \
         JOIN duck.main.lineitem l ON o.id = l.l_orderkey",
    );
    let built = build_steps(&plan).expect("build_steps");
    let has_hash_join = built.fragments.values().any(|fragment| {
        matches!(
            fragment,
            Fragment::HashJoin {
                join_type: JoinKind::Inner,
                ..
            }
        )
    });
    assert!(has_hash_join, "fragments: {:?}", built.fragments);
}

#[test]
fn cross_source_aggregate_emits_aggregate_fragment() {
    let catalog = catalog();
    let plan = plan_sql(
        &catalog,
        "SELECT o.region, count(*) FROM pg.public.orders o \
         JOIN duck.main.lineitem l ON o.id = l.l_orderkey GROUP BY o.region",
    );
    let built = build_steps(&plan).expect("build_steps");
    let has_aggregate = built
        .fragments
        .values()
        .any(|fragment| matches!(fragment, Fragment::Aggregate { .. }));
    assert!(has_aggregate, "fragments: {:?}", built.fragments);
}

#[test]
fn cross_source_order_by_limit_emit_sort_and_limit() {
    let catalog = catalog();
    let plan = plan_sql(
        &catalog,
        "SELECT o.id, l.l_quantity FROM pg.public.orders o \
         JOIN duck.main.lineitem l ON o.id = l.l_orderkey \
         ORDER BY o.id LIMIT 5",
    );
    let built = build_steps(&plan).expect("build_steps");
    let has_sort = built
        .fragments
        .values()
        .any(|fragment| matches!(fragment, Fragment::Sort { .. }));
    let has_limit = built
        .fragments
        .values()
        .any(|fragment| matches!(fragment, Fragment::Limit { .. }));
    assert!(has_sort, "fragments: {:?}", built.fragments);
    assert!(has_limit, "fragments: {:?}", built.fragments);
}

#[test]
fn outputs_are_the_plan_output_names() {
    let catalog = catalog();
    let plan = plan_sql(&catalog, "SELECT id, cust_id FROM pg.public.orders");
    let built = build_steps(&plan).expect("build_steps");
    assert_eq!(built.outputs, vec!["id".to_string(), "cust_id".to_string()]);
}

// --------------------------------------------------------------------------
// hand-built PhysicalShipment: ship-emit ordering (test_ship_emit.py)
// --------------------------------------------------------------------------

/// A plain scan on a source, aliased to its table name.
fn pscan(datasource: &str, schema: &str, table: &str, columns: &[&str]) -> PhysicalPlan {
    PhysicalPlan::Scan(Box::new(PhysicalScan {
        datasource: datasource.to_string(),
        schema_name: schema.to_string(),
        table_name: table.to_string(),
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        filters: None,
        sample: None,
        group_by: None,
        grouping_sets: None,
        aggregates: None,
        output_names: None,
        alias: Some(table.to_string()),
        limit: None,
        offset: 0,
        order_by_keys: None,
        order_by_ascending: None,
        order_by_nulls: None,
        distinct: false,
        dynamic_filter_keys: None,
        estimated_rows: None,
        column_ndv: None,
        seeded_schema: None,
        datasource_kind: DatasourceKind::DuckDb,
    }))
}

#[test]
fn shipment_emits_body_then_ship_then_island_in_order() {
    // Ship a pg dimension body into duck, then read the duck island.
    let body = pscan("pg", "public", "customer", &["id", "nation"]);
    let child = pscan("duck", "main", "lineitem", &["l_orderkey", "l_quantity"]);
    let shipment = PhysicalPlan::Shipment(PhysicalShipment {
        table: "shipped_customer".to_string(),
        datasource: "duck".to_string(),
        body: Box::new(body),
        child: Box::new(child),
    });
    let built = build_steps(&shipment).expect("build_steps");
    // Order: SourceScan (body), Ship, SourceScan (island), Return.
    assert!(
        matches!(built.steps[0], Step::SourceScan { .. }),
        "steps: {:?}",
        built.steps
    );
    assert!(
        matches!(built.steps[1], Step::Ship { .. }),
        "steps: {:?}",
        built.steps
    );
    assert!(
        matches!(built.steps[2], Step::SourceScan { .. }),
        "steps: {:?}",
        built.steps
    );
    assert!(matches!(built.steps[3], Step::Return { .. }));
    // The ship targets the right table on the right source.
    match &built.steps[1] {
        Step::Ship {
            datasource, table, ..
        } => {
            assert_eq!(datasource, "duck");
            assert_eq!(table, "shipped_customer");
        }
        other => panic!("not a ship: {other:?}"),
    }
}

#[test]
fn identical_scans_share_one_read() {
    // Two structurally identical scans (a self-union) share one source read via CSE.
    let left = pscan("duck", "main", "lineitem", &["l_orderkey"]);
    let right = pscan("duck", "main", "lineitem", &["l_orderkey"]);
    let union = PhysicalPlan::Union(fq_plan::physical::PhysicalUnion {
        inputs: vec![left, right],
        distinct: false,
    });
    let built = build_steps(&union).expect("build_steps");
    // One shared source scan, not two.
    assert_eq!(
        count_source_scans(&built.steps),
        1,
        "steps: {:?}",
        built.steps
    );
    // The union raw_sql fragment still references two inputs.
    let raw = built
        .fragments
        .values()
        .find_map(|fragment| match fragment {
            Fragment::RawSql { sql } => Some(sql.clone()),
            _ => None,
        })
        .expect("a raw_sql union fragment");
    assert!(raw.contains("UNION ALL"), "union sql: {raw}");
}
