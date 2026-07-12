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

// --------------------------------------------------------------------------
// hand-built semi-join reduction (test_reduction_gate.py structural half)
// --------------------------------------------------------------------------

use std::collections::BTreeMap;

use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
use fq_plan::logical::JoinType;
use fq_plan::physical::{BuildSide, PhysicalHashJoin};

/// A qualified integer column reference.
fn col(table: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        name,
        Some(DataType::Integer),
    ))
}

/// A `col > 0` filter (makes a build side non-whole-domain).
fn positive_filter(table: &str, name: &str) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Gt,
        left: Box::new(col(table, name)),
        right: Box::new(Expr::Literal {
            value: LiteralValue::Integer(0),
            data_type: DataType::Integer,
        }),
    }
}

/// A configurable plain scan for reduction wiring (filter, estimate, NDV, kind).
#[allow(clippy::too_many_arguments)]
fn rscan(
    datasource: &str,
    table: &str,
    alias: &str,
    columns: &[&str],
    filters: Option<Expr>,
    estimated_rows: Option<u64>,
    ndv: &[(&str, i64)],
    kind: DatasourceKind,
) -> PhysicalPlan {
    let mut ndv_map = BTreeMap::new();
    for (name, value) in ndv {
        ndv_map.insert((*name).to_string(), *value);
    }
    PhysicalPlan::Scan(Box::new(PhysicalScan {
        datasource: datasource.to_string(),
        schema_name: "public".to_string(),
        table_name: table.to_string(),
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        filters,
        sample: None,
        group_by: None,
        grouping_sets: None,
        aggregates: None,
        output_names: None,
        alias: Some(alias.to_string()),
        limit: None,
        offset: 0,
        order_by_keys: None,
        order_by_ascending: None,
        order_by_nulls: None,
        distinct: false,
        dynamic_filter_keys: None,
        estimated_rows,
        column_ndv: if ndv_map.is_empty() {
            None
        } else {
            Some(ndv_map)
        },
        seeded_schema: None,
        datasource_kind: kind,
    }))
}

/// An INNER equi hash join over a big probe (left) and a small filtered build (right).
fn reduction_join(probe: PhysicalPlan, build: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::HashJoin(PhysicalHashJoin {
        left: Box::new(probe),
        right: Box::new(build),
        join_type: JoinType::Inner,
        left_keys: vec![col("p", "key")],
        right_keys: vec![col("b", "key")],
        build_side: BuildSide::Right,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

/// Find the single CollectDistinct step, if any.
fn collect_step(steps: &[Step]) -> Option<&Step> {
    steps
        .iter()
        .find(|step| matches!(step, Step::CollectDistinct { .. }))
}

/// Find the single InjectedScan step, if any.
fn injected_step(steps: &[Step]) -> Option<&Step> {
    steps
        .iter()
        .find(|step| matches!(step, Step::InjectedScan { .. }))
}

#[test]
fn filtered_build_reduces_the_big_probe() {
    // A small FILTERED pg build (NDV 2) vs a big duck probe (NDV 100000): the
    // reduction collects the build's distinct key and injects it into the probe.
    let probe = rscan(
        "duck",
        "lineitem",
        "p",
        &["key", "data"],
        None,
        Some(100_000),
        &[("key", 100_000)],
        DatasourceKind::DuckDb,
    );
    let build = rscan(
        "pg",
        "orders",
        "b",
        &["key"],
        Some(positive_filter("b", "key")),
        Some(2),
        &[("key", 2)],
        DatasourceKind::Postgres,
    );
    let built = build_steps(&reduction_join(probe, build)).expect("build_steps");

    let collect = collect_step(&built.steps).expect("a CollectDistinct step");
    let injected = injected_step(&built.steps).expect("an InjectedScan step");
    // The injection reduces the big DUCK probe on its "key" column, fed by the
    // build's collected keys.
    let Step::CollectDistinct { binding: keys, .. } = collect else {
        unreachable!()
    };
    match injected {
        Step::InjectedScan {
            datasource,
            inject_column,
            keys_from,
            inject_column_ndv,
            ..
        } => {
            assert_eq!(datasource, "duck");
            assert_eq!(inject_column, "key");
            assert_eq!(keys_from, keys, "injection reads the collected keys");
            assert_eq!(*inject_column_ndv, Some(100_000));
        }
        other => panic!("not an injected scan: {other:?}"),
    }
    // The build side is materialized (its keys are collected AND it feeds the join).
    let build_materialized = built.steps.iter().any(|step| {
        matches!(
            step,
            Step::SourceScan {
                materialize: true,
                ..
            }
        )
    });
    assert!(build_materialized, "steps: {:?}", built.steps);
}

#[test]
fn unfiltered_build_donates_whole_domain_and_declines() {
    // An UNFILTERED plain build donates its entire FK domain, so the injection keeps
    // every probe row (pure overhead): no reduction, both sides read in full.
    let probe = rscan(
        "duck",
        "lineitem",
        "p",
        &["key", "data"],
        None,
        Some(100_000),
        &[("key", 100_000)],
        DatasourceKind::DuckDb,
    );
    let build = rscan(
        "pg",
        "orders",
        "b",
        &["key"],
        None,
        Some(2),
        &[("key", 2)],
        DatasourceKind::Postgres,
    );
    let built = build_steps(&reduction_join(probe, build)).expect("build_steps");
    assert!(
        collect_step(&built.steps).is_none(),
        "no reduction: {:?}",
        built.steps
    );
    assert!(injected_step(&built.steps).is_none());
    // Both sides read plainly; the join still merges them.
    assert!(count_source_scans(&built.steps) >= 2);
    assert!(count_merges(&built.steps) >= 1);
}

#[test]
fn reduced_join_still_builds_the_hash_join_fragment() {
    // Reduction changes HOW the inputs are read, not that the coordinator join runs.
    let probe = rscan(
        "duck",
        "lineitem",
        "p",
        &["key", "data"],
        None,
        Some(100_000),
        &[("key", 100_000)],
        DatasourceKind::DuckDb,
    );
    let build = rscan(
        "pg",
        "orders",
        "b",
        &["key"],
        Some(positive_filter("b", "key")),
        Some(2),
        &[("key", 2)],
        DatasourceKind::Postgres,
    );
    let built = build_steps(&reduction_join(probe, build)).expect("build_steps");
    let has_hash_join = built
        .fragments
        .values()
        .any(|fragment| matches!(fragment, Fragment::HashJoin { .. }));
    assert!(has_hash_join, "fragments: {:?}", built.fragments);
}

// --------------------------------------------------------------------------
// M4d regression tests
// --------------------------------------------------------------------------

use fq_plan::physical::PhysicalNestedLoopJoin;

/// The raw_sql of the first SourceScan step.
fn first_source_sql(steps: &[Step]) -> String {
    steps
        .iter()
        .find_map(|step| match step {
            Step::SourceScan {
                scan:
                    fq_physical::steps::ScanSpec {
                        raw_sql: Some(sql), ..
                    },
                ..
            } => Some(sql.clone()),
            _ => None,
        })
        .expect("a raw_sql SourceScan")
}

#[test]
fn same_source_union_all_renders_a_remote_set_op_scan() {
    // FINDING 1: a same-source UNION ALL plans to a PhysicalRemoteSetOp; its source
    // SQL must render both branches joined by UNION ALL (previously NoSourceSql).
    let steps =
        steps_of("SELECT id FROM pg.public.orders UNION ALL SELECT id FROM pg.public.customer");
    let sql = first_source_sql(&steps).to_uppercase();
    assert!(sql.contains("UNION ALL"), "set-op sql: {sql}");
    assert!(sql.contains("ORDERS"), "set-op sql: {sql}");
    assert!(sql.contains("CUSTOMER"), "set-op sql: {sql}");
}

#[test]
fn mis_qualified_join_projection_raises() {
    // FINDING 2: both sides aliased "t" so the join output map yields a (t, b) that
    // the left side's alias map does not expose -> RAISE, never a wrong column.
    let left = rscan(
        "pg",
        "orders",
        "t",
        &["a"],
        None,
        None,
        &[],
        DatasourceKind::Postgres,
    );
    let right = rscan(
        "pg",
        "customer",
        "t",
        &["b"],
        None,
        None,
        &[],
        DatasourceKind::Postgres,
    );
    let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        left_keys: vec![col("t", "a")],
        right_keys: vec![col("t", "b")],
        build_side: BuildSide::Right,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let err = build_steps(&join).unwrap_err();
    assert!(
        matches!(
            err,
            fq_physical::steps::StepError::MissingColumnAlias { .. }
        ),
        "expected MissingColumnAlias, got {err:?}"
    );
}

#[test]
fn nested_loop_condition_with_unknown_column_raises() {
    // FINDING 2: a nested-loop condition referencing a column neither side exposes
    // must RAISE (two_sided), not resolve to a raw name.
    let left = rscan(
        "pg",
        "orders",
        "l",
        &["a"],
        None,
        None,
        &[],
        DatasourceKind::Postgres,
    );
    let right = rscan(
        "duck",
        "lineitem",
        "r",
        &["b"],
        None,
        None,
        &[],
        DatasourceKind::DuckDb,
    );
    let condition = Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(col("x", "foo")), // qualifier "x" is on neither side
        right: Box::new(col("l", "a")),
    };
    let join = PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: Some(condition),
        estimated_rows: None,
        estimate_defaults: None,
    });
    let err = build_steps(&join).unwrap_err();
    assert!(
        matches!(
            err,
            fq_physical::steps::StepError::MissingColumnAlias { .. }
        ),
        "expected MissingColumnAlias, got {err:?}"
    );
}

#[test]
fn self_join_projection_follows_schema_order_not_sorted_keys() {
    // FINDING 3: left alias "z" sorts AFTER right alias "a" in BTreeMap key order,
    // but the join projection must follow OUTPUT-SCHEMA order (left columns first),
    // so a parent reads the intended side.
    let left = rscan(
        "pg",
        "orders",
        "z",
        &["k", "v"],
        None,
        None,
        &[],
        DatasourceKind::Postgres,
    );
    let right = rscan(
        "pg",
        "customer",
        "a",
        &["k", "w"],
        None,
        None,
        &[],
        DatasourceKind::Postgres,
    );
    let join = PhysicalPlan::HashJoin(PhysicalHashJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        left_keys: vec![col("z", "k")],
        right_keys: vec![col("a", "k")],
        build_side: BuildSide::Right,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let built = build_steps(&join).expect("build_steps");
    let project = built
        .fragments
        .values()
        .find_map(|fragment| match fragment {
            Fragment::HashJoin { project, .. } => Some(project.clone()),
            _ => None,
        })
        .expect("a hash-join fragment");
    // Output-schema order: left "k","v" (in_left) then right renamed "right_k","w".
    let aliases: Vec<&str> = project.iter().map(|item| item.alias.as_str()).collect();
    assert_eq!(
        aliases,
        vec!["k", "v", "right_k", "w"],
        "project: {project:?}"
    );
    // The first column comes from the LEFT side, not the alphabetically-first right.
    match &project[0].expr {
        Expr::Column(reference) => {
            assert_eq!(reference.table.as_deref(), Some("in_left"));
            assert_eq!(reference.column, "k");
        }
        other => panic!("not a column: {other:?}"),
    }
}

// --------------------------------------------------------------------------
// hand-built window-over-GROUPING() aggregate: the two-stage split
// --------------------------------------------------------------------------

use fq_plan::expr::NullsOrder;
use fq_plan::physical::PhysicalHashAggregate;

/// A qualified, typed column reference.
fn agg_col(table: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef {
        table: Some(table.to_string()),
        column: name.to_string(),
        data_type: Some(DataType::BigInt),
    })
}

/// An aggregate function call over one argument (`sum(arg)` and friends).
fn agg_call(name: &str, arg: Expr) -> Expr {
    Expr::FunctionCall {
        function_name: name.to_string(),
        args: vec![arg],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// A `GROUPING(col)` call (an aggregate-scoped function, not an aggregate).
fn grouping_call(table: &str, name: &str) -> Expr {
    Expr::FunctionCall {
        function_name: "grouping".to_string(),
        args: vec![agg_col(table, name)],
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// A bare window function call (`rank()`), no arguments.
fn window_fn(name: &str) -> Expr {
    Expr::FunctionCall {
        function_name: name.to_string(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
    }
}

/// A scan on `item` exposing the columns the q86-shaped aggregate references.
fn item_scan() -> PhysicalPlan {
    pscan(
        "duck",
        "main",
        "item",
        &["i_category", "i_class", "ext_price"],
    )
}

/// The q86 shape: `rank() OVER (PARTITION BY grouping(i_category)+grouping(i_class)
/// ORDER BY sum(ext_price) DESC)` over a ROLLUP(i_category, i_class), plus a plain
/// `sum(ext_price)` output and a group-key passthrough.
fn window_over_grouping_aggregate() -> PhysicalPlan {
    let sum_ext = agg_call("sum", agg_col("item", "ext_price"));
    let partition = Expr::BinaryOp {
        op: BinaryOpType::Add,
        left: Box::new(grouping_call("item", "i_category")),
        right: Box::new(grouping_call("item", "i_class")),
    };
    let window = Expr::Window {
        function: Box::new(window_fn("rank")),
        partition_by: vec![partition],
        order_keys: vec![sum_ext.clone()],
        order_ascending: vec![false],
        order_nulls: vec![Some(NullsOrder::Last)],
        frame: None,
    };
    PhysicalPlan::HashAggregate(PhysicalHashAggregate {
        input: Box::new(item_scan()),
        group_by: vec![agg_col("item", "i_category"), agg_col("item", "i_class")],
        aggregates: vec![sum_ext, window, agg_col("item", "i_category")],
        output_names: vec![
            "total".to_string(),
            "rk".to_string(),
            "i_category".to_string(),
        ],
        grouping_sets: Some(vec![
            vec![agg_col("item", "i_category"), agg_col("item", "i_class")],
            vec![agg_col("item", "i_category")],
            vec![],
        ]),
        group_observation: None,
    })
}

/// The raw-SQL strings of every `RawSql` fragment, in fragment-name (emission) order.
fn raw_sql_fragments(built: &fq_physical::steps::BuiltSteps) -> Vec<String> {
    let mut sqls = Vec::new();
    for fragment in built.fragments.values() {
        if let Fragment::RawSql { sql } = fragment {
            sqls.push(sql.clone());
        }
    }
    sqls
}

#[test]
fn window_over_grouping_splits_into_two_stages() {
    let plan = window_over_grouping_aggregate();
    let built = build_steps(&plan).expect("build_steps");
    let raws = raw_sql_fragments(&built);
    assert_eq!(raws.len(), 2, "expected two raw-SQL fragments: {raws:?}");
    let stage1 = raws[0].to_lowercase();
    let stage2 = raws[1].to_lowercase();
    // Stage 1 is the GROUP BY: it computes the grouping() operands as columns (a
    // hidden `__w` output) and carries no window.
    assert!(stage1.contains("group by"), "stage1: {}", raws[0]);
    assert!(stage1.contains("grouping"), "stage1: {}", raws[0]);
    assert!(stage1.contains("__w"), "stage1: {}", raws[0]);
    assert!(
        !stage1.contains("over"),
        "stage1 carries a window: {}",
        raws[0]
    );
    // Stage 2 runs the window over stage-1 columns: no GROUPING()/GROUP BY, the
    // rank window reads the materialized `__w` partition columns.
    assert!(stage2.contains("rank"), "stage2: {}", raws[1]);
    assert!(stage2.contains("over"), "stage2: {}", raws[1]);
    assert!(stage2.contains("__w"), "stage2: {}", raws[1]);
    assert!(
        !stage2.contains("grouping("),
        "stage2 re-runs grouping: {}",
        raws[1]
    );
    assert!(
        !stage2.contains("group by"),
        "stage2 re-groups: {}",
        raws[1]
    );
}

#[test]
fn window_over_grouping_wires_stage2_over_stage1() {
    let plan = window_over_grouping_aggregate();
    let built = build_steps(&plan).expect("build_steps");
    // The two merge steps chain: stage 1 reads the scan binding, stage 2 reads
    // stage 1's binding under `in_0`, and the Return names stage 2's output.
    let merges: Vec<(&String, &BTreeMap<String, String>, &String)> = built
        .steps
        .iter()
        .filter_map(|step| match step {
            Step::Merge {
                fragment,
                inputs,
                binding,
            } => Some((fragment, inputs, binding)),
            _ => None,
        })
        .collect();
    assert_eq!(merges.len(), 2, "steps: {:?}", built.steps);
    let (_stage1_fragment, _stage1_inputs, stage1_binding) = merges[0];
    let (_stage2_fragment, stage2_inputs, stage2_binding) = merges[1];
    assert_eq!(
        stage2_inputs.get("in_0"),
        Some(stage1_binding),
        "stage 2 must read stage 1's output: {:?}",
        built.steps
    );
    let returns_stage2 = built
        .steps
        .iter()
        .any(|step| matches!(step, Step::Return { input } if input == stage2_binding));
    assert!(
        returns_stage2,
        "Return must name stage 2: {:?}",
        built.steps
    );
}

#[test]
fn plain_window_aggregate_stays_one_fragment() {
    // A window OUTPUT with no GROUPING() runs fused as a single raw-SQL aggregate
    // (no split) - pins that the split path does not touch the plain window case.
    let running_sum = Expr::Window {
        function: Box::new(agg_call("sum", agg_col("item", "ext_price"))),
        partition_by: vec![],
        order_keys: vec![agg_col("item", "i_category")],
        order_ascending: vec![true],
        order_nulls: vec![Some(NullsOrder::Last)],
        frame: None,
    };
    let plan = PhysicalPlan::HashAggregate(PhysicalHashAggregate {
        input: Box::new(item_scan()),
        group_by: vec![agg_col("item", "i_category")],
        aggregates: vec![running_sum, agg_col("item", "i_category")],
        output_names: vec!["running".to_string(), "i_category".to_string()],
        grouping_sets: None,
        group_observation: None,
    });
    let built = build_steps(&plan).expect("build_steps");
    let raws = raw_sql_fragments(&built);
    assert_eq!(raws.len(), 1, "plain window must stay fused: {raws:?}");
    assert!(raws[0].to_lowercase().contains("over"), "sql: {}", raws[0]);
}

#[test]
fn window_function_that_is_an_aggregate_over_grouping_raises() {
    // The split rewrites each operand to a stage-1 column; a window whose FUNCTION
    // is itself an aggregate would become a bare `col OVER (...)`, which it cannot
    // soundly express, so it RAISES rather than mis-render.
    let window = Expr::Window {
        function: Box::new(agg_call("sum", agg_col("item", "ext_price"))),
        partition_by: vec![grouping_call("item", "i_category")],
        order_keys: vec![],
        order_ascending: vec![],
        order_nulls: vec![],
        frame: None,
    };
    let plan = PhysicalPlan::HashAggregate(PhysicalHashAggregate {
        input: Box::new(item_scan()),
        group_by: vec![agg_col("item", "i_category")],
        aggregates: vec![window],
        output_names: vec!["x".to_string()],
        grouping_sets: None,
        group_observation: None,
    });
    let error = build_steps(&plan).expect_err("an unsound split must raise");
    assert!(
        matches!(error, fq_physical::steps::StepError::WindowSplitUnsupported),
        "expected WindowSplitUnsupported, got {error:?}"
    );
}
