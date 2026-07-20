//! Physical-planner behavioral tests at the PhysicalPlan-TREE level. Real SQL is
//! driven parse -> bind -> decorrelate -> optimize -> plan for the cross-source
//! join / set-op / CTE paths; the raise sites and fold-in-place lowerings are
//! driven from hand-built logical plans (their exact SQL shape is not the point).
//!
//! The tests target the planner's own cross-source logic: the shapes used here
//! make single-source pushdown decline, so a single-source subtree lowers to
//! LOCAL nodes (a plain `PhysicalScan`, etc.).

use std::sync::Arc;

use fq_catalog::datasource::{DataSource, DataSourceCapability, TableMetadata, TableStatistics};
use fq_catalog::{Catalog, CatalogError, Column, Schema, Table};
use fq_common::{CostConfig, DataType, OptimizerConfig};
use fq_optimize::{build_optimizer, CostModel};
use fq_parse::parse_with_catalog;
use fq_physical::error::PhysicalError;
use fq_physical::PhysicalPlanner;
use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
use fq_plan::logical::{
    Cte, CteRef, GroupedLimit, Join, JoinType, LateralJoin, Limit, LogicalPlan, Projection, Scan,
    SetOpKind, SetOperation, SingleRowGuard, Sort, Values,
};
use fq_plan::physical::{BuildSide, PhysicalPlan};

// --------------------------------------------------------------------------
// Catalog + mock datasource
// --------------------------------------------------------------------------

/// A minimal metadata-only source; capabilities are configurable so a test can
/// toggle JOIN pushdown eligibility.
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

/// Plan a query end to end, returning the physical tree (no cost model).
fn plan_sql(catalog: &Arc<Catalog>, sql: &str) -> PhysicalPlan {
    let logical = optimize(catalog, sql);
    let mut planner = PhysicalPlanner::new(Arc::clone(catalog), None);
    planner.plan(&logical).expect("plan")
}

/// Plan a hand-built logical plan with no cost model.
fn plan_logical(
    catalog: &Arc<Catalog>,
    logical: &LogicalPlan,
) -> Result<PhysicalPlan, PhysicalError> {
    let mut planner = PhysicalPlanner::new(Arc::clone(catalog), None);
    planner.plan(logical)
}

/// Every node in a physical tree that satisfies `predicate`.
fn find_all<'a>(
    plan: &'a PhysicalPlan,
    predicate: &dyn Fn(&PhysicalPlan) -> bool,
) -> Vec<&'a PhysicalPlan> {
    let mut out = Vec::new();
    collect(plan, predicate, &mut out);
    out
}

fn collect<'a>(
    plan: &'a PhysicalPlan,
    predicate: &dyn Fn(&PhysicalPlan) -> bool,
    out: &mut Vec<&'a PhysicalPlan>,
) {
    if predicate(plan) {
        out.push(plan);
    }
    for child in plan.children() {
        collect(child, predicate, out);
    }
}

// --------------------------------------------------------------------------
// hand-built logical-plan helpers
// --------------------------------------------------------------------------

/// A qualified integer column reference.
fn col(table: &str, name: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        name,
        Some(DataType::Integer),
    ))
}

/// A base scan on a source, aliased to its table name.
fn lscan(datasource: &str, schema: &str, table: &str, columns: &[&str]) -> LogicalPlan {
    let mut scan = Scan::new(
        datasource,
        schema,
        table,
        columns.iter().map(|c| (*c).to_string()).collect(),
    );
    scan.alias = Some(table.to_string());
    LogicalPlan::Scan(Box::new(scan))
}

/// An equality predicate `left = right`.
fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

// --------------------------------------------------------------------------
// cross-source join lowering
// --------------------------------------------------------------------------

#[test]
fn cross_source_inner_join_lowers_to_hash_join_with_dynamic_filter() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT orders.id FROM pg.public.orders \
         JOIN duck.main.lineitem ON orders.id = lineitem.l_orderkey",
    );
    let joins = find_all(&physical, &|node| matches!(node, PhysicalPlan::HashJoin(_)));
    assert_eq!(joins.len(), 1, "one cross-source hash join");
    let PhysicalPlan::HashJoin(hash_join) = joins[0] else {
        unreachable!()
    };
    assert_eq!(hash_join.left_keys.len(), 1);
    assert_eq!(hash_join.right_keys.len(), 1);
    // No estimates and no selective filter -> the structural default builds right.
    assert_eq!(hash_join.build_side, BuildSide::Right);
    // The probe (left) scan carries the injected dynamic filter key; the build
    // (right) scan does not.
    let PhysicalPlan::Scan(left_scan) = hash_join.left.as_ref() else {
        panic!("probe side is a scan")
    };
    assert_eq!(left_scan.table_name, "orders");
    assert!(left_scan.dynamic_filter_keys.is_some());
    let PhysicalPlan::Scan(right_scan) = hash_join.right.as_ref() else {
        panic!("build side is a scan")
    };
    assert!(right_scan.dynamic_filter_keys.is_none());
}

#[test]
fn cross_source_non_equi_join_lowers_to_nested_loop() {
    // A hand-built cross-source join with a non-equi condition: no equi-keys, so
    // the planner falls back to a nested loop.
    let catalog = catalog();
    let join = LogicalPlan::Join(Join {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        join_type: JoinType::Inner,
        condition: Some(Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("orders", "id")),
            right: Box::new(col("lineitem", "l_orderkey")),
        }),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let physical = plan_logical(&catalog, &join).expect("plan");
    assert!(matches!(physical, PhysicalPlan::NestedLoopJoin(_)));
}

#[test]
fn same_source_join_pushes_to_one_remote_query() {
    // Single-source pushdown fires FIRST and absorbs the whole
    // projection-over-join subtree into one PhysicalRemoteQuery.
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT orders.id FROM pg.public.orders \
         JOIN pg.public.customer ON orders.cust_id = customer.id",
    );
    let remotes = find_all(&physical, &|node| {
        matches!(node, PhysicalPlan::RemoteQuery(_))
    });
    assert_eq!(
        remotes.len(),
        1,
        "same-source join collapses to one remote query"
    );
    let PhysicalPlan::RemoteQuery(remote) = remotes[0] else {
        unreachable!()
    };
    assert_eq!(remote.datasource, "pg");
    assert!(
        remote.sql.contains("JOIN"),
        "the one remote query joins both tables: {}",
        remote.sql
    );
    // No cross-source join node survives - the whole thing is pushed.
    assert!(find_all(&physical, &|n| matches!(n, PhysicalPlan::RemoteJoin(_))).is_empty());
    assert!(find_all(&physical, &|n| matches!(n, PhysicalPlan::HashJoin(_))).is_empty());
}

#[test]
fn same_source_join_declines_remote_when_source_lacks_join_capability() {
    // A catalog whose pg source does NOT support JOINS: the same-source join must
    // fall back to a local hash join instead of a remote join.
    let orders = Table::new("orders", vec![Column::new("id", DataType::Integer, false)]);
    let customer = Table::new(
        "customer",
        vec![Column::new("id", DataType::Integer, false)],
    );
    let mut cat = Catalog::new();
    cat.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, customer]),
    );
    cat.register_datasource(Arc::new(MockSource {
        name: "pg".to_string(),
        capabilities: vec![],
    }));
    let catalog = Arc::new(cat);
    let physical = plan_sql(
        &catalog,
        "SELECT orders.id FROM pg.public.orders \
         JOIN pg.public.customer ON orders.id = customer.id",
    );
    assert!(find_all(&physical, &|n| matches!(n, PhysicalPlan::RemoteJoin(_))).is_empty());
    assert_eq!(
        find_all(&physical, &|n| matches!(n, PhysicalPlan::HashJoin(_))).len(),
        1
    );
}

// --------------------------------------------------------------------------
// raise sites
// --------------------------------------------------------------------------

#[test]
fn cross_source_natural_using_join_raises() {
    // A cross-source join with no ON condition (NATURAL) cannot be keyed by the
    // merge engine; a conditionless nested loop would be a silent cross product.
    let catalog = catalog();
    let join = LogicalPlan::Join(Join {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        join_type: JoinType::Inner,
        condition: None,
        natural: true,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let error = plan_logical(&catalog, &join).unwrap_err();
    assert!(matches!(error, PhysicalError::CrossSourceNaturalUsingJoin));
}

#[test]
fn unorientable_join_keys_raise() {
    // A cross-source equi-join whose keys resolve to neither side by qualifier or
    // by bare column name.
    let catalog = catalog();
    let join = LogicalPlan::Join(Join {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        join_type: JoinType::Inner,
        condition: Some(eq(col("ghost", "x"), col("phantom", "y"))),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let error = plan_logical(&catalog, &join).unwrap_err();
    match error {
        PhysicalError::UnorientableJoinKeys { first, second } => {
            assert_eq!(first, "x");
            assert_eq!(second, "y");
        }
        other => panic!("expected UnorientableJoinKeys, got {other:?}"),
    }
}

#[test]
fn missing_datasource_raises() {
    let catalog = catalog();
    let scan = lscan("nowhere", "public", "orders", &["id"]);
    let error = plan_logical(&catalog, &scan).unwrap_err();
    match error {
        PhysicalError::DatasourceNotFound(name) => assert_eq!(name, "nowhere"),
        other => panic!("expected DatasourceNotFound, got {other:?}"),
    }
}

#[test]
fn cte_ref_without_producer_raises() {
    let catalog = catalog();
    let dangling = LogicalPlan::CteRef(CteRef {
        name: "missing".to_string(),
        alias: None,
        columns: Some(vec!["id".to_string()]),
        output_names: Some(vec!["id".to_string()]),
    });
    let error = plan_logical(&catalog, &dangling).unwrap_err();
    match error {
        PhysicalError::CteNotInScope { name } => assert_eq!(name, "missing"),
        other => panic!("expected CteNotInScope, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// set operations
// --------------------------------------------------------------------------

#[test]
fn same_source_union_pushes_to_a_remote_set_op() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT id FROM pg.public.orders UNION ALL SELECT id FROM pg.public.customer",
    );
    let remotes = find_all(&physical, &|n| matches!(n, PhysicalPlan::RemoteSetOp(_)));
    assert_eq!(remotes.len(), 1, "same-source union pushes down");
    let PhysicalPlan::RemoteSetOp(remote) = remotes[0] else {
        unreachable!()
    };
    assert_eq!(remote.kind, SetOpKind::Union);
    assert!(!remote.distinct, "UNION ALL is not distinct");
}

#[test]
fn cross_source_union_stays_local() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT id FROM pg.public.orders UNION ALL SELECT l_orderkey FROM duck.main.lineitem",
    );
    assert_eq!(
        find_all(&physical, &|n| matches!(n, PhysicalPlan::Union(_))).len(),
        1
    );
    assert!(find_all(&physical, &|n| matches!(n, PhysicalPlan::RemoteSetOp(_))).is_empty());
}

#[test]
fn cross_source_intersect_stays_a_local_set_operation() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT id FROM pg.public.orders INTERSECT SELECT l_orderkey FROM duck.main.lineitem",
    );
    let ops = find_all(&physical, &|n| matches!(n, PhysicalPlan::SetOperation(_)));
    assert_eq!(ops.len(), 1);
    let PhysicalPlan::SetOperation(op) = ops[0] else {
        unreachable!()
    };
    assert_eq!(op.kind, SetOpKind::Intersect);
}

#[test]
fn limit_folds_into_a_pushed_remote_set_op() {
    // A LIMIT directly over a same-source set operation folds into the pushed
    // remote set op rather than wrapping it in a PhysicalLimit.
    let catalog = catalog();
    let set_op = LogicalPlan::SetOperation(SetOperation {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("pg", "public", "customer", &["id"])),
        kind: SetOpKind::Union,
        distinct: true,
    });
    let limited = LogicalPlan::Limit(Limit {
        input: Box::new(set_op),
        limit: Some(5),
        offset: 0,
    });
    let physical = plan_logical(&catalog, &limited).expect("plan");
    let PhysicalPlan::RemoteSetOp(remote) = &physical else {
        panic!("limit should fold into the remote set op, got {physical:?}")
    };
    assert_eq!(remote.limit, Some(5));
}

// --------------------------------------------------------------------------
// sort folding
// --------------------------------------------------------------------------

#[test]
fn sort_folds_onto_a_scan() {
    // A Sort directly over a single-source scan folds its ORDER BY onto the scan.
    let catalog = catalog();
    let sort = LogicalPlan::Sort(Sort {
        input: Box::new(lscan("pg", "public", "orders", &["id"])),
        sort_keys: vec![col("orders", "id")],
        ascending: vec![true],
        nulls_order: None,
    });
    let physical = plan_logical(&catalog, &sort).expect("plan");
    let PhysicalPlan::Scan(scan) = &physical else {
        panic!("sort should fold onto the scan, got {physical:?}")
    };
    assert_eq!(scan.order_by_keys.as_ref().map(Vec::len), Some(1));
}

// --------------------------------------------------------------------------
// CTE producer registry
// --------------------------------------------------------------------------

#[test]
fn cross_source_cte_registers_a_producer_and_resolves_the_reference() {
    // A non-recursive cross-source CTE: the body materializes once and the child's
    // reference resolves to a CteScan over the named producer.
    let catalog = catalog();
    let cte = LogicalPlan::Cte(Cte {
        name: "recent".to_string(),
        cte_plan: Box::new(lscan("pg", "public", "orders", &["id"])),
        child: Box::new(LogicalPlan::CteRef(CteRef {
            name: "recent".to_string(),
            alias: Some("r".to_string()),
            columns: Some(vec!["id".to_string()]),
            output_names: Some(vec!["id".to_string()]),
        })),
        recursive: false,
        column_names: None,
    });
    let physical = plan_logical(&catalog, &cte).expect("plan");
    let PhysicalPlan::CteScan(scan) = &physical else {
        panic!("a CTE reference lowers to a CteScan, got {physical:?}")
    };
    assert_eq!(scan.alias.as_deref(), Some("r"));
    let PhysicalPlan::Cte(producer) = scan.producer.as_ref() else {
        panic!("the CteScan names a Cte producer")
    };
    assert_eq!(producer.name, "recent");
}

#[test]
fn cte_consumed_twice_shares_one_producer_allocation() {
    // Two references to one cross-source CTE resolve to CteScans over the SAME
    // producer node (pointer-identical), so downstream once-per-producer caches
    // (step-building's cte_bindings) fire across consumers and the body executes
    // once, not once per reference.
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "WITH totals AS (SELECT o.id AS id, sum(l.l_quantity) AS qty \
         FROM pg.public.orders o JOIN duck.main.lineitem l ON o.id = l.l_orderkey \
         GROUP BY o.id) \
         SELECT a.id, a.qty, b.qty FROM totals a JOIN totals b ON a.id = b.id",
    );
    let scans = find_all(&physical, &|node| matches!(node, PhysicalPlan::CteScan(_)));
    assert_eq!(scans.len(), 2, "two references, two CteScans: {physical:?}");
    let (PhysicalPlan::CteScan(first), PhysicalPlan::CteScan(second)) = (scans[0], scans[1]) else {
        panic!("find_all returned non-CteScan nodes")
    };
    assert!(
        std::ptr::eq(first.producer.as_ref(), second.producer.as_ref()),
        "both CteScans must share one producer allocation"
    );
}

#[test]
fn recursive_cross_source_cte_raises_when_not_renderable() {
    // The recursive merge path renders via single-source pushdown, which
    // declines this shape, so it raises RecursiveCteNotRenderable.
    let catalog = catalog();
    let cte = LogicalPlan::Cte(Cte {
        name: "walk".to_string(),
        cte_plan: Box::new(lscan("pg", "public", "orders", &["id"])),
        child: Box::new(LogicalPlan::CteRef(CteRef {
            name: "walk".to_string(),
            alias: None,
            columns: Some(vec!["id".to_string()]),
            output_names: Some(vec!["id".to_string()]),
        })),
        recursive: true,
        column_names: None,
    });
    let error = plan_logical(&catalog, &cte).unwrap_err();
    match error {
        PhysicalError::RecursiveCteNotRenderable { name } => assert_eq!(name, "walk"),
        other => panic!("expected RecursiveCteNotRenderable, got {other:?}"),
    }
}

// --------------------------------------------------------------------------
// values + guard nodes
// --------------------------------------------------------------------------

#[test]
fn values_lowers_to_physical_values() {
    let catalog = catalog();
    let values = LogicalPlan::Values(Values {
        rows: vec![
            vec![Expr::Literal {
                value: LiteralValue::Integer(1),
                data_type: DataType::Integer,
            }],
            vec![Expr::Literal {
                value: LiteralValue::Integer(2),
                data_type: DataType::Integer,
            }],
        ],
        output_names: vec!["x".to_string()],
    });
    let physical = plan_logical(&catalog, &values).expect("plan");
    let PhysicalPlan::Values(node) = &physical else {
        panic!("expected Values, got {physical:?}")
    };
    assert_eq!(node.rows.len(), 2);
    assert_eq!(node.output_names, vec!["x".to_string()]);
}

#[test]
fn single_row_guard_lowers_over_its_input() {
    let catalog = catalog();
    let guard = LogicalPlan::SingleRowGuard(SingleRowGuard {
        input: Box::new(lscan("pg", "public", "orders", &["id"])),
        keys: vec![col("orders", "id")],
    });
    let physical = plan_logical(&catalog, &guard).expect("plan");
    let PhysicalPlan::SingleRowGuard(node) = &physical else {
        panic!("expected SingleRowGuard, got {physical:?}")
    };
    assert!(matches!(node.input.as_ref(), PhysicalPlan::Scan(_)));
    assert_eq!(node.keys.len(), 1);
}

#[test]
fn grouped_limit_lowers_over_its_input() {
    let catalog = catalog();
    let grouped = LogicalPlan::GroupedLimit(GroupedLimit {
        input: Box::new(lscan("pg", "public", "orders", &["id", "cust_id"])),
        keys: vec![col("orders", "cust_id")],
        limit: 3,
        order_by_keys: Some(vec![col("orders", "id")]),
        order_by_ascending: Some(vec![false]),
        order_by_nulls: None,
    });
    let physical = plan_logical(&catalog, &grouped).expect("plan");
    let PhysicalPlan::GroupedLimit(node) = &physical else {
        panic!("expected GroupedLimit, got {physical:?}")
    };
    assert_eq!(node.limit, 3);
    assert_eq!(node.keys.len(), 1);
}

// --------------------------------------------------------------------------
// cross-source LATERAL
// --------------------------------------------------------------------------

#[test]
fn cross_source_lateral_with_multiple_base_relations_raises() {
    // The right side collects two base scans -> unsupported.
    let catalog = catalog();
    let right = LogicalPlan::Join(Join {
        left: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        right: Box::new(lscan("duck", "main", "part", &["p_partkey"])),
        join_type: JoinType::Inner,
        condition: Some(eq(col("lineitem", "l_orderkey"), col("part", "p_partkey"))),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let lateral = LogicalPlan::LateralJoin(LateralJoin {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(right),
        join_type: JoinType::Inner,
    });
    let error = plan_logical(&catalog, &lateral).unwrap_err();
    assert!(matches!(error, PhysicalError::LateralMultipleBaseRelations));
}

#[test]
fn cross_source_lateral_lowers_to_a_lateral_join() {
    // The LATERAL right side (a single base scan) renders for the
    // merge engine via render_correlated_sql, so the cross-source LATERAL lowers to
    // a PhysicalLateralJoin.
    let catalog = catalog();
    let lateral = LogicalPlan::LateralJoin(LateralJoin {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        join_type: JoinType::Inner,
    });
    let physical = plan_logical(&catalog, &lateral).expect("plan");
    let PhysicalPlan::LateralJoin(node) = &physical else {
        panic!("cross-source LATERAL lowers to a lateral join, got {physical:?}")
    };
    // The right side is registered as `lat_b0` in the rendered lateral SQL.
    assert!(
        node.lateral_sql.contains("lat_b0"),
        "the lateral SQL names the registered base relation: {}",
        node.lateral_sql
    );
}

// --------------------------------------------------------------------------
// projection: window detection + distinct propagation
// --------------------------------------------------------------------------

#[test]
fn window_projection_lowers_to_a_window_node() {
    // The parser rejects window syntax, so the window-bearing
    // projection is built directly. A Window expression selects PhysicalWindow.
    // A SINGLE-source window projection pushes as one remote query
    // (has_computed && !has_aggregate), so this drives the window over a
    // CROSS-source join, where pushdown declines and the coordinator Window path
    // is exercised.
    let catalog = catalog();
    let window = Expr::Window {
        function: Box::new(Expr::FunctionCall {
            function_name: "row_number".to_string(),
            args: vec![],
            is_aggregate: false,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
            filter: None,
        }),
        partition_by: vec![],
        order_keys: vec![col("orders", "id")],
        order_ascending: vec![true],
        order_nulls: vec![None],
        frame: None,
    };
    let join = LogicalPlan::Join(Join {
        left: Box::new(lscan("pg", "public", "orders", &["id"])),
        right: Box::new(lscan("duck", "main", "lineitem", &["l_orderkey"])),
        join_type: JoinType::Inner,
        condition: Some(eq(col("orders", "id"), col("lineitem", "l_orderkey"))),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    let projection = LogicalPlan::Projection(Projection {
        input: Box::new(join),
        expressions: vec![col("orders", "id"), window],
        aliases: vec!["id".to_string(), "rn".to_string()],
        distinct: false,
        distinct_on: None,
    });
    let physical = plan_logical(&catalog, &projection).expect("plan");
    assert!(matches!(physical, PhysicalPlan::Window(_)));
}

#[test]
fn distinct_projection_propagates_to_the_lowest_scan() {
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT DISTINCT orders.region FROM pg.public.orders",
    );
    let distinct_scans = find_all(
        &physical,
        &|node| matches!(node, PhysicalPlan::Scan(scan) if scan.distinct),
    );
    assert_eq!(distinct_scans.len(), 1, "the DISTINCT reaches the scan");
}

// --------------------------------------------------------------------------
// aggregate group_observation stamp
// --------------------------------------------------------------------------

#[test]
fn coordinator_aggregate_is_stamped_with_a_group_observation() {
    // A plain-column GROUP BY over a cross-source join lands as a coordinator hash
    // aggregate carrying its learned-stats provenance stamp.
    let catalog = catalog();
    let physical = plan_sql(
        &catalog,
        "SELECT orders.region, count(*) FROM pg.public.orders \
         JOIN duck.main.lineitem ON orders.id = lineitem.l_orderkey \
         GROUP BY orders.region",
    );
    let aggs = find_all(&physical, &|n| matches!(n, PhysicalPlan::HashAggregate(_)));
    assert_eq!(aggs.len(), 1);
    let PhysicalPlan::HashAggregate(agg) = aggs[0] else {
        unreachable!()
    };
    let observation = agg
        .group_observation
        .as_ref()
        .expect("a plain-column group key is stamped");
    assert_eq!(observation.columns, vec!["region".to_string()]);
}
