//! Dim-shipping at the PhysicalPlan-TREE level: real SQL driven
//! parse -> bind -> decorrelate -> optimize -> plan against hand-built stats-
//! serving catalogs, asserting the SHIP decision (ships vs declines) and the
//! shipped SHAPE (a PhysicalShipment wrapping the island PhysicalRemoteQuery, its
//! temp-scan seeding, the ship datasource). Ports the decisions of
//! `tests/e2e_pushdown/test_dim_shipping.py` (SPEC 11.2) without a live engine:
//! single-source rendering needs no DB, so the collapsed island builds from the
//! emitter alone.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use fq_catalog::datasource::{
    ColumnStatistics, DataSource, DataSourceCapability, TableMetadata, TableStatistics,
};
use fq_catalog::{Catalog, CatalogError, Column, Schema, Table};
use fq_common::{CostConfig, DataType, OptimizerConfig};
use fq_optimize::{build_optimizer, CostModel, StatisticsCollector};
use fq_parse::parse_with_catalog;
use fq_physical::PhysicalPlanner;
use fq_plan::physical::PhysicalPlan;

/// A table's row count and per-column NDV statistics.
type TableStats = (Option<i64>, BTreeMap<String, ColumnStatistics>);

/// A stats-serving source that can also RECEIVE shipped temp tables (ShipTarget)
/// and push joins/aggregates. One per datasource; serves stats by table name.
struct ShipSource {
    name: String,
    tables: BTreeMap<String, TableStats>,
}

impl DataSource for ShipSource {
    fn name(&self) -> &str {
        &self.name
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![
            DataSourceCapability::Joins,
            DataSourceCapability::Aggregations,
            DataSourceCapability::ShipTarget,
        ]
    }
    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec!["main".to_string()])
    }
    fn list_tables(&self, _schema: &str) -> Result<Vec<String>, CatalogError> {
        Ok(self.tables.keys().cloned().collect())
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
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>, CatalogError> {
        let Some((row_count, all_stats)) = self.tables.get(table) else {
            return Ok(None);
        };
        let mut column_stats = BTreeMap::new();
        for column in columns {
            if let Some(stats) = all_stats.get(column) {
                column_stats.insert(column.clone(), stats.clone());
            }
        }
        Ok(Some(TableStatistics {
            row_count: *row_count,
            total_size_bytes: 0,
            column_stats,
        }))
    }
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        fq_catalog::datasource::map_native_type_default(type_str)
    }
}

/// A column-NDV statistic.
fn ndv(num_distinct: i64) -> ColumnStatistics {
    ColumnStatistics {
        num_distinct: Some(num_distinct),
        null_fraction: 0.0,
        avg_width: 8,
        min_value: None,
        max_value: None,
    }
}

/// A two-source catalog: `duck_orders.main.orders` (the fact) and
/// `duck_products.main.products` (the small dim). `orders_rows` sizes the fact so
/// a test can drive it above or below the ship floor; products is always 100 rows.
fn catalog(orders_rows: Option<i64>, products_rows: Option<i64>) -> Arc<Catalog> {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("product_id", DataType::Integer, true),
        ],
    );
    let products = Table::new(
        "products",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("category", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "duck_orders",
        "main",
        Schema::with_tables("main", "duck_orders", vec![orders]),
    );
    catalog.insert_schema(
        "duck_products",
        "main",
        Schema::with_tables("main", "duck_products", vec![products]),
    );

    let mut orders_cols = BTreeMap::new();
    orders_cols.insert("product_id".to_string(), ndv(100));
    let mut orders_tables = BTreeMap::new();
    orders_tables.insert("orders".to_string(), (orders_rows, orders_cols));
    catalog.register_datasource(Arc::new(ShipSource {
        name: "duck_orders".to_string(),
        tables: orders_tables,
    }));

    let mut products_cols = BTreeMap::new();
    products_cols.insert("id".to_string(), ndv(100));
    products_cols.insert("category".to_string(), ndv(3));
    let mut products_tables = BTreeMap::new();
    products_tables.insert("products".to_string(), (products_rows, products_cols));
    catalog.register_datasource(Arc::new(ShipSource {
        name: "duck_products".to_string(),
        tables: products_tables,
    }));

    Arc::new(catalog)
}

/// Plan the fact-dim group-by query end to end, with a stats-backed cost model so
/// dim shipping's gates have real sizes to judge.
fn plan(catalog: &Arc<Catalog>) -> PhysicalPlan {
    let sql = "SELECT p.category, count(*) AS n \
               FROM duck_orders.main.orders o \
               JOIN duck_products.main.products p ON o.product_id = p.id \
               GROUP BY p.category";
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    let decorrelated = fq_decorrelate::decorrelate(bound).expect("decorrelate");
    let optimizer_cost = CostModel::new(
        CostConfig::default(),
        Some(StatisticsCollector::new(Arc::clone(catalog), None, None)),
    );
    let optimizer = build_optimizer(&OptimizerConfig::default(), optimizer_cost);
    let logical = optimizer.optimize(decorrelated).expect("optimize");

    let planner_cost = CostModel::new(
        CostConfig::default(),
        Some(StatisticsCollector::new(Arc::clone(catalog), None, None)),
    );
    let mut planner = PhysicalPlanner::new(
        Arc::clone(catalog),
        Some(Rc::new(RefCell::new(planner_cost))),
    );
    planner.plan(&logical).expect("plan")
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

#[test]
fn group_by_dim_ships_and_collapses() {
    // A large fact (500k) INNER-joined to a small dim (100), grouped by a low-card
    // dim column: the dim ships INTO the fact source and the whole subtree
    // collapses to one island wrapped in a PhysicalShipment.
    let catalog = catalog(Some(500_000), Some(100));
    let physical = plan(&catalog);

    let shipments = find_all(&physical, &|node| matches!(node, PhysicalPlan::Shipment(_)));
    assert_eq!(shipments.len(), 1, "one dim ships");
    let PhysicalPlan::Shipment(shipment) = shipments[0] else {
        unreachable!()
    };
    // The dim ships INTO the fact source, under the generated temp name.
    assert_eq!(shipment.datasource, "duck_orders");
    assert_eq!(shipment.table, "__fedq_ship_0");

    // The island the shipment wraps is one remote query on the fact source, seeded
    // (it reads the temp table the python-side probe cannot see).
    let PhysicalPlan::RemoteQuery(island) = shipment.child.as_ref() else {
        panic!("the shipment wraps the collapsed island remote query")
    };
    assert_eq!(island.datasource, "duck_orders");
    let seeded = island
        .seeded_schema
        .as_ref()
        .expect("the island reads a shipped temp table, so it is seeded");
    let seeded_names: Vec<&str> = seeded.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(seeded_names, vec!["category", "n"]);
    // The island stamps its learned-group provenance (a plain-column GROUP BY).
    let observation = island
        .group_observation
        .as_ref()
        .expect("a plain-column group key is stamped");
    assert_eq!(observation.columns, vec!["category".to_string()]);

    // The shipped body materializes the ORIGINAL foreign relation on its own
    // source (a bare base scan lowers to a PhysicalScan there).
    let body_datasource = match shipment.body.as_ref() {
        PhysicalPlan::Scan(scan) => &scan.datasource,
        PhysicalPlan::RemoteQuery(remote) => &remote.datasource,
        other => panic!("unexpected shipped body node: {other:?}"),
    };
    assert_eq!(body_datasource, "duck_products");
}

#[test]
fn fact_below_floor_declines() {
    // The same query with a fact of only 50k rows (below SHIP_LOCAL_FLOOR): the cost
    // gate declines, so NO shipment appears and the cross-source plan stands.
    let catalog = catalog(Some(50_000), Some(100));
    let physical = plan(&catalog);
    assert!(
        find_all(&physical, &|n| matches!(n, PhysicalPlan::Shipment(_))).is_empty(),
        "a small fact does not ship"
    );
    // The proven no-ship plan keeps a cross-source hash join.
    assert_eq!(
        find_all(&physical, &|n| matches!(n, PhysicalPlan::HashJoin(_))).len(),
        1
    );
}

#[test]
fn unknown_foreign_size_declines() {
    // The dim has NO row count (an unknown foreign size): shipping a possibly-huge
    // dim would flood the source, so the gate declines even though the fact is large.
    let catalog = catalog(Some(500_000), None);
    let physical = plan(&catalog);
    assert!(
        find_all(&physical, &|n| matches!(n, PhysicalPlan::Shipment(_))).is_empty(),
        "an unknown foreign size does not ship"
    );
}

// The kill-switch (FEDQ_DIM_SHIPPING=0) gate is tested in its own test binary
// (tests/dim_shipping_kill_switch.rs): it mutates the PROCESS-GLOBAL environment,
// which would race the concurrent ship test in this binary. A separate binary is a
// separate process, so its env mutation is isolated.
