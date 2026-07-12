//! The dim-shipping kill switch (`FEDQ_DIM_SHIPPING=0`) in its OWN test binary:
//! the switch is a process-global environment read, so mutating it must not race a
//! concurrent ship test. A separate integration binary is a separate process, so
//! this file's single test owns the variable for its whole run. Ports the
//! Python kill-switch decline.

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

/// A stats-serving, ship-capable source over one table.
struct ShipSource {
    name: String,
    table: String,
    rows: Option<i64>,
    columns: BTreeMap<String, ColumnStatistics>,
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
        Ok(vec![self.table.clone()])
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
        if table != self.table {
            return Ok(None);
        }
        let mut column_stats = BTreeMap::new();
        for column in columns {
            if let Some(stats) = self.columns.get(column) {
                column_stats.insert(column.clone(), stats.clone());
            }
        }
        Ok(Some(TableStatistics {
            row_count: self.rows,
            total_size_bytes: 0,
            column_stats,
        }))
    }
    fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
        fq_catalog::datasource::map_native_type_default(type_str)
    }
}

fn ndv(num_distinct: i64) -> ColumnStatistics {
    ColumnStatistics {
        num_distinct: Some(num_distinct),
        null_fraction: 0.0,
        avg_width: 8,
        min_value: None,
        max_value: None,
    }
}

/// The same qualifying fact-dim catalog as tests/dim_shipping.rs (fact 500k, dim
/// 100), the shape that ships when the switch is not set.
fn catalog() -> Arc<Catalog> {
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
    catalog.register_datasource(Arc::new(ShipSource {
        name: "duck_orders".to_string(),
        table: "orders".to_string(),
        rows: Some(500_000),
        columns: orders_cols,
    }));
    let mut products_cols = BTreeMap::new();
    products_cols.insert("id".to_string(), ndv(100));
    products_cols.insert("category".to_string(), ndv(3));
    catalog.register_datasource(Arc::new(ShipSource {
        name: "duck_products".to_string(),
        table: "products".to_string(),
        rows: Some(100),
        columns: products_cols,
    }));
    Arc::new(catalog)
}

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

fn has_shipment(plan: &PhysicalPlan) -> bool {
    if matches!(plan, PhysicalPlan::Shipment(_)) {
        return true;
    }
    plan.children().iter().any(|child| has_shipment(child))
}

#[test]
fn kill_switch_disables_a_qualifying_ship() {
    let catalog = catalog();
    // Confirm the shape SHIPS with the switch unset, then that "0" suppresses it -
    // so the assertion proves the switch, not merely a non-qualifying shape.
    std::env::remove_var("FEDQ_DIM_SHIPPING");
    assert!(has_shipment(&plan(&catalog)), "the shape ships when unset");

    std::env::set_var("FEDQ_DIM_SHIPPING", "0");
    let with_switch = plan(&catalog);
    std::env::remove_var("FEDQ_DIM_SHIPPING");
    assert!(
        !has_shipment(&with_switch),
        "FEDQ_DIM_SHIPPING=0 disables shipping"
    );
}
