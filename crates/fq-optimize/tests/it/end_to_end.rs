//! End-to-end estimation: real SQL driven through parse -> bind -> decorrelate ->
//! CostModel against a hand-built catalog with a statistics-serving source. The
//! catalog-builder pattern mirrors `crates/fq-decorrelate/tests/common`.

use std::collections::BTreeMap;
use std::sync::Arc;

use fq_catalog::datasource::{
    ColumnStatistics, DataSource, DataSourceCapability, TableMetadata, TableStatistics,
};
use fq_catalog::{Catalog, CatalogError, Column, Schema, Table};
use fq_common::{CostConfig, DataType};
use fq_optimize::{CostModel, StatisticsCollector};
use fq_parse::parse_with_catalog;
use fq_plan::logical::LogicalPlan;

/// A table's row count and per-column statistics.
type TableStats = (Option<i64>, BTreeMap<String, ColumnStatistics>);

/// A stats-serving source over `pg.public` with `orders` and `products`.
struct PgSource {
    tables: BTreeMap<String, TableStats>,
}

impl DataSource for PgSource {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "pg"
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![]
    }
    fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec!["public".to_string()])
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

fn ndv(num_distinct: i64) -> ColumnStatistics {
    ColumnStatistics {
        num_distinct: Some(num_distinct),
        null_fraction: 0.0,
        avg_width: 8,
        min_value: None,
        max_value: None,
    }
}

/// The source's statistics: orders 1000 rows, products 100 rows, keys/dims sized.
fn source_tables() -> BTreeMap<String, TableStats> {
    let mut orders_cols = BTreeMap::new();
    orders_cols.insert("product_id".to_string(), ndv(100));
    orders_cols.insert("region".to_string(), ndv(4));
    let mut products_cols = BTreeMap::new();
    products_cols.insert("id".to_string(), ndv(100));
    let mut tables = BTreeMap::new();
    tables.insert("orders".to_string(), (Some(1000), orders_cols));
    tables.insert("products".to_string(), (Some(100), products_cols));
    tables
}

/// A catalog whose schema tree binds queries AND whose registered source serves
/// statistics to the cost model.
fn catalog() -> Arc<Catalog> {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("product_id", DataType::Integer, true),
            Column::new("price", DataType::Double, false),
            Column::new("region", DataType::Varchar, true),
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
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, products]),
    );
    catalog.register_datasource(Arc::new(PgSource {
        tables: source_tables(),
    }));
    Arc::new(catalog)
}

/// Parse + bind + decorrelate a query against the shared catalog.
fn plan(catalog: &Catalog, sql: &str) -> LogicalPlan {
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    fq_decorrelate::decorrelate(bound).expect("decorrelate")
}

/// A cost model over a fresh collector for the shared catalog.
fn cost_model(catalog: Arc<Catalog>) -> CostModel {
    let stats = StatisticsCollector::new(catalog, None, None);
    CostModel::new(CostConfig::default(), Some(stats))
}

#[test]
fn base_scan_estimate_is_the_source_row_count() {
    let catalog = catalog();
    let mut cost = cost_model(catalog.clone());
    let logical = plan(&catalog, "SELECT o.id FROM pg.public.orders o");
    let estimate = cost.estimate(&logical).unwrap();
    assert_eq!(estimate.rows, Some(1000));
    assert!(estimate.defaults_used.is_empty());
}

#[test]
fn inner_join_estimate_uses_the_key_ndv() {
    let catalog = catalog();
    let mut cost = cost_model(catalog.clone());
    let logical = plan(
        &catalog,
        "SELECT o.id FROM pg.public.orders o JOIN pg.public.products p ON o.product_id = p.id",
    );
    let estimate = cost.estimate(&logical).unwrap();
    // 1000 * 100 / max_ndv(100, 100) = 1000, gap-free.
    assert_eq!(estimate.rows, Some(1000));
    assert!(estimate.defaults_used.is_empty());
}

#[test]
fn group_by_estimate_is_bounded_by_the_key_ndv() {
    let catalog = catalog();
    let mut cost = cost_model(catalog.clone());
    let logical = plan(
        &catalog,
        "SELECT o.region, count(*) FROM pg.public.orders o GROUP BY o.region",
    );
    let estimate = cost.estimate(&logical).unwrap();
    // region NDV 4 bounds the 1000-row input to 4 groups.
    assert_eq!(estimate.rows, Some(4));
}

#[test]
fn standalone_filter_records_a_gap_with_no_stats_in_scope() {
    // Before pushdown (an M2 rule) the WHERE stays a standalone Filter with no
    // table stats in scope, so it reduces nothing and records the gap.
    let catalog = catalog();
    let mut cost = cost_model(catalog.clone());
    let logical = plan(
        &catalog,
        "SELECT o.id FROM pg.public.orders o WHERE o.region = 'x'",
    );
    let estimate = cost.estimate(&logical).unwrap();
    assert_eq!(estimate.rows, Some(1000));
    assert!(estimate
        .defaults_used
        .iter()
        .any(|gap| gap.starts_with("eq_selectivity(")));
}
