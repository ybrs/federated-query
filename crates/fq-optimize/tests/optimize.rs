//! End-to-end optimizer tests: real SQL driven through parse -> bind ->
//! decorrelate -> the pushdown rule stack, asserting the final plan tree. The
//! catalog-builder mirrors `end_to_end.rs`.

use std::sync::Arc;

use fq_catalog::{Catalog, Column, Schema, Table};
use fq_common::{DataType, OptimizerConfig};
use fq_optimize::build_optimizer;
use fq_parse::parse_with_catalog;
use fq_plan::logical::LogicalPlan;

/// A catalog whose schema tree binds the test queries (no stats source needed:
/// the pushdown rules are stats-free).
fn catalog() -> Arc<Catalog> {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("cust_id", DataType::Integer, true),
            Column::new("region", DataType::Varchar, true),
            Column::new("amount", DataType::Double, false),
            Column::new("year", DataType::Integer, true),
        ],
    );
    let customer = Table::new(
        "customer",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("nation", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, customer]),
    );
    Arc::new(catalog)
}

/// Parse + bind + decorrelate + optimize a query against the shared catalog.
fn optimize(catalog: &Catalog, sql: &str) -> LogicalPlan {
    let parsed = parse_with_catalog(sql, catalog).expect("parse");
    let bound = fq_bind::bind(catalog, parsed).expect("bind");
    let decorrelated = fq_decorrelate::decorrelate(bound).expect("decorrelate");
    let optimizer = build_optimizer(&OptimizerConfig::default());
    optimizer.optimize(decorrelated).expect("optimize")
}

/// Collect every Scan node in a plan.
fn scans(plan: &LogicalPlan) -> Vec<&fq_plan::logical::Scan> {
    let mut out = Vec::new();
    collect_scans(plan, &mut out);
    out
}

/// Recursive helper for `scans`.
fn collect_scans<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a fq_plan::logical::Scan>) {
    if let LogicalPlan::Scan(scan) = plan {
        out.push(scan);
    }
    for child in plan.children() {
        collect_scans(child, out);
    }
}

#[test]
fn single_source_group_by_collapses_onto_one_scan() {
    let catalog = catalog();
    let plan = optimize(
        &catalog,
        "SELECT o.region, SUM(o.amount) FROM pg.public.orders o \
         WHERE o.year = 2000 GROUP BY o.region",
    );
    // The whole WHERE + GROUP BY + aggregate fold onto a single scan.
    let scans = scans(&plan);
    assert_eq!(scans.len(), 1, "one collapsed scan");
    let scan = scans[0];
    assert!(scan.group_by.is_some(), "GROUP BY folded onto the scan");
    assert!(scan.aggregates.is_some(), "aggregates folded onto the scan");
    assert!(scan.filters.is_some(), "the WHERE merged into the scan");
}

#[test]
fn predicate_pushdown_reaches_both_join_scans() {
    let catalog = catalog();
    let plan = optimize(
        &catalog,
        "SELECT o.id FROM pg.public.orders o JOIN pg.public.customer c \
         ON o.cust_id = c.id WHERE o.year = 2000 AND c.nation = 'US'",
    );
    // Each single-sided conjunct reached its own scan.
    for scan in scans(&plan) {
        assert!(
            scan.filters.is_some(),
            "scan {} should carry a pushed filter",
            scan.table_name
        );
    }
}

#[test]
fn top_n_pushes_order_and_limit_into_the_scan() {
    let catalog = catalog();
    let plan = optimize(
        &catalog,
        "SELECT o.id FROM pg.public.orders o ORDER BY o.id LIMIT 10",
    );
    let scans = scans(&plan);
    assert_eq!(scans.len(), 1);
    let scan = scans[0];
    assert!(
        scan.order_by_keys.is_some(),
        "ORDER BY folded into the scan"
    );
    assert_eq!(scan.limit, Some(10), "LIMIT folded into the scan");
}
