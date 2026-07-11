//! Shared harness for the scalar and disjunctive decorrelation test suites: a
//! hand-built catalog, the parse+bind+decorrelate pipeline, and small structural
//! assertion helpers.
//!
// Compiled once per integration-test binary; each suite uses a different subset,
// so unused-in-one-binary helpers are expected.
#![allow(dead_code)]

use fq_catalog::{Catalog, Column, Schema, Table};
use fq_common::DataType;
use fq_decorrelate::{decorrelate, DecorrelationError};
use fq_parse::parse_with_catalog;
use fq_plan::expr::{BinaryOpType, Expr};
use fq_plan::logical::LogicalPlan;

/// pg.public with orders, products, companies. SQL aliases (`orders o`) carry
/// through binding as the relation qualifier.
pub fn catalog() -> Catalog {
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("product_id", DataType::Integer, true),
            Column::new("price", DataType::Double, false),
            Column::new("region", DataType::Varchar, true),
            Column::new("company_id", DataType::Integer, true),
        ],
    );
    let products = Table::new(
        "products",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("category", DataType::Varchar, true),
            Column::new("price", DataType::Double, false),
            Column::new("status", DataType::Varchar, true),
        ],
    );
    let companies = Table::new(
        "companies",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, products, companies]),
    );
    catalog
}

/// Parse + bind + decorrelate, expecting success.
pub fn decorrelate_sql(sql: &str) -> LogicalPlan {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, plan).expect("bind");
    decorrelate(bound).expect("decorrelate")
}

/// Parse + bind + decorrelate, expecting a decorrelation error.
pub fn decorrelate_err(sql: &str) -> DecorrelationError {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, plan).expect("bind");
    decorrelate(bound).expect_err("expected a decorrelation error")
}

/// Assert an expression is a column reference with the given qualifier/name.
pub fn assert_col(expr: &Expr, table: &str, column: &str) {
    let Expr::Column(col) = expr else {
        panic!("expected a column, got {expr:?}");
    };
    assert_eq!(col.table.as_deref(), Some(table), "qualifier");
    assert_eq!(col.column, column, "column name");
}

/// Destructure a binary op, asserting its operator.
pub fn binary_parts(expr: &Expr, op: BinaryOpType) -> (&Expr, &Expr) {
    let Expr::BinaryOp {
        op: actual,
        left,
        right,
    } = expr
    else {
        panic!("expected a binary op, got {expr:?}");
    };
    assert_eq!(*actual, op, "operator");
    (left, right)
}

/// Assert no subquery expression survives anywhere in the plan.
pub fn assert_no_subquery(plan: &LogicalPlan) {
    walk_plan(plan, &mut |node| {
        for expr in node.direct_expressions() {
            assert!(!has_subquery(expr), "surviving subquery in {expr:?}");
        }
    });
}

/// Whether an expression tree contains any subquery node.
pub fn has_subquery(expr: &Expr) -> bool {
    if matches!(
        expr,
        Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
    ) {
        return true;
    }
    expr.children().iter().any(|child| has_subquery(child))
}

/// Depth-first walk applying `visit` to every plan node.
pub fn walk_plan(plan: &LogicalPlan, visit: &mut impl FnMut(&LogicalPlan)) {
    visit(plan);
    for child in plan.children() {
        walk_plan(child, visit);
    }
}

/// Whether the plan tree contains any SingleRowGuard node.
pub fn has_single_row_guard(plan: &LogicalPlan) -> bool {
    let mut found = false;
    walk_plan(plan, &mut |node| {
        if matches!(node, LogicalPlan::SingleRowGuard(_)) {
            found = true;
        }
    });
    found
}

/// Whether a condition tree contains a `table.column` reference.
pub fn condition_mentions(expr: &Expr, table: &str, column: &str) -> bool {
    fq_plan::expr::column_refs(expr)
        .iter()
        .any(|col| col.table.as_deref() == Some(table) && col.column == column)
}
