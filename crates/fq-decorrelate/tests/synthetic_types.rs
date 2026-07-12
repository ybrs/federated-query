//! Every ColumnRef the decorrelator manufactures must carry its DataType. A
//! correlated scalar aggregate subquery (the TPC-DS q32 shape) mints synthetic
//! correlation-key and aggregate-value columns; each must be typed so a downstream
//! `Expr::get_type` never hits an untyped post-binder reference.

mod common;

use common::{decorrelate_sql, walk_plan};
use fq_plan::expr::{column_refs, Expr};
use fq_plan::logical::LogicalPlan;

/// Assert every column reference reachable from any node's direct expressions
/// carries a bound `data_type`, reporting the first untyped one it finds.
fn assert_all_refs_typed(plan: &LogicalPlan) {
    walk_plan(plan, &mut |node| {
        for expr in node.direct_expressions() {
            assert_expr_refs_typed(expr);
        }
    });
}

/// Assert every column reference inside one expression tree carries a type.
fn assert_expr_refs_typed(expr: &Expr) {
    for col in column_refs(expr) {
        assert!(
            col.data_type.is_some(),
            "manufactured column {}.{} carries no data type",
            col.table.as_deref().unwrap_or("<none>"),
            col.column,
        );
    }
}

#[test]
fn correlated_scalar_aggregate_mints_typed_columns() {
    // The q32 shape: a correlated scalar AVG subquery under a comparison threshold.
    // Decorrelation widens the aggregate with a correlation key and hoists the AVG
    // into a value column; both, and the value ref that replaces the subquery, must
    // be typed.
    let plan = decorrelate_sql(
        "SELECT o.id \
         FROM pg.public.orders o \
         WHERE o.price > ( \
             SELECT avg(p.price) FROM pg.public.products p \
             WHERE p.id = o.product_id \
         )",
    );
    assert_all_refs_typed(&plan);
}
