//! End-to-end tests for the flattenable decorrelation path. Real SQL is parsed
//! (fq-parse) and bound (fq-bind) against a hand-built catalog, then decorrelated,
//! and the resulting `LogicalPlan` is asserted structurally. Mirrors the boolean
//! cases of the Python `tests/e2e_decorrelation/` suite.

use fq_catalog::{Catalog, Column, Schema, Table};
use fq_common::DataType;
use fq_decorrelate::{decorrelate, DecorrelationError};
use fq_parse::parse_with_catalog;
use fq_plan::expr::{BinaryOpType, Expr, UnaryOpType};
use fq_plan::logical::{Join, JoinType, LogicalPlan};

/// pg.public with orders, products, and users. Aliases in the SQL (`orders o`)
/// carry through binding as the relation qualifier.
fn catalog() -> Catalog {
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
            Column::new("price", DataType::Double, false),
            Column::new("status", DataType::Varchar, true),
        ],
    );
    let users = Table::new(
        "users",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![orders, products, users]),
    );
    catalog
}

/// Parse + bind + decorrelate, expecting success.
fn decorrelate_sql(sql: &str) -> LogicalPlan {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, plan).expect("bind");
    decorrelate(bound).expect("decorrelate")
}

/// Parse + bind + decorrelate, expecting a decorrelation error.
fn decorrelate_err(sql: &str) -> DecorrelationError {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, plan).expect("bind");
    decorrelate(bound).expect_err("expected a decorrelation error")
}

/// The join directly under the top SELECT projection (the flattenable path builds
/// a Projection over a Join around the source).
fn join_under_projection(plan: LogicalPlan) -> Join {
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected a top Projection, got {plan:?}");
    };
    match *projection.input {
        LogicalPlan::Join(join) => join,
        other => panic!("expected a Join under the projection, got {other:?}"),
    }
}

/// Assert an expression is a column reference with the given qualifier/name.
fn assert_col(expr: &Expr, table: &str, column: &str) {
    let Expr::Column(col) = expr else {
        panic!("expected a column, got {expr:?}");
    };
    assert_eq!(col.table.as_deref(), Some(table), "qualifier");
    assert_eq!(col.column, column, "column name");
}

/// Destructure a binary op, asserting its operator.
fn binary_parts(expr: &Expr, op: BinaryOpType) -> (&Expr, &Expr) {
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

/// Assert no subquery expression survives anywhere in the plan (the pass already
/// guarantees this, but we pin it as the contract).
fn assert_no_subquery(plan: &LogicalPlan) {
    for expr in plan.direct_expressions() {
        assert!(!has_subquery(expr), "surviving subquery in {expr:?}");
    }
    for child in plan.children() {
        assert_no_subquery(child);
    }
}

/// Whether an expression tree contains any subquery node.
fn has_subquery(expr: &Expr) -> bool {
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

/// The SubqueryScan alias of a join's right side.
fn right_alias(join: &Join) -> &str {
    match join.right.as_ref() {
        LogicalPlan::SubqueryScan(scan) => &scan.alias,
        other => panic!("expected a SubqueryScan on the right, got {other:?}"),
    }
}

#[test]
fn correlated_exists_becomes_semi_join() {
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE EXISTS (SELECT 1 FROM pg.public.products p \
                       WHERE p.id = o.product_id AND p.price > 100)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    assert_eq!(right_alias(&join), "__subq_0");
    // Condition: __subq_0.__subq_0_k0 = o.product_id (the pulled key equality).
    let condition = join
        .condition
        .expect("correlated EXISTS carries a condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "__subq_0", "__subq_0_k0");
    assert_col(right, "o", "product_id");
}

#[test]
fn uncorrelated_exists_is_semi_with_limit_one_and_no_condition() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders \
         WHERE EXISTS (SELECT 1 FROM pg.public.products WHERE price > 100)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    assert!(
        join.condition.is_none(),
        "uncorrelated EXISTS has no condition"
    );
    // The right side is the subplan capped at one row.
    let LogicalPlan::SubqueryScan(scan) = join.right.as_ref() else {
        panic!("expected SubqueryScan");
    };
    assert!(
        matches!(scan.input.as_ref(), LogicalPlan::Limit(limit) if limit.limit == Some(1)),
        "expected LIMIT 1, got {:?}",
        scan.input
    );
}

#[test]
fn not_exists_becomes_anti_join() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE NOT EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Anti);
    let condition = join.condition.expect("condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "__subq_0", "__subq_0_k0");
    assert_col(right, "o", "product_id");
}

#[test]
fn not_parenthesized_exists_negates_to_anti() {
    // NOT (EXISTS ...) routes through negated_subquery_conjunct, flipping to ANTI.
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE NOT (EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id))",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Anti);
}

#[test]
fn in_subquery_becomes_semi_with_plain_equality() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders \
         WHERE product_id IN (SELECT id FROM pg.public.products WHERE price > 100)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    // Uncorrelated body: condition is just the value equality, no NULL-awareness.
    let condition = join.condition.expect("condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "orders", "product_id");
    assert_col(right, "__subq_0", "__subq_0_v0");
}

#[test]
fn not_in_subquery_becomes_anti_with_null_aware_condition() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders \
         WHERE product_id NOT IN (SELECT id FROM pg.public.products WHERE status = 'x')",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Anti);
    // Condition: ((product_id = v0) OR (product_id IS NULL)) OR (v0 IS NULL).
    let condition = join.condition.expect("condition");
    let (outer_or, inner_null) = binary_parts(&condition, BinaryOpType::Or);
    assert!(matches!(
        inner_null,
        Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            ..
        }
    ));
    let (eq, outer_null) = binary_parts(outer_or, BinaryOpType::Or);
    assert!(matches!(
        outer_null,
        Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            ..
        }
    ));
    let (left, right) = binary_parts(eq, BinaryOpType::Eq);
    assert_col(left, "orders", "product_id");
    assert_col(right, "__subq_0", "__subq_0_v0");
}

#[test]
fn eq_any_becomes_semi() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders \
         WHERE price = ANY (SELECT price FROM pg.public.products WHERE status = 'x')",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    let condition = join.condition.expect("condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "orders", "price");
    assert_col(right, "__subq_0", "__subq_0_v0");
}

#[test]
fn gt_all_becomes_anti_with_violation_condition() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders \
         WHERE price > ALL (SELECT price FROM pg.public.products WHERE status = 'x')",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Anti);
    // Condition: (NOT(price > v0) OR price IS NULL) OR v0 IS NULL.
    let condition = join.condition.expect("condition");
    let (left_or, value_null) = binary_parts(&condition, BinaryOpType::Or);
    assert!(matches!(
        value_null,
        Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            ..
        }
    ));
    let (negated, left_null) = binary_parts(left_or, BinaryOpType::Or);
    assert!(matches!(
        left_null,
        Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            ..
        }
    ));
    assert!(
        matches!(
            negated,
            Expr::UnaryOp {
                op: UnaryOpType::Not,
                ..
            }
        ),
        "expected NOT(comparison), got {negated:?}"
    );
}

#[test]
fn correlated_in_over_grouped_key_widens_aggregate() {
    // The correlated key p.id is the GROUP BY key, so it crosses the aggregate as a
    // widened output __subq_0_g0 and the condition joins on it.
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.id = o.product_id GROUP BY p.id)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    // The condition ANDs the value equality with the widened-key equality; assert
    // the widened key column appears somewhere in the condition.
    let condition = join.condition.as_ref().expect("condition");
    assert!(
        condition_mentions(condition, "__subq_0", "__subq_0_g0"),
        "expected the widened key __subq_0_g0 in {condition:?}"
    );
    // The aggregate below exposes the widened key as an output.
    assert!(
        aggregate_outputs_key(&join, "__subq_0_g0"),
        "expected __subq_0_g0 among the aggregate outputs"
    );
}

#[test]
fn correlated_in_with_limit_becomes_grouped_limit() {
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.id = o.product_id LIMIT 3)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    // The subquery relation carries a per-key GroupedLimit keyed on the exposed key.
    assert!(
        subquery_has_grouped_limit(&join, 3),
        "expected a GroupedLimit(limit=3) in the subquery relation"
    );
}

// --- give-up matrix -------------------------------------------------------

#[test]
fn in_arity_mismatch_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders \
         WHERE product_id IN (SELECT id, price FROM pg.public.products)",
    );
    assert!(matches!(err, DecorrelationError::Unsupported(m) if m.contains("IN subquery returns")));
}

#[test]
fn quantified_multi_column_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders \
         WHERE price = ANY (SELECT id, price FROM pg.public.products)",
    );
    assert!(
        matches!(err, DecorrelationError::Unsupported(m) if m.contains("must return one column"))
    );
}

#[test]
fn non_equi_correlation_across_aggregate_is_non_flattenable() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.id > o.product_id GROUP BY p.id)",
    );
    assert!(
        matches!(err, DecorrelationError::NonFlattenable(_)),
        "got {err:?}"
    );
}

#[test]
fn correlation_key_outside_group_by_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.category = o.region GROUP BY p.id)",
    );
    assert!(
        matches!(err, DecorrelationError::Unsupported(m) if m.contains("not part of the subquery's GROUP BY"))
    );
}

#[test]
fn non_equi_correlated_limit_is_non_flattenable() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.id > o.product_id LIMIT 3)",
    );
    assert!(
        matches!(err, DecorrelationError::NonFlattenable(_)),
        "got {err:?}"
    );
}

#[test]
fn offset_in_correlated_subquery_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT p.id FROM pg.public.products p \
                              WHERE p.id = o.product_id LIMIT 3 OFFSET 2)",
    );
    assert!(matches!(err, DecorrelationError::Unsupported(m) if m.contains("OFFSET")));
}

#[test]
fn distinct_in_correlated_value_subquery_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT DISTINCT p.id FROM pg.public.products p \
                              WHERE p.category = o.region)",
    );
    assert!(matches!(err, DecorrelationError::Unsupported(m) if m.contains("DISTINCT")));
}

#[test]
fn outer_reference_in_output_column_raises() {
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE product_id IN (SELECT o.product_id FROM pg.public.products p \
                              WHERE p.category = o.region)",
    );
    assert!(matches!(err, DecorrelationError::Unsupported(m) if m.contains("Outer reference")));
}

#[test]
fn skip_level_correlation_raises() {
    // The inner subquery references o (two levels up), which cannot be flattened.
    let err = decorrelate_err(
        "SELECT id FROM pg.public.orders o \
         WHERE EXISTS (SELECT 1 FROM pg.public.products p \
                       WHERE EXISTS (SELECT 1 FROM pg.public.products q \
                                     WHERE q.id = o.product_id AND q.category = p.category))",
    );
    assert!(
        matches!(err, DecorrelationError::Unsupported(m) if m.contains("unsupported position"))
    );
}

#[test]
fn subquery_in_group_by_raises() {
    let err = decorrelate_err(
        "SELECT count(*) FROM pg.public.orders o \
         GROUP BY (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id)",
    );
    assert!(matches!(err, DecorrelationError::Unsupported(m) if m.contains("GROUP BY")));
}

#[test]
fn scalar_subquery_in_predicate_becomes_left_join() {
    // A correlated scalar in a WHERE equality: LEFT join on the widened key, with
    // the value equality kept as a residual filter (not tightened - it is not an
    // ON-TRUE join). The scalar milestone now ports this shape.
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE price = (SELECT max(p.price) FROM pg.public.products p WHERE p.category = o.region)",
    );
    assert_no_subquery(&plan);
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected a top projection");
    };
    let LogicalPlan::Filter(filter) = *projection.input else {
        panic!("expected a residual filter over the LEFT join");
    };
    // Residual: o.price = __subq_0.__subq_0_v0.
    assert!(condition_mentions(
        &filter.predicate,
        "__subq_0",
        "__subq_0_v0"
    ));
    let LogicalPlan::Join(join) = *filter.input else {
        panic!("expected a LEFT join under the filter");
    };
    assert_eq!(join.join_type, JoinType::Left);
    let condition = join
        .condition
        .expect("correlated scalar carries a condition");
    assert!(condition_mentions(&condition, "__subq_0", "__subq_0_g0"));
}

#[test]
fn or_of_existentials_becomes_domain_union_semi() {
    // OR of two positive same-key EXISTS collapses to ONE SEMI join over the UNION
    // of the two key domains (no flag joins, no input replication).
    let plan = decorrelate_sql(
        "SELECT id FROM pg.public.orders o \
         WHERE EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) \
            OR EXISTS (SELECT 1 FROM pg.public.products q WHERE q.id = o.product_id)",
    );
    assert_no_subquery(&plan);
    let join = join_under_projection(plan);
    assert_eq!(join.join_type, JoinType::Semi);
    // The right side is a SubqueryScan over a UNION ALL of the per-branch domains.
    let LogicalPlan::SubqueryScan(scan) = join.right.as_ref() else {
        panic!("expected a SubqueryScan domain on the right");
    };
    assert!(matches!(
        scan.input.as_ref(),
        LogicalPlan::Union(union) if !union.distinct && union.inputs.len() == 2
    ));
    // Condition: o.product_id = <domain>.__subq_2_k0.
    let condition = join.condition.expect("domain SEMI carries a condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "o", "product_id");
    assert_col(right, &scan.alias, &format!("{}_k0", scan.alias));
}

#[test]
fn plan_without_subqueries_passes_through_unchanged() {
    let catalog = catalog();
    let bound = fq_bind::bind(
        &catalog,
        parse_with_catalog("SELECT name FROM pg.public.users WHERE id > 3", &catalog).unwrap(),
    )
    .unwrap();
    let decorrelated = decorrelate(bound.clone()).unwrap();
    assert_eq!(decorrelated, bound, "a subquery-free plan is unchanged");
}

// --- structural assertion helpers used only by the widening/limit tests ---

/// Whether a condition tree contains a `table.column` reference.
fn condition_mentions(expr: &Expr, table: &str, column: &str) -> bool {
    fq_plan::expr::column_refs(expr)
        .iter()
        .any(|col| col.table.as_deref() == Some(table) && col.column == column)
}

/// Whether the subquery relation's aggregate outputs the given widened key name.
fn aggregate_outputs_key(join: &Join, key: &str) -> bool {
    let mut found = false;
    walk_plan(join.right.as_ref(), &mut |node| {
        if let LogicalPlan::Aggregate(aggregate) = node {
            if aggregate.output_names.iter().any(|name| name == key) {
                found = true;
            }
        }
    });
    found
}

/// Whether the subquery relation contains a GroupedLimit with the given limit.
fn subquery_has_grouped_limit(join: &Join, limit: u64) -> bool {
    let mut found = false;
    walk_plan(join.right.as_ref(), &mut |node| {
        if let LogicalPlan::GroupedLimit(grouped) = node {
            if grouped.limit == limit && !grouped.keys.is_empty() {
                found = true;
            }
        }
    });
    found
}

/// Depth-first walk applying `visit` to every plan node.
fn walk_plan(plan: &LogicalPlan, visit: &mut impl FnMut(&LogicalPlan)) {
    visit(plan);
    for child in plan.children() {
        walk_plan(child, visit);
    }
}
