//! fq-bind tests: resolution, typing, and the invalid-query rejections. Parses
//! real SQL (via fq-parse) against a hand-built catalog, then binds. A first
//! slice of test_binder.py.

use fq_bind::{bind, BindError};
use fq_catalog::{Catalog, Column, Schema, Table};
use fq_common::DataType;
use fq_parse::parse_with_catalog;
use fq_plan::{Expr, LogicalPlan};

/// A catalog with pg.public.users(id INT, name VARCHAR, age INT) and
/// pg.public.orders(id INT, user_id INT, total DOUBLE).
fn catalog() -> Catalog {
    let users = Table::new(
        "users",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
            Column::new("age", DataType::Integer, true),
        ],
    );
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("user_id", DataType::Integer, false),
            Column::new("total", DataType::Double, false),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![users, orders]),
    );
    catalog
}

/// Parse + bind, returning the bound plan.
fn bind_sql(sql: &str) -> Result<LogicalPlan, BindError> {
    let catalog = catalog();
    let plan = parse_with_catalog(sql, &catalog).expect("parse");
    bind(&catalog, plan)
}

/// Walk a bound plan and assert every ColumnRef carries a qualifier and a type.
fn assert_all_qualified(plan: &LogicalPlan) {
    for expr in plan_expressions(plan) {
        assert_columns_qualified(&expr);
    }
    for child in plan.children() {
        assert_all_qualified(child);
    }
}

fn assert_columns_qualified(expr: &Expr) {
    if let Expr::Column(column) = expr {
        assert!(
            column.table.is_some(),
            "unqualified column after binding: {}",
            column.column
        );
        assert!(
            column.data_type.is_some(),
            "untyped column after binding: {}",
            column.column
        );
    }
    for child in expr.children() {
        assert_columns_qualified(child);
    }
}

/// The expressions attached directly to a node (enough for these tests).
fn plan_expressions(plan: &LogicalPlan) -> Vec<Expr> {
    match plan {
        LogicalPlan::Projection(node) => node.expressions.clone(),
        LogicalPlan::Filter(node) => vec![node.predicate.clone()],
        LogicalPlan::Aggregate(node) => node
            .group_by
            .iter()
            .chain(&node.aggregates)
            .cloned()
            .collect(),
        LogicalPlan::Sort(node) => node.sort_keys.clone(),
        LogicalPlan::Join(node) => node.condition.clone().into_iter().collect(),
        _ => Vec::new(),
    }
}

#[test]
fn resolves_and_types_columns() {
    let plan = bind_sql("SELECT name FROM pg.public.users WHERE age > 30").unwrap();
    assert_all_qualified(&plan);
    // The WHERE column resolved to users with an integer type.
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let Expr::BinaryOp { left, .. } = &filter.predicate else {
        panic!("expected comparison");
    };
    let Expr::Column(age) = left.as_ref() else {
        panic!("expected column");
    };
    assert_eq!(age.table.as_deref(), Some("users"));
    assert_eq!(age.data_type, Some(DataType::Integer));
}

#[test]
fn scan_read_set_pruned_to_real_columns() {
    // The parser over-collects; binding prunes to columns the table actually has.
    let plan = bind_sql("SELECT name FROM pg.public.users WHERE age > 30").unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let LogicalPlan::Scan(scan) = filter.input.as_ref() else {
        panic!("expected Scan");
    };
    for name in &scan.columns {
        assert!(["id", "name", "age"].contains(&name.as_str()), "{name}");
    }
    assert!(!scan.columns.iter().any(|c| c == "*"));
}

#[test]
fn star_scan_expands_to_all_columns() {
    let plan = bind_sql("SELECT * FROM pg.public.users").unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Scan(scan) = projection.input.as_ref() else {
        panic!("expected Scan");
    };
    assert_eq!(scan.columns, vec!["id", "name", "age"]);
}

#[test]
fn join_condition_resolves_both_sides() {
    let plan = bind_sql(
        "SELECT u.name, o.total FROM pg.public.users AS u \
         JOIN pg.public.orders AS o ON u.id = o.user_id",
    )
    .unwrap();
    assert_all_qualified(&plan);
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        panic!("expected Join");
    };
    let Some(Expr::BinaryOp { left, right, .. }) = &join.condition else {
        panic!("expected join condition");
    };
    let (Expr::Column(l), Expr::Column(r)) = (left.as_ref(), right.as_ref()) else {
        panic!("expected columns");
    };
    assert_eq!(l.table.as_deref(), Some("u"));
    assert_eq!(r.table.as_deref(), Some("o"));
}

#[test]
fn bogus_qualifier_raises() {
    // `bogus.id` names a table the query does not have in scope - MUST raise.
    let result = bind_sql("SELECT bogus.id FROM pg.public.users");
    assert!(
        matches!(result, Err(BindError::TableNotInScope { .. })),
        "{result:?}"
    );
}

#[test]
fn typo_column_raises() {
    let result = bind_sql("SELECT naem FROM pg.public.users");
    assert!(
        matches!(result, Err(BindError::ColumnNotInScope(_))),
        "{result:?}"
    );
}

#[test]
fn qualified_missing_column_raises() {
    let result = bind_sql("SELECT users.nope FROM pg.public.users");
    assert!(
        matches!(result, Err(BindError::ColumnNotInTable { .. })),
        "{result:?}"
    );
}

#[test]
fn ambiguous_column_raises() {
    // `id` exists in both users and orders; unqualified is ambiguous.
    let result = bind_sql(
        "SELECT id FROM pg.public.users AS u JOIN pg.public.orders AS o ON u.id = o.user_id",
    );
    assert!(
        matches!(result, Err(BindError::AmbiguousColumn(_))),
        "{result:?}"
    );
}

#[test]
fn missing_table_raises() {
    let result = bind_sql("SELECT x FROM pg.public.nonexistent");
    assert!(
        matches!(result, Err(BindError::TableNotFound(_))),
        "{result:?}"
    );
}

#[test]
fn case_insensitive_qualifier() {
    // A `USERS` qualifier resolves against the `users` relation.
    let plan = bind_sql("SELECT USERS.name FROM pg.public.users").unwrap();
    assert_all_qualified(&plan);
}

#[test]
fn aggregate_group_and_agg_resolve() {
    let plan = bind_sql("SELECT age, COUNT(*) AS n FROM pg.public.users GROUP BY age").unwrap();
    let LogicalPlan::Aggregate(aggregate) = &plan else {
        panic!("expected Aggregate");
    };
    assert_all_qualified(&plan);
    // The grouping key resolved to a typed users.age.
    let Expr::Column(age) = &aggregate.group_by[0] else {
        panic!("expected column");
    };
    assert_eq!(age.data_type, Some(DataType::Integer));
}
