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

#[test]
fn having_output_alias_resolves() {
    // HAVING references the aggregate output alias `n` (not a base column).
    let plan = bind_sql("SELECT age, COUNT(*) AS n FROM pg.public.users GROUP BY age HAVING n > 5")
        .unwrap();
    let LogicalPlan::Filter(filter) = &plan else {
        panic!("expected HAVING Filter");
    };
    let Expr::BinaryOp { left, .. } = &filter.predicate else {
        panic!("expected comparison");
    };
    let Expr::Column(n) = left.as_ref() else {
        panic!("expected column");
    };
    // The alias reference stays bare (an output, not a base-table column) and
    // takes the aggregate's type (COUNT -> BIGINT).
    assert_eq!(n.table, None);
    assert_eq!(n.column, "n");
    assert_eq!(n.data_type, Some(DataType::BigInt));
}

#[test]
fn order_by_output_alias_resolves() {
    let plan =
        bind_sql("SELECT age, COUNT(*) AS n FROM pg.public.users GROUP BY age ORDER BY n DESC")
            .unwrap();
    let LogicalPlan::Sort(sort) = &plan else {
        panic!("expected Sort");
    };
    let Expr::Column(key) = &sort.sort_keys[0] else {
        panic!("expected column key");
    };
    assert_eq!(key.column, "n");
    assert_eq!(key.data_type, Some(DataType::BigInt));
}

#[test]
fn order_by_positional_ordinal() {
    // ORDER BY 2 orders by the 2nd output (name), not the constant 2.
    let plan = bind_sql("SELECT name, age FROM pg.public.users ORDER BY 2").unwrap();
    let LogicalPlan::Sort(sort) = &plan else {
        panic!("expected Sort");
    };
    let Expr::Column(key) = &sort.sort_keys[0] else {
        panic!("expected column, got {:?}", sort.sort_keys[0]);
    };
    assert_eq!(key.column, "age");
}

#[test]
fn derived_table_binds_and_scopes() {
    let plan =
        bind_sql("SELECT d.total FROM (SELECT total FROM pg.public.orders WHERE total > 1) AS d")
            .unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    // Its input is a bound SubqueryScan whose alias `d` scoped d.total.
    let LogicalPlan::SubqueryScan(subquery_scan) = projection.input.as_ref() else {
        panic!("expected SubqueryScan");
    };
    assert_eq!(subquery_scan.alias, "d");
    // d.total resolved with the DOUBLE type from the inner orders.total.
    let Expr::Column(total) = &projection.expressions[0] else {
        panic!("expected column");
    };
    assert_eq!(total.table.as_deref(), Some("d"));
    assert_eq!(total.data_type, Some(DataType::Double));
}

#[test]
fn cte_binds_and_reference_resolves() {
    let plan = bind_sql(
        "WITH recent AS (SELECT id, total FROM pg.public.orders) \
         SELECT recent.total FROM recent",
    )
    .unwrap();
    let LogicalPlan::Cte(cte) = &plan else {
        panic!("expected Cte");
    };
    // The child body resolved recent.total against the CTE's output columns.
    let LogicalPlan::Projection(body) = cte.child.as_ref() else {
        panic!("expected Projection body");
    };
    let Expr::Column(total) = &body.expressions[0] else {
        panic!("expected column");
    };
    assert_eq!(total.table.as_deref(), Some("recent"));
    assert_eq!(total.data_type, Some(DataType::Double));
}

#[test]
fn set_operation_arity_mismatch_raises() {
    let result =
        bind_sql("SELECT id, name FROM pg.public.users UNION SELECT id FROM pg.public.orders");
    assert!(
        matches!(result, Err(BindError::SetOpArity { .. })),
        "{result:?}"
    );
}

#[test]
fn set_operation_matching_arity_binds() {
    let plan =
        bind_sql("SELECT id FROM pg.public.users UNION SELECT id FROM pg.public.orders").unwrap();
    assert!(matches!(plan, LogicalPlan::SetOperation(_)));
}

#[test]
fn in_subquery_binds_inner_plan() {
    let plan = bind_sql(
        "SELECT name FROM pg.public.users \
         WHERE id IN (SELECT user_id FROM pg.public.orders WHERE total > 100)",
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let Expr::InSubquery {
        value, subquery, ..
    } = &filter.predicate
    else {
        panic!("expected InSubquery");
    };
    // The outer value resolved to users.id.
    let Expr::Column(id) = value.as_ref() else {
        panic!("expected column");
    };
    assert_eq!(id.table.as_deref(), Some("users"));
    // The subquery's own plan is bound (a Projection over orders).
    assert!(matches!(subquery.as_ref(), LogicalPlan::Projection(_)));
}

#[test]
fn correlated_subquery_resolves_outer_column() {
    // EXISTS subquery references the outer users.id (correlated).
    let plan = bind_sql(
        "SELECT name FROM pg.public.users AS u \
         WHERE EXISTS (SELECT 1 FROM pg.public.orders AS o WHERE o.user_id = u.id)",
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let Expr::Exists { subquery, .. } = &filter.predicate else {
        panic!("expected Exists");
    };
    // The subquery bound: o.user_id = u.id, where u.id is the outer correlation.
    let LogicalPlan::Projection(inner) = subquery.as_ref() else {
        panic!("expected inner Projection");
    };
    let LogicalPlan::Filter(inner_filter) = inner.input.as_ref() else {
        panic!("expected inner Filter");
    };
    let Expr::BinaryOp { right, .. } = &inner_filter.predicate else {
        panic!("expected comparison");
    };
    let Expr::Column(outer) = right.as_ref() else {
        panic!("expected column");
    };
    assert_eq!(outer.table.as_deref(), Some("u"));
    assert_eq!(outer.data_type, Some(DataType::Integer));
}
