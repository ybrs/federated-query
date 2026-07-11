//! fq-parse structural tests: the single-table SELECT pipeline and the defensive
//! rejections. A first slice of test_parser.py; joins/stars/subqueries land with
//! their converters.

use fq_parse::{parse, ParseError};
use fq_plan::{BinaryOpType, Expr, LogicalPlan};

#[test]
fn simple_scan_projection() {
    // SELECT name FROM pg.public.users
    let plan = parse("SELECT name FROM pg.public.users").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection, got {plan:?}");
    };
    assert_eq!(projection.aliases, vec!["name"]);
    let LogicalPlan::Scan(scan) = projection.input.as_ref() else {
        panic!("expected Scan");
    };
    assert_eq!(scan.datasource, "pg");
    assert_eq!(scan.schema_name, "public");
    assert_eq!(scan.table_name, "users");
    assert_eq!(scan.columns, vec!["name"]);
}

#[test]
fn where_becomes_filter_over_scan() {
    // SELECT name FROM users WHERE age > 30
    let plan = parse("SELECT name FROM users WHERE age > 30").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter under Projection");
    };
    assert!(matches!(
        filter.predicate,
        Expr::BinaryOp {
            op: BinaryOpType::Gt,
            ..
        }
    ));
    assert!(matches!(filter.input.as_ref(), LogicalPlan::Scan(_)));
    // Over-collected referenced columns: name and age.
    let LogicalPlan::Scan(scan) = filter.input.as_ref() else {
        unreachable!()
    };
    assert!(scan.columns.contains(&"name".to_string()));
    assert!(scan.columns.contains(&"age".to_string()));
    // A bare table name defaults schema to public, datasource empty.
    assert_eq!(scan.schema_name, "public");
}

#[test]
fn group_by_aggregate_pipeline() {
    // The architecture walkthrough query.
    let plan = parse(
        "SELECT city, COUNT(*) AS n FROM pg.public.users WHERE age > 30 \
         GROUP BY city ORDER BY n DESC LIMIT 10",
    )
    .unwrap();

    // Top: Limit -> Sort -> Aggregate -> Filter -> Scan.
    let LogicalPlan::Limit(limit) = plan else {
        panic!("expected Limit at root");
    };
    assert_eq!(limit.limit, Some(10));
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        panic!("expected Sort");
    };
    assert_eq!(sort.ascending, vec![false]); // DESC
    let LogicalPlan::Aggregate(aggregate) = sort.input.as_ref() else {
        panic!("expected Aggregate");
    };
    assert_eq!(aggregate.output_names, vec!["city", "n"]);
    assert_eq!(aggregate.group_by.len(), 1);
    // The aggregates list carries both the group column and the COUNT.
    assert_eq!(aggregate.aggregates.len(), 2);
    assert!(fq_plan::contains_aggregate(&aggregate.aggregates[1]));
    assert!(matches!(aggregate.input.as_ref(), LogicalPlan::Filter(_)));
}

#[test]
fn having_wraps_aggregate() {
    let plan =
        parse("SELECT city, COUNT(*) AS n FROM users GROUP BY city HAVING COUNT(*) > 5").unwrap();
    let LogicalPlan::Filter(having) = plan else {
        panic!("expected HAVING Filter at root");
    };
    assert!(matches!(having.input.as_ref(), LogicalPlan::Aggregate(_)));
}

#[test]
fn offset_only_becomes_limit_node() {
    let plan = parse("SELECT name FROM users OFFSET 5").unwrap();
    let LogicalPlan::Limit(limit) = plan else {
        panic!("expected Limit");
    };
    assert_eq!(limit.limit, None);
    assert_eq!(limit.offset, 5);
}

#[test]
fn multi_statement_raises() {
    assert_eq!(
        parse("SELECT 1 FROM t; SELECT 2 FROM t"),
        Err(ParseError::MultiStatement)
    );
}

#[test]
fn inner_join_builds_join_node() {
    // SELECT u.name, o.total FROM pg.public.users u JOIN duck.main.orders o ON ...
    let plan = parse(
        "SELECT u.name, o.total FROM pg.public.users AS u \
         JOIN duck.main.orders AS o ON u.id = o.user_id WHERE u.age > 30",
    )
    .unwrap();
    // Projection -> Filter -> Join(Scan u, Scan o).
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let LogicalPlan::Join(join) = filter.input.as_ref() else {
        panic!("expected Join");
    };
    assert_eq!(join.join_type, fq_plan::JoinType::Inner);
    assert!(join.condition.is_some());
    let LogicalPlan::Scan(left) = join.left.as_ref() else {
        panic!("left scan");
    };
    let LogicalPlan::Scan(right) = join.right.as_ref() else {
        panic!("right scan");
    };
    assert_eq!(left.datasource, "pg");
    assert_eq!(left.alias.as_deref(), Some("u"));
    assert_eq!(right.datasource, "duck");
    assert_eq!(right.table_name, "orders");
    // Column over-collection is partitioned by qualifier: id/name/age -> u,
    // user_id/total -> o.
    assert!(left.columns.contains(&"name".to_string()));
    assert!(left.columns.contains(&"id".to_string()));
    assert!(right.columns.contains(&"total".to_string()));
    assert!(right.columns.contains(&"user_id".to_string()));
    assert!(!right.columns.contains(&"name".to_string()));
}

#[test]
fn left_join_maps_join_type() {
    let plan = parse("SELECT a FROM t LEFT JOIN s ON t.id = s.id").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        panic!("expected Join");
    };
    assert_eq!(join.join_type, fq_plan::JoinType::Left);
}

#[test]
fn star_without_catalog_raises() {
    // No catalog -> the star cannot expand and must fail loudly.
    let result = parse("SELECT * FROM t");
    assert!(matches!(result, Err(ParseError::Unsupported(_))));
}

#[test]
fn star_expands_from_catalog() {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;
    use fq_parse::parse_with_catalog;

    let mut catalog = Catalog::new();
    let users = Table::new(
        "users",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
        ],
    );
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![users]),
    );

    let plan = parse_with_catalog("SELECT * FROM pg.public.users", &catalog).unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    // The star expanded to the table's concrete columns (no `*` survives).
    assert_eq!(projection.aliases, vec!["id", "name"]);
    assert_eq!(projection.expressions.len(), 2);
    // Each expanded ref is qualified to the table.
    let Expr::Column(first) = &projection.expressions[0] else {
        panic!("expected qualified column");
    };
    assert_eq!(first.table.as_deref(), Some("users"));
    assert_eq!(first.column, "id");
}

#[test]
fn qualified_star_expands_one_table() {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;
    use fq_parse::parse_with_catalog;

    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables(
            "public",
            "pg",
            vec![
                Table::new("users", vec![Column::new("id", DataType::Integer, false)]),
                Table::new(
                    "orders",
                    vec![Column::new("total", DataType::Double, false)],
                ),
            ],
        ),
    );

    // o.* -> only the orders columns.
    let plan = parse_with_catalog(
        "SELECT o.* FROM pg.public.users AS u JOIN pg.public.orders AS o ON u.id = o.total",
        &catalog,
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert_eq!(projection.aliases, vec!["total"]);
}

#[test]
fn distinct_is_rejected() {
    let result = parse("SELECT DISTINCT name FROM t");
    assert!(matches!(result, Err(ParseError::Unsupported(_))));
}

#[test]
fn garbage_sql_raises_parse_error() {
    assert!(matches!(
        parse("SELECT FROM WHERE"),
        Err(ParseError::Parse(_))
    ));
}
