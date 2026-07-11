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
fn comma_join_folds_into_cross_joins() {
    // Implicit-join syntax (the TPC-H form): FROM a, b, c -> left-deep CROSS joins;
    // the equi-predicates stay in WHERE for the optimizer to recover the graph.
    let plan = parse("SELECT t.a FROM t, s, r WHERE t.id = s.id AND s.k = r.k").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter (the WHERE)");
    };
    // ((t CROSS s) CROSS r)
    let LogicalPlan::Join(outer) = filter.input.as_ref() else {
        panic!("expected outer Join");
    };
    assert_eq!(outer.join_type, fq_plan::JoinType::Cross);
    assert!(outer.condition.is_none());
    let LogicalPlan::Join(inner) = outer.left.as_ref() else {
        panic!("expected inner Join");
    };
    assert_eq!(inner.join_type, fq_plan::JoinType::Cross);
}

#[test]
fn like_and_ilike_map_to_binary_ops() {
    use fq_plan::expr::BinaryOpType;
    let plan = parse("SELECT a FROM t WHERE t.name LIKE 'x%'").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let fq_plan::Expr::BinaryOp { op, .. } = &filter.predicate else {
        panic!("expected a binary op predicate, got {:?}", filter.predicate);
    };
    assert_eq!(*op, BinaryOpType::Like);

    // NOT LIKE wraps the Like in a NOT.
    let plan = parse("SELECT a FROM t WHERE t.name NOT LIKE 'x%'").unwrap();
    let LogicalPlan::Projection(p) = plan else {
        panic!()
    };
    let LogicalPlan::Filter(f) = p.input.as_ref() else {
        panic!()
    };
    assert!(matches!(
        &f.predicate,
        fq_plan::Expr::UnaryOp {
            op: fq_plan::expr::UnaryOpType::Not,
            ..
        }
    ));
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
fn distinct_sets_projection_flag() {
    let plan = parse("SELECT DISTINCT name FROM t").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert!(projection.distinct);
}

#[test]
fn garbage_sql_raises_parse_error() {
    assert!(matches!(
        parse("SELECT FROM WHERE"),
        Err(ParseError::Parse(_))
    ));
}

#[test]
fn cast_carries_target_type() {
    let plan = parse("SELECT CAST(a AS DECIMAL(10, 2)) AS d FROM t").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let Expr::Cast { target_type, .. } = &projection.expressions[0] else {
        panic!("expected Cast, got {:?}", projection.expressions[0]);
    };
    assert_eq!(target_type, "DECIMAL(10, 2)");
}

#[test]
fn double_colon_cast() {
    let plan = parse("SELECT a::int FROM t").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert!(matches!(projection.expressions[0], Expr::Cast { .. }));
}

#[test]
fn simple_case_lowers_to_searched() {
    // CASE x WHEN 1 THEN 'a' ELSE 'b' END -> searched form: WHEN x = 1 ...
    let plan = parse("SELECT CASE x WHEN 1 THEN 'a' ELSE 'b' END AS c FROM t").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let Expr::Case { when_clauses, .. } = &projection.expressions[0] else {
        panic!("expected Case");
    };
    // The branch condition is now `x = 1`, not the bare literal `1`.
    assert!(matches!(
        when_clauses[0].0,
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            ..
        }
    ));
}

#[test]
fn between_in_where() {
    let plan = parse("SELECT a FROM t WHERE a BETWEEN 1 AND 10").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    assert!(matches!(filter.predicate, Expr::Between { .. }));
}

#[test]
fn not_between_wraps_in_not() {
    let plan = parse("SELECT a FROM t WHERE a NOT BETWEEN 1 AND 10").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    assert!(matches!(
        filter.predicate,
        Expr::UnaryOp {
            op: fq_plan::UnaryOpType::Not,
            ..
        }
    ));
}

#[test]
fn in_list_predicate() {
    let plan = parse("SELECT a FROM t WHERE a IN (1, 2, 3)").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let Expr::InList { options, .. } = &filter.predicate else {
        panic!("expected InList, got {:?}", filter.predicate);
    };
    assert_eq!(options.len(), 3);
}

#[test]
fn in_subquery_predicate() {
    let plan = parse("SELECT id FROM t WHERE id IN (SELECT uid FROM s)").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let Expr::InSubquery { subquery, .. } = &filter.predicate else {
        panic!("expected InSubquery");
    };
    // The subquery converted to a full plan.
    assert!(matches!(subquery.as_ref(), LogicalPlan::Projection(_)));
}

#[test]
fn exists_predicate() {
    let plan = parse("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.k = t.id)").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    assert!(matches!(filter.predicate, Expr::Exists { .. }));
}

#[test]
fn scalar_subquery_in_select() {
    let plan = parse("SELECT (SELECT MAX(v) FROM s) AS m FROM t").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert!(matches!(projection.expressions[0], Expr::Subquery { .. }));
}

#[test]
fn union_all_builds_set_operation() {
    let plan = parse("SELECT a FROM t UNION ALL SELECT b FROM s").unwrap();
    let LogicalPlan::SetOperation(setop) = plan else {
        panic!("expected SetOperation, got {plan:?}");
    };
    assert_eq!(setop.kind, fq_plan::SetOpKind::Union);
    assert!(!setop.distinct); // ALL
}

#[test]
fn intersect_and_except() {
    assert!(matches!(
        parse("SELECT a FROM t INTERSECT SELECT b FROM s").unwrap(),
        LogicalPlan::SetOperation(_)
    ));
    assert!(matches!(
        parse("SELECT a FROM t EXCEPT SELECT b FROM s").unwrap(),
        LogicalPlan::SetOperation(_)
    ));
}

#[test]
fn derived_table_in_from() {
    let plan = parse("SELECT d.x FROM (SELECT x FROM t WHERE x > 1) AS d").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::SubqueryScan(subquery_scan) = projection.input.as_ref() else {
        panic!("expected SubqueryScan, got {:?}", projection.input);
    };
    assert_eq!(subquery_scan.alias, "d");
    assert!(matches!(
        subquery_scan.input.as_ref(),
        LogicalPlan::Projection(_)
    ));
}

#[test]
fn values_clause() {
    let plan = parse("VALUES (1, 'a'), (2, 'b')").unwrap();
    let LogicalPlan::Values(values) = plan else {
        panic!("expected Values, got {plan:?}");
    };
    assert_eq!(values.rows.len(), 2);
    assert_eq!(values.rows[0].len(), 2);
}

#[test]
fn cte_builds_cte_node_and_ref() {
    // WITH c AS (SELECT a FROM t) SELECT a FROM c
    let plan = parse("WITH c AS (SELECT a FROM t) SELECT a FROM c").unwrap();
    let LogicalPlan::Cte(cte) = plan else {
        panic!("expected Cte, got {plan:?}");
    };
    assert_eq!(cte.name, "c");
    assert!(matches!(cte.cte_plan.as_ref(), LogicalPlan::Projection(_)));
    // The main body references the CTE by name (a CteRef, not a base Scan).
    let LogicalPlan::Projection(main) = cte.child.as_ref() else {
        panic!("expected Projection body");
    };
    assert!(matches!(main.input.as_ref(), LogicalPlan::CteRef(_)));
}

#[test]
fn recursive_cte_raises() {
    let result = parse("WITH RECURSIVE c AS (SELECT 1 AS n) SELECT n FROM c");
    assert!(matches!(result, Err(ParseError::Unsupported(_))));
}

/// The converted single expression of `SELECT <expr> FROM t`.
fn select_expr(sql: &str) -> Expr {
    let plan = parse(sql).unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    projection.expressions.into_iter().next().unwrap()
}

#[test]
fn typed_scalar_functions_become_function_calls() {
    for (sql, name, arg_count) in [
        ("SELECT UPPER(a) FROM t", "UPPER", 1),
        ("SELECT LOWER(a) FROM t", "LOWER", 1),
        ("SELECT ABS(a) FROM t", "ABS", 1),
        ("SELECT COALESCE(a, b, 0) FROM t", "COALESCE", 3),
        ("SELECT NULLIF(a, b) FROM t", "NULLIF", 2),
        ("SELECT SUBSTRING(a FROM 1 FOR 3) FROM t", "SUBSTRING", 3),
        ("SELECT ROUND(a, 2) FROM t", "ROUND", 2),
        ("SELECT REPLACE(a, 'x', 'y') FROM t", "REPLACE", 3),
    ] {
        let Expr::FunctionCall {
            function_name,
            args,
            is_aggregate,
            ..
        } = select_expr(sql)
        else {
            panic!("{sql}: expected FunctionCall");
        };
        assert_eq!(function_name, name, "{sql}");
        assert_eq!(args.len(), arg_count, "{sql}");
        assert!(!is_aggregate, "{sql}");
    }
}

#[test]
fn extract_becomes_extract_node() {
    let Expr::Extract { field, .. } = select_expr("SELECT EXTRACT(YEAR FROM d) FROM t") else {
        panic!("expected Extract");
    };
    assert_eq!(field, "YEAR");
}

#[test]
fn unknown_function_raises() {
    // A typed function the engine does not model fails loudly, not silently.
    let result = parse("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a) FROM t");
    assert!(matches!(result, Err(ParseError::Unsupported(_))));
}

// --- regressions from the fq-parse review ---

#[test]
fn is_null_and_is_not_null() {
    // Review bug #2: polyglot emits a dedicated IsNull variant; these must map to
    // the unary IS NULL / IS NOT NULL, not be rejected as an unknown function.
    let Expr::UnaryOp { op, .. } = select_pred("SELECT a FROM t WHERE a IS NULL") else {
        panic!("expected unary");
    };
    assert_eq!(op, fq_plan::UnaryOpType::IsNull);
    let Expr::UnaryOp { op, .. } = select_pred("SELECT a FROM t WHERE a IS NOT NULL") else {
        panic!("expected unary");
    };
    assert_eq!(op, fq_plan::UnaryOpType::IsNotNull);
}

/// The WHERE predicate of `SELECT ... FROM t WHERE <pred>`.
fn select_pred(sql: &str) -> Expr {
    let LogicalPlan::Projection(projection) = parse(sql).unwrap() else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = *projection.input else {
        panic!("expected Filter");
    };
    filter.predicate
}

#[test]
fn star_except_raises_not_silently_drops() {
    // Review bug #1: a star modifier changes the column set; unhandled it must
    // raise, never silently expand to the wrong columns.
    let result = parse("SELECT * EXCEPT (secret) FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
}

#[test]
fn simple_case_with_function_operand_raises() {
    // Review bug #3: a function-bearing operand would be duplicated per branch.
    let result = parse("SELECT CASE upper(a) WHEN 'X' THEN 1 ELSE 2 END FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
    // A plain-column operand still lowers fine.
    assert!(parse("SELECT CASE a WHEN 'X' THEN 1 ELSE 2 END FROM t").is_ok());
}
