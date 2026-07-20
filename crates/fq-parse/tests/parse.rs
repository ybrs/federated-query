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

/// The WHERE predicate of `SELECT a FROM t WHERE <sql>`.
fn where_predicate(sql: &str) -> fq_plan::Expr {
    let plan = parse(&format!("SELECT a FROM t WHERE {sql}")).unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    filter.predicate.clone()
}

#[test]
fn like_and_ilike_map_to_like_nodes() {
    // Plain LIKE: a case-sensitive Like with no escape.
    let fq_plan::Expr::Like {
        case_insensitive,
        escape,
        ..
    } = where_predicate("t.name LIKE 'x%'")
    else {
        panic!("expected a Like predicate");
    };
    assert!(!case_insensitive);
    assert_eq!(escape, None);

    // ILIKE sets case_insensitive.
    let fq_plan::Expr::Like {
        case_insensitive, ..
    } = where_predicate("t.name ILIKE 'x%'")
    else {
        panic!("expected a Like predicate");
    };
    assert!(case_insensitive);

    // NOT LIKE wraps the Like in a NOT.
    assert!(matches!(
        where_predicate("t.name NOT LIKE 'x%'"),
        fq_plan::Expr::UnaryOp {
            op: fq_plan::expr::UnaryOpType::Not,
            ..
        }
    ));
}

#[test]
fn like_escape_clause_is_carried() {
    // LIKE ... ESCAPE keeps the single escape character on the Like node.
    let fq_plan::Expr::Like { escape, .. } = where_predicate("t.name LIKE 'x!%%' ESCAPE '!'")
    else {
        panic!("expected a Like predicate");
    };
    assert_eq!(escape.as_deref(), Some("!"));

    // ILIKE ... ESCAPE combines case-insensitivity with the escape character.
    let fq_plan::Expr::Like {
        case_insensitive,
        escape,
        ..
    } = where_predicate("t.name ILIKE 'x@_' ESCAPE '@'")
    else {
        panic!("expected a Like predicate");
    };
    assert!(case_insensitive);
    assert_eq!(escape.as_deref(), Some("@"));
}

#[test]
fn like_escape_must_be_single_character() {
    // A multi-character or empty escape string is invalid SQL and RAISES rather
    // than being silently truncated or dropped.
    assert!(parse("SELECT a FROM t WHERE t.name LIKE 'x' ESCAPE '!!'").is_err());
    assert!(parse("SELECT a FROM t WHERE t.name LIKE 'x' ESCAPE ''").is_err());
}

#[test]
fn unaliased_table_defaults_alias_to_its_name() {
    // Every table carries an alias - its own name when none is written (ports
    // Python `_extract_table_alias` -> `alias_or_name`). A None alias would make a
    // column bound to the table name unresolvable in the physical alias map (q18).
    let plan = parse("SELECT a FROM t WHERE b > 1").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter");
    };
    let LogicalPlan::Scan(scan) = filter.input.as_ref() else {
        panic!("expected Scan");
    };
    assert_eq!(scan.alias.as_deref(), Some("t"));
}

#[test]
fn aggregate_argument_columns_are_collected_without_catalog() {
    // polyglot's get_columns does NOT descend into aggregate arguments; the
    // catalog-free fallback must still collect a column referenced only inside an
    // aggregate call (SUM(amount)), else a merge-engine aggregate loses it (q18).
    let plan = parse("SELECT SUM(amount) FROM t WHERE id > 0").unwrap();
    let scan = find_scan(&plan).expect("a Scan");
    assert!(
        scan.columns.contains(&"amount".to_string()),
        "aggregate-argument column not collected: {:?}",
        scan.columns
    );
}

#[test]
fn base_scan_over_collects_all_catalog_columns() {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;
    use fq_parse::parse_with_catalog;

    // With a catalog, a base scan over-collects the table's FULL column set (a
    // guaranteed superset - polyglot under-collects typed-function args); projection
    // pushdown trims later. Here only `id` is referenced, but `name`/`age` are kept.
    let mut catalog = Catalog::new();
    let users = Table::new(
        "users",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar, true),
            Column::new("age", DataType::Integer, true),
        ],
    );
    catalog.insert_schema(
        "pg",
        "public",
        Schema::with_tables("public", "pg", vec![users]),
    );
    let plan = parse_with_catalog("SELECT id FROM pg.public.users", &catalog).unwrap();
    let scan = find_scan(&plan).expect("a Scan");
    assert!(scan.columns.contains(&"name".to_string()));
    assert!(scan.columns.contains(&"age".to_string()));
}

/// The first Scan found in depth-first order, if any.
fn find_scan(plan: &LogicalPlan) -> Option<&fq_plan::logical::Scan> {
    if let LogicalPlan::Scan(scan) = plan {
        return Some(scan);
    }
    plan.children().into_iter().find_map(find_scan)
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
fn star_modifiers_exclude_and_replace() {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;
    use fq_parse::parse_with_catalog;

    let mut catalog = Catalog::new();
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("region", DataType::Varchar, true),
            Column::new("price", DataType::Double, true),
        ],
    );
    catalog.insert_schema(
        "duck",
        "main",
        Schema::with_tables("main", "duck", vec![orders]),
    );

    // EXCLUDE drops region; the output list is id, price.
    let plan =
        parse_with_catalog("SELECT * EXCLUDE (region) FROM duck.main.orders", &catalog).unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert_eq!(projection.aliases, vec!["id", "price"]);

    // REPLACE substitutes an expression for price, keeping its name and position.
    let plan = parse_with_catalog(
        "SELECT * REPLACE (price + 1 AS price) FROM duck.main.orders",
        &catalog,
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert_eq!(projection.aliases, vec!["id", "region", "price"]);
    // The price output is now an expression, not a bare column.
    assert!(
        matches!(projection.expressions[2], Expr::BinaryOp { .. }),
        "price must be replaced by an expression, got {:?}",
        projection.expressions[2]
    );
}

#[test]
fn star_rename_renames_output_column() {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;
    use fq_parse::parse_with_catalog;

    let mut catalog = Catalog::new();
    let orders = Table::new(
        "orders",
        vec![
            Column::new("id", DataType::Integer, false),
            Column::new("region", DataType::Varchar, true),
        ],
    );
    catalog.insert_schema(
        "duck",
        "main",
        Schema::with_tables("main", "duck", vec![orders]),
    );

    let plan = parse_with_catalog(
        "SELECT * RENAME (region AS area) FROM duck.main.orders",
        &catalog,
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    assert_eq!(projection.aliases, vec!["id", "area"]);
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
fn interval_literals_convert_to_interval_exprs() {
    // Postgres text form: the unit rides inside the quoted value.
    let plan = parse("SELECT a FROM t WHERE d > CURRENT_DATE - INTERVAL '30 days'").unwrap();
    let mut found = Vec::new();
    collect_intervals(&plan, &mut found);
    assert_eq!(found, vec![("30 days".to_string(), None)]);

    // Unit-keyword form: INTERVAL '1' YEAR.
    let plan = parse("SELECT a FROM t WHERE d < DATE '1994-01-01' + INTERVAL '1' YEAR").unwrap();
    let mut found = Vec::new();
    collect_intervals(&plan, &mut found);
    assert_eq!(found, vec![("1".to_string(), Some("YEAR".to_string()))]);
}

/// Collect every `Expr::Interval` (value, unit) in a plan tree, in walk order.
fn collect_intervals(plan: &LogicalPlan, found: &mut Vec<(String, Option<String>)>) {
    for expr in plan.direct_expressions() {
        collect_expr_intervals(expr, found);
    }
    for child in plan.children() {
        collect_intervals(child, found);
    }
}

fn collect_expr_intervals(expr: &Expr, found: &mut Vec<(String, Option<String>)>) {
    if let Expr::Interval { value, unit } = expr {
        found.push((value.clone(), unit.clone()));
    }
    for child in expr.children() {
        collect_expr_intervals(child, found);
    }
}

#[test]
fn union_order_by_and_limit_wrap_the_set_operation() {
    // ORDER BY / LIMIT / OFFSET after a set operation apply to the ENTIRE
    // combined result: Limit over Sort over SetOperation.
    let plan = parse("SELECT a FROM t UNION ALL SELECT b FROM s ORDER BY a DESC LIMIT 5 OFFSET 2")
        .unwrap();
    let LogicalPlan::Limit(limit) = plan else {
        panic!("expected Limit, got {plan:?}");
    };
    assert_eq!(limit.limit, Some(5));
    assert_eq!(limit.offset, 2);
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        panic!("expected Sort, got {:?}", limit.input);
    };
    assert_eq!(sort.sort_keys.len(), 1);
    assert!(!sort.ascending[0]);
    assert!(matches!(sort.input.as_ref(), LogicalPlan::SetOperation(_)));
}

#[test]
fn union_limit_alone_wraps_the_set_operation() {
    let plan = parse("SELECT a FROM t UNION ALL SELECT b FROM s LIMIT 20").unwrap();
    let LogicalPlan::Limit(limit) = plan else {
        panic!("expected Limit, got {plan:?}");
    };
    assert_eq!(limit.limit, Some(20));
    assert!(matches!(limit.input.as_ref(), LogicalPlan::SetOperation(_)));
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
fn from_less_select_builds_single_row_values() {
    // SELECT 1 AS n is a single constant row, modelled as a Values relation.
    let plan = parse("SELECT 1 AS n, 'x' AS s").unwrap();
    let LogicalPlan::Values(values) = plan else {
        panic!("expected Values, got {plan:?}");
    };
    assert_eq!(values.rows.len(), 1);
    assert_eq!(values.rows[0].len(), 2);
    assert_eq!(values.output_names, vec!["n", "s"]);
}

#[test]
fn from_less_select_with_relation_clause_raises() {
    // A clause that needs an input relation has nothing to act on.
    let result = parse("SELECT 1 AS n WHERE n > 0");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
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
    let result = parse("SELECT COVAR_POP(a, b) FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
}

// --- conversion edge cases that must not regress ---

#[test]
fn is_null_and_is_not_null() {
    // polyglot emits a dedicated IsNull variant; these must map to
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
    // A star modifier changes the column set; unhandled it must
    // raise, never silently expand to the wrong columns.
    let result = parse("SELECT * EXCEPT (secret) FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
}

#[test]
fn simple_case_with_function_operand_raises() {
    // A function-bearing operand would be duplicated per branch.
    let result = parse("SELECT CASE upper(a) WHEN 'X' THEN 1 ELSE 2 END FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
    // A plain-column operand still lowers fine.
    assert!(parse("SELECT CASE a WHEN 'X' THEN 1 ELSE 2 END FROM t").is_ok());
}

/// The single window expression a `SELECT <window> FROM ...` projects. A window
/// over an aggregate routes through an `Aggregate` node, a ranking window through
/// a `Projection`, so read the first output expression from whichever holds it.
fn only_window(sql: &str) -> Expr {
    match parse(sql).unwrap() {
        LogicalPlan::Projection(projection) => projection.expressions.into_iter().next().unwrap(),
        LogicalPlan::Aggregate(aggregate) => aggregate.aggregates.into_iter().next().unwrap(),
        other => panic!("expected Projection or Aggregate, got {other:?}"),
    }
}

#[test]
fn ranking_window_is_non_aggregate() {
    // rank() OVER (...) mints a NON-aggregate zero-arg call: the emitter renders a
    // zero-arg aggregate as the star form, so an aggregate RANK would emit RANK(*).
    let expr = only_window("SELECT rank() OVER (PARTITION BY a ORDER BY b) AS r FROM t");
    let Expr::Window {
        function,
        partition_by,
        order_keys,
        order_ascending,
        frame,
        ..
    } = expr
    else {
        panic!("expected Window, got {expr:?}");
    };
    let Expr::FunctionCall {
        function_name,
        args,
        is_aggregate,
        ..
    } = function.as_ref()
    else {
        panic!("expected FunctionCall function, got {function:?}");
    };
    assert_eq!(function_name, "RANK");
    assert!(args.is_empty());
    assert!(!is_aggregate, "ranking function must be non-aggregate");
    assert_eq!(partition_by.len(), 1);
    assert_eq!(order_keys.len(), 1);
    assert_eq!(order_ascending, vec![true]);
    assert!(frame.is_none());
}

#[test]
fn aggregate_over_window_stays_aggregate() {
    // sum(x) OVER (...) keeps is_aggregate true (COUNT(*) OVER ... renders right).
    let expr = only_window("SELECT sum(x) OVER (PARTITION BY a) AS s FROM t");
    let Expr::Window { function, .. } = expr else {
        panic!("expected Window, got {expr:?}");
    };
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        ..
    } = function.as_ref()
    else {
        panic!("expected FunctionCall, got {function:?}");
    };
    assert_eq!(function_name, "SUM");
    assert!(is_aggregate);
}

#[test]
fn window_frame_carried_as_verbatim_text() {
    // The ROWS/RANGE frame is carried verbatim so it re-renders to a source.
    let expr = only_window(
        "SELECT max(x) OVER (PARTITION BY a ORDER BY b \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
    );
    let Expr::Window { frame, .. } = expr else {
        panic!("expected Window, got {expr:?}");
    };
    assert_eq!(
        frame,
        Some("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string())
    );
}

#[test]
fn window_order_desc_and_row_number() {
    // row_number() with a DESC window ORDER BY records direction.
    let expr = only_window("SELECT row_number() OVER (ORDER BY b DESC) FROM t");
    let Expr::Window {
        function,
        order_ascending,
        ..
    } = expr
    else {
        panic!("expected Window, got {expr:?}");
    };
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        ..
    } = function.as_ref()
    else {
        panic!("expected FunctionCall, got {function:?}");
    };
    assert_eq!(function_name, "ROW_NUMBER");
    assert!(!is_aggregate);
    assert_eq!(order_ascending, vec![false]);
}

/// The (function_name, arg_count) of the single windowed function call a
/// `SELECT <window> FROM ...` projects. The function under an OVER must be a
/// non-aggregate FunctionCall.
fn windowed_call(sql: &str) -> (String, usize) {
    let Expr::Window { function, .. } = only_window(sql) else {
        panic!("expected Window");
    };
    let Expr::FunctionCall {
        function_name,
        args,
        is_aggregate,
        ..
    } = function.as_ref()
    else {
        panic!("expected FunctionCall, got {function:?}");
    };
    assert!(!is_aggregate, "pure window function must be non-aggregate");
    (function_name.clone(), args.len())
}

#[test]
fn lag_lead_carry_expr_offset_default() {
    // LAG/LEAD lower to NAME(expr [, offset [, default]]) inside the window.
    assert_eq!(
        windowed_call("SELECT lag(v) OVER (ORDER BY k) FROM t"),
        ("LAG".to_string(), 1)
    );
    assert_eq!(
        windowed_call("SELECT lag(v, 2) OVER (ORDER BY k) FROM t"),
        ("LAG".to_string(), 2)
    );
    assert_eq!(
        windowed_call("SELECT lead(v, 1, 0) OVER (ORDER BY k) FROM t"),
        ("LEAD".to_string(), 3)
    );
}

#[test]
fn first_last_value_carry_their_argument() {
    assert_eq!(
        windowed_call("SELECT first_value(v) OVER (ORDER BY k) FROM t"),
        ("FIRST_VALUE".to_string(), 1)
    );
    assert_eq!(
        windowed_call("SELECT last_value(v) OVER (ORDER BY k) FROM t"),
        ("LAST_VALUE".to_string(), 1)
    );
}

#[test]
fn ntile_carries_its_bucket_count() {
    assert_eq!(
        windowed_call("SELECT ntile(4) OVER (ORDER BY k) FROM t"),
        ("NTILE".to_string(), 1)
    );
}

#[test]
fn ignore_nulls_window_modifier_raises() {
    // The engine window plan carries no null-treatment modifier; IGNORE NULLS
    // must raise rather than silently drop the requested semantics.
    let result = parse("SELECT lag(v IGNORE NULLS) OVER (ORDER BY k) FROM t");
    assert!(matches!(result, Err(ParseError::Unsupported(_))));
}

#[test]
fn is_distinct_from_becomes_null_safe_neq_predicate() {
    // a IS DISTINCT FROM b is a general predicate lowering to the NullSafeNeq
    // binary operator, not an unsupported scalar function.
    let plan = parse("SELECT a FROM t WHERE a IS DISTINCT FROM b").unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter under Projection");
    };
    assert!(matches!(
        filter.predicate,
        Expr::BinaryOp {
            op: BinaryOpType::NullSafeNeq,
            ..
        }
    ));
}

#[test]
fn window_in_where_raises() {
    // Window functions evaluate after WHERE, so one there is invalid SQL.
    let result = parse("SELECT a FROM t WHERE rank() OVER (ORDER BY b) > 1");
    assert!(
        matches!(&result, Err(ParseError::Unsupported(message)) if message.contains("WHERE")),
        "{result:?}"
    );
}

#[test]
fn window_in_having_raises() {
    let result = parse("SELECT a, sum(x) FROM t GROUP BY a HAVING rank() OVER (ORDER BY a) > 1");
    assert!(
        matches!(&result, Err(ParseError::Unsupported(message)) if message.contains("HAVING")),
        "{result:?}"
    );
}

#[test]
fn window_in_group_by_raises() {
    let result = parse("SELECT a FROM t GROUP BY rank() OVER (ORDER BY a)");
    assert!(
        matches!(&result, Err(ParseError::Unsupported(message)) if message.contains("GROUP BY")),
        "{result:?}"
    );
}

#[test]
fn window_in_nested_subquery_is_scoped() {
    // A ranked derived table filtered by an outer WHERE is legal: the window
    // belongs to the subquery's own SELECT, so the outer WHERE walk prunes it.
    let plan = parse(
        "SELECT item FROM \
         (SELECT item, rank() OVER (ORDER BY s) AS rnk FROM t) v \
         WHERE rnk < 11",
    );
    assert!(plan.is_ok(), "{plan:?}");
}

#[test]
fn distinct_with_window_raises() {
    // DISTINCT would be silently dropped over a windowed projection.
    let result = parse("SELECT DISTINCT rank() OVER (ORDER BY b) FROM t");
    assert!(
        matches!(&result, Err(ParseError::Unsupported(message)) if message.contains("DISTINCT")),
        "{result:?}"
    );
}

#[test]
fn named_window_inlines_partition() {
    // WINDOW w AS (PARTITION BY a) inlines into SUM(x) OVER w.
    let expr = only_window("SELECT sum(x) OVER w AS s FROM t WINDOW w AS (PARTITION BY a)");
    let Expr::Window {
        partition_by,
        order_keys,
        ..
    } = expr
    else {
        panic!("expected Window, got {expr:?}");
    };
    assert_eq!(partition_by.len(), 1);
    assert!(order_keys.is_empty());
}

#[test]
fn named_window_reference_merges_added_order_by() {
    // OVER (w ORDER BY b DESC) takes PARTITION BY from w and ORDER BY from the
    // reference.
    let expr = only_window(
        "SELECT rank() OVER (w ORDER BY b DESC) AS r FROM t WINDOW w AS (PARTITION BY a)",
    );
    let Expr::Window {
        partition_by,
        order_keys,
        order_ascending,
        ..
    } = expr
    else {
        panic!("expected Window, got {expr:?}");
    };
    assert_eq!(partition_by.len(), 1);
    assert_eq!(order_keys.len(), 1);
    assert_eq!(order_ascending, vec![false]);
}

#[test]
fn qualify_lowers_to_filter_over_subquery() {
    // QUALIFY filters on window outputs: the SELECT becomes a subquery and the
    // QUALIFY predicate a Filter over it.
    let plan =
        parse("SELECT order_id, row_number() OVER (ORDER BY price) AS rn FROM t QUALIFY rn = 1")
            .unwrap();
    // Root projection re-exposes the visible columns; under it a Filter over the
    // wrapping subquery applies the QUALIFY predicate.
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection at root, got {plan:?}");
    };
    assert_eq!(projection.aliases, vec!["order_id", "rn"]);
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!(
            "expected Filter under projection, got {:?}",
            projection.input
        );
    };
    assert!(
        matches!(filter.input.as_ref(), LogicalPlan::SubqueryScan(_)),
        "QUALIFY filter must sit over a subquery, got {:?}",
        filter.input
    );
}

#[test]
fn qualify_over_inline_window_raises() {
    // Only a QUALIFY over a SELECT-list alias is modeled; an inline window in the
    // predicate is not, and must fail loudly rather than build an invalid filter.
    let result = parse("SELECT order_id FROM t QUALIFY row_number() OVER (ORDER BY price) = 1");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
}

#[test]
fn fetch_first_rows_only_becomes_limit() {
    // FETCH FIRST n ROWS ONLY is exactly LIMIT n.
    let plan = parse("SELECT a FROM t ORDER BY a FETCH FIRST 3 ROWS ONLY").unwrap();
    let LogicalPlan::Limit(limit) = plan else {
        panic!("expected Limit, got {plan:?}");
    };
    assert_eq!(limit.limit, Some(3));
}

#[test]
fn fetch_with_ties_or_percent_raises() {
    // WITH TIES and PERCENT change the row set and are not modeled; they raise.
    for sql in [
        "SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS WITH TIES",
        "SELECT a FROM t ORDER BY a FETCH FIRST 10 PERCENT ROWS ONLY",
    ] {
        let result = parse(sql);
        assert!(
            matches!(result, Err(ParseError::Unsupported(_))),
            "{sql}: {result:?}"
        );
    }
}

#[test]
fn undefined_named_window_reference_raises() {
    // An OVER w with no matching WINDOW clause cannot be resolved; it must not be
    // silently dropped.
    let result = parse("SELECT rank() OVER w FROM t");
    assert!(
        matches!(result, Err(ParseError::Unsupported(_))),
        "{result:?}"
    );
}

#[test]
fn comma_join_mixed_with_explicit_join() {
    // FROM a LEFT JOIN b ON ..., c, d : the comma items fold left-deep as CROSS
    // joins onto the explicit-join chain (implicit joins).
    let plan = parse(
        "SELECT a.x FROM a LEFT JOIN b ON a.id = b.id, c, d \
         WHERE a.k = c.k AND a.k = d.k",
    )
    .unwrap();
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected Projection, got {plan:?}");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected Filter, got {:?}", projection.input);
    };
    // Two implicit CROSS folds (c then d) over the LEFT join of a and b.
    let LogicalPlan::Join(outer) = filter.input.as_ref() else {
        panic!("expected Join, got {:?}", filter.input);
    };
    assert!(matches!(outer.join_type, fq_plan::JoinType::Cross));
    let LogicalPlan::Join(inner) = outer.left.as_ref() else {
        panic!("expected nested Join, got {:?}", outer.left);
    };
    assert!(matches!(inner.join_type, fq_plan::JoinType::Cross));
    let LogicalPlan::Join(base) = inner.left.as_ref() else {
        panic!("expected base Join, got {:?}", inner.left);
    };
    assert!(matches!(base.join_type, fq_plan::JoinType::Left));
}

#[test]
fn stddev_samp_is_an_aggregate() {
    // STDDEV_SAMP reaches the aggregate machinery (is_aggregate), routing the
    // query through an Aggregate node.
    let plan = parse("SELECT stddev_samp(x) AS s FROM t").unwrap();
    let LogicalPlan::Aggregate(aggregate) = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        ..
    } = &aggregate.aggregates[0]
    else {
        panic!("expected FunctionCall, got {:?}", aggregate.aggregates[0]);
    };
    assert_eq!(function_name, "STDDEV_SAMP");
    assert!(is_aggregate);
}

#[test]
fn stddev_samp_argument_column_is_over_collected() {
    // A column referenced only inside STDDEV_SAMP must still reach the scan, or
    // the aggregate could not find it (the aggregate-argument column-drop class).
    let plan = parse("SELECT stddev_samp(quantity) FROM t").unwrap();
    let LogicalPlan::Aggregate(aggregate) = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    let LogicalPlan::Scan(scan) = aggregate.input.as_ref() else {
        panic!("expected Scan, got {:?}", aggregate.input);
    };
    assert!(
        scan.columns.contains(&"quantity".to_string()),
        "scan columns {:?} must include the stddev argument",
        scan.columns
    );
}

/// Parse a single-aggregate SELECT and return the first aggregate expression's
/// (function_name, is_aggregate). Each variance/stddev/string/bool aggregate must
/// route through the aggregate machinery under a canonical name.
fn only_aggregate_call(sql: &str) -> (String, bool) {
    let LogicalPlan::Aggregate(aggregate) = parse(sql).unwrap() else {
        panic!("expected Aggregate for {sql}");
    };
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        ..
    } = &aggregate.aggregates[0]
    else {
        panic!("expected FunctionCall, got {:?}", aggregate.aggregates[0]);
    };
    (function_name.clone(), *is_aggregate)
}

#[test]
fn variance_family_are_aggregates() {
    // The variance/stddev family each routes through an Aggregate node under its
    // canonical name so it re-renders to a name every source accepts.
    let cases = [
        ("SELECT var_pop(x) FROM t", "VAR_POP"),
        ("SELECT var_samp(x) FROM t", "VAR_SAMP"),
        ("SELECT variance(x) FROM t", "VARIANCE"),
        ("SELECT stddev(x) FROM t", "STDDEV"),
        ("SELECT stddev_pop(x) FROM t", "STDDEV_POP"),
        ("SELECT stddev_samp(x) FROM t", "STDDEV_SAMP"),
    ];
    for (sql, expected) in cases {
        let (name, is_aggregate) = only_aggregate_call(sql);
        assert_eq!(name, expected, "for {sql}");
        assert!(is_aggregate, "{sql} must be an aggregate");
    }
}

#[test]
fn string_bool_array_aggregates() {
    // STRING_AGG / ARRAY_AGG / BOOL_AND / BOOL_OR are aggregates under canonical
    // names (BOOL_AND/BOOL_OR are the Postgres form of LOGICAL_AND/LOGICAL_OR).
    let (string_name, string_is_agg) = only_aggregate_call("SELECT string_agg(s, ',') FROM t");
    assert_eq!(string_name, "STRING_AGG");
    assert!(string_is_agg);
    let (array_name, array_is_agg) = only_aggregate_call("SELECT array_agg(s) FROM t");
    assert_eq!(array_name, "ARRAY_AGG");
    assert!(array_is_agg);
    for sql in [
        "SELECT bool_and(x > 1) FROM t",
        "SELECT logical_and(x > 1) FROM t",
    ] {
        let (name, is_agg) = only_aggregate_call(sql);
        assert_eq!(name, "BOOL_AND", "for {sql}");
        assert!(is_agg);
    }
    for sql in [
        "SELECT bool_or(x > 1) FROM t",
        "SELECT logical_or(x > 1) FROM t",
    ] {
        let (name, is_agg) = only_aggregate_call(sql);
        assert_eq!(name, "BOOL_OR", "for {sql}");
        assert!(is_agg);
    }
}

#[test]
fn string_agg_carries_separator_argument() {
    // STRING_AGG(value, separator) keeps both arguments so it re-renders exactly.
    let LogicalPlan::Aggregate(aggregate) = parse("SELECT string_agg(s, ', ') FROM t").unwrap()
    else {
        panic!("expected Aggregate");
    };
    let Expr::FunctionCall { args, .. } = &aggregate.aggregates[0] else {
        panic!("expected FunctionCall");
    };
    assert_eq!(args.len(), 2, "value and separator, got {args:?}");
}

#[test]
fn variance_argument_column_is_over_collected() {
    // A column referenced only inside VARIANCE must still reach the scan.
    let LogicalPlan::Aggregate(aggregate) = parse("SELECT variance(quantity) FROM t").unwrap()
    else {
        panic!("expected Aggregate");
    };
    let LogicalPlan::Scan(scan) = aggregate.input.as_ref() else {
        panic!("expected Scan");
    };
    assert!(
        scan.columns.contains(&"quantity".to_string()),
        "scan columns {:?} must include the variance argument",
        scan.columns
    );
}

/// The first aggregate expression of a single-aggregate SELECT, expected to be
/// a FunctionCall carrying an ordered-set WITHIN GROUP key.
fn only_ordered_set(sql: &str) -> (String, usize, bool) {
    let LogicalPlan::Aggregate(aggregate) = parse(sql).unwrap() else {
        panic!("expected Aggregate for {sql}");
    };
    let Expr::FunctionCall {
        function_name,
        args,
        is_aggregate,
        within_group_key,
        within_group_desc,
        ..
    } = &aggregate.aggregates[0]
    else {
        panic!("expected FunctionCall, got {:?}", aggregate.aggregates[0]);
    };
    assert!(is_aggregate, "{sql} must be an aggregate");
    assert!(
        within_group_key.is_some(),
        "{sql} must carry a WITHIN GROUP key"
    );
    (function_name.clone(), args.len(), *within_group_desc)
}

#[test]
fn percentile_within_group_is_ordered_set_aggregate() {
    // PERCENTILE_CONT/DISC carry their fraction as an argument and the ORDER BY
    // column as the WITHIN GROUP key; DESC is recorded.
    let (cont_name, cont_args, cont_desc) =
        only_ordered_set("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY price) FROM t");
    assert_eq!(cont_name, "PERCENTILE_CONT");
    assert_eq!(cont_args, 1);
    assert!(!cont_desc);
    let (disc_name, _, disc_desc) =
        only_ordered_set("SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY price DESC) FROM t");
    assert_eq!(disc_name, "PERCENTILE_DISC");
    assert!(disc_desc, "DESC ordering must be recorded");
}

#[test]
fn mode_within_group_has_no_arguments() {
    // MODE() takes no arguments; the ORDER BY column is the WITHIN GROUP key.
    let (name, arg_count, desc) =
        only_ordered_set("SELECT mode() WITHIN GROUP (ORDER BY status) FROM t");
    assert_eq!(name, "MODE");
    assert_eq!(arg_count, 0);
    assert!(!desc);
}

#[test]
fn median_lowers_to_percentile_cont() {
    // MEDIAN(price) has no canonical Postgres form; it lowers to
    // PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price).
    let (name, arg_count, _) = only_ordered_set("SELECT median(price) FROM t");
    assert_eq!(name, "PERCENTILE_CONT");
    assert_eq!(arg_count, 1, "the 0.5 fraction argument");
}

#[test]
fn ordered_set_argument_column_is_over_collected() {
    // The ordering column inside WITHIN GROUP must still reach the scan.
    let LogicalPlan::Aggregate(aggregate) =
        parse("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY quantity) FROM t").unwrap()
    else {
        panic!("expected Aggregate");
    };
    let LogicalPlan::Scan(scan) = aggregate.input.as_ref() else {
        panic!("expected Scan");
    };
    assert!(
        scan.columns.contains(&"quantity".to_string()),
        "scan columns {:?} must include the ordering column",
        scan.columns
    );
}

#[test]
fn regexp_match_becomes_binary_op() {
    use fq_plan::expr::BinaryOpType;
    // `col ~ pattern` (polyglot REGEXP_LIKE) lowers to the RegexMatch binary op.
    let pred = select_pred("SELECT a FROM t WHERE region ~ '^E[UW]$'");
    let Expr::BinaryOp { op, .. } = pred else {
        panic!("expected a binary op, got {pred:?}");
    };
    assert_eq!(op, BinaryOpType::RegexMatch);
}

#[test]
fn timestamp_literal_carries_its_type() {
    use fq_common::DataType;
    // TIMESTAMP '...' is a typed literal, not a bare string.
    let pred = select_pred("SELECT a FROM t WHERE created_at > TIMESTAMP '2024-01-01 00:00:00'");
    let Expr::BinaryOp { right, .. } = pred else {
        panic!("expected a binary op, got {pred:?}");
    };
    let Expr::Literal { data_type, .. } = right.as_ref() else {
        panic!("expected a literal, got {right:?}");
    };
    assert_eq!(*data_type, DataType::Timestamp);
}

#[test]
fn date_literal_carries_its_type() {
    use fq_common::DataType;
    let pred = select_pred("SELECT a FROM t WHERE created_at >= DATE '1970-01-01'");
    let Expr::BinaryOp { right, .. } = pred else {
        panic!("expected a binary op, got {pred:?}");
    };
    let Expr::Literal { data_type, .. } = right.as_ref() else {
        panic!("expected a literal, got {right:?}");
    };
    assert_eq!(*data_type, DataType::Date);
}

/// The number of set-operation branch leaves: every plan that is not itself a
/// `SetOperation` counts as one branch of the surrounding chain.
fn set_op_leaf_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::SetOperation(set_op) => {
            set_op_leaf_count(&set_op.left) + set_op_leaf_count(&set_op.right)
        }
        _ => 1,
    }
}

/// The topmost `SetOperation` in a plan, unwrapping the outer Projection /
/// Sort / Limit / SubqueryScan wrappers a query places above it.
fn find_set_operation(plan: &LogicalPlan) -> Option<&fq_plan::logical::SetOperation> {
    if let LogicalPlan::SetOperation(set_op) = plan {
        return Some(set_op);
    }
    plan.children().into_iter().find_map(find_set_operation)
}

#[test]
fn set_operation_after_scalar_subquery_in_where_parses() {
    // `<select with a WHERE scalar subquery> UNION ALL <select>` is a valid
    // statement-level set operation, but polyglot attaches the trailing
    // UNION ALL to the WHERE subquery operand, nesting the second branch inside
    // the first branch's predicate. The converter splits it back into a proper
    // two-branch set operation.
    let plan =
        parse("SELECT a FROM t WHERE a > (SELECT m FROM u) UNION ALL SELECT b FROM v").unwrap();
    let set_op = find_set_operation(&plan).expect("a set operation");
    assert_eq!(set_op.kind, fq_plan::SetOpKind::Union);
    assert!(!set_op.distinct, "UNION ALL keeps duplicates");
    assert_eq!(set_op_leaf_count(&plan), 2);
}

#[test]
fn set_operation_after_scalar_subquery_in_having_parses() {
    // The q14 shape: three aggregating branches, each ending in a HAVING scalar
    // subquery, combined by UNION ALL. polyglot swallows every trailing
    // UNION ALL into the preceding branch's HAVING comparison; the converter
    // restores the flat three-branch set operation.
    let sql = "SELECT sum(a) FROM t GROUP BY g HAVING sum(a) > (SELECT m FROM u) \
               UNION ALL SELECT sum(b) FROM t2 GROUP BY g HAVING sum(b) > (SELECT m FROM u) \
               UNION ALL SELECT sum(c) FROM t3 GROUP BY g HAVING sum(c) > (SELECT m FROM u)";
    let plan = parse(sql).unwrap();
    assert_eq!(set_op_leaf_count(&plan), 3);
}

#[test]
fn set_operation_body_of_a_scalar_subquery_still_parses() {
    // A set operation that is genuinely the body of a parenthesized scalar
    // subquery (fully enclosed, no trailing branch) is a legitimate scalar and
    // must keep parsing - the swallow fix must not disturb it.
    let plan = parse("SELECT a FROM t WHERE a > (SELECT max(m) FROM u UNION SELECT max(n) FROM w)")
        .unwrap();
    assert!(find_set_operation(&plan).is_none(), "{plan:?}");
    let filter = find_filter(&plan);
    assert!(
        matches!(&filter.predicate, Expr::BinaryOp { right, .. } if matches!(right.as_ref(), Expr::Subquery { .. })),
        "{filter:?}"
    );
}

// --- ROLLUP / CUBE / GROUPING SETS expand into explicit grouping sets ---

/// The Aggregate under a plan root (unwrapping Sort/Limit/Projection).
fn find_aggregate(plan: &LogicalPlan) -> &fq_plan::logical::Aggregate {
    match plan {
        LogicalPlan::Aggregate(aggregate) => aggregate,
        LogicalPlan::Sort(sort) => find_aggregate(&sort.input),
        LogicalPlan::Limit(limit) => find_aggregate(&limit.input),
        LogicalPlan::Projection(projection) => find_aggregate(&projection.input),
        LogicalPlan::Filter(filter) => find_aggregate(&filter.input),
        other => panic!("expected an Aggregate in the plan, got {other:?}"),
    }
}

#[test]
fn rollup_expands_to_prefix_grouping_sets() {
    // ROLLUP(a, b) = the prefixes [a, b], [a], [] - and group_by carries the
    // distinct union so every pass that reasons about grouping columns sees them.
    let plan = parse("SELECT a, b, count(*) FROM t GROUP BY ROLLUP (a, b)").unwrap();
    let aggregate = find_aggregate(&plan);
    let sets = aggregate.grouping_sets.as_ref().expect("grouping sets");
    assert_eq!(sets.len(), 3);
    assert_eq!(sets[0].len(), 2);
    assert_eq!(sets[1].len(), 1);
    assert_eq!(sets[2].len(), 0);
    assert_eq!(aggregate.group_by.len(), 2);
}

#[test]
fn leading_keys_prepend_to_every_rollup_set() {
    // GROUP BY k, ROLLUP(a): k joins every expanded set.
    let plan = parse("SELECT k, a, count(*) FROM t GROUP BY k, ROLLUP (a)").unwrap();
    let aggregate = find_aggregate(&plan);
    let sets = aggregate.grouping_sets.as_ref().expect("grouping sets");
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].len(), 2);
    assert_eq!(sets[1].len(), 1);
}

#[test]
fn cube_expands_to_all_subsets() {
    let plan = parse("SELECT a, b, count(*) FROM t GROUP BY CUBE (a, b)").unwrap();
    let aggregate = find_aggregate(&plan);
    let sets = aggregate.grouping_sets.as_ref().expect("grouping sets");
    assert_eq!(sets.len(), 4);
}

#[test]
fn explicit_grouping_sets_convert_each_element() {
    let plan =
        parse("SELECT a, b, count(*) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())").unwrap();
    let aggregate = find_aggregate(&plan);
    let sets = aggregate.grouping_sets.as_ref().expect("grouping sets");
    assert_eq!(sets.len(), 3);
    assert_eq!(sets[0].len(), 2);
    assert_eq!(sets[1].len(), 1);
    assert_eq!(sets[2].len(), 0);
}

#[test]
fn plain_group_by_keeps_no_grouping_sets() {
    let plan = parse("SELECT a, count(*) FROM t GROUP BY a").unwrap();
    let aggregate = find_aggregate(&plan);
    assert!(aggregate.grouping_sets.is_none());
}

/// Every column reference in an expression tree.
fn collect_expr_column_refs(expr: &Expr, refs: &mut Vec<fq_plan::ColumnRef>) {
    if let Expr::Column(column) = expr {
        refs.push(column.clone());
    }
    for child in expr.children() {
        collect_expr_column_refs(child, refs);
    }
}

/// The single Filter node in a plan tree (panics if absent or ambiguous).
fn find_filter(plan: &LogicalPlan) -> &fq_plan::Filter {
    let mut found = None;
    collect_filter(plan, &mut found);
    found.expect("expected a Filter node")
}

/// Descend a plan tree looking for its (unique) Filter node.
fn collect_filter<'a>(plan: &'a LogicalPlan, found: &mut Option<&'a fq_plan::Filter>) {
    if let LogicalPlan::Filter(node) = plan {
        assert!(found.is_none(), "more than one Filter in the plan");
        *found = Some(node);
    }
    for child in plan.children() {
        collect_filter(child, found);
    }
}

#[test]
fn left_join_lateral_becomes_left_lateral_join() {
    // A LEFT JOIN LATERAL over a correlated derived table is a dependent join,
    // not a plain derived table; the correlated outer reference stays intact.
    let plan = parse(
        "SELECT o.order_id, t.name FROM orders o \
         LEFT JOIN LATERAL (SELECT p.name FROM products p WHERE p.id = o.product_id) t ON true",
    )
    .unwrap();
    let lateral = find_lateral_join(&plan);
    assert_eq!(lateral.join_type, fq_plan::JoinType::Left);
    let LogicalPlan::SubqueryScan(right) = lateral.right.as_ref() else {
        panic!(
            "expected the lateral right to be a derived table, got {:?}",
            lateral.right
        );
    };
    assert_eq!(right.alias, "t");
    // The subquery's WHERE keeps its correlated reference to the outer relation.
    let filter = find_filter(&lateral.right);
    let mut refs = Vec::new();
    collect_expr_column_refs(&filter.predicate, &mut refs);
    assert!(
        refs.iter()
            .any(|column| column.table.as_deref() == Some("o") && column.column == "product_id"),
        "correlated ref o.product_id lost: {refs:?}",
    );
}

#[test]
fn comma_lateral_becomes_inner_lateral_join() {
    // A comma-separated LATERAL is a dependent join with INNER semantics (an
    // empty right drops the left row), never a plain implicit CROSS join.
    let plan = parse(
        "SELECT o.order_id, t.name FROM orders o, \
         LATERAL (SELECT p.name FROM products p WHERE p.id = o.product_id) t",
    )
    .unwrap();
    let lateral = find_lateral_join(&plan);
    assert_eq!(lateral.join_type, fq_plan::JoinType::Inner);
    let LogicalPlan::SubqueryScan(right) = lateral.right.as_ref() else {
        panic!(
            "expected the lateral right to be a derived table, got {:?}",
            lateral.right
        );
    };
    assert_eq!(right.alias, "t");
}

#[test]
fn lateral_join_with_non_trivial_on_raises() {
    // A LateralJoin carries no ON predicate; an ON that is not TRUE cannot be
    // represented and must raise rather than silently drop the filter.
    let error = parse(
        "SELECT o.order_id, t.name FROM orders o \
         LEFT JOIN LATERAL (SELECT p.name FROM products p WHERE p.id = o.product_id) t \
         ON t.name = o.order_id",
    )
    .unwrap_err();
    assert!(matches!(error, ParseError::Unsupported(_)), "got {error:?}");
}

/// The single LateralJoin node in a plan tree (panics if absent or ambiguous).
fn find_lateral_join(plan: &LogicalPlan) -> &fq_plan::LateralJoin {
    let mut found = None;
    collect_lateral_join(plan, &mut found);
    found.expect("expected a LateralJoin node")
}

/// Descend a plan tree looking for its (unique) LateralJoin node.
fn collect_lateral_join<'a>(plan: &'a LogicalPlan, found: &mut Option<&'a fq_plan::LateralJoin>) {
    if let LogicalPlan::LateralJoin(node) = plan {
        assert!(found.is_none(), "more than one LateralJoin in the plan");
        *found = Some(node);
    }
    for child in plan.children() {
        collect_lateral_join(child, found);
    }
}

/// A catalog holding a single `duckdb_primary.main.orders` table shaped like the
/// e2e pivot fixture, for the PIVOT rewrite tests.
fn orders_catalog() -> fq_catalog::Catalog {
    use fq_catalog::{Catalog, Column, Schema, Table};
    use fq_common::DataType;

    let orders = Table::new(
        "orders",
        vec![
            Column::new("order_id", DataType::Integer, false),
            Column::new("customer_id", DataType::Integer, true),
            Column::new("quantity", DataType::Integer, true),
            Column::new("price", DataType::Double, true),
            Column::new("status", DataType::Varchar, true),
            Column::new("region", DataType::Varchar, true),
        ],
    );
    let mut catalog = Catalog::new();
    catalog.insert_schema(
        "duckdb_primary",
        "main",
        Schema::with_tables("main", "duckdb_primary", vec![orders]),
    );
    catalog
}

#[test]
fn recursive_with_marks_cte_and_registers_self_reference() {
    // WITH RECURSIVE builds a `Cte { recursive: true }` whose body is the UNION,
    // and the recursive branch's `FROM counter` resolves to a CteRef (the name is
    // in scope before its own body is converted).
    let sql = "WITH RECURSIVE counter(n) AS (\
                 SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 5\
               ) SELECT n FROM counter";
    let plan = parse(sql).unwrap();
    let LogicalPlan::Cte(cte) = &plan else {
        panic!("expected Cte, got {plan:?}");
    };
    assert!(cte.recursive, "the CTE is recursive");
    assert_eq!(cte.name, "counter");
    assert_eq!(cte.column_names, Some(vec!["n".to_string()]));
    // The body is a UNION ALL (SetOperation), and its recursive branch names the
    // CTE as a CteRef rather than a base-table Scan.
    let LogicalPlan::SetOperation(body) = cte.cte_plan.as_ref() else {
        panic!("expected a UNION body, got {:?}", cte.cte_plan);
    };
    let mut cte_refs = Vec::new();
    collect_cte_refs(body.right.as_ref(), &mut cte_refs);
    assert_eq!(cte_refs, vec!["counter".to_string()]);
}

#[test]
fn recursive_with_without_column_list_raises() {
    // A recursive CTE has no body schema to infer from, so an omitted column list
    // fails loudly rather than guessing.
    let sql = "WITH RECURSIVE r AS (\
                 SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5\
               ) SELECT n FROM r";
    let error = parse(sql).unwrap_err();
    assert!(matches!(error, ParseError::Unsupported(_)), "got {error:?}");
}

/// Collect the names of every CteRef in a plan subtree.
fn collect_cte_refs(plan: &LogicalPlan, names: &mut Vec<String>) {
    if let LogicalPlan::CteRef(node) = plan {
        names.push(node.name.clone());
    }
    for child in plan.children() {
        collect_cte_refs(child, names);
    }
}

#[test]
fn pivot_rewrites_to_conditional_aggregation() {
    use fq_parse::parse_with_catalog;

    // SELECT * over a static PIVOT expands over the pivoted output: the group
    // columns (every source column but the pivot column `region` and the aggregate
    // argument `price`) plus one conditional-aggregate column per IN value.
    let sql = "SELECT * FROM duckdb_primary.main.orders \
               PIVOT (SUM(price) FOR region IN ('NA', 'EU', 'APAC'))";
    let plan = parse_with_catalog(sql, &orders_catalog()).unwrap();

    let aggregate = find_any_aggregate(&plan).expect("an Aggregate");
    // The pivot column and the aggregate argument are dropped from the GROUP BY.
    let group_names = column_ref_names(&aggregate.group_by);
    assert!(!group_names.contains(&"region".to_string()));
    assert!(!group_names.contains(&"price".to_string()));
    assert!(group_names.contains(&"order_id".to_string()));

    // The three IN values become the three trailing output columns.
    let outputs = &aggregate.output_names;
    assert_eq!(
        &outputs[outputs.len() - 3..],
        &["NA".to_string(), "EU".to_string(), "APAC".to_string()]
    );
    // Each value column is an aggregate over a single-branch CASE.
    let value_aggregate = &aggregate.aggregates[aggregate.aggregates.len() - 1];
    let Expr::FunctionCall {
        function_name,
        args,
        ..
    } = value_aggregate
    else {
        panic!("expected an aggregate call, got {value_aggregate:?}");
    };
    assert_eq!(function_name, "SUM");
    assert!(matches!(args.as_slice(), [Expr::Case { .. }]));
}

#[test]
fn pivot_with_aggregate_alias_suffixes_output_names() {
    use fq_parse::parse_with_catalog;

    // `SUM(price) AS total` suffixes each value column name with `_total`
    // (DuckDB's naming), so the outputs are `processing_total` and `shipped_total`.
    let sql = "SELECT * FROM duckdb_primary.main.orders \
               PIVOT (SUM(price) AS total FOR status IN ('processing', 'shipped'))";
    let plan = parse_with_catalog(sql, &orders_catalog()).unwrap();
    let aggregate = find_any_aggregate(&plan).expect("an Aggregate");
    let outputs = &aggregate.output_names;
    assert_eq!(
        &outputs[outputs.len() - 2..],
        &["processing_total".to_string(), "shipped_total".to_string()]
    );
}

#[test]
fn unsupported_pivot_shapes_raise() {
    use fq_parse::parse_with_catalog;

    // COUNT(*), multiple aggregates, and UNPIVOT all fail loudly rather than
    // silently dropping the pivot.
    let catalog = orders_catalog();
    let cases = [
        "SELECT * FROM duckdb_primary.main.orders \
         PIVOT (COUNT(*) FOR region IN ('NA', 'EU'))",
        "SELECT * FROM duckdb_primary.main.orders \
         PIVOT (SUM(price), COUNT(*) FOR region IN ('NA', 'EU'))",
        "SELECT * FROM duckdb_primary.main.orders \
         UNPIVOT (val FOR col IN (price, quantity))",
    ];
    for sql in cases {
        let error = parse_with_catalog(sql, &catalog).unwrap_err();
        assert!(
            matches!(error, ParseError::Unsupported(_)),
            "{sql} -> {error:?}"
        );
    }
}

/// The first Aggregate found in depth-first order, if any.
fn find_any_aggregate(plan: &LogicalPlan) -> Option<&fq_plan::Aggregate> {
    if let LogicalPlan::Aggregate(node) = plan {
        return Some(node);
    }
    plan.children().into_iter().find_map(find_any_aggregate)
}

/// The column names of a list of expressions that are all bare column references.
fn column_ref_names(exprs: &[Expr]) -> Vec<String> {
    let mut names = Vec::new();
    for expr in exprs {
        if let Expr::Column(column) = expr {
            names.push(column.column.clone());
        }
    }
    names
}
