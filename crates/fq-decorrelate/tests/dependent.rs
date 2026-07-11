//! End-to-end tests for the Neumann-Kemper dependent-join fallback: a correlated
//! scalar subquery whose correlation cannot flatten to a set join (a non-equi
//! predicate crossing an aggregate or a per-outer-row LIMIT) unnests into ordinary
//! relational algebra - a DISTINCT domain, an INNER dependent join, a per-domain
//! reduction (aggregate or ROW_NUMBER top-k), and a LEFT join-back.

mod common;

use common::{assert_col, assert_no_subquery, binary_parts, decorrelate_sql, walk_plan};
use fq_plan::expr::{BinaryOpType, Expr, LiteralValue};
use fq_plan::logical::{Aggregate, JoinType, LogicalPlan, Projection, SubqueryScan};

/// The top projection's expression list.
fn projection_exprs(plan: &LogicalPlan) -> Vec<Expr> {
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected a top projection, got {plan:?}");
    };
    projection.expressions.clone()
}

/// The single LEFT join in the plan (the N-K join-back).
fn only_left_join(plan: &LogicalPlan) -> fq_plan::logical::Join {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::Join(join) = node {
            if join.join_type == JoinType::Left {
                found = Some(join.clone());
            }
        }
    });
    found.expect("expected a LEFT join-back")
}

/// The single INNER join in the plan (the domain-to-inner dependent join).
fn only_inner_join(plan: &LogicalPlan) -> fq_plan::logical::Join {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::Join(join) = node {
            if join.join_type == JoinType::Inner {
                found = Some(join.clone());
            }
        }
    });
    found.expect("expected an INNER dependent join")
}

/// The single Aggregate node in the plan (the dependent relation's reducer).
fn only_aggregate(plan: &LogicalPlan) -> Aggregate {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::Aggregate(aggregate) = node {
            found = Some(aggregate.clone());
        }
    });
    found.expect("expected an aggregate")
}

/// A conjunct of a (possibly nested) top-level AND chain.
fn conjuncts(expr: &Expr) -> Vec<Expr> {
    fq_plan::expr::split_conjuncts(expr)
        .into_iter()
        .cloned()
        .collect()
}

/// The right side of a join, as a `(alias, input)` SubqueryScan.
fn subquery_scan(plan: &LogicalPlan) -> SubqueryScan {
    let LogicalPlan::SubqueryScan(scan) = plan else {
        panic!("expected a SubqueryScan, got {plan:?}");
    };
    scan.clone()
}

// ---------------------------------------------------------------------------
// Case A: aggregate once per domain value.
// ---------------------------------------------------------------------------

#[test]
fn case_a_max_over_non_equi_correlation_unnests_to_dependent_aggregate() {
    // `p.price < o.price` is a non-equi correlation across an aggregate: it cannot
    // flatten, so the scalar path diverts to the N-K dependent aggregate.
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT max(p.price) FROM pg.public.products p \
                       WHERE p.price < o.price) AS m \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);

    // Top projection reads the dependent value column __subq_2.nk_value.
    let exprs = projection_exprs(&plan);
    assert_col(&exprs[1], "__subq_2", "nk_value");

    // Join-back: LEFT, ON o.price = __subq_2.d0.
    let join_back = only_left_join(&plan);
    let condition = join_back.condition.expect("join-back condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "o", "price");
    assert_col(right, "__subq_2", "d0");

    // The join-back right side is a SubqueryScan __subq_2 over the aggregate.
    let dependent = subquery_scan(&join_back.right);
    assert_eq!(dependent.alias, "__subq_2");

    // The aggregate groups on d0 and carries [d0, nk_value] outputs (the domain
    // column is BOTH a group key AND a passthrough output).
    let aggregate = only_aggregate(&plan);
    assert_eq!(aggregate.group_by.len(), 1);
    assert_col(&aggregate.group_by[0], "__subq_1", "d0");
    assert_eq!(
        aggregate.output_names,
        vec!["d0".to_string(), "nk_value".to_string()]
    );
    // aggregates = [passthrough d0, MAX(p.price)].
    assert_col(&aggregate.aggregates[0], "__subq_1", "d0");
    let Expr::FunctionCall { function_name, .. } = &aggregate.aggregates[1] else {
        panic!("expected MAX call, got {:?}", aggregate.aggregates[1]);
    };
    assert_eq!(function_name.to_uppercase(), "MAX");

    // The dependent INNER join: domain (__subq_1) JOIN products p ON p.price < __subq_1.d0.
    let inner = only_inner_join(&plan);
    let inner_condition = inner.condition.expect("dependent join condition");
    let (ileft, iright) = binary_parts(&inner_condition, BinaryOpType::Lt);
    assert_col(ileft, "p", "price");
    assert_col(iright, "__subq_1", "d0");

    // The domain is a DISTINCT projection [o.price AS d0] aliased __subq_1.
    let domain = subquery_scan(&inner.left);
    assert_eq!(domain.alias, "__subq_1");
    let LogicalPlan::Projection(Projection {
        expressions,
        aliases,
        distinct,
        ..
    }) = domain.input.as_ref()
    else {
        panic!("expected a DISTINCT domain projection");
    };
    assert!(*distinct, "domain projection is DISTINCT");
    assert_col(&expressions[0], "o", "price");
    assert_eq!(aliases, &vec!["d0".to_string()]);
}

#[test]
fn case_a_count_reads_coalesce_zero() {
    // COUNT over an empty domain group must read 0, not the LEFT join's NULL.
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT count(*) FROM pg.public.products p \
                       WHERE p.price < o.price) AS c \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    let exprs = projection_exprs(&plan);
    let Expr::FunctionCall {
        function_name,
        args,
        ..
    } = &exprs[1]
    else {
        panic!("expected COALESCE, got {:?}", exprs[1]);
    };
    assert_eq!(function_name, "COALESCE");
    assert_col(&args[0], "__subq_2", "nk_value");
    assert!(matches!(
        args[1],
        Expr::Literal {
            value: LiteralValue::Integer(0),
            ..
        }
    ));
}

#[test]
fn case_a_two_free_vars_keep_d0_d1_alignment() {
    // Two outer free vars whose first-seen order (region, then company_id) differs
    // from their sorted order (company_id < region): a HashMap/BTreeMap dom_map
    // would reorder the domain columns and this test would fail.
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT max(p.price) FROM pg.public.products p \
                       WHERE p.category < o.region AND p.price < o.company_id) AS m \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);

    // Domain projection: [o.region AS d0, o.company_id AS d1], in first-seen order.
    let inner = only_inner_join(&plan);
    let domain = subquery_scan(&inner.left);
    let LogicalPlan::Projection(Projection {
        expressions,
        aliases,
        ..
    }) = domain.input.as_ref()
    else {
        panic!("expected the domain projection");
    };
    assert_col(&expressions[0], "o", "region");
    assert_col(&expressions[1], "o", "company_id");
    assert_eq!(aliases, &vec!["d0".to_string(), "d1".to_string()]);

    // Aggregate group keys and outputs stay aligned: d0 then d1.
    let aggregate = only_aggregate(&plan);
    assert_col(&aggregate.group_by[0], "__subq_1", "d0");
    assert_col(&aggregate.group_by[1], "__subq_1", "d1");
    assert_eq!(
        aggregate.output_names,
        vec!["d0".to_string(), "d1".to_string(), "nk_value".to_string()]
    );

    // Dependent join condition: p.category < __subq_1.d0 AND p.price < __subq_1.d1.
    let inner_condition = inner.condition.expect("dependent join condition");
    let parts = conjuncts(&inner_condition);
    assert_eq!(parts.len(), 2);
    let (l0, r0) = binary_parts(&parts[0], BinaryOpType::Lt);
    assert_col(l0, "p", "category");
    assert_col(r0, "__subq_1", "d0");
    let (l1, r1) = binary_parts(&parts[1], BinaryOpType::Lt);
    assert_col(l1, "p", "price");
    assert_col(r1, "__subq_1", "d1");

    // Join-back equalities: o.region = __subq_2.d0 AND o.company_id = __subq_2.d1.
    let join_back = only_left_join(&plan);
    let condition = join_back.condition.expect("join-back condition");
    let back = conjuncts(&condition);
    assert_eq!(back.len(), 2);
    let (bl0, br0) = binary_parts(&back[0], BinaryOpType::Eq);
    assert_col(bl0, "o", "region");
    assert_col(br0, "__subq_2", "d0");
    let (bl1, br1) = binary_parts(&back[1], BinaryOpType::Eq);
    assert_col(bl1, "o", "company_id");
    assert_col(br1, "__subq_2", "d1");
}

// ---------------------------------------------------------------------------
// Case B: top-k per outer row via ROW_NUMBER.
// ---------------------------------------------------------------------------

#[test]
fn case_b_correlated_order_by_limit_unnests_to_row_number_top_k() {
    // A non-equi correlation crossing a per-outer-row LIMIT cannot flatten; the
    // scalar path builds the ROW_NUMBER top-k dependent relation.
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT p.price FROM pg.public.products p \
                       WHERE p.price < o.price ORDER BY p.price DESC LIMIT 1) AS m \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);

    // Top projection reads the dependent value column.
    let exprs = projection_exprs(&plan);
    assert_col(&exprs[1], "__subq_2", "nk_value");

    // A ROW_NUMBER window partitioned by the domain column d0 exists.
    let mut window = None;
    walk_plan(&plan, &mut |node| {
        for expr in node.direct_expressions() {
            collect_window(expr, &mut window);
        }
    });
    let Expr::Window {
        function,
        partition_by,
        ..
    } = window.expect("expected a ROW_NUMBER window")
    else {
        unreachable!()
    };
    let Expr::FunctionCall { function_name, .. } = function.as_ref() else {
        panic!("expected the window function call");
    };
    assert_eq!(function_name.to_uppercase(), "ROW_NUMBER");
    assert_eq!(partition_by.len(), 1);
    assert_col(&partition_by[0], "__subq_1", "d0");

    // The cap filter keeps rank <= 1.
    let mut cap = None;
    walk_plan(&plan, &mut |node| {
        if let LogicalPlan::Filter(filter) = node {
            cap = Some(filter.predicate.clone());
        }
    });
    let cap = cap.expect("expected a rank cap filter");
    let (rank, bound) = binary_parts(&cap, BinaryOpType::Lte);
    assert_col(rank, "__subq_3", "nk_rank");
    assert!(matches!(
        bound,
        Expr::Literal {
            value: LiteralValue::Integer(1),
            ..
        }
    ));

    // The join-back's dependent relation (__subq_2) drops the rank: it exposes
    // only [d0, nk_value].
    let join_back = only_left_join(&plan);
    let dependent = subquery_scan(&join_back.right);
    assert_eq!(dependent.alias, "__subq_2");
    let LogicalPlan::Projection(projection) = dependent.input.as_ref() else {
        panic!("expected the domain-values projection");
    };
    assert_eq!(
        projection.aliases,
        vec!["d0".to_string(), "nk_value".to_string()],
        "rank column is dropped before the join-back"
    );
}

/// Capture the first Window expression found anywhere in an expression tree.
fn collect_window(expr: &Expr, out: &mut Option<Expr>) {
    if matches!(expr, Expr::Window { .. }) {
        *out = Some(expr.clone());
        return;
    }
    for child in expr.children() {
        collect_window(child, out);
    }
}

// ---------------------------------------------------------------------------
// Guards.
// ---------------------------------------------------------------------------

#[test]
fn and_join_empty_list_is_an_error() {
    // The correctness guard: an empty conjunct list never silently yields no
    // predicate - it raises (used across the dependent-join condition builders).
    assert!(fq_decorrelate::helpers::and_join(vec![]).is_err());
}
