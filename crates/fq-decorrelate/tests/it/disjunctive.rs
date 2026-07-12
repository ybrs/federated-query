//! End-to-end tests for the disjunctive decorrelation path: the domain-union SEMI
//! collapse, the boolean-flag Union fallback, and the OR-inside-a-conjunct domain
//! SEMI. Also pins the flag-branch passthrough columns as QUALIFIED (no star).

use crate::common::{
    assert_col, assert_no_subquery, binary_parts, decorrelate_sql, has_single_row_guard, walk_plan,
};
use fq_plan::expr::{BinaryOpType, Expr, LiteralValue};
use fq_plan::logical::{JoinType, LogicalPlan};

/// The single SEMI join in the plan.
fn only_semi_join(plan: &LogicalPlan) -> fq_plan::logical::Join {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::Join(join) = node {
            if join.join_type == JoinType::Semi {
                found = Some(join.clone());
            }
        }
    });
    found.expect("expected a SEMI join")
}

#[test]
fn or_of_same_key_existentials_is_one_domain_semi() {
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) \
            OR EXISTS (SELECT 1 FROM pg.public.products q WHERE q.id = o.product_id)",
    );
    assert_no_subquery(&plan);
    let join = only_semi_join(&plan);
    // The right side is a SubqueryScan over a UNION ALL of the two key domains.
    let LogicalPlan::SubqueryScan(scan) = join.right.as_ref() else {
        panic!("expected a SubqueryScan domain, got {:?}", join.right);
    };
    let LogicalPlan::Union(union) = scan.input.as_ref() else {
        panic!("expected a UNION ALL under the domain");
    };
    assert!(!union.distinct, "domain union is UNION ALL");
    assert_eq!(union.inputs.len(), 2, "one branch per disjunct");
    // Every branch projects its key to the one canonical domain name.
    let key_name = format!("{}_k0", scan.alias);
    for branch in &union.inputs {
        let LogicalPlan::Projection(projection) = branch else {
            panic!("expected a per-branch projection");
        };
        assert_eq!(projection.aliases, vec![key_name.clone()]);
    }
    // Condition: o.product_id = <domain>.k0.
    let condition = join.condition.expect("domain SEMI carries a condition");
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "o", "product_id");
    assert_col(right, &scan.alias, &key_name);
}

#[test]
fn or_with_a_plain_predicate_takes_the_flag_path() {
    // A plain disjunct (o.price > 100) blocks the domain collapse, so each subquery
    // disjunct becomes a boolean flag column OR'd in a single filter.
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) \
            OR o.price > 100",
    );
    assert_no_subquery(&plan);
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a top projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected a filter over the flag union");
    };
    // Predicate is an OR of the flag column and the plain predicate.
    let (flag_term, plain_term) = binary_parts(&filter.predicate, BinaryOpType::Or);
    // The flag term is an unqualified reference to the fresh flag column.
    let Expr::Column(flag) = flag_term else {
        panic!("expected a flag column, got {flag_term:?}");
    };
    assert!(
        flag.column.ends_with("_flag"),
        "flag column: {}",
        flag.column
    );
    let (left, _) = binary_parts(plain_term, BinaryOpType::Gt);
    assert_col(left, "o", "price");
    // Under the filter: a non-distinct union of the SEMI/ANTI flag branches.
    assert!(matches!(
        filter.input.as_ref(),
        LogicalPlan::Union(u) if !u.distinct && u.inputs.len() == 2
    ));
}

#[test]
fn projection_exists_becomes_flag_union_with_qualified_passthrough() {
    let plan = decorrelate_sql(
        "SELECT o.id, EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) AS has_p \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    assert!(!has_single_row_guard(&plan), "boolean flag needs no guard");
    let LogicalPlan::Projection(top) = &plan else {
        panic!("expected a top projection");
    };
    assert_eq!(top.aliases, vec!["id".to_string(), "has_p".to_string()]);
    // The has_p slot reads the flag column (unqualified fresh output name).
    let Expr::Column(flag) = &top.expressions[1] else {
        panic!("expected the flag column, got {:?}", top.expressions[1]);
    };
    assert!(flag.column.ends_with("_flag"));
    // The flag union's branches carry the passthrough as QUALIFIED columns, no star.
    let LogicalPlan::Union(union) = top.input.as_ref() else {
        panic!("expected a flag union under the projection");
    };
    for branch in &union.inputs {
        let LogicalPlan::Projection(projection) = branch else {
            panic!("expected a branch projection");
        };
        assert!(
            projection.aliases.iter().all(|a| a != "*"),
            "no star alias in a flag branch"
        );
        // The final expression is the literal flag; the rest are qualified columns.
        let (flag_expr, passthrough) = projection.expressions.split_last().unwrap();
        assert!(matches!(
            flag_expr,
            Expr::Literal {
                value: LiteralValue::Boolean(_),
                ..
            }
        ));
        for expr in passthrough {
            assert_col(expr, "o", &expr_column_name(expr));
        }
    }
}

/// The column name of a plain column-ref expression (helper for the assertion).
fn expr_column_name(expr: &Expr) -> String {
    let Expr::Column(col) = expr else {
        panic!("expected a column, got {expr:?}");
    };
    col.column.clone()
}

#[test]
fn or_of_existentials_inside_a_conjunct_is_domain_semi() {
    // `... AND (EXISTS(p) OR EXISTS(q))`: the OR conjunct collapses to a domain
    // SEMI join, and the plain conjunct stays as a residual filter.
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE o.region = 'east' \
           AND (EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) \
                OR EXISTS (SELECT 1 FROM pg.public.products q WHERE q.id = o.product_id))",
    );
    assert_no_subquery(&plan);
    // The residual filter keeps only the plain conjunct.
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a top projection");
    };
    let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
        panic!("expected a residual filter over the domain SEMI");
    };
    let (left, _) = binary_parts(&filter.predicate, BinaryOpType::Eq);
    assert_col(left, "o", "region");
    // Under it: the domain SEMI join over a two-branch UNION.
    let join = only_semi_join(&plan);
    let LogicalPlan::SubqueryScan(scan) = join.right.as_ref() else {
        panic!("expected a domain SubqueryScan");
    };
    assert!(matches!(
        scan.input.as_ref(),
        LogicalPlan::Union(u) if !u.distinct && u.inputs.len() == 2
    ));
}
