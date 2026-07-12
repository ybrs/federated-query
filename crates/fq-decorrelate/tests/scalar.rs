//! End-to-end tests for the scalar-subquery decorrelation path: LEFT joins,
//! SingleRowGuard placement, COUNT -> COALESCE, correlated sort keys, the
//! WHERE-equality tighten, and the dependent-join seam.

mod common;

use common::{
    assert_col, assert_no_subquery, binary_parts, condition_mentions, decorrelate_err,
    decorrelate_sql, has_single_row_guard, walk_plan,
};
use fq_decorrelate::DecorrelationError;
use fq_plan::expr::{BinaryOpType, Expr, LiteralValue};
use fq_plan::logical::{JoinType, LogicalPlan, SingleRowGuard};

/// The top projection's expression list.
fn projection_exprs(plan: LogicalPlan) -> Vec<Expr> {
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected a top projection, got {plan:?}");
    };
    projection.expressions
}

/// The single LEFT join in the plan (there is exactly one in these tests).
fn only_left_join(plan: &LogicalPlan) -> fq_plan::logical::Join {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::Join(join) = node {
            if join.join_type == JoinType::Left {
                found = Some(join.clone());
            }
        }
    });
    found.expect("expected a LEFT join")
}

/// The single SingleRowGuard in the plan, or None.
fn single_row_guard(plan: &LogicalPlan) -> Option<SingleRowGuard> {
    let mut found = None;
    walk_plan(plan, &mut |node| {
        if let LogicalPlan::SingleRowGuard(guard) = node {
            found = Some(guard.clone());
        }
    });
    found
}

#[test]
fn uncorrelated_aggregate_scalar_is_left_join_on_true_no_guard() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT max(p.price) FROM pg.public.products p) AS mx \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    assert!(
        !has_single_row_guard(&plan),
        "aggregate scalar needs no guard"
    );
    let join = only_left_join(&plan);
    // Uncorrelated -> ON TRUE.
    assert!(matches!(
        join.condition,
        Some(Expr::Literal {
            value: LiteralValue::Boolean(true),
            ..
        })
    ));
    // The scalar is replaced by the __subq_0-qualified value column.
    let exprs = projection_exprs(plan);
    assert_col(&exprs[1], "__subq_0", "__subq_0_v0");
}

#[test]
fn correlated_aggregate_scalar_is_left_join_on_condition_no_guard() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT max(p.price) FROM pg.public.products p \
                       WHERE p.category = o.region) AS mx \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    assert!(
        !has_single_row_guard(&plan),
        "grouped-by-key needs no guard"
    );
    let join = only_left_join(&plan);
    let condition = join
        .condition
        .expect("correlated scalar carries a condition");
    // ON o.region = __subq_0.__subq_0_g0.
    let (left, right) = binary_parts(&condition, BinaryOpType::Eq);
    assert_col(left, "o", "region");
    assert_col(right, "__subq_0", "__subq_0_g0");
    let exprs = projection_exprs(plan);
    assert_col(&exprs[1], "__subq_0", "__subq_0_v0");
}

#[test]
fn count_scalar_replacement_is_coalesce_zero() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT count(*) FROM pg.public.products p \
                       WHERE p.category = o.region) AS c \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    let exprs = projection_exprs(plan);
    let Expr::FunctionCall {
        function_name,
        args,
        ..
    } = &exprs[1]
    else {
        panic!("expected COALESCE, got {:?}", exprs[1]);
    };
    assert_eq!(function_name, "COALESCE");
    assert_col(&args[0], "__subq_0", "__subq_0_v0");
    assert!(matches!(
        args[1],
        Expr::Literal {
            value: LiteralValue::Integer(0),
            ..
        }
    ));
}

#[test]
fn plain_row_scalar_with_limit_one_has_no_guard() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT p.category FROM pg.public.products p \
                       WHERE p.id = o.product_id LIMIT 1) AS c \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    assert!(
        !has_single_row_guard(&plan),
        "LIMIT 1 is provably single-row"
    );
    // The subquery relation carries a per-key GroupedLimit(limit=1).
    let mut found = false;
    walk_plan(&plan, &mut |node| {
        if let LogicalPlan::GroupedLimit(grouped) = node {
            if grouped.limit == 1 && !grouped.keys.is_empty() {
                found = true;
            }
        }
    });
    assert!(found, "expected a keyed GroupedLimit(1)");
}

#[test]
fn plain_row_uncorrelated_scalar_has_keyless_guard() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT p.category FROM pg.public.products p \
                       WHERE p.status = 'x') AS c \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    let guard = single_row_guard(&plan).expect("uncorrelated plain scalar is guarded");
    assert!(
        guard.keys.is_empty(),
        "keyless guard: at most one row total"
    );
    // LEFT join ON TRUE.
    let join = only_left_join(&plan);
    assert!(matches!(
        join.condition,
        Some(Expr::Literal {
            value: LiteralValue::Boolean(true),
            ..
        })
    ));
}

#[test]
fn plain_row_correlated_scalar_has_keyed_guard() {
    let plan = decorrelate_sql(
        "SELECT o.id, (SELECT p.category FROM pg.public.products p \
                       WHERE p.id = o.product_id) AS c \
         FROM pg.public.orders o",
    );
    assert_no_subquery(&plan);
    let guard = single_row_guard(&plan).expect("correlated plain scalar is guarded");
    assert_eq!(guard.keys.len(), 1, "one key per correlation tuple");
    // The guard key is the exposed correlation column, unqualified physical name.
    let Expr::Column(col) = &guard.keys[0] else {
        panic!("expected a column guard key");
    };
    assert_eq!(col.column, "__subq_0_k0");
    assert!(col.table.is_none(), "guard key is an inner physical name");
}

#[test]
fn uncorrelated_scalar_equality_in_where_tightens_to_inner_join() {
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE o.price = (SELECT max(p.price) FROM pg.public.products p)",
    );
    assert_no_subquery(&plan);
    // No residual filter: the equality became the INNER join condition.
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a top projection");
    };
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        panic!("expected the join directly under the projection (no residual filter)");
    };
    assert_eq!(join.join_type, JoinType::Inner);
    let condition = join.condition.as_ref().expect("tightened equi condition");
    let (left, right) = binary_parts(condition, BinaryOpType::Eq);
    assert_col(left, "o", "price");
    assert_col(right, "__subq_0", "__subq_0_v0");
}

#[test]
fn non_flattenable_scalar_unnests_to_dependent_join() {
    // A non-equi correlation across an aggregate cannot flatten; the scalar path
    // now diverts to the N-K dependent join and produces correlation-free algebra.
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         WHERE o.price = (SELECT max(p.price) FROM pg.public.products p \
                          WHERE p.price < o.price)",
    );
    assert_no_subquery(&plan);
    // The residual WHERE equality now reads the dependent relation's value column.
    let mut reads_value = false;
    walk_plan(&plan, &mut |node| {
        for expr in node.direct_expressions() {
            if condition_mentions(expr, "__subq_2", "nk_value") {
                reads_value = true;
            }
        }
    });
    assert!(reads_value, "residual predicate reads the N-K value column");
    // The dependent relation is a SubqueryScan over an aggregate exposing d0/nk_value.
    let mut found_aggregate = false;
    walk_plan(&plan, &mut |node| {
        if let LogicalPlan::Aggregate(aggregate) = node {
            if aggregate.output_names == vec!["d0".to_string(), "nk_value".to_string()] {
                found_aggregate = true;
            }
        }
    });
    assert!(
        found_aggregate,
        "dependent aggregate exposes [d0, nk_value]"
    );
}

#[test]
fn correlated_sort_key_subquery_widens_below_projection_then_prunes() {
    let plan = decorrelate_sql(
        "SELECT o.id FROM pg.public.orders o \
         ORDER BY (SELECT max(p.price) FROM pg.public.products p WHERE p.category = o.region)",
    );
    assert_no_subquery(&plan);
    // Top: the pruning projection restoring the original output (only o.id / id).
    let LogicalPlan::Projection(prune) = &plan else {
        panic!("expected a top pruning projection");
    };
    assert_eq!(prune.aliases, vec!["id".to_string()]);
    // Under it: a Sort over the widened projection that carries the helper column.
    let LogicalPlan::Sort(sort) = prune.input.as_ref() else {
        panic!("expected a Sort under the pruning projection");
    };
    let LogicalPlan::Projection(widened) = sort.input.as_ref() else {
        panic!("expected the widened projection under the Sort");
    };
    assert!(
        widened.aliases.iter().any(|a| a == "__subq_0_v0"),
        "widened projection carries the sort-key helper column, aliases: {:?}",
        widened.aliases
    );
    // The join providing the sort value is a LEFT join beneath the projection.
    assert_eq!(only_left_join(&plan).join_type, JoinType::Left);
    // The Sort key references the joined subquery value.
    assert!(condition_mentions(
        &sort.sort_keys[0],
        "__subq_0",
        "__subq_0_v0"
    ));
}

#[test]
fn multi_column_scalar_raises() {
    let err = decorrelate_err(
        "SELECT o.id, (SELECT p.id, p.price FROM pg.public.products p LIMIT 1) \
         FROM pg.public.orders o",
    );
    assert!(
        matches!(&err, DecorrelationError::Unsupported(m) if m.contains("exactly one column")),
        "got {err:?}"
    );
}
