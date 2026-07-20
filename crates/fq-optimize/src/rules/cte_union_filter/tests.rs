//! Tests for the CTE union-filter pushdown rule. No cost model: every assertion is
//! about which consumer filters get translated onto the body's grouping columns and
//! inserted directly under the aggregate, and which shapes decline.

use super::*;

use fq_common::DataType;
use fq_plan::expr::{BinaryOpType, LiteralValue};
use fq_plan::logical::Scan;

use crate::rules::OptimizationRule;
use crate::RuleBasedOptimizer;

// ------------------------------- builders -----------------------------------

/// A qualified integer column `table.column`.
fn col(table: &str, column: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        column,
        Some(DataType::Integer),
    ))
}

/// An integer literal.
fn int(value: i64) -> Expr {
    Expr::Literal {
        value: LiteralValue::Integer(value),
        data_type: DataType::Integer,
    }
}

/// A string literal.
fn text(value: &str) -> Expr {
    Expr::Literal {
        value: LiteralValue::String(value.to_string()),
        data_type: DataType::Varchar,
    }
}

/// `left op right`.
fn binop(op: BinaryOpType, left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// `left = right`.
fn eq(left: Expr, right: Expr) -> Expr {
    binop(BinaryOpType::Eq, left, right)
}

/// A plain SUM aggregate call.
fn sum(table: &str, column: &str) -> Expr {
    Expr::FunctionCall {
        function_name: "SUM".to_string(),
        args: vec![col(table, column)],
        is_aggregate: true,
        distinct: false,
        within_group_key: None,
        within_group_desc: false,
        filter: None,
    }
}

/// A base-table scan aliased to its own name.
fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
    let mut node = Scan::new(
        "duck",
        "main",
        table,
        columns.iter().map(|c| (*c).to_string()).collect(),
    );
    node.alias = Some(table.to_string());
    LogicalPlan::Scan(Box::new(node))
}

/// The body aggregate: GROUP BY t.g, t.q with SUM(t.v) AS total, outputs g/q/total.
fn agg_body() -> LogicalPlan {
    LogicalPlan::Aggregate(Aggregate {
        input: Box::new(scan("t", &["g", "q", "v"])),
        group_by: vec![col("t", "g"), col("t", "q")],
        aggregates: vec![col("t", "g"), col("t", "q"), sum("t", "v")],
        output_names: vec!["g".to_string(), "q".to_string(), "total".to_string()],
        grouping_sets: None,
    })
}

/// A CteRef exposing the standard g/q/total output schema.
fn cte_ref(alias: Option<&str>) -> LogicalPlan {
    LogicalPlan::CteRef(CteRef {
        name: "c".to_string(),
        alias: alias.map(str::to_string),
        columns: None,
        output_names: Some(vec!["g".to_string(), "q".to_string(), "total".to_string()]),
    })
}

/// A filtered consumer `... FROM c WHERE predicate`.
fn consumer(alias: Option<&str>, predicate: Expr) -> LogicalPlan {
    LogicalPlan::Filter(Filter {
        input: Box::new(cte_ref(alias)),
        predicate,
    })
}

/// A CTE wrapper over the given body and consumer child.
fn cte(body: LogicalPlan, child: LogicalPlan, recursive: bool) -> LogicalPlan {
    LogicalPlan::Cte(Cte {
        name: "c".to_string(),
        cte_plan: Box::new(body),
        child: Box::new(child),
        recursive,
        column_names: None,
    })
}

/// The rebuilt body of a CTE result.
fn body_of(plan: &LogicalPlan) -> &LogicalPlan {
    let LogicalPlan::Cte(cte) = plan else {
        panic!("expected a Cte at the root");
    };
    &cte.cte_plan
}

/// The predicate of the Filter directly under the body aggregate, or None when the
/// aggregate input is not a Filter.
fn inserted_predicate(body: &LogicalPlan) -> Option<Expr> {
    let LogicalPlan::Aggregate(aggregate) = body else {
        return None;
    };
    match aggregate.input.as_ref() {
        LogicalPlan::Filter(filter) => Some(filter.predicate.clone()),
        _ => None,
    }
}

/// Apply the rule once.
fn apply(plan: LogicalPlan) -> LogicalPlan {
    CTEUnionFilterPushdown.apply(plan).unwrap()
}

// -------------------------------- tests -------------------------------------

#[test]
fn single_consumer_pushes_translated_filter_under_aggregate() {
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let result = apply(cte(agg_body(), child, false));
    // The consumer's `c.q = 1` translates onto the grouping column `t.q`.
    assert_eq!(
        inserted_predicate(body_of(&result)),
        Some(eq(col("t", "q"), int(1)))
    );
}

#[test]
fn two_consumers_push_or_of_translated_filters() {
    let child = LogicalPlan::Union(Union {
        inputs: vec![
            consumer(Some("c"), eq(col("c", "q"), int(1))),
            consumer(Some("c"), eq(col("c", "q"), int(2))),
        ],
        distinct: false,
    });
    let result = apply(cte(agg_body(), child, false));
    let expected = binop(
        BinaryOpType::Or,
        eq(col("t", "q"), int(1)),
        eq(col("t", "q"), int(2)),
    );
    assert_eq!(inserted_predicate(body_of(&result)), Some(expected));
}

#[test]
fn second_apply_is_a_fixpoint() {
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let once = apply(cte(agg_body(), child, false));
    // Re-applying finds the predicate already embedded and declines (idempotent).
    let twice = apply(once.clone());
    assert_eq!(twice, once);
}

#[test]
fn recursive_cte_declines() {
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let plan = cte(agg_body(), child, true);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn bare_unfiltered_consumer_declines() {
    // The CteRef's parent is a Projection, not a Filter: it needs all rows.
    let child = LogicalPlan::Projection(Projection {
        input: Box::new(cte_ref(Some("c"))),
        expressions: vec![col("c", "g")],
        aliases: vec!["g".to_string()],
        distinct: false,
        distinct_on: None,
    });
    let plan = cte(agg_body(), child, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn shadowed_name_declines() {
    // A nested CTE also named "c" shadows the name: references would be ambiguous.
    // The inner is recursive so it also declines - the whole plan stays put.
    let inner = cte(
        agg_body(),
        consumer(Some("c"), eq(col("c", "q"), int(1))),
        true,
    );
    let plan = cte(agg_body(), inner, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn distinct_on_projection_declines() {
    let body = LogicalPlan::Projection(Projection {
        input: Box::new(agg_body()),
        expressions: vec![col("t", "g")],
        aliases: vec!["g".to_string()],
        distinct: false,
        distinct_on: Some(vec![col("t", "g")]),
    });
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let plan = cte(body, child, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn grouping_set_aggregate_declines() {
    let LogicalPlan::Aggregate(mut aggregate) = agg_body() else {
        unreachable!()
    };
    aggregate.grouping_sets = Some(vec![vec![col("t", "g")], vec![]]);
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let plan = cte(LogicalPlan::Aggregate(aggregate), child, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn consumer_filtering_only_an_aggregate_output_declines() {
    // `c.total > 0` maps to the SUM output, not a grouping column, so the arm is
    // empty and the whole union declines.
    let child = consumer(
        Some("c"),
        binop(BinaryOpType::Gt, col("c", "total"), int(0)),
    );
    let plan = cte(agg_body(), child, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn union_body_pushes_into_every_branch() {
    let body = LogicalPlan::Union(Union {
        inputs: vec![agg_body(), agg_body()],
        distinct: false,
    });
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let result = apply(cte(body, child, false));
    let LogicalPlan::Union(union) = body_of(&result) else {
        panic!("expected a Union body");
    };
    for branch in &union.inputs {
        assert_eq!(inserted_predicate(branch), Some(eq(col("t", "q"), int(1))));
    }
}

#[test]
fn union_body_one_branch_without_sink_declines_all() {
    // Second branch is a bare scan (no aggregate sink): the whole union declines.
    let body = LogicalPlan::Union(Union {
        inputs: vec![agg_body(), scan("t", &["g", "q", "v"])],
        distinct: false,
    });
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let plan = cte(body, child, false);
    assert_eq!(apply(plan.clone()), plan);
}

#[test]
fn constant_tag_output_substitutes_the_literal() {
    // Body outputs a constant channel tag `'web'` as its second column.
    let body = LogicalPlan::Aggregate(Aggregate {
        input: Box::new(scan("t", &["g", "v"])),
        group_by: vec![col("t", "g")],
        aggregates: vec![col("t", "g"), text("web"), sum("t", "v")],
        output_names: vec!["g".to_string(), "chan".to_string(), "total".to_string()],
        grouping_sets: None,
    });
    let cte_ref = LogicalPlan::CteRef(CteRef {
        name: "c".to_string(),
        alias: Some("c".to_string()),
        columns: None,
        output_names: Some(vec![
            "g".to_string(),
            "chan".to_string(),
            "total".to_string(),
        ]),
    });
    let child = LogicalPlan::Filter(Filter {
        input: Box::new(cte_ref),
        predicate: eq(col("c", "chan"), text("web")),
    });
    let result = apply(cte(body, child, false));
    // `c.chan = 'web'` folds to `'web' = 'web'` (the tag is constant per branch).
    assert_eq!(
        inserted_predicate(body_of(&result)),
        Some(eq(text("web"), text("web")))
    );
}

#[test]
fn bare_unaliased_consumer_resolves_via_cte_name() {
    // WITH c AS (...) SELECT ... FROM c WHERE c.q = 1  (no alias on the reference).
    let child = consumer(None, eq(col("c", "q"), int(1)));
    let result = apply(cte(agg_body(), child, false));
    assert_eq!(
        inserted_predicate(body_of(&result)),
        Some(eq(col("t", "q"), int(1)))
    );
}

#[test]
fn projection_chain_chases_the_output_position() {
    // A renaming projection sits above the aggregate: g->gg, q->qq, total->tt. The
    // consumer filters `c.qq`, which chases through the projection to grouping t.q.
    let projection = LogicalPlan::Projection(Projection {
        input: Box::new(agg_body()),
        expressions: vec![col("t", "g"), col("t", "q"), col("t", "total")],
        aliases: vec!["gg".to_string(), "qq".to_string(), "tt".to_string()],
        distinct: false,
        distinct_on: None,
    });
    let cte_ref = LogicalPlan::CteRef(CteRef {
        name: "c".to_string(),
        alias: Some("c".to_string()),
        columns: None,
        output_names: Some(vec!["gg".to_string(), "qq".to_string(), "tt".to_string()]),
    });
    let child = LogicalPlan::Filter(Filter {
        input: Box::new(cte_ref),
        predicate: eq(col("c", "qq"), int(1)),
    });
    let result = apply(cte(projection, child, false));
    // The rebuilt body still has the projection; the filter lands under its
    // aggregate, translated onto t.q.
    let LogicalPlan::Projection(projection) = body_of(&result) else {
        panic!("expected the projection to survive on top of the body");
    };
    assert_eq!(
        inserted_predicate(&projection.input),
        Some(eq(col("t", "q"), int(1)))
    );
}

#[test]
fn driver_validates_the_rewritten_scope() {
    // The inserted filter references t, which the aggregate's input exposes, so the
    // driver's validate_scope accepts the rewrite.
    let child = consumer(Some("c"), eq(col("c", "q"), int(1)));
    let optimizer = RuleBasedOptimizer::new(vec![Box::new(CTEUnionFilterPushdown)]);
    let result = optimizer.optimize(cte(agg_body(), child, false)).unwrap();
    assert_eq!(
        inserted_predicate(body_of(&result)),
        Some(eq(col("t", "q"), int(1)))
    );
}
