//! `LogicalPlan::max_depth`: what it counts, and that it counts iteratively.
//!
//! The measure is what the bind-time depth guard decides on, so it must see
//! every chain a recursive pass will later walk - including the plans hanging
//! off subquery expressions, which the ordinary expression walk stops at.

use fq_common::DataType;
use fq_plan::expr::LiteralValue;
use fq_plan::logical::{Filter, Projection, Values};
use fq_plan::{ColumnRef, Expr, LogicalPlan};

/// A one-row VALUES standing in for a leaf relation.
fn leaf() -> LogicalPlan {
    LogicalPlan::Values(Values {
        rows: vec![vec![Expr::Literal {
            value: LiteralValue::Integer(1),
            data_type: DataType::Integer,
        }]],
        output_names: vec!["x".to_string()],
    })
}

/// `input` under a projection of its single column.
fn project(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Projection(Projection {
        input: Box::new(input),
        expressions: vec![Expr::Column(ColumnRef::new(None, "x", None))],
        aliases: vec!["x".to_string()],
        distinct: false,
        distinct_on: None,
    })
}

/// `input` filtered by `EXISTS (subquery)`.
fn filter_on_exists(input: LogicalPlan, subquery: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Filter(Filter {
        input: Box::new(input),
        predicate: Expr::Exists {
            subquery: Box::new(subquery),
            negated: false,
        },
    })
}

#[test]
fn a_single_node_is_one_level() {
    assert_eq!(leaf().max_depth(), 1);
}

#[test]
fn each_plan_node_adds_one_level() {
    assert_eq!(project(project(leaf())).max_depth(), 3);
}

#[test]
fn a_plan_inside_a_subquery_expression_continues_the_chain() {
    // The EXISTS subplan is three levels of its own hanging off the filter, so
    // it - not the one-level input - decides the depth. An expression walk that
    // stopped at the subquery boundary would report 2 here.
    let subquery = project(project(leaf()));
    let plan = filter_on_exists(leaf(), subquery);
    assert_eq!(plan.max_depth(), 4);
}

#[test]
fn expression_nesting_alone_does_not_add_levels() {
    let mut predicate = Expr::Column(ColumnRef::new(None, "x", None));
    for _ in 0..50 {
        predicate = Expr::UnaryOp {
            op: fq_plan::UnaryOpType::Not,
            operand: Box::new(predicate),
        };
    }
    let plan = LogicalPlan::Filter(Filter {
        input: Box::new(leaf()),
        predicate,
    });
    assert_eq!(plan.max_depth(), 2);
}

#[test]
fn a_plan_far_past_the_limit_still_measures() {
    // The guard's input is whatever the parser produced, which can be orders of
    // magnitude past the limit; the measure must report a number there rather
    // than die on the way to reporting one.
    let mut plan = leaf();
    for _ in 0..5_000 {
        plan = project(plan);
    }
    assert_eq!(plan.max_depth(), 5_001);
}
