//! Translation of `tests/test_expression_visitor.py`.
//!
//! The Python file tested a visitor extension point. With `Expr` as an enum the
//! visitor retires (see the expr.rs port note), so the three tests become:
//! - dispatch: every variant reports its own label (`variant_label`);
//! - coverage: every variant maps to a DISTINCT label, and the label set is
//!   exactly the expected set - the compile-checked stand-in for the ABC
//!   "interface covers every type" test (exhaustiveness itself is enforced by the
//!   compiler at every `match`);
//! - nested collection: `column_refs` gathers columns in order.

use std::collections::HashSet;

use fq_common::DataType;
use fq_plan::expr::LiteralValue;
use fq_plan::logical::Values;
use fq_plan::{column_refs, BinaryOpType, ColumnRef, Expr, LogicalPlan, Quantifier};

/// A minimal logical plan standing in for a subquery body (the Python
/// `_dummy_subquery`).
fn dummy_subquery() -> LogicalPlan {
    LogicalPlan::Values(Values {
        rows: vec![vec![Expr::Literal {
            value: LiteralValue::Integer(1),
            data_type: DataType::Integer,
        }]],
        output_names: vec!["x".to_string()],
    })
}

fn col() -> Expr {
    Expr::Column(ColumnRef::new(None, "a", None))
}

fn lit() -> Expr {
    Expr::Literal {
        value: LiteralValue::Integer(1),
        data_type: DataType::Integer,
    }
}

/// One instance of every expression variant, paired with its expected label.
// A flat one-of-each fixture: long by nature, clearer as one list than split.
#[allow(clippy::too_many_lines)]
fn one_of_each() -> Vec<(Expr, &'static str)> {
    let plan = dummy_subquery();
    vec![
        (col(), "column_ref"),
        (lit(), "literal"),
        (
            Expr::BinaryOp {
                op: BinaryOpType::Add,
                left: Box::new(col()),
                right: Box::new(lit()),
            },
            "binary_op",
        ),
        (
            Expr::UnaryOp {
                op: fq_plan::UnaryOpType::Not,
                operand: Box::new(lit()),
            },
            "unary_op",
        ),
        (
            Expr::FunctionCall {
                function_name: "UPPER".to_string(),
                args: vec![col()],
                is_aggregate: false,
                distinct: false,
                within_group_key: None,
                within_group_desc: false,
                filter: None,
            },
            "function_call",
        ),
        (
            Expr::Case {
                when_clauses: vec![(col(), lit())],
                else_result: Some(Box::new(lit())),
            },
            "case_expr",
        ),
        (
            Expr::InList {
                value: Box::new(col()),
                options: vec![lit()],
            },
            "in_list",
        ),
        (
            Expr::Between {
                value: Box::new(col()),
                lower: Box::new(lit()),
                upper: Box::new(lit()),
            },
            "between",
        ),
        (
            Expr::Cast {
                expr: Box::new(col()),
                target_type: "VARCHAR".to_string(),
                data_type: None,
            },
            "cast",
        ),
        (
            Expr::Extract {
                field: "YEAR".to_string(),
                source: Box::new(col()),
            },
            "extract",
        ),
        (
            Expr::Interval {
                value: "30".to_string(),
                unit: Some("DAYS".to_string()),
            },
            "interval",
        ),
        (
            Expr::Window {
                function: Box::new(Expr::FunctionCall {
                    function_name: "SUM".to_string(),
                    args: vec![col()],
                    is_aggregate: true,
                    distinct: false,
                    within_group_key: None,
                    within_group_desc: false,
                    filter: None,
                }),
                partition_by: vec![col()],
                order_keys: vec![col()],
                order_ascending: vec![true],
                order_nulls: vec![None],
                frame: None,
            },
            "window_expr",
        ),
        (
            Expr::Subquery {
                subquery: Box::new(plan.clone()),
            },
            "subquery",
        ),
        (
            Expr::Exists {
                subquery: Box::new(plan.clone()),
                negated: false,
            },
            "exists",
        ),
        (
            Expr::InSubquery {
                value: Box::new(col()),
                subquery: Box::new(plan.clone()),
                negated: false,
            },
            "in_subquery",
        ),
        (
            Expr::QuantifiedComparison {
                operator: BinaryOpType::Eq,
                quantifier: Quantifier::Any,
                left: Box::new(col()),
                subquery: Box::new(plan),
            },
            "quantified_comparison",
        ),
        (
            Expr::Tuple {
                items: vec![col(), lit()],
            },
            "tuple",
        ),
    ]
}

#[test]
fn every_variant_reports_its_own_label() {
    for (expr, expected) in one_of_each() {
        assert_eq!(expr.variant_label(), expected);
    }
}

#[test]
fn labels_are_distinct_and_cover_every_variant() {
    let labels: Vec<&str> = one_of_each().iter().map(|(_, label)| *label).collect();
    let unique: HashSet<&str> = labels.iter().copied().collect();
    // Every variant maps to a distinct label (no two share a hook).
    assert_eq!(unique.len(), labels.len());
    // The full expected label set (the ABC-coverage stand-in).
    let expected: HashSet<&str> = [
        "column_ref",
        "literal",
        "binary_op",
        "unary_op",
        "function_call",
        "case_expr",
        "in_list",
        "between",
        "cast",
        "extract",
        "interval",
        "window_expr",
        "subquery",
        "exists",
        "in_subquery",
        "quantified_comparison",
        "tuple",
    ]
    .into_iter()
    .collect();
    assert_eq!(unique, expected);
}

#[test]
fn column_refs_collects_nested_in_order() {
    // UPPER(a) BETWEEN b AND CAST(c AS INTEGER)
    let expr = Expr::Between {
        value: Box::new(Expr::FunctionCall {
            function_name: "UPPER".to_string(),
            args: vec![Expr::Column(ColumnRef::new(None, "a", None))],
            is_aggregate: false,
            distinct: false,
            within_group_key: None,
            within_group_desc: false,
            filter: None,
        }),
        lower: Box::new(Expr::Column(ColumnRef::new(None, "b", None))),
        upper: Box::new(Expr::Cast {
            expr: Box::new(Expr::Column(ColumnRef::new(None, "c", None))),
            target_type: "INTEGER".to_string(),
            data_type: None,
        }),
    };
    let names: Vec<&str> = column_refs(&expr)
        .iter()
        .map(|col| col.column.as_str())
        .collect();
    assert_eq!(names, vec!["a", "b", "c"]);
}
