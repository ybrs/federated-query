//! The selectivity pricing tree. Ports the `_tracked_*_selectivity` family of
//! `optimizer/cost.py`. Clean-Rust-portable (formulas verified numerically).
//!
//! Selectivity from measured statistics, or UNKNOWN (`None`) with the gap
//! recorded - never a fabricated prior. Callers price an unknown at its
//! no-reduction bound (a selectivity's ceiling is 1.0).

use fq_catalog::datasource::TableStatistics;
use fq_plan::expr::{
    combine_and, split_conjuncts, BinaryOpType, ColumnRef, Expr, LiteralValue, UnaryOpType,
};

use crate::cost::ordinal::{
    column_stats_or_none, interpolate_fraction, interval_terms, ordinal, ordinal_of_literal,
    range_parts,
};
use crate::subplan_signature::python_class_name;

/// Tracked selectivity of a predicate: `(Some(fraction), gaps)` when priced,
/// `(None, gaps)` when UNKNOWN. Dispatch by node type.
pub fn tracked_selectivity(
    predicate: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    match predicate {
        Expr::BinaryOp { op, left, right } => tracked_binary(*op, left, right, stats, target),
        Expr::UnaryOp { op, operand } => tracked_unary(*op, operand, stats, target),
        Expr::Between {
            value,
            lower,
            upper,
        } => tracked_between(value, lower, upper, stats, target),
        Expr::InList { value, options } => tracked_in_list(value, options, stats, target),
        // Pattern selectivity is not derivable from catalog statistics.
        Expr::Like { .. } => (None, vec![format!("like_selectivity({target})")]),
        other => (
            None,
            vec![format!(
                "selectivity({target}:{})",
                python_class_name(other)
            )],
        ),
    }
}

/// Dispatch a binary predicate to its tracked selectivity rule.
fn tracked_binary(
    op: BinaryOpType,
    left: &Expr,
    right: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    match op {
        BinaryOpType::And => {
            // The caller destructured the AND into op + two borrowed &Expr operands;
            // clone each into an owned box (we cannot move out of the borrows) so the
            // node can be rebuilt below and priced as a conjunction.
            let (left_box, right_box) = (Box::new(left.clone()), Box::new(right.clone()));
            // Rebuild the AND as an owned inline-variant Expr; `op` plus the two boxed
            // operands are the complete BinaryOp field set (a fresh node, no base).
            let combined = Expr::BinaryOp {
                op,
                left: left_box,
                right: right_box,
            };
            let (fraction, defaults) = tracked_conjunction(&combined, stats, target);
            (Some(fraction), defaults)
        }
        BinaryOpType::Or => tracked_or(left, right, stats, target),
        BinaryOpType::Eq | BinaryOpType::Neq => tracked_equality(op, left, right, stats, target),
        BinaryOpType::Lt | BinaryOpType::Lte | BinaryOpType::Gt | BinaryOpType::Gte => {
            match range_fraction(op, left, right, stats) {
                Some(fraction) => (Some(fraction), Vec::new()),
                None => (None, vec![format!("range_selectivity({target})")]),
            }
        }
        other => (
            None,
            vec![format!("selectivity({target}:{})", other.value())],
        ),
    }
}

/// An AND tree: both-bounded range pairs on one column combine into a single
/// interval fraction FIRST; every other conjunct multiplies in independently.
/// Returns a defined fraction always (unknown conjuncts are skipped, gaps ride in
/// the defaults).
pub fn tracked_conjunction(
    predicate: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (f64, Vec<String>) {
    let conjuncts = split_conjuncts(predicate);
    let (remaining, fraction) = interval_terms(&conjuncts, stats);
    let mut selectivity = fraction;
    let mut defaults = Vec::new();
    for conjunct in remaining {
        let (one, one_defaults) = tracked_selectivity(conjunct, stats, target);
        // An UNKNOWN conjunct reduces nothing (its ceiling is 1.0); the product
        // of the known conjuncts remains a valid upper bound.
        if let Some(one) = one {
            selectivity *= one;
        }
        defaults.extend(one_defaults);
    }
    (selectivity, defaults)
}

/// OR as the inclusion-exclusion complement of its two sides; either side UNKNOWN
/// makes the disjunction unknown.
fn tracked_or(
    left: &Expr,
    right: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    let (left_sel, mut defaults) = tracked_selectivity(left, stats, target);
    let (right_sel, right_defaults) = tracked_selectivity(right, stats, target);
    defaults.extend(right_defaults);
    match (left_sel, right_sel) {
        (Some(left_sel), Some(right_sel)) => {
            (Some(1.0 - (1.0 - left_sel) * (1.0 - right_sel)), defaults)
        }
        _ => (None, defaults),
    }
}

/// BETWEEN as the equivalent both-bounded conjunction, so the interval pairing
/// (not the product of two marginals) prices it. Built for estimation only.
fn tracked_between(
    value: &Expr,
    lower: &Expr,
    upper: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    // BETWEEN lowered to its lower comparison bound so the interval pairing (not a
    // product of two marginals) prices it; a fresh inline-variant Expr, no base node.
    let low = Expr::BinaryOp {
        op: BinaryOpType::Gte,
        left: Box::new(value.clone()),
        right: Box::new(lower.clone()),
    };
    // Upper comparison bound of the same lowered BETWEEN; a fresh inline-variant Expr
    // built from the borrowed operands (there is no base node to copy from).
    let high = Expr::BinaryOp {
        op: BinaryOpType::Lte,
        left: Box::new(value.clone()),
        right: Box::new(upper.clone()),
    };
    let combined = combine_and(vec![low, high]).expect("two-term conjunction");
    let (fraction, defaults) = tracked_conjunction(&combined, stats, target);
    (Some(fraction), defaults)
}

/// IN over a column with a known NDV keeps `len(options)/ndv` of the rows; an
/// unknown NDV or a non-column value is UNKNOWN.
fn tracked_in_list(
    value: &Expr,
    options: &[Expr],
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    let col_ref = as_column(value);
    let col_stats = column_stats_or_none(stats, col_ref);
    match col_stats.and_then(|stats| stats.num_distinct) {
        Some(ndv) if ndv > 0 => (
            Some((options.len() as f64 / ndv as f64).min(1.0)),
            Vec::new(),
        ),
        _ => {
            let name = col_ref.map_or_else(|| python_class_name(value).to_string(), name_of);
            (None, vec![format!("in_selectivity({target}.{name})")])
        }
    }
}

/// EQ is `1/ndv` when the column's NDV is known; NEQ its complement.
fn tracked_equality(
    op: BinaryOpType,
    left: &Expr,
    right: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    let (base, defaults) = tracked_eq_base(left, right, stats, target);
    if op == BinaryOpType::Neq {
        return (base.map(|selectivity| 1.0 - selectivity), defaults);
    }
    (base, defaults)
}

/// The equality selectivity: `1/ndv`, or UNKNOWN with the gap recorded. A
/// literal-vs-literal equality is EXACTLY computable (no statistics, no gap).
fn tracked_eq_base(
    left: &Expr,
    right: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    if let (Expr::Literal { value: left, .. }, Expr::Literal { value: right, .. }) = (left, right) {
        return (
            Some(if literal_eq(left, right) { 1.0 } else { 0.0 }),
            Vec::new(),
        );
    }
    let col_ref = extract_column_ref(left, right);
    let col_stats = column_stats_or_none(stats, col_ref);
    match col_stats.map(|stats| stats.num_distinct) {
        Some(Some(0)) => (Some(0.0), Vec::new()),
        Some(Some(ndv)) => (Some((1.0 / ndv as f64).min(1.0)), Vec::new()),
        _ => {
            // The Python fallback name is the column, else `type(binop).__name__`.
            let name = col_ref.map_or_else(|| "BinaryOp".to_string(), name_of);
            (None, vec![format!("eq_selectivity({target}.{name})")])
        }
    }
}

/// NOT complements; IS NULL / IS NOT NULL read the null fraction when known.
fn tracked_unary(
    op: UnaryOpType,
    operand: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    match op {
        UnaryOpType::Not => {
            let (inner, defaults) = tracked_selectivity(operand, stats, target);
            (inner.map(|selectivity| 1.0 - selectivity), defaults)
        }
        UnaryOpType::IsNull | UnaryOpType::IsNotNull => tracked_null(op, operand, stats, target),
        UnaryOpType::Negate => (None, vec![format!("selectivity({target}:{})", op.value())]),
    }
}

/// IS NULL from the column's null fraction; IS NOT NULL complements.
fn tracked_null(
    op: UnaryOpType,
    operand: &Expr,
    stats: Option<&TableStatistics>,
    target: &str,
) -> (Option<f64>, Vec<String>) {
    let col_ref = as_column(operand);
    let col_stats = column_stats_or_none(stats, col_ref);
    let Some(col_stats) = col_stats else {
        let name = col_ref.map_or_else(|| python_class_name(operand).to_string(), name_of);
        return (None, vec![format!("null_fraction({target}.{name})")]);
    };
    // null_fraction is a non-Option f64 in Rust: known once col_stats exists.
    let fraction = col_stats.null_fraction;
    if op == UnaryOpType::IsNotNull {
        return (Some(1.0 - fraction), Vec::new());
    }
    (Some(fraction), Vec::new())
}

/// `(literal - min) / (max - min)` oriented by the comparison direction; `None`
/// when the column, bounds, or literal share no ordinal scale.
fn range_fraction(
    op: BinaryOpType,
    left: &Expr,
    right: &Expr,
    stats: Option<&TableStatistics>,
) -> Option<f64> {
    let (col_ref, value, below) = range_parts(op, left, right);
    let col_stats = column_stats_or_none(stats, col_ref)?;
    let low = col_stats.min_value.as_ref().and_then(ordinal);
    let high = col_stats.max_value.as_ref().and_then(ordinal);
    let point = value.as_ref().and_then(ordinal_of_literal);
    interpolate_fraction(low, high, point, below)
}

/// The column side of a column-vs-value comparison, or `None`.
fn extract_column_ref<'a>(left: &'a Expr, right: &'a Expr) -> Option<&'a ColumnRef> {
    as_column(left).or_else(|| as_column(right))
}

/// The `ColumnRef` when the expression is a plain column reference.
fn as_column(expr: &Expr) -> Option<&ColumnRef> {
    match expr {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}

/// A column reference's name for provenance entries.
fn name_of(col_ref: &ColumnRef) -> String {
    col_ref.column.clone()
}

/// Literal equality with Python `value == value` semantics: numbers and booleans
/// compare across types on one numeric scale (`5 == 5.0`, `true == 1` are TRUE, as
/// in Python), strings compare exactly, and NULL equals only NULL. This is the
/// exact literal-vs-literal tag of a union/CASE fold, so it must not read `5 = 5.0`
/// as a mismatch (which would zero the arm's selectivity).
fn literal_eq(left: &LiteralValue, right: &LiteralValue) -> bool {
    match (left, right) {
        (LiteralValue::String(left), LiteralValue::String(right)) => left == right,
        (LiteralValue::Null, LiteralValue::Null) => true,
        _ => match (numeric_value(left), numeric_value(right)) {
            (Some(left), Some(right)) => left == right,
            _ => false,
        },
    }
}

/// A literal's value on the shared numeric scale (booleans as 0.0/1.0); None for
/// non-numeric literals (String, Null), which never compare numerically.
fn numeric_value(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Integer(integer) => Some(*integer as f64),
        LiteralValue::Float(float) => Some(*float),
        LiteralValue::Boolean(boolean) => Some(f64::from(u8::from(*boolean))),
        LiteralValue::String(_) | LiteralValue::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_catalog::datasource::{ColumnStatistics, StatValue};
    use fq_common::DataType;
    use std::collections::BTreeMap;

    fn col(name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some("t".into()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn int(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    fn stats(entries: Vec<(&str, ColumnStatistics)>) -> TableStatistics {
        let mut column_stats = BTreeMap::new();
        for (name, cs) in entries {
            column_stats.insert(name.to_string(), cs);
        }
        TableStatistics {
            row_count: Some(1000),
            total_size_bytes: 0,
            column_stats,
        }
    }

    fn cs(num_distinct: Option<i64>, null_fraction: f64) -> ColumnStatistics {
        ColumnStatistics {
            num_distinct,
            null_fraction,
            avg_width: 8,
            min_value: None,
            max_value: None,
        }
    }

    fn cs_range(min: i64, max: i64) -> ColumnStatistics {
        ColumnStatistics {
            num_distinct: Some(100),
            null_fraction: 0.0,
            avg_width: 8,
            min_value: Some(StatValue::Integer(min)),
            max_value: Some(StatValue::Integer(max)),
        }
    }

    fn binop(op: BinaryOpType, left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn eq_is_one_over_ndv() {
        let table = stats(vec![("a", cs(Some(4), 0.0))]);
        let predicate = binop(BinaryOpType::Eq, col("a"), int(3));
        let (sel, gaps) = tracked_selectivity(&predicate, Some(&table), "t");
        assert_eq!(sel, Some(0.25));
        assert!(gaps.is_empty());
    }

    #[test]
    fn eq_ndv_zero_is_zero() {
        let table = stats(vec![("a", cs(Some(0), 0.0))]);
        let predicate = binop(BinaryOpType::Eq, col("a"), int(3));
        assert_eq!(
            tracked_selectivity(&predicate, Some(&table), "t").0,
            Some(0.0)
        );
    }

    #[test]
    fn literal_equals_literal_is_exact_no_gap() {
        let same = binop(BinaryOpType::Eq, int(5), int(5));
        assert_eq!(tracked_selectivity(&same, None, "t"), (Some(1.0), vec![]));
        let diff = binop(BinaryOpType::Eq, int(5), int(6));
        assert_eq!(tracked_selectivity(&diff, None, "t"), (Some(0.0), vec![]));
    }

    #[test]
    fn literal_equality_compares_across_numeric_types() {
        // Python value semantics: 5 == 5.0 and true == 1 are exact matches; a
        // narrow enum-equality read would mis-tag them 0.0 and zero the arm.
        let float5 = Expr::Literal {
            value: LiteralValue::Float(5.0),
            data_type: DataType::Double,
        };
        let bool_true = Expr::Literal {
            value: LiteralValue::Boolean(true),
            data_type: DataType::Boolean,
        };
        let int_float = binop(BinaryOpType::Eq, int(5), float5);
        assert_eq!(tracked_selectivity(&int_float, None, "t").0, Some(1.0));
        let bool_int = binop(BinaryOpType::Eq, bool_true, int(1));
        assert_eq!(tracked_selectivity(&bool_int, None, "t").0, Some(1.0));
        // A string never compares numerically to a number.
        let string5 = Expr::Literal {
            value: LiteralValue::String("5".into()),
            data_type: DataType::Varchar,
        };
        let string_int = binop(BinaryOpType::Eq, string5, int(5));
        assert_eq!(tracked_selectivity(&string_int, None, "t").0, Some(0.0));
    }

    #[test]
    fn eq_unknown_records_gap() {
        let predicate = binop(BinaryOpType::Eq, col("a"), int(3));
        let (sel, gaps) = tracked_selectivity(&predicate, None, "t");
        assert_eq!(sel, None);
        assert_eq!(gaps, vec!["eq_selectivity(t.a)"]);
    }

    #[test]
    fn in_list_is_len_over_ndv() {
        let table = stats(vec![("a", cs(Some(10), 0.0))]);
        let predicate = Expr::InList {
            value: Box::new(col("a")),
            options: vec![int(1), int(2), int(3)],
        };
        assert_eq!(
            tracked_selectivity(&predicate, Some(&table), "t").0,
            Some(0.3)
        );
    }

    #[test]
    fn range_interpolates_on_ints() {
        let table = stats(vec![("a", cs_range(0, 100))]);
        // a < 25 keeps 25% (below).
        let predicate = binop(BinaryOpType::Lt, col("a"), int(25));
        assert_eq!(
            tracked_selectivity(&predicate, Some(&table), "t").0,
            Some(0.25)
        );
        // a > 25 keeps 75%.
        let predicate = binop(BinaryOpType::Gt, col("a"), int(25));
        assert_eq!(
            tracked_selectivity(&predicate, Some(&table), "t").0,
            Some(0.75)
        );
    }

    #[test]
    fn interval_pair_beats_product_of_marginals() {
        // o_orderdate in a 3-month window of a full year: pairing prices tight.
        let mut cs = cs_range(0, 0);
        cs.min_value = Some(StatValue::Text("1992-01-01".into()));
        cs.max_value = Some(StatValue::Text("1998-12-31".into()));
        let table = stats(vec![("d", cs)]);
        let d = |name| Expr::Column(ColumnRef::new(Some("t".into()), name, None));
        let date = |value: &str| Expr::Literal {
            value: LiteralValue::String(value.to_string()),
            data_type: DataType::Date,
        };
        let ge = binop(BinaryOpType::Gte, d("d"), date("1994-01-01"));
        let lt = binop(BinaryOpType::Lt, d("d"), date("1994-04-01"));
        let combined = combine_and(vec![ge, lt]).unwrap();
        let (paired, _) = tracked_conjunction(&combined, Some(&table), "t");
        // Product of the two one-sided marginals (as a contrast) is much larger.
        let (below, _) = tracked_selectivity(
            &binop(BinaryOpType::Lt, d("d"), date("1994-04-01")),
            Some(&table),
            "t",
        );
        let (above, _) = tracked_selectivity(
            &binop(BinaryOpType::Gte, d("d"), date("1994-01-01")),
            Some(&table),
            "t",
        );
        let product = below.unwrap() * above.unwrap();
        assert!(
            paired < product,
            "paired {paired} should beat product {product}"
        );
        assert!(
            paired < 0.05,
            "a 3-month window is a few percent, got {paired}"
        );
    }

    #[test]
    fn not_complements_and_or_includes() {
        let table = stats(vec![("a", cs(Some(4), 0.0))]);
        let eq = binop(BinaryOpType::Eq, col("a"), int(3));
        let not = Expr::UnaryOp {
            op: UnaryOpType::Not,
            operand: Box::new(eq.clone()),
        };
        assert_eq!(tracked_selectivity(&not, Some(&table), "t").0, Some(0.75));

        // OR of two 0.25 predicates: 1 - 0.75*0.75 = 0.4375.
        let or = binop(BinaryOpType::Or, eq.clone(), eq);
        assert_eq!(tracked_selectivity(&or, Some(&table), "t").0, Some(0.4375));
    }

    #[test]
    fn is_null_reads_null_fraction() {
        let table = stats(vec![("a", cs(Some(4), 0.2))]);
        let is_null = Expr::UnaryOp {
            op: UnaryOpType::IsNull,
            operand: Box::new(col("a")),
        };
        assert_eq!(
            tracked_selectivity(&is_null, Some(&table), "t").0,
            Some(0.2)
        );
        let is_not_null = Expr::UnaryOp {
            op: UnaryOpType::IsNotNull,
            operand: Box::new(col("a")),
        };
        assert_eq!(
            tracked_selectivity(&is_not_null, Some(&table), "t").0,
            Some(0.8)
        );
    }

    #[test]
    fn like_records_gap() {
        let like = Expr::Like {
            case_insensitive: false,
            expr: Box::new(col("a")),
            pattern: Box::new(Expr::Literal {
                value: LiteralValue::String("%x%".into()),
                data_type: DataType::Varchar,
            }),
            escape: None,
        };
        let (sel, gaps) = tracked_selectivity(&like, None, "t");
        assert_eq!(sel, None);
        assert_eq!(gaps, vec!["like_selectivity(t)"]);
    }
}
