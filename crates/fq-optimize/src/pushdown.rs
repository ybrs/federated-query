//! The two pushdown helpers the estimation layer needs. The rest of the
//! pushdown rules land with the rules milestone.

use std::collections::HashSet;

use fq_plan::expr::{column_refs, BinaryOpType, Expr};

/// Whether a predicate is a column-to-column equality (a join key).
///
/// Shared by predicate pushdown (folds these into `Join.condition`) and the
/// join-graph extraction (these are the edges join ordering walks).
pub fn is_equi_predicate(predicate: &Expr) -> bool {
    matches!(
        predicate,
        Expr::BinaryOp { op: BinaryOpType::Eq, left, right }
            if matches!(**left, Expr::Column(_)) && matches!(**right, Expr::Column(_))
    )
}

/// Every column in an expression as its bare name (no table qualifier). Built on
/// the shared `column_refs` walker, so every expression node type is covered.
pub fn bare_names(expr: &Expr) -> HashSet<String> {
    let mut names = HashSet::new();
    for reference in column_refs(expr) {
        names.insert(reference.column.clone());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{ColumnRef, LiteralValue};

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn lit(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    #[test]
    fn equi_predicate_needs_two_columns() {
        let equi = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col("l", "a")),
            right: Box::new(col("r", "b")),
        };
        assert!(is_equi_predicate(&equi));

        // column = literal is not an equi-join predicate.
        let filter = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(col("l", "a")),
            right: Box::new(lit(3)),
        };
        assert!(!is_equi_predicate(&filter));
    }

    #[test]
    fn bare_names_drops_qualifier_and_dedupes() {
        // a = b AND a = 3 -> {a, b}
        let predicate = Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("l", "a")),
                right: Box::new(col("r", "b")),
            }),
            right: Box::new(Expr::BinaryOp {
                op: BinaryOpType::Eq,
                left: Box::new(col("l", "a")),
                right: Box::new(lit(3)),
            }),
        };
        let names = bare_names(&predicate);
        assert_eq!(names.len(), 2);
        assert!(names.contains("a"));
        assert!(names.contains("b"));
    }
}
