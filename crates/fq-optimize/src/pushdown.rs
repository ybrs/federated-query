//! Shared predicate/column helpers for the pushdown optimization rules. Ports
//! `optimizer/pushdown.py`.
//!
//! Predicate, projection, semi-join and order-by pushdown all ask the same two
//! questions - "what columns does this expression reference?" and "does a column
//! set belong to one side of a join?" - so the answers live here once instead of
//! being re-implemented per rule. A node type with no rule in `available_columns`
//! returns the EMPTY set (conservative: the caller then declines to push, keeping
//! correctness over the optimization).

use std::collections::HashSet;

use fq_plan::expr::{column_refs, BinaryOpType, ColumnRef, Expr};
use fq_plan::logical::{LogicalPlan, Projection, Scan};

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

/// A column reference as `table.column`, or bare `column` if unqualified. Ports
/// `_ref_key`. An empty qualifier is treated as absent (matches Python truthiness).
fn ref_key(reference: &ColumnRef) -> String {
    match &reference.table {
        Some(table) if !table.is_empty() => format!("{table}.{}", reference.column),
        _ => reference.column.clone(),
    }
}

/// Every column in an expression as `table.col` (or bare `col`). The qualifier
/// decides which join side a filter belongs to when both inputs expose a column
/// of the same name. Ports `qualified_or_bare_names`.
pub fn qualified_or_bare_names(expr: &Expr) -> HashSet<String> {
    let mut names = HashSet::new();
    for reference in column_refs(expr) {
        names.insert(ref_key(reference));
    }
    names
}

/// One authoritative name per PLAIN column key (`table.col` or bare `col`).
/// Non-column items (e.g. `UPPER(x)`) are ignored, matching GROUP BY / ORDER BY
/// where only plain column keys participate in side matching. Ports
/// `column_key_set`.
pub fn column_key_set(items: &[Expr]) -> HashSet<String> {
    let mut names = HashSet::new();
    for item in items {
        if let Expr::Column(reference) = item {
            names.insert(ref_key(reference));
        }
    }
    names
}

/// Whether every column in `cols` belongs uniquely to one join side. Ports
/// `columns_belong_to_side`.
// Every column set here is built by `qualified_or_bare_names` / `available_columns`
// (default hasher), so generalizing over the hasher would only add noise.
#[allow(clippy::implicit_hasher)]
pub fn columns_belong_to_side(
    cols: &HashSet<String>,
    side_cols: &HashSet<String>,
    other_cols: &HashSet<String>,
) -> bool {
    cols.iter()
        .all(|col| belongs_to_side(col, side_cols, other_cols))
}

/// Whether one column reference belongs uniquely to `side_cols`. It must be
/// exposed by this side (matched by bare name, tolerating alias-vs-physical-name
/// differences) AND not by the other side - unless it is explicitly qualified to
/// this side, which is unambiguous even when both sides share the bare name (a
/// self-join). Ports `_belongs_to_side`.
fn belongs_to_side(col: &str, side_cols: &HashSet<String>, other_cols: &HashSet<String>) -> bool {
    let bare = col.rsplit('.').next().unwrap_or(col);
    if !side_has(side_cols, bare) {
        return false;
    }
    if !side_has(other_cols, bare) {
        return true;
    }
    side_cols.contains(col) && !other_cols.contains(col)
}

/// Whether a column set contains the bare name, qualified or unqualified. Ports
/// `_side_has`.
fn side_has(cols: &HashSet<String>, bare: &str) -> bool {
    let suffix = format!(".{bare}");
    cols.iter()
        .any(|entry| entry == bare || entry.ends_with(&suffix))
}

/// Column names a plan subtree exposes, as both bare and qualified forms. The
/// single answer to "which columns does this side of a join expose?". Both forms
/// are returned so a reference resolves whether written bare, aliased, or by
/// physical name. A node with no rule returns the empty set. Ports
/// `available_columns`.
pub fn available_columns(plan: &LogicalPlan) -> HashSet<String> {
    match plan {
        LogicalPlan::Scan(scan) => scan_columns(scan),
        LogicalPlan::Join(join) => {
            let mut cols = available_columns(&join.left);
            cols.extend(available_columns(&join.right));
            cols
        }
        LogicalPlan::Projection(projection) => projection_columns(projection),
        LogicalPlan::Aggregate(aggregate) => aggregate.output_names.iter().cloned().collect(),
        LogicalPlan::Filter(filter) => available_columns(&filter.input),
        LogicalPlan::Limit(limit) => available_columns(&limit.input),
        LogicalPlan::Sort(sort) => available_columns(&sort.input),
        LogicalPlan::CteRef(cte_ref) => {
            let alias = cte_ref.alias.as_deref().unwrap_or(&cte_ref.name);
            aliased_names(&plan.schema(), alias)
        }
        LogicalPlan::SubqueryScan(subquery_scan) => {
            aliased_names(&plan.schema(), &subquery_scan.alias)
        }
        LogicalPlan::Union(_) | LogicalPlan::SetOperation(_) => set_operation_columns(plan),
        _ => HashSet::new(),
    }
}

/// Columns usable above a set operation: those available in EVERY branch (the
/// intersection). A predicate pushed through a set operation lands in each
/// branch, so a reference must resolve in all of them. Ports
/// `_set_operation_columns`.
fn set_operation_columns(plan: &LogicalPlan) -> HashSet<String> {
    let branches: Vec<&LogicalPlan> = match plan {
        LogicalPlan::Union(union) => union.inputs.iter().collect(),
        LogicalPlan::SetOperation(set_op) => vec![&set_op.left, &set_op.right],
        _ => Vec::new(),
    };
    let mut common: Option<HashSet<String>> = None;
    for branch in branches {
        let cols = available_columns(branch);
        common = Some(match common {
            None => cols,
            Some(existing) => &existing & &cols,
        });
    }
    common.unwrap_or_default()
}

/// A scan's columns as bare and alias-qualified (`alias.col`) names. Ports
/// `_scan_columns`.
fn scan_columns(scan: &Scan) -> HashSet<String> {
    let table_ref = scan.alias.as_deref().unwrap_or(&scan.table_name);
    let mut names = HashSet::new();
    for col in &scan.columns {
        names.insert(col.clone());
        names.insert(format!("{table_ref}.{col}"));
    }
    names
}

/// A projection's output aliases plus the columns its expressions reference. A
/// bare `*` falls back to the input. Ports `_projection_columns`.
fn projection_columns(projection: &Projection) -> HashSet<String> {
    let mut names: HashSet<String> = projection.aliases.iter().cloned().collect();
    if names.contains("*") {
        return available_columns(&projection.input);
    }
    for expr in &projection.expressions {
        if let Expr::Column(reference) = expr {
            if let Some(table) = &reference.table {
                names.insert(format!("{table}.{}", reference.column));
            }
            names.insert(reference.column.clone());
        }
    }
    names
}

/// Output columns of an aliased relation (CTE reference, derived table) as bare
/// and alias-qualified names. Ports `_aliased_names`.
fn aliased_names(columns: &[String], alias: &str) -> HashSet<String> {
    let mut names = HashSet::new();
    for column in columns {
        names.insert(column.clone());
        names.insert(format!("{alias}.{column}"));
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

    use fq_plan::logical::{Aggregate, Join, JoinType, Scan};

    /// A scan reading `columns` from `table`, aliased to `table`.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        let mut node = Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = Some(table.to_string());
        LogicalPlan::Scan(Box::new(node))
    }

    #[test]
    fn qualified_or_bare_names_uses_the_qualified_form() {
        // One key per ref: the qualified form for a qualified column.
        let qualified = qualified_or_bare_names(&col("orders", "id"));
        assert!(qualified.contains("orders.id"));
        assert!(!qualified.contains("id"));
        // A bare (unqualified) column contributes its bare name only.
        let bare = qualified_or_bare_names(&Expr::Column(ColumnRef::new(
            None,
            "id",
            Some(DataType::Integer),
        )));
        assert!(bare.contains("id"));
    }

    #[test]
    fn available_columns_of_join_is_union_of_sides() {
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("orders", &["id"])),
            right: Box::new(scan("customer", &["region"])),
            join_type: JoinType::Inner,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let cols = available_columns(&join);
        assert!(cols.contains("orders.id"));
        assert!(cols.contains("customer.region"));
    }

    #[test]
    fn available_columns_of_aggregate_is_output_names() {
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(scan("sales", &["region", "amount"])),
            group_by: vec![col("sales", "region")],
            aggregates: vec![col("sales", "amount")],
            output_names: vec!["region".to_string(), "total".to_string()],
            grouping_sets: None,
        });
        let cols = available_columns(&aggregate);
        assert!(cols.contains("region"));
        assert!(cols.contains("total"));
        // Aggregate output names are bare only (they are the emitted names).
        assert!(!cols.contains("sales.region"));
    }

    #[test]
    fn belongs_to_side_uses_qualifier_on_a_self_join_tie() {
        // Both sides expose bare `id`; only the exact qualified match belongs.
        let left: HashSet<String> = ["a.id".to_string(), "id".to_string()].into_iter().collect();
        let right: HashSet<String> = ["b.id".to_string(), "id".to_string()].into_iter().collect();
        let cols: HashSet<String> = ["a.id".to_string()].into_iter().collect();
        assert!(columns_belong_to_side(&cols, &left, &right));
        assert!(!columns_belong_to_side(&cols, &right, &left));
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
