//! Shared decorrelation plumbing: subquery detection, correlation classification,
//! column-ref replacement, and the inner-alias collector. These are the pure
//! predicates the preparer, the boolean path, and the dependent-join path all
//! build on. Ports the free helpers scattered through `decorrelation.py`
//! (`_is_subquery_node`, `_expression_has_subquery`, `_collect_inner_aliases`,
//! `_references_outer`/`_references_inner`/`_is_inner_column`, `_replace_column_refs`,
//! `_and_join`/`_or_join`, `_is_null_check`).

use std::collections::HashSet;

use fq_common::DataType;
use fq_plan::expr::{
    column_refs, combine_and, combine_or, BinaryOpType, ColumnRef, Expr, LiteralValue, UnaryOpType,
};
use fq_plan::logical::LogicalPlan;

use crate::error::DecorrelationError;

/// Build a binary-operator expression over two operands. A one-line constructor
/// shared by the match/violation/condition builders so no call site re-boxes the
/// operands by hand.
pub fn binary(op: BinaryOpType, left: Expr, right: Expr) -> Expr {
    // Fresh node built from two operands passed in - there is no base node to copy
    // from. Field list (op/left/right) is the complete BinaryOp variant.
    Expr::BinaryOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A manufactured column reference qualified to `table`, carrying the data type of
/// the value it stands for. The join-condition and exposed-key refs the pass mints
/// above the SubqueryScan boundary use this, so every such ref carries both its
/// relation qualifier AND its bound type (a post-binder ref must have both).
pub fn qualified_col(table: &str, column: &str, data_type: DataType) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        column,
        Some(data_type),
    ))
}

/// A manufactured UNqualified column reference (`table = None`), carrying the data
/// type of the value it stands for. Used for the inner physical output names inside
/// a subquery's own projection (the value columns and already-widened aggregate
/// keys); the SubqueryScan alias above re-qualifies them, but the type is set here.
pub fn unqualified_col(column: &str, data_type: DataType) -> Expr {
    Expr::Column(ColumnRef::new(None, column, Some(data_type)))
}

/// A re-read of an EXISTING relation output column by name, qualified to the
/// relation `qualifier`. Unlike `qualified_col`, this does not manufacture a new
/// value: it references a column the relation already exposes, whose type is the
/// relation's own (a base table's catalog type). The decorrelator holds no catalog,
/// so a base-table column type is not derivable here; the reference carries the
/// qualifier and leaves the type for the relation that owns the column.
pub fn relation_col(qualifier: &str, column: &str) -> Expr {
    Expr::Column(ColumnRef::new(Some(qualifier.to_string()), column, None))
}

/// A re-read of an EXISTING inner relation output column by unqualified name, used
/// where a wrapping projection re-exposes an input's columns by name (pruning
/// helper columns back to the original output). The type is the input relation's;
/// as with `relation_col`, a base-table column type is not derivable without the
/// catalog, so it is left for the owning relation.
pub fn output_col(column: &str) -> Expr {
    Expr::Column(ColumnRef::new(None, column, None))
}

/// The output column data types of a relation, one per output column, derived from
/// the expressions that DEFINE those columns: a projection's select list, an
/// aggregate's calls, or a VALUES row. Set operations and unions take their
/// left/first branch (all branches share a type per position); single-input
/// wrappers pass through; a SEMI/ANTI join exposes only its left. A relation whose
/// outputs are raw catalog columns (a bare Scan or CteRef) has no locally derivable
/// types - the decorrelator holds no catalog - and raises.
pub(crate) fn relation_output_types(
    plan: &LogicalPlan,
) -> Result<Vec<DataType>, DecorrelationError> {
    match plan {
        LogicalPlan::Projection(node) => Ok(node.expressions.iter().map(Expr::get_type).collect()),
        LogicalPlan::Aggregate(node) => Ok(node.aggregates.iter().map(Expr::get_type).collect()),
        LogicalPlan::Values(node) => values_row_types(node),
        LogicalPlan::SetOperation(node) => relation_output_types(&node.left),
        LogicalPlan::Union(node) => match node.inputs.first() {
            Some(first) => relation_output_types(first),
            None => Err(DecorrelationError::Invariant(
                "an empty union has no output column types".to_string(),
            )),
        },
        LogicalPlan::Filter(node) => relation_output_types(&node.input),
        LogicalPlan::Sort(node) => relation_output_types(&node.input),
        LogicalPlan::Limit(node) => relation_output_types(&node.input),
        LogicalPlan::GroupedLimit(node) => relation_output_types(&node.input),
        LogicalPlan::SingleRowGuard(node) => relation_output_types(&node.input),
        LogicalPlan::SubqueryScan(node) => relation_output_types(&node.input),
        LogicalPlan::Join(node) => join_output_types(node),
        other => Err(DecorrelationError::Unsupported(format!(
            "cannot derive output column types for a {} relation without the catalog",
            plan_kind(other)
        ))),
    }
}

/// The output column types of a VALUES relation, taken from its first row.
fn values_row_types(node: &fq_plan::logical::Values) -> Result<Vec<DataType>, DecorrelationError> {
    match node.rows.first() {
        Some(row) => Ok(row.iter().map(Expr::get_type).collect()),
        None => Err(DecorrelationError::Invariant(
            "a VALUES relation with no rows has no output column types".to_string(),
        )),
    }
}

/// The output column types of a join: left types, plus right types unless the join
/// is existential (SEMI/ANTI expose only the left side).
fn join_output_types(node: &fq_plan::logical::Join) -> Result<Vec<DataType>, DecorrelationError> {
    let mut types = relation_output_types(&node.left)?;
    if !matches!(
        node.join_type,
        fq_plan::logical::JoinType::Semi | fq_plan::logical::JoinType::Anti
    ) {
        types.extend(relation_output_types(&node.right)?);
    }
    Ok(types)
}

/// One typed unqualified column reference per output name, pairing each name with
/// its derived type. Raises when the name and type counts disagree - a length
/// mismatch means the type derivation lost track of a column, and shipping an
/// untyped or mis-typed ref would defeat the type contract.
pub(crate) fn typed_output_refs(
    names: &[String],
    types: &[DataType],
) -> Result<Vec<Expr>, DecorrelationError> {
    if names.len() != types.len() {
        return Err(DecorrelationError::Invariant(format!(
            "relation exposes {} output names but {} derived types",
            names.len(),
            types.len()
        )));
    }
    let mut refs = Vec::with_capacity(names.len());
    for (name, data_type) in names.iter().zip(types.iter()) {
        refs.push(unqualified_col(name, *data_type));
    }
    Ok(refs)
}

/// A node-kind label for the "cannot derive types" error message.
fn plan_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::CteRef(_) => "CteRef",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::LateralJoin(_) => "LateralJoin",
        _ => "unsupported",
    }
}

/// A boolean literal expression (the constant-EXISTS residual).
pub fn bool_literal(value: bool) -> Expr {
    // Fresh node built from a bool constant - there is no base node to copy from.
    // Field list (value/data_type) is the complete Literal variant.
    Expr::Literal {
        value: LiteralValue::Boolean(value),
        data_type: DataType::Boolean,
    }
}

/// Whether an expression IS one of the four subquery-bearing nodes. Ports
/// `_SUBQUERY_NODE_TYPES` membership.
pub fn is_subquery_node(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::QuantifiedComparison { .. }
    )
}

/// Whether an expression tree contains a subquery node at the top or nested
/// inside a non-subquery parent. Does NOT descend into a subquery's own inner
/// scope (`Expr::children` stops at subquery boundaries). Ports
/// `_expression_has_subquery`.
pub fn expression_has_subquery(expr: &Expr) -> bool {
    if is_subquery_node(expr) {
        return true;
    }
    expr.children()
        .iter()
        .any(|child| expression_has_subquery(child))
}

/// Build an `IS NULL` check over an operand. Ports `_is_null_check`'s construction
/// site (the NULL-aware NOT IN / op ALL terms).
pub fn null_check(operand: Expr) -> Expr {
    // Fresh node wrapping an operand in IS NULL - there is no base node to copy
    // from. Field list (op/operand) is the complete UnaryOp variant.
    Expr::UnaryOp {
        op: UnaryOpType::IsNull,
        operand: Box::new(operand),
    }
}

/// Rebuild an expression, replacing any `ColumnRef` for which `lookup` returns a
/// replacement expression. Recurses via `Expr::map_children`, so it never
/// descends into a subquery boundary. Ports `_replace_column_refs`; the caller
/// supplies the mapping (a `HashMap` for the exposure map, an ordered structure
/// for the N-K domain map - both reduce to a `(table, column)` lookup).
pub fn replace_column_refs(expr: Expr, lookup: &impl Fn(&ColumnRef) -> Option<Expr>) -> Expr {
    if let Expr::Column(col) = &expr {
        if let Some(replacement) = lookup(col) {
            return replacement;
        }
        return expr;
    }
    expr.map_children(&mut |child| replace_column_refs(child, lookup))
}

/// AND a non-empty list of predicates, raising when the list is empty. Ports
/// `_and_join` (which raises rather than silently producing no predicate - an
/// empty conjunct list is always an upstream bug).
pub fn and_join(terms: Vec<Expr>) -> Result<Expr, DecorrelationError> {
    combine_and(terms).ok_or_else(|| {
        DecorrelationError::Invariant("cannot build an AND predicate from no conjuncts".to_string())
    })
}

/// OR a non-empty list of predicates, raising when the list is empty. Ports
/// `_or_join`.
pub fn or_join(terms: Vec<Expr>) -> Result<Expr, DecorrelationError> {
    combine_or(terms).ok_or_else(|| {
        DecorrelationError::Invariant("cannot build an OR predicate from no disjuncts".to_string())
    })
}

/// Every relation alias/name defined INSIDE a subquery body, collected
/// transitively (descending through `SubqueryScan` bodies). A column ref whose
/// qualifier is NOT in this set is an outer (correlated) reference. Ports
/// `_collect_inner_aliases`.
///
/// This is deliberately the TRANSITIVE collector - distinct from the physical
/// planner's boundary-stopping one, which stops at a `SubqueryScan`. Keep them
/// separate.
pub fn collect_inner_aliases(plan: &LogicalPlan) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_inner_aliases_into(plan, &mut aliases);
    aliases
}

/// Recursive helper for `collect_inner_aliases`.
fn collect_inner_aliases_into(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan(scan) => match &scan.alias {
            Some(alias) if *alias != scan.table_name => {
                aliases.insert(alias.clone());
            }
            _ => {
                aliases.insert(scan.table_name.clone());
            }
        },
        LogicalPlan::SubqueryScan(node) => {
            aliases.insert(node.alias.clone());
        }
        LogicalPlan::CteRef(node) => {
            let name = node.alias.clone().unwrap_or_else(|| node.name.clone());
            aliases.insert(name);
        }
        _ => {}
    }
    for child in plan.children() {
        collect_inner_aliases_into(child, aliases);
    }
}

/// Whether a column reference is an OUTER (correlated) reference: it carries a
/// qualifier that is not one of the subquery's own relations. The `is_some`
/// guard is load-bearing - it excludes the unqualified `*` of `COUNT(*)` from
/// being read as a correlation. Ports `_is_outer_ref`.
// inner_aliases is always built by `collect_inner_aliases` (default hasher), so
// generalizing over the hasher would only add noise (mirrors fq-plan).
#[allow(clippy::implicit_hasher)]
pub fn is_outer_ref(col: &ColumnRef, inner_aliases: &HashSet<String>) -> bool {
    match &col.table {
        Some(table) => !inner_aliases.contains(table),
        None => false,
    }
}

/// Whether an expression tree references any outer column. Ports
/// `_references_outer`.
#[allow(clippy::implicit_hasher)]
pub fn references_outer(expr: &Expr, inner_aliases: &HashSet<String>) -> bool {
    column_refs(expr)
        .iter()
        .any(|col| is_outer_ref(col, inner_aliases))
}

/// Whether an expression tree references any INNER column. Note the asymmetry
/// with `references_outer`: an UNqualified ref counts as inner here (the
/// conservative side). Ports `_references_inner`.
#[allow(clippy::implicit_hasher)]
pub fn references_inner(expr: &Expr, inner_aliases: &HashSet<String>) -> bool {
    column_refs(expr).iter().any(|col| match &col.table {
        Some(table) => inner_aliases.contains(table),
        None => true,
    })
}

/// Whether an expression is a PURE inner column: a `ColumnRef` qualified by one
/// of the subquery's own relations. Ports `_is_inner_column`.
#[allow(clippy::implicit_hasher)]
pub fn is_inner_column(expr: &Expr, inner_aliases: &HashSet<String>) -> bool {
    match expr {
        Expr::Column(col) => match &col.table {
            Some(table) => inner_aliases.contains(table),
            None => false,
        },
        _ => false,
    }
}

/// Whether a plan is correlated: any expression attached directly to any node in
/// the tree references an outer column. Ports `_is_correlated`.
#[allow(clippy::implicit_hasher)]
pub fn is_correlated(plan: &LogicalPlan, inner_aliases: &HashSet<String>) -> bool {
    if plan
        .direct_expressions()
        .iter()
        .any(|expr| references_outer(expr, inner_aliases))
    {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| is_correlated(child, inner_aliases))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, LiteralValue};
    use fq_plan::logical::{Filter, Scan, SubqueryScan};

    /// A (possibly qualified) integer column reference for tests.
    fn col(table: Option<&str>, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            table.map(str::to_string),
            name,
            Some(DataType::Integer),
        ))
    }

    /// An equality comparison for tests.
    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// A base-table scan (optionally aliased) for tests.
    fn scan(table: &str, alias: Option<&str>, columns: &[&str]) -> LogicalPlan {
        let mut node = Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = alias.map(str::to_string);
        LogicalPlan::Scan(Box::new(node))
    }

    #[test]
    fn subquery_detection() {
        let exists = Expr::Exists {
            subquery: Box::new(scan("p", None, &["id"])),
            negated: false,
        };
        assert!(is_subquery_node(&exists));
        assert!(expression_has_subquery(&exists));
        // Nested inside a binary op.
        let wrapped = eq(col(Some("o"), "x"), exists);
        assert!(expression_has_subquery(&wrapped));
        assert!(!expression_has_subquery(&col(Some("o"), "x")));
    }

    #[test]
    fn inner_aliases_prefers_alias_over_table_name() {
        // Aliased scan hides the base name.
        let aliased = collect_inner_aliases(&scan("products", Some("p"), &["id"]));
        assert!(aliased.contains("p"));
        assert!(!aliased.contains("products"));
        // Unaliased scan contributes its table name.
        let plain = collect_inner_aliases(&scan("products", None, &["id"]));
        assert!(plain.contains("products"));
    }

    #[test]
    fn inner_aliases_descends_into_subquery_scan() {
        let inner = LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(scan("orders", Some("o"), &["id"])),
            alias: "sub".to_string(),
            column_names: None,
        });
        let aliases = collect_inner_aliases(&inner);
        // Both the boundary alias AND the transitively-collected inner alias.
        assert!(aliases.contains("sub"));
        assert!(aliases.contains("o"));
    }

    #[test]
    fn outer_ref_excludes_unqualified_star() {
        let mut inner = HashSet::new();
        inner.insert("p".to_string());
        // A qualified ref to a non-inner relation is outer.
        let outer = ColumnRef::new(Some("o".to_string()), "id", None);
        assert!(is_outer_ref(&outer, &inner));
        // The inner relation is not outer.
        let in_col = ColumnRef::new(Some("p".to_string()), "id", None);
        assert!(!is_outer_ref(&in_col, &inner));
        // An unqualified ref (COUNT(*) star) is never outer.
        let star = ColumnRef::new(None, "*", None);
        assert!(!is_outer_ref(&star, &inner));
    }

    #[test]
    fn references_outer_and_inner_polarity() {
        let mut inner = HashSet::new();
        inner.insert("p".to_string());
        // p.id = o.product_id : references both inner and outer.
        let predicate = eq(col(Some("p"), "id"), col(Some("o"), "product_id"));
        assert!(references_outer(&predicate, &inner));
        assert!(references_inner(&predicate, &inner));
        assert!(is_inner_column(&col(Some("p"), "id"), &inner));
        assert!(!is_inner_column(&col(Some("o"), "product_id"), &inner));
    }

    #[test]
    fn is_correlated_walks_filter_predicate() {
        let mut inner = HashSet::new();
        inner.insert("p".to_string());
        let correlated = LogicalPlan::Filter(Filter {
            input: Box::new(scan("products", Some("p"), &["id"])),
            predicate: eq(col(Some("p"), "id"), col(Some("o"), "pid")),
        });
        assert!(is_correlated(&correlated, &inner));
        let plain = LogicalPlan::Filter(Filter {
            input: Box::new(scan("products", Some("p"), &["id"])),
            predicate: eq(col(Some("p"), "id"), col(Some("p"), "other")),
        });
        assert!(!is_correlated(&plain, &inner));
    }

    #[test]
    fn replace_column_refs_swaps_matching_qualifier() {
        // Replace o.pid with sub.k0.
        let predicate = eq(col(Some("p"), "id"), col(Some("o"), "pid"));
        let replaced = replace_column_refs(predicate, &|c: &ColumnRef| {
            if c.table.as_deref() == Some("o") && c.column == "pid" {
                Some(Expr::Column(ColumnRef::new(
                    Some("sub".to_string()),
                    "k0",
                    None,
                )))
            } else {
                None
            }
        });
        let expected = eq(
            col(Some("p"), "id"),
            Expr::Column(ColumnRef::new(Some("sub".to_string()), "k0", None)),
        );
        assert_eq!(replaced, expected);
    }

    #[test]
    fn and_join_empty_is_error() {
        assert!(and_join(vec![]).is_err());
        assert!(or_join(vec![]).is_err());
        let single = Expr::Literal {
            value: LiteralValue::Boolean(true),
            data_type: DataType::Boolean,
        };
        assert_eq!(and_join(vec![single.clone()]).unwrap(), single);
    }
}
