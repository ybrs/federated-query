//! Join-graph extraction for the cost-based join-ordering optimizer. Ports
//! `optimizer/join_graph.py`. Clean-Rust-portable (no persisted artifact).
//!
//! A REGION is a maximal subtree of REORDERABLE joins: INNER/CROSS joins (never
//! NATURAL/USING) plus the Filter nodes between them, whose conjuncts join the
//! predicate pool. Everything else stops descent and becomes an opaque ATOM.
//!
//! Every conjunct is classified by the exact set of atoms it references. A
//! reference resolving to no atom or to more than one is a malformed plan and
//! RAISES: silently mis-placing a predicate manufactures wrong results.

use std::collections::BTreeSet;

use thiserror::Error;

use fq_plan::expr::{column_refs, split_conjuncts, Expr};
use fq_plan::logical::{JoinType, LogicalPlan};

use crate::pushdown::is_equi_predicate;

/// A join region whose predicates cannot be soundly mapped to its atoms.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum JoinGraphError {
    /// An unqualified column reached a region predicate; every post-binder
    /// reference must carry its relation qualifier.
    #[error("unqualified column {0:?} in a join-region predicate; every post-binder reference must carry its relation qualifier")]
    Unqualified(String),

    /// A reference resolves to a number of atoms other than exactly one.
    #[error("reference {relation}.{column} resolves to {atom_count} atoms; a region predicate must map to exactly one")]
    AmbiguousReference {
        relation: String,
        column: String,
        atom_count: usize,
    },
}

/// One reorderable input of a join region.
///
/// `index` is the atom's position in the original plan (FROM order) - the
/// deterministic tie-break for the enumerator. `qualifiers` are the relation
/// names a predicate above this subtree can reference.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinAtom {
    pub index: usize,
    pub plan: LogicalPlan,
    pub qualifiers: BTreeSet<String>,
}

/// One predicate conjunct of a region, mapped to the atoms it references.
///
/// `is_equi` marks a column-to-column equality - a join-graph edge the
/// enumerator can use as a key; everything else contributes selectivity only.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinConjunct {
    pub expression: Expr,
    pub atom_indexes: BTreeSet<usize>,
    pub is_equi: bool,
}

/// A reorderable join region: its atoms and its classified conjuncts.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinRegion {
    pub atoms: Vec<JoinAtom>,
    pub conjuncts: Vec<JoinConjunct>,
}

/// The join region rooted at this node, or `None` when there is none. The root
/// may be the top join itself or a Filter directly above it (its conjuncts
/// belong to the region's pool).
pub fn extract_region(root: &LogicalPlan) -> Result<Option<JoinRegion>, JoinGraphError> {
    if !is_region_root(root) {
        return Ok(None);
    }
    let mut atoms = Vec::new();
    let mut expressions = Vec::new();
    descend(root, &mut atoms, &mut expressions);
    let mut conjuncts = Vec::with_capacity(expressions.len());
    for expression in expressions {
        conjuncts.push(classify(expression, &atoms)?);
    }
    Ok(Some(JoinRegion { atoms, conjuncts }))
}

/// A join the enumerator may reorder: INNER/CROSS with an explicit (or no)
/// condition. NATURAL/USING joins carry an implicit condition that reordering
/// would silently drop, so they are atoms.
pub fn is_region_join(node: &LogicalPlan) -> bool {
    let LogicalPlan::Join(join) = node else {
        return false;
    };
    if join.natural || join.using.is_some() {
        return false;
    }
    matches!(join.join_type, JoinType::Inner | JoinType::Cross)
}

/// Whether a region starts here: a reorderable join, possibly under a Filter
/// whose conjuncts then join the region's predicate pool.
fn is_region_root(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Filter(filter) => is_region_join(&filter.input),
        other => is_region_join(other),
    }
}

/// Accumulate the region's atoms and predicate expressions (owned, so the region
/// outlives the input plan).
fn descend(node: &LogicalPlan, atoms: &mut Vec<JoinAtom>, expressions: &mut Vec<Expr>) {
    if let LogicalPlan::Filter(filter) = node {
        for conjunct in split_conjuncts(&filter.predicate) {
            expressions.push(conjunct.clone());
        }
        descend(&filter.input, atoms, expressions);
        return;
    }
    if is_region_join(node) {
        let LogicalPlan::Join(join) = node else {
            unreachable!("is_region_join guarantees a Join");
        };
        if let Some(condition) = &join.condition {
            for conjunct in split_conjuncts(condition) {
                expressions.push(conjunct.clone());
            }
        }
        descend(&join.left, atoms, expressions);
        descend(&join.right, atoms, expressions);
        return;
    }
    // Anything else stops the region: an opaque atom at the next index, keeping
    // the original left-to-right (FROM) order.
    // `node` is borrowed; the join-ordering search needs an OWNED atom plan, so
    // this clone of the opaque subtree is unavoidable (we cannot move out of &node).
    let plan = node.clone();
    atoms.push(JoinAtom {
        index: atoms.len(),
        plan,
        qualifiers: visible_qualifiers(node),
    });
}

/// The relation names a predicate above this subtree can reference.
fn visible_qualifiers(node: &LogicalPlan) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    collect_qualifiers(node, &mut found);
    found
}

/// Collect visible relation names, stopping where aliasing hides them.
fn collect_qualifiers(node: &LogicalPlan, found: &mut BTreeSet<String>) {
    match node {
        LogicalPlan::Scan(scan) => {
            found.insert(
                scan.alias
                    .clone()
                    .unwrap_or_else(|| scan.table_name.clone()),
            );
        }
        LogicalPlan::SubqueryScan(subquery) => {
            // A derived table re-aliases its whole subplan: only its own alias is
            // visible above; the inner relation names are hidden.
            found.insert(subquery.alias.clone());
        }
        LogicalPlan::CteRef(cte_ref) => {
            found.insert(
                cte_ref
                    .alias
                    .clone()
                    .unwrap_or_else(|| cte_ref.name.clone()),
            );
        }
        other => {
            for child in other.children() {
                collect_qualifiers(child, found);
            }
        }
    }
}

/// Map one conjunct to the exact set of atoms it references.
fn classify(expression: Expr, atoms: &[JoinAtom]) -> Result<JoinConjunct, JoinGraphError> {
    let mut atom_indexes = BTreeSet::new();
    for reference in column_refs(&expression) {
        atom_indexes.insert(owning_atom(reference, atoms)?);
    }
    let is_equi = is_equi_predicate(&expression);
    Ok(JoinConjunct {
        expression,
        atom_indexes,
        is_equi,
    })
}

/// The single atom a qualified reference resolves to; anything else raises.
fn owning_atom(
    reference: &fq_plan::expr::ColumnRef,
    atoms: &[JoinAtom],
) -> Result<usize, JoinGraphError> {
    let Some(qualifier) = reference.table.as_deref().filter(|table| !table.is_empty()) else {
        return Err(JoinGraphError::Unqualified(reference.column.clone()));
    };
    let mut owners = Vec::new();
    for atom in atoms {
        if atom.qualifiers.contains(qualifier) {
            owners.push(atom.index);
        }
    }
    if owners.len() != 1 {
        return Err(JoinGraphError::AmbiguousReference {
            relation: qualifier.to_string(),
            column: reference.column.clone(),
            atom_count: owners.len(),
        });
    }
    Ok(owners[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, Expr};
    use fq_plan::logical::{Aggregate, Filter, Join, Scan};

    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(Box::new(Scan::new(
            "duck",
            "main",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        )))
    }

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn inner(left: LogicalPlan, right: LogicalPlan, condition: Option<Expr>) -> LogicalPlan {
        LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        })
    }

    #[test]
    fn three_table_chain_yields_three_atoms_in_from_order() {
        // ((a JOIN b) JOIN c) with a chain of equi conditions.
        let join_ab = inner(
            scan("a", &["id", "b_id"]),
            scan("b", &["id", "c_id"]),
            Some(eq(col("a", "b_id"), col("b", "id"))),
        );
        let join_abc = inner(
            join_ab,
            scan("c", &["id"]),
            Some(eq(col("b", "c_id"), col("c", "id"))),
        );
        let region = extract_region(&join_abc).unwrap().unwrap();
        assert_eq!(region.atoms.len(), 3);
        // FROM order preserved: a, b, c.
        assert!(region.atoms[0].qualifiers.contains("a"));
        assert!(region.atoms[1].qualifiers.contains("b"));
        assert!(region.atoms[2].qualifiers.contains("c"));
        // Two equi edges, each touching exactly two atoms.
        let edges: Vec<&JoinConjunct> = region
            .conjuncts
            .iter()
            .filter(|c| c.is_equi && c.atom_indexes.len() == 2)
            .collect();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn filter_above_top_join_joins_the_pool() {
        let join = inner(
            scan("a", &["id"]),
            scan("b", &["id"]),
            Some(eq(col("a", "id"), col("b", "id"))),
        );
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(join),
            predicate: Expr::BinaryOp {
                op: BinaryOpType::Gt,
                left: Box::new(col("a", "id")),
                right: Box::new(Expr::Literal {
                    value: fq_plan::expr::LiteralValue::Integer(3),
                    data_type: DataType::Integer,
                }),
            },
        });
        let region = extract_region(&filter).unwrap().unwrap();
        // The equi condition plus the filter conjunct are both pooled.
        assert_eq!(region.conjuncts.len(), 2);
        // The filter conjunct touches only atom a (single-atom selectivity term).
        let single: Vec<&JoinConjunct> = region
            .conjuncts
            .iter()
            .filter(|c| !c.is_equi && c.atom_indexes.len() == 1)
            .collect();
        assert_eq!(single.len(), 1);
    }

    #[test]
    fn aggregate_stops_descent_as_one_atom() {
        // An aggregate over a join is opaque: it becomes a single atom exposing
        // every underlying name.
        let inner_join = inner(
            scan("a", &["id"]),
            scan("b", &["id"]),
            Some(eq(col("a", "id"), col("b", "id"))),
        );
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(inner_join),
            group_by: vec![col("a", "id")],
            aggregates: vec![],
            output_names: vec!["id".to_string()],
            grouping_sets: None,
        });
        let top = inner(
            aggregate,
            scan("c", &["id"]),
            Some(eq(col("a", "id"), col("c", "id"))),
        );
        let region = extract_region(&top).unwrap().unwrap();
        assert_eq!(region.atoms.len(), 2);
        // The aggregate atom exposes both a and b (visible-through composite).
        assert!(region.atoms[0].qualifiers.contains("a"));
        assert!(region.atoms[0].qualifiers.contains("b"));
    }

    #[test]
    fn non_region_root_returns_none() {
        assert_eq!(extract_region(&scan("a", &["id"])).unwrap(), None);
    }

    #[test]
    fn left_join_is_an_atom_not_a_region() {
        let left_join = LogicalPlan::Join(Join {
            left: Box::new(scan("a", &["id"])),
            right: Box::new(scan("b", &["id"])),
            join_type: JoinType::Left,
            condition: Some(eq(col("a", "id"), col("b", "id"))),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        assert_eq!(extract_region(&left_join).unwrap(), None);
    }

    #[test]
    fn unqualified_reference_raises() {
        let join = inner(
            scan("a", &["id"]),
            scan("b", &["id"]),
            Some(eq(
                Expr::Column(ColumnRef::new(None, "id", Some(DataType::Integer))),
                col("b", "id"),
            )),
        );
        let error = extract_region(&join).unwrap_err();
        assert!(matches!(error, JoinGraphError::Unqualified(_)));
    }

    #[test]
    fn reference_to_unknown_relation_raises() {
        let join = inner(
            scan("a", &["id"]),
            scan("b", &["id"]),
            Some(eq(col("a", "id"), col("zzz", "id"))),
        );
        let error = extract_region(&join).unwrap_err();
        assert!(matches!(
            error,
            JoinGraphError::AmbiguousReference { atom_count: 0, .. }
        ));
    }
}
