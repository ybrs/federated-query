//! Cross-source LATERAL lowering: a merge-engine dependent join. The left and the
//! right's single base relation are materialized, the base reduced to the left's
//! correlation domain (a dynamic filter) before transfer. Ports
//! `_plan_lateral_join` and its correlation extraction.

use std::collections::HashMap;

use fq_emit::quote_ident;
use fq_plan::expr::{BinaryOpType, Expr};
use fq_plan::logical::{JoinType, LateralJoin, LogicalPlan, Scan};
use fq_plan::physical::{PhysicalLateralJoin, PhysicalPlan};

use super::PhysicalPlanner;
use crate::error::PhysicalError;

/// Comparison operators a LATERAL correlation can use to derive a domain filter.
fn is_comparison(op: BinaryOpType) -> bool {
    matches!(
        op,
        BinaryOpType::Eq
            | BinaryOpType::Lt
            | BinaryOpType::Lte
            | BinaryOpType::Gt
            | BinaryOpType::Gte
    )
}

/// Operator flip when the inner column is on the right of the comparison. Ports
/// `_FLIP_OP`.
fn flip_op(op: BinaryOpType) -> BinaryOpType {
    match op {
        BinaryOpType::Eq => BinaryOpType::Eq,
        BinaryOpType::Lt => BinaryOpType::Gt,
        BinaryOpType::Lte => BinaryOpType::Gte,
        BinaryOpType::Gt => BinaryOpType::Lt,
        BinaryOpType::Gte => BinaryOpType::Lte,
        // `flip_op` is only reached after `is_comparison` passes; a non-comparison
        // here is a caller bug. Fail loud (Python's `_FLIP_OP[op]` KeyError), never
        // return a wrong flipped operator.
        _ => unreachable!("flip_op called on a non-comparison operator"),
    }
}

impl PhysicalPlanner {
    /// Plan a cross-source LATERAL as a merge-engine dependent join. Same-source
    /// laterals were already pushed as one remote query by single-source pushdown.
    pub(super) fn plan_lateral_join(
        &mut self,
        node: &LateralJoin,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let mut base_scans: Vec<&Scan> = Vec::new();
        Self::collect_base_scans(&node.right, &mut base_scans);
        if base_scans.len() != 1 {
            return Err(PhysicalError::LateralMultipleBaseRelations);
        }
        let base = base_scans[0];
        let base_name = "lat_b0";
        let mut scan_names: HashMap<*const Scan, String> = HashMap::new();
        scan_names.insert(std::ptr::from_ref(base), base_name.to_string());
        let Some(right_sql) = self
            .single_source()
            .render_correlated_sql(&node.right, &scan_names)?
        else {
            return Err(PhysicalError::LateralNotRenderable);
        };
        let outer_alias = lateral_outer_alias(&node.left);
        let base_alias = base
            .alias
            .clone()
            .unwrap_or_else(|| base.table_name.clone());
        let lateral_sql = lateral_sql(node, &outer_alias, &right_sql);
        let correlations = lateral_correlations(&node.right, &base_alias, &outer_alias);
        let mut output_names = node.left.schema();
        output_names.extend(node.right.schema());

        let left = self.plan_node(&node.left)?;
        let base_scan = self.plan_scan_node(base)?;
        Ok(PhysicalPlan::LateralJoin(Box::new(PhysicalLateralJoin {
            left: Box::new(left),
            left_name: "lat_left".to_string(),
            left_alias: outer_alias,
            base_scan: Box::new(base_scan),
            base_name: base_name.to_string(),
            lateral_sql,
            output_names,
            join_type: node.join_type,
            correlations,
        })))
    }
}

/// The left relation's alias, referenced by the right's correlation: the leftmost
/// base scan's alias or table name, else `lat_left`. Ports `_lateral_outer_alias`.
fn lateral_outer_alias(left: &LogicalPlan) -> String {
    match leftmost_scan(left) {
        Some(scan) => scan
            .alias
            .clone()
            .unwrap_or_else(|| scan.table_name.clone()),
        None => "lat_left".to_string(),
    }
}

/// The leftmost base scan of a subtree. Ports `_leftmost_scan`.
fn leftmost_scan(node: &LogicalPlan) -> Option<&Scan> {
    if let LogicalPlan::Scan(scan) = node {
        return Some(scan);
    }
    leftmost_scan(node.children().first()?)
}

/// Build the merge-engine LATERAL SQL over the registered relations `lat_left` and
/// the rendered right subquery. Hand-built text over already-rendered pieces (the
/// register names `lat_left`/`sub`/`outer_alias` are engine-internal, emitted
/// verbatim; only the column names are quoted). Ports `_lateral_sql`.
fn lateral_sql(node: &LateralJoin, outer_alias: &str, right_sql: &str) -> String {
    let mut items = Vec::new();
    for column in node.left.schema() {
        items.push(format!("{outer_alias}.{}", quote_ident(&column)));
    }
    for column in node.right.schema() {
        items.push(format!("sub.{}", quote_ident(&column)));
    }
    let keyword = if node.join_type == JoinType::Left {
        "LEFT JOIN"
    } else {
        "INNER JOIN"
    };
    format!(
        "SELECT {} FROM lat_left AS {outer_alias} {keyword} LATERAL ({right_sql}) AS sub ON TRUE",
        items.join(", ")
    )
}

/// Extract `(inner col, op, outer col)` terms for the dynamic filter. Ports
/// `_lateral_correlations`.
fn lateral_correlations(
    right: &LogicalPlan,
    base_alias: &str,
    outer_alias: &str,
) -> Vec<(String, BinaryOpType, String)> {
    let mut terms = Vec::new();
    for predicate in lateral_predicates(right) {
        if let Some(term) = correlation_term(predicate, base_alias, outer_alias) {
            terms.push(term);
        }
    }
    terms
}

/// All conjuncts of every Filter in a subtree. Ports `_lateral_predicates`.
fn lateral_predicates(node: &LogicalPlan) -> Vec<&Expr> {
    let mut predicates = Vec::new();
    if let LogicalPlan::Filter(filter) = node {
        predicates.extend(fq_plan::expr::split_conjuncts(&filter.predicate));
    }
    for child in node.children() {
        predicates.extend(lateral_predicates(child));
    }
    predicates
}

/// Decompose `inner OP outer` into a normalized `(inner, op, outer)`. Ports
/// `_correlation_term`.
fn correlation_term(
    predicate: &Expr,
    base_alias: &str,
    outer_alias: &str,
) -> Option<(String, BinaryOpType, String)> {
    let Expr::BinaryOp { op, left, right } = predicate else {
        return None;
    };
    if !is_comparison(*op) {
        return None;
    }
    if let (Some(inner), Some(outer)) =
        (ref_column(left, base_alias), ref_column(right, outer_alias))
    {
        return Some((inner.to_string(), *op, outer.to_string()));
    }
    if let (Some(outer), Some(inner)) =
        (ref_column(left, outer_alias), ref_column(right, base_alias))
    {
        return Some((inner.to_string(), flip_op(*op), outer.to_string()));
    }
    None
}

/// The column name when `expr` is a column ref qualified by `alias`, else None.
/// Ports `_is_ref` (returning the name so the caller need not re-match).
fn ref_column<'a>(expr: &'a Expr, alias: &str) -> Option<&'a str> {
    match expr {
        Expr::Column(reference) if reference.table.as_deref() == Some(alias) => {
            Some(reference.column.as_str())
        }
        _ => None,
    }
}
