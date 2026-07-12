//! Set-operation lowering: push a whole UNION/INTERSECT/EXCEPT to one source when
//! both branches share it, else evaluate it locally. Ports `_plan_set_operation`
//! and its helpers.

use fq_plan::expr::Expr;
use fq_plan::logical::{SetOpKind, SetOperation};
use fq_plan::physical::{
    PhysicalPlan, PhysicalProjection, PhysicalRemoteSetOp, PhysicalScan, PhysicalSetOperation,
    PhysicalUnion,
};

use super::PhysicalPlanner;
use crate::error::PhysicalError;
use crate::single_source::same_source;

impl PhysicalPlanner {
    /// Plan a set operation, pushing it to one source when both branches share it.
    /// Ports `_plan_set_operation`. The originals are kept until the remote path is
    /// committed (Rust moves the branches into whichever node is built).
    pub(super) fn plan_set_operation(
        &mut self,
        set_op: &SetOperation,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let left = self.plan_node(&set_op.left)?;
        let right = self.plan_node(&set_op.right)?;
        match self.try_plan_remote_set_op(set_op, &left, &right) {
            Some(remote) => Ok(remote),
            None => Ok(plan_local_set_op(set_op, left, right)),
        }
    }

    /// Build a single remote set operation when both branches push together and
    /// resolve to the same source. Ports `_try_plan_remote_set_op` (the connection
    /// check becomes a catalog lookup, since the node no longer carries it).
    fn try_plan_remote_set_op(
        &self,
        set_op: &SetOperation,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
    ) -> Option<PhysicalPlan> {
        let left_branch = as_pushable_set_branch(left)?;
        let right_branch = as_pushable_set_branch(right)?;
        let left_source = branch_datasource(&left_branch);
        let right_source = branch_datasource(&right_branch);
        if !same_source(Some(left_source), Some(right_source)) {
            return None;
        }
        // The source must resolve (Python checked the live connection; the node no
        // longer carries it, so validate against the catalog by name).
        self.catalog().get_datasource(left_source)?;
        let datasource = left_source.to_string();
        // Fresh PhysicalRemoteSetOp built from the logical SetOperation and the two
        // pushable branches: no base to copy, so every field is listed (order_by /
        // limit / offset start empty; a parent Sort or Limit folds them in later).
        Some(PhysicalPlan::RemoteSetOp(Box::new(PhysicalRemoteSetOp {
            left: Box::new(left_branch),
            right: Box::new(right_branch),
            kind: set_op.kind,
            distinct: set_op.distinct,
            datasource,
            order_by_keys: None,
            order_by_ascending: None,
            order_by_nulls: None,
            limit: None,
            offset: 0,
        })))
    }
}

/// The datasource name a pushable set branch reads from.
fn branch_datasource(branch: &PhysicalPlan) -> &str {
    match branch {
        PhysicalPlan::Scan(scan) => &scan.datasource,
        PhysicalPlan::RemoteSetOp(remote) => &remote.datasource,
        // `as_pushable_set_branch` only ever yields the two variants above.
        _ => unreachable!("a pushable set branch is a Scan or a RemoteSetOp"),
    }
}

/// Return a single-source branch (a scan, a nested remote set op, or a bare-column
/// projection collapsed onto its scan) or None. Ports `_as_pushable_set_branch`.
fn as_pushable_set_branch(node: &PhysicalPlan) -> Option<PhysicalPlan> {
    match node {
        // The branch subtree is cloned so the ORIGINAL left/right survive for the
        // local-set-op fallback when the OTHER branch turns out not to be pushable
        // (plan_set_operation moves the owned originals into whichever node is built).
        PhysicalPlan::Scan(_) | PhysicalPlan::RemoteSetOp(_) => Some(node.clone()),
        PhysicalPlan::Projection(projection) => {
            collapse_projection_scan(projection).map(|scan| PhysicalPlan::Scan(Box::new(scan)))
        }
        _ => None,
    }
}

/// Fold a plain-column projection over a scan into one scan (the projection only
/// selects bare, un-renamed columns). Ports `_collapse_projection_scan`.
fn collapse_projection_scan(projection: &PhysicalProjection) -> Option<PhysicalScan> {
    let PhysicalPlan::Scan(scan) = projection.input.as_ref() else {
        return None;
    };
    if scan.group_by.is_some() || scan.aggregates.is_some() {
        return None;
    }
    let columns = bare_projection_columns(projection)?;
    let mut collapsed = (**scan).clone();
    collapsed.columns = columns;
    Some(collapsed)
}

/// Projected column names when every item is an unaliased column (each expression
/// a bare `Column` whose name equals its positionally aligned output name). Ports
/// `_bare_projection_columns`.
fn bare_projection_columns(projection: &PhysicalProjection) -> Option<Vec<String>> {
    let names = &projection.output_names;
    let mut columns = Vec::with_capacity(projection.expressions.len());
    for (index, expression) in projection.expressions.iter().enumerate() {
        let Expr::Column(reference) = expression else {
            return None;
        };
        if index < names.len() && names[index] != reference.column {
            return None;
        }
        columns.push(reference.column.clone());
    }
    Some(columns)
}

/// Evaluate a set operation locally when it cannot be pushed down. Ports
/// `_plan_local_set_op`.
fn plan_local_set_op(
    set_op: &SetOperation,
    left: PhysicalPlan,
    right: PhysicalPlan,
) -> PhysicalPlan {
    match set_op.kind {
        // Fresh PhysicalUnion lowered from the logical SetOperation: no base to copy,
        // so both fields are listed.
        SetOpKind::Union => PhysicalPlan::Union(PhysicalUnion {
            inputs: vec![left, right],
            distinct: set_op.distinct,
        }),
        SetOpKind::Intersect | SetOpKind::Except => {
            // Fresh PhysicalSetOperation lowered from the logical SetOperation: no base
            // to copy, so every field is listed.
            PhysicalPlan::SetOperation(PhysicalSetOperation {
                left: Box::new(left),
                right: Box::new(right),
                kind: set_op.kind,
                distinct: set_op.distinct,
            })
        }
    }
}
