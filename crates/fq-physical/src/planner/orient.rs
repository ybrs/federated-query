//! Orientation size helpers. Ports `estimate_defaults.orientation_rows` /
//! `larger_estimated_side` (VESTIGIAL in fq-optimize until this, their first
//! consumer). Shared by hash-join build-side choice and the semi-join reducer so
//! they never disagree about which side is bigger.

use fq_plan::physical::PhysicalPlan;

/// Names a join side without borrowing the plan (avoids the pointer-identity
/// compares Python did with `is`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Left,
    Right,
}

/// A physical node's size for orientation: a remote query's real OUTPUT estimate
/// when the optimizer computed one (its `estimated_rows` is only the max-base
/// floor), else the node's threaded estimate. `None` for nodes carrying none.
pub fn orientation_rows(node: &PhysicalPlan) -> Option<u64> {
    match node {
        PhysicalPlan::RemoteQuery(query) => query.output_estimated_rows.or(query.estimated_rows),
        PhysicalPlan::Scan(scan) => scan.estimated_rows,
        PhysicalPlan::HashJoin(join) => join.estimated_rows,
        PhysicalPlan::NestedLoopJoin(join) => join.estimated_rows,
        // Every other physical node carries no row estimate -> genuinely UNKNOWN,
        // which MUST abstain (a size-lookup default, not a node-handling gap).
        _ => None,
    }
}

/// Which side has the greater orientation size, or `None` when either is unknown
/// or the two tie (only a strict, both-known inequality orients).
pub fn larger_estimated_side(left: &PhysicalPlan, right: &PhysicalPlan) -> Option<Side> {
    match (orientation_rows(left), orientation_rows(right)) {
        (Some(l), Some(r)) if l != r => Some(if r > l { Side::Right } else { Side::Left }),
        _ => None,
    }
}
