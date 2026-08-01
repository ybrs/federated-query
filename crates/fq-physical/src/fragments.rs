//! The SQL this engine hands to a source, collected from a physical plan.
//!
//! A pushed fragment is the point where the engine stops executing and asks a
//! foreign executor for an answer: the subtree is re-rendered as text and
//! whatever comes back is trusted. Collecting the fragments makes that boundary
//! inspectable. Each one carries both the text actually sent and the
//! dialect-neutral form of the same query, so a reference engine can run the
//! canonical form over the same data and be compared against what the source
//! returned.
//!
//! Collection is a pure read of the plan: it renders nothing the planner did not
//! already render, reads no data, and contacts no source.

use fq_plan::physical::{PhysicalRemoteQuery, PhysicalRemoteSetOp, PhysicalScan};
use fq_plan::PhysicalPlan;

use crate::steps::{
    render_remote_set_op, render_remote_set_op_canonical, render_scan_canonical, render_scan_sql,
    StepError,
};

/// One query handed to one source in one round trip.
#[derive(Debug, Clone, PartialEq)]
pub struct PushdownFragment {
    /// The datasource this text is sent to, by catalog name.
    pub datasource: String,
    /// The query in canonical Postgres, before transpiling to the source's
    /// dialect. Dialect-neutral, so a reference engine can execute it.
    pub canonical_sql: String,
    /// The text actually sent, in the source's own dialect.
    pub source_sql: String,
    /// The column names this fragment promises its parent, in order.
    pub output_names: Vec<String>,
    /// The source receives an IN-list of join keys appended at execution time, so
    /// `source_sql` is the base query rather than the exact text the source ran.
    /// Running the base form reproduces everything except that key filter.
    pub has_dynamic_filter: bool,
}

/// Collect every fragment a plan pushes to a source, in plan order.
///
/// A pushed set operation and a pushed same-source subtree are ONE fragment
/// each: their branches are part of the text that is sent, not separate round
/// trips, so collection does not descend into them.
pub fn collect_pushdown_fragments(plan: &PhysicalPlan) -> Result<Vec<PushdownFragment>, StepError> {
    let mut fragments = Vec::new();
    collect_into(plan, &mut fragments)?;
    Ok(fragments)
}

/// Record this node's fragment if it reads a source, else descend into children.
fn collect_into(plan: &PhysicalPlan, out: &mut Vec<PushdownFragment>) -> Result<(), StepError> {
    if let Some(fragment) = source_fragment(plan)? {
        out.push(fragment);
        return Ok(());
    }
    collect_children(plan, out)
}

/// Collect every child's fragments in order.
fn collect_children(plan: &PhysicalPlan, out: &mut Vec<PushdownFragment>) -> Result<(), StepError> {
    for child in plan.children() {
        collect_into(child, out)?;
    }
    Ok(())
}

/// The fragment for a node that reads a source, or None for any other node.
fn source_fragment(plan: &PhysicalPlan) -> Result<Option<PushdownFragment>, StepError> {
    match plan {
        PhysicalPlan::Scan(scan) => Ok(Some(scan_fragment(scan)?)),
        PhysicalPlan::RemoteQuery(remote) => Ok(Some(remote_query_fragment(remote))),
        PhysicalPlan::RemoteSetOp(set_op) => Ok(Some(set_op_fragment(set_op)?)),
        _ => Ok(None),
    }
}

/// The fragment for a plain source scan.
fn scan_fragment(scan: &PhysicalScan) -> Result<PushdownFragment, StepError> {
    Ok(PushdownFragment {
        datasource: scan.datasource.clone(),
        canonical_sql: render_scan_canonical(scan)?,
        source_sql: render_scan_sql(scan)?,
        output_names: scan_output_names(scan),
        has_dynamic_filter: scan.dynamic_filter_keys.is_some(),
    })
}

/// The fragment for a same-source subtree already rendered by the pushdown pass.
fn remote_query_fragment(remote: &PhysicalRemoteQuery) -> PushdownFragment {
    PushdownFragment {
        datasource: remote.datasource.clone(),
        canonical_sql: remote.canonical_sql.clone(),
        source_sql: remote.sql.clone(),
        output_names: remote.output_names.clone(),
        has_dynamic_filter: false,
    }
}

/// The fragment for a set operation kept entirely on one source. Its output names
/// are the left branch's, which is the schema a set operation carries.
fn set_op_fragment(set_op: &PhysicalRemoteSetOp) -> Result<PushdownFragment, StepError> {
    Ok(PushdownFragment {
        datasource: set_op.datasource.clone(),
        canonical_sql: render_remote_set_op_canonical(set_op)?,
        source_sql: render_remote_set_op(set_op)?,
        output_names: branch_output_names(&set_op.left),
        has_dynamic_filter: false,
    })
}

/// A scan's output column names: its explicit list when it computes one (an
/// aggregate or grouped scan), else the columns it reads.
fn scan_output_names(scan: &PhysicalScan) -> Vec<String> {
    match &scan.output_names {
        Some(names) => names.clone(),
        None => scan.columns.clone(),
    }
}

/// A set-operation branch's output names: a scan's own, a nested set operation's
/// left branch, and an empty list for any other branch shape.
fn branch_output_names(branch: &PhysicalPlan) -> Vec<String> {
    match branch {
        PhysicalPlan::Scan(scan) => scan_output_names(scan),
        PhysicalPlan::RemoteSetOp(nested) => branch_output_names(&nested.left),
        _ => Vec::new(),
    }
}
