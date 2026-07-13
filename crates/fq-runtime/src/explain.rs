//! Minimal EXPLAIN support for the runtime.
//!
//! fq-parse does not emit an `Explain` logical node (a leading EXPLAIN raises
//! `ParseError::Unsupported`), and there is no costed EXPLAIN document builder
//! (`_PlanFormatter` / estimated-cost annotation).
//! So the runtime recognizes a leading `EXPLAIN` keyword, plans the inner
//! statement through the normal pipeline, and renders a textual PHYSICAL plan
//! tree WITHOUT executing it. The output is honest - a real plan, no fabricated
//! result rows - and clearly labeled as a structural (uncosted) description.

use std::collections::HashMap;

use fq_physical::steps::planned_injections;
use fq_physical::{render_remote_set_op, render_scan_sql, StepError};
use fq_plan::physical::PhysicalPlan;

/// If `sql` begins with the `EXPLAIN` keyword (case-insensitive, followed by
/// whitespace), return the inner statement text; otherwise `None`. The caller
/// plans and describes the inner statement instead of executing it.
pub fn strip_explain(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start();
    let mut chars = trimmed.char_indices();
    for expected in "explain".chars() {
        match chars.next() {
            Some((_, actual)) if actual.eq_ignore_ascii_case(&expected) => {}
            _ => return None,
        }
    }
    // The keyword must be followed by whitespace, not be a prefix of an identifier.
    match chars.next() {
        Some((index, next)) if next.is_whitespace() => Some(trimmed[index..].trim_start()),
        _ => None,
    }
}

/// Render a physical plan as an indented tree of node labels, one line per node.
/// Used as the EXPLAIN result: one `plan` column, one row per line. A source-leaf
/// scan's label carries its effective pushed SQL (rendered from the same renderer
/// that emits the execution SQL), so a render failure propagates rather than
/// silently mislabeling the row.
pub fn describe(plan: &PhysicalPlan) -> Result<Vec<String>, StepError> {
    let mut lines = Vec::new();
    // The SAME winners pre-pass the step emitter runs: scans it will reduce get
    // a `+inject(<column>)` tag, so the semi-join reduction is visible in the
    // plan dump without executing anything.
    let injections = planned_injections(plan);
    describe_into(plan, 0, &injections, &mut lines)?;
    Ok(lines)
}

/// Append one `depth`-indented label for `plan`, then recurse into its children.
fn describe_into(
    plan: &PhysicalPlan,
    depth: usize,
    injections: &HashMap<*const PhysicalPlan, String>,
    lines: &mut Vec<String>,
) -> Result<(), StepError> {
    let indent = "  ".repeat(depth);
    let mut label = node_label(plan)?;
    if let Some(column) = injections.get(&std::ptr::from_ref(plan)) {
        label = tag_injected(&label, column);
    }
    lines.push(format!("{indent}{label}"));
    // A same-source set operation renders its whole (possibly nested) combined SQL
    // in ONE round trip; its branch children are folded into that SQL and never
    // executed as separate reads, so the EXPLAIN treats it as a leaf.
    if matches!(plan, PhysicalPlan::RemoteSetOp(_)) {
        return Ok(());
    }
    for child in plan.children() {
        describe_into(child, depth + 1, injections, lines)?;
    }
    Ok(())
}

/// Insert a `+inject(<column>)` tag into a node label. The tag sits BEFORE the
/// ` :: ` SQL separator so consumers that re-parse the rendered SQL after `::`
/// never see the tag inside the SQL text.
fn tag_injected(label: &str, column: &str) -> String {
    match label.split_once(" :: ") {
        Some((head, sql)) => format!("{head} +inject({column}) :: {sql}"),
        None => format!("{label} +inject({column})"),
    }
}

/// Collapse rendered SQL to a single line (newlines/tabs -> spaces, runs
/// squeezed) so one plan node stays on one EXPLAIN row.
fn one_line(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut prev_space = false;
    for ch in sql.chars() {
        let space = ch.is_whitespace();
        if space {
            if !prev_space {
                out.push(' ');
            }
        } else {
            out.push(ch);
        }
        prev_space = space;
    }
    out.trim().to_string()
}

/// A one-line label for a physical node: the variant name, plus the read target
/// for the source-leaf nodes. Exhaustive (no `_` arm) so a new node type is a
/// compile error here, never a silently unlabeled row. A source-leaf `Scan`
/// renders its effective pushed SQL through the SAME renderer the execution plane
/// uses (`render_scan_sql`), so the plan dump shows the exact SELECT sent to the
/// source with every folded filter/aggregate/group-by/order/limit/DISTINCT.
fn node_label(plan: &PhysicalPlan) -> Result<String, StepError> {
    let label = match plan {
        PhysicalPlan::Scan(node) => {
            format!(
                "Scan [{}] :: {}",
                node.datasource,
                one_line(&render_scan_sql(node)?)
            )
        }
        PhysicalPlan::RemoteQuery(node) => {
            format!(
                "RemoteQuery [{}] :: {}",
                node.datasource,
                one_line(&node.sql)
            )
        }
        PhysicalPlan::Gather(node) => {
            format!("Gather [{}] :: {}", node.datasource, one_line(&node.query))
        }
        PhysicalPlan::Cte(_) => "Cte".to_string(),
        PhysicalPlan::CteScan(_) => "CteScan".to_string(),
        PhysicalPlan::Shipment(node) => {
            format!("Shipment {} [{}]", node.table, node.datasource)
        }
        PhysicalPlan::AliasedRelation(_) => "AliasedRelation".to_string(),
        PhysicalPlan::CteMergeQuery(_) => "CteMergeQuery".to_string(),
        PhysicalPlan::Projection(_) => "Projection".to_string(),
        PhysicalPlan::Window(_) => "Window".to_string(),
        PhysicalPlan::Filter(_) => "Filter".to_string(),
        // The join type is part of the label: a SEMI/ANTI vs INNER distinction
        // is plan shape, and tests pin it through the plan dump.
        PhysicalPlan::HashJoin(node) => format!("HashJoin [{}]", node.join_type.value()),
        PhysicalPlan::RemoteJoin(_) => "RemoteJoin".to_string(),
        PhysicalPlan::NestedLoopJoin(_) => "NestedLoopJoin".to_string(),
        PhysicalPlan::HashAggregate(_) => "HashAggregate".to_string(),
        PhysicalPlan::Sort(_) => "Sort".to_string(),
        PhysicalPlan::Limit(_) => "Limit".to_string(),
        PhysicalPlan::Values(_) => "Values".to_string(),
        PhysicalPlan::Union(_) => "Union".to_string(),
        PhysicalPlan::RemoteSetOp(node) => {
            format!(
                "RemoteSetOp [{}] :: {}",
                node.datasource,
                one_line(&render_remote_set_op(node)?)
            )
        }
        PhysicalPlan::SetOperation(_) => "SetOperation".to_string(),
        PhysicalPlan::SingleRowGuard(_) => "SingleRowGuard".to_string(),
        PhysicalPlan::GroupedLimit(_) => "GroupedLimit".to_string(),
        PhysicalPlan::LateralJoin(_) => "LateralJoin".to_string(),
        PhysicalPlan::Explain(_) => "Explain".to_string(),
    };
    Ok(label)
}
