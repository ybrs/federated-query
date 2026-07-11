//! Minimal EXPLAIN support for the runtime.
//!
//! fq-parse does not yet emit an `Explain` logical node (a leading EXPLAIN raises
//! `ParseError::Unsupported`), and the full EXPLAIN document builder
//! (`_PlanFormatter` / estimated-cost annotation) is deferred to a later crate.
//! So the runtime recognizes a leading `EXPLAIN` keyword, plans the inner
//! statement through the normal pipeline, and renders a textual PHYSICAL plan
//! tree WITHOUT executing it. The output is honest - a real plan, no fabricated
//! result rows - and clearly labeled as a structural (uncosted) description.

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
/// Used as the EXPLAIN result: one `plan` column, one row per line.
pub fn describe(plan: &PhysicalPlan) -> Vec<String> {
    let mut lines = Vec::new();
    describe_into(plan, 0, &mut lines);
    lines
}

/// Append one `depth`-indented label for `plan`, then recurse into its children.
fn describe_into(plan: &PhysicalPlan, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    lines.push(format!("{indent}{}", node_label(plan)));
    for child in plan.children() {
        describe_into(child, depth + 1, lines);
    }
}

/// A one-line label for a physical node: the variant name, plus the read target
/// for the two source-leaf nodes. Exhaustive (no `_` arm) so a new node type is a
/// compile error here, never a silently unlabeled row.
fn node_label(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Scan(node) => format!(
            "Scan {}.{}.{}",
            node.datasource, node.schema_name, node.table_name
        ),
        PhysicalPlan::RemoteQuery(node) => format!("RemoteQuery [{}]", node.datasource),
        PhysicalPlan::Gather(node) => format!("Gather [{}]", node.datasource),
        PhysicalPlan::Cte(_) => "Cte".to_string(),
        PhysicalPlan::CteScan(_) => "CteScan".to_string(),
        PhysicalPlan::Shipment(_) => "Shipment".to_string(),
        PhysicalPlan::AliasedRelation(_) => "AliasedRelation".to_string(),
        PhysicalPlan::CteMergeQuery(_) => "CteMergeQuery".to_string(),
        PhysicalPlan::Projection(_) => "Projection".to_string(),
        PhysicalPlan::Window(_) => "Window".to_string(),
        PhysicalPlan::Filter(_) => "Filter".to_string(),
        PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
        PhysicalPlan::RemoteJoin(_) => "RemoteJoin".to_string(),
        PhysicalPlan::NestedLoopJoin(_) => "NestedLoopJoin".to_string(),
        PhysicalPlan::HashAggregate(_) => "HashAggregate".to_string(),
        PhysicalPlan::Sort(_) => "Sort".to_string(),
        PhysicalPlan::Limit(_) => "Limit".to_string(),
        PhysicalPlan::Values(_) => "Values".to_string(),
        PhysicalPlan::Union(_) => "Union".to_string(),
        PhysicalPlan::RemoteSetOp(_) => "RemoteSetOp".to_string(),
        PhysicalPlan::SetOperation(_) => "SetOperation".to_string(),
        PhysicalPlan::SingleRowGuard(_) => "SingleRowGuard".to_string(),
        PhysicalPlan::GroupedLimit(_) => "GroupedLimit".to_string(),
        PhysicalPlan::LateralJoin(_) => "LateralJoin".to_string(),
        PhysicalPlan::Explain(_) => "Explain".to_string(),
    }
}
