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
use std::sync::Arc;

use fq_physical::steps::planned_injections;
use fq_physical::{render_remote_set_op, render_scan_sql, StepError};
use fq_common::CostConfig;
use fq_plan::physical::{BuildSide, PhysicalPlan};
use fq_plan::Expr;

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
pub fn describe(plan: &PhysicalPlan, cost: &CostConfig) -> Result<Vec<String>, StepError> {
    let mut lines = Vec::new();
    // The SAME winners pre-pass the step emitter runs: scans it will reduce get
    // a `+inject(<column>)` tag, so the semi-join reduction is visible in the
    // plan dump without executing anything.
    let injections = planned_injections(plan);
    // Each distinct CTE producer allocation gets one small id, printed on its
    // `Cte` producer line AND on every `CteScan` that reads it, so that a body
    // materialized ONCE and shared by many references is visible in the dump.
    let producer_ids = cte_producer_ids(plan);
    describe_into(plan, 0, &injections, &producer_ids, cost, &mut lines)?;
    Ok(lines)
}

/// Assign each distinct CTE producer (by allocation address) a small sequential
/// id. A `CteScan` keys on its shared producer's address, so two references to
/// one materialized body resolve to the SAME id.
fn cte_producer_ids(plan: &PhysicalPlan) -> HashMap<*const PhysicalPlan, usize> {
    let mut ids = HashMap::new();
    assign_producer_ids(plan, &mut ids);
    ids
}

/// Walk the tree registering every CTE producer address in first-seen order.
fn assign_producer_ids(plan: &PhysicalPlan, ids: &mut HashMap<*const PhysicalPlan, usize>) {
    if let PhysicalPlan::CteScan(node) = plan {
        let key = Arc::as_ptr(&node.producer);
        let next = ids.len();
        ids.entry(key).or_insert(next);
    }
    for child in plan.children() {
        assign_producer_ids(child, ids);
    }
}

/// Append one `depth`-indented label for `plan`, then recurse into its children.
fn describe_into(
    plan: &PhysicalPlan,
    depth: usize,
    injections: &HashMap<*const PhysicalPlan, String>,
    producer_ids: &HashMap<*const PhysicalPlan, usize>,
    cost: &CostConfig,
    lines: &mut Vec<String>,
) -> Result<(), StepError> {
    let indent = "  ".repeat(depth);
    let mut label = node_label(plan, producer_ids)?;
    if let Some(column) = injections.get(&std::ptr::from_ref(plan)) {
        label = tag_injected(&label, column);
    }
    lines.push(format!("{indent}{label}{}", annotations(plan, cost)));
    // A same-source set operation renders its whole (possibly nested) combined SQL
    // in ONE round trip; its branch children are folded into that SQL and never
    // executed as separate reads, so the EXPLAIN treats it as a leaf.
    if matches!(plan, PhysicalPlan::RemoteSetOp(_)) {
        return Ok(());
    }
    for child in plan.children() {
        describe_into(child, depth + 1, injections, producer_ids, cost, lines)?;
    }
    Ok(())
}

/// Everything appended after a node's label: its network cost, its row estimate,
/// and ONE merged provenance tag naming every default those two rested on. The
/// tag is merged deliberately - two `stats=defaulted[...]` groups on one line
/// would leave a reader guessing which number each belonged to.
fn annotations(plan: &PhysicalPlan, cost: &CostConfig) -> String {
    let (cost_text, cost_defaults) = network_cost(plan, cost);
    let (rows_text, rows_defaults) = estimate_annotation(plan);
    let mut defaults: Vec<&str> = rows_defaults;
    defaults.extend(cost_defaults);
    let mut suffix = format!("{cost_text}{rows_text}");
    if !defaults.is_empty() {
        use std::fmt::Write;
        write!(suffix, " stats=defaulted[{}]", defaults.join(", "))
            .expect("writing to a String never fails");
    }
    suffix
}

/// The network cost of a node that crosses the wire, in milliseconds - one round
/// trip plus the transfer of its estimated output - with the default inputs the
/// number rested on. The formula is `network_rtt_ms + rows * row_bytes *
/// network_byte_cost`, both weights read from the live `cost` settings.
///
/// Only nodes that TALK TO A SOURCE are priced. Coordinator-local work (join,
/// aggregate, sort) is deliberately unpriced: this is a federated engine, the
/// wire dominates, and one number mixing the two would hide which is which.
///
/// THE ROUND-TRIP TERM IS MEASURED; THE BYTE TERM IS NOT. Row width comes from
/// `ColumnStatistics::avg_width`, which every source reports as a fixed
/// placeholder ("sources expose no cheap width"), so any cost carrying a byte
/// term is tagged `stats=defaulted[avg_width]`. It is real arithmetic over a
/// placeholder input, and the tag says so rather than letting the number pass as
/// a measurement. Real widths need per-source work (pg_stats, parquet column
/// sizes) and would drop the tag without changing the formula.
fn network_cost<'a>(plan: &'a PhysicalPlan, cost: &CostConfig) -> (String, Vec<&'a str>) {
    let rows = match plan {
        PhysicalPlan::Scan(node) => node.estimated_rows,
        PhysicalPlan::RemoteQuery(node) => node.output_estimated_rows.or(node.estimated_rows),
        _ => return (String::new(), Vec::new()),
    };
    let Some(rows) = rows else {
        // The node crosses the wire, so it HAS a cost; the row estimate needed to
        // price the transfer is missing. Reporting the round trip alone would
        // understate it silently, so the whole number is withheld.
        return (
            format!(" cost=?(rtt {:.1}ms)", cost.network_rtt_ms),
            Vec::new(),
        );
    };
    let transfer = rows as f64 * placeholder_row_bytes(plan) * cost.network_byte_cost;
    (
        format!(" cost={:.2}ms", cost.network_rtt_ms + transfer),
        vec!["avg_width"],
    )
}

/// The per-row byte estimate: the node's projected column count times the
/// placeholder width every source reports. Named `placeholder` because that is
/// what it is - see `network_cost_suffix`.
fn placeholder_row_bytes(plan: &PhysicalPlan) -> f64 {
    const PLACEHOLDER_AVG_WIDTH: f64 = 8.0;
    let columns = match plan {
        PhysicalPlan::Scan(node) => node.columns.len(),
        PhysicalPlan::RemoteQuery(node) => node.output_names.len(),
        _ => 0,
    };
    columns as f64 * PLACEHOLDER_AVG_WIDTH
}

/// Append the runtime IN filter a probe scan will receive, as a real predicate
/// on the rendered SQL: `... WHERE col IN (...)`.
///
/// The value list stays `(...)`. The build-side keys exist only once the join
/// has run, and fetching a sample of them would make EXPLAIN read data - which
/// it does not do, and must not start doing. What matters for reading a plan is
/// that the reduction fires and WHICH column it narrows; the values do not
/// change that. Only single-column keys are annotated, because a composite key
/// does not render as one IN list.
fn annotate_dynamic_filter(scan: &fq_plan::physical::PhysicalScan, sql: String) -> String {
    let Some(keys) = scan.dynamic_filter_keys.as_ref() else {
        return sql;
    };
    let [key] = keys.as_slice() else {
        return sql;
    };
    let predicate = format!("{} IN (...)", show_expr(key));
    if sql.contains(" WHERE ") {
        format!("{sql} AND {predicate}")
    } else {
        format!("{sql} WHERE {predicate}")
    }
}

/// A join's equality keys and which side is built, e.g.
/// ` keys=(o.id = l.orderkey) build=Right`. The keys decide whether the join is
/// pushable and the build side decides which input is materialized, so both are
/// read when a join is being tuned. Rendered with the emitter's own expression
/// renderer, so the dump cannot drift from the SQL the engine builds.
fn join_details(left_keys: &[Expr], right_keys: &[Expr], build: BuildSide) -> String {
    let mut pairs = Vec::with_capacity(left_keys.len());
    for (left, right) in left_keys.iter().zip(right_keys) {
        pairs.push(format!("{} = {}", show_expr(left), show_expr(right)));
    }
    let keys = if pairs.is_empty() {
        String::new()
    } else {
        format!(" keys=({})", pairs.join(", "))
    };
    format!("{keys} build={build:?}")
}

/// An aggregate's grouping expressions, e.g. ` group=[c.segment]`. A grouped
/// aggregate and a whole-input one are different plans, and the key list is what
/// the group-count estimate is drawn from.
fn group_details(group_by: &[Expr]) -> String {
    if group_by.is_empty() {
        return " group=[] (whole input)".to_string();
    }
    let keys: Vec<String> = group_by.iter().map(show_expr).collect();
    format!(" group=[{}]", keys.join(", "))
}

/// One expression as canonical text. A renderer failure falls back to the
/// expression's node name rather than dropping the detail: EXPLAIN describes a
/// plan and must not claim a join has no keys because one of them would not
/// render.
fn show_expr(expr: &Expr) -> String {
    fq_emit::render_expr(expr, &fq_emit::CANONICAL_SOURCE_RESOLVER)
        .unwrap_or_else(|_| "<unrenderable>".to_string())
}

/// The cardinality annotation for one node: ` rows=<n>` when the planner
/// estimated it, ` rows=?` when the node carries an estimate slot the planner
/// could not fill, and nothing at all for a node that has no slot.
///
/// `rows=?` is deliberate. An unfilled estimate is a MEASUREMENT THAT DOES NOT
/// EXIST, and printing nothing there is indistinguishable from a node that was
/// never meant to carry one - so a plan read for tuning would silently mistake
/// a gap for a design. `stats=defaulted[...]` follows when the number rests on
/// named default constants rather than real statistics: the row count may be
/// present and still be a guess, and a gate reading it deserves to know which.
fn estimate_annotation(plan: &PhysicalPlan) -> (String, Vec<&str>) {
    let (rows, defaults) = match plan {
        PhysicalPlan::Scan(node) => (Some(node.estimated_rows), None),
        PhysicalPlan::HashJoin(node) => (
            Some(node.estimated_rows),
            node.estimate_defaults.as_deref(),
        ),
        PhysicalPlan::NestedLoopJoin(node) => (
            Some(node.estimated_rows),
            node.estimate_defaults.as_deref(),
        ),
        PhysicalPlan::RemoteQuery(node) => (Some(node.estimated_rows), None),
        _ => (None, None),
    };
    let Some(estimate) = rows else {
        return (String::new(), Vec::new());
    };
    let text = match estimate {
        Some(count) => format!(" rows={count}"),
        None => " rows=?".to_string(),
    };
    let named = defaults.map_or_else(Vec::new, |names| {
        names.iter().map(String::as_str).collect()
    });
    (text, named)
}

/// The CTE name carried by a `CteScan`'s producer. The producer is always a
/// `PhysicalPlan::Cte` (the planner builds it that way); any other node is a
/// violated invariant surfaced loudly rather than mislabeled.
fn producer_cte_name(producer: &PhysicalPlan) -> &str {
    match producer {
        PhysicalPlan::Cte(node) => &node.name,
        _ => unreachable!("a CteScan producer must be a PhysicalPlan::Cte"),
    }
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
fn node_label(
    plan: &PhysicalPlan,
    producer_ids: &HashMap<*const PhysicalPlan, usize>,
) -> Result<String, StepError> {
    let label = match plan {
        // A scan of the materialized-view store is labeled `MaterializedScan`
        // with the VIEW name, so a plan over a materialized view is never
        // indistinguishable from a source read; every other scan names its
        // datasource. Both carry the exact pushed SQL.
        PhysicalPlan::Scan(node) => {
            let head = match node.datasource_kind {
                fq_plan::physical::DatasourceKind::Materialized => {
                    format!("MaterializedScan [{}]", node.table_name)
                }
                fq_plan::physical::DatasourceKind::Postgres
                | fq_plan::physical::DatasourceKind::DuckDb
                | fq_plan::physical::DatasourceKind::ClickHouse
                | fq_plan::physical::DatasourceKind::MySql
                | fq_plan::physical::DatasourceKind::Parquet => {
                    format!("Scan [{}]", node.datasource)
                }
            };
            let sql = one_line(&render_scan_sql(node)?);
            format!("{head} :: {}", annotate_dynamic_filter(node, sql))
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
        // The producer carries its CTE name and its shared-allocation id. When it
        // is reached as a CteScan child its address is the scan's producer key, so
        // the id here matches the id on every reading CteScan.
        PhysicalPlan::Cte(node) => {
            let id = producer_ids
                .get(&std::ptr::from_ref(plan))
                .copied()
                .unwrap_or(0);
            format!("Cte [{}] #{}", node.name, id)
        }
        // A reference names the CTE it reads and the id of the SHARED producer, so
        // two references over one materialized body print the same #id.
        PhysicalPlan::CteScan(node) => {
            let id = producer_ids
                .get(&Arc::as_ptr(&node.producer))
                .copied()
                .unwrap_or(0);
            format!("CteScan [{}] #{}", producer_cte_name(&node.producer), id)
        }
        PhysicalPlan::Shipment(node) => {
            format!("Shipment {} [{}]", node.table, node.datasource)
        }
        PhysicalPlan::AliasedRelation(_) => "AliasedRelation".to_string(),
        PhysicalPlan::CteMergeQuery(_) => "CteMergeQuery".to_string(),
        PhysicalPlan::TableFunction(node) => {
            format!(
                "EventAnalysis [{}({})]",
                node.call.function_name(),
                node.call.dataset()
            )
        }
        PhysicalPlan::Projection(_) => "Projection".to_string(),
        PhysicalPlan::Window(_) => "Window".to_string(),
        PhysicalPlan::Filter(_) => "Filter".to_string(),
        // The join type is part of the label: a SEMI/ANTI vs INNER distinction
        // is plan shape, and tests pin it through the plan dump.
        PhysicalPlan::HashJoin(node) => format!(
            "HashJoin [{}]{}",
            node.join_type.value(),
            join_details(&node.left_keys, &node.right_keys, node.build_side)
        ),
        PhysicalPlan::RemoteJoin(_) => "RemoteJoin".to_string(),
        PhysicalPlan::NestedLoopJoin(_) => "NestedLoopJoin".to_string(),
        PhysicalPlan::HashAggregate(node) => {
            format!("HashAggregate{}", group_details(&node.group_by))
        }
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
