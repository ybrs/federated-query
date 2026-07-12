//! Push the union (OR) of every consumer's filter into a shared CTE/Union body.
//! Ports `optimizer/cte_union_filter.py`.
//!
//! A multi-referenced CTE computes rows for EVERY consumer while each consumer
//! keeps only a slice. Ordinary predicate pushdown must treat a shared CTE as
//! opaque - pushing one consumer's filter would starve the others - but the OR of
//! ALL consumers' filters is exact: every consumer keeps its own filter, and any
//! row some consumer needs still passes the union. The pushed filter references
//! only GROUPING columns of the body's top aggregate (a filter constant within a
//! group drops whole groups, never partial rows of a kept group), landing directly
//! under the aggregate where ordinary pushdown then sinks it to the base scans.
//!
//! This is a PURE performance rewrite: correctness is preserved by DECLINING. Every
//! guard returns the plan unchanged; nothing here ever raises.

use std::collections::HashMap;
use std::convert::Infallible;

use fq_decorrelate::helpers::replace_column_refs;
use fq_plan::expr::{column_refs, combine_and, combine_or, split_conjuncts, ColumnRef, Expr};
use fq_plan::logical::{
    Aggregate, Cte, CteRef, Filter, LogicalPlan, Projection, SetOpKind, SetOperation, Union,
};

use crate::error::OptimizeError;

use super::OptimizationRule;

/// Push the OR of all consumer filters into each shared CTE body.
pub struct CTEUnionFilterPushdown;

impl OptimizationRule for CTEUnionFilterPushdown {
    /// This rule's identifier (used in the `validate_scope` stage label).
    fn name(&self) -> &'static str {
        "CTEUnionFilterPushdown"
    }

    /// Attempt the union-filter push at every CTE wrapper. Infallible in substance
    /// (correctness is preserved by declining), so it always returns `Ok`.
    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        Ok(apply_rec(plan))
    }
}

/// Recurse the plan post-order (children rebuilt first), attempting the push at
/// each rebuilt CTE wrapper.
fn apply_rec(node: LogicalPlan) -> LogicalPlan {
    let rebuilt = node
        .try_map_children(|child| Ok::<_, Infallible>(apply_rec(child)))
        .unwrap_or_else(|never| match never {});
    match rebuilt {
        LogicalPlan::Cte(cte) => push_union_filter(cte),
        other => other,
    }
}

/// The CTE with the consumers' union filter in its body, or unchanged when any
/// guard declines: a recursive body, a bare unfiltered consumer, a shadowed name,
/// an untranslatable filter column, or a non-aggregate body top.
fn push_union_filter(cte: Cte) -> LogicalPlan {
    if cte.recursive {
        return LogicalPlan::Cte(cte);
    }
    let Some(consumers) = consumer_filters(&cte.child, &cte.name) else {
        return LogicalPlan::Cte(cte);
    };
    let Some(rebuilt) = pushed_body(&cte.cte_plan, &consumers) else {
        return LogicalPlan::Cte(cte);
    };
    if rebuilt == *cte.cte_plan {
        return LogicalPlan::Cte(cte);
    }
    LogicalPlan::Cte(Cte {
        cte_plan: Box::new(rebuilt),
        ..cte
    })
}

/// The body with the union filter inserted at every branch sink, or None when any
/// branch declines (all-or-nothing keeps the plan unchanged rather than
/// half-filtered). A row filter on output columns commutes with UNION (dedup
/// included, since it drops whole duplicate groups), so union shapes recurse per
/// branch, translated per branch.
fn pushed_body(body: &LogicalPlan, consumers: &[(Filter, CteRef)]) -> Option<LogicalPlan> {
    match body {
        LogicalPlan::Union(union) => pushed_union(union, consumers),
        LogicalPlan::SetOperation(set_op) if set_op.kind == SetOpKind::Union => {
            pushed_set_operation(set_op, consumers)
        }
        other => pushed_branch(other, consumers),
    }
}

/// An n-ary UNION with the filter pushed into every input, or None when any input
/// declines.
fn pushed_union(union: &Union, consumers: &[(Filter, CteRef)]) -> Option<LogicalPlan> {
    let mut rebuilt = Vec::with_capacity(union.inputs.len());
    for branch in &union.inputs {
        rebuilt.push(pushed_body(branch, consumers)?);
    }
    // The source union is borrowed (&Union), so we cannot mutate it or `..`-copy it;
    // build a fresh Union whose only new field is the rebuilt branches, copying the
    // scalar `distinct`. {inputs, distinct} is the complete Union field set.
    Some(LogicalPlan::Union(Union {
        inputs: rebuilt,
        distinct: union.distinct,
    }))
}

/// A binary UNION set operation with the filter pushed into both sides, or None
/// when either declines.
fn pushed_set_operation(
    set_op: &SetOperation,
    consumers: &[(Filter, CteRef)],
) -> Option<LogicalPlan> {
    let left = pushed_body(&set_op.left, consumers)?;
    let right = pushed_body(&set_op.right, consumers)?;
    // The source set operation is borrowed (&SetOperation), so we cannot mutate or
    // `..`-copy it; build a fresh node with the rewritten branches, copying the
    // scalar kind/distinct. {left, right, kind, distinct} is the complete field set.
    Some(LogicalPlan::SetOperation(SetOperation {
        left: Box::new(left),
        right: Box::new(right),
        kind: set_op.kind,
        distinct: set_op.distinct,
    }))
}

/// One leaf branch with the union filter at its aggregate sink, or None (decline).
fn pushed_branch(branch: &LogicalPlan, consumers: &[(Filter, CteRef)]) -> Option<LogicalPlan> {
    let sink = aggregate_sink(branch)?;
    let predicate = union_predicate(consumers, &sink)?;
    if already_pushed(branch, &predicate) {
        return Some(branch.clone());
    }
    Some(insert_filter(&sink, predicate))
}

/// Every `(Filter, CteRef)` consumer of the named CTE, or None when a consumer is
/// unfiltered (it needs all rows) or a nested CTE shadows the name (references
/// would be ambiguous).
fn consumer_filters(child: &LogicalPlan, name: &str) -> Option<Vec<(Filter, CteRef)>> {
    let mut consumers = Vec::new();
    if !collect_consumers(child, None, name, &mut consumers) {
        return None;
    }
    if consumers.is_empty() {
        None
    } else {
        Some(consumers)
    }
}

/// Walk the child subtree collecting filtered consumers; returns false to DECLINE
/// the whole CTE on a shadowed name or a bare (non-Filter-parented) consumer.
fn collect_consumers(
    node: &LogicalPlan,
    parent: Option<&LogicalPlan>,
    name: &str,
    consumers: &mut Vec<(Filter, CteRef)>,
) -> bool {
    if let LogicalPlan::Cte(cte) = node {
        if cte.name == name {
            return false;
        }
    }
    if let LogicalPlan::CteRef(cte_ref) = node {
        if cte_ref.name == name {
            return record_consumer(parent, cte_ref, consumers);
        }
    }
    node.children()
        .iter()
        .all(|child| collect_consumers(child, Some(node), name, consumers))
}

/// Record one matching CteRef, requiring its parent be a Filter; false otherwise
/// (a consumer that needs all rows - pushing would starve it).
fn record_consumer(
    parent: Option<&LogicalPlan>,
    cte_ref: &CteRef,
    consumers: &mut Vec<(Filter, CteRef)>,
) -> bool {
    let Some(LogicalPlan::Filter(filter)) = parent else {
        return false;
    };
    consumers.push((filter.clone(), cte_ref.clone()));
    true
}

/// The projection chain and top aggregate forming the body's translatable sink.
struct Sink {
    chain: Vec<Projection>,
    aggregate: Aggregate,
}

/// The body's `(projection chain, aggregate)` sink, or None. DISTINCT ON
/// projections decline (they pick a survivor per group, so removing input rows
/// changes kept rows); grouping-set aggregates decline (a filter under ROLLUP
/// changes super-aggregate rows a consumer may read through IS NULL).
fn aggregate_sink(body: &LogicalPlan) -> Option<Sink> {
    let mut chain = Vec::new();
    let mut node = body;
    while let LogicalPlan::Projection(projection) = node {
        if projection.distinct_on.is_some() {
            break;
        }
        chain.push(projection.clone());
        node = &projection.input;
    }
    let LogicalPlan::Aggregate(aggregate) = node else {
        return None;
    };
    if aggregate.grouping_sets.is_some()
        || aggregate.output_names.len() != aggregate.aggregates.len()
    {
        return None;
    }
    Some(Sink {
        chain,
        aggregate: aggregate.clone(),
    })
}

/// OR of every consumer's translated filter, or None when any consumer contributes
/// nothing pushable (its arm would be TRUE, making the whole union a no-op).
fn union_predicate(consumers: &[(Filter, CteRef)], sink: &Sink) -> Option<Expr> {
    let mut translated = Vec::with_capacity(consumers.len());
    for (filter, cte_ref) in consumers {
        translated.push(consumer_predicate(filter, cte_ref, sink)?);
    }
    combine_or(translated)
}

/// AND of this consumer's pushable, TRANSLATED conjuncts, or None when none
/// survive. A conjunct that does not translate (it references an aggregate output,
/// e.g. q04's year_total > 0) is simply DROPPED - the arm gets weaker (more rows
/// through the union), which is safe; the consumer's own filter still applies.
fn consumer_predicate(filter: &Filter, cte_ref: &CteRef, sink: &Sink) -> Option<Expr> {
    let exposed = cte_ref.alias.as_deref().unwrap_or(&cte_ref.name);
    let mut kept = Vec::new();
    for conjunct in split_conjuncts(&filter.predicate) {
        if !pushable(conjunct, exposed) {
            continue;
        }
        if let Some(rewritten) = translate_to_group_columns(conjunct, cte_ref, sink) {
            kept.push(rewritten);
        }
    }
    combine_and(kept)
}

/// Whether an expression is a deterministic shape over ONLY the given consumer
/// alias, safe to duplicate into the CTE body.
fn pushable(expr: &Expr, alias: &str) -> bool {
    if let Expr::Column(column) = expr {
        return column.table.as_deref() == Some(alias);
    }
    if !is_pushable_shape(expr) {
        return false;
    }
    expr.children().iter().all(|child| pushable(child, alias))
}

/// The allowlist of deterministic expression shapes safe to duplicate into a CTE
/// body; anything else (function calls, CASE, casts, subqueries) is left in place.
fn is_pushable_shape(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Column(_)
            | Expr::Literal { .. }
            | Expr::InList { .. }
            | Expr::Between { .. }
    )
}

/// The conjunct rewritten onto the aggregate's grouping columns, or None when any
/// referenced output is not a plain grouping column (or a constant tag).
fn translate_to_group_columns(conjunct: &Expr, cte_ref: &CteRef, sink: &Sink) -> Option<Expr> {
    let mut mapping: HashMap<(Option<String>, String), Expr> = HashMap::new();
    for (table, column) in referenced_pairs(conjunct) {
        let replacement = group_column_for(&column, cte_ref, sink)?;
        mapping.insert((table, column), replacement);
    }
    Some(replace_column_refs(conjunct.clone(), &|c: &ColumnRef| {
        mapping.get(&(c.table.clone(), c.column.clone())).cloned()
    }))
}

/// The distinct (qualifier, column) pairs referenced by an expression, in first-
/// occurrence order.
fn referenced_pairs(expr: &Expr) -> Vec<(Option<String>, String)> {
    let mut pairs = Vec::new();
    for reference in column_refs(expr) {
        let pair = (reference.table.clone(), reference.column.clone());
        if !pairs.contains(&pair) {
            pairs.push(pair);
        }
    }
    pairs
}

/// The aggregate's grouping ColumnRef (or constant Literal) producing the CTE
/// output named `column`, chased by POSITION through the passthrough projections,
/// or None when it resolves to a computed or aggregate output.
fn group_column_for(column: &str, cte_ref: &CteRef, sink: &Sink) -> Option<Expr> {
    let outputs = cte_ref.output_names.as_deref().unwrap_or(&[]);
    let index = outputs.iter().position(|name| name == column)?;
    let position = chase_position(index, sink)?;
    let replacement = sink.aggregate.aggregates.get(position)?;
    match replacement {
        // A constant output column (q04's sale_type channel tag) holds for the
        // whole branch, so substituting it lets the engine fold the comparison.
        Expr::Literal { .. } => Some(replacement.clone()),
        Expr::Column(_) if sink.aggregate.group_by.contains(replacement) => {
            Some(replacement.clone())
        }
        _ => None,
    }
}

/// Follow an output position down the projection chain to the aggregate's output
/// position, or None when a projection computes it. Each layer's reference resolves
/// against the NAMES OF ITS OWN INPUT (the next chain element, or the aggregate).
fn chase_position(mut position: usize, sink: &Sink) -> Option<usize> {
    for (index, projection) in sink.chain.iter().enumerate() {
        let Expr::Column(column) = projection.expressions.get(position)? else {
            return None;
        };
        let below = if index + 1 < sink.chain.len() {
            &sink.chain[index + 1].aliases
        } else {
            &sink.aggregate.output_names
        };
        position = below.iter().position(|name| *name == column.column)?;
    }
    Some(position)
}

/// Whether an equal predicate already sits in the body (as a Filter conjunct or a
/// scan filter) - the pushdown pass moves the inserted filter, so idempotence must
/// look everywhere, not just at the sink.
fn already_pushed(body: &LogicalPlan, predicate: &Expr) -> bool {
    if node_embeds_predicate(body, predicate) {
        return true;
    }
    body.children()
        .iter()
        .any(|child| already_pushed(child, predicate))
}

/// Whether one node's own filter conjuncts already include the predicate.
fn node_embeds_predicate(node: &LogicalPlan, predicate: &Expr) -> bool {
    let filters = match node {
        LogicalPlan::Filter(filter) => Some(&filter.predicate),
        LogicalPlan::Scan(scan) => scan.filters.as_ref(),
        _ => None,
    };
    match filters {
        Some(expr) => split_conjuncts(expr).iter().any(|c| **c == *predicate),
        None => false,
    }
}

/// The body rebuilt with the union filter directly under the aggregate (where
/// ordinary pushdown then sinks it to the base scans).
fn insert_filter(sink: &Sink, predicate: Expr) -> LogicalPlan {
    // The sink is borrowed (&Sink), so clone the aggregate's input subtree for the
    // freshly inserted Filter to own (we cannot move out of a borrow).
    let aggregate_input = sink.aggregate.input.clone();
    // A fresh Filter inserted directly under the aggregate; its {input, predicate}
    // (the cloned input and the pushed predicate) is the complete Filter field set.
    let pushed = LogicalPlan::Filter(Filter {
        input: aggregate_input,
        predicate,
    });
    let mut aggregate = sink.aggregate.clone();
    aggregate.input = Box::new(pushed);
    let mut node = LogicalPlan::Aggregate(aggregate);
    for projection in sink.chain.iter().rev() {
        let mut projection = projection.clone();
        projection.input = Box::new(node);
        node = LogicalPlan::Projection(projection);
    }
    node
}

#[cfg(test)]
mod tests;
