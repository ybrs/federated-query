//! Referential closure: every qualified column a node references must be
//! produced by one of its children.
//!
//! This is the invariant the plan already relies on everywhere and states
//! nowhere. Side assignment, join-key orientation and merge-relation resolution
//! all assume that a `ColumnRef` naming a relation can be answered by the subtree
//! beneath it; when a pass manufactures a node that does not produce a column its
//! parent asks for, nothing notices until execution, where it surfaces as an
//! Arrow schema error naming neither the plan node nor the pushdown decision that
//! dropped it.
//!
//! The check is a pure walk of the plan: it reads no data, contacts no source and
//! renders nothing, so it costs the size of the plan and cannot threaten the
//! planning budget.
//!
//! WHAT IT IS STRICT ABOUT. A source node - a scan, a pushed same-source query,
//! a pushed set operation - exposes exactly the columns its own text produces,
//! and that exposure is exact. Every other node is treated as exposing whatever
//! its children expose PLUS the names it computes, which is permissive by
//! construction: a node that renames or aggregates can be modelled loosely
//! without weakening the sources, and the check still answers the question that
//! matters - was this reference producible by the subtree at all.
//!
//! UNQUALIFIED REFERENCES ARE NOT CHECKED. A reference with no relation is a
//! computed output carrying its own physical name, which is how an aggregate
//! output or an exposed merge column is written. The qualified case is the one
//! where the plan makes a claim about a relation, and it is the claim that can be
//! false.
//!
//! THE RULE THAT DRIVES `exposure`: a node that INTRODUCES A NAME for a relation
//! exposes that relation's output names under it. The nodes that introduce one
//! are a scan (its alias, else its table), an aliased relation, a CTE definition
//! and each reference to it, and a table function (its FROM alias). Every one of
//! them is modelled here; a new node that names a relation and is not added will
//! reject valid plans, because a reference to that name resolves to nothing.
//!
//! WHAT IS NOT MODELLED, so that the gaps are a list rather than a surprise: a
//! shipment names a temp table on the target source, but its island addresses
//! that name inside rendered SQL rather than through a `ColumnRef`, so there is
//! nothing here to resolve; a cross-source LATERAL carries its own left and base
//! names for the same reason. Both fall through to their children's exposure,
//! which is permissive. `Gather` and `CteMergeQuery` carry SQL text rather than
//! expressions, so this walk sees no references inside them at all.

use std::collections::BTreeSet;

use fq_plan::expr::{column_refs, ColumnRef, Expr};
use fq_plan::physical::{PhysicalPlan, PhysicalRemoteQuery, PhysicalRemoteSetOp, PhysicalScan};

use crate::error::PhysicalError;

/// A qualified column reference: the relation named, and the column asked of it.
type QualifiedColumn = (String, String);

/// Raise unless every qualified reference in `plan` is produced beneath it.
///
/// Walks the whole plan; the first unproducible reference raises, naming the node
/// that made it, the reference, and what the subtree beneath it does expose.
pub fn check_referential_closure(plan: &PhysicalPlan) -> Result<(), PhysicalError> {
    for child in plan.children() {
        check_referential_closure(child)?;
    }
    check_node(plan)
}

/// Check one node's own references against what its children expose.
fn check_node(plan: &PhysicalPlan) -> Result<(), PhysicalError> {
    let exposed = children_exposure(plan);
    for expression in own_expressions(plan) {
        check_expression(plan, expression, &exposed)?;
    }
    Ok(())
}

/// Check every qualified reference in one expression against the exposure.
fn check_expression(
    plan: &PhysicalPlan,
    expression: &Expr,
    exposed: &BTreeSet<QualifiedColumn>,
) -> Result<(), PhysicalError> {
    for reference in column_refs(expression) {
        let Some(key) = qualified_key(reference) else {
            continue;
        };
        if !exposed.contains(&key) {
            return Err(unproducible(plan, &key, exposed));
        }
    }
    Ok(())
}

/// The (relation, column) key of a reference, or None when it names no relation.
fn qualified_key(reference: &ColumnRef) -> Option<QualifiedColumn> {
    if reference.column == "*" {
        return None;
    }
    let table = reference.table.as_ref()?;
    if table.is_empty() {
        return None;
    }
    Some((table.clone(), reference.column.clone()))
}

/// Build the error naming the node, the reference, and the exposed columns.
fn unproducible(
    plan: &PhysicalPlan,
    key: &QualifiedColumn,
    exposed: &BTreeSet<QualifiedColumn>,
) -> PhysicalError {
    let mut names = Vec::with_capacity(exposed.len());
    for (table, column) in exposed {
        names.push(format!("{table}.{column}"));
    }
    PhysicalError::UnproducibleColumn {
        node: crate::steps::variant_name(plan).to_string(),
        reference: format!("{}.{}", key.0, key.1),
        exposes: names,
    }
}

/// Everything the children of `plan` expose, taken together.
fn children_exposure(plan: &PhysicalPlan) -> BTreeSet<QualifiedColumn> {
    let mut exposed = BTreeSet::new();
    for child in plan.children() {
        exposed.extend(exposure(child));
    }
    exposed
}

/// The qualified columns a node makes available to its parent.
///
/// A source node answers exactly; every other node answers with its children's
/// exposure, which is permissive for the nodes that rename or compute and keeps
/// the sources exact.
fn exposure(plan: &PhysicalPlan) -> BTreeSet<QualifiedColumn> {
    match plan {
        PhysicalPlan::Scan(scan) => scan_exposure(scan),
        PhysicalPlan::RemoteQuery(remote) => remote_query_exposure(remote),
        PhysicalPlan::RemoteSetOp(set_op) => remote_set_op_exposure(set_op),
        PhysicalPlan::AliasedRelation(aliased) => named_exposure(&aliased.alias, &aliased.input),
        PhysicalPlan::CteScan(scan) => cte_scan_exposure(scan),
        PhysicalPlan::Cte(cte) => cte_exposure(cte),
        PhysicalPlan::TableFunction(call) => table_function_exposure(call),
        other => children_exposure(other),
    }
}

/// A scan exposes its read columns under its alias, or under its table name when
/// it carries no alias.
fn scan_exposure(scan: &PhysicalScan) -> BTreeSet<QualifiedColumn> {
    let qualifier = scan
        .alias
        .clone()
        .unwrap_or_else(|| scan.table_name.clone());
    let mut exposed = BTreeSet::new();
    for column in &scan.columns {
        exposed.insert((qualifier.clone(), column.clone()));
    }
    exposed
}

/// A pushed same-source query exposes exactly the references its column-alias map
/// answers. That map is what the merge path resolves through, so a reference
/// absent from it is one the island's SELECT list does not produce.
fn remote_query_exposure(remote: &PhysicalRemoteQuery) -> BTreeSet<QualifiedColumn> {
    let mut exposed = BTreeSet::new();
    for (table, column) in remote.column_alias_map.keys() {
        if let Some(name) = table {
            exposed.insert((name.clone(), column.clone()));
        }
    }
    exposed
}

/// A pushed set operation carries its left branch's schema, so it exposes what
/// that branch exposes.
fn remote_set_op_exposure(set_op: &PhysicalRemoteSetOp) -> BTreeSet<QualifiedColumn> {
    exposure(&set_op.left)
}

/// A node that presents its input under a name exposes that input's OUTPUT names
/// under it - the names after any rename the input performs, not the source
/// columns they were computed from. The input's own exposure is kept as well,
/// since a reference written before the name was introduced still names the
/// inner relation.
fn named_exposure(name: &str, input: &PhysicalPlan) -> BTreeSet<QualifiedColumn> {
    let mut exposed = exposure(input);
    for output in output_names(input) {
        exposed.insert((name.to_string(), output));
    }
    exposed
}

/// A CTE reference is written against its alias when it has one and against the
/// CTE's own name otherwise, so it exposes its producer under whichever applies.
fn cte_scan_exposure(scan: &fq_plan::physical::PhysicalCteScan) -> BTreeSet<QualifiedColumn> {
    let name = scan.alias.clone().unwrap_or_else(|| scan.name.clone());
    named_exposure(&name, &scan.producer)
}

/// A CTE definition exposes its body under the CTE's name, using the declared
/// column list when it has one, since that list renames the body's outputs.
fn cte_exposure(cte: &fq_plan::physical::PhysicalCte) -> BTreeSet<QualifiedColumn> {
    let mut exposed = exposure(&cte.body);
    for output in cte_output_names(cte) {
        exposed.insert((cte.name.clone(), output));
    }
    exposed
}

/// A table function is a relation named by its FROM alias, so it exposes the
/// columns it produces under that name. It reads no child, so nothing else
/// beneath it could answer a reference to it.
fn table_function_exposure(
    call: &fq_plan::physical::PhysicalTableFunction,
) -> BTreeSet<QualifiedColumn> {
    let mut exposed = BTreeSet::new();
    for output in &call.output_names {
        exposed.insert((call.alias.clone(), output.clone()));
    }
    exposed
}

/// A CTE's column names: its declared list, else the names its body produces.
fn cte_output_names(cte: &fq_plan::physical::PhysicalCte) -> Vec<String> {
    match &cte.column_names {
        Some(names) => names.clone(),
        None => output_names(&cte.body),
    }
}

/// The column names a node presents to its parent, in order.
///
/// A node that computes a select list answers with that list; one that only
/// filters or reorders answers with its input's. A node whose names this walk
/// does not model answers with nothing, which makes an alias over it expose
/// nothing new and can only relax the check, never tighten it wrongly.
fn output_names(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::Projection(node) => node.output_names.clone(),
        PhysicalPlan::Window(node) => node.output_names.clone(),
        PhysicalPlan::HashAggregate(node) => node.output_names.clone(),
        PhysicalPlan::RemoteQuery(node) => node.output_names.clone(),
        PhysicalPlan::Values(node) => node.output_names.clone(),
        PhysicalPlan::TableFunction(node) => node.output_names.clone(),
        PhysicalPlan::CteMergeQuery(node) => node.output_names.clone(),
        PhysicalPlan::LateralJoin(node) => node.output_names.clone(),
        PhysicalPlan::Scan(node) => scan_output_names(node),
        PhysicalPlan::Cte(node) => cte_output_names(node),
        PhysicalPlan::CteScan(node) => output_names(&node.producer),
        PhysicalPlan::Union(node) => first_branch_output_names(&node.inputs),
        PhysicalPlan::SetOperation(node) => output_names(&node.left),
        PhysicalPlan::RemoteSetOp(node) => output_names(&node.left),
        PhysicalPlan::HashJoin(node) => join_output_names(node.join_type, &node.left, &node.right),
        PhysicalPlan::NestedLoopJoin(node) => {
            join_output_names(node.join_type, &node.left, &node.right)
        }
        PhysicalPlan::RemoteJoin(node) => {
            join_output_names(node.join_type, &node.left, &node.right)
        }
        other => passthrough_output_names(other),
    }
}

/// A union carries its first branch's names; every branch shares that schema.
fn first_branch_output_names(inputs: &[PhysicalPlan]) -> Vec<String> {
    match inputs.first() {
        Some(first) => output_names(first),
        None => Vec::new(),
    }
}

/// A join's names: the left side's for SEMI and ANTI, which emit no right
/// columns, and both sides' otherwise.
fn join_output_names(
    join_type: fq_plan::logical::JoinType,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) -> Vec<String> {
    let mut names = output_names(left);
    if matches!(
        join_type,
        fq_plan::logical::JoinType::Semi | fq_plan::logical::JoinType::Anti
    ) {
        return names;
    }
    names.extend(output_names(right));
    names
}

/// A scan's names: the list it computes when it aggregates, else the columns it
/// reads.
fn scan_output_names(scan: &PhysicalScan) -> Vec<String> {
    match &scan.output_names {
        Some(names) => names.clone(),
        None => scan.columns.clone(),
    }
}

/// The names of a node that adds none of its own: its single child's, or nothing
/// when it has several and no rule says which side names the result.
fn passthrough_output_names(plan: &PhysicalPlan) -> Vec<String> {
    let children = plan.children();
    if children.len() == 1 {
        return output_names(children[0]);
    }
    Vec::new()
}

/// The expressions a node evaluates itself, as opposed to those its children own.
///
/// Exhaustive: a new node forces a new arm rather than silently going unchecked.
fn own_expressions(plan: &PhysicalPlan) -> Vec<&Expr> {
    match plan {
        PhysicalPlan::Projection(node) => {
            with_optional(&node.expressions, node.distinct_on.as_ref())
        }
        PhysicalPlan::Window(node) => node.expressions.iter().collect(),
        PhysicalPlan::Filter(node) => vec![&node.predicate],
        PhysicalPlan::HashJoin(node) => join_key_expressions(&node.left_keys, &node.right_keys),
        PhysicalPlan::NestedLoopJoin(node) => node.condition.iter().collect(),
        PhysicalPlan::Sort(node) => node.sort_keys.iter().collect(),
        PhysicalPlan::HashAggregate(node) => aggregate_expressions(node),
        PhysicalPlan::SingleRowGuard(node) => node.keys.iter().collect(),
        PhysicalPlan::GroupedLimit(node) => grouped_limit_expressions(node),
        other => unchecked_expressions(other),
    }
}

/// The nodes whose own references this check does not model.
///
/// A source node's expressions address its own table and are rendered into the
/// text it sends, so there is nothing above it to resolve them against. The
/// remaining nodes carry no expressions of their own, or carry SQL text rather
/// than expressions, and a reference inside rendered text is not a `ColumnRef`
/// this walk can see.
fn unchecked_expressions(plan: &PhysicalPlan) -> Vec<&Expr> {
    match plan {
        PhysicalPlan::RemoteJoin(node) => vec![&node.condition],
        _ => Vec::new(),
    }
}

/// A projection's select list plus its DISTINCT ON keys, when it has them.
fn with_optional<'p>(expressions: &'p [Expr], optional: Option<&'p Vec<Expr>>) -> Vec<&'p Expr> {
    let mut all: Vec<&Expr> = expressions.iter().collect();
    if let Some(extra) = optional {
        all.extend(extra.iter());
    }
    all
}

/// Both sides' join keys, which the join evaluates against its own children.
fn join_key_expressions<'p>(left: &'p [Expr], right: &'p [Expr]) -> Vec<&'p Expr> {
    let mut keys: Vec<&Expr> = left.iter().collect();
    keys.extend(right.iter());
    keys
}

/// An aggregate's grouping keys, its aggregate calls, and any grouping sets.
fn aggregate_expressions(node: &fq_plan::physical::PhysicalHashAggregate) -> Vec<&Expr> {
    let mut all: Vec<&Expr> = node.group_by.iter().collect();
    all.extend(node.aggregates.iter());
    if let Some(sets) = &node.grouping_sets {
        for set in sets {
            all.extend(set.iter());
        }
    }
    all
}

/// A grouped limit's partition keys and its ordering keys.
fn grouped_limit_expressions(node: &fq_plan::physical::PhysicalGroupedLimit) -> Vec<&Expr> {
    let mut all: Vec<&Expr> = node.keys.iter().collect();
    if let Some(keys) = &node.order_by_keys {
        all.extend(keys.iter());
    }
    all
}
