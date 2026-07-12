//! `PredicatePushdown`: route each filter conjunct to the site that can evaluate
//! it - into scans, below joins (folding equi keys into the ON condition), into
//! set-operation branches - plus OR factoring, per-relation OR derivation, and
//! transitive-constant derivation through join equalities. Ports
//! `PredicatePushdownRule`.

use std::collections::HashMap;
use std::collections::HashSet;

use fq_plan::expr::{
    and_expressions, combine_and, combine_or, split_conjuncts, split_disjuncts, BinaryOpType, Expr,
};
use fq_plan::logical::{Filter, Join, JoinType, LogicalPlan, Scan};

use crate::error::OptimizeError;
use crate::pushdown::{
    available_columns, columns_belong_to_side, is_equi_predicate, qualified_or_bare_names,
};

use super::{set_operation_branches, with_set_operation_branches, OptimizationRule};

/// Push filters closer to data sources.
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "PredicatePushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        push_down(plan)
    }
}

/// An OR with more branches than this derives nothing (the derived predicate
/// would be as long as the original and rarely selective).
const MAX_DERIVE_BRANCHES: usize = 8;

/// One of the two sides of a join.
#[derive(Debug, Clone, Copy)]
enum Side {
    Left,
    Right,
}

/// Recursively push predicates down the plan tree. Filter is where pushdown
/// happens; a Join first sinks its single-sided condition conjuncts then
/// recurses; every other handled node just recurses; anything else bottoms out.
/// Ports `_push_down`.
fn push_down(plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
    match plan {
        LogicalPlan::Filter(filter) => push_filter(filter),
        LogicalPlan::Join(join) => {
            let pushed = push_join_condition(join);
            LogicalPlan::Join(pushed).try_map_children(push_down)
        }
        recurse @ (LogicalPlan::SetOperation(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::SubqueryScan(_)
        | LogicalPlan::Cte(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SingleRowGuard(_)) => recurse.try_map_children(push_down),
        other => Ok(other),
    }
}

/// Route a filter to the handler for its input node type. The conjunction is NOT
/// split here (each handler distributes conjuncts itself in one pass; splitting
/// here plus re-merging stacked filters would oscillate forever). Ports
/// `_push_filter`.
fn push_filter(filter: Filter) -> Result<LogicalPlan, OptimizeError> {
    let Filter { input, predicate } = filter;
    let predicate = factor_filter_predicate(predicate);
    let predicate = derive_disjunction_predicates(predicate);
    match *input {
        LogicalPlan::Filter(inner) => merge_filters(predicate, inner),
        LogicalPlan::Projection(projection) => {
            push_filter_through_projection(&predicate, projection)
        }
        LogicalPlan::Join(join) => push_filter_below_join(predicate, join),
        LogicalPlan::Scan(scan) => Ok(push_filter_to_scan(&predicate, *scan)),
        set_op @ (LogicalPlan::Union(_) | LogicalPlan::SetOperation(_)) => {
            push_filter_into_set_operation(predicate, set_op)
        }
        other => rehome_filter_over_opaque(predicate, other),
    }
}

// ---------------------------------------------------------------------------
// OR handling: common-conjunct factoring + per-relation OR derivation
// ---------------------------------------------------------------------------

/// Lift conjuncts common to every OR branch out of each conjunct's disjunction
/// (a WHERE is an AND of conjuncts, a disjunction is one of them). Ports
/// `_factor_filter_predicate` / `_factor_each_conjunct`.
fn factor_filter_predicate(predicate: Expr) -> Expr {
    let conjuncts = split_conjuncts(&predicate);
    let mut result = Vec::with_capacity(conjuncts.len());
    let mut changed = false;
    for conjunct in conjuncts {
        match factor_common_conjuncts(conjunct) {
            Some(rewritten) => {
                changed = true;
                result.push(rewritten);
            }
            None => result.push(conjunct.clone()),
        }
    }
    if !changed {
        return predicate;
    }
    combine_and(result).expect("a non-empty conjunct list combines")
}

/// Rewrite `(A and c) or (B and c)` as `c and (A or B)`; None if unchanged. Ports
/// `_factor_common_conjuncts`.
fn factor_common_conjuncts(predicate: &Expr) -> Option<Expr> {
    let branches = split_disjuncts(predicate);
    if branches.len() < 2 {
        return None;
    }
    let branch_conjuncts: Vec<Vec<&Expr>> = branches
        .iter()
        .map(|branch| split_conjuncts(branch))
        .collect();
    let common = common_conjuncts(&branch_conjuncts);
    if common.is_empty() {
        return None;
    }
    Some(rebuild_factored(&common, &branch_conjuncts))
}

/// Conjuncts that appear (structurally) in every OR branch. Ports
/// `_common_conjuncts`.
fn common_conjuncts<'a>(branch_conjuncts: &[Vec<&'a Expr>]) -> Vec<&'a Expr> {
    let mut common = Vec::new();
    for conjunct in &branch_conjuncts[0] {
        if branch_conjuncts
            .iter()
            .all(|branch| refs_contain(branch, conjunct))
        {
            common.push(*conjunct);
        }
    }
    common
}

/// Build `common and (remainder_1 or ... or remainder_n)`. A branch that reduces
/// to nothing is TRUE, so the whole OR is TRUE and only the common conjuncts
/// remain. Ports `_rebuild_factored`.
fn rebuild_factored(common: &[&Expr], branch_conjuncts: &[Vec<&Expr>]) -> Expr {
    let factored =
        combine_and(common.iter().map(|expr| (*expr).clone()).collect()).expect("common non-empty");
    let mut remainders = Vec::with_capacity(branch_conjuncts.len());
    for branch in branch_conjuncts {
        let rest = drop_common(branch, common);
        if rest.is_empty() {
            return factored;
        }
        remainders.push(combine_and(rest).expect("rest non-empty"));
    }
    and_expressions(Some(factored), combine_or(remainders)).expect("factored is present")
}

/// A branch's conjuncts with the common ones removed. Ports `_drop_common`.
fn drop_common(branch: &[&Expr], common: &[&Expr]) -> Vec<Expr> {
    let mut rest = Vec::new();
    for conjunct in branch {
        if !refs_contain(common, conjunct) {
            rest.push((*conjunct).clone());
        }
    }
    rest
}

/// Add per-relation predicates IMPLIED by multi-relation ORs (the original OR
/// stays for exactness; the derived conjuncts are redundant but pushable).
/// Already-present conjuncts are never re-added. Ports
/// `_derive_disjunction_predicates`.
fn derive_disjunction_predicates(predicate: Expr) -> Expr {
    let conjuncts: Vec<Expr> = split_conjuncts(&predicate).into_iter().cloned().collect();
    let derived = derived_from_disjunctions(&conjuncts);
    if derived.is_empty() {
        return predicate;
    }
    let mut all = conjuncts;
    all.extend(derived);
    combine_and(all).expect("non-empty")
}

/// The implied single-relation ORs not already among the conjuncts. Ports
/// `_derived_from_disjunctions`.
fn derived_from_disjunctions(conjuncts: &[Expr]) -> Vec<Expr> {
    let mut derived: Vec<Expr> = Vec::new();
    for conjunct in conjuncts {
        for candidate in implied_single_relation_ors(conjunct) {
            if !conjuncts.contains(&candidate) && !derived.contains(&candidate) {
                derived.push(candidate);
            }
        }
    }
    derived
}

/// The per-relation predicates implied by one OR conjunct. Ports
/// `_implied_single_relation_ors`.
fn implied_single_relation_ors(conjunct: &Expr) -> Vec<Expr> {
    let branches = split_disjuncts(conjunct);
    if branches.len() < 2 || branches.len() > MAX_DERIVE_BRANCHES {
        return Vec::new();
    }
    let per_branch: Vec<HashMap<String, Vec<Expr>>> = branches
        .iter()
        .map(|branch| single_relation_groups(&split_conjuncts(branch)))
        .collect();
    let mut implied = Vec::new();
    for relation in relations_in_every_branch(&per_branch) {
        implied.push(relation_or(&per_branch, &relation));
    }
    implied
}

/// Relations that EVERY branch constrains, in deterministic (sorted) order. Ports
/// `_relations_in_every_branch`.
fn relations_in_every_branch(per_branch: &[HashMap<String, Vec<Expr>>]) -> Vec<String> {
    let mut relations: HashSet<String> = per_branch[0].keys().cloned().collect();
    for groups in &per_branch[1..] {
        let keys: HashSet<String> = groups.keys().cloned().collect();
        relations = &relations & &keys;
    }
    let mut sorted: Vec<String> = relations.into_iter().collect();
    sorted.sort();
    sorted
}

/// OR together each branch's conjuncts on one relation. Ports `_relation_or`.
fn relation_or(per_branch: &[HashMap<String, Vec<Expr>>], relation: &str) -> Expr {
    let mut parts = Vec::with_capacity(per_branch.len());
    for groups in per_branch {
        let conjuncts = groups.get(relation).cloned().unwrap_or_default();
        parts.push(combine_and(conjuncts).expect("relation present in every branch"));
    }
    combine_or(parts).expect("non-empty")
}

/// A branch's conjuncts grouped by the ONE relation they reference. Ports
/// `_single_relation_groups`.
fn single_relation_groups(branch_conjuncts: &[&Expr]) -> HashMap<String, Vec<Expr>> {
    let mut groups: HashMap<String, Vec<Expr>> = HashMap::new();
    for conjunct in branch_conjuncts {
        if let Some(relation) = sole_relation(conjunct) {
            groups
                .entry(relation)
                .or_default()
                .push((*conjunct).clone());
        }
    }
    groups
}

/// The single qualifier every column of an expression carries, or None
/// (unqualified refs, multiple relations, or no columns at all). Ports
/// `_sole_relation`.
fn sole_relation(expression: &Expr) -> Option<String> {
    let mut qualifiers = HashSet::new();
    for reference in fq_plan::expr::column_refs(expression) {
        match &reference.table {
            Some(table) if !table.is_empty() => {
                qualifiers.insert(table.clone());
            }
            _ => return None,
        }
    }
    if qualifiers.len() == 1 {
        qualifiers.into_iter().next()
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Per-input-type filter handlers
// ---------------------------------------------------------------------------

/// Merge two adjacent filters into one AND, then re-run pushdown on the collapsed
/// node. Ports `_merge_filters`.
fn merge_filters(predicate: Expr, mut inner: Filter) -> Result<LogicalPlan, OptimizeError> {
    // A fresh AND combining the two owned predicates; an inline enum variant with no
    // base node, and {op, left, right} is the complete BinaryOp field set.
    let merged = Expr::BinaryOp {
        op: BinaryOpType::And,
        left: Box::new(predicate),
        right: Box::new(inner.predicate),
    };
    // In-place on the owned inner filter: only the predicate becomes the merged AND;
    // its `input` is preserved by identity (the collapsed node keeps the inner input).
    inner.predicate = merged;
    push_down(LogicalPlan::Filter(inner))
}

/// Push the conjuncts a projection's input can evaluate below it; the rest stay
/// above (they reference projection-computed outputs). Ports
/// `_push_filter_through_projection` / `_assemble_projection`.
fn push_filter_through_projection(
    predicate: &Expr,
    mut projection: fq_plan::logical::Projection,
) -> Result<LogicalPlan, OptimizeError> {
    let input_cols = available_columns(&projection.input);
    let conjuncts = split_conjuncts(predicate);
    let (below, above) = partition_by_evaluability(&conjuncts, &input_cols);
    let input = *projection.input;
    projection.input = Box::new(push_side(input, below)?);
    let new_projection = LogicalPlan::Projection(projection);
    Ok(wrap_above(new_projection, above))
}

/// Split conjuncts into those evaluable by `input_cols` and those not. Ports
/// `_partition_by_evaluability`.
fn partition_by_evaluability(
    conjuncts: &[&Expr],
    input_cols: &HashSet<String>,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut below = Vec::new();
    let mut above = Vec::new();
    for conjunct in conjuncts {
        let cols = qualified_or_bare_names(conjunct);
        if can_evaluate_predicate(&cols, input_cols) {
            below.push((*conjunct).clone());
        } else {
            above.push((*conjunct).clone());
        }
    }
    (below, above)
}

/// Whether every predicate column can be evaluated with the available columns
/// (bare names tolerate a qualified match). Ports `_can_evaluate_predicate`.
fn can_evaluate_predicate(pred_cols: &HashSet<String>, available: &HashSet<String>) -> bool {
    for pred_col in pred_cols {
        if available.contains(pred_col) {
            continue;
        }
        if pred_col.contains('.') {
            return false;
        }
        let suffix = format!(".{pred_col}");
        if available
            .iter()
            .any(|avail| avail == pred_col || avail.ends_with(&suffix))
        {
            continue;
        }
        return false;
    }
    true
}

/// Distribute pushable conjuncts into every set-operation branch (a deterministic
/// predicate commutes with UNION/INTERSECT/EXCEPT when applied to every branch);
/// a conjunct descends only when every branch can evaluate it. Ports
/// `_push_filter_into_set_operation`.
fn push_filter_into_set_operation(
    predicate: Expr,
    set_op: LogicalPlan,
) -> Result<LogicalPlan, OptimizeError> {
    let branches = set_operation_branches(&set_op);
    let conjuncts = split_conjuncts(&predicate);
    let (below, above) = partition_for_branches(&conjuncts, &branches);
    if below.is_empty() {
        return rehome_filter_over_opaque(predicate, set_op);
    }
    let mut pushed = Vec::with_capacity(branches.len());
    for branch in branches {
        pushed.push(push_side(branch, below.clone())?);
    }
    let rebuilt = with_set_operation_branches(set_op, pushed);
    Ok(wrap_above(rebuilt, above))
}

/// Split conjuncts into those EVERY branch can evaluate and the rest. Ports
/// `_partition_for_branches`.
fn partition_for_branches(conjuncts: &[&Expr], branches: &[LogicalPlan]) -> (Vec<Expr>, Vec<Expr>) {
    let availables: Vec<HashSet<String>> = branches.iter().map(available_columns).collect();
    let mut below = Vec::new();
    let mut above = Vec::new();
    for conjunct in conjuncts {
        let cols = qualified_or_bare_names(conjunct);
        if availables
            .iter()
            .all(|available| can_evaluate_predicate(&cols, available))
        {
            below.push((*conjunct).clone());
        } else {
            above.push((*conjunct).clone());
        }
    }
    (below, above)
}

/// Push a filter into a scan, merging conjuncts UNIQUELY (a conjunct the scan
/// already carries is not re-ANDed - derived predicates are re-derived every
/// pass, and a blind AND would grow the scan filter forever). Ports
/// `_push_filter_to_scan`.
fn push_filter_to_scan(predicate: &Expr, mut scan: Scan) -> LogicalPlan {
    let existing: Vec<Expr> = scan
        .filters
        .as_ref()
        .map(|filters| split_conjuncts(filters).into_iter().cloned().collect())
        .unwrap_or_default();
    let mut merged = existing.clone();
    for conjunct in split_conjuncts(predicate) {
        if !merged.contains(conjunct) {
            merged.push(conjunct.clone());
        }
    }
    if merged == existing {
        return LogicalPlan::Scan(Box::new(scan));
    }
    scan.filters = combine_and(merged);
    LogicalPlan::Scan(Box::new(scan))
}

// ---------------------------------------------------------------------------
// Join filter routing (INNER bucketing, outer-join side rules, transitives)
// ---------------------------------------------------------------------------

/// The four buckets a join filter's conjuncts route to.
#[derive(Default)]
struct JoinGroups {
    left: Vec<Expr>,
    right: Vec<Expr>,
    equi: Vec<Expr>,
    residual: Vec<Expr>,
}

/// Route each conjunct of a filter to the join site that can evaluate it (INNER),
/// or keep the filter above an outer join. Ports `_push_filter_below_join`.
fn push_filter_below_join(predicate: Expr, join: Join) -> Result<LogicalPlan, OptimizeError> {
    if join.join_type != JoinType::Inner {
        return push_filter_below_outer_join(predicate, join);
    }
    let mut conjuncts: Vec<Expr> = split_conjuncts(&predicate).into_iter().cloned().collect();
    let transitive = transitive_constants(&conjuncts, &join);
    conjuncts.extend(transitive);
    let groups = group_join_conjuncts(&conjuncts, &join);
    assemble_pushed_join(join, groups)
}

/// Constants implied through the join's equality condition (`a = lit` plus
/// condition `a = b` implies `b = lit`). Ports `_transitive_constants`.
fn transitive_constants(conjuncts: &[Expr], join: &Join) -> Vec<Expr> {
    let mut literals: HashMap<(Option<String>, String), Expr> = HashMap::new();
    for conjunct in conjuncts {
        note_literal_equality(conjunct, &mut literals);
    }
    if literals.is_empty() || join.condition.is_none() {
        return Vec::new();
    }
    derive_from_condition(join, &literals, conjuncts)
}

/// Record a `column = literal` conjunct (either orientation). Ports
/// `_note_literal_equality`.
fn note_literal_equality(conjunct: &Expr, literals: &mut HashMap<(Option<String>, String), Expr>) {
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = conjunct
    else {
        return;
    };
    if let (Expr::Column(column), Expr::Literal { .. }) = (left.as_ref(), right.as_ref()) {
        literals.insert(
            (column.table.clone(), column.column.clone()),
            (**right).clone(),
        );
    }
    if let (Expr::Column(column), Expr::Literal { .. }) = (right.as_ref(), left.as_ref()) {
        literals.insert(
            (column.table.clone(), column.column.clone()),
            (**left).clone(),
        );
    }
}

/// The derived `other = lit` conjuncts, deduplicated against what is already
/// present (the rule runs to a fixed point). Ports `_derive_from_condition`.
fn derive_from_condition(
    join: &Join,
    literals: &HashMap<(Option<String>, String), Expr>,
    conjuncts: &[Expr],
) -> Vec<Expr> {
    let condition = join.condition.as_ref().expect("condition checked present");
    let cond_conjuncts = split_conjuncts(condition);
    let mut present: Vec<Expr> = conjuncts.to_vec();
    present.extend(cond_conjuncts.iter().map(|expr| (*expr).clone()));
    let mut derived: Vec<Expr> = Vec::new();
    for equi in &cond_conjuncts {
        if let Some(candidate) = transit_one(equi, literals) {
            if !present.contains(&candidate) && !derived.contains(&candidate) {
                derived.push(candidate);
            }
        }
    }
    derived
}

/// `other = lit` for one condition equality, or None. Ports `_transit_one`.
fn transit_one(equi: &Expr, literals: &HashMap<(Option<String>, String), Expr>) -> Option<Expr> {
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = equi
    else {
        return None;
    };
    let (Expr::Column(_), Expr::Column(_)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    for (source, target) in [(left, right), (right, left)] {
        if let Expr::Column(source_col) = source.as_ref() {
            if let Some(literal) =
                literals.get(&(source_col.table.clone(), source_col.column.clone()))
            {
                // A fresh transitive `target = literal` equality; an inline enum
                // variant with no base node, built from the borrowed target/literal
                // (cloned to own them), with the complete BinaryOp field set.
                return Some(Expr::BinaryOp {
                    op: BinaryOpType::Eq,
                    left: Box::new((**target).clone()),
                    right: Box::new(literal.clone()),
                });
            }
        }
    }
    None
}

/// Bucket conjuncts by destination: left side, right side, equi-key, residual.
/// Ports `_group_join_conjuncts` / `_classify_join_conjunct`.
fn group_join_conjuncts(conjuncts: &[Expr], join: &Join) -> JoinGroups {
    let left_cols = available_columns(&join.left);
    let right_cols = available_columns(&join.right);
    let mut groups = JoinGroups::default();
    for conjunct in conjuncts {
        let cols = qualified_or_bare_names(conjunct);
        if columns_belong_to_side(&cols, &left_cols, &right_cols) {
            groups.left.push(conjunct.clone());
        } else if columns_belong_to_side(&cols, &right_cols, &left_cols) {
            groups.right.push(conjunct.clone());
        } else if is_equi_predicate(conjunct) {
            groups.equi.push(conjunct.clone());
        } else {
            groups.residual.push(conjunct.clone());
        }
    }
    groups
}

/// Rebuild the join with side conjuncts pushed and equi keys folded into the ON
/// condition; a NATURAL/USING join keeps equalities as a residual filter (its ON
/// clause is implicit). Ports `_assemble_pushed_join`.
fn assemble_pushed_join(mut join: Join, groups: JoinGroups) -> Result<LogicalPlan, OptimizeError> {
    let mut equi = groups.equi;
    let mut residual = groups.residual;
    if join.natural || join.using.is_some() {
        residual.append(&mut equi);
    }
    let left_input = *join.left;
    let right_input = *join.right;
    let new_left = push_side(left_input, groups.left)?;
    let new_right = push_side(right_input, groups.right)?;
    let new_condition = fold_equi_conjuncts(join.condition.take(), equi);
    join.left = Box::new(new_left);
    join.right = Box::new(new_right);
    join.condition = new_condition;
    Ok(wrap_residual(LogicalPlan::Join(join), residual))
}

/// Push a group of one-sided conjuncts into a join input, then optimize it. Ports
/// `_push_side`.
fn push_side(side: LogicalPlan, conjuncts: Vec<Expr>) -> Result<LogicalPlan, OptimizeError> {
    if conjuncts.is_empty() {
        return push_down(side);
    }
    let predicate = combine_and(conjuncts).expect("non-empty");
    // A fresh Filter synthesized to carry the pushed conjuncts into the side; there
    // is no base Filter here, and {input, predicate} is the complete Filter field set.
    push_down(LogicalPlan::Filter(Filter {
        input: Box::new(side),
        predicate,
    }))
}

/// AND cross-side equality keys into the join's existing ON condition. Ports
/// `_fold_equi_conjuncts`.
fn fold_equi_conjuncts(condition: Option<Expr>, equi_conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut combined = condition;
    for conjunct in equi_conjuncts {
        combined = and_expressions(combined, Some(conjunct));
    }
    combined
}

/// Put leftover cross-side non-equi conjuncts back as one filter on top. Ports
/// `_wrap_residual`.
fn wrap_residual(new_join: LogicalPlan, residual: Vec<Expr>) -> LogicalPlan {
    if residual.is_empty() {
        return new_join;
    }
    // A fresh Filter synthesized to re-home leftover cross-side conjuncts on top of
    // the join; no base Filter, and {input, predicate} is the complete field set.
    LogicalPlan::Filter(Filter {
        input: Box::new(new_join),
        predicate: combine_and(residual).expect("non-empty"),
    })
}

/// A Filter over `plan` for the leftover `above` conjuncts, or `plan` unchanged.
fn wrap_above(plan: LogicalPlan, above: Vec<Expr>) -> LogicalPlan {
    if above.is_empty() {
        return plan;
    }
    // A fresh Filter synthesized to keep the non-pushable conjuncts above the node;
    // no base Filter, and {input, predicate} is the complete Filter field set.
    LogicalPlan::Filter(Filter {
        input: Box::new(plan),
        predicate: combine_and(above).expect("non-empty"),
    })
}

// ---------------------------------------------------------------------------
// Join-condition conjunct sinking + outer-join preserved-side push
// ---------------------------------------------------------------------------

/// The non-preserved side a single-sided condition conjunct may move into for
/// each join type. RIGHT is deliberately EXCLUDED (its non-preserved side is the
/// whole LEFT subtree, which single-source rendering cannot carry on the ON
/// clause). Ports `_CONDITION_PUSH_SIDE`.
fn condition_push_side(join_type: JoinType) -> Option<Side> {
    match join_type {
        JoinType::Left | JoinType::Semi | JoinType::Anti => Some(Side::Right),
        _ => None,
    }
}

/// The join side whose rows always survive, or None for FULL. SEMI/ANTI expose
/// only left columns and keep/drop whole left rows, so - like LEFT - a left-only
/// conjunct can descend into the left input. Ports `_preserved_side`.
fn preserved_side(join_type: JoinType) -> Option<Side> {
    match join_type {
        JoinType::Left | JoinType::Semi | JoinType::Anti => Some(Side::Left),
        JoinType::Right => Some(Side::Right),
        _ => None,
    }
}

/// Move condition conjuncts that reference only the non-preserved side into that
/// input as a filter. At least one conjunct must remain as the join condition.
/// Ports `_push_join_condition`.
fn push_join_condition(mut join: Join) -> Join {
    let Some(side) = condition_push_side(join.join_type) else {
        return join;
    };
    if join.condition.is_none() || join.natural || join.using.is_some() {
        return join;
    }
    let (movable, kept) = split_single_sided(&join, side);
    if movable.is_empty() || kept.is_empty() {
        return join;
    }
    let filtered_predicate = combine_and(movable).expect("movable non-empty");
    // condition_push_side only yields the RIGHT (non-preserved) side today.
    match side {
        Side::Right => {
            let side_input = *join.right;
            // A fresh Filter synthesized to sink the movable conjuncts into the
            // right input; no base Filter, {input, predicate} is the complete set.
            join.right = Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(side_input),
                predicate: filtered_predicate,
            }));
        }
        Side::Left => {
            let side_input = *join.left;
            // A fresh Filter synthesized to sink the movable conjuncts into the
            // left input; no base Filter, {input, predicate} is the complete set.
            join.left = Box::new(LogicalPlan::Filter(Filter {
                input: Box::new(side_input),
                predicate: filtered_predicate,
            }));
        }
    }
    join.condition = combine_and(kept);
    join
}

/// `(movable, kept)` condition conjuncts: movable ones reference at least one
/// column and only the given side's columns. Ports `_split_single_sided`.
fn split_single_sided(join: &Join, side: Side) -> (Vec<Expr>, Vec<Expr>) {
    let (side_plan, other_plan) = match side {
        Side::Right => (&join.right, &join.left),
        Side::Left => (&join.left, &join.right),
    };
    let side_cols = available_columns(side_plan);
    let other_cols = available_columns(other_plan);
    let condition = join.condition.as_ref().expect("condition present");
    let mut movable = Vec::new();
    let mut kept = Vec::new();
    for conjunct in split_conjuncts(condition) {
        if movable_conjunct(conjunct, &side_cols, &other_cols) {
            movable.push(conjunct.clone());
        } else {
            kept.push(conjunct.clone());
        }
    }
    (movable, kept)
}

/// Whether one conjunct references only the pushable side (a constant conjunct
/// stays). Ports `_movable_conjunct`.
fn movable_conjunct(
    conjunct: &Expr,
    side_cols: &HashSet<String>,
    other_cols: &HashSet<String>,
) -> bool {
    let cols = qualified_or_bare_names(conjunct);
    if cols.is_empty() {
        return false;
    }
    columns_belong_to_side(&cols, side_cols, other_cols)
}

/// Push only preserved-side conjuncts below an outer join; keep the rest above.
/// Ports `_push_filter_below_outer_join`.
fn push_filter_below_outer_join(predicate: Expr, join: Join) -> Result<LogicalPlan, OptimizeError> {
    let Some(preserved) = preserved_side(join.join_type) else {
        return rehome_filter_over_join(predicate, join);
    };
    let conjuncts: Vec<Expr> = split_conjuncts(&predicate).into_iter().cloned().collect();
    let join = widen_condition_transitively(join, &conjuncts, preserved);
    // A join under a residual filter is never visited bare by the walker, so its
    // single-sided condition conjuncts (incl. just-derived transitive constants)
    // migrate here or never.
    let join = push_join_condition(join);
    let (down, keep) = partition_preserved(&conjuncts, &join, preserved);
    assemble_outer_join(join, down, keep, preserved)
}

/// AND constants derived from PRESERVED-side filter conjuncts into the outer
/// join's condition (the existing condition-push then sinks them into the
/// nullable input). Ports `_widen_condition_transitively`.
fn widen_condition_transitively(mut join: Join, conjuncts: &[Expr], preserved: Side) -> Join {
    let (preserved_plan, nullable_plan) = match preserved {
        Side::Left => (&join.left, &join.right),
        Side::Right => (&join.right, &join.left),
    };
    let preserved_cols = available_columns(preserved_plan);
    let nullable_cols = available_columns(nullable_plan);
    let mut anchored = Vec::new();
    for conjunct in conjuncts {
        let names = qualified_or_bare_names(conjunct);
        if columns_belong_to_side(&names, &preserved_cols, &nullable_cols) {
            anchored.push(conjunct.clone());
        }
    }
    let derived = transitive_constants(&anchored, &join);
    if derived.is_empty() {
        return join;
    }
    let condition = join
        .condition
        .as_ref()
        .expect("derived implies a condition");
    let mut widened: Vec<Expr> = split_conjuncts(condition).into_iter().cloned().collect();
    widened.extend(derived);
    join.condition = combine_and(widened);
    join
}

/// Split conjuncts into preserved-side-only (pushable) and the rest. Ports
/// `_partition_preserved`.
fn partition_preserved(conjuncts: &[Expr], join: &Join, preserved: Side) -> (Vec<Expr>, Vec<Expr>) {
    let (side_plan, other_plan) = match preserved {
        Side::Left => (&join.left, &join.right),
        Side::Right => (&join.right, &join.left),
    };
    let side_cols = available_columns(side_plan);
    let other_cols = available_columns(other_plan);
    let mut down = Vec::new();
    let mut keep = Vec::new();
    for conjunct in conjuncts {
        let cols = qualified_or_bare_names(conjunct);
        if columns_belong_to_side(&cols, &side_cols, &other_cols) {
            down.push(conjunct.clone());
        } else {
            keep.push(conjunct.clone());
        }
    }
    (down, keep)
}

/// Rebuild an outer join with preserved-side conjuncts pushed, the rest kept
/// above. Ports `_assemble_outer_join`.
fn assemble_outer_join(
    mut join: Join,
    down: Vec<Expr>,
    keep: Vec<Expr>,
    preserved: Side,
) -> Result<LogicalPlan, OptimizeError> {
    let left_input = *join.left;
    let right_input = *join.right;
    let (new_left, new_right) = match preserved {
        Side::Left => (push_side(left_input, down)?, push_down(right_input)?),
        Side::Right => (push_down(left_input)?, push_side(right_input, down)?),
    };
    join.left = Box::new(new_left);
    join.right = Box::new(new_right);
    let new_join = LogicalPlan::Join(join);
    if keep.is_empty() {
        return Ok(new_join);
    }
    // A fresh Filter synthesized to keep the non-pushable conjuncts above the outer
    // join; no base Filter, and {input, predicate} is the complete Filter field set.
    Ok(LogicalPlan::Filter(Filter {
        input: Box::new(new_join),
        predicate: combine_and(keep).expect("non-empty"),
    }))
}

/// Keep the filter above a FULL outer join while still pushing into both sides
/// (model_copy semantics preserve NATURAL/USING). Ports `_rehome_filter_over_join`.
fn rehome_filter_over_join(predicate: Expr, mut join: Join) -> Result<LogicalPlan, OptimizeError> {
    let left_input = *join.left;
    let right_input = *join.right;
    join.left = Box::new(push_down(left_input)?);
    join.right = Box::new(push_down(right_input)?);
    // A fresh Filter re-homed above the FULL outer join (the original was decomposed
    // into predicate + join by the caller); {input, predicate} is the complete set.
    Ok(LogicalPlan::Filter(Filter {
        input: Box::new(LogicalPlan::Join(join)),
        predicate,
    }))
}

/// Keep a filter above a node that cannot absorb it, optimizing the child so
/// lower filters descend. Ports `_rehome_filter_over_opaque`.
fn rehome_filter_over_opaque(
    predicate: Expr,
    input: LogicalPlan,
) -> Result<LogicalPlan, OptimizeError> {
    // Whether the child changed or not, the re-homed filter is structurally the
    // same node as the original when nothing moved, so change detection holds.
    let new_input = push_down(input)?;
    // A fresh Filter re-homed above the opaque child (the original was decomposed
    // into predicate + input by the caller); {input, predicate} is the complete set.
    Ok(LogicalPlan::Filter(Filter {
        input: Box::new(new_input),
        predicate,
    }))
}

/// Whether a `&[&Expr]` slice holds an expression structurally equal to `target`.
fn refs_contain(exprs: &[&Expr], target: &Expr) -> bool {
    exprs.contains(&target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{ColumnRef, LiteralValue};
    use fq_plan::logical::Projection;

    /// A scan reading `columns` from `table`, aliased to `table`.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        let mut node = Scan::new(
            "ds",
            "public",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = Some(table.to_string());
        LogicalPlan::Scan(Box::new(node))
    }

    /// A qualified column reference.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    /// An integer literal.
    fn int(value: i64) -> Expr {
        Expr::Literal {
            value: LiteralValue::Integer(value),
            data_type: DataType::Integer,
        }
    }

    /// A binary operation.
    fn binop(op: BinaryOpType, left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// `left = right`.
    fn eq(left: Expr, right: Expr) -> Expr {
        binop(BinaryOpType::Eq, left, right)
    }

    /// A Filter over `input`.
    fn filter(input: LogicalPlan, predicate: Expr) -> LogicalPlan {
        LogicalPlan::Filter(Filter {
            input: Box::new(input),
            predicate,
        })
    }

    /// An inner join over two inputs with an ON condition.
    fn inner_join(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> LogicalPlan {
        LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(condition),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        })
    }

    #[test]
    fn scan_filter_merge_is_unique() {
        // Pushing `t.a = 1` twice must not duplicate it in the scan filter.
        let scan_node = match scan("t", &["a"]) {
            LogicalPlan::Scan(node) => *node,
            _ => unreachable!(),
        };
        let once = push_filter_to_scan(&eq(col("t", "a"), int(1)), scan_node);
        let LogicalPlan::Scan(scan_once) = once else {
            unreachable!()
        };
        let twice = push_filter_to_scan(&eq(col("t", "a"), int(1)), *scan_once);
        let LogicalPlan::Scan(scan_twice) = twice else {
            unreachable!()
        };
        assert_eq!(scan_twice.filters, Some(eq(col("t", "a"), int(1))));
    }

    #[test]
    fn inner_join_routes_each_side_to_its_scan() {
        // WHERE o.year = 2000 AND c.region = 'EU' over orders JOIN customer.
        let join = inner_join(
            scan("orders", &["cust_id", "year"]),
            scan("customer", &["id", "region"]),
            eq(col("orders", "cust_id"), col("customer", "id")),
        );
        let predicate = binop(
            BinaryOpType::And,
            eq(col("orders", "year"), int(2000)),
            eq(
                col("customer", "region"),
                Expr::Literal {
                    value: LiteralValue::String("EU".to_string()),
                    data_type: DataType::Varchar,
                },
            ),
        );
        let plan = filter(join, predicate);
        let result = PredicatePushdown.apply(plan).unwrap();
        // Both single-sided conjuncts reach their scans; the join surfaces.
        let LogicalPlan::Join(join) = result else {
            panic!("the filter dissolved into the join sides");
        };
        let LogicalPlan::Scan(orders) = *join.left else {
            panic!("left is the orders scan");
        };
        let LogicalPlan::Scan(customer) = *join.right else {
            panic!("right is the customer scan");
        };
        assert_eq!(orders.filters, Some(eq(col("orders", "year"), int(2000))));
        assert!(customer.filters.is_some());
    }

    #[test]
    fn transitive_constant_reaches_the_other_side() {
        // WHERE o.k = 5 with ON o.k = c.k implies c.k = 5, routed to customer.
        let join = inner_join(
            scan("orders", &["k"]),
            scan("customer", &["k"]),
            eq(col("orders", "k"), col("customer", "k")),
        );
        let plan = filter(join, eq(col("orders", "k"), int(5)));
        let LogicalPlan::Join(join) = PredicatePushdown.apply(plan).unwrap() else {
            panic!("join surfaces");
        };
        let LogicalPlan::Scan(customer) = *join.right else {
            panic!("right scan");
        };
        // c.k = 5 was derived transitively and pushed to the customer scan.
        assert_eq!(customer.filters, Some(eq(col("customer", "k"), int(5))));
    }

    #[test]
    fn left_join_only_pushes_preserved_side() {
        // LEFT JOIN, WHERE l.a = 1 (left/preserved) AND r.b = 2 (nullable).
        let join = LogicalPlan::Join(Join {
            left: Box::new(scan("l", &["a", "k"])),
            right: Box::new(scan("r", &["b", "k"])),
            join_type: JoinType::Left,
            condition: Some(eq(col("l", "k"), col("r", "k"))),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let predicate = binop(
            BinaryOpType::And,
            eq(col("l", "a"), int(1)),
            eq(col("r", "b"), int(2)),
        );
        let result = PredicatePushdown.apply(filter(join, predicate)).unwrap();
        // The nullable-side conjunct stays above; only the left one descends.
        let LogicalPlan::Filter(top) = result else {
            panic!("the r.b = 2 conjunct is kept above the outer join");
        };
        assert_eq!(top.predicate, eq(col("r", "b"), int(2)));
        let LogicalPlan::Join(join) = *top.input else {
            panic!("a join under the residual filter");
        };
        // Left scan got l.a = 1.
        let LogicalPlan::Scan(left) = *join.left else {
            panic!("left scan");
        };
        assert_eq!(left.filters, Some(eq(col("l", "a"), int(1))));
    }

    #[test]
    fn or_factoring_exposes_a_common_join_key() {
        // (l.k = r.k AND l.a = 1) OR (l.k = r.k AND l.a = 2)
        //   factors to  l.k = r.k AND (l.a = 1 OR l.a = 2)
        let branch1 = binop(
            BinaryOpType::And,
            eq(col("l", "k"), col("r", "k")),
            eq(col("l", "a"), int(1)),
        );
        let branch2 = binop(
            BinaryOpType::And,
            eq(col("l", "k"), col("r", "k")),
            eq(col("l", "a"), int(2)),
        );
        let predicate = binop(BinaryOpType::Or, branch1, branch2);
        let factored = factor_filter_predicate(predicate);
        let conjuncts = split_conjuncts(&factored);
        // The common equi-join key is lifted to a top-level conjunct.
        assert!(conjuncts.contains(&&eq(col("l", "k"), col("r", "k"))));
    }

    #[test]
    fn predicate_pushdown_is_idempotent() {
        let join = inner_join(
            scan("orders", &["k", "year"]),
            scan("customer", &["k"]),
            eq(col("orders", "k"), col("customer", "k")),
        );
        let predicate = eq(col("orders", "year"), int(2000));
        let plan = filter(join, predicate);
        let once = PredicatePushdown.apply(plan).unwrap();
        let twice = PredicatePushdown.apply(once.clone()).unwrap();
        assert_eq!(once, twice, "apply twice must equal apply once");
    }

    #[test]
    fn filter_through_projection_partitions_conjuncts() {
        // Projection exposes only t.a; a conjunct on t.a descends, one on an alias
        // stays above.
        let projection = LogicalPlan::Projection(Projection {
            input: Box::new(scan("t", &["a"])),
            expressions: vec![col("t", "a")],
            aliases: vec!["computed".to_string()],
            distinct: false,
            distinct_on: None,
        });
        let predicate = eq(col("t", "a"), int(1));
        let result = PredicatePushdown
            .apply(filter(projection, predicate))
            .unwrap();
        // t.a = 1 descended: the projection is now on top, filter below it.
        let LogicalPlan::Projection(projection) = result else {
            panic!("projection surfaces above the pushed filter");
        };
        let LogicalPlan::Scan(scan) = *projection.input else {
            panic!("the conjunct reached the scan");
        };
        assert_eq!(scan.filters, Some(eq(col("t", "a"), int(1))));
    }
}
