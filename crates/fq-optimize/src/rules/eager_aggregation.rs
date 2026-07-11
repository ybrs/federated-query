//! Eager aggregation: push a PARTIAL aggregate below the joins that only
//! decorate it, so the fact collapses before those joins run. Ports
//! `optimizer/eager_aggregation.py`.
//!
//! ```text
//! Aggregate(G, sums) over Join(D..., S)
//!     ->  Aggregate(G', merge-sums) over Join(D..., SubqueryScan(
//!             Aggregate((G n S) u K, partial-sums) over S, alias))
//! ```
//!
//! S is the subtree supplying every aggregate input; the D_i are decorating dims
//! (INNER plain-column equi joins whose columns feed no aggregate); K are the
//! S-side join keys. Join multiplicity is a function of the key alone and every
//! raw row inside a partial shares that key, so the final merge counts each partial
//! exactly as the original counted each raw row (Yan & Larson 1995).
//!
//! Cost-gated. Every miss DECLINES (returns the plan unchanged); only a malformed
//! rewrite (a peeled dim joining nothing, a dropped lifted conjunct) RAISES - a
//! misplaced predicate manufactures wrong results, so it crashes rather than lie.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

use fq_decorrelate::helpers::replace_column_refs;
use fq_plan::expr::{column_refs, combine_and, split_conjuncts, ColumnRef, Expr};
use fq_plan::logical::{Aggregate, Join, JoinType, LogicalPlan, Scan, SubqueryScan};

use crate::cost::CostModel;
use crate::error::OptimizeError;
use crate::pushdown::is_equi_predicate;
use crate::subplan_signature::scan_qualifier;

use super::OptimizationRule;

/// The partial must COLLAPSE: estimated groups at or below this fraction of the S
/// subtree's estimated rows, both KNOWN (an unknown or bound-fed size declines - a
/// non-collapsing partial is pure overhead).
const EAGER_COLLAPSE_MAX_RATIO: f64 = 0.5;

/// Peel-subset enumeration cap: more eligible dims than this declines (the subset
/// space doubles per dim; the q04 family has one or two).
const MAX_PEEL_CANDIDATES: usize = 4;

/// Alias prefix for the partial's derived table; also the idempotence guard (an
/// aggregate whose inputs come from an eager alias never re-splits).
const EAGER_PREFIX: &str = "__eager_";

/// A peeled dim above this row budget cannot be collapsed by full dim shipping, so
/// eager aggregation steps in to shrink the fact before it joins back; a subset
/// whose every peeled dim is shippable declines (full dim shipping dominates).
pub const SHIP_ROW_BUDGET: u64 = 200_000;

/// The partial aggregate plus the bookkeeping the final rewrite needs.
struct Partial {
    /// The partial `Aggregate` node (unwrapped; the derived-table alias wraps it
    /// in `build_final`).
    aggregate: LogicalPlan,
    alias: String,
    keys: Vec<ColumnRef>,
    /// The synthetic `__eager_sum_i` output name of each pushed SUM, in order.
    sums: Vec<String>,
}

/// Rewrite Aggregate-over-join into final-over-Join(dims, partial).
pub struct EagerAggregation {
    cost_model: Rc<RefCell<CostModel>>,
    counter: Cell<usize>,
}

impl EagerAggregation {
    /// The rule over the session's SHARED cost model (the same `Rc<RefCell<..>>`
    /// handle `JoinOrdering` holds); it prices collapse with that estimator.
    pub fn new(cost_model: Rc<RefCell<CostModel>>) -> Self {
        Self {
            cost_model,
            counter: Cell::new(0),
        }
    }

    /// Depth-first: children first (a CTE body's aggregate rewrites too), then
    /// attempt the split on this node. A partial once inserted is not re-visited by
    /// this same descent.
    fn rewrite(&self, node: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        let node = node.try_map_children(|child| self.rewrite(child))?;
        if let LogicalPlan::Aggregate(agg) = &node {
            if let Some(split) = self.try_split(agg)? {
                return Ok(split);
            }
        }
        Ok(node)
    }

    /// The rewritten final-over-partial tree, or None to keep `agg`.
    fn try_split(&self, agg: &Aggregate) -> Result<Option<LogicalPlan>, OptimizeError> {
        let Some((scans, conjuncts)) = qualifying_shape(agg) else {
            return Ok(None);
        };
        let candidates = peel_candidates(agg, &scans);
        if candidates.is_empty() || candidates.len() > MAX_PEEL_CANDIDATES {
            return Ok(None);
        }
        self.best_split(agg, &scans, &conjuncts, &candidates)
    }

    /// The rewrite for the best-collapsing peel subset passing the cost gate, or
    /// None when no subset passes.
    fn best_split(
        &self,
        agg: &Aggregate,
        scans: &[&Scan],
        conjuncts: &[&Expr],
        candidates: &[&Scan],
    ) -> Result<Option<LogicalPlan>, OptimizeError> {
        let mut best = None;
        let mut best_ratio = EAGER_COLLAPSE_MAX_RATIO;
        for indexes in ordered_subsets(candidates.len()) {
            let peeled: Vec<&Scan> = indexes.iter().map(|&index| candidates[index]).collect();
            if let Some((ratio, tree)) = self.evaluate_subset(agg, scans, conjuncts, &peeled)? {
                if ratio <= best_ratio {
                    best_ratio = ratio;
                    best = Some(tree);
                }
            }
        }
        Ok(best)
    }

    /// (collapse ratio, rewritten tree) for one peel subset, or None when the
    /// subset's shape or cost declines.
    fn evaluate_subset(
        &self,
        agg: &Aggregate,
        scans: &[&Scan],
        conjuncts: &[&Expr],
        peeled: &[&Scan],
    ) -> Result<Option<(f64, LogicalPlan)>, OptimizeError> {
        if !self.rescues_unshippable(peeled)? {
            return Ok(None);
        }
        let (inner_conjuncts, lifted) = partition_conjuncts(conjuncts, peeled);
        let survivors = survivors(scans, peeled);
        let Some(s_tree) = join_survivors(&survivors, &inner_conjuncts) else {
            return Ok(None);
        };
        let Some(partial) = self.build_partial(agg, &survivors, &lifted, s_tree) else {
            return Ok(None);
        };
        let Some(ratio) = self.collapse_ratio(&partial, &survivors)? else {
            return Ok(None);
        };
        if ratio > EAGER_COLLAPSE_MAX_RATIO {
            return Ok(None);
        }
        Ok(Some((ratio, build_final(agg, &partial, peeled, &lifted)?)))
    }

    /// Whether at least one peeled dim exceeds the ship budget (or has an unknown
    /// size). When every peeled dim is shippable the FULL collapse dominates, so
    /// the rule steps aside and lets dim shipping have it.
    fn rescues_unshippable(&self, peeled: &[&Scan]) -> Result<bool, OptimizeError> {
        for scan in peeled {
            let plan = LogicalPlan::Scan(Box::new((*scan).clone()));
            let estimate = self.cost_model.borrow_mut().estimate(&plan)?;
            if estimate.rows.is_none_or(|rows| rows > SHIP_ROW_BUDGET) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The partial aggregate plus its rewrite bookkeeping; None when a needed column
    /// is not a plain survivor column or output names would collide.
    fn build_partial(
        &self,
        agg: &Aggregate,
        survivors: &[&Scan],
        lifted: &[&Expr],
        s_tree: LogicalPlan,
    ) -> Option<Partial> {
        let survivor_names: HashSet<String> =
            survivors.iter().map(|scan| scan_qualifier(scan)).collect();
        let keys = partial_keys(agg, lifted, &survivor_names)?;
        let (select_list, names, sums) = partial_outputs(agg, &keys)?;
        let alias = format!("{EAGER_PREFIX}{}", self.counter.get());
        self.counter.set(self.counter.get() + 1);
        // The partial: group by the surviving group keys plus the lifted join keys;
        // aggregates IS the output select list (grouping columns pass through).
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(s_tree),
            group_by: keys.iter().map(|key| Expr::Column(key.clone())).collect(),
            aggregates: select_list,
            output_names: names,
            grouping_sets: None,
        });
        Some(Partial {
            aggregate,
            alias,
            keys,
            sums,
        })
    }

    /// Estimated partial groups over the LARGEST base scan in S; None when either is
    /// UNKNOWN or rests on gaps (a bound must not license the rewrite). The largest
    /// base is the denominator deliberately: a JOIN estimate under-counts through
    /// composite/containment asymmetries, and an under-counted denominator would
    /// veto exactly the rewrites that matter.
    fn collapse_ratio(
        &self,
        partial: &Partial,
        survivors: &[&Scan],
    ) -> Result<Option<f64>, OptimizeError> {
        let groups = self.cost_model.borrow_mut().estimate(&partial.aggregate)?;
        let (Some(group_rows), true) = (groups.rows, groups.defaults_used.is_empty()) else {
            return Ok(None);
        };
        let Some(largest) = self.largest_scan_rows(survivors)? else {
            return Ok(None);
        };
        if largest == 0 {
            return Ok(None);
        }
        Ok(Some(group_rows as f64 / largest as f64))
    }

    /// The largest survivor scan's estimated rows; None when any scan is UNKNOWN or
    /// gap-fed (the gate needs measurements, not bounds).
    fn largest_scan_rows(&self, survivors: &[&Scan]) -> Result<Option<u64>, OptimizeError> {
        let mut largest: Option<u64> = None;
        for scan in survivors {
            let plan = LogicalPlan::Scan(Box::new((*scan).clone()));
            let estimate = self.cost_model.borrow_mut().estimate(&plan)?;
            let Some(rows) = estimate.rows else {
                return Ok(None);
            };
            if !estimate.defaults_used.is_empty() {
                return Ok(None);
            }
            largest = Some(largest.map_or(rows, |current| current.max(rows)));
        }
        Ok(largest)
    }
}

impl OptimizationRule for EagerAggregation {
    /// This rule's identifier (used in the `validate_scope` stage label).
    fn name(&self) -> &'static str {
        "EagerAggregation"
    }

    /// Rewrite every qualifying aggregate. FEDQ_EAGER_AGG=0 is the kill switch
    /// (mirrors FEDQ_DIM_SHIPPING); the driver detects the change by `!=`.
    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        if std::env::var("FEDQ_EAGER_AGG").as_deref() == Ok("0") {
            return Ok(plan);
        }
        self.counter.set(0);
        self.rewrite(plan)
    }
}

// ------------------------------- shape gates --------------------------------

/// (scans, join conjuncts) when the aggregate and its input match the v1 shape,
/// else None.
fn qualifying_shape(agg: &Aggregate) -> Option<(Vec<&Scan>, Vec<&Expr>)> {
    if agg.grouping_sets.is_some() || !sum_only_outputs(agg) || !plain_column_keys(&agg.group_by) {
        return None;
    }
    let mut scans = Vec::new();
    let mut conjuncts = Vec::new();
    if !collect_inner_tree(&agg.input, &mut scans, &mut conjuncts) {
        return None;
    }
    if !conjuncts.iter().all(|conjunct| is_equi_predicate(conjunct)) || single_source(&scans) {
        return None;
    }
    Some((scans, conjuncts))
}

/// Every output is a passthrough group column, a CONSTANT (a literal tag column
/// like q04's sale_type), or a plain SUM call - AND the input holds no eager
/// partial (the idempotence guard against re-splitting an already-split aggregate).
fn sum_only_outputs(agg: &Aggregate) -> bool {
    for entry in &agg.aggregates {
        if matches!(entry, Expr::Column(_) | Expr::Literal { .. }) {
            continue;
        }
        if !plain_sum(entry) {
            return false;
        }
    }
    !contains_eager_partial(&agg.input)
}

/// Whether an output entry is a plain (non-DISTINCT, non-ordered-set) SUM call.
fn plain_sum(entry: &Expr) -> bool {
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        distinct,
        within_group_key,
        ..
    } = entry
    else {
        return false;
    };
    if !is_aggregate || *distinct || within_group_key.is_some() {
        return false;
    }
    function_name.eq_ignore_ascii_case("SUM")
}

/// Whether the subtree already holds one of this rule's partials (a SubqueryScan
/// under the reserved alias prefix).
fn contains_eager_partial(node: &LogicalPlan) -> bool {
    if let LogicalPlan::SubqueryScan(subquery) = node {
        if subquery.alias.starts_with(EAGER_PREFIX) {
            return true;
        }
    }
    node.children()
        .iter()
        .any(|child| contains_eager_partial(child))
}

/// Whether every group key is a qualified plain column reference.
fn plain_column_keys(keys: &[Expr]) -> bool {
    keys.iter().all(|key| {
        matches!(key, Expr::Column(column) if column.table.as_deref().is_some_and(|t| !t.is_empty()))
    })
}

/// Collect the scans and join conjuncts of a pure INNER-join tree of base scans;
/// false when the subtree contains anything else.
fn collect_inner_tree<'a>(
    node: &'a LogicalPlan,
    scans: &mut Vec<&'a Scan>,
    conjuncts: &mut Vec<&'a Expr>,
) -> bool {
    if let LogicalPlan::Scan(scan) = node {
        scans.push(scan);
        return true;
    }
    let LogicalPlan::Join(join) = node else {
        return false;
    };
    if join.join_type != JoinType::Inner {
        return false;
    }
    let Some(condition) = &join.condition else {
        return false;
    };
    conjuncts.extend(split_conjuncts(condition));
    collect_inner_tree(&join.left, scans, conjuncts)
        && collect_inner_tree(&join.right, scans, conjuncts)
}

/// Whether every scan reads one datasource. A single-source subtree pushes whole
/// and aggregates at the source in one pass - a partial there is pure overhead; the
/// transform exists only to shrink what CROSSES a source boundary.
fn single_source(scans: &[&Scan]) -> bool {
    let mut sources = HashSet::new();
    for scan in scans {
        sources.insert(scan.datasource.as_str());
    }
    sources.len() <= 1
}

// ------------------------------ candidate choice ----------------------------

/// The dims eligible to move ABOVE the partial: scans owning no aggregate-argument
/// column. Empty when no core anchors a partial.
fn peel_candidates<'a>(agg: &Aggregate, scans: &[&'a Scan]) -> Vec<&'a Scan> {
    let core = agg_arg_owners(agg, scans);
    if core.is_empty() {
        return Vec::new();
    }
    let mut candidates = Vec::new();
    for scan in scans {
        if !core.contains(&scan_qualifier(scan)) {
            candidates.push(*scan);
        }
    }
    candidates
}

/// The qualifiers of scans referenced by any SUM argument.
fn agg_arg_owners(agg: &Aggregate, scans: &[&Scan]) -> HashSet<String> {
    let qualifiers: HashSet<String> = scans.iter().map(|scan| scan_qualifier(scan)).collect();
    let mut owners = HashSet::new();
    for entry in &agg.aggregates {
        if matches!(entry, Expr::Column(_)) {
            continue;
        }
        for reference in column_refs(entry) {
            if let Some(table) = &reference.table {
                if qualifiers.contains(table) {
                    owners.insert(table.clone());
                }
            }
        }
    }
    owners
}

/// The index subsets of `0..n`, in (size ascending, lexicographic member) order -
/// Python's `combinations` order, so the best-of tie-break stays faithful.
fn ordered_subsets(n: usize) -> Vec<Vec<usize>> {
    let mut masks: Vec<u32> = (1u32..(1u32 << n)).collect();
    masks.sort_by_key(|&mask| (mask.count_ones(), mask_members(mask)));
    masks.iter().map(|&mask| mask_members(mask)).collect()
}

/// The set-bit indexes of a mask, ascending.
fn mask_members(mask: u32) -> Vec<usize> {
    let mut members = Vec::new();
    for index in 0..(u32::BITS as usize) {
        if mask & (1u32 << index) != 0 {
            members.push(index);
        }
    }
    members
}

// ------------------------------ conjunct pooling ----------------------------

/// (conjuncts internal to S, conjuncts lifted above the partial). A conjunct is
/// lifted when it references any peeled dim; every other conjunct joins two
/// survivors.
fn partition_conjuncts<'a>(
    conjuncts: &[&'a Expr],
    peeled: &[&Scan],
) -> (Vec<&'a Expr>, Vec<&'a Expr>) {
    let peeled_names: HashSet<String> = peeled.iter().map(|scan| scan_qualifier(scan)).collect();
    let mut inner = Vec::new();
    let mut lifted = Vec::new();
    for &conjunct in conjuncts {
        if conjunct_touches(conjunct, &peeled_names) == 0 {
            inner.push(conjunct);
        } else {
            lifted.push(conjunct);
        }
    }
    (inner, lifted)
}

/// How many sides of a plain equality reference a peeled dim.
fn conjunct_touches(conjunct: &Expr, peeled_names: &HashSet<String>) -> usize {
    let mut touches = 0;
    for reference in conjunct_tables(conjunct) {
        if reference
            .table
            .as_deref()
            .is_some_and(|t| peeled_names.contains(t))
        {
            touches += 1;
        }
    }
    touches
}

/// The two column refs of a plain equi conjunct (empty for any other shape).
fn conjunct_tables(conjunct: &Expr) -> Vec<&ColumnRef> {
    if let Expr::BinaryOp { left, right, .. } = conjunct {
        if let (Expr::Column(left), Expr::Column(right)) = (left.as_ref(), right.as_ref()) {
            return vec![left, right];
        }
    }
    Vec::new()
}

/// The S-side scans, in original order.
fn survivors<'a>(scans: &[&'a Scan], peeled: &[&Scan]) -> Vec<&'a Scan> {
    let mut result = Vec::new();
    for &scan in scans {
        if !peeled.contains(&scan) {
            result.push(scan);
        }
    }
    result
}

/// A left-deep INNER join over the survivors placing every internal conjunct
/// exactly once, or None when a survivor would join with no condition (a
/// manufactured cross join) or a conjunct is left unplaced.
fn join_survivors(survivors: &[&Scan], inner_conjuncts: &[&Expr]) -> Option<LogicalPlan> {
    let (first, rest) = survivors.split_first()?;
    let mut current = LogicalPlan::Scan(Box::new((*first).clone()));
    let mut in_scope: HashSet<String> = std::iter::once(scan_qualifier(first)).collect();
    let mut pending: Vec<Expr> = inner_conjuncts.iter().map(|&c| c.clone()).collect();
    for scan in rest {
        in_scope.insert(scan_qualifier(scan));
        let (covered, remaining) = covered_conjuncts(&pending, &in_scope);
        if covered.is_empty() {
            return None;
        }
        pending = remaining;
        current = inner_join(
            current,
            LogicalPlan::Scan(Box::new((*scan).clone())),
            covered,
        );
    }
    if pending.is_empty() {
        Some(current)
    } else {
        None
    }
}

/// (conjuncts whose both sides are in scope, the rest), preserving order.
fn covered_conjuncts(pending: &[Expr], in_scope: &HashSet<String>) -> (Vec<Expr>, Vec<Expr>) {
    let mut covered = Vec::new();
    let mut remaining = Vec::new();
    for conjunct in pending {
        if conjunct_in_scope(conjunct, in_scope) {
            covered.push(conjunct.clone());
        } else {
            remaining.push(conjunct.clone());
        }
    }
    (covered, remaining)
}

/// Whether both sides of a plain equi conjunct are qualified into the scope set.
fn conjunct_in_scope(conjunct: &Expr, in_scope: &HashSet<String>) -> bool {
    let tables = conjunct_tables(conjunct);
    !tables.is_empty()
        && tables.iter().all(|reference| {
            reference
                .table
                .as_deref()
                .is_some_and(|t| in_scope.contains(t))
        })
}

/// One left-deep INNER join step over an owned conjunct list (always non-empty at
/// its call sites, so the condition is `Some`).
fn inner_join(left: LogicalPlan, right: LogicalPlan, conjuncts: Vec<Expr>) -> LogicalPlan {
    LogicalPlan::Join(Join {
        left: Box::new(left),
        right: Box::new(right),
        join_type: JoinType::Inner,
        condition: combine_and(conjuncts),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    })
}

// ------------------------------- partial build ------------------------------

/// The partial's group keys: surviving original keys plus each lifted conjunct's
/// survivor-side column, deduplicated; None when there is no survivor key at all.
fn partial_keys(
    agg: &Aggregate,
    lifted: &[&Expr],
    survivor_names: &HashSet<String>,
) -> Option<Vec<ColumnRef>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for key in &agg.group_by {
        if let Expr::Column(column) = key {
            if column
                .table
                .as_deref()
                .is_some_and(|t| survivor_names.contains(t))
            {
                add_key(&mut keys, &mut seen, column);
            }
        }
    }
    for conjunct in lifted {
        for reference in conjunct_tables(conjunct) {
            if reference
                .table
                .as_deref()
                .is_some_and(|t| survivor_names.contains(t))
            {
                add_key(&mut keys, &mut seen, reference);
            }
        }
    }
    if keys.is_empty() {
        None
    } else {
        Some(keys)
    }
}

/// Append a group key once per (qualifier, column).
fn add_key(
    keys: &mut Vec<ColumnRef>,
    seen: &mut HashSet<(Option<String>, String)>,
    reference: &ColumnRef,
) {
    let identity = (reference.table.clone(), reference.column.clone());
    if seen.insert(identity) {
        keys.push(reference.clone());
    }
}

/// (select list, output names, per-SUM synthetic names) for the partial; None on
/// an output-name collision (v1 declines rather than de-duplicating).
fn partial_outputs(
    agg: &Aggregate,
    keys: &[ColumnRef],
) -> Option<(Vec<Expr>, Vec<String>, Vec<String>)> {
    let mut select_list: Vec<Expr> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for key in keys {
        select_list.push(Expr::Column(key.clone()));
        names.push(key.column.clone());
    }
    let mut sum_names = Vec::new();
    for entry in &agg.aggregates {
        // Group passthroughs are already emitted via `keys`; a constant output
        // needs nothing from the partial; only SUM calls compute here.
        if matches!(entry, Expr::Column(_) | Expr::Literal { .. }) {
            continue;
        }
        let name = format!("{EAGER_PREFIX}sum_{}", sum_names.len());
        select_list.push(entry.clone());
        names.push(name.clone());
        sum_names.push(name);
    }
    if has_duplicate(&names) {
        return None;
    }
    Some((select_list, names, sum_names))
}

/// Whether a name list has a repeated entry.
fn has_duplicate(names: &[String]) -> bool {
    let mut seen = HashSet::new();
    !names.iter().all(|name| seen.insert(name))
}

// -------------------------------- final build -------------------------------

/// The final tree: peeled dims joined back onto the partial's derived table, topped
/// by the merge aggregate with the ORIGINAL outputs.
fn build_final(
    agg: &Aggregate,
    partial: &Partial,
    peeled: &[&Scan],
    lifted: &[&Expr],
) -> Result<LogicalPlan, OptimizeError> {
    let mapping = alias_mapping(partial);
    // Rewrite ONCE up front: the ordering walk and the coverage check must see the
    // same qualifiers (the S side of every lifted conjunct now reads the alias).
    let mut pending = rewrite_all(lifted, &mapping);
    let mut current = LogicalPlan::SubqueryScan(SubqueryScan {
        input: Box::new(partial.aggregate.clone()),
        alias: partial.alias.clone(),
        column_names: None,
    });
    let mut in_scope: HashSet<String> = std::iter::once(partial.alias.clone()).collect();
    for scan in joinable_order(peeled, &pending, &in_scope)? {
        in_scope.insert(scan_qualifier(scan));
        let (covered, remaining) = covered_conjuncts(&pending, &in_scope);
        pending = remaining;
        current = inner_join(current, LogicalPlan::Scan(Box::new(scan.clone())), covered);
    }
    require_all_placed(&pending)?;
    Ok(final_aggregate(agg, partial, current, &mapping))
}

/// Rewrite map: every partial group key, addressed by its ORIGINAL qualifier, now
/// reads from the partial's alias under the same name.
fn alias_mapping(partial: &Partial) -> ColumnMapping {
    let mut mapping = ColumnMapping::new();
    for key in &partial.keys {
        let mut replacement = key.clone();
        replacement.table = Some(partial.alias.clone());
        mapping.insert(
            (key.table.clone(), key.column.clone()),
            Expr::Column(replacement),
        );
    }
    mapping
}

/// A (qualifier, column) -> replacement expression map for the alias rewrite.
type ColumnMapping = std::collections::HashMap<(Option<String>, String), Expr>;

/// Every expression with the alias mapping applied.
fn rewrite_all(expressions: &[&Expr], mapping: &ColumnMapping) -> Vec<Expr> {
    expressions
        .iter()
        .map(|expr| apply_mapping((*expr).clone(), mapping))
        .collect()
}

/// One expression with the alias mapping applied.
fn apply_mapping(expr: Expr, mapping: &ColumnMapping) -> Expr {
    replace_column_refs(expr, &|c: &ColumnRef| {
        mapping.get(&(c.table.clone(), c.column.clone())).cloned()
    })
}

/// The peeled dims ordered so each joins something already in scope (a snowflake
/// leaf waits for its parent dim); RAISES when no order exists - a peel subset with
/// an unjoinable dim is a partitioning bug.
fn joinable_order<'a>(
    peeled: &[&'a Scan],
    lifted: &[Expr],
    base_scope: &HashSet<String>,
) -> Result<Vec<&'a Scan>, OptimizeError> {
    let mut remaining: Vec<&Scan> = peeled.to_vec();
    let mut scope = base_scope.clone();
    let mut ordered = Vec::new();
    while !remaining.is_empty() {
        let index = next_joinable(&remaining, lifted, &scope)?;
        let scan = remaining.remove(index);
        scope.insert(scan_qualifier(scan));
        ordered.push(scan);
    }
    Ok(ordered)
}

/// The index of the first peeled dim (original order) reachable from the scope, or
/// a RAISE when none is.
fn next_joinable(
    remaining: &[&Scan],
    lifted: &[Expr],
    scope: &HashSet<String>,
) -> Result<usize, OptimizeError> {
    for (index, scan) in remaining.iter().enumerate() {
        if reaches_scope(scan, lifted, scope) {
            return Ok(index);
        }
    }
    Err(OptimizeError::EagerAggregation(
        "a peeled dim joins nothing in scope; the conjunct partition is inconsistent".to_string(),
    ))
}

/// Whether any lifted conjunct links this dim to the current scope.
fn reaches_scope(scan: &Scan, lifted: &[Expr], scope: &HashSet<String>) -> bool {
    let qualifier = scan_qualifier(scan);
    for conjunct in lifted {
        let names: HashSet<&str> = conjunct_tables(conjunct)
            .iter()
            .filter_map(|reference| reference.table.as_deref())
            .collect();
        if names.contains(qualifier.as_str()) && names.iter().any(|table| scope.contains(*table)) {
            return true;
        }
    }
    false
}

/// Every lifted conjunct must be placed exactly once; a dropped predicate
/// manufactures wrong results - crash instead.
fn require_all_placed(pending: &[Expr]) -> Result<(), OptimizeError> {
    if pending.is_empty() {
        return Ok(());
    }
    Err(OptimizeError::EagerAggregation(format!(
        "dropped {} join conjunct(s); every lifted conjunct must be placed exactly once",
        pending.len()
    )))
}

/// The merge aggregate: original group keys (S-side ones rewritten to the alias),
/// each SUM merged as SUM of its partial column, original output names preserved.
fn final_aggregate(
    agg: &Aggregate,
    partial: &Partial,
    joined: LogicalPlan,
    mapping: &ColumnMapping,
) -> LogicalPlan {
    let group_by = agg
        .group_by
        .iter()
        .map(|key| apply_mapping(key.clone(), mapping))
        .collect();
    let mut select_list = Vec::new();
    let mut sum_names = partial.sums.iter();
    for entry in &agg.aggregates {
        select_list.push(merge_entry(entry, &partial.alias, mapping, &mut sum_names));
    }
    LogicalPlan::Aggregate(Aggregate {
        input: Box::new(joined),
        group_by,
        aggregates: select_list,
        output_names: agg.output_names.clone(),
        grouping_sets: None,
    })
}

/// One final-aggregate output: a constant tag survives unchanged, a group column is
/// rewritten to the alias, and a SUM merges over its partial column.
fn merge_entry(
    entry: &Expr,
    alias: &str,
    mapping: &ColumnMapping,
    sum_names: &mut std::slice::Iter<String>,
) -> Expr {
    if matches!(entry, Expr::Literal { .. }) {
        return entry.clone();
    }
    if matches!(entry, Expr::Column(_)) {
        return apply_mapping(entry.clone(), mapping);
    }
    let sum_name = sum_names
        .next()
        .expect("each SUM entry consumes exactly one synthetic partial name");
    merge_sum(entry, alias, sum_name)
}

/// The merge SUM: the original call with its argument swapped for the partial's
/// synthetic column (distinct/within-group flags are gate-guaranteed empty).
fn merge_sum(entry: &Expr, alias: &str, sum_name: &str) -> Expr {
    let Expr::FunctionCall {
        function_name,
        is_aggregate,
        distinct,
        within_group_desc,
        ..
    } = entry
    else {
        unreachable!("sum_only_outputs guarantees a plain SUM FunctionCall here");
    };
    let partial_ref = Expr::Column(ColumnRef {
        table: Some(alias.to_string()),
        column: sum_name.to_string(),
        data_type: Some(entry.get_type()),
    });
    Expr::FunctionCall {
        function_name: function_name.clone(),
        args: vec![partial_ref],
        is_aggregate: *is_aggregate,
        distinct: *distinct,
        within_group_key: None,
        within_group_desc: *within_group_desc,
    }
}

#[cfg(test)]
mod tests;
