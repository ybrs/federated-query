//! Semi-join reduction: read the build side, collect its distinct join key, and
//! inject it into the probe scan as `col IN (...)` so the big read shrinks before
//! transfer, while STILL producing the coordinator hash-join fragment (the join
//! enforces exactness - injection is only a reduction). Ports the reduction /
//! orientation / tracing / winners machinery of `executor/rust_ir.py`.
//!
//! Not implemented here: the `injected_sql` source-side key-filter placement inside
//! an island / aggregate probe base (see `scan_spec::injected_probe_spec`); the
//! engine wraps the base output with the IN filter instead, which is equally correct.

use std::collections::HashMap;

use fq_optimize::useless_key_reduction;
use fq_plan::physical::{
    output_column_names, physical_column_name, BuildSide, ColumnAliasMap, PhysicalHashJoin,
    PhysicalPlan, PhysicalProjection, PhysicalScan, PhysicalSetOperation, PhysicalUnion,
};
use fq_plan::{Expr, JoinType, SetOpKind};

use crate::planner::orient::{orientation_rows, Side};

use super::cse::{emit_step_once, scan_share_key};
use super::emit_nodes::{emit, source_datasource};
use super::error::StepError;
use super::scan_spec::{injectable_scan, injected_probe_spec, source_scan_spec};
use super::types::{ExtraInjection, Step};
use super::{node_id, observe, same_node, Ctx, NodeId};

/// The connection-scoped cap on collected distinct keys (must match the fedqrs
/// engine's `DYN_KEYS_TEMP_TABLE` fill cap). Ports `_DYNAMIC_FILTER_MAX_KEYS`.
pub const DYNAMIC_FILTER_MAX_KEYS: usize = 2000;

/// A reduction candidate donating keys into a traced base. Ports the winners-dict
/// entry (`_note_candidate`): the build subtree, its join key, the inject column per
/// traced base, and a key-count proxy score (fewest donated keys first).
#[derive(Clone)]
pub(crate) struct Candidate<'p> {
    build: &'p PhysicalPlan,
    build_key: &'p Expr,
    columns: HashMap<NodeId, String>,
    score: u64,
}

/// The emitted build side of one reduction: its distinct-keys binding and the
/// physical key column those keys are named by.
struct Reduction<'p> {
    build: &'p PhysicalPlan,
    keys_binding: String,
    build_key: String,
}

// ---------------------------------------------------------------------------
// reduction gates
// ---------------------------------------------------------------------------

/// Whether the semi-join reduction applies to a join: single-key, and at least one
/// injectable side. SEMI donates the existential right and needs a plain-scan left
/// probe; LEFT reduces only the nullable right; INNER needs one injectable side.
/// Ports `_can_reduce`.
pub(super) fn can_reduce(join: &PhysicalHashJoin) -> bool {
    if join.left_keys.len() != 1 || join.right_keys.len() != 1 {
        return false;
    }
    match join.join_type {
        JoinType::Semi => probe_preference(&join.left) > 0,
        JoinType::Left => left_reducible(join),
        JoinType::Inner => {
            injection_rank(&join.left, &join.left_keys[0]) > 0
                || injection_rank(&join.right, &join.right_keys[0]) > 0
        }
        _ => false,
    }
}

/// Whether a LEFT join's nullable right side can be key-reduced by the preserved
/// left's keys: a single equi key whose injectable base preserves it. Ports
/// `_left_reducible`.
fn left_reducible(join: &PhysicalHashJoin) -> bool {
    if join.left_keys.len() != 1 || join.right_keys.len() != 1 {
        return false;
    }
    let inject_col = key_physical(&join.right_keys[0], &join.right.column_aliases());
    reducible_probe_base(&join.right, &inject_col).is_some()
}

/// Whether the oriented probe descends to an injectable base for the inject column
/// (the reduced emission requires it; an INNER probe can itself be a join). Ports
/// `_probe_base_resolvable`.
pub(super) fn probe_base_resolvable(join: &PhysicalHashJoin) -> bool {
    let (_, probe, _, probe_key) = orient_join(join);
    probe_injection_bases(probe, probe_key).is_some()
}

/// The cost-based usefulness gate: false when the build side's distinct keys cover
/// the probe column's whole value domain (an unfiltered dimension's FK domain -
/// the injection keeps every probe row and costs pure overhead). A missing statistic
/// keeps reduce-by-default. Ports `_reduction_filters`.
pub(super) fn reduction_filters(join: &PhysicalHashJoin) -> bool {
    let (build, probe, build_key, probe_key) = orient_join(join);
    if build_donates_whole_domain(build) {
        return false;
    }
    let bases = probe_injection_bases(probe, probe_key);
    let probe_ndv = bases
        .as_ref()
        .and_then(|bases| bases.first())
        .and_then(|(base, inject_col)| node_column_ndv(base, inject_col));
    let keys_ndv = node_column_ndv(build, &build_key_name(build, build_key));
    let build_rows = output_rows(build);
    if matches!(build, PhysicalPlan::RemoteQuery(_)) && build_rows.is_none() {
        return true;
    }
    !useless_key_reduction(as_u64(keys_ndv), build_rows, as_u64(probe_ndv))
}

/// An UNFILTERED plain base scan donates its entire key domain, so under FK
/// containment the injection keeps every probe row (pure overhead). Ports
/// `_build_donates_whole_domain`.
fn build_donates_whole_domain(build: &PhysicalPlan) -> bool {
    let PhysicalPlan::Scan(scan) = build else {
        return false;
    };
    if scan.filters.is_some() || scan.sample.is_some() {
        return false;
    }
    !scan_folds_grouping(scan)
}

// ---------------------------------------------------------------------------
// injection ranking / preference
// ---------------------------------------------------------------------------

/// A node's injection rank: `probe_preference` extended so a composite whose key
/// traces to injectable bases ranks like a plain scan. Ports `_injection_rank`.
fn injection_rank(node: &PhysicalPlan, key_expr: &Expr) -> i64 {
    let rank = probe_preference(node);
    if rank > 0 {
        return rank;
    }
    if probe_injection_bases(node, key_expr).is_some() {
        return 1;
    }
    0
}

/// Rank a node as an injection target: a pushed remote query first (the collapsed
/// fact island), then a plain scan, then nothing. Ports `_probe_preference`.
fn probe_preference(node: &PhysicalPlan) -> i64 {
    if matches!(node, PhysicalPlan::RemoteQuery(_)) {
        return 2;
    }
    if injectable_scan(node) {
        return 1;
    }
    0
}

// ---------------------------------------------------------------------------
// orientation
// ---------------------------------------------------------------------------

/// Pick (build, probe, build_key, probe_key). SEMI/LEFT have fixed roles; INNER is
/// cost-based (reduce the larger side seen through derived/union wrappers), then a
/// build-side fallback, then an injection-rank compare with a size-urgency
/// tie-break. Ports `_orient_join`.
fn orient_join(join: &PhysicalHashJoin) -> (&PhysicalPlan, &PhysicalPlan, &Expr, &Expr) {
    let left = join.left.as_ref();
    let right = join.right.as_ref();
    let (lk, rk) = (&join.left_keys[0], &join.right_keys[0]);
    match join.join_type {
        JoinType::Semi => return (right, left, rk, lk),
        JoinType::Left => return (left, right, lk, rk),
        _ => {}
    }
    if let Some(probe) = cardinality_probe(join) {
        if same_node(probe, right) {
            return (left, right, lk, rk);
        }
        if same_node(probe, left) {
            return (right, left, rk, lk);
        }
    }
    orient_inner_ranked(join, left, right, lk, rk)
}

/// The rank/urgency branch of INNER orientation (after the cardinality probe
/// declines). Ports the tail of `_orient_join`.
fn orient_inner_ranked<'p>(
    join: &'p PhysicalHashJoin,
    left: &'p PhysicalPlan,
    right: &'p PhysicalPlan,
    lk: &'p Expr,
    rk: &'p Expr,
) -> (&'p PhysicalPlan, &'p PhysicalPlan, &'p Expr, &'p Expr) {
    if injectable_scan(left) && injectable_scan(right) {
        if join.build_side == BuildSide::Left {
            return (left, right, lk, rk);
        }
        return (right, left, rk, lk);
    }
    let left_rank = injection_rank(left, lk);
    let right_rank = injection_rank(right, rk);
    if left_rank == right_rank {
        if side_urgency(left, lk) > side_urgency(right, rk) {
            return (right, left, rk, lk);
        }
        return (left, right, lk, rk);
    }
    if right_rank > left_rank {
        return (left, right, lk, rk);
    }
    (right, left, rk, lk)
}

/// The larger injectable side to reduce, seen through derived-table / union
/// wrappers, or None to fall back. Ports `_cardinality_probe`.
fn cardinality_probe(join: &PhysicalHashJoin) -> Option<&PhysicalPlan> {
    let left = join.left.as_ref();
    let right = join.right.as_ref();
    let larger = larger_derived_side(left, right)?;
    let key = if same_node(larger, left) {
        &join.left_keys[0]
    } else {
        &join.right_keys[0]
    };
    if injection_rank(larger, key) > 0 {
        Some(larger)
    } else {
        None
    }
}

/// The bigger of two join sides by derived size, or None when either is unjudgeable
/// or they tie. Ports `_larger_derived_side`.
fn larger_derived_side<'p>(
    left: &'p PhysicalPlan,
    right: &'p PhysicalPlan,
) -> Option<&'p PhysicalPlan> {
    match (derived_side_rows(left), derived_side_rows(right)) {
        (Some(left_rows), Some(right_rows)) if left_rows != right_rows => {
            Some(if right_rows > left_rows { right } else { left })
        }
        _ => None,
    }
}

/// A join side's size, descending through size-preserving wrappers and summing a
/// union's branches; None on a collapsing node. Ports `_derived_side_rows`.
fn derived_side_rows(node: &PhysicalPlan) -> Option<u64> {
    if let Some(direct) = orientation_rows(node) {
        return Some(direct);
    }
    if let Some(passthrough) = passthrough_input(node) {
        return derived_side_rows(passthrough);
    }
    if matches!(node, PhysicalPlan::Union(_) | PhysicalPlan::SetOperation(_)) {
        return summed_side_rows(node);
    }
    None
}

/// The summed derived size of a union's branches, or None if any is unjudgeable.
/// Ports `_summed_side_rows`.
fn summed_side_rows(node: &PhysicalPlan) -> Option<u64> {
    let mut total = 0u64;
    for child in node.children() {
        total += derived_side_rows(child)?;
    }
    Some(total)
}

/// The single input a SIZE-PRESERVING wrapper exposes (derived table, filter, or a
/// non-DISTINCT projection). Ports `_passthrough_input`.
fn passthrough_input(node: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match node {
        PhysicalPlan::AliasedRelation(aliased) => Some(&aliased.input),
        PhysicalPlan::Filter(filter) => Some(&filter.input),
        PhysicalPlan::Projection(projection)
            if !projection.distinct && projection.distinct_on.is_none() =>
        {
            Some(&projection.input)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// size / urgency helpers
// ---------------------------------------------------------------------------

/// A node's own row estimate field (NOT a remote's real output estimate). Ports the
/// `getattr(node, "estimated_rows", None)` reads.
fn node_estimated_rows(node: &PhysicalPlan) -> Option<u64> {
    match node {
        PhysicalPlan::Scan(scan) => scan.estimated_rows,
        PhysicalPlan::RemoteQuery(remote) => remote.estimated_rows,
        PhysicalPlan::HashJoin(join) => join.estimated_rows,
        PhysicalPlan::NestedLoopJoin(join) => join.estimated_rows,
        _ => None,
    }
}

/// The rows a node actually produces: a remote query's real OUTPUT estimate, any
/// other node's threaded estimate. Ports `_output_rows`.
fn output_rows(node: &PhysicalPlan) -> Option<u64> {
    match node {
        PhysicalPlan::RemoteQuery(remote) => remote.output_estimated_rows,
        other => node_estimated_rows(other),
    }
}

/// `estimated_rows` when threaded, else -1: an unknown side cannot be known big. A
/// statless remote exposes its widest column NDV as a size floor. Ports `_known_rows`.
fn known_rows(node: &PhysicalPlan) -> i64 {
    if let Some(rows) = node_estimated_rows(node) {
        return clamp_i64(rows);
    }
    if let PhysicalPlan::RemoteQuery(remote) = node {
        if let Some(ndv) = &remote.column_ndv {
            if let Some(max) = ndv.values().max() {
                return *max;
            }
        }
    }
    -1
}

/// How much a side needs to be the reduced probe: its known size floor, or
/// effectively infinite for a statless remote island. Ports `_probe_urgency`.
fn probe_urgency(node: &PhysicalPlan) -> i64 {
    let rows = known_rows(node);
    if rows < 0 && matches!(node, PhysicalPlan::RemoteQuery(_)) {
        return 1i64 << 62;
    }
    rows
}

/// A side's probe urgency, derived from itself or - when it is an unjudgeable
/// composite - from the base relations its key traces to. Ports `_side_urgency`.
fn side_urgency(node: &PhysicalPlan, key_expr: &Expr) -> i64 {
    let urgency = probe_urgency(node);
    if urgency >= 0 {
        return urgency;
    }
    let Some(bases) = probe_injection_bases(node, key_expr) else {
        return urgency;
    };
    let mut best = urgency;
    for (base, _) in &bases {
        best = best.max(probe_urgency(base));
    }
    best
}

// ---------------------------------------------------------------------------
// probe base tracing
// ---------------------------------------------------------------------------

/// [(base, inject column)] for a probe: the legacy single-base resolution first,
/// else the traced descent through join cascades / decorrelation wrappers / unions.
/// None = untraceable. Ports `_probe_injection_bases`.
fn probe_injection_bases<'p>(
    probe: &'p PhysicalPlan,
    key_expr: &Expr,
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    let inject_col = key_physical(key_expr, &probe.column_aliases());
    if let Some(base) = reducible_probe_base(probe, &inject_col) {
        return Some(vec![(base, inject_col)]);
    }
    traced_injection_bases(probe, &key_pair_of(key_expr))
}

/// The single-source base a probe reduces to, descending through pure single-input
/// wrappers, IF the inject column survives unchanged. Ports `_reducible_probe_base`.
fn reducible_probe_base<'p>(probe: &'p PhysicalPlan, inject_col: &str) -> Option<&'p PhysicalPlan> {
    let mut node = probe;
    while !is_probe_base(node) {
        node = single_wrapper_input(node)?;
    }
    if base_output_names(node)
        .iter()
        .any(|name| name == inject_col)
    {
        Some(node)
    } else {
        None
    }
}

/// A node the engine can inject a dynamic filter into directly: a scan or a pushed
/// remote query. Ports `_is_probe_base`.
fn is_probe_base(node: &PhysicalPlan) -> bool {
    matches!(node, PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_))
}

/// The sole input of a column-passthrough wrapper (alias/projection). Ports
/// `_single_wrapper_input`.
fn single_wrapper_input(node: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match node {
        PhysicalPlan::AliasedRelation(aliased) => Some(&aliased.input),
        PhysicalPlan::Projection(projection) => Some(&projection.input),
        _ => None,
    }
}

/// The output column names of a probe base. Ports `_base_output_names`.
fn base_output_names(node: &PhysicalPlan) -> Vec<String> {
    let mut names = Vec::new();
    for name in node.column_aliases().values() {
        names.push(name.clone());
    }
    names
}

/// The base scan(s) that ORIGINATE the probe key column, descending only where
/// removing base rows lacking the keys cannot change what the reduction join sees.
/// Ports `_traced_injection_bases`.
fn traced_injection_bases<'p>(
    node: &'p PhysicalPlan,
    key_pair: &(Option<String>, String),
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    match node {
        PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_) => traced_base_entry(node, key_pair),
        PhysicalPlan::AliasedRelation(aliased) => {
            let inner = alias_inner_pair(node, &aliased.input, key_pair)?;
            traced_injection_bases(&aliased.input, &inner)
        }
        PhysicalPlan::Projection(projection) => {
            let inner = projection_inner_pair(node, projection, key_pair)?;
            traced_injection_bases(&projection.input, &inner)
        }
        PhysicalPlan::Filter(filter) => traced_injection_bases(&filter.input, key_pair),
        PhysicalPlan::HashJoin(join) => join_injection_bases(join, key_pair),
        PhysicalPlan::Union(union) => union_injection_bases(node, union, key_pair),
        PhysicalPlan::SetOperation(set_op) => setop_injection_bases(node, set_op, key_pair),
        _ => None,
    }
}

/// [(base, physical column)] when a PLAIN injectable base exposes the key; an
/// aggregate/limited scan only reduces via the legacy vetted shape. Ports
/// `_traced_base_entry`.
fn traced_base_entry<'p>(
    base: &'p PhysicalPlan,
    key_pair: &(Option<String>, String),
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    if matches!(base, PhysicalPlan::Scan(_)) && !injectable_scan(base) {
        return None;
    }
    let physical = base.column_aliases().get(key_pair)?.clone();
    Some(vec![(base, physical)])
}

/// Translate the key through an alias wrapper to its single input column, or None
/// when hidden/ambiguous. Ports `_alias_inner_pair`.
fn alias_inner_pair(
    node: &PhysicalPlan,
    input: &PhysicalPlan,
    key_pair: &(Option<String>, String),
) -> Option<(Option<String>, String)> {
    let physical = node.column_aliases().get(key_pair)?.clone();
    unique_source_pair(input, &physical)
}

/// The single (qualifier, column) of `input` whose output name is `physical`, or
/// None when hidden/ambiguous.
fn unique_source_pair(input: &PhysicalPlan, physical: &str) -> Option<(Option<String>, String)> {
    let mut matches = Vec::new();
    for (pair, name) in input.column_aliases() {
        if name == physical {
            matches.push(pair);
        }
    }
    if matches.len() != 1 {
        return None;
    }
    matches.into_iter().next()
}

/// Translate the key through a projection to the input column it re-exposes, or None
/// (computed, ambiguous, DISTINCT ON, or star). Ports `_projection_inner_pair`.
fn projection_inner_pair(
    node: &PhysicalPlan,
    projection: &PhysicalProjection,
    key_pair: &(Option<String>, String),
) -> Option<(Option<String>, String)> {
    if projection.distinct_on.is_some() {
        return None;
    }
    if !node.column_aliases().contains_key(key_pair) {
        return None;
    }
    let mut matches = Vec::new();
    for (index, name) in projection.output_names.iter().enumerate() {
        if name == &key_pair.1 {
            matches.push(&projection.expressions[index]);
        }
    }
    if matches.len() != 1 {
        return None;
    }
    match matches[0] {
        Expr::Column(col) if col.column != "*" => Some((col.table.clone(), col.column.clone())),
        _ => None,
    }
}

/// Descend into the single join side carrying the key, when that side is preserved
/// (not null-extended). Ports `_join_injection_bases`.
fn join_injection_bases<'p>(
    join: &'p PhysicalHashJoin,
    key_pair: &(Option<String>, String),
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    let mut sides = Vec::new();
    if join.left.column_aliases().contains_key(key_pair) {
        sides.push(Side::Left);
    }
    if join.right.column_aliases().contains_key(key_pair) {
        sides.push(Side::Right);
    }
    if sides.len() != 1 {
        return None;
    }
    let side = sides[0];
    if !injection_side_safe(join.join_type, side) {
        return None;
    }
    let child = match side {
        Side::Left => &join.left,
        Side::Right => &join.right,
    };
    traced_injection_bases(child, key_pair)
}

/// Whether a join side is preserved (not null-extended) so its base may be
/// pre-filtered by a key superset. FULL/CROSS extend both: never. Ports
/// `_injection_side_safe`.
fn injection_side_safe(join_type: JoinType, side: Side) -> bool {
    match join_type {
        JoinType::Inner => true,
        JoinType::Left | JoinType::Semi | JoinType::Anti => side == Side::Left,
        JoinType::Right => side == Side::Right,
        JoinType::Full | JoinType::Cross => false,
    }
}

/// Descend into every union branch that traces the key. Ports `_union_injection_bases`.
fn union_injection_bases<'p>(
    node: &'p PhysicalPlan,
    union: &'p PhysicalUnion,
    key_pair: &(Option<String>, String),
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    if !node.column_aliases().contains_key(key_pair) {
        return None;
    }
    let position = union_position(node, key_pair)?;
    let mut branches: Vec<&PhysicalPlan> = Vec::new();
    for branch in &union.inputs {
        branches.push(branch);
    }
    traced_branches(&branches, position)
}

/// Descend into both branches of a binary UNION (INTERSECT/EXCEPT never traced).
/// Ports `_setop_injection_bases`.
fn setop_injection_bases<'p>(
    node: &'p PhysicalPlan,
    set_op: &'p PhysicalSetOperation,
    key_pair: &(Option<String>, String),
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    if set_op.kind != SetOpKind::Union || !node.column_aliases().contains_key(key_pair) {
        return None;
    }
    let position = union_position(node, key_pair)?;
    let branches: Vec<&PhysicalPlan> = vec![&set_op.left, &set_op.right];
    traced_branches(&branches, position)
}

/// The output position of a union key column by name. Ports the `schema().names.index`
/// lookup, but by name (no source probe).
fn union_position(node: &PhysicalPlan, key_pair: &(Option<String>, String)) -> Option<usize> {
    output_column_names(node)
        .iter()
        .position(|name| name == &key_pair.1)
}

/// Trace each branch that resolves the union-position column; None if none do. Ports
/// the shared branch loop of `_union_injection_bases` / `_setop_injection_bases`.
fn traced_branches<'p>(
    branches: &[&'p PhysicalPlan],
    position: usize,
) -> Option<Vec<(&'p PhysicalPlan, String)>> {
    let mut bases = Vec::new();
    for branch in branches {
        let Some(pair) = branch_key_pair(branch, position) else {
            continue;
        };
        if let Some(traced) = traced_injection_bases(branch, &pair) {
            bases.extend(traced);
        }
    }
    if bases.is_empty() {
        None
    } else {
        Some(bases)
    }
}

/// The (qualifier, column) naming a branch's output at a union position, or None
/// when hidden/ambiguous. Ports `_branch_key_pair` + `_branch_identity_pair`.
fn branch_key_pair(branch: &PhysicalPlan, position: usize) -> Option<(Option<String>, String)> {
    let names = output_column_names(branch);
    let physical = names.get(position)?.clone();
    let mut matches = Vec::new();
    for (pair, name) in branch.column_aliases() {
        if name == physical {
            matches.push(pair);
        }
    }
    if matches.len() == 1 {
        return matches.into_iter().next();
    }
    branch_identity_pair(&matches, &physical)
}

/// Resolve an ambiguous branch column to its output-alias identity `(None, out)`.
/// Ports `_branch_identity_pair`.
fn branch_identity_pair(
    matches: &[(Option<String>, String)],
    physical: &str,
) -> Option<(Option<String>, String)> {
    let mut identity = Vec::new();
    for pair in matches {
        if pair.1 == physical {
            identity.push(pair.clone());
        }
    }
    if identity.len() == 1 {
        identity.into_iter().next()
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// injection-winner pre-pass
// ---------------------------------------------------------------------------

/// Per traced-base id, the reduction candidate donating the FEWEST keys (fewest
/// first). Runs BEFORE emission because emission is bottom-up and the innermost join
/// would otherwise claim a shared base regardless of selectivity. Ports
/// `_injection_winners`.
pub(super) fn injection_winners(plan: &PhysicalPlan) -> HashMap<NodeId, Vec<Candidate<'_>>> {
    let mut winners: HashMap<NodeId, Vec<Candidate<'_>>> = HashMap::new();
    let mut nodes = Vec::new();
    walk_plan_nodes(plan, &mut nodes);
    for node in nodes {
        if let PhysicalPlan::HashJoin(join) = node {
            if reduction_applies(join) {
                note_candidate(&mut winners, join);
            }
        }
    }
    winners
}

/// Per traced base, the inject column of the WINNING candidate (fewest keys).
/// The EXPLAIN-facing projection of `injection_winners`: base node address ->
/// the column execution will constrain with `IN (build keys)`.
pub(super) fn winning_injections(plan: &PhysicalPlan) -> HashMap<NodeId, String> {
    let mut columns = HashMap::new();
    for (base, candidates) in injection_winners(plan) {
        let Some(winner) = candidates.first() else {
            continue;
        };
        if let Some(column) = winner.columns.get(&base) {
            columns.insert(base, column.clone());
        }
    }
    columns
}

/// Yield every node of a physical plan tree (pre-order). Ports `_walk_plan_nodes`.
fn walk_plan_nodes<'p>(node: &'p PhysicalPlan, out: &mut Vec<&'p PhysicalPlan>) {
    out.push(node);
    for child in node.children() {
        walk_plan_nodes(child, out);
    }
}

/// Whether the emitter will take the reduced path for this join. Ports
/// `_reduction_applies`.
fn reduction_applies(join: &PhysicalHashJoin) -> bool {
    can_reduce(join) && probe_base_resolvable(join) && reduction_filters(join)
}

/// Record this join's reduction against every base it traces to (skipping bases its
/// build subtree contains). Ports `_note_candidate`.
fn note_candidate<'p>(
    winners: &mut HashMap<NodeId, Vec<Candidate<'p>>>,
    join: &'p PhysicalHashJoin,
) {
    let (build, probe, build_key, probe_key) = orient_join(join);
    let Some(bases) = probe_injection_bases(probe, probe_key) else {
        return;
    };
    let mut columns: HashMap<NodeId, String> = HashMap::new();
    for (base, inject_col) in &bases {
        columns.insert(node_id(base), inject_col.clone());
    }
    let score = candidate_score(build);
    for (base, _) in &bases {
        if subtree_contains(build, base) {
            continue;
        }
        add_candidate(winners, node_id(base), build, build_key, &columns, score);
    }
}

/// Append a candidate to a base's winner list (unless an equivalent one is listed),
/// keeping the list sorted by score ascending. Ports the tail of `_note_candidate`.
fn add_candidate<'p>(
    winners: &mut HashMap<NodeId, Vec<Candidate<'p>>>,
    base: NodeId,
    build: &'p PhysicalPlan,
    build_key: &'p Expr,
    columns: &HashMap<NodeId, String>,
    score: u64,
) {
    let candidate = Candidate {
        build,
        build_key,
        columns: columns.clone(),
        score,
    };
    let entries = winners.entry(base).or_default();
    if !candidate_known(entries, &candidate) {
        entries.push(candidate);
        entries.sort_by_key(|entry| entry.score);
    }
}

/// Whether an equivalent candidate (same build node) is already listed. Ports
/// `_candidate_known`.
fn candidate_known(entries: &[Candidate<'_>], candidate: &Candidate<'_>) -> bool {
    entries
        .iter()
        .any(|entry| same_node(entry.build, candidate.build))
}

/// A candidate's key-count proxy: the build's output estimate, else infinite. Ports
/// `_candidate_score`.
fn candidate_score(build: &PhysicalPlan) -> u64 {
    output_rows(build)
        .or_else(|| node_estimated_rows(build))
        .unwrap_or(1u64 << 62)
}

/// Whether `target` (by identity) appears in `node`'s subtree. Ports `_subtree_contains`.
fn subtree_contains(node: &PhysicalPlan, target: &PhysicalPlan) -> bool {
    let mut nodes = Vec::new();
    walk_plan_nodes(node, &mut nodes);
    nodes.iter().any(|candidate| same_node(candidate, target))
}

// ---------------------------------------------------------------------------
// reduced-join emission
// ---------------------------------------------------------------------------

/// The build/collect/inject reduction path; returns (left, right) bindings oriented
/// back to the join's positions. Ports `_emit_reduced_join`.
pub(super) fn emit_reduced_join<'p>(
    join: &'p PhysicalHashJoin,
    ctx: &mut Ctx<'p>,
) -> Result<(String, String), StepError> {
    let (build_child, probe_child, build_key, probe_key) = orient_join(join);
    let (build_binding, probe_binding) =
        emit_reduced_inputs(ctx, build_child, probe_child, build_key, probe_key)?;
    let left = join.left.as_ref();
    let right = join.right.as_ref();
    let left_binding = if same_node(build_child, left) {
        build_binding.clone()
    } else {
        probe_binding.clone()
    };
    let right_binding = if same_node(build_child, right) {
        build_binding
    } else {
        probe_binding
    };
    Ok((left_binding, right_binding))
}

/// Emit the build side, its distinct keys, and the injected probe scan. Ports
/// `_emit_reduced_inputs`.
fn emit_reduced_inputs<'p>(
    ctx: &mut Ctx<'p>,
    build_child: &'p PhysicalPlan,
    probe_child: &'p PhysicalPlan,
    build_key: &'p Expr,
    probe_key: &Expr,
) -> Result<(String, String), StepError> {
    let build_binding = emit_build_side(ctx, build_child)?;
    let (keys_binding, anchor_key) = emit_keys_for(ctx, build_child, &build_binding, build_key);
    let reduction = Reduction {
        build: build_child,
        keys_binding,
        build_key: anchor_key,
    };
    let probe_binding = emit_probe(ctx, probe_child, probe_key, &reduction)?;
    Ok((build_binding, probe_binding))
}

/// The build input's binding: a dedicated materialized read for a plain scan, else
/// the subtree's own emission. Cached by node identity. Ports `_emit_build_side`.
fn emit_build_side<'p>(
    ctx: &mut Ctx<'p>,
    build_child: &'p PhysicalPlan,
) -> Result<String, StepError> {
    if let Some(cached) = ctx.build_bindings.get(&node_id(build_child)) {
        return Ok(cached.clone());
    }
    let binding = if matches!(build_child, PhysicalPlan::Scan(_)) {
        emit_build_scan(ctx, build_child)?
    } else {
        emit(build_child, ctx)?
    };
    ctx.build_bindings
        .insert(node_id(build_child), binding.clone());
    Ok(binding)
}

/// A fully materialized build-side scan (also distinct-scanned), sharing an
/// identical read. Ports `_emit_build_scan`.
fn emit_build_scan(ctx: &mut Ctx<'_>, build_child: &PhysicalPlan) -> Result<String, StepError> {
    let binding = ctx.names.binding();
    let step = Step::SourceScan {
        datasource: source_datasource(build_child)?,
        scan: source_scan_spec(build_child)?,
        binding,
        materialize: true,
    };
    let key = scan_share_key(build_child, &step);
    let shared = emit_step_once(ctx, step, Some(key));
    ctx.scan_bindings
        .insert(node_id(build_child), shared.clone());
    Ok(shared)
}

/// (keys binding, key name) of a build side's distinct join keys, anchored to the
/// originating base scan when traceable. Ports `_emit_keys_for`.
fn emit_keys_for<'p>(
    ctx: &mut Ctx<'p>,
    build_child: &'p PhysicalPlan,
    build_binding: &str,
    build_key: &'p Expr,
) -> (String, String) {
    let (anchor_binding, anchor_key) = match collect_anchor(ctx, build_child, build_key) {
        Some(anchor) => anchor,
        None => (
            build_binding.to_string(),
            key_physical(build_key, &build_child.column_aliases()),
        ),
    };
    let keys_binding = emit_collect_distinct(ctx, &anchor_binding, &anchor_key);
    observe::record_ndv_observation(ctx, build_child, build_key, &keys_binding);
    (keys_binding, anchor_key)
}

/// (binding, physical column) of the base relation originating the build key, or
/// None when the build IS a base or the key is not traceable. Ports `_collect_anchor`.
fn collect_anchor(
    ctx: &Ctx<'_>,
    build_child: &PhysicalPlan,
    build_key: &Expr,
) -> Option<(String, String)> {
    if matches!(
        build_child,
        PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_)
    ) {
        return None;
    }
    let scan = originating_relation(build_child, build_key)?;
    let binding = ctx.scan_bindings.get(&node_id(scan))?.clone();
    Some((binding, key_physical(build_key, &scan.column_aliases())))
}

/// The base scan/remote exposing the key column under its own qualifier. Ports
/// `_originating_relation`. Shared with the NDV observation recorder.
pub(super) fn originating_relation<'p>(
    node: &'p PhysicalPlan,
    key_expr: &Expr,
) -> Option<&'p PhysicalPlan> {
    let key_pair = key_pair_of(key_expr);
    if matches!(node, PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_)) {
        if node.column_aliases().contains_key(&key_pair) {
            return Some(node);
        }
        return None;
    }
    for child in node.children() {
        if let Some(found) = originating_relation(child, key_expr) {
            return Some(found);
        }
    }
    None
}

/// Collect the build side's distinct join-key values, capped. Ports
/// `_emit_collect_distinct`.
fn emit_collect_distinct(ctx: &mut Ctx<'_>, input_binding: &str, key: &str) -> String {
    let binding = ctx.names.binding();
    let step = Step::CollectDistinct {
        input: input_binding.to_string(),
        key: key.to_string(),
        cap: DYNAMIC_FILTER_MAX_KEYS,
        binding,
    };
    emit_step_once(ctx, step, None)
}

/// Emit the probe with the build keys injected into its base scan(s). Ports
/// `_emit_probe`.
fn emit_probe<'p>(
    ctx: &mut Ctx<'p>,
    probe_child: &'p PhysicalPlan,
    probe_key: &Expr,
    reduction: &Reduction<'p>,
) -> Result<String, StepError> {
    let Some(bases) = probe_injection_bases(probe_child, probe_key) else {
        // The gate chose the reduced path; reaching here means gate and emission
        // disagree - a loud raise, never a silent wrong plan.
        return Err(StepError::LostInjectionBase);
    };
    for &(base, ref inject_col) in &bases {
        if let Some(cached) = ctx.injected.get(&node_id(base)) {
            let cached = cached.clone();
            if same_node(base, probe_child) {
                return Ok(cached);
            }
            continue;
        }
        let base_binding = emit_base_injection(ctx, base, inject_col, reduction)?;
        ctx.injected.insert(node_id(base), base_binding.clone());
        if same_node(base, probe_child) {
            return Ok(base_binding);
        }
    }
    emit(probe_child, ctx)
}

/// One injected read of a base: the most selective planned candidate is the PRIMARY
/// injection, further candidates ride as bounded extra IN lists. Ports
/// `_emit_base_injection`.
fn emit_base_injection<'p>(
    ctx: &mut Ctx<'p>,
    base: &'p PhysicalPlan,
    inject_col: &str,
    reduction: &Reduction<'p>,
) -> Result<String, StepError> {
    let candidates = ctx
        .injection_winners
        .get(&node_id(base))
        .cloned()
        .unwrap_or_default();
    let mut keys_binding = reduction.keys_binding.clone();
    let mut build_key = reduction.build_key.clone();
    let mut column = inject_col.to_string();
    if let Some(first) = candidates.first() {
        if !same_node(first.build, reduction.build) {
            let (winner_binding, winner_key) = winner_keys(ctx, first)?;
            keys_binding = winner_binding;
            build_key = winner_key;
            column = candidate_column(first, base);
        }
    }
    let extras = extra_injections(ctx, base, &column, &candidates)?;
    emit_injected_scan(ctx, base, &column, &keys_binding, &build_key, extras)
}

/// The inject column a candidate donates to a base (present by construction). Ports
/// the `candidate["columns"][id(base)]` reads.
fn candidate_column(candidate: &Candidate<'_>, base: &PhysicalPlan) -> String {
    candidate
        .columns
        .get(&node_id(base))
        .cloned()
        .expect("a candidate registered against a base carries its inject column")
}

/// The runner-up candidates as extra (column, keys) injections, capped at two. Ports
/// `_extra_injections`.
fn extra_injections<'p>(
    ctx: &mut Ctx<'p>,
    base: &'p PhysicalPlan,
    primary_column: &str,
    candidates: &[Candidate<'p>],
) -> Result<Vec<ExtraInjection>, StepError> {
    let mut extras = Vec::new();
    for candidate in candidates.iter().skip(1) {
        if extras.len() == 2 {
            break;
        }
        let column = candidate_column(candidate, base);
        if column == primary_column {
            continue;
        }
        let (keys_binding, _) = winner_keys(ctx, candidate)?;
        extras.push(ExtraInjection {
            column,
            keys_from: keys_binding,
        });
    }
    Ok(extras)
}

/// The winning candidate's distinct-keys binding, emitted once. Ports `_winner_keys`.
fn winner_keys<'p>(
    ctx: &mut Ctx<'p>,
    winner: &Candidate<'p>,
) -> Result<(String, String), StepError> {
    if let Some(cached) = ctx.winner_keys.get(&node_id(winner.build)) {
        return Ok(cached.clone());
    }
    let build_binding = emit_build_side(ctx, winner.build)?;
    let keys = emit_keys_for(ctx, winner.build, &build_binding, winner.build_key);
    ctx.winner_keys.insert(node_id(winner.build), keys.clone());
    Ok(keys)
}

/// A probe base with the build's keys pushed in as `col IN (...)`; the probe
/// column's NDV rides along for the delivery-strategy guard. Ports
/// `_emit_injected_scan`.
fn emit_injected_scan(
    ctx: &mut Ctx<'_>,
    base: &PhysicalPlan,
    inject_col: &str,
    keys_binding: &str,
    _build_key: &str,
    extras: Vec<ExtraInjection>,
) -> Result<String, StepError> {
    let binding = ctx.names.binding();
    let inject_column_ndv = node_column_ndv(base, inject_col);
    let step = Step::InjectedScan {
        datasource: source_datasource(base)?,
        scan: injected_probe_spec(base)?,
        inject_column: inject_col.to_string(),
        keys_from: keys_binding.to_string(),
        binding,
        inject_column_ndv,
        extra_injections: extras,
    };
    Ok(emit_step_once(ctx, step, None))
}

// ---------------------------------------------------------------------------
// small shared helpers
// ---------------------------------------------------------------------------

/// The physical output name of a join-key column (a ColumnRef after binding).
fn key_physical(key: &Expr, aliases: &ColumnAliasMap) -> String {
    match key {
        Expr::Column(col) => physical_column_name(col, aliases),
        _ => panic!("a join key must be a column reference after binding"),
    }
}

/// The physical output name of the build side's join-key column. Ports
/// `_build_key_name`.
fn build_key_name(build: &PhysicalPlan, build_key: &Expr) -> String {
    key_physical(build_key, &build.column_aliases())
}

/// The (qualifier, column) pair of a column-reference key.
fn key_pair_of(key: &Expr) -> (Option<String>, String) {
    match key {
        Expr::Column(col) => (col.table.clone(), col.column.clone()),
        _ => panic!("a join key must be a column reference after binding"),
    }
}

/// A node's cost-model NDV for one output column, or None when unthreaded. Ports
/// `_node_column_ndv`.
fn node_column_ndv(node: &PhysicalPlan, column: &str) -> Option<i64> {
    let ndv_map = match node {
        PhysicalPlan::Scan(scan) => scan.column_ndv.as_ref(),
        PhysicalPlan::RemoteQuery(remote) => remote.column_ndv.as_ref(),
        _ => None,
    }?;
    ndv_map.get(column).copied()
}

/// Whether a scan folds an aggregate/grouping shape.
fn scan_folds_grouping(scan: &PhysicalScan) -> bool {
    scan.aggregates
        .as_ref()
        .is_some_and(|aggs| !aggs.is_empty())
        || scan.group_by.as_ref().is_some_and(|keys| !keys.is_empty())
        || scan
            .grouping_sets
            .as_ref()
            .is_some_and(|sets| !sets.is_empty())
}

/// A non-negative NDV as `u64` (a negative sentinel becomes unknown).
fn as_u64(value: Option<i64>) -> Option<u64> {
    value.and_then(|number| u64::try_from(number).ok())
}

/// A `u64` row count clamped into `i64` for the urgency arithmetic.
fn clamp_i64(rows: u64) -> i64 {
    i64::try_from(rows).unwrap_or(i64::MAX)
}
