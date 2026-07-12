//! Cost-based join ordering: a Selinger left-deep DP (with a GOO greedy fallback
//! above the DP size bound) plus a data-locality term, re-emitting each join
//! region in predicate-pushdown normal form. Ports `optimizer/join_ordering.py`.
//!
//! Given a `JoinRegion` (atoms + classified conjuncts) and a cardinality
//! estimator, `choose_order` picks a join order per CONNECTED COMPONENT of the
//! join graph:
//!
//! - Selinger-style dynamic programming over connected subsets for components up
//!   to the configured DP limit. Only CONNECTED extensions are ever generated, so
//!   an intra-component cross product is structurally impossible.
//! - A GOO-style greedy walk (cheapest connected pair, then always the cheapest
//!   connected extension) above the limit.
//!
//! Components are combined smallest-output-first with CROSS joins at the top -
//! the only place a cross product legitimately exists. Cost is C_out (the sum of
//! every join's output rows); the locality term rides only in the tie-break key.
//! Everything is deterministic: iteration is over sorted atom indexes and ties
//! break on the lexicographically smallest atom sequence.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use fq_plan::expr::{and_expressions, combine_and, BinaryOpType, ColumnRef, Expr};
use fq_plan::logical::{Filter, Join, JoinType, LogicalPlan};

use crate::cost::CostModel;
use crate::error::OptimizeError;
use crate::estimate_defaults::{
    apply_conjunct_term, cap_composite_denom, combine_defaults, max_known_ndv, CardinalityEstimate,
    TRANSFER_WEIGHT,
};
use crate::join_graph::{extract_region, is_region_join, JoinAtom, JoinConjunct, JoinRegion};

use super::OptimizationRule;

/// The largest region the DP bitmask (a `u64` over global atom indexes) can
/// address. A larger region declines reordering (keeps its written order) rather
/// than shift past the bitmask domain. No real query joins this many base tables
/// in one connected region.
const MAX_REGION_ATOMS: usize = 64;

// ------------------------------- result types -------------------------------

/// One left-deep join step: the atom joined, its estimate, and which region
/// conjuncts (by position) are placed on this join.
#[derive(Debug, Clone, PartialEq)]
struct JoinStep {
    atom_index: usize,
    estimate: CardinalityEstimate,
    conjunct_positions: Vec<usize>,
}

/// One connected component's chosen left-deep order.
#[derive(Debug, Clone, PartialEq)]
struct ComponentOrder {
    first_atom: usize,
    steps: Vec<JoinStep>,
    total: CardinalityEstimate,
}

/// One CROSS between successive component subtrees: its estimate and the
/// (non-equi) region conjuncts that span the two sides and ride on it.
#[derive(Debug, Clone, PartialEq)]
struct CrossStep {
    estimate: CardinalityEstimate,
    conjunct_positions: Vec<usize>,
}

/// The whole region's order: component subtrees combined by CROSS joins, with one
/// step per CROSS between successive components.
#[derive(Debug, Clone, PartialEq)]
struct RegionOrder {
    components: Vec<ComponentOrder>,
    cross_steps: Vec<CrossStep>,
}

/// One left-deep prefix during enumeration (internal only).
///
/// `island_source`/`transfer` implement the LOCALITY model: only the leading
/// same-source run of a left-deep component collapses into one remote query, so a
/// candidate tracks whether its prefix is still pure and how many rows have
/// crossed the wire so far. Island state is SUBSET-DETERMINED (the prefix of a
/// full candidate is the whole subset, so it is pure iff every atom in the subset
/// shares one source), which is why the DP stays keyed on the plain subset.
#[derive(Debug, Clone)]
struct Candidate {
    cost: f64,
    sequence: Vec<usize>,
    steps: Vec<JoinStep>,
    estimate: CardinalityEstimate,
    transfer: f64,
    island_source: Option<String>,
}

// ------------------------------- the estimator ------------------------------

/// The cardinality oracle the enumerator consults. Implementations answer from
/// source statistics (production) or from hand-fed tables (tests); the enumerator
/// itself never sees a statistic. `conjunct_positions` index `region.conjuncts`.
trait RegionEstimator {
    /// The estimated rows of one atom under its single-atom (local) conjuncts.
    fn atom_estimate(
        &self,
        region: &JoinRegion,
        atom_index: usize,
        local_positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError>;

    /// The estimated rows of joining the accumulated left side with one atom under
    /// the conjuncts newly placed at this step.
    fn join_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        atom_index: usize,
        atom: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError>;

    /// The estimated rows of CROSSing two component subtrees, reduced by any
    /// non-equi conjuncts that span them.
    fn cross_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        right: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError>;
}

/// A `RegionEstimator` answering from the `CostModel` and source statistics. The
/// cost model is behind a `RefCell` because `CostModel::estimate` is `&mut self`
/// while `OptimizationRule::apply` is `&self`.
struct CostModelRegionEstimator<'a> {
    cost_model: &'a RefCell<CostModel>,
}

impl RegionEstimator for CostModelRegionEstimator<'_> {
    /// The atom with its local filters folded in, estimated exactly as the plan
    /// will look after pushdown, so the optimizer's joint fixpoint stays stable.
    fn atom_estimate(
        &self,
        region: &JoinRegion,
        atom_index: usize,
        local_positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        let filters = local_exprs(region, local_positions);
        let plan = with_local_filters(region.atoms[atom_index].plan.clone(), &filters);
        Ok(self.cost_model.borrow_mut().estimate(&plan)?)
    }

    /// left x atom / capped key NDV x non-equi selectivity. `choose_order` gates
    /// enumeration on every atom being KNOWN, so an unknown side here is an
    /// enumerator bug, never a statistics gap.
    fn join_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        atom_index: usize,
        atom: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        let (Some(left_rows), Some(atom_rows)) = (left.rows, atom.rows) else {
            return Err(OptimizeError::JoinOrder(
                "join_estimate over an UNKNOWN side; choose_order must decline \
                 regions with unknown atoms before enumerating"
                    .to_string(),
            ));
        };
        let terms = self.step_terms(region, left_rows, atom_rows, atom_index, positions)?;
        let denom = cap_composite_denom(terms.denom, terms.equi_count, left_rows, atom_rows);
        let rows = left_rows as f64 * atom_rows as f64 * terms.selectivity / denom;
        Ok(CardinalityEstimate::known(
            (rows as u64).max(1),
            combine_defaults(&[left, atom], &terms.defaults),
        ))
    }

    /// left x right, reduced by any spanning non-equi conjuncts. An equi conjunct
    /// here is an enumerator bug: equi edges define components, so one can never
    /// span two. An UNKNOWN factor reduces nothing (ceiling 1.0), a valid bound.
    fn cross_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        right: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        let (Some(left_rows), Some(right_rows)) = (left.rows, right.rows) else {
            return Err(OptimizeError::JoinOrder(
                "cross_estimate over an UNKNOWN side; choose_order must decline \
                 regions with unknown atoms before enumerating"
                    .to_string(),
            ));
        };
        let mut rows = left_rows as f64 * right_rows as f64;
        let mut defaults = Vec::new();
        for &position in positions {
            let conjunct = &region.conjuncts[position];
            if conjunct.is_equi {
                return Err(OptimizeError::JoinOrder(
                    "an equi conjunct spans two components; the component partition \
                     is broken"
                        .to_string(),
                ));
            }
            let (factor, defs) = self
                .cost_model
                .borrow()
                .conjunct_selectivity(&conjunct.expression, "cross")?;
            if let Some(factor) = factor {
                rows *= factor;
            }
            defaults.extend(defs);
        }
        Ok(CardinalityEstimate::known(
            (rows as u64).max(1),
            combine_defaults(&[left, right], &defaults),
        ))
    }
}

/// The folded per-step terms: the equi-key NDV denominator, the non-equi
/// selectivity, the equi-key count (which decides the composite cap) and the
/// provenance of every gap.
struct StepTerms {
    denom: f64,
    selectivity: f64,
    equi_count: u32,
    defaults: Vec<String>,
}

impl CostModelRegionEstimator<'_> {
    /// Fold every conjunct placed at one step into the join estimate's terms.
    fn step_terms(
        &self,
        region: &JoinRegion,
        left_rows: u64,
        atom_rows: u64,
        atom_index: usize,
        positions: &[usize],
    ) -> Result<StepTerms, OptimizeError> {
        let mut terms = StepTerms {
            denom: 1.0,
            selectivity: 1.0,
            equi_count: 0,
            defaults: Vec::new(),
        };
        for &position in positions {
            let conjunct = &region.conjuncts[position];
            let (is_equi, value, defs) =
                self.term(region, conjunct, left_rows, atom_rows, atom_index)?;
            apply_conjunct_term(
                is_equi,
                value,
                &mut terms.denom,
                &mut terms.selectivity,
                &mut terms.equi_count,
            );
            terms.defaults.extend(defs);
        }
        Ok(terms)
    }

    /// (is_equi, value, defaults) for one conjunct: an equi key's value is its
    /// NDV; anything else's is the tracked selectivity.
    fn term(
        &self,
        region: &JoinRegion,
        conjunct: &JoinConjunct,
        left_rows: u64,
        atom_rows: u64,
        atom_index: usize,
    ) -> Result<(bool, Option<f64>, Vec<String>), OptimizeError> {
        if !conjunct.is_equi || conjunct.atom_indexes.len() != 2 {
            let (factor, defaults) = self
                .cost_model
                .borrow()
                .conjunct_selectivity(&conjunct.expression, "join")?;
            return Ok((false, factor, defaults));
        }
        let (atom_ref, other_ref) = orient_refs(region, conjunct, atom_index)?;
        let (atom_ndv, mut defaults) = self.ref_ndv(region, atom_ref, atom_rows)?;
        let (other_ndv, other_defaults) = self.ref_ndv(region, other_ref, left_rows)?;
        defaults.extend(other_defaults);
        let value = max_known_ndv(atom_ndv, other_ndv).map(|ndv| ndv as f64);
        Ok((true, value, defaults))
    }

    /// A key's NDV clamped by its side's current rows, or UNKNOWN (None) with the
    /// gap recorded - never a fabricated fraction of the rows. The NDV is read
    /// from the OWNER atom's ORIGINAL base plan, not the accumulated subtree.
    fn ref_ndv(
        &self,
        region: &JoinRegion,
        reference: &ColumnRef,
        side_rows: u64,
    ) -> Result<(Option<u64>, Vec<String>), OptimizeError> {
        let owner = atom_owning(region, reference)?;
        let ndv = self
            .cost_model
            .borrow()
            .column_ndv(&owner.plan, reference)?;
        match ndv {
            None => Ok((
                None,
                vec![format!(
                    "ndv({}.{})",
                    reference.table.as_deref().unwrap_or_default(),
                    reference.column
                )],
            )),
            Some(ndv) => {
                let clamped = u64::try_from(ndv).unwrap_or(0).min(side_rows).max(1);
                Ok((Some(clamped), Vec::new()))
            }
        }
    }
}

// --------------------------- graph decomposition ----------------------------

/// Atom adjacency over EQUI conjuncts only. Only a column-to-column equality is a
/// hash-joinable edge; a non-equi conjunct executes as a nested-loop/cross, far
/// costlier than its estimated output. Non-equi conjuncts still place, they just
/// never make two atoms adjacent.
fn adjacency(region: &JoinRegion) -> BTreeMap<usize, BTreeSet<usize>> {
    let mut adjacency = BTreeMap::new();
    for atom in &region.atoms {
        adjacency.insert(atom.index, BTreeSet::new());
    }
    for conjunct in &region.conjuncts {
        if !conjunct.is_equi {
            continue;
        }
        for &first in &conjunct.atom_indexes {
            for &second in &conjunct.atom_indexes {
                if first != second {
                    adjacency.entry(first).or_default().insert(second);
                }
            }
        }
    }
    adjacency
}

/// The join graph's connected components, each a sorted atom-index list.
fn connected_components(region: &JoinRegion) -> Vec<Vec<usize>> {
    let adjacency = adjacency(region);
    let mut seen = BTreeSet::new();
    let mut components = Vec::new();
    for atom in &region.atoms {
        if !seen.contains(&atom.index) {
            let mut component = flood(&adjacency, atom.index, &mut seen);
            component.sort_unstable();
            components.push(component);
        }
    }
    components
}

/// Collect one component by flood fill from a starting atom.
fn flood(
    adjacency: &BTreeMap<usize, BTreeSet<usize>>,
    start: usize,
    seen: &mut BTreeSet<usize>,
) -> Vec<usize> {
    let mut stack = vec![start];
    let mut component = Vec::new();
    while let Some(index) = stack.pop() {
        if !seen.insert(index) {
            continue;
        }
        component.push(index);
        for &neighbor in &adjacency[&index] {
            stack.push(neighbor);
        }
    }
    component
}

/// The bitmask of an atom-index set (all indexes are `< MAX_REGION_ATOMS`).
fn atoms_mask(atoms: &BTreeSet<usize>) -> u64 {
    let mut mask = 0u64;
    for &atom in atoms {
        mask |= 1u64 << atom;
    }
    mask
}

/// Positions of the multi-atom conjuncts covered by `subset | {atom_index}` but
/// not by `subset` alone - the ones a step growing the subset must place (each
/// conjunct placed exactly once, on the first step covering all its atoms).
fn newly_covered(region: &JoinRegion, subset: u64, atom_index: usize) -> Vec<usize> {
    let grown = subset | (1u64 << atom_index);
    let mut positions = Vec::new();
    for (position, conjunct) in region.conjuncts.iter().enumerate() {
        if conjunct.atom_indexes.len() < 2 {
            continue;
        }
        let mask = atoms_mask(&conjunct.atom_indexes);
        if mask & grown == mask && mask & subset != mask {
            positions.push(position);
        }
    }
    positions
}

/// Whether any of the conjuncts at these positions is an equi edge.
fn any_equi(region: &JoinRegion, positions: &[usize]) -> bool {
    positions
        .iter()
        .any(|&position| region.conjuncts[position].is_equi)
}

/// Positions of the conjuncts referencing exactly this one atom (its local
/// filters).
fn local_positions(region: &JoinRegion, atom_index: usize) -> Vec<usize> {
    let mut positions = Vec::new();
    for (position, conjunct) in region.conjuncts.iter().enumerate() {
        if conjunct.atom_indexes.len() == 1 && conjunct.atom_indexes.contains(&atom_index) {
            positions.push(position);
        }
    }
    positions
}

/// The expressions of the conjuncts at the given region positions.
fn local_exprs(region: &JoinRegion, positions: &[usize]) -> Vec<Expr> {
    let mut expressions = Vec::with_capacity(positions.len());
    for &position in positions {
        expressions.push(region.conjuncts[position].expression.clone());
    }
    expressions
}

/// Positions of the conjuncts that SPAN the covered set and the incoming
/// component: fully covered by their union but by neither side alone (a conjunct
/// inside one side was already placed by that side's steps).
fn spanning_positions(
    region: &JoinRegion,
    covered: &BTreeSet<usize>,
    incoming: &BTreeSet<usize>,
) -> Vec<usize> {
    let mut positions = Vec::new();
    for (position, conjunct) in region.conjuncts.iter().enumerate() {
        if conjunct.atom_indexes.len() < 2 {
            continue;
        }
        let spans_union = conjunct
            .atom_indexes
            .iter()
            .all(|a| covered.contains(a) || incoming.contains(a));
        let in_covered = conjunct.atom_indexes.iter().all(|a| covered.contains(a));
        let in_incoming = conjunct.atom_indexes.iter().all(|a| incoming.contains(a));
        if spans_union && !in_covered && !in_incoming {
            positions.push(position);
        }
    }
    positions
}

/// The atom indexes a component order covers.
fn component_atoms(order: &ComponentOrder) -> BTreeSet<usize> {
    let mut atoms = BTreeSet::new();
    atoms.insert(order.first_atom);
    for step in &order.steps {
        atoms.insert(step.atom_index);
    }
    atoms
}

/// Components sort by ascending output rows, then by atom sequence.
fn component_sort_key(order: &ComponentOrder) -> (u64, Vec<usize>) {
    let mut sequence = vec![order.first_atom];
    for step in &order.steps {
        sequence.push(step.atom_index);
    }
    (order.total.rows.unwrap_or(u64::MAX), sequence)
}

// ------------------------------- the locality model -------------------------

/// The single datasource every scan in an atom's subtree reads from, or None when
/// the atom is coordinator-resident: mixed sources, CTE references, VALUES -
/// exactly the shapes single-source pushdown declines to collapse, so no transfer
/// is saved by keeping them in an island.
fn atom_source(plan: &LogicalPlan) -> Option<String> {
    let mut sources = BTreeSet::new();
    if !collect_atom_sources(plan, &mut sources) {
        return None;
    }
    if sources.len() == 1 {
        sources.into_iter().next()
    } else {
        None
    }
}

/// Gather the subtree's scan datasources; false on a non-collapsible leaf.
fn collect_atom_sources(plan: &LogicalPlan, sources: &mut BTreeSet<String>) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => {
            sources.insert(scan.datasource.clone());
            true
        }
        LogicalPlan::CteRef(_) | LogicalPlan::Values(_) => false,
        other => {
            let mut collapsible = true;
            for child in other.children() {
                if !collect_atom_sources(child, sources) {
                    collapsible = false;
                }
            }
            collapsible
        }
    }
}

/// The (transfer, island) state after joining one more atom.
///
/// Same source as the still-pure island: the join runs inside the source,
/// nothing crosses. On the first source switch the island's CURRENT result
/// crosses the wire, plus the new atom's own rows (unless it is
/// coordinator-resident and fetches nothing). Once mixed, each further sourced
/// atom ships its own rows. `atom_rows` is the new atom's FULL row estimate.
fn extended_transfer(
    candidate: &Candidate,
    atom_source: Option<&str>,
    atom_rows: u64,
) -> (f64, Option<String>) {
    if let Some(island) = &candidate.island_source {
        if atom_source == Some(island.as_str()) {
            return (candidate.transfer, candidate.island_source.clone());
        }
    }
    let mut crossed = candidate.transfer;
    if candidate.island_source.is_some() {
        let island_rows = candidate
            .estimate
            .rows
            .expect("a pure island's estimate is known (gated by choose_order)");
        crossed += island_rows as f64;
    }
    if atom_source.is_some() {
        crossed += atom_rows as f64;
    }
    (crossed, None)
}

// ------------------------------- the enumerator -----------------------------

/// The immutable enumeration context (region + estimator + per-atom base data),
/// carried once instead of threaded through every DP/greedy helper.
struct Enumerator<'a> {
    region: &'a JoinRegion,
    estimator: &'a dyn RegionEstimator,
    atom_estimates: &'a [CardinalityEstimate],
    atom_sources: &'a [Option<String>],
    max_dp: usize,
}

impl Enumerator<'_> {
    /// A seed prefix over one atom (cost 0, its base estimate, pure island).
    fn seed(&self, index: usize) -> Candidate {
        Candidate {
            cost: 0.0,
            sequence: vec![index],
            steps: Vec::new(),
            estimate: self.atom_estimates[index].clone(),
            transfer: 0.0,
            island_source: self.atom_sources[index].clone(),
        }
    }

    /// One component's order: trivial, DP, or greedy by its size.
    fn order_component(&self, component: &[usize]) -> Result<ComponentOrder, OptimizeError> {
        if component.len() == 1 {
            return Ok(ComponentOrder {
                first_atom: component[0],
                steps: Vec::new(),
                total: self.atom_estimates[component[0]].clone(),
            });
        }
        let candidate = if component.len() <= self.max_dp {
            self.dp_best(component)?
        } else {
            self.greedy(component)?
        };
        Ok(to_component_order(candidate))
    }

    /// Selinger-style DP over connected subsets, left-deep, deterministic.
    fn dp_best(&self, component: &[usize]) -> Result<Candidate, OptimizeError> {
        let mut best: BTreeMap<u64, Candidate> = BTreeMap::new();
        for &index in component {
            best.insert(1u64 << index, self.seed(index));
        }
        for size in 2..=component.len() {
            self.dp_layer(component, &mut best, size)?;
        }
        let full_mask = component
            .iter()
            .fold(0u64, |mask, &index| mask | (1u64 << index));
        best.remove(&full_mask).ok_or_else(|| {
            OptimizeError::JoinOrder("DP never covered a connected component fully".to_string())
        })
    }

    /// Extend every (size-1)-subset by each connected atom, keeping the best
    /// candidate per resulting subset.
    fn dp_layer(
        &self,
        component: &[usize],
        best: &mut BTreeMap<u64, Candidate>,
        size: usize,
    ) -> Result<(), OptimizeError> {
        let mut additions: BTreeMap<u64, Candidate> = BTreeMap::new();
        for subset in subsets_of_size(best, size - 1) {
            let candidate = best[&subset].clone();
            self.extend_subset(component, &candidate, subset, &mut additions)?;
        }
        best.extend(additions);
        Ok(())
    }

    /// Try every connected single-atom extension of one candidate.
    fn extend_subset(
        &self,
        component: &[usize],
        candidate: &Candidate,
        subset: u64,
        additions: &mut BTreeMap<u64, Candidate>,
    ) -> Result<(), OptimizeError> {
        for &atom_index in component {
            if subset & (1u64 << atom_index) != 0 {
                continue;
            }
            if let Some(extended) = self.try_extend(candidate, subset, atom_index)? {
                keep_better(additions, subset | (1u64 << atom_index), extended);
            }
        }
        Ok(())
    }

    /// The candidate extended by one atom, or None when no EQUI conjunct connects
    /// the atom to the subset (a cross/nested-loop, never generated in a
    /// component). The transfer charge is the atom's FULL rows, deliberately
    /// ignoring the runtime key reduction (measured to mis-price island breaking).
    fn try_extend(
        &self,
        candidate: &Candidate,
        subset: u64,
        atom_index: usize,
    ) -> Result<Option<Candidate>, OptimizeError> {
        let positions = newly_covered(self.region, subset, atom_index);
        if !any_equi(self.region, &positions) {
            return Ok(None);
        }
        let atom = &self.atom_estimates[atom_index];
        let estimate = self.estimator.join_estimate(
            self.region,
            &candidate.estimate,
            atom_index,
            atom,
            &positions,
        )?;
        let atom_rows = atom.rows.ok_or_else(|| {
            OptimizeError::JoinOrder("try_extend over an UNKNOWN atom (post-gate)".to_string())
        })?;
        let step_rows = estimate.rows.ok_or_else(|| {
            OptimizeError::JoinOrder("join_estimate returned UNKNOWN (post-gate)".to_string())
        })?;
        let (transfer, island) = extended_transfer(
            candidate,
            self.atom_sources[atom_index].as_deref(),
            atom_rows,
        );
        let mut sequence = candidate.sequence.clone();
        sequence.push(atom_index);
        let mut steps = candidate.steps.clone();
        steps.push(JoinStep {
            atom_index,
            estimate: estimate.clone(),
            conjunct_positions: positions,
        });
        Ok(Some(Candidate {
            cost: candidate.cost + step_rows as f64,
            sequence,
            steps,
            estimate,
            transfer,
            island_source: island,
        }))
    }

    /// GOO-style left-deep greedy for components beyond the DP limit.
    fn greedy(&self, component: &[usize]) -> Result<Candidate, OptimizeError> {
        let mut candidate = self.best_pair(component)?;
        while candidate.sequence.len() < component.len() {
            candidate = self.best_extension(component, &candidate)?;
        }
        Ok(candidate)
    }

    /// The cheapest connected starting pair of the component.
    fn best_pair(&self, component: &[usize]) -> Result<Candidate, OptimizeError> {
        let mut best = None;
        for &first in component {
            let contender = self.pair_from(component, first)?;
            best = better(best, contender);
        }
        best.ok_or_else(|| {
            OptimizeError::JoinOrder(
                "no connected starting pair in a multi-atom component".to_string(),
            )
        })
    }

    /// The cheapest connected pair starting from one given atom.
    fn pair_from(
        &self,
        component: &[usize],
        first: usize,
    ) -> Result<Option<Candidate>, OptimizeError> {
        let start = self.seed(first);
        let subset = 1u64 << first;
        let mut best = None;
        for &second in component {
            if second != first {
                let contender = self.try_extend(&start, subset, second)?;
                best = better(best, contender);
            }
        }
        Ok(best)
    }

    /// The cheapest connected one-atom extension of the greedy prefix.
    fn best_extension(
        &self,
        component: &[usize],
        candidate: &Candidate,
    ) -> Result<Candidate, OptimizeError> {
        let subset = candidate
            .sequence
            .iter()
            .fold(0u64, |mask, &index| mask | (1u64 << index));
        let mut best = None;
        for &atom_index in component {
            if subset & (1u64 << atom_index) == 0 {
                let contender = self.try_extend(candidate, subset, atom_index)?;
                best = better(best, contender);
            }
        }
        best.ok_or_else(|| {
            OptimizeError::JoinOrder(
                "greedy walk stranded: no connected extension left".to_string(),
            )
        })
    }

    /// Combine component orders with CROSS joins, smallest output first. A
    /// conjunct spanning two components (necessarily non-equi) rides on the CROSS
    /// that first covers all its atoms, so predicate conservation holds.
    fn combine(&self, ordered: Vec<ComponentOrder>) -> Result<RegionOrder, OptimizeError> {
        let mut cross_steps = Vec::new();
        let mut running = ordered[0].total.clone();
        let mut covered = component_atoms(&ordered[0]);
        for component in &ordered[1..] {
            let incoming = component_atoms(component);
            let positions = spanning_positions(self.region, &covered, &incoming);
            let estimate = self.estimator.cross_estimate(
                self.region,
                &running,
                &component.total,
                &positions,
            )?;
            cross_steps.push(CrossStep {
                estimate: estimate.clone(),
                conjunct_positions: positions,
            });
            running = estimate;
            covered.extend(incoming);
        }
        Ok(RegionOrder {
            components: ordered,
            cross_steps,
        })
    }
}

/// The DP subsets of one size, in deterministic sorted order (by ascending
/// member-index sequence, matching Python's `tuple(sorted(subset))`).
fn subsets_of_size(best: &BTreeMap<u64, Candidate>, size: usize) -> Vec<u64> {
    let mut found: Vec<u64> = best
        .keys()
        .copied()
        .filter(|mask| mask.count_ones() as usize == size)
        .collect();
    found.sort_by_key(|mask| mask_members(*mask));
    found
}

/// The set bits of a mask as an ascending index list.
fn mask_members(mask: u64) -> Vec<usize> {
    let mut members = Vec::new();
    for index in 0..MAX_REGION_ATOMS {
        if mask & (1u64 << index) != 0 {
            members.push(index);
        }
    }
    members
}

/// Keep the cheaper candidate per subset (ties: smaller atom sequence).
fn keep_better(additions: &mut BTreeMap<u64, Candidate>, key: u64, candidate: Candidate) {
    let replace = match additions.get(&key) {
        None => true,
        Some(current) => candidate_less(&candidate, current),
    };
    if replace {
        additions.insert(key, candidate);
    }
}

/// The smaller candidate by (cost + weighted transfer, sequence); tolerates None.
fn better(best: Option<Candidate>, contender: Option<Candidate>) -> Option<Candidate> {
    match (best, contender) {
        (best, None) => best,
        (None, Some(contender)) => Some(contender),
        (Some(best), Some(contender)) => {
            if candidate_less(&contender, &best) {
                Some(contender)
            } else {
                Some(best)
            }
        }
    }
}

/// Whether `left` orders strictly before `right`: first the C_out plus the
/// weighted transfer cost (a finite positive scalar), then the atom sequence as
/// the deterministic tie-break. `cost` stays pure C_out; the locality term is
/// combined only here.
fn candidate_less(left: &Candidate, right: &Candidate) -> bool {
    let left_key = left.cost + TRANSFER_WEIGHT * left.transfer;
    let right_key = right.cost + TRANSFER_WEIGHT * right.transfer;
    match left_key.total_cmp(&right_key) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => left.sequence < right.sequence,
    }
}

/// Freeze an enumeration candidate into the component's final order.
fn to_component_order(candidate: Candidate) -> ComponentOrder {
    ComponentOrder {
        first_atom: candidate.sequence[0],
        steps: candidate.steps,
        total: candidate.estimate,
    }
}

/// The chosen join order for a region: DP per connected component up to `max_dp`
/// atoms, greedy above, components CROSSed smallest-first.
///
/// `Ok(None)` when any atom's estimate is UNKNOWN: a DP over a mix of measured
/// and unknown sizes is confidently wrong, and a DP over all-unknowns is noise -
/// the caller keeps the written order instead.
fn choose_order(
    region: &JoinRegion,
    estimator: &dyn RegionEstimator,
    max_dp: usize,
) -> Result<Option<RegionOrder>, OptimizeError> {
    if region.atoms.is_empty() {
        return Err(OptimizeError::JoinOrder(
            "choose_order over a region with no atoms".to_string(),
        ));
    }
    if region.atoms.len() > MAX_REGION_ATOMS {
        // A valid but enormous region the bitmask DP cannot address: DECLINE (leave
        // its written order) rather than fail the query. This returns before any
        // `1 << index` shift, so the bitmask domain stays well-defined. No real
        // query joins this many base tables in one connected region.
        return Ok(None);
    }
    let atom_estimates = base_atom_estimates(region, estimator)?;
    if atom_estimates
        .iter()
        .any(|estimate| estimate.rows.is_none())
    {
        return Ok(None);
    }
    let atom_sources = atom_source_list(region);
    let enumerator = Enumerator {
        region,
        estimator,
        atom_estimates: &atom_estimates,
        atom_sources: &atom_sources,
        max_dp,
    };
    let mut ordered = Vec::new();
    for component in connected_components(region) {
        ordered.push(enumerator.order_component(&component)?);
    }
    ordered.sort_by_key(component_sort_key);
    Ok(Some(enumerator.combine(ordered)?))
}

/// Every atom's base estimate (under its single-atom conjuncts), by index.
fn base_atom_estimates(
    region: &JoinRegion,
    estimator: &dyn RegionEstimator,
) -> Result<Vec<CardinalityEstimate>, OptimizeError> {
    let mut estimates = Vec::with_capacity(region.atoms.len());
    for atom in &region.atoms {
        let positions = local_positions(region, atom.index);
        estimates.push(estimator.atom_estimate(region, atom.index, &positions)?);
    }
    Ok(estimates)
}

/// Every atom's source identity, by index (parallel to atom estimates).
fn atom_source_list(region: &JoinRegion) -> Vec<Option<String>> {
    let mut sources = Vec::with_capacity(region.atoms.len());
    for atom in &region.atoms {
        sources.push(atom_source(&atom.plan));
    }
    sources
}

// ----------------------------- reference orientation ------------------------

/// (atom-side ref, other-side ref) of a two-atom equi conjunct being placed on
/// the step that joins `atom_index`.
fn orient_refs<'a>(
    region: &'a JoinRegion,
    conjunct: &'a JoinConjunct,
    atom_index: usize,
) -> Result<(&'a ColumnRef, &'a ColumnRef), OptimizeError> {
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = &conjunct.expression
    else {
        return Err(OptimizeError::JoinOrder(
            "an equi conjunct is not an equality expression".to_string(),
        ));
    };
    let (Expr::Column(left_ref), Expr::Column(right_ref)) = (left.as_ref(), right.as_ref()) else {
        return Err(OptimizeError::JoinOrder(
            "an equi conjunct's operands are not both columns".to_string(),
        ));
    };
    let qualifiers = &region.atoms[atom_index].qualifiers;
    if owns(qualifiers, left_ref) {
        return Ok((left_ref, right_ref));
    }
    if owns(qualifiers, right_ref) {
        return Ok((right_ref, left_ref));
    }
    Err(OptimizeError::JoinOrder(
        "an equi conjunct was placed on a join step that touches neither of its \
         columns - the enumerator's placement is broken"
            .to_string(),
    ))
}

/// Whether a qualifier set owns a column reference's relation.
fn owns(qualifiers: &BTreeSet<String>, reference: &ColumnRef) -> bool {
    reference
        .table
        .as_ref()
        .is_some_and(|table| qualifiers.contains(table))
}

/// The region atom whose qualifiers own a column reference.
fn atom_owning<'a>(
    region: &'a JoinRegion,
    reference: &ColumnRef,
) -> Result<&'a JoinAtom, OptimizeError> {
    for atom in &region.atoms {
        if owns(&atom.qualifiers, reference) {
            return Ok(atom);
        }
    }
    Err(OptimizeError::JoinOrder(format!(
        "reference {}.{} owns no atom in its region",
        reference.table.as_deref().unwrap_or_default(),
        reference.column
    )))
}

/// The atom-side column refs of every two-atom equi conjunct touching one atom -
/// the join keys a downstream reduction may inject on.
fn atom_equi_refs(
    region: &JoinRegion,
    atom_index: usize,
) -> Result<Vec<&ColumnRef>, OptimizeError> {
    let mut refs = Vec::new();
    for conjunct in &region.conjuncts {
        if !conjunct.is_equi || conjunct.atom_indexes.len() != 2 {
            continue;
        }
        if conjunct.atom_indexes.contains(&atom_index) {
            let (atom_ref, _other) = orient_refs(region, conjunct, atom_index)?;
            refs.push(atom_ref);
        }
    }
    Ok(refs)
}

// -------------------------------- re-emission -------------------------------

/// Fold single-atom conjuncts back onto their atom: straight into a scan's
/// pushed-down filters (PredicatePushdown's end state), a Filter above anything
/// else.
fn with_local_filters(plan: LogicalPlan, expressions: &[Expr]) -> LogicalPlan {
    let Some(predicate) = combine_and(expressions.to_vec()) else {
        return plan;
    };
    match plan {
        LogicalPlan::Scan(mut scan) => {
            scan.filters = and_expressions(scan.filters.take(), Some(predicate));
            LogicalPlan::Scan(scan)
        }
        // A fresh Filter synthesized to carry the single-atom conjuncts above a
        // non-scan atom; no base Filter, {input, predicate} is the complete set.
        other => LogicalPlan::Filter(Filter {
            input: Box::new(other),
            predicate,
        }),
    }
}

/// A step's placed conjuncts split into (equi keys, residuals).
fn split_step_conjuncts(region: &JoinRegion, positions: &[usize]) -> (Vec<Expr>, Vec<Expr>) {
    let mut equi = Vec::new();
    let mut residual = Vec::new();
    for &position in positions {
        let conjunct = &region.conjuncts[position];
        if conjunct.is_equi {
            equi.push(conjunct.expression.clone());
        } else {
            residual.push(conjunct.expression.clone());
        }
    }
    (equi, residual)
}

/// Every region conjunct must be placed exactly once; a dropped or duplicated
/// predicate manufactures wrong results - crash instead.
fn verify_placement(total: usize, placed: &[usize]) -> Result<(), OptimizeError> {
    let mut sorted = placed.to_vec();
    sorted.sort_unstable();
    let expected: Vec<usize> = (0..total).collect();
    if sorted != expected {
        return Err(OptimizeError::JoinOrder(format!(
            "join reordering placed conjuncts {sorted:?} of {total}; every conjunct \
             must be placed exactly once"
        )));
    }
    Ok(())
}

// ---------------------------------- the rule --------------------------------

/// Cost-based join ordering over inner-join regions.
///
/// Runs after predicate pushdown (it reads folded equi conditions and embedded
/// scan filters), chooses a cardinality-driven order per region and re-emits the
/// region in pushdown's normal form. Every region conjunct must be placed exactly
/// once - a mismatch raises instead of silently dropping a predicate.
pub struct JoinOrdering {
    cost_model: Rc<RefCell<CostModel>>,
    max_join_reorder_size: usize,
}

impl JoinOrdering {
    /// The rule over a SHARED cost model (the `Rc<RefCell<..>>` handle
    /// `EagerAggregation` also holds - it is always on and needs the estimator
    /// even when join reordering is off) and the DP size limit.
    pub fn new(cost_model: Rc<RefCell<CostModel>>, max_join_reorder_size: usize) -> Self {
        Self {
            cost_model,
            max_join_reorder_size,
        }
    }

    /// Reorder the region rooted here, or recurse into the children. CTE bodies
    /// register on the way down, so a CteRef atom in any region below estimates
    /// its body instead of being unknown.
    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        if let LogicalPlan::Cte(cte) = &plan {
            self.cost_model.borrow_mut().register_cte(cte);
        }
        match extract_region(&plan)? {
            None => plan.try_map_children(|child| self.rewrite(child)),
            Some(region) => self.reorder_region(plan, &region),
        }
    }

    /// Order one region and re-emit it. A region `choose_order` declines (an
    /// UNKNOWN atom) or that is not worth reordering keeps its written order,
    /// while nested regions still get their chance via the recurse.
    fn reorder_region(
        &self,
        root: LogicalPlan,
        region: &JoinRegion,
    ) -> Result<LogicalPlan, OptimizeError> {
        if !worth_reordering(region, &root) {
            return root.try_map_children(|child| self.rewrite(child));
        }
        let order = {
            let estimator = CostModelRegionEstimator {
                cost_model: &self.cost_model,
            };
            choose_order(region, &estimator, self.max_join_reorder_size)?
        };
        let Some(order) = order else {
            return root.try_map_children(|child| self.rewrite(child));
        };
        let mut placed = Vec::new();
        let rebuilt = self.emit(region, &order, &mut placed)?;
        verify_placement(region.conjuncts.len(), &placed)?;
        Ok(rebuilt)
    }

    /// The re-emitted region: component subtrees CROSSed smallest-first, zero-atom
    /// (constant) conjuncts as one filter above everything.
    fn emit(
        &self,
        region: &JoinRegion,
        order: &RegionOrder,
        placed: &mut Vec<usize>,
    ) -> Result<LogicalPlan, OptimizeError> {
        let mut current = self.emit_component(region, &order.components[0], placed)?;
        for (position, component) in order.components[1..].iter().enumerate() {
            let right = self.emit_component(region, component, placed)?;
            current = emit_cross(region, current, right, &order.cross_steps[position], placed);
        }
        Ok(wrap_constant_conjuncts(region, current, placed))
    }

    /// One component as its left-deep join subtree.
    fn emit_component(
        &self,
        region: &JoinRegion,
        component: &ComponentOrder,
        placed: &mut Vec<usize>,
    ) -> Result<LogicalPlan, OptimizeError> {
        let mut current = self.emit_atom(region, component.first_atom, placed)?;
        for step in &component.steps {
            current = self.emit_step(region, current, step, placed)?;
        }
        Ok(current)
    }

    /// One atom: nested regions inside it rewritten, its local conjuncts folded
    /// back onto it, and its cost estimate recorded on a Scan leaf.
    fn emit_atom(
        &self,
        region: &JoinRegion,
        atom_index: usize,
        placed: &mut Vec<usize>,
    ) -> Result<LogicalPlan, OptimizeError> {
        let plan = self.rewrite(region.atoms[atom_index].plan.clone())?;
        let positions = local_positions(region, atom_index);
        placed.extend(positions.iter().copied());
        let emitted = with_local_filters(plan, &local_exprs(region, &positions));
        self.record_atom_estimate(region, atom_index, &positions, emitted)
    }

    /// Annotate a bare-Scan atom with its cost estimate and its join-key NDVs so
    /// the downstream reduction can orient by size. A Filter-wrapped or composite
    /// atom is returned unchanged (a composite already carries its estimate on its
    /// Join; a Filter-wrapped atom keeps None and falls back).
    fn record_atom_estimate(
        &self,
        region: &JoinRegion,
        atom_index: usize,
        local_positions: &[usize],
        emitted: LogicalPlan,
    ) -> Result<LogicalPlan, OptimizeError> {
        match emitted {
            LogicalPlan::Scan(mut scan) => {
                let estimator = CostModelRegionEstimator {
                    cost_model: &self.cost_model,
                };
                let estimate = estimator.atom_estimate(region, atom_index, local_positions)?;
                scan.estimated_rows = estimate.rows;
                scan.column_ndv = self.atom_key_ndv(region, atom_index)?;
                Ok(LogicalPlan::Scan(scan))
            }
            other => Ok(other),
        }
    }

    /// Base NDV per equi-join-key column of one atom, from the same source
    /// statistics the estimates used; keys the source has no NDV for are omitted.
    /// None when the atom joins on no equi key with a known NDV.
    fn atom_key_ndv(
        &self,
        region: &JoinRegion,
        atom_index: usize,
    ) -> Result<Option<BTreeMap<String, i64>>, OptimizeError> {
        let mut ndv_map = BTreeMap::new();
        for reference in atom_equi_refs(region, atom_index)? {
            let ndv = self
                .cost_model
                .borrow()
                .column_ndv(&region.atoms[atom_index].plan, reference)?;
            if let Some(ndv) = ndv {
                ndv_map.insert(reference.column.clone(), ndv);
            }
        }
        Ok(if ndv_map.is_empty() {
            None
        } else {
            Some(ndv_map)
        })
    }

    /// One left-deep join step in PredicatePushdown's normal form: equi keys in
    /// the condition, non-equi cross-side conjuncts as one residual filter.
    fn emit_step(
        &self,
        region: &JoinRegion,
        left_tree: LogicalPlan,
        step: &JoinStep,
        placed: &mut Vec<usize>,
    ) -> Result<LogicalPlan, OptimizeError> {
        let atom_tree = self.emit_atom(region, step.atom_index, placed)?;
        placed.extend(step.conjunct_positions.iter().copied());
        let (equi, residual) = split_step_conjuncts(region, &step.conjunct_positions);
        // A genuinely fresh INNER join for the reordered tree; there is no base node,
        // and its estimated_rows/estimate_defaults are freshly stamped FROM the DP
        // search result (step.estimate). The field list here is the complete Join set.
        let join = LogicalPlan::Join(Join {
            left: Box::new(left_tree),
            right: Box::new(atom_tree),
            join_type: JoinType::Inner,
            condition: combine_and(equi),
            natural: false,
            using: None,
            estimated_rows: step.estimate.rows,
            estimate_defaults: Some(step.estimate.defaults_used.clone()),
        });
        Ok(residual_filter(join, residual))
    }
}

/// One CROSS between component subtrees - the only cross product that survives
/// reordering - with any spanning (non-equi) conjuncts as its residual filter.
fn emit_cross(
    region: &JoinRegion,
    left_tree: LogicalPlan,
    right_tree: LogicalPlan,
    cross: &CrossStep,
    placed: &mut Vec<usize>,
) -> LogicalPlan {
    placed.extend(cross.conjunct_positions.iter().copied());
    // A genuinely fresh CROSS join for the reordered tree; no base node, and its
    // estimated_rows/estimate_defaults are freshly stamped FROM the DP search result
    // (cross.estimate). The field list here is the complete Join field set.
    let join = LogicalPlan::Join(Join {
        left: Box::new(left_tree),
        right: Box::new(right_tree),
        join_type: JoinType::Cross,
        condition: None,
        natural: false,
        using: None,
        estimated_rows: cross.estimate.rows,
        estimate_defaults: Some(cross.estimate.defaults_used.clone()),
    });
    residual_filter(join, local_exprs(region, &cross.conjunct_positions))
}

/// Conjuncts referencing no column (e.g. WHERE 1 = 0) evaluate above the whole
/// region as one filter; they constrain no particular atom.
fn wrap_constant_conjuncts(
    region: &JoinRegion,
    tree: LogicalPlan,
    placed: &mut Vec<usize>,
) -> LogicalPlan {
    let mut expressions = Vec::new();
    for (position, conjunct) in region.conjuncts.iter().enumerate() {
        if conjunct.atom_indexes.is_empty() {
            placed.push(position);
            expressions.push(conjunct.expression.clone());
        }
    }
    residual_filter(tree, expressions)
}

/// 3+ atoms always; 2 atoms only when a condition-less join would gain a
/// connecting equality (turning a cross product into an inner join).
fn worth_reordering(region: &JoinRegion, root: &LogicalPlan) -> bool {
    if region.atoms.len() >= 3 {
        return true;
    }
    has_edge(region) && has_conditionless_join(root)
}

/// A tree wrapped in a residual Filter of the given conjuncts, or the bare tree
/// when there are none.
fn residual_filter(tree: LogicalPlan, expressions: Vec<Expr>) -> LogicalPlan {
    match combine_and(expressions) {
        None => tree,
        // A fresh Filter synthesized to carry residual conjuncts above a reordered
        // join step; no base Filter, {input, predicate} is the complete field set.
        Some(predicate) => LogicalPlan::Filter(Filter {
            input: Box::new(tree),
            predicate,
        }),
    }
}

/// Whether the pool holds any multi-atom equi conjunct.
fn has_edge(region: &JoinRegion) -> bool {
    region
        .conjuncts
        .iter()
        .any(|conjunct| conjunct.is_equi && conjunct.atom_indexes.len() >= 2)
}

/// Whether the region contains a reorderable join with no condition.
fn has_conditionless_join(node: &LogicalPlan) -> bool {
    if let LogicalPlan::Filter(filter) = node {
        return has_conditionless_join(&filter.input);
    }
    let LogicalPlan::Join(join) = node else {
        return false;
    };
    if !is_region_join(node) {
        return false;
    }
    if join.condition.is_none() {
        return true;
    }
    has_conditionless_join(&join.left) || has_conditionless_join(&join.right)
}

impl OptimizationRule for JoinOrdering {
    /// This rule's identifier (used in the `validate_scope` stage label).
    fn name(&self) -> &'static str {
        "JoinOrdering"
    }

    /// Reorder every region in the plan. A fresh CTE registry per plan walk: a
    /// previous query's same-named CTE body must never feed this walk's CteRef
    /// estimates. The driver detects the (no-)change by structural equality.
    fn apply(&self, plan: LogicalPlan) -> Result<LogicalPlan, OptimizeError> {
        self.cost_model.borrow_mut().reset_cte_registry();
        self.rewrite(plan)
    }
}

#[cfg(test)]
mod tests;
