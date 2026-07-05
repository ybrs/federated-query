"""Join-order enumeration for the cost-based optimizer (left-deep DP + greedy).

Given a JoinRegion (atoms + classified conjuncts) and a cardinality estimator,
``choose_order`` picks a join order per CONNECTED COMPONENT of the join graph:

- Selinger-style dynamic programming over connected subsets for components up
  to the configured DP limit. Only CONNECTED extensions are ever generated, so
  an intra-component cross product is structurally impossible - the property
  that fixes the measured q08/q09 blow-ups.
- A GOO-style greedy walk (cheapest connected pair, then always the cheapest
  connected extension) above the limit.

Components are combined smallest-output-first with CROSS joins at the top -
the only place a cross product legitimately exists. Cost is C_out (the sum of
every join's output rows); richer terms belong in the estimator, not here.

Everything is deterministic: iteration is over sorted atom indexes and ties
break on the lexicographically smallest atom sequence.
"""

from abc import ABC, abstractmethod
from typing import Dict, FrozenSet, List, Optional, Set

from ..model import StateModel
from .estimate_defaults import CardinalityEstimate, combine_defaults
from .join_graph import JoinConjunct, JoinRegion


class JoinOrderError(Exception):
    """A join-ordering invariant was violated (a bug, never a user error)."""


class RegionEstimator(ABC):
    """The cardinality oracle the enumerator consults.

    Implementations answer from source statistics (production) or from
    hand-fed tables (tests); the enumerator itself never sees a statistic.
    """

    @abstractmethod
    def atom_estimate(
        self, region: JoinRegion, atom_index: int,
        local_conjuncts: List[JoinConjunct],
    ) -> CardinalityEstimate:
        """The estimated rows of one atom under its single-atom conjuncts."""

    @abstractmethod
    def join_estimate(
        self, region: JoinRegion, left: CardinalityEstimate, atom_index: int,
        atom: CardinalityEstimate, conjuncts: List[JoinConjunct],
    ) -> CardinalityEstimate:
        """The estimated rows of joining the accumulated left side with one
        atom under the conjuncts newly placed at this step."""


class JoinStep(StateModel):
    """One left-deep join step: the atom joined, the estimate, and which
    region conjuncts (by position) are placed on this join."""

    atom_index: int
    estimate: CardinalityEstimate
    conjunct_positions: List[int]

    @classmethod
    def create(
        cls,
        *,
        atom_index: int,
        estimate: CardinalityEstimate,
        conjunct_positions: List[int],
    ) -> "JoinStep":
        """Sanctioned fresh-construction path for JoinStep.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            atom_index=atom_index,
            estimate=estimate,
            conjunct_positions=conjunct_positions,
        )


class ComponentOrder(StateModel):
    """One connected component's chosen left-deep order."""

    first_atom: int
    steps: List[JoinStep]
    total: CardinalityEstimate

    @classmethod
    def create(
        cls,
        *,
        first_atom: int,
        steps: List[JoinStep],
        total: CardinalityEstimate,
    ) -> "ComponentOrder":
        """Sanctioned fresh-construction path for ComponentOrder.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            first_atom=first_atom,
            steps=steps,
            total=total,
        )


class RegionOrder(StateModel):
    """The whole region's order: component subtrees combined by CROSS joins,
    with one estimate per CROSS between successive components."""

    components: List[ComponentOrder]
    cross_estimates: List[CardinalityEstimate]

    @classmethod
    def create(
        cls,
        *,
        components: List[ComponentOrder],
        cross_estimates: List[CardinalityEstimate],
    ) -> "RegionOrder":
        """Sanctioned fresh-construction path for RegionOrder.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            components=components,
            cross_estimates=cross_estimates,
        )


class _Candidate:
    """One left-deep prefix during enumeration (mutable, internal only)."""

    def __init__(self, cost, sequence, steps, estimate):
        """A prefix with its accumulated C_out cost and current estimate."""
        self.cost = cost
        self.sequence = sequence
        self.steps = steps
        self.estimate = estimate


def choose_order(
    region: JoinRegion, estimator: RegionEstimator, max_dp_size: int
) -> RegionOrder:
    """The chosen join order for a region: DP per connected component up to
    max_dp_size atoms, greedy above, components CROSSed smallest-first."""
    if not region.atoms:
        raise JoinOrderError("choose_order over a region with no atoms")
    atom_estimates = _atom_estimates(region, estimator)
    ordered: List[ComponentOrder] = []
    for component in _connected_components(region):
        ordered.append(
            _order_component(region, estimator, atom_estimates, component, max_dp_size)
        )
    ordered.sort(key=_component_sort_key)
    return _combine(ordered)


def _atom_estimates(region, estimator) -> List[CardinalityEstimate]:
    """Every atom's base estimate (under its single-atom conjuncts), by index."""
    estimates = []
    for atom in region.atoms:
        local = _local_conjuncts(region, atom.index)
        estimates.append(estimator.atom_estimate(region, atom.index, local))
    return estimates


def _local_conjuncts(region, atom_index: int) -> List[JoinConjunct]:
    """The conjuncts referencing exactly this one atom (its local filters)."""
    found = []
    for conjunct in region.conjuncts:
        if conjunct.atom_indexes == frozenset({atom_index}):
            found.append(conjunct)
    return found


def _adjacency(region) -> Dict[int, Set[int]]:
    """Atom adjacency: every pair inside a multi-atom conjunct is connected."""
    adjacency: Dict[int, Set[int]] = {}
    for atom in region.atoms:
        adjacency[atom.index] = set()
    for conjunct in region.conjuncts:
        for first in conjunct.atom_indexes:
            for second in conjunct.atom_indexes:
                if first != second:
                    adjacency[first].add(second)
    return adjacency


def _connected_components(region) -> List[List[int]]:
    """The join graph's connected components, each a sorted atom-index list."""
    adjacency = _adjacency(region)
    seen: Set[int] = set()
    components = []
    for atom in region.atoms:
        if atom.index not in seen:
            components.append(sorted(_flood(adjacency, atom.index, seen)))
    return components


def _flood(adjacency, start: int, seen: Set[int]) -> List[int]:
    """Collect one component by flood fill from a starting atom."""
    stack = [start]
    component = []
    while stack:
        index = stack.pop()
        if index in seen:
            continue
        seen.add(index)
        component.append(index)
        for neighbor in sorted(adjacency[index]):
            stack.append(neighbor)
    return component


def _order_component(
    region, estimator, atom_estimates, component: List[int], max_dp_size: int
) -> ComponentOrder:
    """One component's order: trivial, DP, or greedy by its size."""
    if len(component) == 1:
        # A lone atom is already ordered; its own estimate is the component's
        # total output and there are no join steps to record.
        return ComponentOrder.create(
            first_atom=component[0], steps=[], total=atom_estimates[component[0]]
        )
    if len(component) <= max_dp_size:
        candidate = _dp_best(region, estimator, atom_estimates, component)
    else:
        candidate = _greedy(region, estimator, atom_estimates, component)
    return _to_component_order(candidate)


def _to_component_order(candidate: _Candidate) -> ComponentOrder:
    """Freeze an enumeration candidate into the component's final order."""
    # The winning left-deep prefix covering the whole component: its last
    # step's estimate is the component's total output cardinality.
    return ComponentOrder.create(
        first_atom=candidate.sequence[0],
        steps=candidate.steps,
        total=candidate.estimate,
    )


def _dp_best(region, estimator, atom_estimates, component: List[int]) -> _Candidate:
    """Selinger-style DP over connected subsets, left-deep, deterministic."""
    best: Dict[FrozenSet[int], _Candidate] = {}
    for index in component:
        best[frozenset({index})] = _Candidate(
            0.0, [index], [], atom_estimates[index]
        )
    for size in range(2, len(component) + 1):
        _dp_layer(region, estimator, atom_estimates, component, best, size)
    full = best.get(frozenset(component))
    if full is None:
        raise JoinOrderError("DP never covered a connected component fully")
    return full


def _dp_layer(region, estimator, atom_estimates, component, best, size: int) -> None:
    """Extend every (size-1)-subset by each connected atom, keeping the best
    candidate per resulting subset."""
    additions: Dict[FrozenSet[int], _Candidate] = {}
    for subset in _subsets_of_size(best, size - 1):
        _extend_subset(
            region, estimator, atom_estimates, component, best[subset], subset,
            additions,
        )
    best.update(additions)


def _subsets_of_size(best, size: int) -> List[FrozenSet[int]]:
    """The DP subsets of one size, in deterministic sorted order."""
    found = []
    for subset in best:
        if len(subset) == size:
            found.append(subset)
    found.sort(key=_subset_key)
    return found


def _subset_key(subset: FrozenSet[int]) -> tuple:
    """A subset's deterministic sort key."""
    return tuple(sorted(subset))


def _extend_subset(
    region, estimator, atom_estimates, component, candidate, subset, additions
) -> None:
    """Try every connected single-atom extension of one candidate."""
    for atom_index in component:
        if atom_index in subset:
            continue
        extended = _try_extend(
            region, estimator, atom_estimates, candidate, subset, atom_index
        )
        if extended is not None:
            _keep_better(additions, subset | {atom_index}, extended)


def _try_extend(
    region, estimator, atom_estimates, candidate, subset, atom_index: int
) -> Optional[_Candidate]:
    """The candidate extended by one atom, or None when no conjunct connects
    the atom to the subset (a cross product - never generated here)."""
    positions = _newly_covered(region, subset, atom_index)
    if not positions:
        return None
    estimate = estimator.join_estimate(
        region, candidate.estimate, atom_index, atom_estimates[atom_index],
        _conjuncts_at(region, positions),
    )
    # One more left-deep step: this atom joined under the newly covered
    # conjuncts, its estimate appended for EXPLAIN and the C_out cost.
    step = JoinStep.create(
        atom_index=atom_index, estimate=estimate, conjunct_positions=positions
    )
    return _Candidate(
        candidate.cost + estimate.rows, candidate.sequence + [atom_index],
        candidate.steps + [step], estimate,
    )


def _newly_covered(region, subset, atom_index: int) -> List[int]:
    """Positions of the multi-atom conjuncts that become fully covered exactly
    when this atom joins the subset (each conjunct is placed exactly once)."""
    grown = set(subset)
    grown.add(atom_index)
    positions = []
    for position, conjunct in enumerate(region.conjuncts):
        if len(conjunct.atom_indexes) < 2:
            continue
        if conjunct.atom_indexes <= grown and not conjunct.atom_indexes <= subset:
            positions.append(position)
    return positions


def _conjuncts_at(region, positions: List[int]) -> List[JoinConjunct]:
    """The conjunct objects at the given region positions."""
    found = []
    for position in positions:
        found.append(region.conjuncts[position])
    return found


def _keep_better(additions, key, candidate: _Candidate) -> None:
    """Keep the cheaper candidate per subset (ties: smaller atom sequence)."""
    current = additions.get(key)
    if current is None or _candidate_key(candidate) < _candidate_key(current):
        additions[key] = candidate


def _candidate_key(candidate: _Candidate) -> tuple:
    """Candidate ordering: C_out cost first, atom sequence as the tie-break."""
    return (candidate.cost, candidate.sequence)


def _greedy(region, estimator, atom_estimates, component: List[int]) -> _Candidate:
    """GOO-style left-deep greedy for components beyond the DP limit."""
    candidate = _best_pair(region, estimator, atom_estimates, component)
    while len(candidate.sequence) < len(component):
        candidate = _best_extension(
            region, estimator, atom_estimates, component, candidate
        )
    return candidate


def _best_pair(region, estimator, atom_estimates, component) -> _Candidate:
    """The cheapest connected starting pair of the component."""
    best = None
    for first in component:
        best = _better(
            best, _pair_from(region, estimator, atom_estimates, component, first)
        )
    if best is None:
        raise JoinOrderError("no connected starting pair in a multi-atom component")
    return best


def _pair_from(region, estimator, atom_estimates, component, first: int):
    """The cheapest connected pair starting from one given atom."""
    start = _Candidate(0.0, [first], [], atom_estimates[first])
    best = None
    for second in component:
        if second != first:
            best = _better(
                best,
                _try_extend(region, estimator, atom_estimates, start,
                            frozenset({first}), second),
            )
    return best


def _best_extension(region, estimator, atom_estimates, component, candidate) -> _Candidate:
    """The cheapest connected one-atom extension of the greedy prefix."""
    subset = frozenset(candidate.sequence)
    best = None
    for atom_index in component:
        if atom_index not in subset:
            best = _better(
                best,
                _try_extend(region, estimator, atom_estimates, candidate,
                            subset, atom_index),
            )
    if best is None:
        raise JoinOrderError("greedy walk stranded: no connected extension left")
    return best


def _better(best, contender) -> Optional[_Candidate]:
    """The smaller candidate by (cost, sequence); tolerates None inputs."""
    if contender is None:
        return best
    if best is None or _candidate_key(contender) < _candidate_key(best):
        return contender
    return best


def _component_sort_key(order: ComponentOrder) -> tuple:
    """Components sort by ascending output rows, then by atom sequence."""
    sequence = [order.first_atom]
    for step in order.steps:
        sequence.append(step.atom_index)
    return (order.total.rows, sequence)


def _combine(ordered: List[ComponentOrder]) -> RegionOrder:
    """Combine component orders with CROSS joins, smallest output first."""
    cross_estimates: List[CardinalityEstimate] = []
    running = ordered[0].total
    for component in ordered[1:]:
        # The CROSS between two disconnected components: nothing constrains
        # it, so the estimate is the plain product of the two sides' totals.
        crossed = CardinalityEstimate.create(
            rows=running.rows * component.total.rows,
            defaults_used=combine_defaults([running, component.total], []),
        )
        cross_estimates.append(crossed)
        running = crossed
    # The region's final order: each component its own left-deep subtree,
    # CROSSed together in ascending-cardinality order.
    return RegionOrder.create(components=ordered, cross_estimates=cross_estimates)
