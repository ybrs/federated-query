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
from ..plan.expressions import and_expressions, combine_and
from ..plan.logical import (
    Filter,
    Join,
    JoinType,
    LogicalPlanNode,
    Scan,
    transform_children,
)
from .estimate_defaults import (
    DEFAULT_NDV_FRACTION,
    CardinalityEstimate,
    combine_defaults,
)
from .join_graph import (
    JoinConjunct,
    JoinRegion,
    extract_region,
    is_region_join,
)
from .rules import OptimizationRule


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


# ---------------- CostModel-backed estimator + the rule ----------------


def _with_local_filters(plan, conjuncts: List[JoinConjunct]):
    """Fold single-atom conjuncts back onto their atom: straight into a
    scan's pushed-down filters (PredicatePushdown's end state, keeping the
    joint fixpoint stable), a Filter above anything else."""
    if not conjuncts:
        return plan
    expressions = []
    for conjunct in conjuncts:
        expressions.append(conjunct.expression)
    predicate = combine_and(expressions)
    if isinstance(plan, Scan):
        return plan.model_copy(
            update={"filters": and_expressions(plan.filters, predicate)}
        )
    # A non-scan atom cannot absorb the predicate itself; a Filter above it
    # lets predicate pushdown drive it further down on the next pass.
    return Filter.create(input=plan, predicate=predicate)


def _atom_owning(region: JoinRegion, ref) -> "JoinAtom":
    """The region atom whose qualifiers own a column reference."""
    for atom in region.atoms:
        if ref.table in atom.qualifiers:
            return atom
    raise JoinOrderError(
        f"reference {ref.table}.{ref.column} owns no atom in its region"
    )


def _orient_refs(region: JoinRegion, conjunct: JoinConjunct, atom_index: int):
    """(atom-side ref, other-side ref) of a two-atom equi conjunct being
    placed on the step that joins atom_index."""
    left_ref = conjunct.expression.left
    right_ref = conjunct.expression.right
    qualifiers = region.atoms[atom_index].qualifiers
    if left_ref.table in qualifiers:
        return left_ref, right_ref
    if right_ref.table in qualifiers:
        return right_ref, left_ref
    raise JoinOrderError(
        "an equi conjunct was placed on a join step that touches neither "
        "of its columns - the enumerator's placement is broken"
    )


class CostModelRegionEstimator(RegionEstimator):
    """RegionEstimator answering from the CostModel and source statistics."""

    def __init__(self, cost_model):
        """Wire the estimator to the shared cost model."""
        self.cost_model = cost_model

    def atom_estimate(self, region, atom_index, local_conjuncts):
        """The atom with its local filters folded in, estimated exactly as
        the plan will look after pushdown, so iterations stay stable."""
        plan = _with_local_filters(region.atoms[atom_index].plan, local_conjuncts)
        return self.cost_model.estimate(plan)

    def join_estimate(self, region, left, atom_index, atom, conjuncts):
        """left x atom, reduced by every placed conjunct: the NDV formula for
        equi keys, tracked selectivity for everything else."""
        rows = float(left.rows) * float(atom.rows)
        defaults: List[str] = []
        for conjunct in conjuncts:
            factor, conjunct_defaults = self._factor(
                region, conjunct, left, atom, atom_index
            )
            rows *= factor
            defaults.extend(conjunct_defaults)
        # The step's estimate: the sides' provenance plus whatever defaults
        # the conjuncts' selectivities needed.
        return CardinalityEstimate.create(
            rows=max(1, int(rows)),
            defaults_used=combine_defaults([left, atom], defaults),
        )

    def _factor(self, region, conjunct, left, atom, atom_index):
        """One conjunct's multiplicative selectivity at this join step."""
        if not conjunct.is_equi or len(conjunct.atom_indexes) != 2:
            return self.cost_model.conjunct_selectivity(conjunct.expression, "join")
        atom_ref, other_ref = _orient_refs(region, conjunct, atom_index)
        atom_ndv, atom_defaults = self._ref_ndv(region, atom_ref, atom.rows)
        other_ndv, other_defaults = self._ref_ndv(region, other_ref, left.rows)
        return 1.0 / max(atom_ndv, other_ndv), atom_defaults + other_defaults

    def _ref_ndv(self, region, ref, side_rows: int):
        """A key's NDV clamped by its side's current rows; the fallback is the
        named NDV default, recorded with the key it applied to."""
        atom = _atom_owning(region, ref)
        ndv = self.cost_model.column_ndv(atom.plan, ref)
        if ndv is None:
            fallback = max(1, int(side_rows * DEFAULT_NDV_FRACTION))
            return fallback, [f"ndv({ref.table}.{ref.column})"]
        return max(1, min(ndv, side_rows)), []


class JoinOrderingRule(OptimizationRule):
    """Cost-based join ordering over inner-join regions.

    Runs after predicate pushdown (it reads folded equi conditions and
    embedded scan filters), chooses a cardinality-driven order per region and
    re-emits the region in pushdown's normal form. Every region conjunct must
    be placed exactly once - a mismatch raises instead of silently dropping
    a predicate.
    """

    def __init__(self, cost_model, max_join_reorder_size: int):
        """The rule over a cost model and the DP size limit."""
        self.cost_model = cost_model
        self.max_join_reorder_size = max_join_reorder_size
        self.estimator = CostModelRegionEstimator(cost_model)

    def name(self) -> str:
        """Return this rule's identifier (used in logging and EXPLAIN)."""
        return "JoinOrdering"

    def apply(self, plan: LogicalPlanNode) -> Optional[LogicalPlanNode]:
        """Reorder every region in the plan; None when nothing changed."""
        rewritten = self._rewrite(plan)
        if rewritten is plan:
            return None
        return rewritten

    def _rewrite(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Reorder the region rooted here, or recurse into the children."""
        region = extract_region(plan)
        if region is None:
            return transform_children(plan, self._rewrite)
        return self._reorder_region(plan, region)

    def _reorder_region(self, root, region: JoinRegion) -> LogicalPlanNode:
        """Order one region and re-emit it, preserving identity on no-change
        so the optimizer's fixpoint terminates."""
        if not self._worth_reordering(region, root):
            return transform_children(root, self._rewrite)
        order = choose_order(region, self.estimator, self.max_join_reorder_size)
        placed: List[int] = []
        rebuilt = self._emit(region, order, placed)
        self._verify_placement(len(region.conjuncts), placed)
        if rebuilt == root:
            return root
        return rebuilt

    def _worth_reordering(self, region: JoinRegion, root) -> bool:
        """3+ atoms always; 2 atoms only when a condition-less join would gain
        a connecting equality (turning a cross product into an inner join)."""
        if len(region.atoms) >= 3:
            return True
        return self._has_edge(region) and self._has_conditionless_join(root)

    def _has_edge(self, region: JoinRegion) -> bool:
        """Whether the pool holds any multi-atom equi conjunct."""
        for conjunct in region.conjuncts:
            if conjunct.is_equi and len(conjunct.atom_indexes) >= 2:
                return True
        return False

    def _has_conditionless_join(self, node) -> bool:
        """Whether the region contains a reorderable join with no condition."""
        if isinstance(node, Filter):
            return self._has_conditionless_join(node.input)
        if not is_region_join(node):
            return False
        if node.condition is None:
            return True
        return (self._has_conditionless_join(node.left)
                or self._has_conditionless_join(node.right))

    def _verify_placement(self, total: int, placed: List[int]) -> None:
        """Every region conjunct must be placed exactly once; a dropped or
        duplicated predicate manufactures wrong results - crash instead."""
        if sorted(placed) != list(range(total)):
            raise JoinOrderError(
                f"join reordering placed conjuncts {sorted(placed)} of "
                f"{total}; every conjunct must be placed exactly once"
            )

    def _emit(self, region, order: RegionOrder, placed: List[int]):
        """The re-emitted region: component subtrees CROSSed smallest-first,
        zero-atom (constant) conjuncts as one filter above everything."""
        current = self._emit_component(region, order.components[0], placed)
        for position, component in enumerate(order.components[1:]):
            right = self._emit_component(region, component, placed)
            estimate = order.cross_estimates[position]
            # The CROSS between disconnected components - the only cross
            # product that legitimately survives reordering.
            current = Join.create(
                left=current, right=right, join_type=JoinType.CROSS,
                condition=None, estimated_rows=estimate.rows,
                estimate_defaults=estimate.defaults_used,
            )
        return self._wrap_constant_conjuncts(region, current, placed)

    def _wrap_constant_conjuncts(self, region, tree, placed: List[int]):
        """Conjuncts referencing no column (e.g. WHERE 1 = 0) evaluate above
        the whole region as one filter; they constrain no particular atom."""
        expressions = []
        for position, conjunct in enumerate(region.conjuncts):
            if not conjunct.atom_indexes:
                placed.append(position)
                expressions.append(conjunct.expression)
        if not expressions:
            return tree
        # The constant conjuncts as one region-topping filter, conserved
        # verbatim so the placement guard accounts for them.
        return Filter.create(input=tree, predicate=combine_and(expressions))

    def _emit_component(self, region, component: ComponentOrder, placed):
        """One component as its left-deep join subtree."""
        current = self._emit_atom(region, component.first_atom, placed)
        for step in component.steps:
            current = self._emit_step(region, current, step, placed)
        return current

    def _emit_atom(self, region, atom_index: int, placed: List[int]):
        """One atom: nested regions inside it rewritten, its local conjuncts
        folded back onto it."""
        plan = self._rewrite(region.atoms[atom_index].plan)
        local_positions = self._local_positions(region, atom_index)
        placed.extend(local_positions)
        return _with_local_filters(plan, _conjuncts_at(region, local_positions))

    def _local_positions(self, region, atom_index: int) -> List[int]:
        """Positions of the conjuncts referencing exactly this one atom."""
        positions = []
        for position, conjunct in enumerate(region.conjuncts):
            if conjunct.atom_indexes == frozenset({atom_index}):
                positions.append(position)
        return positions

    def _emit_step(self, region, left_tree, step: JoinStep, placed: List[int]):
        """One left-deep join step in PredicatePushdown's normal form: equi
        keys in the condition, non-equi cross-side conjuncts as one filter."""
        atom_tree = self._emit_atom(region, step.atom_index, placed)
        placed.extend(step.conjunct_positions)
        equi, residual = self._split_step_conjuncts(region, step.conjunct_positions)
        # The reordered join, annotated with the enumerator's estimate and
        # its statistics provenance for EXPLAIN.
        join = Join.create(
            left=left_tree, right=atom_tree, join_type=JoinType.INNER,
            condition=combine_and(equi), estimated_rows=step.estimate.rows,
            estimate_defaults=step.estimate.defaults_used,
        )
        if not residual:
            return join
        # Cross-side non-equi conjuncts evaluate only after the join: one
        # residual filter above it, exactly as predicate pushdown leaves them.
        return Filter.create(input=join, predicate=combine_and(residual))

    def _split_step_conjuncts(self, region, positions: List[int]):
        """A step's placed conjuncts split into (equi keys, residuals)."""
        equi = []
        residual = []
        for conjunct in _conjuncts_at(region, positions):
            if conjunct.is_equi:
                equi.append(conjunct.expression)
            else:
                residual.append(conjunct.expression)
        return equi, residual
