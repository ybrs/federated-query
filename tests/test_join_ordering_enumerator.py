"""Tests for the join-order enumerator (M4 of the cost-based optimizer).

The enumerator is exercised with a STUB estimator (hand-fed cardinalities and
per-edge selectivities, no databases), so every assertion is about ordering
decisions, cross-product avoidance and determinism - not about statistics.
"""

import pytest

from federated_query.optimizer.estimate_defaults import CardinalityEstimate
from federated_query.optimizer.join_graph import (
    JoinAtom,
    JoinConjunct,
    JoinRegion,
)
from federated_query.optimizer.join_ordering import RegionEstimator, choose_order
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
)
from federated_query.plan.logical import Scan


def _atom(index, name):
    """A named scan atom (the plan content is irrelevant to the enumerator)."""
    scan = Scan(
        datasource="ds", schema_name="s", table_name=name,
        columns=["k"], alias=name,
    )
    return JoinAtom(index=index, plan=scan, qualifiers={name})


def _edge(atoms_pair, names):
    """An equi conjunct connecting two atoms (expression content unused)."""
    left = ColumnRef(table=names[0], column="k", data_type=DataType.INTEGER)
    right = ColumnRef(table=names[1], column="k", data_type=DataType.INTEGER)
    return JoinConjunct(
        expression=BinaryOp(op=BinaryOpType.EQ, left=left, right=right),
        atom_indexes=frozenset(atoms_pair),
        is_equi=True,
    )


class _StubEstimator(RegionEstimator):
    """Cardinalities from a rows-per-atom table and selectivity-per-edge map
    keyed by the conjunct's atom_indexes frozenset."""

    def __init__(self, rows_by_atom, selectivity_by_edge):
        self.rows_by_atom = rows_by_atom
        self.selectivity_by_edge = selectivity_by_edge

    def atom_estimate(self, region, atom_index, local_conjuncts):
        """The atom's hand-fed base cardinality."""
        return CardinalityEstimate.create(
            rows=self.rows_by_atom[atom_index], defaults_used=[]
        )

    def join_estimate(self, region, left, atom_index, atom, conjuncts):
        """left x atom x the product of the placed edges' selectivities."""
        rows = float(left.rows) * float(atom.rows)
        for conjunct in conjuncts:
            rows *= self.selectivity_by_edge[conjunct.atom_indexes]
        return CardinalityEstimate.create(
            rows=max(1, int(rows)), defaults_used=[]
        )

    def cross_estimate(self, region, left, right, conjuncts):
        """left x right x the spanning conjuncts' selectivities."""
        rows = float(left.rows) * float(right.rows)
        for conjunct in conjuncts:
            rows *= self.selectivity_by_edge[conjunct.atom_indexes]
        return CardinalityEstimate.create(
            rows=max(1, int(rows)), defaults_used=[]
        )


def _region(names, edges):
    """A region of named atoms plus (i, j) equi edges."""
    atoms = []
    for index, name in enumerate(names):
        atoms.append(_atom(index, name))
    conjuncts = []
    for pair in edges:
        conjuncts.append(_edge(pair, (names[pair[0]], names[pair[1]])))
    return JoinRegion(atoms=atoms, conjuncts=conjuncts)


def _sequence(order):
    """The flat atom-index order of a single-component result."""
    assert len(order.components) == 1
    component = order.components[0]
    result = [component.first_atom]
    for step in component.steps:
        result.append(step.atom_index)
    return result


def _q05_shape():
    """The q05 join graph: cyclic (customer-supplier directly on nationkey AND
    via orders-lineitem), with SF1-like cardinalities. The measured trap: a
    small-tables-first order joins customer x supplier early on nationkey and
    builds a ~60M-row intermediate (10x slower than FROM order)."""
    names = ["region", "nation", "supplier", "customer", "orders", "lineitem"]
    edges = [
        (0, 1),  # region-nation
        (1, 2),  # nation-supplier
        (2, 3),  # supplier-customer (nationkey - the trap edge)
        (3, 4),  # customer-orders
        (4, 5),  # orders-lineitem
        (2, 5),  # supplier-lineitem
    ]
    rows = {0: 1, 1: 25, 2: 10_000, 3: 150_000, 4: 225_000, 5: 6_000_000}
    selectivity = {
        frozenset({0, 1}): 1.0 / 5,
        frozenset({1, 2}): 1.0 / 25,
        frozenset({2, 3}): 1.0 / 25,
        frozenset({3, 4}): 1.0 / 150_000,
        frozenset({4, 5}): 1.0 / 1_500_000,
        frozenset({2, 5}): 1.0 / 10_000,
    }
    region = _region(names, edges)
    return region, _StubEstimator(rows, selectivity)


def test_q05_cycle_avoids_nationkey_blowup():
    """The measured q05 trap builds customer x supplier on the nationkey edge
    alone: a 60M-row intermediate (10x slower). Whatever order the DP picks,
    no step's estimate may come anywhere near that blow-up."""
    region, estimator = _q05_shape()
    order = choose_order(region, estimator, max_dp_size=10)
    for step in order.components[0].steps:
        assert step.estimate.rows < 2_000_000


def test_q05_never_starts_with_the_trap_pair():
    """The order must not begin by joining supplier and customer directly:
    that is exactly the nationkey-only join the experiment measured 10x
    slower. Their edge may only be placed once another path constrains it."""
    region, estimator = _q05_shape()
    sequence = _sequence(choose_order(region, estimator, max_dp_size=10))
    assert set(sequence[:2]) != {2, 3}


def test_q05_beats_the_naive_small_first_order():
    """The chosen order's C_out must beat the naive small-tables-first order
    (region, nation, supplier, customer, ...), whose customer step joins on
    the nationkey edge alone and explodes to ~12M rows under these stats."""
    region, estimator = _q05_shape()
    order = choose_order(region, estimator, max_dp_size=10)
    chosen_cost = 0
    for step in order.components[0].steps:
        chosen_cost += step.estimate.rows
    # Naive order hand-evaluated under the same stub selectivities:
    # r*n=5, *s=2000, *c(nationkey only)=12M, *o=18M, *l=7200.
    naive_cost = 5 + 2000 + 12_000_000 + 18_000_000 + 7200
    assert chosen_cost < naive_cost


def test_q08_shape_avoids_cross_product():
    """part and supplier share no edge (the q08/q09 killer): every step of the
    chosen order must be connected - no cross join inside the component."""
    names = ["part", "supplier", "lineitem"]
    edges = [(0, 2), (1, 2)]
    rows = {0: 200, 1: 10_000, 2: 6_000_000}
    selectivity = {
        frozenset({0, 2}): 1.0 / 200_000,
        frozenset({1, 2}): 1.0 / 10_000,
    }
    region = _region(names, edges)
    order = choose_order(region, _StubEstimator(rows, selectivity), max_dp_size=10)
    component = order.components[0]
    for step in component.steps:
        assert step.conjunct_positions, "a step joined with no connecting edge"


def test_disconnected_components_cross_at_top_smallest_first():
    """Two disconnected pairs become two components combined by a CROSS at
    the top, the smaller-output component first."""
    names = ["a", "b", "c", "d"]
    edges = [(0, 1), (2, 3)]
    rows = {0: 1000, 1: 1000, 2: 10, 3: 10}
    selectivity = {frozenset({0, 1}): 1.0 / 1000, frozenset({2, 3}): 1.0 / 10}
    region = _region(names, edges)
    order = choose_order(region, _StubEstimator(rows, selectivity), max_dp_size=10)
    assert len(order.components) == 2
    assert len(order.cross_steps) == 1
    # c-d yields 10 rows, a-b yields 1000: the smaller component leads.
    assert order.components[0].first_atom in (2, 3)
    assert order.cross_steps[0].estimate.rows == 10 * 1000


def test_greedy_used_above_dp_limit():
    """A 12-atom chain forces the greedy path; the result is still a fully
    connected order over every atom."""
    count = 12
    names = []
    for index in range(count):
        names.append(f"t{index}")
    edges = []
    rows = {}
    selectivity = {}
    for index in range(count):
        rows[index] = 10 * (index + 1)
        if index:
            edges.append((index - 1, index))
            selectivity[frozenset({index - 1, index})] = 0.01
    region = _region(names, edges)
    order = choose_order(region, _StubEstimator(rows, selectivity), max_dp_size=10)
    sequence = _sequence(order)
    assert sorted(sequence) == list(range(count))
    for step in order.components[0].steps:
        assert step.conjunct_positions


def test_deterministic_across_runs():
    """The same region and estimator produce the identical order twice."""
    region, estimator = _q05_shape()
    first = choose_order(region, estimator, max_dp_size=10)
    second = choose_order(region, estimator, max_dp_size=10)
    assert first == second


def test_single_atom_region():
    """A one-atom component has no steps and its atom estimate as total."""
    region = _region(["only"], [])
    estimator = _StubEstimator({0: 42}, {})
    order = choose_order(region, estimator, max_dp_size=10)
    assert len(order.components) == 1
    assert order.components[0].first_atom == 0
    assert order.components[0].steps == []
    assert order.components[0].total.rows == 42


def test_every_multi_atom_conjunct_placed_exactly_once():
    """Every 2+-atom conjunct of the region appears in exactly one step's
    conjunct_positions - the conservation property re-emission relies on."""
    region, estimator = _q05_shape()
    order = choose_order(region, estimator, max_dp_size=10)
    placed = []
    for component in order.components:
        for step in component.steps:
            placed.extend(step.conjunct_positions)
    expected = []
    for position, conjunct in enumerate(region.conjuncts):
        if len(conjunct.atom_indexes) >= 2:
            expected.append(position)
    assert sorted(placed) == sorted(expected)


def _q07_shape():
    """The q07 join graph: n1 and n2 are connected to each other ONLY by a
    non-equi conjunct (the FRANCE/GERMANY OR), while every other connection
    is an equi key. Measured: starting with the n1 x n2 pair executes as a
    nested-loop/cross and runs 7x slower than joining n2 last through its
    equi key, despite the pair's tiny estimated output."""
    names = ["n1", "n2", "supplier", "lineitem", "orders", "customer"]
    edges = [
        (0, 2),  # n1-supplier (equi)
        (2, 3),  # supplier-lineitem (equi)
        (3, 4),  # lineitem-orders (equi)
        (4, 5),  # orders-customer (equi)
        (1, 5),  # n2-customer (equi)
    ]
    region = _region(names, edges)
    # The OR conjunct: non-equi, spanning n1 and n2 only.
    or_conjunct = JoinConjunct(
        expression=BinaryOp(
            op=BinaryOpType.OR,
            left=ColumnRef(table="n1", column="n_name", data_type=DataType.VARCHAR),
            right=ColumnRef(table="n2", column="n_name", data_type=DataType.VARCHAR),
        ),
        atom_indexes=frozenset({0, 1}),
        is_equi=False,
    )
    conjuncts = list(region.conjuncts)
    conjuncts.append(or_conjunct)
    region = JoinRegion(atoms=region.atoms, conjuncts=conjuncts)
    rows = {0: 25, 1: 25, 2: 10_000, 3: 6_000_000, 4: 1_500_000, 5: 150_000}
    selectivity = {
        frozenset({0, 2}): 1.0 / 25,
        frozenset({2, 3}): 1.0 / 10_000,
        frozenset({3, 4}): 1.0 / 1_500_000,
        frozenset({4, 5}): 1.0 / 150_000,
        frozenset({1, 5}): 1.0 / 25,
        frozenset({0, 1}): 0.0032,  # the OR's true selectivity - very small
    }
    return region, _StubEstimator(rows, selectivity)


def test_q07_shape_never_pairs_on_a_non_equi_edge():
    """Even though the n1 x n2 pair has the smallest estimated output, every
    join step must place at least one EQUI conjunct: a non-equi-only
    connection is a nested-loop in the engine, not a hash join."""
    region, estimator = _q07_shape()
    order = choose_order(region, estimator, max_dp_size=10)
    assert len(order.components) == 1
    for step in order.components[0].steps:
        placed_equi = False
        for position in step.conjunct_positions:
            if region.conjuncts[position].is_equi:
                placed_equi = True
        assert placed_equi, "a step joined without any equi key"


def test_q07_shape_or_conjunct_still_placed_once():
    """The non-equi OR conjunct is still placed exactly once (on the step
    that completes its atom set), so predicate conservation holds."""
    region, estimator = _q07_shape()
    order = choose_order(region, estimator, max_dp_size=10)
    or_position = len(region.conjuncts) - 1
    placements = 0
    for component in order.components:
        for step in component.steps:
            if or_position in step.conjunct_positions:
                placements += 1
    for cross in order.cross_steps:
        if or_position in cross.conjunct_positions:
            placements += 1
    assert placements == 1


def test_non_equi_only_connection_splits_components():
    """Two atoms linked ONLY by a non-equi conjunct are separate components:
    the conjunct rides on the CROSS step between them, never lost."""
    region = _region(["a", "b"], [])
    non_equi = JoinConjunct(
        expression=BinaryOp(
            op=BinaryOpType.GT,
            left=ColumnRef(table="a", column="k", data_type=DataType.INTEGER),
            right=ColumnRef(table="b", column="k", data_type=DataType.INTEGER),
        ),
        atom_indexes=frozenset({0, 1}),
        is_equi=False,
    )
    region = JoinRegion(atoms=region.atoms, conjuncts=[non_equi])
    estimator = _StubEstimator({0: 10, 1: 20}, {frozenset({0, 1}): 0.5})
    order = choose_order(region, estimator, max_dp_size=10)
    assert len(order.components) == 2
    assert len(order.cross_steps) == 1
    assert order.cross_steps[0].conjunct_positions == [0]
