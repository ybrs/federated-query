"""Tests for join-graph extraction (M3 of the cost-based join optimizer).

extract_region turns a maximal INNER/CROSS join region (plus the Filter nodes
sitting inside it) into atoms - the relations to reorder - and a pool of
classified conjuncts, each mapped to exactly the atoms it references.
"""

import pytest

from federated_query.optimizer.join_graph import (
    JoinGraphError,
    extract_region,
)
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    Literal,
)
from federated_query.plan.logical import (
    Filter,
    Join,
    JoinType,
    Scan,
    SubqueryScan,
)


def _scan(table, alias, columns=("k", "v")):
    """A qualified scan atom for graph tests."""
    return Scan(
        datasource="ds", schema_name="s", table_name=table,
        columns=list(columns), alias=alias,
    )


def _col(table, column):
    """A qualified column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _eq(left, right):
    """left = right."""
    return BinaryOp(op=BinaryOpType.EQ, left=left, right=right)


def _join(left, right, condition, join_type=JoinType.INNER):
    """A join node for graph tests."""
    return Join(left=left, right=right, join_type=join_type, condition=condition)


def _three_scan_region():
    """(a JOIN b ON a.k=b.k) JOIN c ON b.k=c.k."""
    a, b, c = _scan("ta", "a"), _scan("tb", "b"), _scan("tc", "c")
    ab = _join(a, b, _eq(_col("a", "k"), _col("b", "k")))
    return _join(ab, c, _eq(_col("b", "k"), _col("c", "k")))


def test_three_scans_three_atoms_two_edges():
    """A left-deep inner tree yields one atom per scan and one classified
    equi conjunct per join condition."""
    region = extract_region(_three_scan_region())
    assert len(region.atoms) == 3
    assert len(region.conjuncts) == 2
    for conjunct in region.conjuncts:
        assert conjunct.is_equi
        assert len(conjunct.atom_indexes) == 2


def test_atom_order_is_from_order():
    """Atom indexes follow the original (FROM) order of the tree."""
    region = extract_region(_three_scan_region())
    tables = []
    for atom in region.atoms:
        tables.append(atom.plan.table_name)
    assert tables == ["ta", "tb", "tc"]


def test_cross_join_with_filter_becomes_an_edge():
    """A CROSS join under a Filter with a cross-side equality contributes the
    equality to the pool - the connecting edge join ordering needs."""
    a, b = _scan("ta", "a"), _scan("tb", "b")
    cross = _join(a, b, None, join_type=JoinType.CROSS)
    root = Filter(input=cross, predicate=_eq(_col("a", "k"), _col("b", "k")))
    region = extract_region(root)
    assert len(region.atoms) == 2
    assert len(region.conjuncts) == 1
    assert region.conjuncts[0].is_equi
    assert region.conjuncts[0].atom_indexes == frozenset({0, 1})


def test_semi_join_subtree_is_one_atom():
    """A SEMI join (from EXISTS decorrelation) is a region boundary: the whole
    SEMI subtree becomes a single atom."""
    a, b, c = _scan("ta", "a"), _scan("tb", "b"), _scan("tc", "c")
    semi = _join(a, b, _eq(_col("a", "k"), _col("b", "k")), join_type=JoinType.SEMI)
    root = _join(semi, c, _eq(_col("a", "k"), _col("c", "k")))
    region = extract_region(root)
    assert len(region.atoms) == 2
    assert isinstance(region.atoms[0].plan, Join)
    assert region.atoms[0].plan.join_type == JoinType.SEMI


def test_semi_atom_exposes_inner_qualifiers():
    """Conjuncts above a SEMI atom reference its left side's aliases; the
    atom's qualifier set must expose them."""
    a, b, c = _scan("ta", "a"), _scan("tb", "b"), _scan("tc", "c")
    semi = _join(a, b, _eq(_col("a", "k"), _col("b", "k")), join_type=JoinType.SEMI)
    root = _join(semi, c, _eq(_col("a", "k"), _col("c", "k")))
    region = extract_region(root)
    join_conjunct = region.conjuncts[0]
    assert join_conjunct.atom_indexes == frozenset({0, 1})


def test_natural_join_is_an_atom():
    """A NATURAL/USING join renders with no ON; reordering across it would
    drop its implicit condition, so it is a boundary atom."""
    a, b, c = _scan("ta", "a"), _scan("tb", "b"), _scan("tc", "c")
    natural = Join(
        left=a, right=b, join_type=JoinType.INNER, condition=None, natural=True
    )
    root = _join(natural, c, _eq(_col("a", "k"), _col("c", "k")))
    region = extract_region(root)
    assert len(region.atoms) == 2
    assert region.atoms[0].plan.natural


def test_subquery_scan_atom_owns_its_alias():
    """A derived table atom is referenced through its own alias only."""
    a = _scan("ta", "a")
    derived = SubqueryScan(input=_scan("tb", "hidden"), alias="dt")
    root = _join(a, derived, _eq(_col("a", "k"), _col("dt", "k")))
    region = extract_region(root)
    assert len(region.atoms) == 2
    assert region.conjuncts[0].atom_indexes == frozenset({0, 1})


def test_hidden_alias_behind_subquery_raises():
    """A conjunct referencing an alias hidden behind a SubqueryScan resolves
    to no atom - that is a malformed plan and must raise."""
    a = _scan("ta", "a")
    derived = SubqueryScan(input=_scan("tb", "hidden"), alias="dt")
    root = _join(a, derived, _eq(_col("a", "k"), _col("hidden", "k")))
    with pytest.raises(JoinGraphError):
        extract_region(root)


def test_unqualified_column_raises():
    """An unqualified column in a region predicate is an upstream binder bug."""
    a, b = _scan("ta", "a"), _scan("tb", "b")
    bad = _eq(ColumnRef(table=None, column="k", data_type=DataType.INTEGER),
              _col("b", "k"))
    root = _join(a, b, bad)
    with pytest.raises(JoinGraphError):
        extract_region(root)


def test_duplicate_qualifier_raises():
    """The same alias visible from two atoms cannot be resolved - raise."""
    a1, a2 = _scan("ta", "a"), _scan("tb", "a")
    root = _join(a1, a2, _eq(_col("a", "k"), _col("a", "v")))
    with pytest.raises(JoinGraphError):
        extract_region(root)


def test_single_atom_conjunct():
    """A one-sided predicate maps to exactly its atom."""
    a, b = _scan("ta", "a"), _scan("tb", "b")
    join = _join(a, b, _eq(_col("a", "k"), _col("b", "k")))
    local = BinaryOp(
        op=BinaryOpType.GT, left=_col("a", "v"),
        right=Literal(value=10, data_type=DataType.INTEGER),
    )
    root = Filter(input=join, predicate=local)
    region = extract_region(root)
    found = []
    for conjunct in region.conjuncts:
        if not conjunct.is_equi:
            found.append(conjunct)
    assert len(found) == 1
    assert found[0].atom_indexes == frozenset({0})


def test_non_region_root_returns_none():
    """extract_region on a plain scan (no reorderable join) is None."""
    assert extract_region(_scan("ta", "a")) is None


def test_left_join_root_returns_none():
    """A LEFT join root is not a reorderable region."""
    a, b = _scan("ta", "a"), _scan("tb", "b")
    left = _join(a, b, _eq(_col("a", "k"), _col("b", "k")), join_type=JoinType.LEFT)
    assert extract_region(left) is None
