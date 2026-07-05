"""Join-graph extraction for the cost-based join-ordering optimizer.

A REGION is a maximal subtree of reorderable joins: INNER and CROSS joins
(with a real ON condition, never NATURAL/USING) plus the Filter nodes sitting
between them, whose conjuncts join the predicate pool. Everything else - a
scan, a derived table, an outer/semi/anti join, a lateral, an aggregate -
stops the descent and becomes an ATOM: an opaque relation the enumerator is
free to reorder but never looks inside.

Every conjunct is classified by the exact set of atoms it references. A
reference that resolves to no atom or to more than one is a malformed plan
and raises: silently mis-placing a predicate manufactures wrong results.
"""

from typing import FrozenSet, List, Optional, Set

from ..model import StateModel
from ..plan.expressions import Expression, column_refs, split_conjuncts
from ..plan.logical import (
    CTERef,
    Filter,
    Join,
    JoinType,
    LogicalPlanNode,
    Scan,
    SubqueryScan,
)
from .pushdown import is_equi_predicate


class JoinGraphError(Exception):
    """A join region whose predicates cannot be soundly mapped to its atoms."""


class JoinAtom(StateModel):
    """One reorderable input of a join region.

    ``index`` is the atom's position in the original plan (the FROM order) -
    the deterministic tie-break for the enumerator. ``qualifiers`` are the
    relation names a predicate above this subtree can reference.
    """

    index: int
    plan: LogicalPlanNode
    qualifiers: Set[str]

    @classmethod
    def create(
        cls,
        *,
        index: int,
        plan: LogicalPlanNode,
        qualifiers: Set[str],
    ) -> "JoinAtom":
        """Sanctioned fresh-construction path for JoinAtom.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            index=index,
            plan=plan,
            qualifiers=qualifiers,
        )


class JoinConjunct(StateModel):
    """One predicate conjunct of a region, mapped to the atoms it references.

    ``is_equi`` marks a column-to-column equality - a join-graph edge the
    enumerator can use as a key; everything else contributes selectivity only.
    """

    expression: Expression
    atom_indexes: FrozenSet[int]
    is_equi: bool

    @classmethod
    def create(
        cls,
        *,
        expression: Expression,
        atom_indexes: FrozenSet[int],
        is_equi: bool,
    ) -> "JoinConjunct":
        """Sanctioned fresh-construction path for JoinConjunct.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            expression=expression,
            atom_indexes=atom_indexes,
            is_equi=is_equi,
        )


class JoinRegion(StateModel):
    """A reorderable join region: its atoms and its classified conjuncts."""

    atoms: List[JoinAtom]
    conjuncts: List[JoinConjunct]

    @classmethod
    def create(
        cls,
        *,
        atoms: List[JoinAtom],
        conjuncts: List[JoinConjunct],
    ) -> "JoinRegion":
        """Sanctioned fresh-construction path for JoinRegion.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            atoms=atoms,
            conjuncts=conjuncts,
        )


def extract_region(root: LogicalPlanNode) -> Optional[JoinRegion]:
    """The join region rooted at this node, or None when there is none.

    The root may be the top join itself or a Filter directly above it (its
    conjuncts belong to the region's pool).
    """
    if not _is_region_root(root):
        return None
    atoms: List[JoinAtom] = []
    expressions: List[Expression] = []
    _descend(root, atoms, expressions)
    conjuncts = []
    for expression in expressions:
        conjuncts.append(_classify(expression, atoms))
    # The extracted region: reorderable atoms in original order plus every
    # predicate of the region classified by the atoms it touches.
    return JoinRegion.create(atoms=atoms, conjuncts=conjuncts)


def _is_region_root(node: LogicalPlanNode) -> bool:
    """Whether a region starts here: a reorderable join, possibly under a
    Filter whose conjuncts then join the region's predicate pool."""
    if isinstance(node, Filter):
        return is_region_join(node.input)
    return is_region_join(node)


def is_region_join(node: LogicalPlanNode) -> bool:
    """A join the enumerator may reorder: INNER/CROSS with an explicit (or no)
    condition. NATURAL/USING joins carry an implicit condition that reordering
    would silently drop, so they are atoms."""
    if not isinstance(node, Join):
        return False
    if node.natural or node.using is not None:
        return False
    return node.join_type in (JoinType.INNER, JoinType.CROSS)


def _descend(node, atoms: List[JoinAtom], expressions: List[Expression]) -> None:
    """Accumulate the region's atoms and predicate expressions."""
    if isinstance(node, Filter):
        expressions.extend(split_conjuncts(node.predicate))
        _descend(node.input, atoms, expressions)
        return
    if is_region_join(node):
        if node.condition is not None:
            expressions.extend(split_conjuncts(node.condition))
        _descend(node.left, atoms, expressions)
        _descend(node.right, atoms, expressions)
        return
    # Anything else stops the region: it becomes an opaque atom at the next
    # index, keeping the original left-to-right (FROM) order.
    atoms.append(
        JoinAtom.create(
            index=len(atoms), plan=node, qualifiers=_visible_qualifiers(node)
        )
    )


def _visible_qualifiers(node: LogicalPlanNode) -> Set[str]:
    """The relation names a predicate above this subtree can reference."""
    found: Set[str] = set()
    _collect_qualifiers(node, found)
    return found


def _collect_qualifiers(node: LogicalPlanNode, found: Set[str]) -> None:
    """Collect visible relation names, stopping where aliasing hides them."""
    if isinstance(node, Scan):
        found.add(node.alias if node.alias else node.table_name)
        return
    if isinstance(node, SubqueryScan):
        # A derived table re-aliases its whole subplan: only its own alias is
        # visible above; the inner relation names are hidden.
        found.add(node.alias)
        return
    if isinstance(node, CTERef):
        found.add(node.alias if node.alias else node.name)
        return
    for child in node.children():
        _collect_qualifiers(child, found)


def _classify(expression: Expression, atoms: List[JoinAtom]) -> JoinConjunct:
    """Map one conjunct to the exact set of atoms it references."""
    indexes: Set[int] = set()
    for ref in column_refs(expression):
        indexes.add(_owning_atom(ref, atoms))
    # The classified conjunct: its atom set drives placement and connectivity;
    # is_equi marks it as a usable join-key edge.
    return JoinConjunct.create(
        expression=expression,
        atom_indexes=frozenset(indexes),
        is_equi=is_equi_predicate(expression),
    )


def _owning_atom(ref, atoms: List[JoinAtom]) -> int:
    """The single atom a qualified reference resolves to; anything else raises."""
    if not ref.table:
        raise JoinGraphError(
            f"unqualified column {ref.column!r} in a join-region predicate; "
            "every post-binder reference must carry its relation qualifier"
        )
    owners: List[int] = []
    for atom in atoms:
        if ref.table in atom.qualifiers:
            owners.append(atom.index)
    if len(owners) != 1:
        raise JoinGraphError(
            f"reference {ref.table}.{ref.column} resolves to "
            f"{len(owners)} atoms; a region predicate must map to exactly one"
        )
    return owners[0]
