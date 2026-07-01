"""Shared predicate/column helpers for the pushdown optimization rules.

Predicate, projection, and order-by pushdown all need to ask the same two
questions - "what columns does this expression reference?" and "does a column
set belong to one side of a join?" - so the answers live here once instead of
being re-implemented per rule.
"""

from typing import List, Set

from ..plan.expressions import Expression, ColumnRef, column_refs
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Join,
    Projection,
    Aggregate,
    Filter,
    Limit,
    SubqueryScan,
)


class SidePlacementError(Exception):
    """A qualified column does not resolve to exactly one join side.

    Raised instead of falling back to a name guess: after binding every base
    column carries its resolving relation, so a column that resolves to neither
    side (or to both) means the plan mis-placed the predicate - a loud bug, not
    something to paper over.
    """


def relation_identities(plan: LogicalPlanNode) -> Set[str]:
    """The relation identities a plan subtree EXPOSES (scan alias and table
    name, derived-table alias).

    This is the authoritative basis for deciding which join side owns a column:
    a bound ColumnRef carries its resolving relation in ``.table``, so the side
    that owns it is the side whose relation identities contain that qualifier -
    no column-name matching against a scan's read-set (which may be ``['*']``).
    """
    identities: Set[str] = set()
    _collect_relation_identities(plan, identities)
    return identities


def _collect_relation_identities(plan: LogicalPlanNode, identities: Set[str]) -> None:
    """Gather relation identities a subtree exposes, stopping at a derived-table
    boundary (a SubqueryScan exposes its alias, not its internals)."""
    if isinstance(plan, Scan):
        # A scan's effective identity is its alias when set, else its table
        # name. The binder qualifies every ColumnRef with this same value, so a
        # self-join / self-correlation stays distinguishable (an aliased outer
        # 'orders O' is {O}, the inner 'orders' is {orders}).
        identities.add(plan.alias if plan.alias else plan.table_name)
        return
    if isinstance(plan, SubqueryScan):
        identities.add(plan.alias)
        return
    for child in plan.children():
        _collect_relation_identities(child, identities)


def _column_side(ref: ColumnRef, left_ids: Set[str], right_ids: Set[str]) -> str:
    """Which join side owns a single column, by its resolved qualifier.

    Raises SidePlacementError when the column is unqualified or its qualifier
    resolves to neither side or (ambiguously) to both.
    """
    if ref.table is None:
        raise SidePlacementError(
            f"unqualified column '{ref.column}' reached a join-side decision"
        )
    in_left = ref.table in left_ids
    in_right = ref.table in right_ids
    if in_left == in_right:
        raise SidePlacementError(
            f"column '{ref.table}.{ref.column}' does not resolve to exactly one join side"
        )
    if in_left:
        return "left"
    return "right"


def predicate_join_side(
    predicate: Expression, left_ids: Set[str], right_ids: Set[str]
) -> str:
    """Classify a predicate as belonging to the 'left', 'right', or 'both' join
    sides, purely by its columns' resolved qualifiers.

    A predicate with no columns (a constant) is treated as 'left' so it still
    pushes to a single side, matching the prior behavior. Any column whose
    qualifier resolves to neither side raises SidePlacementError.
    """
    saw_left = False
    saw_right = False
    for ref in column_refs(predicate):
        if _column_side(ref, left_ids, right_ids) == "left":
            saw_left = True
        else:
            saw_right = True
    if saw_left and saw_right:
        return "both"
    if saw_right:
        return "right"
    return "left"


def _ref_key(ref: ColumnRef) -> str:
    """A column reference as ``table.column``, or bare ``column`` if unqualified."""
    if ref.table:
        return f"{ref.table}.{ref.column}"
    return ref.column


def qualified_or_bare_names(expr: Expression) -> Set[str]:
    """Every column in an expression as ``table.col`` (or bare ``col``).

    The qualifier decides which join side a filter belongs to when both inputs
    expose a column of the same name. Built on the shared column_refs walker, so
    every expression node type is covered.
    """
    names: Set[str] = set()
    for ref in column_refs(expr):
        names.add(_ref_key(ref))
    return names


def bare_names(expr: Expression) -> Set[str]:
    """Every column in an expression as its bare name (no table qualifier)."""
    names: Set[str] = set()
    for ref in column_refs(expr):
        names.add(ref.column)
    return names


def column_key_set(items: List[Expression]) -> Set[str]:
    """One authoritative name per plain column key (``table.col`` or bare ``col``).

    Mirrors ``qualified_or_bare_names``: a qualified key contributes only its
    qualified form, so side matching (columns_belong_to_side) judges it by that
    one form and bare-name tolerance handles alias-vs-physical differences.
    Non-column items (e.g. ``UPPER(x)``) are ignored, matching GROUP BY / ORDER BY
    where only plain column keys participate in side matching.
    """
    names: Set[str] = set()
    for item in items:
        if isinstance(item, ColumnRef):
            names.add(_ref_key(item))
    return names


def columns_belong_to_side(
    cols: Set[str], side_cols: Set[str], other_cols: Set[str]
) -> bool:
    """Whether every column in ``cols`` belongs uniquely to one join side."""
    for col in cols:
        if not _belongs_to_side(col, side_cols, other_cols):
            return False
    return True


def _belongs_to_side(col: str, side_cols: Set[str], other_cols: Set[str]) -> bool:
    """Whether one column reference belongs uniquely to ``side_cols``.

    It must be exposed by this side (matched by bare name, which tolerates
    alias-vs-physical-name differences like ``o.amount`` vs ``orders.amount``)
    AND not by the other side - unless it is explicitly qualified to this side,
    which is unambiguous even when both sides share the bare name (a self-join).
    """
    bare = col.split(".")[-1]
    if not _side_has(side_cols, bare):
        return False
    if not _side_has(other_cols, bare):
        return True
    return col in side_cols and col not in other_cols


def _side_has(cols: Set[str], bare: str) -> bool:
    """Whether a column set contains the bare name, qualified or unqualified."""
    for entry in cols:
        if entry == bare or entry.endswith(f".{bare}"):
            return True
    return False


def available_columns(plan: LogicalPlanNode) -> Set[str]:
    """Column names a plan subtree exposes, as both bare and qualified forms.

    The single answer to "which columns does this side of a join expose?", used
    by both predicate and order-by pushdown to decide which side a predicate or
    sort key belongs to. Both forms are returned so a reference resolves whether
    written bare (``id``), aliased (``o.id``), or by physical name
    (``orders.id``). A node type with no rule returns the empty set, which is
    conservative: the caller then declines to push (correctness over the
    optimization).
    """
    if isinstance(plan, Scan):
        return _scan_columns(plan)
    if isinstance(plan, Join):
        return available_columns(plan.left) | available_columns(plan.right)
    if isinstance(plan, Projection):
        return _projection_columns(plan)
    if isinstance(plan, Aggregate):
        return set(plan.output_names)
    if isinstance(plan, (Filter, Limit)):
        return available_columns(plan.input)
    return set()


def _scan_columns(scan: Scan) -> Set[str]:
    """A scan's columns as bare and alias-qualified (``alias.col``) names."""
    table_ref = scan.alias if scan.alias else scan.table_name
    names: Set[str] = set()
    for col in scan.columns:
        names.add(col)
        names.add(f"{table_ref}.{col}")
    return names


def _projection_columns(projection: Projection) -> Set[str]:
    """A projection's output aliases plus the columns its expressions reference.

    Aliases are what a parent sees; the referenced columns let an ORDER BY over
    an alias be matched back to the input. A bare ``*`` falls back to the input.
    """
    names = set(projection.aliases)
    if "*" in names:
        return available_columns(projection.input)
    for expr in projection.expressions:
        if isinstance(expr, ColumnRef):
            if expr.table:
                names.add(f"{expr.table}.{expr.column}")
            names.add(expr.column)
    return names
