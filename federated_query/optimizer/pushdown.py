"""Shared predicate/column helpers for the pushdown optimization rules.

Predicate, projection, and order-by pushdown all need to ask the same two
questions - "what columns does this expression reference?" and "does a column
set belong to one side of a join?" - so the answers live here once instead of
being re-implemented per rule.
"""

from typing import List, Set

from ..plan.expressions import Expression, ColumnRef, column_refs


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
    """Both qualified and bare names of the plain column keys in a list.

    Non-column items (e.g. ``UPPER(x)``) are ignored: this matches GROUP BY /
    ORDER BY keys, where only bare column keys participate in side matching.
    """
    names: Set[str] = set()
    for item in items:
        if isinstance(item, ColumnRef):
            if item.table:
                names.add(f"{item.table}.{item.column}")
            names.add(item.column)
    return names


def columns_belong_to_side(
    cols: Set[str], side_cols: Set[str], other_cols: Set[str]
) -> bool:
    """Whether every column in ``cols`` belongs uniquely to one join side.

    A column matches a side if it is present there exactly (qualified) or by bare
    name - handling unqualified references and alias-vs-physical-name mismatches
    (``o.amount`` vs ``orders.amount``) - and is NOT present on the other side.
    """
    for col in cols:
        if col in side_cols:
            continue
        bare = col.split(".")[-1]
        if _side_has(side_cols, bare) and not _side_has(other_cols, bare):
            continue
        return False
    return True


def _side_has(cols: Set[str], bare: str) -> bool:
    """Whether a column set contains the bare name, qualified or unqualified."""
    for entry in cols:
        if entry == bare or entry.endswith(f".{bare}"):
            return True
    return False
