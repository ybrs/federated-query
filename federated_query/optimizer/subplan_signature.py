"""Canonical signature for a logical subplan.

A signature identifies a subplan MODULO alias names and constant values, so the
learned catalog can key a measured cardinality (a group count, later a join
output) by the subplan that produced it and find it again at plan time. Two
structurally-equivalent subplans - same base tables, join graph, and filter
SHAPES, differing only in aliases or constants - hash to the SAME signature.

Dropping constants is deliberate and matches the target workload: an agent (or
analyst) exploring a dataset fires many near-identical queries differing only in
filter values, so a shape-level key lets query N's measurement warm query N+1.
It is correctness-neutral - a signature only decides which learned number the
planner reads - so a coarse collision costs at worst a slightly-off estimate,
never a wrong answer.
"""

import hashlib
import json
from typing import Dict, List, Optional

from ..plan.expressions import (
    BinaryOp,
    ColumnRef,
    Expression,
    column_refs,
    split_conjuncts,
)
from ..plan.logical import Filter, Join, LogicalPlanNode, Scan


def subplan_signature(node: LogicalPlanNode) -> str:
    """The canonical signature (a hex digest) of a logical subplan."""
    alias_map = _alias_map(node)
    parts = {
        "tables": _base_tables(node),
        "joins": _join_shapes(node, alias_map),
        "filters": _filter_shapes(node, alias_map),
    }
    canonical = json.dumps(parts, sort_keys=True)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


def group_column_names(group_by) -> Optional[List[str]]:
    """The group-by column names when every key is a plain qualified column,
    else None (an expression key has no stable name to key group_stats on).
    Shared by the write provenance, the cost-model read, and the stamp."""
    names = []
    for key in group_by:
        if not isinstance(key, ColumnRef) or not key.table:
            return None
        names.append(key.column)
    return names


def _base_tables(node: LogicalPlanNode) -> List[str]:
    """Every base table the subplan reads, as sorted ``ds.schema.table`` names
    (multiplicity kept, so a self-join reads as two entries)."""
    names = []
    _collect_scans(node, names)
    names.sort()
    return names


def _collect_scans(node: LogicalPlanNode, names: List[str]) -> None:
    """Append each Scan's qualified table name in the subtree."""
    if isinstance(node, Scan):
        names.append(_table_name(node))
        return
    for child in node.children():
        _collect_scans(child, names)


def _table_name(scan: Scan) -> str:
    """A scan's alias-independent ``ds.schema.table`` identity."""
    return f"{scan.datasource}.{scan.schema_name}.{scan.table_name}"


def _alias_map(node: LogicalPlanNode) -> Dict[str, str]:
    """Map each scan's qualifier (its alias, else its table name) to the base
    table name, so a column reference can be rendered alias-neutrally."""
    mapping: Dict[str, str] = {}
    _collect_aliases(node, mapping)
    return mapping


def _collect_aliases(node: LogicalPlanNode, mapping: Dict[str, str]) -> None:
    """Record every scan's qualifier -> base table name in the subtree."""
    if isinstance(node, Scan):
        qualifier = scan_qualifier(node)
        mapping[qualifier] = _table_name(node)
        return
    for child in node.children():
        _collect_aliases(child, mapping)


def scan_qualifier(scan: Scan) -> str:
    """The qualifier columns use to name this scan: its alias, else its table."""
    return scan.alias if scan.alias else scan.table_name


def _base_column(ref: ColumnRef, alias_map: Dict[str, str]) -> str:
    """A column reference rendered alias-neutrally as ``base_table.column``, or
    ``?.column`` when its qualifier resolves to no scan in this subtree."""
    table = alias_map.get(ref.table, "?")
    return f"{table}.{ref.column}"


def _join_shapes(node: LogicalPlanNode, alias_map) -> List[str]:
    """Every join's equi-key column pairs, alias-neutral and sorted, so the join
    GRAPH (which base columns join which) is captured independent of order."""
    shapes: List[str] = []
    _collect_join_shapes(node, alias_map, shapes)
    shapes.sort()
    return shapes


def _collect_join_shapes(node, alias_map, shapes) -> None:
    """Append the equi-key shapes of every Join condition in the subtree."""
    if isinstance(node, Join) and node.condition is not None:
        for conjunct in split_conjuncts(node.condition):
            shape = _equi_shape(conjunct, alias_map)
            if shape is not None:
                shapes.append(shape)
    for child in node.children():
        _collect_join_shapes(child, alias_map, shapes)


def _equi_shape(conjunct: Expression, alias_map) -> Optional[str]:
    """A ``col = col`` equality as a sorted ``a|b`` base-column pair, or None."""
    if not (isinstance(conjunct, BinaryOp) and conjunct.op.value == "="):
        return None
    if not (isinstance(conjunct.left, ColumnRef) and isinstance(conjunct.right, ColumnRef)):
        return None
    pair = sorted(
        [_base_column(conjunct.left, alias_map), _base_column(conjunct.right, alias_map)]
    )
    return "|".join(pair)


def _filter_shapes(node: LogicalPlanNode, alias_map) -> List[str]:
    """Every filter conjunct's SHAPE - the base columns it touches and its
    operator - with CONSTANTS dropped, sorted. Both a Filter node's predicate
    and a scan's folded filters count."""
    shapes: List[str] = []
    _collect_filter_shapes(node, alias_map, shapes)
    shapes.sort()
    return shapes


def _collect_filter_shapes(node, alias_map, shapes) -> None:
    """Append the conjunct shapes of every filter predicate in the subtree."""
    predicate = _node_predicate(node)
    if predicate is not None:
        for conjunct in split_conjuncts(predicate):
            shapes.append(_conjunct_shape(conjunct, alias_map))
    for child in node.children():
        _collect_filter_shapes(child, alias_map, shapes)


def _node_predicate(node: LogicalPlanNode) -> Optional[Expression]:
    """A node's filter predicate: a Filter's, or a Scan's folded filters."""
    if isinstance(node, Filter):
        return node.predicate
    if isinstance(node, Scan):
        return node.filters
    return None


def _conjunct_shape(conjunct: Expression, alias_map) -> str:
    """One filter conjunct as ``op(base_col,base_col,...)`` - its operator and
    the base columns it references, constants dropped."""
    operator = conjunct.op.value if isinstance(conjunct, BinaryOp) else type(conjunct).__name__
    columns = []
    for ref in column_refs(conjunct):
        columns.append(_base_column(ref, alias_map))
    columns.sort()
    return f"{operator}({','.join(columns)})"
