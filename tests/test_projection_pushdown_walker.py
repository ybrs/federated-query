"""Tests for prune-walker totality (PP-M4): derived tables, laterals,
decorrelation wrappers, and the fail-loud unknown-node arm.
"""

import pytest

from federated_query.optimizer.rules import ProjectionPushdownRule
from federated_query.plan.expressions import ColumnRef, DataType
from federated_query.plan.logical import (
    Aggregate,
    GroupedLimit,
    Join,
    JoinType,
    LateralJoin,
    LogicalPlanNode,
    Projection,
    Scan,
    SingleRowGuard,
    Sort,
    SubqueryScan,
)
from federated_query.plan.expressions import BinaryOp, BinaryOpType


def _scan(table, alias, columns):
    """A scan with explicit alias and columns."""
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


def _scans_by_alias(plan, found=None):
    """Every scan in a tree keyed by alias."""
    if found is None:
        found = {}
    if isinstance(plan, Scan):
        found[plan.alias] = plan
    for child in plan.children():
        _scans_by_alias(child, found)
    return found


def test_q07_shape_prunes_inside_derived_table():
    """Sort(Aggregate(SubqueryScan(Projection(join)))) - the q07/q09 shape -
    must prune the scans INSIDE the derived table."""
    supplier = _scan("supplier", "su", ("s_id", "s_nation", "s_comment"))
    lineitem = _scan("lineitem", "l", ("l_s", "l_price", "l_ship", "l_tax"))
    join = Join(
        left=supplier, right=lineitem, join_type=JoinType.INNER,
        condition=_eq(_col("su", "s_id"), _col("l", "l_s")),
    )
    inner = Projection(
        input=join,
        expressions=[_col("su", "s_nation"), _col("l", "l_price")],
        aliases=["nation", "volume"],
    )
    derived = SubqueryScan(input=inner, alias="shipping")
    aggregate = Aggregate(
        input=derived,
        group_by=[_col("shipping", "nation")],
        aggregates=[_col("shipping", "volume")],
        output_names=["nation", "volume"],
    )
    tree = Sort(
        input=aggregate,
        sort_keys=[_col("shipping", "nation")], ascending=[True],
        nulls_order=[None],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    scans = _scans_by_alias(result)
    assert scans["su"].columns == ["s_id", "s_nation"]
    assert scans["l"].columns == ["l_s", "l_price"]


def test_bare_scan_derived_table_untouched():
    """A derived table whose body is a bare scan (its columns ARE its output
    schema) must not be pruned."""
    derived = SubqueryScan(
        input=_scan("t", "t", ("a", "b", "c")), alias="dt"
    )
    tree = Projection(
        input=derived, expressions=[_col("dt", "a")], aliases=["a"]
    )
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scans_by_alias(pruned)["t"].columns == ["a", "b", "c"]


def test_lateral_left_keeps_columns_used_by_the_body():
    """A column of the outer side referenced only INSIDE the lateral body
    must survive pruning of the outer scan."""
    outer = _scan("t", "t", ("a", "b", "c"))
    body = Projection(
        input=Scan(
            datasource="ds", schema_name="s", table_name="u", columns=["k", "v"],
            alias="u",
            filters=_eq(_col("u", "k"), _col("t", "b")),
        ),
        expressions=[_col("u", "v")],
        aliases=["v"],
    )
    lateral = LateralJoin(
        left=outer, right=SubqueryScan(input=body, alias="lat"),
        join_type=JoinType.INNER,
    )
    tree = Projection(
        input=lateral,
        expressions=[_col("t", "a"), _col("lat", "v")],
        aliases=["a", "v"],
    )
    result = ProjectionPushdownRule().apply(tree)
    assert result is not None
    scans = _scans_by_alias(result)
    assert scans["t"].columns == ["a", "b"]
    assert scans["u"].columns == ["k", "v"]


def test_keys_survive_below_decorrelation_wrappers():
    """GroupedLimit and SingleRowGuard keys must be collected so the scan
    below them keeps the key columns."""
    scan = _scan("t", "t", ("a", "b", "c"))
    guarded = SingleRowGuard(input=scan, keys=[_col("t", "b")])
    limited = GroupedLimit(
        input=guarded, keys=[_col("t", "c")], limit=1,
        order_by_keys=[_col("t", "a")], order_by_ascending=[True],
        order_by_nulls=[None],
    )
    tree = Projection(input=limited, expressions=[_col("t", "a")], aliases=["a"])
    result = ProjectionPushdownRule().apply(tree)
    pruned = result if result is not None else tree
    assert _scans_by_alias(pruned)["t"].columns == ["a", "b", "c"]


def test_unknown_node_type_raises():
    """A plan node the prune walker does not know must raise, never be
    silently passed through (a skipped node could hide scans that then keep
    stale semantics)."""

    class _Mystery(LogicalPlanNode):
        def children(self):
            return []

        def schema(self):
            return []

    tree = Projection(
        input=_Mystery(), expressions=[_col("t", "a")], aliases=["a"]
    )
    rule = ProjectionPushdownRule()
    with pytest.raises(ValueError):
        rule._prune_columns(tree, {"t.a"})
