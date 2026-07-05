"""Tests for the cost-based JoinOrderingRule (M5).

The rule extracts inner-join regions, chooses a cardinality-driven order via
the CostModel-backed estimator, and re-emits the region in PredicatePushdown's
normal form with per-join estimates annotated. Statistics are hand-seeded
through the fake-source collector pattern - no databases.
"""

import pytest

from federated_query.config.config import CostConfig
from federated_query.datasources.base import ColumnStatistics, TableStatistics
from federated_query.optimizer.cost import CostModel
from federated_query.optimizer.join_ordering import (
    JoinOrderError,
    JoinOrderingRule,
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
    Projection,
    Scan,
)
from tests.test_cost_model import _seeded_collector


def _stats(row_count, ndv_by_column):
    """Table statistics with per-column NDVs."""
    column_stats = {}
    for column, ndv in ndv_by_column.items():
        column_stats[column] = ColumnStatistics(
            num_distinct=ndv, null_fraction=0.0, avg_width=8
        )
    return TableStatistics(
        row_count=row_count, total_size_bytes=0, column_stats=column_stats
    )


def _q09_like_model():
    """SF-like stats for the q08/q09 killer shape: part and supplier share no
    predicate; both join lineitem."""
    tables = {
        ("s", "part"): _stats(200_000, {"p_partkey": 200_000}),
        ("s", "supplier"): _stats(10_000, {"s_suppkey": 10_000}),
        ("s", "lineitem"): _stats(
            6_000_000, {"l_partkey": 200_000, "l_suppkey": 10_000}
        ),
    }
    return CostModel(CostConfig(), _seeded_collector("ds", tables))


def _scan(table, alias, columns):
    """A qualified scan of a seeded table."""
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


def _q09_from_order_tree():
    """FROM-order left-deep: (part JOIN supplier <no condition>) JOIN lineitem
    ON p=l AND s=l - the shape that OOM-killed q09 at SF1."""
    part = _scan("part", "p", ["p_partkey"])
    supplier = _scan("supplier", "su", ["s_suppkey"])
    lineitem = _scan("lineitem", "l", ["l_partkey", "l_suppkey"])
    cross = Join(left=part, right=supplier, join_type=JoinType.INNER, condition=None)
    condition = BinaryOp(
        op=BinaryOpType.AND,
        left=_eq(_col("p", "p_partkey"), _col("l", "l_partkey")),
        right=_eq(_col("su", "s_suppkey"), _col("l", "l_suppkey")),
    )
    return Join(left=cross, right=lineitem, join_type=JoinType.INNER,
                condition=condition)


def _collect_joins(plan, found):
    """Collect every Join node in a plan tree."""
    if isinstance(plan, Join):
        found.append(plan)
    for child in plan.children():
        _collect_joins(child, found)


def test_q09_shape_no_conditionless_join_remains():
    """The FROM order starts with part x supplier (no shared predicate). The
    rule must reorder so every emitted join carries a condition."""
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    result = rule.apply(_q09_from_order_tree())
    assert result is not None
    joins = []
    _collect_joins(result, joins)
    assert len(joins) == 2
    for join in joins:
        assert join.condition is not None


def test_q09_shape_annotates_estimates():
    """Every emitted join carries its estimated rows; with fully seeded
    statistics no defaults are recorded."""
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    result = rule.apply(_q09_from_order_tree())
    joins = []
    _collect_joins(result, joins)
    for join in joins:
        assert join.estimated_rows is not None
        assert join.estimated_rows > 0
        assert not join.estimate_defaults


def test_rule_is_idempotent():
    """Re-applying the rule to its own output changes nothing."""
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    first = rule.apply(_q09_from_order_tree())
    assert first is not None
    assert rule.apply(first) is None


def test_left_join_root_untouched():
    """A LEFT join is not a reorderable region; the rule must not change it."""
    left = Join(
        left=_scan("part", "p", ["p_partkey"]),
        right=_scan("lineitem", "l", ["l_partkey", "l_suppkey"]),
        join_type=JoinType.LEFT,
        condition=_eq(_col("p", "p_partkey"), _col("l", "l_partkey")),
    )
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    assert rule.apply(left) is None


def test_nested_region_below_projection_reorders():
    """A region under a pass-through parent (Projection) still reorders."""
    tree = Projection(
        input=_q09_from_order_tree(),
        expressions=[_col("l", "l_partkey")],
        aliases=["l_partkey"],
    )
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    result = rule.apply(tree)
    assert result is not None
    joins = []
    _collect_joins(result, joins)
    for join in joins:
        assert join.condition is not None


def test_two_atom_join_with_condition_untouched():
    """A connected 2-atom join has nothing to reorder (build-side choice is
    the physical planner's job); the rule leaves it alone."""
    join = Join(
        left=_scan("part", "p", ["p_partkey"]),
        right=_scan("lineitem", "l", ["l_partkey", "l_suppkey"]),
        join_type=JoinType.INNER,
        condition=_eq(_col("p", "p_partkey"), _col("l", "l_partkey")),
    )
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    assert rule.apply(join) is None


def test_two_atom_cross_with_filter_becomes_inner():
    """A 2-atom CROSS whose connecting equality sits in a Filter above is
    worth rewriting: the equality moves into the join condition."""
    cross = Join(
        left=_scan("part", "p", ["p_partkey"]),
        right=_scan("lineitem", "l", ["l_partkey", "l_suppkey"]),
        join_type=JoinType.CROSS,
        condition=None,
    )
    tree = Filter(
        input=cross,
        predicate=_eq(_col("p", "p_partkey"), _col("l", "l_partkey")),
    )
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    result = rule.apply(tree)
    assert result is not None
    joins = []
    _collect_joins(result, joins)
    assert len(joins) == 1
    assert joins[0].join_type == JoinType.INNER
    assert joins[0].condition is not None


def test_local_filter_folds_into_scan():
    """A single-atom conjunct absorbed from a Filter is folded straight into
    its scan's pushed-down filters (PredicatePushdown's end state)."""
    join = Join(
        left=_scan("part", "p", ["p_partkey"]),
        right=_scan("lineitem", "l", ["l_partkey", "l_suppkey"]),
        join_type=JoinType.CROSS,
        condition=None,
    )
    local = BinaryOp(
        op=BinaryOpType.GT, left=_col("p", "p_partkey"),
        right=Literal(value=10, data_type=DataType.INTEGER),
    )
    connect = _eq(_col("p", "p_partkey"), _col("l", "l_partkey"))
    tree = Filter(
        input=join,
        predicate=BinaryOp(op=BinaryOpType.AND, left=local, right=connect),
    )
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    result = rule.apply(tree)
    assert result is not None
    scans = _scans_by_table(result)
    assert scans["part"].filters is not None


def _scans_by_table(plan):
    """Every scan in a tree keyed by its table name."""
    found = {}
    _collect_scans(plan, found)
    return found


def _collect_scans(plan, found):
    """Walk the tree collecting scans."""
    if isinstance(plan, Scan):
        found[plan.table_name] = plan
    for child in plan.children():
        _collect_scans(child, found)


def test_placement_guard_raises_on_dropped_conjunct():
    """The conservation guard must raise when placements do not cover every
    region conjunct exactly once."""
    rule = JoinOrderingRule(_q09_like_model(), max_join_reorder_size=10)
    with pytest.raises(JoinOrderError):
        rule._verify_placement(3, [0, 1])
    with pytest.raises(JoinOrderError):
        rule._verify_placement(2, [0, 0, 1])
