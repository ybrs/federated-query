"""Unit tests for single-sided join-CONDITION pushdown (PredicatePushdown).

A LEFT/RIGHT/SEMI/ANTI join condition conjunct that references only the
NON-PRESERVED side moves into that input (matching is unchanged for every
preserved row; a non-matching inner row never appears on its own). The
dangerous mistakes - moving a preserved-side conjunct (drops null-extended
rows), touching a FULL join, or emptying the condition - must be refused.
"""

from federated_query.optimizer.rules import PredicatePushdownRule
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    Literal,
    combine_and,
)
from federated_query.plan.logical import Filter, Join, JoinType, Scan


def _scan(table, alias, columns):
    """A bound scan with qualified columns."""
    return Scan.create(
        datasource="duck", schema_name="main", table_name=table,
        columns=list(columns), alias=alias,
    )


def _col(table, column):
    """A qualified column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _eq(left, right):
    """A column-to-column equality conjunct."""
    return BinaryOp(op=BinaryOpType.EQ, left=left, right=right)


def _lit_filter(table, column, value):
    """A single-sided column-to-literal conjunct."""
    return BinaryOp(
        op=BinaryOpType.GT,
        left=_col(table, column),
        right=Literal(value=value, data_type=DataType.INTEGER),
    )


def _join(join_type, condition):
    """A customer-orders join of the given type over the given condition."""
    return Join.create(
        left=_scan("customer", "c", ["c_custkey"]),
        right=_scan("orders", "o", ["o_custkey", "o_total"]),
        join_type=join_type,
        condition=condition,
    )


def _pushed(join):
    """The join after one PredicatePushdown pass."""
    return PredicatePushdownRule().apply(join)


def test_left_join_moves_nullable_side_conjunct():
    """q13's shape: the right-only conjunct becomes a filter under the right
    input and the condition keeps only the equi part."""
    condition = combine_and([
        _eq(_col("c", "c_custkey"), _col("o", "o_custkey")),
        _lit_filter("o", "o_total", 100),
    ])
    result = _pushed(_join(JoinType.LEFT, condition))
    # The moved conjunct lands in the right side's scan (ordinary filter
    # pushdown carries it the rest of the way down in the same pass).
    assert result.right.filters == _lit_filter("o", "o_total", 100)
    assert result.condition == _eq(_col("c", "c_custkey"), _col("o", "o_custkey"))


def test_semi_and_anti_joins_move_inner_side_conjuncts():
    """SEMI/ANTI existence tests against a filtered inner side are identical."""
    for join_type in (JoinType.SEMI, JoinType.ANTI):
        condition = combine_and([
            _eq(_col("c", "c_custkey"), _col("o", "o_custkey")),
            _lit_filter("o", "o_total", 7),
        ])
        result = _pushed(_join(join_type, condition))
        assert result.right.filters == _lit_filter("o", "o_total", 7), join_type
        assert result.condition == _eq(_col("c", "c_custkey"), _col("o", "o_custkey"))


def test_preserved_side_conjunct_stays_in_the_condition():
    """A LEFT join conjunct on the PRESERVED left must not move: left rows
    failing it still null-extend, a filter would drop them."""
    condition = combine_and([
        _eq(_col("c", "c_custkey"), _col("o", "o_custkey")),
        _lit_filter("c", "c_custkey", 5),
    ])
    result = _pushed(_join(JoinType.LEFT, condition))
    assert result.left.filters is None
    assert result.condition == condition


def test_full_join_condition_is_never_touched():
    """FULL preserves both sides; filtering either loses null-extended rows."""
    condition = combine_and([
        _eq(_col("c", "c_custkey"), _col("o", "o_custkey")),
        _lit_filter("o", "o_total", 3),
    ])
    result = _pushed(_join(JoinType.FULL, condition))
    assert result.right.filters is None
    assert result.condition == condition


def test_condition_is_never_emptied():
    """A condition that is entirely single-sided stays put: the join must keep
    at least one conjunct (an ON TRUE outer join is a different plan shape)."""
    condition = _lit_filter("o", "o_total", 9)
    result = _pushed(_join(JoinType.LEFT, condition))
    assert result.right.filters is None
    assert result.condition == condition
