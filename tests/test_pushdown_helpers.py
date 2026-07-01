"""Tests for the shared pushdown column helpers (one available-columns walker).

available_columns is the single answer both predicate and order-by pushdown use
to decide which side of a join a reference belongs to. It must cover every
relational node (Join included) and decide side-membership without being fooled
by an ambiguous bare name.
"""

from federated_query.optimizer import pushdown
from federated_query.plan.logical import Scan, Join, JoinType


def _scan(table, columns):
    """A base scan of one table with the given columns."""
    return Scan(
        datasource="ds", schema_name="public", table_name=table, columns=columns
    )


def test_available_columns_scan_has_bare_and_qualified():
    """A scan exposes each column both bare and table-qualified."""
    cols = pushdown.available_columns(_scan("orders", ["id", "amount"]))
    assert cols == {"id", "amount", "orders.id", "orders.amount"}


def test_available_columns_unions_both_join_sides():
    """A join exposes both sides - including a nested join (the B2 regression).

    The order-by walker previously returned {} for a Join child, so on 3+-way
    joins a sort key on a nested side was treated as unavailable and mis-pushed.
    """
    nested = Join(
        left=_scan("a", ["id", "x"]),
        right=_scan("b", ["bid", "y"]),
        join_type=JoinType.INNER,
        condition=None,
    )
    three_way = Join(
        left=nested,
        right=_scan("c", ["cid", "z"]),
        join_type=JoinType.INNER,
        condition=None,
    )
    cols = pushdown.available_columns(three_way)
    assert "a.x" in cols
    assert "b.y" in cols
    assert "c.z" in cols


def test_qualified_column_belongs_to_its_side():
    """A column qualified to one side belongs there, even on a shared bare name."""
    left = pushdown.available_columns(_scan("orders", ["id", "amount"]))
    right = pushdown.available_columns(_scan("customers", ["id", "name"]))
    assert pushdown.columns_belong_to_side({"orders.amount"}, left, right)
    # 'id' exists on both -> qualifying to one side is still unambiguous
    assert pushdown.columns_belong_to_side({"orders.id"}, left, right)
    assert not pushdown.columns_belong_to_side({"orders.id"}, right, left)


def test_bare_ambiguous_column_belongs_to_neither_side():
    """A bare name present on both sides is ambiguous and pushes to neither."""
    left = pushdown.available_columns(_scan("orders", ["id", "amount"]))
    right = pushdown.available_columns(_scan("customers", ["id", "name"]))
    assert not pushdown.columns_belong_to_side({"id"}, left, right)
    assert not pushdown.columns_belong_to_side({"id"}, right, left)
    # a bare name unique to one side still belongs there
    assert pushdown.columns_belong_to_side({"amount"}, left, right)
