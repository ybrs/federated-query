"""Tests for PhysicalRemoteJoin query construction."""

from typing import List

from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    Literal,
)
from federated_query.plan.logical import JoinType
from federated_query.plan.physical import PhysicalRemoteJoin, PhysicalScan


def _make_filter(column: str, value, data_type: DataType) -> BinaryOp:
    """Create a simple equality filter expression for a column."""
    return BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table=None, column=column),
        right=Literal(value=value, data_type=data_type),
    )


def _make_scan(
    table_name: str,
    columns: List[str],
    filter_expr: BinaryOp,
    alias: str,
) -> PhysicalScan:
    """Build a PhysicalScan with the provided filter and alias."""
    return PhysicalScan(
        datasource="default",
        schema_name="public",
        table_name=table_name,
        columns=columns,
        filters=filter_expr,
        alias=alias,
    )


def _make_join_condition() -> BinaryOp:
    """Create equality join predicate between left and right id columns."""
    left_col = ColumnRef(table="l", column="id")
    right_col = ColumnRef(table="r", column="id")
    return BinaryOp(op=BinaryOpType.EQ, left=left_col, right=right_col)


def test_remote_join_keeps_side_filters_before_join():
    """Ensure side filters stay on their sources for outer join correctness."""
    left_filter = _make_filter("region", "west", DataType.VARCHAR)
    right_filter = _make_filter("active", True, DataType.BOOLEAN)
    left_scan = _make_scan("left_table", ["id", "region"], left_filter, "l")
    right_scan = _make_scan("right_table", ["id", "active"], right_filter, "r")
    remote_join = PhysicalRemoteJoin(
        left=left_scan,
        right=right_scan,
        join_type=JoinType.LEFT,
        condition=_make_join_condition(),
        datasource_connection=None,
    )

    query = remote_join._build_query()

    assert query.count(" WHERE ") == 2
    parts_after_on = query.split(" ON ", 1)[1]
    assert " WHERE " not in parts_after_on
    assert (
        '(SELECT * FROM "public"."right_table" WHERE (active = True)) AS r'
        in query
    )
