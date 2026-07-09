"""Tests for the canonical subplan signature: same STRUCTURE (modulo aliases and
constants) hashes equal; a real structural difference hashes differently."""

from federated_query.optimizer.subplan_signature import subplan_signature
from federated_query.plan.logical import Scan, Filter, Join, JoinType
from federated_query.plan.expressions import (
    ColumnRef, Literal, BinaryOp, BinaryOpType, DataType,
)


def _col(table, column):
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _build(
    fact_alias="ss", dim_alias="d", fact_table="store_sales",
    join_col="d_date_sk", filter_op=BinaryOpType.EQ, filter_const=1998,
):
    """A fact JOIN dim ON fact.k = dim.<join_col>, dim filtered by <op> <const>.
    Every knob is a structural or cosmetic axis a test varies."""
    fact = Scan(
        datasource="duck", schema_name="main", table_name=fact_table,
        columns=["ss_sold_date_sk"], alias=fact_alias,
    )
    dim = Scan(
        datasource="pg", schema_name="public", table_name="date_dim",
        columns=["d_date_sk", "d_year"], alias=dim_alias,
        filters=BinaryOp(
            op=filter_op, left=_col(dim_alias, "d_year"),
            right=Literal(value=filter_const, data_type=DataType.INTEGER),
        ),
    )
    condition = BinaryOp(
        op=BinaryOpType.EQ, left=_col(fact_alias, "ss_sold_date_sk"),
        right=_col(dim_alias, join_col),
    )
    return Join(left=fact, right=dim, join_type=JoinType.INNER, condition=condition)


def test_alias_neutral():
    """Different aliases, same tables/joins/filters -> same signature."""
    assert subplan_signature(_build(fact_alias="ss", dim_alias="d")) == \
        subplan_signature(_build(fact_alias="f1", dim_alias="dd"))


def test_constant_neutral():
    """Different filter constant, same shape -> same signature (the exploratory
    pattern: query N's measurement warms N+1)."""
    assert subplan_signature(_build(filter_const=1998)) == \
        subplan_signature(_build(filter_const=2001))


def test_different_table_differs():
    """A different fact table -> different signature."""
    assert subplan_signature(_build(fact_table="store_sales")) != \
        subplan_signature(_build(fact_table="catalog_sales"))


def test_different_join_key_differs():
    """Joining on a different dim column -> different signature."""
    assert subplan_signature(_build(join_col="d_date_sk")) != \
        subplan_signature(_build(join_col="d_month_seq"))


def test_different_filter_operator_differs():
    """A different filter operator (= vs >) -> different signature (op is part of
    the shape; only the constant is dropped)."""
    assert subplan_signature(_build(filter_op=BinaryOpType.EQ)) != \
        subplan_signature(_build(filter_op=BinaryOpType.GT))


def test_filter_presence_differs():
    """A filtered subplan differs from the same join with no filter."""
    filtered = _build()
    unfiltered_dim = Scan(
        datasource="pg", schema_name="public", table_name="date_dim",
        columns=["d_date_sk", "d_year"], alias="d",
    )
    unfiltered = filtered.model_copy(update={"right": unfiltered_dim})
    assert subplan_signature(filtered) != subplan_signature(unfiltered)


def test_stable_across_calls():
    """The signature is deterministic - the same subplan hashes the same twice."""
    plan = _build()
    assert subplan_signature(plan) == subplan_signature(plan)
