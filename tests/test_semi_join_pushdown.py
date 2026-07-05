"""Tests for SemiJoinPushdownRule.

A SEMI/ANTI join whose condition references only ONE side of the INNER join
beneath it commutes below that inner join. This lets a highly selective
existential filter (q18: 1.5M orders -> 57 via a HAVING subquery) apply
BEFORE an expensive fact join instead of at the very top. The rule fires
only when provably safe: the outer join is SEMI/ANTI, the inner is a plain
INNER (not natural/using/cross/outer), and the semi condition's non-subquery
columns all live on one inner side.
"""

from federated_query.optimizer.rules import SemiJoinPushdownRule
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
)
from federated_query.plan.logical import (
    Join,
    JoinType,
    Scan,
    SubqueryScan,
    Projection,
)


def _scan(table, alias, columns):
    """A qualified scan."""
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


def _subq():
    """A decorrelated subquery relation exposing one key column."""
    return SubqueryScan(
        input=Projection(
            input=_scan("t", "t", ("k",)),
            expressions=[_col("t", "k")],
            aliases=["subk"],
        ),
        alias="__subq_0",
    )


def _semi_over_inner(join_type=JoinType.SEMI, semi_ref_table="orders",
                     inner_type=JoinType.INNER, natural=False):
    """SEMI/ANTI( INNER(customer, orders), subq ) - the q18 skeleton.

    The inner join is customer <-> orders on custkey; the semi condition
    references `semi_ref_table` (orders by default = the left inner side)."""
    customer = _scan("customer", "customer", ("c_custkey", "c_name"))
    orders = _scan("orders", "orders", ("o_orderkey", "o_custkey"))
    inner = Join(
        left=customer, right=orders, join_type=inner_type,
        condition=_eq(_col("customer", "c_custkey"), _col("orders", "o_custkey")),
        natural=natural,
    )
    subq = _subq()
    semi_cond = _eq(_col(semi_ref_table,
                         "o_orderkey" if semi_ref_table == "orders" else "c_custkey"),
                    _col("__subq_0", "subk"))
    return Join(left=inner, right=subq, join_type=join_type, condition=semi_cond)


def _find(plan, predicate):
    """First node in the tree satisfying predicate, or None."""
    if predicate(plan):
        return plan
    for child in plan.children():
        found = _find(child, predicate)
        if found is not None:
            return found
    return None


def _is_semi(node):
    """A SEMI or ANTI join node."""
    return isinstance(node, Join) and node.join_type in (JoinType.SEMI, JoinType.ANTI)


def test_semi_pushes_to_the_referenced_left_side():
    """The semi condition references orders (left inner side): the result is
    INNER(SEMI(customer<->orders... wait) - the semi wraps the orders side."""
    tree = _semi_over_inner(semi_ref_table="orders")
    result = SemiJoinPushdownRule().apply(tree)
    assert result is not None
    # Top node is now the INNER join, not the SEMI.
    assert result.join_type == JoinType.INNER
    # The SEMI now sits over the orders scan (the side its condition names).
    semi = _find(result, _is_semi)
    assert semi is not None
    orders_scan = _find(semi.left, lambda n: isinstance(n, Scan) and n.alias == "orders")
    assert orders_scan is not None


def test_anti_join_also_pushes():
    """ANTI commutes through INNER for the same reason SEMI does (the anti
    condition depends only on the one inner side)."""
    tree = _semi_over_inner(join_type=JoinType.ANTI, semi_ref_table="orders")
    result = SemiJoinPushdownRule().apply(tree)
    assert result is not None
    assert result.join_type == JoinType.INNER
    assert _find(result, _is_semi).join_type == JoinType.ANTI


def test_semi_referencing_both_inner_sides_is_not_pushed():
    """If the semi condition touches BOTH inner sides it needs the full join
    output and must NOT be pushed."""
    customer = _scan("customer", "customer", ("c_custkey", "c_name"))
    orders = _scan("orders", "orders", ("o_orderkey", "o_custkey"))
    inner = Join(
        left=customer, right=orders, join_type=JoinType.INNER,
        condition=_eq(_col("customer", "c_custkey"), _col("orders", "o_custkey")),
    )
    both = BinaryOp(
        op=BinaryOpType.AND,
        left=_eq(_col("orders", "o_orderkey"), _col("__subq_0", "subk")),
        right=_eq(_col("customer", "c_custkey"), _col("__subq_0", "subk")),
    )
    tree = Join(left=inner, right=_subq(), join_type=JoinType.SEMI, condition=both)
    result = SemiJoinPushdownRule().apply(tree)
    settled = result if result is not None else tree
    # Unchanged: the SEMI is still the top node.
    assert settled.join_type == JoinType.SEMI


def test_not_pushed_when_inner_is_an_outer_join():
    """Only a plain INNER join is a safe host; a LEFT join is left alone."""
    tree = _semi_over_inner(inner_type=JoinType.LEFT)
    result = SemiJoinPushdownRule().apply(tree)
    settled = result if result is not None else tree
    assert settled.join_type == JoinType.SEMI


def test_not_pushed_when_inner_is_natural():
    """A NATURAL inner join carries an implicit condition; do not disturb it."""
    tree = _semi_over_inner(natural=True)
    result = SemiJoinPushdownRule().apply(tree)
    settled = result if result is not None else tree
    assert settled.join_type == JoinType.SEMI


def test_plain_inner_join_untouched():
    """A tree with no SEMI/ANTI join is returned unchanged."""
    customer = _scan("customer", "customer", ("c_custkey",))
    orders = _scan("orders", "orders", ("o_custkey",))
    tree = Join(
        left=customer, right=orders, join_type=JoinType.INNER,
        condition=_eq(_col("customer", "c_custkey"), _col("orders", "o_custkey")),
    )
    result = SemiJoinPushdownRule().apply(tree)
    settled = result if result is not None else tree
    assert settled == tree


def test_pushes_all_the_way_to_the_referenced_relation():
    """Over a 3-table inner region, the semi (referencing orders only) ends up
    directly above the orders scan - customer and lineitem stay outside it."""
    customer = _scan("customer", "customer", ("c_custkey", "c_name"))
    orders = _scan("orders", "orders", ("o_orderkey", "o_custkey"))
    lineitem = _scan("lineitem", "lineitem", ("l_orderkey", "l_quantity"))
    cust_ord = Join(
        left=customer, right=orders, join_type=JoinType.INNER,
        condition=_eq(_col("customer", "c_custkey"), _col("orders", "o_custkey")),
    )
    inner = Join(
        left=cust_ord, right=lineitem, join_type=JoinType.INNER,
        condition=_eq(_col("orders", "o_orderkey"), _col("lineitem", "l_orderkey")),
    )
    tree = Join(
        left=inner, right=_subq(), join_type=JoinType.SEMI,
        condition=_eq(_col("orders", "o_orderkey"), _col("__subq_0", "subk")),
    )
    rule = SemiJoinPushdownRule()
    result = rule.apply(tree)
    # Re-apply to fixpoint.
    while True:
        again = rule.apply(result)
        if again is None or again == result:
            break
        result = again
    semi = _find(result, _is_semi)
    # The semi's left subtree contains orders but neither customer nor lineitem.
    assert _find(semi.left, lambda n: isinstance(n, Scan) and n.alias == "orders")
    assert _find(semi.left, lambda n: isinstance(n, Scan) and n.alias == "customer") is None
    assert _find(semi.left, lambda n: isinstance(n, Scan) and n.alias == "lineitem") is None
