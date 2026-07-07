"""Unit tests for the CTE union-filter pushdown rule.

These pin the safety decisions of CTEUnionFilterPushdownRule: the OR of
every consumer's filter may enter the shared body ONLY translated onto
grouping (or constant) output columns, only when every consumer is
filtered, and idempotently - the dangerous mistakes (starving a bare
consumer, pushing an aggregate-output comparison, double-inserting) must
be refused.
"""

from federated_query.optimizer.cte_union_filter import CTEUnionFilterPushdownRule
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    FunctionCall,
    Literal,
    split_conjuncts,
    split_disjuncts,
)
from federated_query.plan.logical import (
    CTE,
    CTERef,
    Aggregate,
    Filter,
    Scan,
    Union,
)


def _scan():
    """The CTE body's base scan."""
    return Scan(
        datasource="duck",
        schema_name="main",
        table_name="sales",
        columns=["d_year", "county", "price"],
        alias="s",
    )


def _col(table, column):
    """A qualified integer column reference."""
    return ColumnRef(table=table, column=column, data_type=DataType.INTEGER)


def _eq(ref, value):
    """An equality comparison against an integer literal."""
    return BinaryOp(
        op=BinaryOpType.EQ,
        left=ref,
        right=Literal(value=value, data_type=DataType.INTEGER),
    )


def _body(channel=None):
    """A body aggregate: group by (d_year, county), one SUM output. With a
    channel value, a constant tag column is added (the q04 union shape)."""
    select = [_col("s", "d_year"), _col("s", "county")]
    names = ["d_year", "county"]
    if channel is not None:
        select.append(Literal(value=channel, data_type=DataType.VARCHAR))
        names.append("channel")
    select.append(
        FunctionCall(function_name="SUM", args=[_col("s", "price")], is_aggregate=True)
    )
    names.append("total")
    return Aggregate(
        input=_scan(),
        group_by=[_col("s", "d_year"), _col("s", "county")],
        aggregates=select,
        output_names=names,
    )


def _consumer(alias, year, outputs):
    """One consumer: Filter(alias.d_year = year AND alias.total > 0, ref)."""
    ref = CTERef(name="t", alias=alias, output_names=outputs)
    total_guard = BinaryOp(
        op=BinaryOpType.GT,
        left=_col(alias, "total"),
        right=Literal(value=0, data_type=DataType.INTEGER),
    )
    predicate = BinaryOp(
        op=BinaryOpType.AND, left=_eq(_col(alias, "d_year"), year), right=total_guard
    )
    return Filter(input=ref, predicate=predicate)


def _walk(node):
    """Yield a plan node and all of its descendants."""
    yield node
    for child in node.children():
        yield from _walk(child)


def _pushed_filter(plan):
    """The filter inserted under the body aggregate, or None."""
    for node in _walk(plan):
        if isinstance(node, Aggregate) and isinstance(node.input, Filter):
            return node.input
    return None


def test_pushes_or_of_all_consumer_filters_under_the_aggregate():
    """Two filtered consumers: the body gains OR(d_year=2000, d_year=2001)
    under the aggregate; the untranslatable total > 0 conjunct (an aggregate
    output) is dropped from both arms; consumer filters stay in place."""
    outputs = ["d_year", "county", "total"]
    child = Union(
        inputs=[_consumer("c1", 2000, outputs), _consumer("c2", 2001, outputs)],
        distinct=False,
    )
    cte = CTE(name="t", cte_plan=_body(), child=child)
    result = CTEUnionFilterPushdownRule().apply(cte)
    pushed = _pushed_filter(result)
    assert pushed is not None, "the union filter must land under the aggregate"
    arms = split_disjuncts(pushed.predicate)
    assert len(arms) == 2
    for arm in arms:
        for conjunct in split_conjuncts(arm):
            assert "total" not in repr(conjunct)
    assert repr(_col("s", "d_year")) in repr(pushed.predicate)
    consumer_filters = 0
    for node in _walk(result.child):
        if isinstance(node, Filter):
            consumer_filters += 1
    assert consumer_filters == 2, "consumer filters must stay in place"


def test_declines_when_any_consumer_is_unfiltered():
    """A bare CTERef consumer needs every row: nothing may be pushed."""
    outputs = ["d_year", "county", "total"]
    bare = CTERef(name="t", alias="c2", output_names=outputs)
    child = Union(inputs=[_consumer("c1", 2000, outputs), bare], distinct=False)
    cte = CTE(name="t", cte_plan=_body(), child=child)
    result = CTEUnionFilterPushdownRule().apply(cte)
    assert _pushed_filter(result) is None


def test_declines_when_no_conjunct_translates():
    """A consumer filtering ONLY on an aggregate output contributes a TRUE
    arm, which would make the whole union a no-op: decline."""
    outputs = ["d_year", "county", "total"]
    ref = CTERef(name="t", alias="c1", output_names=outputs)
    only_total = BinaryOp(
        op=BinaryOpType.GT,
        left=_col("c1", "total"),
        right=Literal(value=0, data_type=DataType.INTEGER),
    )
    child = Filter(input=ref, predicate=only_total)
    cte = CTE(name="t", cte_plan=_body(), child=child)
    result = CTEUnionFilterPushdownRule().apply(cte)
    assert _pushed_filter(result) is None


def test_declines_a_recursive_cte():
    """WITH RECURSIVE bodies reference themselves: never touched."""
    outputs = ["d_year", "county", "total"]
    child = _consumer("c1", 2000, outputs)
    cte = CTE(name="t", cte_plan=_body(), child=child, recursive=True)
    result = CTEUnionFilterPushdownRule().apply(cte)
    assert _pushed_filter(result) is None


def test_union_body_pushes_per_branch_substituting_literal_tags():
    """A union body (q04's year_total): each branch gets the filter with its
    OWN constant channel tag substituted, so the engine folds the channel
    comparison per branch."""
    outputs = ["d_year", "county", "channel", "total"]
    ref = CTERef(name="t", alias="c1", output_names=outputs)
    predicate = BinaryOp(
        op=BinaryOpType.AND,
        left=_eq(_col("c1", "d_year"), 2000),
        right=BinaryOp(
            op=BinaryOpType.EQ,
            left=_col("c1", "channel"),
            right=Literal(value="s", data_type=DataType.VARCHAR),
        ),
    )
    child = Filter(input=ref, predicate=predicate)
    body = Union(inputs=[_body("s"), _body("w")], distinct=False)
    cte = CTE(name="t", cte_plan=body, child=child)
    result = CTEUnionFilterPushdownRule().apply(cte)
    pushed = []
    for node in _walk(result.cte_plan):
        if isinstance(node, Aggregate) and isinstance(node.input, Filter):
            pushed.append(node.input.predicate)
    assert len(pushed) == 2, "every union branch must receive the filter"
    assert pushed[0] != pushed[1], "each branch must get ITS OWN literal tag"
    reprs = repr(pushed[0]) + repr(pushed[1])
    assert "Literal(w)" in reprs, "the branch literal must be substituted"
    for predicate in pushed:
        assert "channel" not in repr(predicate), "no unresolved output name"


def test_apply_is_idempotent():
    """Re-applying the rule to an already-pushed plan changes nothing (the
    optimizer iterates to a fixed point)."""
    outputs = ["d_year", "county", "total"]
    child = Union(
        inputs=[_consumer("c1", 2000, outputs), _consumer("c2", 2001, outputs)],
        distinct=False,
    )
    cte = CTE(name="t", cte_plan=_body(), child=child)
    rule = CTEUnionFilterPushdownRule()
    once = rule.apply(cte)
    twice = rule.apply(once)
    assert once == twice
