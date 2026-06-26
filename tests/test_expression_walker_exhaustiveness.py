"""Lint: expression-tree walkers must descend into every compound node type.

A recursive expression walker that handles some node types but silently returns
nothing for others (the historical bug: Cast / Extract / CaseExpr / WindowExpr
were skipped) drops the columns and correlations nested inside them - a silent
wrong result, with no error.

This test plants a uniquely-named sentinel ColumnRef in EVERY expression-bearing
slot of one instance of EVERY compound expression type, then asserts each
recursive column collector finds all of them. If a walker fails to descend into
a node type (or into one of its child slots), the sentinel for that slot is
missing and the test fails, naming the gap.

It is also future-proofing: every Expression subclass must be classified here as
leaf / subquery / compound, and every compound type must have a sentinel builder.
A new Expression subclass therefore breaks this test until it is classified and
(if compound) added to the builders - forcing the author to make the walkers
cover it instead of silently skipping it.
"""

import pytest

from federated_query.plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    DataType,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    FunctionCall,
    Cast,
    Extract,
    CaseExpr,
    BetweenExpression,
    InList,
    TupleExpression,
    WindowExpr,
    Interval,
    ExistsExpression,
    InSubquery,
    SubqueryExpression,
    QuantifiedComparison,
)
from federated_query.optimizer.decorrelation import _expression_column_refs
from federated_query.optimizer.rules import (
    ProjectionPushdownRule,
    PredicatePushdownRule,
)


# Leaf nodes carry no child expressions, so a walker correctly returns nothing
# for them.
LEAF_EXPRESSIONS = frozenset({ColumnRef, Literal, Interval})

# Subquery nodes carry a nested plan with its own scope; the generic
# expression walkers intentionally do NOT descend into them (correlation is
# analysed through a dedicated path). This is a documented boundary, not a gap.
SUBQUERY_EXPRESSIONS = frozenset(
    {ExistsExpression, InSubquery, SubqueryExpression, QuantifiedComparison}
)

# Compound nodes carry child expressions a walker MUST descend into.
COMPOUND_EXPRESSIONS = frozenset(
    {
        BinaryOp,
        UnaryOp,
        FunctionCall,
        Cast,
        Extract,
        CaseExpr,
        BetweenExpression,
        InList,
        TupleExpression,
        WindowExpr,
    }
)


def _all_expression_subclasses() -> set:
    """Every concrete Expression subclass, found recursively."""
    found = set()
    pending = list(Expression.__subclasses__())
    while pending:
        cls = pending.pop()
        found.add(cls)
        pending.extend(cls.__subclasses__())
    return found


def _col(name: str) -> ColumnRef:
    """A sentinel column reference whose name a walker must surface."""
    return ColumnRef(table=None, column=name)


def _lit() -> Literal:
    """A constant used to fill non-sentinel slots."""
    return Literal(value=1, data_type=DataType.INTEGER)


def _compound_instances() -> dict:
    """One instance per compound type with a distinct sentinel in each child slot.

    Returns {type: (instance, frozenset_of_planted_sentinel_names)}.
    """
    instances = {
        BinaryOp: BinaryOp(
            op=BinaryOpType.EQ, left=_col("s_binop_left"), right=_col("s_binop_right")
        ),
        UnaryOp: UnaryOp(op=UnaryOpType.NOT, operand=_col("s_unop_operand")),
        FunctionCall: FunctionCall(
            function_name="f",
            args=[_col("s_fn_arg0"), _col("s_fn_arg1")],
            within_group_key=_col("s_fn_within_group"),
        ),
        Cast: Cast(expr=_col("s_cast_expr"), target_type="INT"),
        Extract: Extract(field="YEAR", source=_col("s_extract_source")),
        CaseExpr: CaseExpr(
            when_clauses=[(_col("s_case_cond"), _col("s_case_result"))],
            else_result=_col("s_case_else"),
        ),
        BetweenExpression: BetweenExpression(
            value=_col("s_btw_value"),
            lower=_col("s_btw_lower"),
            upper=_col("s_btw_upper"),
        ),
        InList: InList(value=_col("s_in_value"), options=[_col("s_in_option")]),
        TupleExpression: TupleExpression(
            items=(_col("s_tuple0"), _col("s_tuple1"))
        ),
        WindowExpr: WindowExpr(
            function=_col("s_win_function"),
            partition_by=[_col("s_win_partition")],
            order_keys=[_col("s_win_order")],
            order_ascending=[True],
            order_nulls=[None],
            frame=None,
        ),
    }
    planted = {
        cls: frozenset(_sentinels_of(instance))
        for cls, instance in instances.items()
    }
    return {cls: (instances[cls], planted[cls]) for cls in instances}


def _sentinels_of(expr: Expression) -> set:
    """Every sentinel column name reachable in an expression, by direct walk."""
    names = set()
    if isinstance(expr, ColumnRef) and expr.column.startswith("s_"):
        names.add(expr.column)
    for child in _direct_children(expr):
        names |= _sentinels_of(child)
    return names


def _direct_children(expr: Expression) -> list:
    """Child expressions used only to compute the expected sentinel set."""
    if isinstance(expr, BinaryOp):
        return [expr.left, expr.right]
    if isinstance(expr, UnaryOp):
        return [expr.operand]
    if isinstance(expr, FunctionCall):
        return list(expr.args) + [expr.within_group_key]
    if isinstance(expr, Cast):
        return [expr.expr]
    if isinstance(expr, Extract):
        return [expr.source]
    if isinstance(expr, CaseExpr):
        children = []
        for condition, result in expr.when_clauses:
            children.extend([condition, result])
        if expr.else_result is not None:
            children.append(expr.else_result)
        return children
    if isinstance(expr, BetweenExpression):
        return [expr.value, expr.lower, expr.upper]
    if isinstance(expr, InList):
        return [expr.value] + list(expr.options)
    if isinstance(expr, TupleExpression):
        return list(expr.items)
    if isinstance(expr, WindowExpr):
        return [expr.function] + list(expr.partition_by) + list(expr.order_keys)
    return []


# (name, callable returning an iterable of column-name strings). These are the
# recursive collectors that previously silently dropped nested columns.
_WALKERS = [
    (
        "decorrelation._expression_column_refs",
        lambda expr: [ref.column for ref in _expression_column_refs(expr)],
    ),
    (
        "ProjectionPushdownRule._extract_columns",
        lambda expr: ProjectionPushdownRule()._extract_columns(expr),
    ),
    (
        "PredicatePushdownRule._extract_column_refs",
        lambda expr: PredicatePushdownRule()._extract_column_refs(expr),
    ),
]

_INSTANCES = _compound_instances()


def test_expression_classification_covers_every_subclass():
    """Every Expression subclass is classified leaf / subquery / compound.

    Forces a new subclass to be triaged here (and, if compound, given a sentinel
    builder) so the walkers below are tested against it.
    """
    classified = LEAF_EXPRESSIONS | SUBQUERY_EXPRESSIONS | COMPOUND_EXPRESSIONS
    actual = _all_expression_subclasses()
    unclassified = actual - classified
    stale = classified - actual
    assert not unclassified, f"unclassified Expression subclasses: {unclassified}"
    assert not stale, f"classified types that no longer exist: {stale}"


def test_every_compound_type_has_a_sentinel_builder():
    """Each compound type must have an instance so it is exercised below."""
    missing = COMPOUND_EXPRESSIONS - set(_INSTANCES)
    assert not missing, f"compound types without a sentinel builder: {missing}"


@pytest.mark.parametrize("walker_name,walker", _WALKERS, ids=[w[0] for w in _WALKERS])
@pytest.mark.parametrize(
    "compound_type",
    sorted(COMPOUND_EXPRESSIONS, key=lambda c: c.__name__),
    ids=lambda c: c.__name__,
)
def test_walker_descends_into_every_compound_slot(
    compound_type, walker_name, walker
):
    """Each recursive collector must surface every nested column.

    A missing sentinel means the walker does not descend into that node type (or
    one of its child slots), which would silently drop the columns/correlations
    inside it.
    """
    instance, planted = _INSTANCES[compound_type]
    found = set(walker(instance))
    missing = planted - found
    assert not missing, (
        f"{walker_name} does not surface {sorted(missing)} inside "
        f"{compound_type.__name__}: it skips that node or child slot"
    )
