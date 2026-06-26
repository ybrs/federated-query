"""Guard against silent field drops in plan-node reconstruction.

Plan nodes are frozen dataclasses that get rebuilt at many sites. When a rebuild
re-lists fields by hand and misses one, that field silently reverts to its
default - a wrong answer with no error. This exact failure mode dropped
``Projection.distinct_on`` and ``FunctionCall.within_group_key`` until caught.

The structural fix is to reconstruct nodes with ``dataclasses.replace`` (or
``with_children``), which copies every field and changes only what is named.
This test pins the ``with_children`` contract: rebuilding a node with its own
children must return an equal node, so any field a ``with_children`` forgets to
carry fails here loudly. Every node type that has children is covered; adding a
new field with a hand-written ``with_children`` that drops it breaks this test.
"""

import dataclasses

import pytest

from federated_query.plan.expressions import ColumnRef, DataType, FunctionCall, Literal
from federated_query.plan.logical import (
    Aggregate,
    CTE,
    Explain,
    ExplainFormat,
    Filter,
    GroupedLimit,
    Join,
    JoinType,
    LateralJoin,
    Limit,
    Projection,
    Scan,
    SetOperation,
    SetOpKind,
    SingleRowGuard,
    Sort,
    SubqueryScan,
    Union,
)

COL = ColumnRef(table=None, column="c")


def _leaf(name="leaf"):
    """A minimal leaf plan node to use as a child."""
    return Scan(datasource="d", schema_name="s", table_name=name, columns=["c"])


# Every node built with NON-DEFAULT values in each optional/flag field, so a
# field that with_children fails to carry would revert to its default and break
# the round-trip equality below.
NODES = [
    Projection(_leaf(), [COL], ["c"], distinct=True, distinct_on=[COL]),
    Filter(_leaf(), COL),
    Sort(_leaf(), [COL], [False], ["LAST"]),
    Limit(_leaf(), 5, 2),
    Aggregate(_leaf(), [COL], [COL], ["c"], grouping_sets=[[COL], []]),
    Join(_leaf("l"), _leaf("r"), JoinType.LEFT, COL, natural=True, using=["c"]),
    LateralJoin(_leaf("l"), _leaf("r"), JoinType.LEFT),
    SetOperation(_leaf("l"), _leaf("r"), SetOpKind.UNION, distinct=True),
    Union([_leaf("l"), _leaf("r")], distinct=True),
    CTE("w", _leaf("c"), _leaf("b"), recursive=True, column_names=["c"]),
    SubqueryScan(_leaf(), "a"),
    Explain(_leaf(), ExplainFormat.JSON),
    GroupedLimit(_leaf(), [COL], 3, [COL], [False], ["LAST"]),
    SingleRowGuard(_leaf(), [COL]),
]


@pytest.mark.parametrize("node", NODES, ids=lambda n: type(n).__name__)
def test_with_children_preserves_all_fields(node):
    """Rebuilding a node with its own children must change nothing else."""
    rebuilt = node.with_children(node.children())
    assert rebuilt == node


_NO_DEFAULT = object()


def _field_default(field_info):
    """Return a Pydantic field's default value, or _NO_DEFAULT if it is required."""
    from pydantic_core import PydanticUndefined

    if field_info.default is not PydanticUndefined:
        return field_info.default
    if field_info.default_factory is not None:
        return field_info.default_factory()
    return _NO_DEFAULT


@pytest.mark.parametrize("node", NODES, ids=lambda n: type(n).__name__)
def test_dropping_any_optional_field_is_detected(node):
    """Prove the round-trip guard has teeth: reverting any field to its default
    (exactly what a rebuild that forgets the field produces) must make the node
    compare unequal, so the equality check in the guard above cannot pass when a
    field is silently dropped.
    """
    for name, field_info in type(node).model_fields.items():
        default = _field_default(field_info)
        if default is _NO_DEFAULT:
            continue
        if getattr(node, name) == default:
            continue
        dropped = node.model_copy(update={name: default})
        assert (
            dropped != node
        ), f"a dropped {type(node).__name__}.{name} would NOT be detected"


def test_distinct_on_drop_is_detected():
    """Reproduce the exact historical bug: a hand-written Projection rebuild that
    omits distinct_on. The equality check the guard relies on must flag it, while
    the real with_children path must preserve it.
    """
    correct = Projection(_leaf(), [COL], ["c"], distinct=True, distinct_on=[COL])
    # The buggy reconstruction that returned wrong DISTINCT ON answers:
    buggy = Projection(_leaf(), correct.expressions, correct.aliases, correct.distinct)
    assert buggy != correct
    assert correct.with_children(correct.children()) == correct


def test_function_call_within_group_drop_is_detected():
    """Same proof for FunctionCall.within_group_key, the ordered-set field the
    binder/decorrelation rebuilds must carry (dropped it before the replace fix).
    """
    correct = FunctionCall(
        function_name="PERCENTILE_CONT",
        args=[Literal(value=0.5, data_type=DataType.DOUBLE)],
        is_aggregate=True,
        within_group_key=COL,
    )
    # A rebuild that re-lists fields but forgets within_group_key:
    buggy = FunctionCall(
        function_name=correct.function_name,
        args=correct.args,
        is_aggregate=correct.is_aggregate,
    )
    assert buggy != correct
    assert dataclasses.replace(correct, args=list(correct.args)) == correct


def test_every_childed_node_type_is_covered():
    """Fail if a new logical node with children lacks a case above.

    This keeps the guard exhaustive: a newly added node type with a child field
    must be added to NODES, or this test points it out.
    """
    from federated_query.plan import logical
    from federated_query.plan.logical import LogicalPlanNode

    covered = {type(node) for node in NODES}
    child_fields = {"input", "left", "right", "inputs", "child", "cte_plan"}
    missing = []
    for name in dir(logical):
        obj = getattr(logical, name)
        if not isinstance(obj, type) or not issubclass(obj, LogicalPlanNode):
            continue
        if obj is LogicalPlanNode:
            continue
        field_names = set(obj.model_fields.keys())
        if child_fields & field_names and obj not in covered:
            missing.append(name)
    assert not missing, f"Node types with children not covered by NODES: {missing}"
