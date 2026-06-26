"""Guard that every node's hand-written `with_children` preserves all fields.

Plan nodes are Pydantic models, so equality and `model_copy` cover every field by
construction -- we do not test those (that would be testing the library). What is
ours, and can be wrong, is each node's `with_children`: it is written by hand as
`model_copy(update={...})`, and a node could update the wrong field or, for a
multi-child node, forget one. This test pins that contract: rebuilding a node
with its own children must return an equal node. A new node type with children
must be added to NODES, or the coverage test below points it out.
"""

import pytest

from federated_query.plan.expressions import ColumnRef
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


# Each node is built with NON-DEFAULT values in every optional field, so a
# with_children that fails to carry one would revert it to its default and break
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


def test_every_childed_node_type_is_covered():
    """Fail if a logical node with children is missing from NODES above."""
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
        if child_fields & set(obj.model_fields.keys()) and obj not in covered:
            missing.append(name)
    assert not missing, f"Node types with children not covered by NODES: {missing}"
