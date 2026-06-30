"""transform_children rebuilds a node only when a child actually changed.

The shared recurse-and-rebuild the pushdown rules use. It must preserve node
identity when nothing changed (the rules' change-detection depends on that) and
rebuild via with_children, preserving every other field, when a child changed.
"""

from federated_query.plan.logical import transform_children, Limit, Sort
from federated_query.plan.expressions import ColumnRef


def _sort_over_limit() -> Sort:
    """A Sort(input=Limit(...)) with sort keys to check field preservation."""
    inner = Limit(input=_leaf(), limit=10, offset=2)
    return Sort(
        input=inner,
        sort_keys=[ColumnRef(table=None, column="a")],
        ascending=[True],
        nulls_order=["LAST"],
    )


def _leaf() -> Limit:
    """A trivial leaf-ish node (a Limit with no further structure to recurse)."""
    return Limit(input=Sort(input=_base(), sort_keys=[], ascending=[]), limit=1)


def _base():
    """A Values node used as a concrete leaf input."""
    from federated_query.plan.logical import Values
    from federated_query.plan.expressions import Literal, DataType

    return Values(
        rows=[[Literal(value=1, data_type=DataType.INTEGER)]], output_names=["a"]
    )


def test_returns_same_node_when_no_child_changes():
    """An identity transform leaves the node itself unchanged (same object)."""
    node = _sort_over_limit()
    result = transform_children(node, lambda child: child)
    assert result is node


def test_rebuilds_and_preserves_other_fields_when_child_changes():
    """Changing a child rebuilds the node, keeping its own fields intact."""
    node = _sort_over_limit()
    replacement = Limit(input=node.input.input, limit=99, offset=0)
    result = transform_children(node, lambda child: replacement)
    assert result is not node
    assert result.input is replacement
    # The Sort's own fields survive the rebuild.
    assert result.sort_keys == node.sort_keys
    assert result.ascending == node.ascending
    assert result.nulls_order == node.nulls_order
