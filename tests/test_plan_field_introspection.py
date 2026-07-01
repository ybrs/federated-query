"""Structural guard: annotation-driven expression introspection over plan nodes.

LogicalPlanNode.direct_expressions() does not enumerate node types by hand; it
asks field_introspection which fields hold Expression values, decided from the
field type annotations. This test pins that the classifier is TOTAL over every
real plan-node field (so none is ever silently skipped) and that an
unclassifiable annotation (Any / ambiguous union) RAISES - the structural teeth
that make incompleteness loud instead of silent.

A new LogicalPlanNode subclass, or a new field on one, is covered automatically:
if its annotation is classifiable the introspection handles it, and if it is not
(e.g. typed Any) this test fails until it is typed concretely or handled.
"""

from typing import Any, List, Optional, Union

import pytest

from federated_query.plan.logical import LogicalPlanNode, Filter, Scan, Aggregate
from federated_query.plan.expressions import (
    Expression,
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    Literal,
    DataType,
)
from federated_query.plan.field_introspection import (
    FieldIntrospectionError,
    field_holds_expressions,
    innermost_type,
    model_expression_values,
)


def _all_plan_node_subclasses() -> set:
    """Every concrete LogicalPlanNode subclass, found recursively."""
    found = set()
    pending = list(LogicalPlanNode.__subclasses__())
    while pending:
        cls = pending.pop()
        found.add(cls)
        pending.extend(cls.__subclasses__())
    return found


def test_field_classifier_is_total_over_every_plan_node_field():
    """Every field of every plan node classifies as expr-or-not without raising.

    field_holds_expressions returns True (the field holds expressions) or False
    (a child plan or a scalar); it only raises on an annotation it cannot reason
    about. Asserting it never raises over all real fields proves the
    introspection - and thus direct_expressions() - covers every node totally.
    """
    for cls in _all_plan_node_subclasses():
        for field_name, info in cls.model_fields.items():
            # Must not raise; the return value is the classification.
            result = field_holds_expressions(info.annotation)
            assert isinstance(result, bool), f"{cls.__name__}.{field_name}"


def test_direct_expressions_collects_a_nodes_expressions():
    """direct_expressions() surfaces exactly the expressions a node carries."""
    predicate = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table="t", column="a"),
        right=Literal(value=1, data_type=DataType.INTEGER),
    )
    node = Filter(input=Scan(datasource="d", schema_name="s", table_name="t", columns=["a"]), predicate=predicate)
    assert node.direct_expressions() == [predicate]


def test_direct_expressions_flattens_optional_and_nested_lists():
    """Optional list and list-of-lists expression fields are flattened, None ok."""
    group_key = ColumnRef(table="t", column="g")
    agg_call = ColumnRef(table="t", column="a")
    grouping_member = ColumnRef(table="t", column="gs")
    node = Aggregate(
        input=Scan(datasource="d", schema_name="s", table_name="t", columns=["a"]),
        group_by=[group_key],
        aggregates=[agg_call],
        output_names=["g", "a"],
        grouping_sets=[[grouping_member], []],
    )
    collected = node.direct_expressions()
    assert group_key in collected
    assert agg_call in collected
    assert grouping_member in collected


def test_classifier_raises_on_any_typed_field():
    """An Any-typed field is ambiguous and must raise, not be assumed scalar."""
    with pytest.raises(FieldIntrospectionError):
        field_holds_expressions(Any)


def test_classifier_raises_on_ambiguous_union():
    """A union of two concrete types cannot be classified and must raise."""
    with pytest.raises(FieldIntrospectionError):
        field_holds_expressions(Union[int, ColumnRef])


def test_innermost_type_peels_optional_and_lists():
    """Optional/List wrappers peel to the single inner type."""
    assert innermost_type(Optional[List[Expression]]) is Expression
    assert innermost_type(List[List[Expression]]) is Expression
    assert innermost_type(Optional[str]) is str


def test_model_expression_values_matches_base_method():
    """The free function and the base method agree (one source of truth)."""
    node = Filter(
        input=Scan(datasource="d", schema_name="s", table_name="t", columns=["a"]),
        predicate=ColumnRef(table="t", column="a"),
    )
    assert model_expression_values(node) == node.direct_expressions()
