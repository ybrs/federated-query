"""Unit tests for the well-scoped-plan validator.

The validator re-checks, after a plan rewrite, that every qualified column
reference resolves to a relation in scope at the operator that uses it. It
catches the whole class of mis-scoped plans (item-1 being one instance), not a
single case.
"""

import pytest

from federated_query.plan.logical import (
    LogicalPlanNode,
    Scan,
    Filter,
    Join,
    JoinType,
    LateralJoin,
)
from federated_query.plan.expressions import (
    ColumnRef,
    BinaryOp,
    BinaryOpType,
)
from federated_query.optimizer.scope_validator import (
    ScopeError,
    find_scope_violations,
    validate_scope,
    _output_qualifiers,
)


def _scan(table, alias):
    """A scan of one table under an alias."""
    return Scan(datasource="d", schema_name="s", table_name=table, columns=["id"], alias=alias)


def _eq(left_tbl, left_col, right_tbl, right_col):
    """An equality between two qualified columns."""
    return BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table=left_tbl, column=left_col),
        right=ColumnRef(table=right_tbl, column=right_col),
    )


def test_valid_join_has_no_violations():
    """A join whose condition references only its two inputs is well-scoped."""
    plan = Join(
        left=_scan("a", "a"),
        right=_scan("b", "b"),
        join_type=JoinType.INNER,
        condition=_eq("a", "id", "b", "id"),
    )
    assert find_scope_violations(plan) == []


def test_reference_to_out_of_scope_qualifier_is_flagged():
    """A condition referencing a relation not among the join inputs is flagged."""
    plan = Join(
        left=_scan("a", "a"),
        right=_scan("b", "b"),
        join_type=JoinType.SEMI,
        condition=_eq("a", "id", "cities", "country"),
    )
    violations = find_scope_violations(plan)
    assert len(violations) == 1
    assert "cities.country" in violations[0]


def test_validate_scope_raises_with_stage_label():
    """validate_scope raises ScopeError, prefixing the stage that produced it."""
    plan = Filter(
        input=_scan("a", "a"),
        predicate=ColumnRef(table="ghost", column="x"),
    )
    with pytest.raises(ScopeError) as exc_info:
        validate_scope(plan, "after decorrelation")
    assert "after decorrelation" in str(exc_info.value)
    assert "ghost.x" in str(exc_info.value)


def test_lateral_right_may_reference_left():
    """A LATERAL join's right side correlating to its left is in scope, not flagged."""
    correlated_right = Filter(
        input=_scan("b", "b"),
        predicate=_eq("b", "id", "a", "id"),
    )
    plan = LateralJoin(
        left=_scan("a", "a"),
        right=correlated_right,
        join_type=JoinType.INNER,
    )
    assert find_scope_violations(plan) == []


def test_plain_join_right_may_not_reference_left():
    """A non-lateral join's right side correlating to its sibling is flagged."""
    correlated_right = Filter(
        input=_scan("b", "b"),
        predicate=_eq("b", "id", "a", "id"),
    )
    plan = Join(
        left=_scan("a", "a"),
        right=correlated_right,
        join_type=JoinType.INNER,
        condition=None,
    )
    violations = find_scope_violations(plan)
    assert len(violations) == 1
    assert "a.id" in violations[0]


def test_output_qualifiers_raises_on_unknown_node_type():
    """The validator's own walker raises on an unhandled plan node type."""

    class _BogusNode(LogicalPlanNode):
        """A stand-in node type the qualifier walker has no rule for."""

    with pytest.raises(ValueError):
        _output_qualifiers(_BogusNode())
