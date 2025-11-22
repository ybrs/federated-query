"""Tests for logical plan optimization rules."""

import pytest
from federated_query.plan.logical import (
    Scan,
    Filter,
    Projection,
    Join,
    JoinType,
    Aggregate,
)
from federated_query.plan.expressions import (
    Literal,
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    DataType,
    FunctionCall,
)
from federated_query.optimizer.rules import ExpressionSimplificationRule


class TestExpressionSimplificationRule:
    """Test expression simplification rule on logical plans."""

    def test_simplify_filter_predicate(self):
        """Test simplification of filter predicates."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        col = ColumnRef(None, "id", DataType.BIGINT)
        filter_expr = BinaryOp(
            op=BinaryOpType.AND,
            left=col,
            right=Literal(True, DataType.BOOLEAN)
        )

        filter_node = Filter(scan, filter_expr)

        result = rule.apply(filter_node)
        assert isinstance(result, Filter)
        assert result.predicate == col

    def test_simplify_filter_with_constant_folding(self):
        """Test constant folding in filter predicates."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        col = ColumnRef(None, "id", DataType.BIGINT)
        filter_expr = BinaryOp(
            op=BinaryOpType.GT,
            left=col,
            right=BinaryOp(
                op=BinaryOpType.ADD,
                left=Literal(10, DataType.BIGINT),
                right=Literal(5, DataType.BIGINT)
            )
        )

        filter_node = Filter(scan, filter_expr)

        result = rule.apply(filter_node)
        assert isinstance(result, Filter)
        assert isinstance(result.predicate, BinaryOp)
        assert result.predicate.op == BinaryOpType.GT
        assert isinstance(result.predicate.right, Literal)
        assert result.predicate.right.value == 15

    def test_simplify_project_expressions(self):
        """Test simplification of project expressions."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "age"]
        )

        col = ColumnRef(None, "age", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=col,
            right=Literal(0, DataType.BIGINT)
        )

        project = Projection(scan, [expr], ["age_plus_zero"])

        result = rule.apply(project)
        assert isinstance(result, Projection)
        assert result.expressions[0] == col

    def test_simplify_join_condition(self):
        """Test simplification of join conditions."""
        rule = ExpressionSimplificationRule()

        left_scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        right_scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="orders",
            columns=["user_id", "amount"]
        )

        left_col = ColumnRef("users", "id", DataType.BIGINT)
        right_col = ColumnRef("orders", "user_id", DataType.BIGINT)

        condition = BinaryOp(
            op=BinaryOpType.AND,
            left=BinaryOp(
                op=BinaryOpType.EQ,
                left=left_col,
                right=right_col
            ),
            right=Literal(True, DataType.BOOLEAN)
        )

        join = Join(left_scan, right_scan, JoinType.INNER, condition)

        result = rule.apply(join)
        assert isinstance(result, Join)
        assert isinstance(result.condition, BinaryOp)
        assert result.condition.op == BinaryOpType.EQ

    def test_simplify_aggregate_expressions(self):
        """Test simplification of aggregate expressions."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="sales",
            columns=["region", "amount"]
        )

        group_col = ColumnRef(None, "region", DataType.VARCHAR)

        agg_expr = FunctionCall("SUM", [
            BinaryOp(
                op=BinaryOpType.MULTIPLY,
                left=ColumnRef(None, "amount", DataType.BIGINT),
                right=Literal(1, DataType.BIGINT)
            )
        ], is_aggregate=True)

        aggregate = Aggregate(
            scan,
            [group_col],
            [agg_expr],
            ["region", "total"]
        )

        result = rule.apply(aggregate)
        assert isinstance(result, Aggregate)
        assert isinstance(result.aggregates[0], FunctionCall)
        agg_arg = result.aggregates[0].args[0]
        assert isinstance(agg_arg, ColumnRef)
        assert agg_arg.column == "amount"

    def test_simplify_scan_filters(self):
        """Test simplification of scan filters."""
        rule = ExpressionSimplificationRule()

        col = ColumnRef(None, "id", DataType.BIGINT)
        filter_expr = BinaryOp(
            op=BinaryOpType.OR,
            left=col,
            right=Literal(False, DataType.BOOLEAN)
        )

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "name"],
            filters=filter_expr
        )

        result = rule.apply(scan)
        assert isinstance(result, Scan)
        assert result.filters == col

    def test_no_change_when_no_simplification(self):
        """Test that rule returns original plan when no simplification possible."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "name"]
        )

        col = ColumnRef(None, "id", DataType.BIGINT)
        filter_node = Filter(scan, col)

        result = rule.apply(filter_node)
        assert result == filter_node

    def test_simplify_nested_plan(self):
        """Test simplification of nested logical plans."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "age"]
        )

        col = ColumnRef(None, "age", DataType.BIGINT)

        inner_filter = BinaryOp(
            op=BinaryOpType.GT,
            left=col,
            right=BinaryOp(
                op=BinaryOpType.ADD,
                left=Literal(10, DataType.BIGINT),
                right=Literal(5, DataType.BIGINT)
            )
        )

        filter1 = Filter(scan, inner_filter)

        outer_filter = BinaryOp(
            op=BinaryOpType.AND,
            left=ColumnRef(None, "id", DataType.BIGINT),
            right=Literal(True, DataType.BOOLEAN)
        )

        filter2 = Filter(filter1, outer_filter)

        result = rule.apply(filter2)
        assert isinstance(result, Filter)
        assert result.predicate == ColumnRef(None, "id", DataType.BIGINT)

        inner = result.input
        assert isinstance(inner, Filter)
        assert isinstance(inner.predicate, BinaryOp)
        assert isinstance(inner.predicate.right, Literal)
        assert inner.predicate.right.value == 15

    def test_simplify_complex_expression(self):
        """Test simplification of complex nested expression."""
        rule = ExpressionSimplificationRule()

        scan = Scan(
            datasource="db",
            schema_name="public",
            table_name="users",
            columns=["id", "age"]
        )

        col = ColumnRef(None, "age", DataType.BIGINT)

        complex_expr = BinaryOp(
            op=BinaryOpType.AND,
            left=BinaryOp(
                op=BinaryOpType.OR,
                left=col,
                right=Literal(False, DataType.BOOLEAN)
            ),
            right=Literal(True, DataType.BOOLEAN)
        )

        filter_node = Filter(scan, complex_expr)

        result = rule.apply(filter_node)
        assert isinstance(result, Filter)
        assert result.predicate == col
