"""Tests for expression rewriting and optimization."""

import pytest
from federated_query.plan.expressions import (
    Literal,
    ColumnRef,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    DataType,
)
from federated_query.optimizer.expression_rewriter import (
    ConstantFoldingRewriter,
    ExpressionSimplificationRewriter,
    CompositeExpressionRewriter,
)


class TestConstantFolding:
    """Test constant folding optimization."""

    def test_fold_arithmetic_addition(self):
        """Test constant folding for addition."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=Literal(1, DataType.BIGINT),
            right=Literal(2, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 3

    def test_fold_arithmetic_subtraction(self):
        """Test constant folding for subtraction."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.SUBTRACT,
            left=Literal(5, DataType.BIGINT),
            right=Literal(3, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 2

    def test_fold_arithmetic_multiplication(self):
        """Test constant folding for multiplication."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.MULTIPLY,
            left=Literal(4, DataType.BIGINT),
            right=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 20

    def test_fold_arithmetic_division(self):
        """Test constant folding for division."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.DIVIDE,
            left=Literal(10, DataType.BIGINT),
            right=Literal(2, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 5.0

    def test_fold_arithmetic_division_by_zero(self):
        """Test that division by zero is not folded."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.DIVIDE,
            left=Literal(10, DataType.BIGINT),
            right=Literal(0, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, BinaryOp)

    def test_fold_comparison_equal(self):
        """Test constant folding for equality."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.EQ,
            left=Literal(5, DataType.BIGINT),
            right=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_comparison_not_equal(self):
        """Test constant folding for inequality."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.NEQ,
            left=Literal(5, DataType.BIGINT),
            right=Literal(3, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_comparison_less_than(self):
        """Test constant folding for less than."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.LT,
            left=Literal(3, DataType.BIGINT),
            right=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_comparison_greater_than(self):
        """Test constant folding for greater than."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.GT,
            left=Literal(5, DataType.BIGINT),
            right=Literal(3, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_logical_and(self):
        """Test constant folding for AND."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.AND,
            left=Literal(True, DataType.BOOLEAN),
            right=Literal(False, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is False

    def test_fold_logical_or(self):
        """Test constant folding for OR."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.OR,
            left=Literal(True, DataType.BOOLEAN),
            right=Literal(False, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_unary_not(self):
        """Test constant folding for NOT."""
        rewriter = ConstantFoldingRewriter()
        expr = UnaryOp(
            op=UnaryOpType.NOT,
            operand=Literal(True, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is False

    def test_fold_unary_negate(self):
        """Test constant folding for negation."""
        rewriter = ConstantFoldingRewriter()
        expr = UnaryOp(
            op=UnaryOpType.NEGATE,
            operand=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == -5

    def test_fold_is_null_on_null(self):
        """Test constant folding for IS NULL on null value."""
        rewriter = ConstantFoldingRewriter()
        expr = UnaryOp(
            op=UnaryOpType.IS_NULL,
            operand=Literal(None, DataType.NULL)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_is_null_on_value(self):
        """Test constant folding for IS NULL on non-null value."""
        rewriter = ConstantFoldingRewriter()
        expr = UnaryOp(
            op=UnaryOpType.IS_NULL,
            operand=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is False

    def test_fold_is_not_null(self):
        """Test constant folding for IS NOT NULL."""
        rewriter = ConstantFoldingRewriter()
        expr = UnaryOp(
            op=UnaryOpType.IS_NOT_NULL,
            operand=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_fold_nested_expression(self):
        """Test constant folding for nested expressions."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=BinaryOp(
                op=BinaryOpType.MULTIPLY,
                left=Literal(2, DataType.BIGINT),
                right=Literal(3, DataType.BIGINT)
            ),
            right=Literal(4, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 10

    def test_no_fold_with_column_ref(self):
        """Test that expressions with column refs are not folded."""
        rewriter = ConstantFoldingRewriter()
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=ColumnRef(None, "x", DataType.BIGINT),
            right=Literal(5, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, BinaryOp)


class TestExpressionSimplification:
    """Test expression simplification optimization."""

    def test_simplify_and_with_true(self):
        """Test x AND TRUE -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = BinaryOp(
            op=BinaryOpType.AND,
            left=col,
            right=Literal(True, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_true_and_x(self):
        """Test TRUE AND x -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = BinaryOp(
            op=BinaryOpType.AND,
            left=Literal(True, DataType.BOOLEAN),
            right=col
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_and_with_false(self):
        """Test x AND FALSE -> FALSE."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = BinaryOp(
            op=BinaryOpType.AND,
            left=col,
            right=Literal(False, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is False

    def test_simplify_or_with_false(self):
        """Test x OR FALSE -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = BinaryOp(
            op=BinaryOpType.OR,
            left=col,
            right=Literal(False, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_or_with_true(self):
        """Test x OR TRUE -> TRUE."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = BinaryOp(
            op=BinaryOpType.OR,
            left=col,
            right=Literal(True, DataType.BOOLEAN)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value is True

    def test_simplify_add_zero(self):
        """Test x + 0 -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=col,
            right=Literal(0, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_subtract_zero(self):
        """Test x - 0 -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.SUBTRACT,
            left=col,
            right=Literal(0, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_subtract_self(self):
        """Test x - x -> 0."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.SUBTRACT,
            left=col,
            right=col
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 0

    def test_simplify_multiply_zero(self):
        """Test x * 0 -> 0."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.MULTIPLY,
            left=col,
            right=Literal(0, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert isinstance(result, Literal)
        assert result.value == 0

    def test_simplify_multiply_one(self):
        """Test x * 1 -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.MULTIPLY,
            left=col,
            right=Literal(1, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_divide_one(self):
        """Test x / 1 -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.DIVIDE,
            left=col,
            right=Literal(1, DataType.BIGINT)
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_double_negation(self):
        """Test NOT (NOT x) -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BOOLEAN)
        expr = UnaryOp(
            op=UnaryOpType.NOT,
            operand=UnaryOp(
                op=UnaryOpType.NOT,
                operand=col
            )
        )
        result = rewriter.rewrite(expr)
        assert result == col

    def test_simplify_double_negate(self):
        """Test -(-x) -> x."""
        rewriter = ExpressionSimplificationRewriter()
        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = UnaryOp(
            op=UnaryOpType.NEGATE,
            operand=UnaryOp(
                op=UnaryOpType.NEGATE,
                operand=col
            )
        )
        result = rewriter.rewrite(expr)
        assert result == col


class TestCompositeRewriter:
    """Test composite rewriter applying multiple rules."""

    def test_composite_fold_and_simplify(self):
        """Test that composite applies both folding and simplification."""
        composite = CompositeExpressionRewriter()
        composite.add_rewriter(ConstantFoldingRewriter())
        composite.add_rewriter(ExpressionSimplificationRewriter())

        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=col,
            right=BinaryOp(
                op=BinaryOpType.ADD,
                left=Literal(1, DataType.BIGINT),
                right=Literal(2, DataType.BIGINT)
            )
        )

        result = composite.rewrite(expr)
        assert isinstance(result, BinaryOp)
        assert result.left == col
        assert isinstance(result.right, Literal)
        assert result.right.value == 3

    def test_composite_simplify_folded_result(self):
        """Test simplification of constant folding result."""
        composite = CompositeExpressionRewriter()
        composite.add_rewriter(ConstantFoldingRewriter())
        composite.add_rewriter(ExpressionSimplificationRewriter())

        col = ColumnRef(None, "x", DataType.BIGINT)
        expr = BinaryOp(
            op=BinaryOpType.ADD,
            left=col,
            right=BinaryOp(
                op=BinaryOpType.SUBTRACT,
                left=Literal(5, DataType.BIGINT),
                right=Literal(5, DataType.BIGINT)
            )
        )

        result = composite.rewrite(expr)
        assert result == col
