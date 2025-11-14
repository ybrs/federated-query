"""Expression rewriting and optimization.

This module provides expression-level optimizations like constant folding,
expression simplification, and predicate normalization.
"""

from abc import ABC, abstractmethod
from typing import Optional
from ..plan.expressions import (
    Expression,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    Literal,
    ColumnRef,
    FunctionCall,
    CaseExpr,
    DataType,
    InList,
    BetweenExpression,
)


class ExpressionRewriter(ABC):
    """Base class for expression rewriters."""

    @abstractmethod
    def rewrite(self, expr: Expression) -> Expression:
        """Rewrite an expression.

        Args:
            expr: Input expression

        Returns:
            Rewritten expression (may be same as input)
        """
        pass

    def rewrite_binary_op(self, expr: BinaryOp) -> Expression:
        """Rewrite binary operation."""
        left = self.rewrite(expr.left)
        right = self.rewrite(expr.right)

        if left == expr.left and right == expr.right:
            return expr

        return BinaryOp(op=expr.op, left=left, right=right)

    def rewrite_unary_op(self, expr: UnaryOp) -> Expression:
        """Rewrite unary operation."""
        operand = self.rewrite(expr.operand)

        if operand == expr.operand:
            return expr

        return UnaryOp(op=expr.op, operand=operand)

    def rewrite_function_call(self, expr: FunctionCall) -> Expression:
        """Rewrite function call."""
        rewritten_args = []
        changed = False

        for arg in expr.args:
            rewritten_arg = self.rewrite(arg)
            rewritten_args.append(rewritten_arg)
            if rewritten_arg != arg:
                changed = True

        if not changed:
            return expr

        return FunctionCall(
            function_name=expr.function_name,
            args=rewritten_args,
            is_aggregate=expr.is_aggregate
        )

    def rewrite_case_expr(self, expr: CaseExpr) -> Expression:
        """Rewrite CASE expression."""
        rewritten_when_clauses = []
        changed = False

        for condition, result in expr.when_clauses:
            rewritten_condition = self.rewrite(condition)
            rewritten_result = self.rewrite(result)
            rewritten_when_clauses.append((rewritten_condition, rewritten_result))

            if rewritten_condition != condition or rewritten_result != result:
                changed = True

        rewritten_else = None
        if expr.else_result:
            rewritten_else = self.rewrite(expr.else_result)
            if rewritten_else != expr.else_result:
                changed = True

        if not changed:
            return expr

        return CaseExpr(
            when_clauses=rewritten_when_clauses,
            else_result=rewritten_else
        )

    def rewrite_in_list(self, expr: InList) -> Expression:
        """Rewrite IN list expression."""
        rewritten_value = self.rewrite(expr.value)
        changed = rewritten_value != expr.value
        rewritten_options = []

        for option in expr.options:
            rewritten_option = self.rewrite(option)
            rewritten_options.append(rewritten_option)
            if rewritten_option != option:
                changed = True

        if not changed:
            return expr

        return InList(value=rewritten_value, options=rewritten_options)

    def rewrite_between(self, expr: BetweenExpression) -> Expression:
        """Rewrite BETWEEN expression."""
        rewritten_value = self.rewrite(expr.value)
        rewritten_lower = self.rewrite(expr.lower)
        rewritten_upper = self.rewrite(expr.upper)

        if (
            rewritten_value == expr.value
            and rewritten_lower == expr.lower
            and rewritten_upper == expr.upper
        ):
            return expr

        return BetweenExpression(
            value=rewritten_value,
            lower=rewritten_lower,
            upper=rewritten_upper,
        )


class ConstantFoldingRewriter(ExpressionRewriter):
    """Fold constant expressions at compile time."""

    def rewrite(self, expr: Expression) -> Expression:
        """Rewrite expression with constant folding."""
        if isinstance(expr, Literal):
            return expr

        if isinstance(expr, ColumnRef):
            return expr

        if isinstance(expr, BinaryOp):
            return self._fold_binary_op(expr)

        if isinstance(expr, UnaryOp):
            return self._fold_unary_op(expr)

        if isinstance(expr, FunctionCall):
            return self.rewrite_function_call(expr)

        if isinstance(expr, CaseExpr):
            return self.rewrite_case_expr(expr)

        if isinstance(expr, InList):
            return self.rewrite_in_list(expr)

        if isinstance(expr, BetweenExpression):
            return self.rewrite_between(expr)

        return expr

    def _fold_binary_op(self, expr: BinaryOp) -> Expression:
        """Fold binary operations with constant operands."""
        left = self.rewrite(expr.left)
        right = self.rewrite(expr.right)

        if not isinstance(left, Literal) or not isinstance(right, Literal):
            if left == expr.left and right == expr.right:
                return expr
            return BinaryOp(op=expr.op, left=left, right=right)

        left_val = left.value
        right_val = right.value

        if left_val is None or right_val is None:
            return Literal(None, DataType.NULL)

        result_value = None
        result_type = None

        if expr.op == BinaryOpType.ADD:
            result_value = left_val + right_val
            result_type = self._infer_numeric_type(result_value)
        elif expr.op == BinaryOpType.SUBTRACT:
            result_value = left_val - right_val
            result_type = self._infer_numeric_type(result_value)
        elif expr.op == BinaryOpType.MULTIPLY:
            result_value = left_val * right_val
            result_type = self._infer_numeric_type(result_value)
        elif expr.op == BinaryOpType.DIVIDE:
            if right_val == 0:
                return BinaryOp(op=expr.op, left=left, right=right)
            result_value = left_val / right_val
            result_type = DataType.DOUBLE
        elif expr.op == BinaryOpType.MODULO:
            if right_val == 0:
                return BinaryOp(op=expr.op, left=left, right=right)
            result_value = left_val % right_val
            result_type = self._infer_numeric_type(result_value)
        elif expr.op == BinaryOpType.EQ:
            result_value = left_val == right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.NEQ:
            result_value = left_val != right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.LT:
            result_value = left_val < right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.LTE:
            result_value = left_val <= right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.GT:
            result_value = left_val > right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.GTE:
            result_value = left_val >= right_val
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.AND:
            result_value = bool(left_val) and bool(right_val)
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.OR:
            result_value = bool(left_val) or bool(right_val)
            result_type = DataType.BOOLEAN
        elif expr.op == BinaryOpType.CONCAT:
            result_value = str(left_val) + str(right_val)
            result_type = DataType.VARCHAR
        else:
            return BinaryOp(op=expr.op, left=left, right=right)

        return Literal(result_value, result_type)

    def _fold_unary_op(self, expr: UnaryOp) -> Expression:
        """Fold unary operations with constant operands."""
        operand = self.rewrite(expr.operand)

        if not isinstance(operand, Literal):
            if operand == expr.operand:
                return expr
            return UnaryOp(op=expr.op, operand=operand)

        operand_val = operand.value

        if operand_val is None:
            if expr.op == UnaryOpType.IS_NULL:
                return Literal(True, DataType.BOOLEAN)
            if expr.op == UnaryOpType.IS_NOT_NULL:
                return Literal(False, DataType.BOOLEAN)
            return Literal(None, DataType.NULL)

        result_value = None
        result_type = None

        if expr.op == UnaryOpType.NOT:
            result_value = not bool(operand_val)
            result_type = DataType.BOOLEAN
        elif expr.op == UnaryOpType.NEGATE:
            result_value = -operand_val
            result_type = self._infer_numeric_type(result_value)
        elif expr.op == UnaryOpType.IS_NULL:
            result_value = False
            result_type = DataType.BOOLEAN
        elif expr.op == UnaryOpType.IS_NOT_NULL:
            result_value = True
            result_type = DataType.BOOLEAN
        else:
            return UnaryOp(op=expr.op, operand=operand)

        return Literal(result_value, result_type)

    def _infer_numeric_type(self, value) -> DataType:
        """Infer numeric data type from value."""
        if isinstance(value, bool):
            return DataType.BOOLEAN
        if isinstance(value, int):
            return DataType.BIGINT
        if isinstance(value, float):
            return DataType.DOUBLE
        return DataType.VARCHAR


class ExpressionSimplificationRewriter(ExpressionRewriter):
    """Simplify expressions using algebraic rules."""

    def rewrite(self, expr: Expression) -> Expression:
        """Rewrite expression with simplification."""
        if isinstance(expr, Literal) or isinstance(expr, ColumnRef):
            return expr

        if isinstance(expr, BinaryOp):
            return self._simplify_binary_op(expr)

        if isinstance(expr, UnaryOp):
            return self._simplify_unary_op(expr)

        if isinstance(expr, FunctionCall):
            return self.rewrite_function_call(expr)

        if isinstance(expr, CaseExpr):
            return self.rewrite_case_expr(expr)

        if isinstance(expr, InList):
            return self.rewrite_in_list(expr)

        if isinstance(expr, BetweenExpression):
            return self.rewrite_between(expr)

        return expr

    def _simplify_binary_op(self, expr: BinaryOp) -> Expression:
        """Simplify binary operations."""
        left = self.rewrite(expr.left)
        right = self.rewrite(expr.right)

        if expr.op == BinaryOpType.AND:
            if self._is_true_literal(left):
                return right
            if self._is_true_literal(right):
                return left
            if self._is_false_literal(left) or self._is_false_literal(right):
                return Literal(False, DataType.BOOLEAN)
            if left == right:
                return left

        elif expr.op == BinaryOpType.OR:
            if self._is_false_literal(left):
                return right
            if self._is_false_literal(right):
                return left
            if self._is_true_literal(left) or self._is_true_literal(right):
                return Literal(True, DataType.BOOLEAN)
            if left == right:
                return left

        elif expr.op == BinaryOpType.ADD:
            if self._is_zero_literal(left):
                return right
            if self._is_zero_literal(right):
                return left

        elif expr.op == BinaryOpType.SUBTRACT:
            if self._is_zero_literal(right):
                return left
            if left == right:
                return Literal(0, DataType.BIGINT)

        elif expr.op == BinaryOpType.MULTIPLY:
            if self._is_zero_literal(left) or self._is_zero_literal(right):
                return Literal(0, DataType.BIGINT)
            if self._is_one_literal(left):
                return right
            if self._is_one_literal(right):
                return left

        elif expr.op == BinaryOpType.DIVIDE:
            if self._is_one_literal(right):
                return left
            if self._is_zero_literal(left):
                return Literal(0, DataType.BIGINT)

        if left == expr.left and right == expr.right:
            return expr

        return BinaryOp(op=expr.op, left=left, right=right)

    def _simplify_unary_op(self, expr: UnaryOp) -> Expression:
        """Simplify unary operations."""
        operand = self.rewrite(expr.operand)

        if expr.op == UnaryOpType.NOT:
            if isinstance(operand, UnaryOp) and operand.op == UnaryOpType.NOT:
                return operand.operand
            if self._is_true_literal(operand):
                return Literal(False, DataType.BOOLEAN)
            if self._is_false_literal(operand):
                return Literal(True, DataType.BOOLEAN)

        elif expr.op == UnaryOpType.NEGATE:
            if isinstance(operand, UnaryOp) and operand.op == UnaryOpType.NEGATE:
                return operand.operand

        if operand == expr.operand:
            return expr

        return UnaryOp(op=expr.op, operand=operand)

    def _is_true_literal(self, expr: Expression) -> bool:
        """Check if expression is TRUE literal."""
        return isinstance(expr, Literal) and expr.value is True

    def _is_false_literal(self, expr: Expression) -> bool:
        """Check if expression is FALSE literal."""
        return isinstance(expr, Literal) and expr.value is False

    def _is_zero_literal(self, expr: Expression) -> bool:
        """Check if expression is zero literal."""
        return isinstance(expr, Literal) and expr.value == 0

    def _is_one_literal(self, expr: Expression) -> bool:
        """Check if expression is one literal."""
        return isinstance(expr, Literal) and expr.value == 1


class CompositeExpressionRewriter(ExpressionRewriter):
    """Apply multiple rewriters in sequence."""

    def __init__(self):
        """Initialize composite rewriter."""
        self.rewriters = []

    def add_rewriter(self, rewriter: ExpressionRewriter) -> None:
        """Add a rewriter to the pipeline."""
        self.rewriters.append(rewriter)

    def rewrite(self, expr: Expression) -> Expression:
        """Apply all rewriters in sequence."""
        current = expr
        for rewriter in self.rewriters:
            current = rewriter.rewrite(current)
        return current
