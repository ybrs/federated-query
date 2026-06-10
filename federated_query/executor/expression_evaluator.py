"""Vectorized expression evaluation over Arrow record batches.

This is the single local expression engine used by physical operators
(filters, projections, join conditions). It implements SQL three-valued
logic: AND/OR use Kleene kernels, comparisons propagate NULL, and boolean
masks with NULL entries drop rows when used to filter (SQL WHERE semantics).
"""

from typing import Dict, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    FunctionCall,
    CaseExpr,
    InList,
    BetweenExpression,
)


class ExpressionEvaluationError(Exception):
    """Raised when an expression cannot be evaluated locally."""


_COMPARISON_KERNELS = {
    BinaryOpType.EQ: pc.equal,
    BinaryOpType.NEQ: pc.not_equal,
    BinaryOpType.LT: pc.less,
    BinaryOpType.LTE: pc.less_equal,
    BinaryOpType.GT: pc.greater,
    BinaryOpType.GTE: pc.greater_equal,
}

_ARITHMETIC_KERNELS = {
    BinaryOpType.ADD: pc.add,
    BinaryOpType.SUBTRACT: pc.subtract,
    BinaryOpType.MULTIPLY: pc.multiply,
    BinaryOpType.DIVIDE: pc.divide,
}

_LOGICAL_KERNELS = {
    BinaryOpType.AND: pc.and_kleene,
    BinaryOpType.OR: pc.or_kleene,
}


class ExpressionEvaluator:
    """Evaluates a bound expression against one RecordBatch, vectorized."""

    def __init__(
        self,
        batch: pa.RecordBatch,
        alias_map: Optional[Dict[Tuple[Optional[str], str], str]] = None,
    ):
        """Capture the batch and an optional (table, column) -> name map."""
        self.batch = batch
        self.alias_map = alias_map if alias_map is not None else {}

    def evaluate(self, expr: Expression) -> pa.Array:
        """Evaluate the expression to an array of batch length."""
        result = self._eval(expr)
        return self._broadcast(result)

    def _broadcast(self, value) -> pa.Array:
        """Materialize a kernel result as an array of batch length."""
        if isinstance(value, pa.Array):
            return value
        if isinstance(value, pa.ChunkedArray):
            return value.combine_chunks()
        rows = []
        for _ in range(self.batch.num_rows):
            rows.append(value.as_py())
        return pa.array(rows, type=value.type)

    def _eval(self, expr: Expression):
        """Dispatch evaluation by expression type."""
        handler = self._find_handler(expr)
        if handler is None:
            raise ExpressionEvaluationError(
                f"Unsupported expression for local evaluation: {type(expr).__name__}"
            )
        return handler(expr)

    def _find_handler(self, expr: Expression):
        """Look up the evaluation handler for an expression type."""
        handlers = [
            (ColumnRef, self._eval_column),
            (Literal, self._eval_literal),
            (BinaryOp, self._eval_binary),
            (UnaryOp, self._eval_unary),
            (FunctionCall, self._eval_function),
            (CaseExpr, self._eval_case),
            (InList, self._eval_in_list),
            (BetweenExpression, self._eval_between),
        ]
        for expr_type, handler in handlers:
            if isinstance(expr, expr_type):
                return handler
        return None

    def _eval_column(self, expr: ColumnRef):
        """Resolve a column reference against the batch by name."""
        if expr.table is not None:
            mapped = self.alias_map.get((expr.table, expr.column))
            if mapped is not None:
                return self.batch.column(mapped)
        return self.batch.column(expr.column)

    def _eval_literal(self, expr: Literal):
        """Convert a literal into an Arrow scalar."""
        return pa.scalar(expr.value)

    def _eval_binary(self, expr: BinaryOp):
        """Evaluate a binary operation with SQL NULL semantics."""
        if expr.op == BinaryOpType.LIKE:
            return self._eval_like(expr)
        kernel = self._binary_kernel(expr.op)
        left = self._eval(expr.left)
        right = self._eval(expr.right)
        return kernel(left, right)

    def _binary_kernel(self, op: BinaryOpType):
        """Find the Arrow kernel for a binary operator."""
        for kernel_map in (_COMPARISON_KERNELS, _ARITHMETIC_KERNELS, _LOGICAL_KERNELS):
            kernel = kernel_map.get(op)
            if kernel is not None:
                return kernel
        raise ExpressionEvaluationError(
            f"Unsupported binary operator for local evaluation: {op.value}"
        )

    def _eval_like(self, expr: BinaryOp):
        """Evaluate LIKE with a literal or per-row pattern."""
        value = self._broadcast(self._eval(expr.left))
        if isinstance(expr.right, Literal):
            return pc.match_like(value, str(expr.right.value))
        patterns = self._broadcast(self._eval(expr.right))
        return self._match_like_per_row(value, patterns)

    def _match_like_per_row(self, value: pa.Array, patterns: pa.Array):
        """Evaluate LIKE row by row when the pattern is not constant."""
        results = []
        for row_index in range(len(value)):
            pattern = patterns[row_index].as_py()
            if pattern is None or not value[row_index].is_valid:
                results.append(None)
                continue
            matched = pc.match_like(value.slice(row_index, 1), pattern)
            results.append(matched[0].as_py())
        return pa.array(results, type=pa.bool_())

    def _eval_unary(self, expr: UnaryOp):
        """Evaluate NOT / IS NULL / IS NOT NULL / negation."""
        operand = self._broadcast(self._eval(expr.operand))
        if expr.op == UnaryOpType.NOT:
            return pc.invert(operand)
        if expr.op == UnaryOpType.IS_NULL:
            return pc.is_null(operand)
        if expr.op == UnaryOpType.IS_NOT_NULL:
            return pc.is_valid(operand)
        return pc.negate(operand)

    def _eval_function(self, expr: FunctionCall):
        """Evaluate a scalar (non-aggregate) function call."""
        if expr.is_aggregate:
            raise ExpressionEvaluationError(
                f"Aggregate {expr.function_name} cannot be evaluated per-row"
            )
        args = []
        for arg in expr.args:
            args.append(self._broadcast(self._eval(arg)))
        return self._apply_function(expr.function_name.upper(), args)

    def _apply_function(self, name: str, args):
        """Apply a supported scalar function to evaluated arguments."""
        kernels = {
            "UPPER": pc.utf8_upper,
            "LOWER": pc.utf8_lower,
            "ABS": pc.abs,
            "LENGTH": pc.utf8_length,
        }
        if name == "COALESCE":
            return pc.coalesce(*args)
        kernel = kernels.get(name)
        if kernel is None:
            raise ExpressionEvaluationError(f"Unsupported function: {name}")
        return kernel(args[0])

    def _eval_case(self, expr: CaseExpr):
        """Evaluate CASE by folding WHEN branches with if_else."""
        result = self._case_default(expr)
        for condition, value in reversed(expr.when_clauses):
            condition_mask = self._broadcast(self._eval(condition))
            # SQL CASE treats a NULL condition as "not matched", while
            # if_else would propagate it to a NULL result.
            condition_mask = pc.fill_null(condition_mask, False)
            branch_value = self._broadcast(self._eval(value))
            result = pc.if_else(condition_mask, branch_value, result)
        return result

    def _case_default(self, expr: CaseExpr):
        """Build the ELSE value (typed NULLs when ELSE is absent)."""
        if expr.else_result is not None:
            return self._broadcast(self._eval(expr.else_result))
        first_value = self._broadcast(self._eval(expr.when_clauses[0][1]))
        return pa.nulls(self.batch.num_rows, type=first_value.type)

    def _eval_in_list(self, expr: InList):
        """Evaluate IN (literal list) as a fold of OR'ed equalities."""
        value = self._broadcast(self._eval(expr.value))
        result = None
        for option in expr.options:
            equal = pc.equal(value, self._eval(option))
            result = equal if result is None else pc.or_kleene(result, equal)
        if result is None:
            raise ExpressionEvaluationError("IN list must have at least one option")
        return result

    def _eval_between(self, expr: BetweenExpression):
        """Evaluate BETWEEN as lower <= value AND value <= upper."""
        value = self._broadcast(self._eval(expr.value))
        lower_check = pc.greater_equal(value, self._eval(expr.lower))
        upper_check = pc.less_equal(value, self._eval(expr.upper))
        return pc.and_kleene(lower_check, upper_check)
