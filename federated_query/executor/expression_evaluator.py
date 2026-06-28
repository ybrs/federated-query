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
    DataType,
    FunctionCall,
    Cast,
    CaseExpr,
    InList,
    BetweenExpression,
)

# Maps engine DataTypes to the Arrow type a local CAST converts to.
_ARROW_CAST_TYPES = {
    DataType.INTEGER: pa.int64(),
    DataType.BIGINT: pa.int64(),
    DataType.FLOAT: pa.float32(),
    DataType.DOUBLE: pa.float64(),
    DataType.DECIMAL: pa.float64(),
    DataType.VARCHAR: pa.string(),
    DataType.TEXT: pa.string(),
    DataType.BOOLEAN: pa.bool_(),
    DataType.DATE: pa.date32(),
    DataType.TIMESTAMP: pa.timestamp("us"),
}


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
            (Cast, self._eval_cast),
            (CaseExpr, self._eval_case),
            (InList, self._eval_in_list),
            (BetweenExpression, self._eval_between),
        ]
        for expr_type, handler in handlers:
            if isinstance(expr, expr_type):
                return handler
        return None

    def _eval_column(self, expr: ColumnRef):
        """Resolve a column reference against the batch via the alias map.

        An unqualified reference (no table) reads the batch column of that name -
        the physical name an aggregate output / exposed column carries. A
        QUALIFIED reference must resolve through the alias map; if it does not,
        the plan produced a reference the relation does not expose, which raises
        rather than guessing by bare name (which could pick a same-named column
        from another relation).
        """
        if expr.table is not None:
            mapped = self.alias_map.get((expr.table, expr.column))
            if mapped is None:
                raise ExpressionEvaluationError(
                    f"Unresolved column reference {expr.table}.{expr.column}; "
                    f"relation exposes {sorted(self.alias_map.keys())}"
                )
            return self.batch.column(mapped)
        return self.batch.column(expr.column)

    def _eval_literal(self, expr: Literal):
        """Convert a literal into an Arrow scalar."""
        return pa.scalar(expr.value)

    def _eval_binary(self, expr: BinaryOp):
        """Evaluate a binary operation with SQL NULL semantics."""
        special = self._special_binary_handler(expr.op)
        if special is not None:
            return special(expr)
        kernel = self._binary_kernel(expr.op)
        left = self._eval(expr.left)
        right = self._eval(expr.right)
        return kernel(left, right)

    def _special_binary_handler(self, op: BinaryOpType):
        """Return a handler for operators that are not plain kernels."""
        handlers = {
            BinaryOpType.LIKE: self._eval_like,
            BinaryOpType.ILIKE: self._eval_ilike,
            BinaryOpType.CONCAT: self._eval_concat,
            BinaryOpType.NULL_SAFE_EQ: self._eval_null_safe_eq,
            BinaryOpType.NULL_SAFE_NEQ: self._eval_null_safe_neq,
            BinaryOpType.REGEX_MATCH: self._eval_regex_match,
            BinaryOpType.REGEX_IMATCH: self._eval_regex_imatch,
        }
        return handlers.get(op)

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
        """Evaluate case-sensitive LIKE."""
        return self._eval_like_impl(expr, ignore_case=False)

    def _eval_ilike(self, expr: BinaryOp):
        """Evaluate case-insensitive ILIKE."""
        return self._eval_like_impl(expr, ignore_case=True)

    def _eval_like_impl(self, expr: BinaryOp, ignore_case: bool):
        """Evaluate LIKE/ILIKE with a literal or per-row pattern."""
        value = self._broadcast(self._eval(expr.left))
        if isinstance(expr.right, Literal):
            pattern = str(expr.right.value)
            return pc.match_like(value, pattern, ignore_case=ignore_case)
        patterns = self._broadcast(self._eval(expr.right))
        return self._match_like_per_row(value, patterns, ignore_case)

    def _match_like_per_row(
        self, value: pa.Array, patterns: pa.Array, ignore_case: bool
    ):
        """Evaluate LIKE/ILIKE row by row when the pattern is not constant."""
        results = []
        for row_index in range(len(value)):
            results.append(
                self._match_like_one(value, patterns, row_index, ignore_case)
            )
        return pa.array(results, type=pa.bool_())

    def _match_like_one(self, value, patterns, row_index, ignore_case):
        """Match a single row's value against its per-row pattern."""
        pattern = patterns[row_index].as_py()
        if pattern is None or not value[row_index].is_valid:
            return None
        cell = value.slice(row_index, 1)
        matched = pc.match_like(cell, pattern, ignore_case=ignore_case)
        return matched[0].as_py()

    def _eval_null_safe_eq(self, expr: BinaryOp):
        """Evaluate ``a IS NOT DISTINCT FROM b`` (NULL equals NULL, never UNKNOWN)."""
        left = self._broadcast(self._eval(expr.left))
        right = self._broadcast(self._eval(expr.right))
        equal = pc.fill_null(pc.equal(left, right), False)
        both_null = pc.and_(pc.is_null(left), pc.is_null(right))
        return pc.or_(equal, both_null)

    def _eval_null_safe_neq(self, expr: BinaryOp):
        """Evaluate ``a IS DISTINCT FROM b`` as the negation of null-safe equals."""
        return pc.invert(self._eval_null_safe_eq(expr))

    def _eval_regex_match(self, expr: BinaryOp):
        """Evaluate ``a ~ pattern`` (POSIX regex match)."""
        return self._eval_regex(expr, ignore_case=False)

    def _eval_regex_imatch(self, expr: BinaryOp):
        """Evaluate ``a ~* pattern`` (case-insensitive POSIX regex match)."""
        return self._eval_regex(expr, ignore_case=True)

    def _eval_regex(self, expr: BinaryOp, ignore_case: bool):
        """Evaluate a regex match with a literal pattern, propagating NULLs."""
        value = self._broadcast(self._eval(expr.left))
        if not isinstance(expr.right, Literal):
            raise ExpressionEvaluationError(
                "Regex match requires a constant pattern for local evaluation"
            )
        pattern = str(expr.right.value)
        return pc.match_substring_regex(value, pattern, ignore_case=ignore_case)

    def _eval_concat(self, expr: BinaryOp):
        """Evaluate ``a || b`` as string concat with SQL NULL propagation."""
        left = self._broadcast(self._eval(expr.left))
        right = self._broadcast(self._eval(expr.right))
        return self._concat_arrays([left, right], null_handling="emit_null")

    def _concat_arrays(self, arrays, null_handling: str):
        """Join string-cast arrays element-wise with the given NULL policy."""
        string_args = []
        for array in arrays:
            string_args.append(self._to_string_array(array))
        return pc.binary_join_element_wise(
            *string_args, "", null_handling=null_handling
        )

    def _to_string_array(self, array: pa.Array) -> pa.Array:
        """Cast an array to UTF-8 string, leaving NULLs intact."""
        if pa.types.is_string(array.type) or pa.types.is_large_string(array.type):
            return array
        return pc.cast(array, pa.string())

    def _eval_cast(self, expr: Cast) -> pa.Array:
        """Evaluate CAST locally via an Arrow type conversion."""
        value = self._broadcast(self._eval(expr.expr))
        target = _ARROW_CAST_TYPES.get(expr.get_type())
        if target is None:
            raise ExpressionEvaluationError(
                f"Unsupported local CAST target: {expr.target_type}"
            )
        return pc.cast(value, target)

    def _eval_unary(self, expr: UnaryOp):
        """Evaluate NOT / IS NULL / IS NOT NULL / negation."""
        operand = self._broadcast(self._eval(expr.operand))
        if expr.op == UnaryOpType.NOT:
            return pc.invert(operand)
        if expr.op == UnaryOpType.IS_NULL:
            return pc.is_null(operand)
        if expr.op == UnaryOpType.IS_NOT_NULL:
            return pc.is_valid(operand)
        if expr.op == UnaryOpType.NEGATE:
            return pc.negate(operand)
        raise ExpressionEvaluationError(f"Unsupported unary operator: {expr.op.value}")

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
        if name == "CONCAT":
            return self._concat_arrays(args, null_handling="skip")
        kernel = kernels.get(name)
        if kernel is None:
            raise ExpressionEvaluationError(f"Unsupported function: {name}")
        return kernel(args[0])

    def _eval_case(self, expr: CaseExpr):
        """Evaluate CASE, computing each branch only on its matching rows.

        SQL CASE is short-circuiting: an unguarded branch (e.g. ``10/(id-1)``)
        must never be evaluated on rows another branch already matched.
        Evaluating a branch over the full batch would raise (divide by zero),
        so each branch value is computed on the masked subset and scattered
        back into the result.
        """
        result = pa.nulls(self.batch.num_rows, type=self._case_result_type(expr))
        pending = pa.array([True] * self.batch.num_rows, type=pa.bool_())
        for condition, value in expr.when_clauses:
            mask = pc.and_(pending, self._condition_mask(condition))
            result = pc.if_else(mask, self._eval_value_masked(value, mask), result)
            pending = pc.and_(pending, pc.invert(mask))
        if expr.else_result is not None:
            branch = self._eval_value_masked(expr.else_result, pending)
            result = pc.if_else(pending, branch, result)
        return result

    def _condition_mask(self, condition: Expression) -> pa.Array:
        """Evaluate a WHEN condition, treating NULL as not-matched."""
        return pc.fill_null(self._broadcast(self._eval(condition)), False)

    def _case_result_type(self, expr: CaseExpr):
        """Infer the CASE result type without faulting on guarded branches."""
        empty = self.batch.slice(0, 0)
        sample = expr.when_clauses[0][1] if expr.when_clauses else expr.else_result
        return ExpressionEvaluator(empty, self.alias_map).evaluate(sample).type

    def _eval_value_masked(self, value: Expression, mask: pa.Array) -> pa.Array:
        """Evaluate a branch value on masked rows, scattered to batch length."""
        filtered = self.batch.filter(mask)
        sub_result = ExpressionEvaluator(filtered, self.alias_map).evaluate(value)
        if filtered.num_rows == 0:
            return pa.nulls(self.batch.num_rows, type=sub_result.type)
        return pc.take(sub_result, self._scatter_indices(mask))

    def _scatter_indices(self, mask: pa.Array) -> pa.Array:
        """Map each row to its position in the filtered subset (0 if unset)."""
        indices = []
        counter = 0
        for flag in mask:
            if flag.as_py():
                indices.append(counter)
                counter += 1
            else:
                indices.append(0)
        return pa.array(indices, type=pa.int64())

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
