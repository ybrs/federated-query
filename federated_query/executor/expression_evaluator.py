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
    Extract,
    InList,
    BetweenExpression,
)
from ..plan.arrow_types import arrow_type_for, is_renderable


# EXTRACT(field FROM source) maps each field keyword to the Arrow temporal
# kernel that reads it. Every kernel takes one temporal array and returns an
# integer array, matching SQL EXTRACT's numeric result.
# A 64-bit integer needs at most 19 decimal digits; used to size the decimal256
# an integer operand is widened to for exact decimal x integer arithmetic.
_INTEGER_DECIMAL_DIGITS = 19


def _extract_dow(array: pa.Array) -> pa.Array:
    """EXTRACT(DOW): Sunday=0 .. Saturday=6, matching SQL / DuckDB / PostgreSQL.

    Arrow's day_of_week defaults to Monday=0; week_start=7 makes Sunday the 0.
    """
    return pc.day_of_week(array, count_from_zero=True, week_start=7)


_EXTRACT_KERNELS = {
    "YEAR": pc.year,
    "MONTH": pc.month,
    "DAY": pc.day,
    "HOUR": pc.hour,
    "MINUTE": pc.minute,
    "SECOND": pc.second,
    "QUARTER": pc.quarter,
    "WEEK": pc.iso_week,
    "DOW": _extract_dow,
    "DOY": pc.day_of_year,
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
            (Extract, self._eval_extract),
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
        return self._apply_binary_kernel(expr.op, kernel, left, right)

    def _apply_binary_kernel(self, op, kernel, left, right):
        """Apply a binary kernel, widening decimals to decimal256 when they overflow.

        Arrow rejects a decimal128 arithmetic result whose precision exceeds 38;
        SQL engines fall back to a wider numeric. When two decimal operands would
        overflow, both are cast to decimal256 (precision up to 76) so the value
        is still computed EXACTLY - float would lose the last cent.
        """
        if op in _ARITHMETIC_KERNELS and self._decimal_would_overflow(left, right):
            wide = kernel(self._to_decimal256(left), self._to_decimal256(right))
            return self._narrow_decimal(wide)
        return kernel(left, right)

    def _to_decimal256(self, value):
        """Cast a decimal-or-integer operand to decimal256 for exact wide arithmetic.

        A decimal keeps its precision and scale; an integer becomes a scale-0
        decimal wide enough to hold it, so ``decimal * int`` also computes wide.
        """
        if pa.types.is_decimal(value.type):
            return pc.cast(value, pa.decimal256(value.type.precision, value.type.scale))
        return pc.cast(value, pa.decimal256(_INTEGER_DECIMAL_DIGITS, 0))

    def _narrow_decimal(self, value):
        """Cap a wide decimal result back to decimal128(38, scale).

        The exact value was computed in decimal256; downstream consumers (the
        DuckDB merge engine) accept only decimal128 (precision <= 38), so the
        result is capped at 38 like DuckDB caps its own decimal arithmetic. A
        value that genuinely needs > 38 digits raises rather than truncating.
        """
        return pc.cast(value, pa.decimal128(38, value.type.scale))

    def _decimal_would_overflow(self, left, right) -> bool:
        """Whether a decimal arithmetic op would exceed Arrow's decimal128 cap of 38.

        Fires for decimal x decimal and decimal x integer (both computed exactly
        in decimal256); a float operand is left alone, since Arrow promotes to
        double there rather than overflowing a decimal.
        """
        if not self._is_decimal_or_int(left.type) or not self._is_decimal_or_int(
            right.type
        ):
            return False
        if not pa.types.is_decimal(left.type) and not pa.types.is_decimal(right.type):
            return False
        left_digits = self._decimal_digits(left.type)
        right_digits = self._decimal_digits(right.type)
        return left_digits + right_digits + 1 > 38

    def _is_decimal_or_int(self, arrow_type) -> bool:
        """Whether a type participates in exact decimal widening (decimal or int)."""
        return pa.types.is_decimal(arrow_type) or pa.types.is_integer(arrow_type)

    def _decimal_digits(self, arrow_type) -> int:
        """Precision of a decimal type, or the digit budget of an integer type."""
        if pa.types.is_decimal(arrow_type):
            return arrow_type.precision
        return _INTEGER_DECIMAL_DIGITS

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
        data_type = expr.get_type()
        if not is_renderable(data_type):
            raise ExpressionEvaluationError(
                f"Unsupported local CAST target: {expr.target_type}"
            )
        return pc.cast(value, arrow_type_for(data_type))

    def _eval_extract(self, expr: Extract) -> pa.Array:
        """Evaluate EXTRACT(field FROM source) via the Arrow temporal kernel."""
        source = self._broadcast(self._eval(expr.source))
        kernel = _EXTRACT_KERNELS.get(expr.field.upper())
        if kernel is None:
            raise ExpressionEvaluationError(
                f"Unsupported EXTRACT field: {expr.field}"
            )
        return kernel(source)

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
        name = expr.function_name.upper()
        if name in ("SUBSTRING", "SUBSTR"):
            return self._eval_substring(expr)
        if name == "ROUND":
            return self._eval_round(expr)
        args = []
        for arg in expr.args:
            args.append(self._broadcast(self._eval(arg)))
        return self._apply_function(name, args)

    def _eval_substring(self, expr: FunctionCall) -> pa.Array:
        """Evaluate SUBSTRING(s FROM start FOR length).

        SQL positions are 1-based; Arrow's utf8_slice_codeunits takes a 0-based
        [begin, stop) range. A start <= 0 (or a window reaching before position
        1) is clamped to the string start, matching SQL - and never handed to
        Arrow negative, which would count from the END and silently corrupt the
        result. start/length must be constant.
        """
        text = self._broadcast(self._eval(expr.args[0]))
        start = self._scalar_int(expr.args[1])
        begin = max(start - 1, 0)
        if len(expr.args) >= 3:
            stop = max(start - 1 + self._scalar_int(expr.args[2]), 0)
            return pc.utf8_slice_codeunits(text, begin, stop)
        return pc.utf8_slice_codeunits(text, begin)

    def _eval_round(self, expr: FunctionCall) -> pa.Array:
        """Evaluate ROUND(x) or ROUND(x, n); a second argument (n) is a constant.

        This path serves type inference and constant folding; the cross-source
        execution runs in DataFusion, which evaluates the same round.
        """
        value = self._broadcast(self._eval(expr.args[0]))
        if len(expr.args) >= 2:
            return pc.round(value, ndigits=self._scalar_int(expr.args[1]))
        return pc.round(value)

    def _scalar_int(self, expr: Expression) -> int:
        """A SUBSTRING position/length argument as a constant int.

        Only constant bounds are supported; a per-row position/length (a column
        or computed expression) raises loudly rather than crashing on a missing
        scalar method or silently mis-slicing.
        """
        value = self._eval(expr)
        if not isinstance(value, pa.Scalar):
            raise ExpressionEvaluationError(
                "SUBSTRING position and length must be constant integers"
            )
        return value.as_py()

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
        if name == "NULLIF":
            return self._eval_nullif(args)
        kernel = kernels.get(name)
        if kernel is None:
            raise ExpressionEvaluationError(f"Unsupported function: {name}")
        return kernel(args[0])

    def _eval_nullif(self, args):
        """NULLIF(a, b): NULL where a = b, else a (a's type).

        The first argument is already an evaluated array, so it is used once -
        no re-evaluation of the underlying expression (which a CASE rewrite
        would double-evaluate, wrong for a volatile argument).
        """
        left = args[0]
        equals = pc.equal(left, args[1])
        return pc.if_else(equals, pa.scalar(None, type=left.type), left)

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
