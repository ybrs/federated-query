"""Unit tests for the vectorized expression evaluator."""

import pyarrow as pa
import pytest

from federated_query.executor.expression_evaluator import (
    ExpressionEvaluator,
    ExpressionEvaluationError,
)
from federated_query.plan.expressions import (
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
    Extract,
    DataType,
)
import datetime


def make_batch():
    """Build a batch with NULLs to exercise three-valued logic."""
    return pa.RecordBatch.from_pydict(
        {
            "id": pa.array([1, 2, 3, 4]),
            "amount": pa.array([100.0, None, 300.0, 50.0]),
            "name": pa.array(["alice", "bob", None, "temp_x"]),
            "flag": pa.array([True, None, False, True]),
        }
    )


def evaluate(expr):
    """Evaluate an expression against the standard test batch."""
    return ExpressionEvaluator(make_batch()).evaluate(expr).to_pylist()


def col(name):
    """Shorthand for an unqualified column reference."""
    return ColumnRef(table=None, column=name)


def lit(value, data_type=DataType.INTEGER):
    """Shorthand for a literal."""
    return Literal(value=value, data_type=data_type)


def test_comparison_propagates_null():
    """amount > 60 must yield NULL where amount is NULL."""
    expr = BinaryOp(op=BinaryOpType.GT, left=col("amount"), right=lit(60))
    assert evaluate(expr) == [True, None, True, False]


def test_or_uses_kleene_logic():
    """NULL OR TRUE must be TRUE, NULL OR FALSE must be NULL."""
    is_big = BinaryOp(op=BinaryOpType.GT, left=col("amount"), right=lit(60))
    is_one = BinaryOp(op=BinaryOpType.EQ, left=col("id"), right=lit(2))
    expr = BinaryOp(op=BinaryOpType.OR, left=is_big, right=is_one)
    assert evaluate(expr) == [True, True, True, False]


def test_and_uses_kleene_logic():
    """FALSE AND NULL must be FALSE, TRUE AND NULL must be NULL."""
    is_big = BinaryOp(op=BinaryOpType.GT, left=col("amount"), right=lit(60))
    expr = BinaryOp(op=BinaryOpType.AND, left=col("flag"), right=is_big)
    assert evaluate(expr) == [True, None, False, False]


def test_not_propagates_null():
    """NOT NULL must stay NULL."""
    expr = UnaryOp(op=UnaryOpType.NOT, operand=col("flag"))
    assert evaluate(expr) == [False, None, True, False]


def test_is_null_and_is_not_null():
    """IS NULL / IS NOT NULL must return crisp booleans."""
    is_null = UnaryOp(op=UnaryOpType.IS_NULL, operand=col("amount"))
    not_null = UnaryOp(op=UnaryOpType.IS_NOT_NULL, operand=col("amount"))
    assert evaluate(is_null) == [False, True, False, False]
    assert evaluate(not_null) == [True, False, True, True]


def test_arithmetic_on_columns():
    """id * 10 computes per row."""
    expr = BinaryOp(op=BinaryOpType.MULTIPLY, left=col("id"), right=lit(10))
    assert evaluate(expr) == [10, 20, 30, 40]


def test_like_matches_pattern():
    """LIKE 'temp%' matches only the temp-prefixed name; NULL stays NULL."""
    pattern = Literal(value="temp%", data_type=DataType.VARCHAR)
    expr = BinaryOp(op=BinaryOpType.LIKE, left=col("name"), right=pattern)
    assert evaluate(expr) == [False, False, None, True]


def test_case_expression_null_condition_falls_through():
    """A NULL WHEN condition must fall through, not poison the result."""
    is_big = BinaryOp(op=BinaryOpType.GT, left=col("amount"), right=lit(60))
    expr = CaseExpr(
        when_clauses=[(is_big, Literal(value="big", data_type=DataType.VARCHAR))],
        else_result=Literal(value="small", data_type=DataType.VARCHAR),
    )
    assert evaluate(expr) == ["big", "small", "big", "small"]


def test_coalesce_function():
    """COALESCE replaces NULLs with the fallback."""
    expr = FunctionCall(
        function_name="COALESCE",
        args=[col("amount"), Literal(value=0.0, data_type=DataType.DOUBLE)],
    )
    assert evaluate(expr) == [100.0, 0.0, 300.0, 50.0]


def test_nullif_function():
    """NULLIF(a, b) yields NULL where a = b, else a; the type is a's type."""
    expr = FunctionCall(
        function_name="NULLIF",
        args=[col("amount"), Literal(value=300.0, data_type=DataType.DOUBLE)],
    )
    # amount = [100, NULL, 300, 50]: the 300 becomes NULL; a NULL input (a = b is
    # NULL, not true) stays a's value, which is itself NULL here.
    assert evaluate(expr) == [100.0, None, None, 50.0]


def test_round_function():
    """ROUND(x) rounds to an integer; ROUND(x, n) rounds to n decimals."""
    whole = FunctionCall(function_name="ROUND", args=[col("amount")])
    assert evaluate(whole) == [100.0, None, 300.0, 50.0]

    fractional = BinaryOp(op=BinaryOpType.DIVIDE, left=col("amount"), right=lit(3.0))
    rounded = FunctionCall(function_name="ROUND", args=[fractional, lit(2)])
    result = evaluate(rounded)
    assert result[1] is None
    assert result[0] == pytest.approx(33.33)
    assert result[2] == pytest.approx(100.0)
    assert result[3] == pytest.approx(16.67)


def test_in_list_null_value_yields_null():
    """NULL IN (...) must be NULL (unknown), not FALSE."""
    expr = InList(value=col("amount"), options=[lit(100), lit(300)])
    assert evaluate(expr) == [True, None, True, False]


def test_between_expression():
    """BETWEEN evaluates inclusively with NULL propagation."""
    expr = BetweenExpression(value=col("amount"), lower=lit(60), upper=lit(310))
    assert evaluate(expr) == [True, None, True, False]


def test_literal_broadcasts_to_batch_length():
    """A bare literal evaluates to a full-length array."""
    assert evaluate(lit(7)) == [7, 7, 7, 7]


def test_aggregate_function_rejected():
    """Aggregates cannot be evaluated per-row and must fail loudly."""
    agg = FunctionCall(function_name="SUM", args=[col("amount")], is_aggregate=True)
    with pytest.raises(ExpressionEvaluationError):
        evaluate(agg)


def test_unknown_function_rejected():
    """Unknown scalar functions must fail loudly."""
    func = FunctionCall(function_name="FROBNICATE", args=[col("id")])
    with pytest.raises(ExpressionEvaluationError):
        evaluate(func)


def _substring(text, start, length=None):
    """Build a SUBSTRING(text FROM start [FOR length]) call over constant bounds."""
    args = [lit(text, DataType.VARCHAR), lit(start, DataType.INTEGER)]
    if length is not None:
        args.append(lit(length, DataType.INTEGER))
    return FunctionCall(function_name="SUBSTRING", args=args)


def test_substring_is_one_based():
    """SUBSTRING positions are 1-based and inclusive."""
    # The literal broadcasts across the standard batch, so every row is equal.
    assert set(evaluate(_substring("abcdef", 1, 2))) == {"ab"}
    assert set(evaluate(_substring("abcdef", 3, 2))) == {"cd"}


def test_substring_start_at_or_below_one_clamps_to_string_start():
    """A start <= 0 clamps to the string start, never a from-the-end Arrow slice."""
    # SUBSTRING('abcdef' FROM 0) -> whole string (not the last char).
    assert set(evaluate(_substring("abcdef", 0))) == {"abcdef"}
    # A window reaching before position 1 keeps only the in-range chars.
    assert set(evaluate(_substring("abcdef", -2, 5))) == {"ab"}
    assert set(evaluate(_substring("abcdef", 0, 3))) == {"ab"}


def test_substring_non_constant_bounds_raise():
    """A per-row (column) position/length is unsupported and raises loudly."""
    expr = FunctionCall(
        function_name="SUBSTRING",
        args=[lit("abc", DataType.VARCHAR), col("id")],
    )
    with pytest.raises(ExpressionEvaluationError):
        evaluate(expr)


def test_extract_dow_is_sunday_zero():
    """EXTRACT(DOW) is Sunday=0..Saturday=6, matching SQL - not Arrow's Monday=0."""
    batch = pa.RecordBatch.from_pydict(
        {"d": pa.array([datetime.date(2026, 7, 5), datetime.date(2026, 7, 6)])}
    )  # Sunday, Monday
    expr = Extract(field="DOW", source=col("d"))
    assert ExpressionEvaluator(batch).evaluate(expr).to_pylist() == [0, 1]


def test_decimal_times_integer_overflow_widens_not_crashes():
    """decimal(38) * int overflows precision 38; it widens to decimal256, no crash."""
    batch = pa.RecordBatch.from_pydict(
        {
            "d": pa.array([2], pa.decimal128(38, 0)),
            "i": pa.array([3], pa.int64()),
        }
    )
    expr = BinaryOp(op=BinaryOpType.MULTIPLY, left=col("d"), right=col("i"))
    result = ExpressionEvaluator(batch).evaluate(expr).to_pylist()
    assert [int(v) for v in result] == [6]
