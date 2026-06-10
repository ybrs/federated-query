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
    DataType,
)


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
        when_clauses=[(is_big, Literal("big", DataType.VARCHAR))],
        else_result=Literal("small", DataType.VARCHAR),
    )
    assert evaluate(expr) == ["big", "small", "big", "small"]


def test_coalesce_function():
    """COALESCE replaces NULLs with the fallback."""
    expr = FunctionCall(
        function_name="COALESCE",
        args=[col("amount"), Literal(0.0, DataType.DOUBLE)],
    )
    assert evaluate(expr) == [100.0, 0.0, 300.0, 50.0]


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
