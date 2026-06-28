"""The merge-path resolvers reject an unresolved qualified reference loudly.

A qualified ColumnRef absent from a relation's alias map is an internal planning
error (the relation does not expose that column). The resolvers raise instead of
dropping the qualifier and resolving by the bare column name - which could bind
to a same-named column of another relation. Unqualified references (aggregate
outputs, decorrelation-exposed names) keep their bare name, which is correct.
"""

import pyarrow as pa
import pytest

from federated_query.plan.emit.resolver import MergeResolver, ColumnResolutionError
from federated_query.executor.expression_evaluator import (
    ExpressionEvaluator,
    ExpressionEvaluationError,
)
from federated_query.plan.expressions import ColumnRef


def test_merge_resolver_resolves_mapped_qualified_reference():
    """A qualified reference present in the alias map maps to its physical name."""
    resolver = MergeResolver({("v", "a"): "a"})
    node = resolver.resolve("v", "a")
    assert node.sql() == '"a"'


def test_merge_resolver_raises_on_unresolved_qualified_reference():
    """A qualified reference absent from the map raises, never bare-name guesses."""
    resolver = MergeResolver({("v", "a"): "a"})
    with pytest.raises(ColumnResolutionError) as exc_info:
        resolver.resolve("w", "a")
    assert "w.a" in str(exc_info.value)


def test_merge_resolver_keeps_unqualified_bare_name():
    """An unqualified reference (no table) is emitted as the bare name."""
    resolver = MergeResolver({})
    node = resolver.resolve(None, "total")
    assert node.sql() == '"total"'


def test_evaluator_resolves_mapped_qualified_reference():
    """The evaluator reads a mapped qualified reference's physical column."""
    batch = pa.RecordBatch.from_pydict({"a": pa.array([7])})
    evaluator = ExpressionEvaluator(batch, alias_map={("v", "a"): "a"})
    result = evaluator.evaluate(ColumnRef(table="v", column="a"))
    assert result.to_pylist() == [7]


def test_evaluator_raises_on_unresolved_qualified_reference():
    """A qualified reference absent from the alias map raises, never guesses."""
    batch = pa.RecordBatch.from_pydict({"a": pa.array([1])})
    evaluator = ExpressionEvaluator(batch, alias_map={("v", "a"): "a"})
    with pytest.raises(ExpressionEvaluationError) as exc_info:
        evaluator.evaluate(ColumnRef(table="w", column="a"))
    assert "w.a" in str(exc_info.value)


def test_evaluator_keeps_unqualified_bare_name():
    """An unqualified reference reads the batch column of that name."""
    batch = pa.RecordBatch.from_pydict({"total": pa.array([5])})
    evaluator = ExpressionEvaluator(batch)
    result = evaluator.evaluate(ColumnRef(table=None, column="total"))
    assert result.to_pylist() == [5]
