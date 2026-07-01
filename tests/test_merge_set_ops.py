"""Local UNION/INTERSECT/EXCEPT run through the DuckDB merge engine.

These operators only execute locally for cross-source set operations (the
same-source case is pushed as ``PhysicalRemoteSetOp``), so the e2e suites do not
exercise them. These tests pin the merge-engine path and its multiset semantics.
"""

import pyarrow as pa
import pytest

from federated_query.executor.merge_engine import MergeEngine
from federated_query.plan.logical import SetOpKind
from federated_query.plan.physical import (
    PhysicalPlanNode,
    PhysicalSetOperation,
    PhysicalUnion,
)


class _Node(PhysicalPlanNode):
    """Minimal physical node yielding a fixed table's batches."""

    table: pa.Table

    def children(self):
        """A leaf node has no children."""
        return []

    def schema(self) -> pa.Schema:
        """Return the table schema."""
        return self.table.schema

    def column_aliases(self):
        """No qualified-name remapping for this fake node."""
        return {}

    def execute(self):
        """Yield the table's batches."""
        for batch in self.table.to_batches():
            yield batch

    def estimated_cost(self) -> float:
        """Free; this is a test fixture."""
        return 0.0


@pytest.fixture
def engine():
    """A merge engine for the duration of one test."""
    eng = MergeEngine("256MB", None)
    yield eng
    eng.close()


@pytest.fixture
def left():
    """Left input with a duplicate (1, x) row."""
    return _Node(
        table=pa.table(
            {"a": pa.array([1, 1, 2, 3]), "b": pa.array(["x", "x", "y", "z"])}
        )
    )


@pytest.fixture
def right():
    """Right input with two (2, y) rows."""
    return _Node(
        table=pa.table({"a": pa.array([1, 2, 2]), "b": pa.array(["x", "y", "y"])})
    )


def _run(op, engine) -> list:
    """Execute an operator on the engine and return sorted (a, b) tuples."""
    op.set_merge_engine(engine)
    rows = []
    for batch in op.execute():
        data = batch.to_pydict()
        for i in range(batch.num_rows):
            rows.append((data["a"][i], data["b"][i]))
    return sorted(rows)


def test_union_distinct(engine, left, right):
    """UNION removes duplicates across both branches."""
    op = PhysicalUnion(inputs=[left, right], distinct=True)
    assert _run(op, engine) == [(1, "x"), (2, "y"), (3, "z")]


def test_intersect_distinct(engine, left, right):
    """INTERSECT keeps distinct rows present in both inputs."""
    op = PhysicalSetOperation(
        left=left, right=right, kind=SetOpKind.INTERSECT, distinct=True
    )
    assert _run(op, engine) == [(1, "x"), (2, "y")]


def test_intersect_all(engine, left, right):
    """INTERSECT ALL keeps the per-row minimum multiplicity."""
    op = PhysicalSetOperation(
        left=left, right=right, kind=SetOpKind.INTERSECT, distinct=False
    )
    assert _run(op, engine) == [(1, "x"), (2, "y")]


def test_except_distinct(engine, left, right):
    """EXCEPT keeps distinct left rows absent from the right."""
    op = PhysicalSetOperation(
        left=left, right=right, kind=SetOpKind.EXCEPT, distinct=True
    )
    assert _run(op, engine) == [(3, "z")]


def test_except_all(engine, left, right):
    """EXCEPT ALL subtracts right multiplicities from the left."""
    op = PhysicalSetOperation(
        left=left, right=right, kind=SetOpKind.EXCEPT, distinct=False
    )
    assert _run(op, engine) == [(1, "x"), (3, "z")]
