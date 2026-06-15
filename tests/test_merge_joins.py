"""Every PhysicalHashJoin shape through the DuckDB merge engine.

The e2e decorrelation suite only produces INNER/LEFT/SEMI/ANTI joins, so
RIGHT and FULL outer joins (and the exact NULL-key handling and ``right_``
duplicate-name convention) would otherwise be untested. This pins all six
shapes directly, against PostgreSQL/DuckDB outer-join NULL semantics.
"""

from collections import Counter

import pyarrow as pa
import pytest

from federated_query.executor.merge_engine import MergeEngine
from federated_query.plan.expressions import ColumnRef
from federated_query.plan.logical import JoinType
from federated_query.plan.physical import PhysicalHashJoin


class _Node:
    """Minimal physical node replaying a fixed table."""

    def __init__(self, table: pa.Table):
        """Hold the table to replay."""
        self._table = table

    def schema(self) -> pa.Schema:
        """Return the table schema."""
        return self._table.schema

    def column_aliases(self):
        """No qualified-name remapping."""
        return {}

    def apply_dynamic_filter(self, key_columns, value_tuples) -> bool:
        """Fake probe declines the G9 dynamic filter."""
        return False

    def execute(self):
        """Yield the table's batches."""
        for batch in self._table.to_batches():
            yield batch


@pytest.fixture
def engine():
    """A merge engine for the duration of one test."""
    eng = MergeEngine("256MB", None)
    yield eng
    eng.close()


@pytest.fixture
def left():
    """Left input with a non-matching key (1) and a NULL key."""
    return _Node(
        pa.table({"k": pa.array([1, 2, 3, None]), "v": pa.array(["a", "b", "c", "d"])})
    )


@pytest.fixture
def right():
    """Right input with a duplicate key (3) and a NULL key."""
    return _Node(
        pa.table({"k": pa.array([2, 3, 3, None]), "w": pa.array(["x", "y", "z", "q"])})
    )


def _join_keys(engine, left, right, join_type, columns) -> Counter:
    """Run the join and return a multiset of the selected output columns."""
    join = PhysicalHashJoin(
        left=left,
        right=right,
        join_type=join_type,
        left_keys=[ColumnRef(table=None, column="k")],
        right_keys=[ColumnRef(table=None, column="k")],
        build_side="right",
    )
    join.set_merge_engine(engine)
    counts: Counter = Counter()
    for batch in join.execute():
        data = batch.to_pydict()
        for i in range(batch.num_rows):
            row = []
            for column in columns:
                row.append(data[column][i] if column in data else None)
            counts[tuple(row)] += 1
    return counts


def test_inner(engine, left, right):
    """INNER keeps matched pairs only; NULL keys never match."""
    assert _join_keys(engine, left, right, JoinType.INNER, ["k", "right_k"]) == Counter(
        {(2, 2): 1, (3, 3): 2}
    )


def test_left(engine, left, right):
    """LEFT keeps every left row; unmatched get NULL right (incl. the NULL key)."""
    assert _join_keys(engine, left, right, JoinType.LEFT, ["k", "right_k"]) == Counter(
        {(2, 2): 1, (3, 3): 2, (1, None): 1, (None, None): 1}
    )


def test_right(engine, left, right):
    """RIGHT keeps every right row; the unmatched NULL-key right row gets NULL left."""
    assert _join_keys(engine, left, right, JoinType.RIGHT, ["k", "right_k"]) == Counter(
        {(2, 2): 1, (3, 3): 2, (None, None): 1}
    )


def test_full(engine, left, right):
    """FULL keeps both sides' unmatched rows (two distinct NULL-key rows)."""
    assert _join_keys(engine, left, right, JoinType.FULL, ["k", "right_k"]) == Counter(
        {(2, 2): 1, (3, 3): 2, (1, None): 1, (None, None): 2}
    )


def test_semi(engine, left, right):
    """SEMI emits each matching left row once, left columns only."""
    assert _join_keys(engine, left, right, JoinType.SEMI, ["k"]) == Counter(
        {(2,): 1, (3,): 1}
    )


def test_anti(engine, left, right):
    """ANTI emits left rows with no match, including the NULL key."""
    assert _join_keys(engine, left, right, JoinType.ANTI, ["k"]) == Counter(
        {(1,): 1, (None,): 1}
    )
