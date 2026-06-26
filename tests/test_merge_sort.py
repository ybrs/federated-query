"""PhysicalSort through the merge engine, focusing on NULLS placement.

The merge path renders per-key ``NULLS FIRST/LAST`` (a capability the old
single-placement pyarrow path lacked and the e2e suite does not assert), so
this pins explicit placement, the per-direction default, and multi-key order.
"""

import pyarrow as pa
import pytest

from federated_query.executor.merge_engine import MergeEngine
from federated_query.plan.expressions import ColumnRef
from federated_query.plan.physical import PhysicalPlanNode, PhysicalSort


class _Node(PhysicalPlanNode):
    """Minimal physical node replaying a fixed table."""

    table: pa.Table

    def children(self):
        """A leaf node has no children."""
        return []

    def schema(self) -> pa.Schema:
        """Return the table schema."""
        return self.table.schema

    def column_aliases(self):
        """No qualified-name remapping."""
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


def _sorted_column(engine, table, keys, ascending, nulls_order, column):
    """Run the sort and return the values of one output column in result order."""
    op = PhysicalSort(
        input=_Node(table=table),
        sort_keys=keys,
        ascending=ascending,
        nulls_order=nulls_order,
    )
    op.set_merge_engine(engine)
    values = []
    for batch in op.execute():
        values.extend(batch.to_pydict()[column])
    return values


def test_ascending_default_nulls_last(engine):
    """Ascending with no explicit placement orders NULLs last (Postgres default)."""
    table = pa.table({"a": pa.array([3, None, 1, 2])})
    keys = [ColumnRef(table=None, column="a")]
    assert _sorted_column(engine, table, keys, [True], None, "a") == [1, 2, 3, None]


def test_descending_default_nulls_first(engine):
    """Descending with no explicit placement orders NULLs first (Postgres default)."""
    table = pa.table({"a": pa.array([3, None, 1, 2])})
    keys = [ColumnRef(table=None, column="a")]
    assert _sorted_column(engine, table, keys, [False], None, "a") == [None, 3, 2, 1]


def test_explicit_nulls_first_ascending(engine):
    """An explicit NULLS FIRST overrides the ascending default."""
    table = pa.table({"a": pa.array([3, None, 1, 2])})
    keys = [ColumnRef(table=None, column="a")]
    assert _sorted_column(engine, table, keys, [True], ["FIRST"], "a") == [
        None,
        1,
        2,
        3,
    ]


def test_per_key_nulls_placement(engine):
    """Each key gets its own NULLS placement (not one placement for all keys)."""
    # Group by g ASC NULLS LAST, then within group order t DESC NULLS FIRST.
    table = pa.table(
        {
            "g": pa.array([1, 1, 1, 2]),
            "t": pa.array([5, None, 7, 9]),
            "tag": pa.array(["a", "b", "c", "d"]),
        }
    )
    keys = [ColumnRef(table=None, column="g"), ColumnRef(table=None, column="t")]
    order = _sorted_column(engine, table, keys, [True, False], ["LAST", "FIRST"], "tag")
    # group 1: t DESC NULLS FIRST -> NULL(b), 7(c), 5(a); then group 2: 9(d)
    assert order == ["b", "c", "a", "d"]
