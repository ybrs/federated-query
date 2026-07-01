"""Tests for SEMI and ANTI join execution in physical operators."""

from typing import Any

import pyarrow as pa
from federated_query.plan.physical import (
    PhysicalHashJoin,
    PhysicalNestedLoopJoin,
    PhysicalPlanNode,
)
from federated_query.plan.logical import JoinType
from federated_query.plan.expressions import ColumnRef, BinaryOp, BinaryOpType


class FakeNode(PhysicalPlanNode):
    """Simple plan node that yields fixed batches for testing."""

    batches: Any

    def children(self):
        """Return no children for leaf node."""
        return []

    def execute(self):
        """Yield stored batches in order."""
        for batch in self.batches:
            yield batch

    def schema(self):
        """Return schema of first batch or empty schema."""
        if len(self.batches) == 0:
            return pa.schema([])
        return self.batches[0].schema

    def estimated_cost(self):
        """Return zero cost for test node."""
        return 0.0

    def column_aliases(self):
        """No column alias mapping for test node."""
        return {}


def _make_batch(values, names):
    """Create a RecordBatch from list-of-columns values and column names."""
    arrays = []
    index = 0
    while index < len(values):
        arrays.append(pa.array(values[index]))
        index += 1
    return pa.RecordBatch.from_arrays(arrays, names=names)


def _with_engine(node):
    """Attach the DuckDB merge engine (and to children) for local execution."""
    from federated_query.executor.executor import _attach_merge_engine
    from federated_query.executor.merge_engine import MergeEngine

    _attach_merge_engine(node, MergeEngine("256MB", None))
    return node


def _rows_from_batches(batches):
    """Convert batches to list of row dicts."""
    rows = []
    for batch in batches:
        data = batch.to_pydict()
        if len(data.keys()) == 0:
            continue
        count = len(list(data.values())[0])
        row_index = 0
        while row_index < count:
            row = {}
            for name in data.keys():
                row[name] = data[name][row_index]
            rows.append(row)
            row_index += 1
    return rows


def _semi_join_over_colliding_columns(join_type):
    """A SEMI/ANTI join whose right side has a colliding ``id`` and a unique col."""
    left = _make_batch([[1, 2], ["a", "b"]], ["id", "val"])
    right = _make_batch([[1], ["x"]], ["id", "rval"])
    return PhysicalHashJoin(
        left=FakeNode(batches=[left]),
        right=FakeNode(batches=[right]),
        join_type=join_type,
        left_keys=[ColumnRef(table=None, column="id")],
        right_keys=[ColumnRef(table=None, column="id")],
        build_side="right",
    )


def test_semi_join_schema_is_left_only():
    """SEMI join declares left columns only - no phantom right_* columns.

    _merge_join_select_list emits left columns only for SEMI/ANTI; schema() must
    match, or the empty-result path (which builds from schema()) and any parent
    reading the column set positionally would see columns the join never yields.
    """
    join = _semi_join_over_colliding_columns(JoinType.SEMI)
    assert join.schema().names == ["id", "val"]


def test_anti_join_schema_is_left_only():
    """ANTI join declares left columns only, matching its execution SELECT list."""
    join = _semi_join_over_colliding_columns(JoinType.ANTI)
    assert join.schema().names == ["id", "val"]


def test_inner_join_schema_renames_colliding_right_column():
    """INNER join keeps both sides; a colliding right column becomes right_<name>."""
    join = _semi_join_over_colliding_columns(JoinType.INNER)
    assert join.schema().names == ["id", "val", "right_id", "rval"]


def test_hash_join_semi_emits_only_matching_left_rows():
    """Input: hash SEMI join on id; Expect: only left rows with matching id, no right cols."""
    left = _make_batch([[1, 2], ["a", "b"]], ["id", "val"])
    right = _make_batch([[1], ["x"]], ["id", "rval"])
    left_node = FakeNode(batches=[left])
    right_node = FakeNode(batches=[right])
    join = PhysicalHashJoin(
        left=left_node,
        right=right_node,
        join_type=JoinType.SEMI,
        left_keys=[ColumnRef(table=None, column="id")],
        right_keys=[ColumnRef(table=None, column="id")],
        build_side="right",
    )

    rows = _rows_from_batches(list(_with_engine(join).execute()))

    assert len(rows) == 1
    assert "rval" not in rows[0]
    assert rows[0]["id"] == 1
    assert rows[0]["val"] == "a"


def test_hash_join_anti_emits_only_non_matching_left_rows():
    """Input: hash ANTI join on id; Expect: left rows without match, no right cols."""
    left = _make_batch([[1, 2], ["a", "b"]], ["id", "val"])
    right = _make_batch([[1], ["x"]], ["id", "rval"])
    left_node = FakeNode(batches=[left])
    right_node = FakeNode(batches=[right])
    join = PhysicalHashJoin(
        left=left_node,
        right=right_node,
        join_type=JoinType.ANTI,
        left_keys=[ColumnRef(table=None, column="id")],
        right_keys=[ColumnRef(table=None, column="id")],
        build_side="right",
    )

    rows = _rows_from_batches(list(_with_engine(join).execute()))

    assert len(rows) == 1
    assert rows[0]["id"] == 2
    assert rows[0]["val"] == "b"


def test_nested_loop_semi_respects_condition():
    """Input: nested-loop SEMI join with condition; Expect: only left rows satisfying predicate."""
    left = _make_batch([[1, 2], ["a", "b"]], ["id", "val"])
    right = _make_batch([[2], ["x"]], ["rid", "rval"])
    left_node = FakeNode(batches=[left])
    right_node = FakeNode(batches=[right])
    condition = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table=None, column="id"),
        right=ColumnRef(table=None, column="rid"),
    )
    join = PhysicalNestedLoopJoin(
        left=left_node,
        right=right_node,
        join_type=JoinType.SEMI,
        condition=condition,
    )

    rows = _rows_from_batches(list(_with_engine(join).execute()))

    assert len(rows) == 1
    assert rows[0]["id"] == 2
    assert rows[0]["val"] == "b"
