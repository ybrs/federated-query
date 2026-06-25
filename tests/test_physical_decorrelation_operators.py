"""Unit tests for physical operators added/fixed for decorrelation.

Covers PhysicalValues, PhysicalUnion, PhysicalSingleRowGuard,
PhysicalGroupedLimit, NULL-key behaviour of the hash join, and SQL
semantics of global aggregates over empty input.
"""

import pyarrow as pa
import pytest

from federated_query.plan.logical import JoinType
from federated_query.plan.physical import (
    PhysicalPlanNode,
    PhysicalHashJoin,
    PhysicalHashAggregate,
    PhysicalValues,
    PhysicalUnion,
    PhysicalSingleRowGuard,
    PhysicalGroupedLimit,
    CardinalityViolationError,
)
from federated_query.plan.expressions import (
    ColumnRef,
    Literal,
    FunctionCall,
    DataType,
)


class StaticSource(PhysicalPlanNode):
    """Physical node that replays a fixed list of batches."""

    def __init__(self, batches):
        self.batches = batches

    def children(self):
        """No children: this is a leaf source."""
        return []

    def execute(self):
        """Yield the configured batches."""
        for batch in self.batches:
            yield batch

    def schema(self):
        """Schema of the first batch (or empty)."""
        if self.batches:
            return self.batches[0].schema
        return pa.schema([])

    def estimated_cost(self):
        """Static sources are free."""
        return 0.0


def batch_of(**columns):
    """Build a record batch from keyword columns."""
    return pa.RecordBatch.from_pydict(columns)


def rows_of(node):
    """Execute a physical node and collect rows as dicts."""
    rows = []
    for batch in node.execute():
        for row in batch.to_pylist():
            rows.append(row)
    return rows


def col(name):
    """Shorthand for an unqualified column reference."""
    return ColumnRef(table=None, column=name)


def test_values_emits_literal_row():
    """PhysicalValues evaluates literal expressions into one batch."""
    node = PhysicalValues(
        rows=[[Literal(42, DataType.INTEGER), Literal("x", DataType.VARCHAR)]],
        output_names=["answer", "tag"],
    )
    assert rows_of(node) == [{"answer": 42, "tag": "x"}]


def test_union_all_concatenates():
    """UNION ALL keeps duplicates from both branches."""
    left = StaticSource([batch_of(id=[1, 2])])
    right = StaticSource([batch_of(id=[2, 3])])
    node = PhysicalUnion(inputs=[left, right], distinct=False)
    assert rows_of(node) == [{"id": 1}, {"id": 2}, {"id": 2}, {"id": 3}]


def test_union_distinct_deduplicates():
    """UNION DISTINCT removes rows already emitted."""
    left = StaticSource([batch_of(id=[1, 2])])
    right = StaticSource([batch_of(id=[2, 3])])
    node = PhysicalUnion(inputs=[left, right], distinct=True)
    assert rows_of(node) == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_single_row_guard_passes_one_row():
    """A single global row flows through the guard untouched."""
    node = PhysicalSingleRowGuard(input=StaticSource([batch_of(v=[10])]), keys=[])
    assert rows_of(node) == [{"v": 10}]


def test_single_row_guard_raises_on_second_row():
    """Two global rows must raise a cardinality error."""
    node = PhysicalSingleRowGuard(input=StaticSource([batch_of(v=[10, 20])]), keys=[])
    with pytest.raises(CardinalityViolationError):
        rows_of(node)


def test_single_row_guard_raises_on_duplicate_key():
    """A repeated correlation key must raise a cardinality error."""
    node = PhysicalSingleRowGuard(
        input=StaticSource([batch_of(k=[1, 2, 1], v=[10, 20, 30])]),
        keys=[col("k")],
    )
    with pytest.raises(CardinalityViolationError):
        rows_of(node)


def test_grouped_limit_keeps_first_row_per_key():
    """GroupedLimit(1) keeps only the first row of each key."""
    node = PhysicalGroupedLimit(
        input=StaticSource([batch_of(k=[1, 1, 2, 2, 3], v=[10, 11, 20, 21, 30])]),
        keys=[col("k")],
        limit=1,
    )
    assert rows_of(node) == [
        {"k": 1, "v": 10},
        {"k": 2, "v": 20},
        {"k": 3, "v": 30},
    ]


def _merge_engine():
    """An in-memory merge engine for exercising the DuckDB push path."""
    from federated_query.executor.merge_engine import MergeEngine

    return MergeEngine("1GB", None)


def test_grouped_limit_merge_path_matches_python():
    """The merge-engine ROW_NUMBER push keeps the same rows and order as Python."""
    rows = batch_of(k=[1, 1, 1, 2, 2, 3], v=[10, 11, 12, 20, 21, 30])
    python_node = PhysicalGroupedLimit(
        input=StaticSource([rows]), keys=[col("k")], limit=2
    )
    merge_node = PhysicalGroupedLimit(
        input=StaticSource([rows]), keys=[col("k")], limit=2
    )
    merge_node.set_merge_engine(_merge_engine())
    assert rows_of(merge_node) == rows_of(python_node)
    assert rows_of(merge_node) == [
        {"k": 1, "v": 10},
        {"k": 1, "v": 11},
        {"k": 2, "v": 20},
        {"k": 2, "v": 21},
        {"k": 3, "v": 30},
    ]


def test_grouped_limit_merge_path_ordered_latest_per_key():
    """An ordered per-key limit (latest row) matches between merge and Python."""
    rows = batch_of(k=[1, 1, 1, 2, 2], v=[10, 12, 11, 20, 21])
    fields = dict(
        keys=[col("k")],
        limit=1,
        order_by_keys=[col("v")],
        order_by_ascending=[False],
    )
    python_node = PhysicalGroupedLimit(input=StaticSource([rows]), **fields)
    merge_node = PhysicalGroupedLimit(input=StaticSource([rows]), **fields)
    merge_node.set_merge_engine(_merge_engine())
    assert rows_of(merge_node) == rows_of(python_node)
    assert rows_of(merge_node) == [{"k": 1, "v": 12}, {"k": 2, "v": 21}]


def test_hash_semi_join_null_keys_never_match():
    """A NULL key must not match a NULL build key (SQL equality)."""
    left = StaticSource([batch_of(id=[1, None, 3])])
    right = StaticSource([batch_of(ref=[1, None])])
    node = PhysicalHashJoin(
        left=left,
        right=right,
        join_type=JoinType.SEMI,
        left_keys=[col("id")],
        right_keys=[col("ref")],
        build_side="right",
    )
    assert rows_of(node) == [{"id": 1}]


def test_hash_anti_join_null_keys_kept():
    """ANTI join keeps NULL-key rows: NULL never equals anything."""
    left = StaticSource([batch_of(id=[1, None, 3])])
    right = StaticSource([batch_of(ref=[1, None])])
    node = PhysicalHashJoin(
        left=left,
        right=right,
        join_type=JoinType.ANTI,
        left_keys=[col("id")],
        right_keys=[col("ref")],
        build_side="right",
    )
    assert rows_of(node) == [{"id": None}, {"id": 3}]


def test_left_hash_join_null_probe_key_emits_outer_row():
    """LEFT join with a NULL probe key produces the NULL-padded row."""
    left = StaticSource([batch_of(id=[1, None])])
    right = StaticSource([batch_of(ref=[1], payload=["a"])])
    node = PhysicalHashJoin(
        left=left,
        right=right,
        join_type=JoinType.LEFT,
        left_keys=[col("id")],
        right_keys=[col("ref")],
        build_side="right",
    )
    assert rows_of(node) == [
        {"id": 1, "ref": 1, "payload": "a"},
        {"id": None, "ref": None, "payload": None},
    ]


def test_global_aggregate_over_empty_input_emits_one_row():
    """SQL: SELECT COUNT(*), MAX(v) over no rows is one row (0, NULL)."""
    empty = StaticSource([])
    count_star = FunctionCall(function_name="COUNT", args=[], is_aggregate=True)
    max_v = FunctionCall(function_name="MAX", args=[col("v")], is_aggregate=True)
    node = PhysicalHashAggregate(
        input=empty,
        group_by=[],
        aggregates=[count_star, max_v],
        output_names=["cnt", "max_v"],
    )
    assert rows_of(node) == [{"cnt": 0, "max_v": None}]


def test_sum_over_empty_input_is_null():
    """SQL: SUM over no rows is NULL, not 0."""
    empty = StaticSource([])
    sum_v = FunctionCall(function_name="SUM", args=[col("v")], is_aggregate=True)
    node = PhysicalHashAggregate(
        input=empty, group_by=[], aggregates=[sum_v], output_names=["total"]
    )
    assert rows_of(node) == [{"total": None}]


def test_count_column_skips_nulls():
    """SQL: COUNT(col) counts only non-NULL values."""
    source = StaticSource([batch_of(v=[1, None, 3])])
    count_v = FunctionCall(function_name="COUNT", args=[col("v")], is_aggregate=True)
    node = PhysicalHashAggregate(
        input=source, group_by=[], aggregates=[count_v], output_names=["cnt"]
    )
    assert rows_of(node) == [{"cnt": 2}]
