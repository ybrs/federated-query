import pyarrow as pa
import pytest

from federated_query.plan.physical import (
    PhysicalHashAggregate,
    PhysicalPlanNode,
    PhysicalProject,
)
from federated_query.plan.expressions import ColumnRef, FunctionCall


class DummyPlanNode(PhysicalPlanNode):
    """Simple physical node for testing."""

    def __init__(self, batches, schema=None):
        self._batches = batches
        if schema is not None:
            self._schema = schema
        elif batches:
            self._schema = batches[0].schema
        else:
            fields = []
            self._schema = pa.schema(fields)

    def children(self):
        nodes = []
        return nodes

    def execute(self):
        for batch in self._batches:
            yield batch

    def schema(self):
        return self._schema

    def estimated_cost(self):
        return 0.0


def test_project_projects_named_columns_without_arrow_error():
    # Regression guard for the projection bug documented in review.md. The
    # current PhysicalProject implementation attempts to access Arrow arrays by
    # numeric index even when columns are addressed by name, which crashes when
    # the execution engine requests a named column that is not at index 0. This
    # test ensures that a simple single-column projection keeps working once the
    # bug is fixed.
    arrays = []
    arrays.append(pa.array([1, 2]))
    batch = pa.RecordBatch.from_arrays(arrays, names=["customer_id"])
    child = DummyPlanNode([batch])
    column_ref = ColumnRef(None, "customer_id")
    project = PhysicalProject(child, [column_ref], ["customer_id"])
    produced_batches = []
    for produced in project.execute():
        produced_batches.append(produced)
    assert len(produced_batches) == 1
    first_batch = produced_batches[0]
    assert first_batch.num_rows == 2
    assert first_batch.column(0)[0].as_py() == 1
    assert first_batch.column(0)[1].as_py() == 2


def test_hash_aggregate_respects_select_list_order():
    # Regression guard for the hash aggregate output ordering bug. review.md
    # highlights that the physical operator currently reorders aggregate outputs
    # when producing batches, which breaks callers that rely on the SELECT list
    # ordering. This test locks down the desired column ordering for COUNT(*)
    # followed by the grouped column.
    arrays = []
    arrays.append(pa.array([7, 7, 8], type=pa.int64()))
    batch = pa.RecordBatch.from_arrays(arrays, names=["customer_id"])
    child = DummyPlanNode([batch])
    group_by = []
    group_by.append(ColumnRef(None, "customer_id"))
    count_arg = ColumnRef(None, "*")
    aggregates = []
    aggregates.append(FunctionCall("COUNT", [count_arg], is_aggregate=True))
    aggregates.append(ColumnRef(None, "customer_id"))
    output_names = ["order_count", "customer_id"]
    aggregate = PhysicalHashAggregate(child, group_by, aggregates, output_names)
    iterator = aggregate.execute()
    first_batch = next(iterator)
    count_values = first_batch.column(0).to_pylist()
    assert count_values == [2, 1]
    customer_values = first_batch.column(1).to_pylist()
    assert customer_values == [7, 8]


def test_hash_aggregate_returns_zero_row_for_empty_input():
    # Regression guard for the empty-input aggregation bug. As described in
    # review.md, the PhysicalHashAggregate operator currently yields no rows for
    # empty child iterators, causing COUNT(*) queries to return an empty result
    # instead of a zero row. This test captures the expected zero-count output
    # once the operator handles empty input correctly.
    batches = []
    fields = []
    fields.append(pa.field("value", pa.int64()))
    schema = pa.schema(fields)
    child = DummyPlanNode(batches, schema=schema)
    count_arg = ColumnRef(None, "*")
    aggregates = []
    aggregates.append(FunctionCall("COUNT", [count_arg], is_aggregate=True))
    output_names = ["total_rows"]
    aggregate = PhysicalHashAggregate(child, [], aggregates, output_names)
    iterator = aggregate.execute()
    first_batch = next(iterator)
    assert first_batch.num_columns == 1
    assert first_batch.column(0)[0].as_py() == 0
