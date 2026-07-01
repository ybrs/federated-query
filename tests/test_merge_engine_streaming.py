"""The DuckDB merge engine must STREAM the probe side, never materialize it.

A cross-source hash join feeds its probe (often the big, remote side) to the
coordinator DuckDB. We hand DuckDB a streaming ``pyarrow.RecordBatchReader`` so
it pulls batches lazily; we must NOT drain the probe into a ``pa.Table`` in our
own code first. These tests pin that contract.
"""

import pyarrow as pa

from federated_query.plan.physical import (
    PhysicalHashJoin,
    PhysicalPlanNode,
    _MERGE_LEFT_RELATION,
    _MERGE_RIGHT_RELATION,
)
from federated_query.plan.logical import JoinType


class _CountingNode(PhysicalPlanNode):
    """A minimal physical node whose batch production is observable."""

    out_schema: pa.Schema
    batch_count: int
    pulled: int = 0

    def children(self):
        """A leaf node has no children."""
        return []

    def schema(self) -> pa.Schema:
        """Return the fixed output schema."""
        return self.out_schema

    def column_aliases(self):
        """No qualified-name remapping for this fake node."""
        return {}

    def execute(self):
        """Yield one-row batches, counting each pull as it happens."""
        for value in range(self.batch_count):
            self.pulled += 1
            yield pa.record_batch(
                {self.out_schema.names[0]: pa.array([value], self.out_schema.types[0])}
            )

    def estimated_cost(self) -> float:
        """Free; this is a test fixture."""
        return 0.0


def _make_join(build_node, probe_node) -> PhysicalHashJoin:
    """Build an INNER hash join whose left is built and right is probed."""
    return PhysicalHashJoin(
        left=build_node,
        right=probe_node,
        join_type=JoinType.INNER,
        left_keys=[],
        right_keys=[],
        build_side="left",
    )


def test_probe_input_is_a_streaming_reader_not_a_table():
    """The probe must be registered as a streaming reader, not a pa.Table."""
    schema = pa.schema([("file_id", pa.int64())])
    build = _CountingNode(out_schema=schema, batch_count=1)
    probe = _CountingNode(out_schema=schema, batch_count=5)
    join = _make_join(build, probe)

    inputs = join._merge_inputs(list(build.execute()))
    probe_input = inputs[_MERGE_RIGHT_RELATION]

    assert isinstance(probe_input, pa.RecordBatchReader)
    assert not isinstance(probe_input, pa.Table)


def test_building_inputs_does_not_drain_the_probe():
    """Constructing the merge inputs must not fully consume the probe stream."""
    schema = pa.schema([("file_id", pa.int64())])
    build = _CountingNode(out_schema=schema, batch_count=1)
    probe = _CountingNode(out_schema=schema, batch_count=5)
    join = _make_join(build, probe)

    inputs = join._merge_inputs(list(build.execute()))

    # The probe has 5 batches; building inputs must not have pulled them all.
    assert probe.pulled < 5
    # And the reader still yields every batch when DuckDB actually pulls it.
    drained = list(inputs[_MERGE_RIGHT_RELATION])
    assert len(drained) == 5
    assert probe.pulled == 5
