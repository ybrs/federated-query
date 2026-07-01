"""The one build-key extractor raises on an unsupported key, never returns [].

Both the hash join and the EXPLAIN dynamic-filter collector pull build-key
arrays through _key_column_arrays. A non-ColumnRef key is unsupported and must
raise (loud), not silently yield no keys - which would render an empty dynamic
filter as if the build side had no values.
"""

import pyarrow as pa
import pytest

from federated_query.plan.physical import _key_column_arrays
from federated_query.plan.expressions import ColumnRef, Literal, DataType


def test_key_column_arrays_resolves_bare_column():
    """A bare column key reads its own column from the batch."""
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["id"])
    arrays = _key_column_arrays(batch, [ColumnRef(table=None, column="id")], None)
    assert arrays[0].to_pylist() == [1, 2]


def test_key_column_arrays_resolves_qualified_column_via_aliases():
    """A qualified key reads the renamed column its alias map points to."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1]), pa.array([2])], names=["id", "right_id"]
    )
    aliases = {("t1", "id"): "id", ("t2", "id"): "right_id"}
    arrays = _key_column_arrays(batch, [ColumnRef(table="t2", column="id")], aliases)
    assert arrays[0].to_pylist() == [2]


def test_key_column_arrays_raises_on_non_column_key():
    """A non-ColumnRef key raises instead of silently returning no arrays."""
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], names=["id"])
    bad_key = Literal(value=1, data_type=DataType.INTEGER)
    with pytest.raises(NotImplementedError):
        _key_column_arrays(batch, [bad_key], None)
