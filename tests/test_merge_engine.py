"""The MergeEngine coordinator itself (executor/merge_engine.py).

Covers the properties the operator tests rely on: a per-call cursor so a child
run() pulled while a parent run() is mid-query does not clash (the reentrancy
that would otherwise deadlock), warmup, and empty inputs.
"""

import pyarrow as pa

from federated_query.executor.merge_engine import MergeEngine

_JOIN_SQL = (
    'SELECT l."id" AS "id" FROM in_left AS l '
    'INNER JOIN in_right AS r ON l."id" = r."id"'
)


def _collect(iterator):
    """Materialise a result iterator's rows as a sorted list of the id column."""
    ids = []
    for batch in iterator:
        ids.extend(batch.to_pydict()["id"])
    return sorted(ids)


def test_run_simple_join():
    """A registered pair of tables joins and streams back."""
    eng = MergeEngine("256MB", None)
    left = pa.table({"id": pa.array([1, 2, 3])})
    right = pa.table({"id": pa.array([2, 3, 4])})
    assert _collect(eng.run(_JOIN_SQL, {"in_left": left, "in_right": right})) == [2, 3]
    eng.close()


def test_nested_run_uses_isolated_cursors():
    """A run() whose probe is itself fed by another run() must not clash.

    The inner run() executes on its own cursor while the outer run() is mid
    query; if cursors were shared this would corrupt the outer query's state.
    """
    eng = MergeEngine("256MB", None)

    def inner_stream():
        inner_left = pa.table({"id": pa.array([1, 2, 3, 4])})
        inner_right = pa.table({"id": pa.array([2, 3])})
        return eng.run(_JOIN_SQL, {"in_left": inner_left, "in_right": inner_right})

    probe = pa.RecordBatchReader.from_batches(
        pa.schema([("id", pa.int64())]), inner_stream()
    )
    build = pa.table({"id": pa.array([3])})
    outer = eng.run(_JOIN_SQL, {"in_left": build, "in_right": probe})
    assert _collect(outer) == [3]
    eng.close()


def test_empty_input_yields_no_rows():
    """An empty registered input produces an empty result, not an error."""
    eng = MergeEngine("256MB", None)
    empty = pa.schema([("id", pa.int64())]).empty_table()
    right = pa.table({"id": pa.array([1, 2])})
    assert _collect(eng.run(_JOIN_SQL, {"in_left": empty, "in_right": right})) == []
    eng.close()


def test_warmup_runs_cleanly():
    """warmup() executes a trivial join without error and leaves the engine usable."""
    eng = MergeEngine("256MB", None)
    eng.warmup()
    left = pa.table({"id": pa.array([5])})
    assert _collect(eng.run(_JOIN_SQL, {"in_left": left, "in_right": left})) == [5]
    eng.close()
