"""The optional per-query profiler (executor/profiling.py).

It is gated behind FEDQ_PROFILE and not touched by the e2e suites, so its
self-time arithmetic (a node's time minus its children's) and stage timing are
pinned here directly.
"""

import time

import pyarrow as pa

from federated_query.executor.profiling import QueryProfiler, profiling_enabled


class _Node:
    """A fake operator that sleeps a fixed time, then pulls its children."""

    def __init__(self, label, children, delay_s, rows):
        """Hold the label, child nodes, own delay, and number of rows to emit."""
        self._label = label
        self._children = children
        self._delay = delay_s
        self._rows = rows

    def children(self):
        """Return the child nodes."""
        return self._children

    def __repr__(self):
        """Identify the node in the profile report."""
        return self._label

    def execute(self):
        """Drain children, sleep for this node's own work, then emit rows."""
        for child in self._children:
            list(child.execute())
        time.sleep(self._delay)
        for _ in range(self._rows):
            yield pa.record_batch({"x": pa.array([1])})


def test_profiling_enabled_env(monkeypatch):
    """profiling_enabled() honours the FEDQ_PROFILE environment variable."""
    monkeypatch.delenv("FEDQ_PROFILE", raising=False)
    assert profiling_enabled() is False
    monkeypatch.setenv("FEDQ_PROFILE", "1")
    assert profiling_enabled() is True
    monkeypatch.setenv("FEDQ_PROFILE", "0")
    assert profiling_enabled() is False


def test_self_time_excludes_children():
    """A node's self-time is its total minus the time spent in its children."""
    scan_a = _Node("ScanA", [], 0.02, 1)
    scan_b = _Node("ScanB", [], 0.05, 1)
    join = _Node("Join", [scan_a, scan_b], 0.01, 1)
    root = _Node("Agg", [join], 0.005, 1)

    profiler = QueryProfiler()
    profiler.instrument(root)
    list(root.execute())

    join_id = id(join)
    join_self = profiler._self_seconds(join_id)
    join_total = profiler._node_seconds[join_id]
    # Join's own work is ~10ms; its total includes the ~70ms of child scans.
    assert 0.005 < join_self < 0.04
    assert join_total > join_self + 0.05


def test_report_lists_stages_and_operators():
    """The rendered report contains the stage and operator sections."""
    root = _Node("Agg", [_Node("Scan", [], 0.001, 1)], 0.001, 1)
    profiler = QueryProfiler()
    with profiler.stage("plan"):
        time.sleep(0.001)
    profiler.instrument(root)
    list(root.execute())

    report = profiler.report()
    assert "stages:" in report
    assert "plan" in report
    assert "operators (self / total ms)" in report
    assert "Agg" in report and "Scan" in report
