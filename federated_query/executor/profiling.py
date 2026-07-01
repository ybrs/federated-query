"""Optional per-query profiling for the execution pipeline.

Enabled by setting the ``FEDQ_PROFILE`` environment variable. When on, the
:class:`QueryExecutor` records how long each planning stage takes and, during
that operator's own ``execute()`` minus the time it spends pulling its children.
A remote scan's self-time is therefore its fetch cost, and a hash join's
self-time is its local merge cost, which is exactly what is needed to tell a
fetch-bound query apart from a compute-bound one.
"""

import os
import time
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional

import pyarrow as pa

from ..plan.physical import PhysicalPlanNode


def profiling_enabled() -> bool:
    """Whether per-query profiling is switched on via ``FEDQ_PROFILE``."""
    return os.environ.get("FEDQ_PROFILE", "") not in ("", "0", "false", "False")


@contextmanager
def stage_timer(profiler: "Optional[QueryProfiler]", name: str):
    """Time a named pipeline stage, doing nothing when profiling is off."""
    if profiler is None:
        yield
        return
    with profiler.stage(name):
        yield


class QueryProfiler:
    """Collects pipeline-stage and per-operator timings for one query."""

    def __init__(self):
        """Initialize empty stage and operator timing records."""
        self._stages: List[tuple] = []
        self._node_seconds: Dict[int, float] = {}
        self._node_label: Dict[int, str] = {}
        self._node_children: Dict[int, List[int]] = {}
        self._root: Optional[int] = None

    @contextmanager
    def stage(self, name: str):
        """Record the wall-clock duration of a named planning stage."""
        started = time.perf_counter()
        try:
            yield
        finally:
            self._stages.append((name, time.perf_counter() - started))

    def instrument(self, plan: PhysicalPlanNode) -> None:
        """Wrap every operator in the plan so its execution time is recorded."""
        self._root = id(plan)
        self._wrap_node(plan)

    def _wrap_node(self, node: PhysicalPlanNode) -> None:
        """Replace one node's execute() with a timing wrapper, then recurse."""
        node_id = id(node)
        if node_id in self._node_seconds:
            return
        self._node_seconds[node_id] = 0.0
        self._node_label[node_id] = repr(node)
        children = node.children()
        self._node_children[node_id] = self._child_ids(children)
        node.execute = self._timed_execute(node_id, node.execute)
        for child in children:
            self._wrap_node(child)

    def _child_ids(self, children: List[PhysicalPlanNode]) -> List[int]:
        """Return the identity of each child node."""
        ids = []
        for child in children:
            ids.append(id(child))
        return ids

    def _timed_execute(self, node_id: int, original):
        """Build a generator that accumulates time spent producing batches."""

        def timed() -> Iterator[pa.RecordBatch]:
            # Time each resume of the underlying generator: the span from asking
            # for a batch to receiving it is the work this operator (and the
            # children it pulls) did for that batch.
            source = original()
            while True:
                started = time.perf_counter()
                batch = self._next_batch(source)
                self._node_seconds[node_id] += time.perf_counter() - started
                if batch is None:
                    return
                yield batch

        return timed

    def _next_batch(self, source: Iterator[pa.RecordBatch]):
        """Pull the next batch from a generator, or None when it is exhausted."""
        try:
            return next(source)
        except StopIteration:
            return None

    def _self_seconds(self, node_id: int) -> float:
        """Operator self-time: total time minus the time spent in its children."""
        total = self._node_seconds[node_id]
        for child_id in self._node_children[node_id]:
            total -= self._node_seconds[child_id]
        return total

    def report(self) -> str:
        """Render the collected stage and operator timings as text."""
        lines = ["-- profile --", "stages:"]
        self._append_stage_lines(lines)
        lines.append("operators (self / total ms):")
        if self._root is not None:
            self._append_node_lines(lines, self._root, 0)
        return "\n".join(lines)

    def _append_stage_lines(self, lines: List[str]) -> None:
        """Append one line per planning stage plus a total."""
        total = 0.0
        for name, seconds in self._stages:
            total += seconds
            lines.append(f"  {name:<12} {seconds * 1000:8.2f} ms")
        lines.append(f"  {'total':<12} {total * 1000:8.2f} ms")

    def _append_node_lines(self, lines: List[str], node_id: int, depth: int) -> None:
        """Append the operator subtree rooted at node_id, indented by depth."""
        indent = "  " * (depth + 1)
        self_ms = self._self_seconds(node_id) * 1000
        total_ms = self._node_seconds[node_id] * 1000
        label = self._node_label[node_id]
        lines.append(f"{indent}{self_ms:7.2f} / {total_ms:7.2f}  {label}")
        for child_id in self._node_children[node_id]:
            self._append_node_lines(lines, child_id, depth + 1)
