"""In-memory DuckDB coordinator that merges the Arrow streams of a query.

The local physical operators (joins, aggregates, sorts, set operations) hand
their child Arrow streams to this engine instead of combining them row at a
time in Python. DuckDB is a vectorized, multi-threaded, out-of-core engine with
correct SQL semantics, and it consumes and returns Arrow streams lazily.

One ``MergeEngine`` is created on first use by an :class:`Executor` and reused
across every query that executor runs (opening a fresh in-memory DuckDB costs
~10ms, so a per-query connection would dwarf the local join it accelerates).
Every local operator in a plan runs its own small SQL statement over its
registered Arrow inputs on an isolated cursor, so reuse stays safe.
"""

from contextlib import contextmanager
from typing import Dict, Iterator, Optional

import duckdb
import pyarrow as pa


class MergeEngine:
    """Vectorized local execution engine backed by an in-memory DuckDB."""

    def __init__(self, memory_limit: str, temp_directory: Optional[str]):
        """Open the in-memory coordinator and apply its spill/memory settings."""
        self._connection = duckdb.connect(":memory:")
        self._connection.execute(f"SET memory_limit='{memory_limit}'")
        if temp_directory is not None:
            self._connection.execute(f"SET temp_directory='{temp_directory}'")

    def run(self, sql: str, inputs: Dict[str, object]) -> Iterator[pa.RecordBatch]:
        """Register the named Arrow inputs, stream the SQL result, then clean up.

        A fresh cursor isolates this statement's query state, so a child
        operator pulled lazily while this query runs (it registers its own
        inputs and executes its own SQL on another cursor) never clashes with
        this one. The cursor is closed deterministically even when the consumer
        stops reading early, so no DuckDB query stays active with live worker
        threads.
        """
        cursor = self._connection.cursor()
        try:
            yield from self._stream(cursor, sql, inputs)
        finally:
            cursor.close()

    def _stream(
        self, cursor, sql: str, inputs: Dict[str, object]
    ) -> Iterator[pa.RecordBatch]:
        """Register inputs on the cursor and yield the result batches lazily."""
        for name, arrow_input in inputs.items():
            cursor.register(name, arrow_input)
        reader = cursor.execute(sql).to_arrow_reader()
        for batch in reader:
            yield batch

    def schema(self, sql: str, inputs: Dict[str, object]) -> pa.Schema:
        """Return a query's result schema without fetching its rows.

        The Arrow reader exposes its schema before any batch is pulled, so an
        unlike reading the first batch, which an empty result never produces.
        """
        cursor = self._connection.cursor()
        try:
            for name, arrow_input in inputs.items():
                cursor.register(name, arrow_input)
            return cursor.execute(sql).to_arrow_reader().schema
        finally:
            cursor.close()

    @contextmanager
    def join_session(self):
        """A single-cursor session for a join that buffers one side in DuckDB.

        A hash join must read all of one side (the build) up front, and cross-
        source dynamic filtering needs that side's DISTINCT keys before the other
        side is fetched. Rather than draining the build into a Python Arrow table
        (unbounded, unspillable), the session streams it into a DuckDB temp table
        - which DuckDB owns and can spill - then computes DISTINCT and runs the
        join over it. Temp tables are connection-local, so the build table, its
        DISTINCT scan, and the join all run on this one cursor, which stays open
        while the result reader is consumed and drops its temps on exit.
        """
        cursor = self._connection.cursor()
        session = _MergeSession(cursor)
        try:
            yield session
        finally:
            session.close()

    def warmup(self) -> None:
        """Run a trivial join so the first real query pays no DuckDB setup cost.

        The first DuckDB statement in a process spins up the thread pool and
        compiles the join operator (tens of ms). Calling this at session start
        folds that one-time cost into startup instead of the user's first query.
        """
        one = pa.table({"k": pa.array([1])})
        list(
            self.run(
                "SELECT a.k FROM warm_a AS a JOIN warm_b AS b ON a.k = b.k",
                {"warm_a": one, "warm_b": one},
            )
        )

    def close(self) -> None:
        """Close the coordinator connection and release its resources."""
        self._connection.close()


class _MergeSession:
    """One cursor's worth of join state: buffered build tables plus streaming.

    Created by MergeEngine.join_session. Everything runs on the single cursor so
    a connection-local temp table is visible to the DISTINCT scan and the join.
    """

    def __init__(self, cursor):
        """Capture the cursor and track temp tables to drop on close."""
        self._cursor = cursor
        self._temps = []

    def materialize(self, name: str, reader: pa.RecordBatchReader) -> None:
        """Stream an Arrow reader into a DuckDB temp table (DuckDB-owned, spillable).

        DuckDB pulls the reader through the C data interface (no per-batch Python
        loop) and holds the rows in its buffer manager, which spills under the
        memory limit - unlike a Python-side Arrow table.
        """
        source = f"{name}_src"
        self._cursor.register(source, reader)
        self._cursor.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM {source}")
        self._cursor.unregister(source)
        self._temps.append(name)

    def fetch(self, sql: str):
        """Run a query on this cursor and return all rows (small result sets)."""
        return self._cursor.execute(sql).fetchall()

    def stream(
        self, sql: str, inputs: Dict[str, object]
    ) -> Iterator[pa.RecordBatch]:
        """Register streaming inputs and yield the query's result batches lazily."""
        for name, arrow_input in inputs.items():
            self._cursor.register(name, arrow_input)
        reader = self._cursor.execute(sql).to_arrow_reader()
        for batch in reader:
            yield batch

    def close(self) -> None:
        """Drop the session's temp tables and close its cursor."""
        for name in self._temps:
            self._cursor.execute(f"DROP TABLE IF EXISTS {name}")
        self._cursor.close()
