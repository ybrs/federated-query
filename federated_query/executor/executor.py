"""Query executor."""

import os
from typing import Iterator, Optional, TYPE_CHECKING
import pyarrow as pa
from ..plan.physical import PhysicalPlanNode, PhysicalExplain
from ..config.config import ExecutorConfig
from .merge_engine import MergeEngine

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


def _duckdb_merge_forced() -> bool:
    """The Rust engine is the default execution path. The DuckDB merge engine is
    kept behind ``FEDQ_ENGINE=duckdb`` for comparison, and is still the fallback
    for a plan the Rust engine does not yet cover (e.g. cross-source lateral)."""
    return os.environ.get("FEDQ_ENGINE", "rust").lower() == "duckdb"


def _attach_merge_engine(node: PhysicalPlanNode, engine: MergeEngine) -> None:
    """Attach the executor's merge engine to every node in the plan tree."""
    node.set_merge_engine(engine)
    for child in node.children():
        _attach_merge_engine(child, engine)


class Executor:
    """Query executor that runs physical plans."""

    def __init__(self, catalog=None, config: Optional[ExecutorConfig] = None):
        """Initialize executor.

        Args:
            catalog: Optional catalog, used by callers that plan-then-execute
                through this executor (the decorrelation test harness). Production
                plans through QueryExecutor's own planner and leaves this None.
            config: Executor configuration (uses defaults if not provided).
        """
        if config is None:
            # Default executor tuning when the caller supplies none, so every
            # downstream operator sees a concrete config rather than None.
            config = ExecutorConfig.create()
        self.config = config
        self.catalog = catalog
        # The merge engine is created once on first use and reused across every
        # query this executor runs: opening a fresh in-memory DuckDB costs ~10ms,
        # so a per-query connection would dwarf the local join it accelerates.
        # Per-operator cursors keep concurrent statements isolated on it.
        self._merge_engine: Optional[MergeEngine] = None

    def execute(
        self,
        plan: PhysicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a physical plan.

        Args:
            plan: Physical plan to execute

        Yields:
            Arrow record batches with results
        """
        _attach_merge_engine(plan, self._get_merge_engine())
        yield from plan.execute()

    def _get_merge_engine(self) -> MergeEngine:
        """Return the reused merge engine, creating it once on first use."""
        if self._merge_engine is None:
            self._merge_engine = MergeEngine(
                self.config.merge_engine_memory_limit,
                self.config.merge_engine_temp_directory,
            )
        return self._merge_engine

    def warmup(self) -> None:
        """Create and warm the merge engine so the first query is not slowed.

        Intended for long-lived runtimes (e.g. the CLI session) to fold DuckDB's
        one-time setup cost into startup. Short-lived callers (such as tests) can
        skip it and let the engine be created lazily on first use.
        """
        self._get_merge_engine().warmup()

    def close(self) -> None:
        """Close the reused merge engine, if one was created."""
        if self._merge_engine is not None:
            self._merge_engine.close()
            self._merge_engine = None

    def execute_to_table(
        self,
        plan: PhysicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> pa.Table:
        """Execute a physical plan and materialize as Arrow table.

        Args:
            plan: Physical plan to execute

        Returns:
            Arrow table with all results
        """
        table = self._maybe_execute_via_rust(plan, query_executor)
        if table is not None:
            return table
        batches = list(self.execute(plan, query_executor=query_executor))
        if not batches:
            # Return empty table with schema
            return pa.Table.from_batches([], schema=plan.schema())
        return pa.Table.from_batches(batches)

    def _maybe_execute_via_rust(
        self, plan: PhysicalPlanNode, query_executor: Optional["QueryExecutor"]
    ) -> Optional[pa.Table]:
        """Run the plan on the Rust engine (the default), or return None to fall
        back to the DuckDB merge engine. Falls back when the merge engine is
        forced, for EXPLAIN (plan introspection, not data), when no datasources
        are available, or for a plan operator the Rust engine does not yet cover
        (UnsupportedIR); any other Rust error propagates."""
        if _duckdb_merge_forced() or isinstance(plan, PhysicalExplain):
            return None
        datasources = self._routable_datasources(query_executor)
        if datasources is None:
            return None
        from .rust_ir import execute_via_rust, UnsupportedIR

        # Some nodes (window) infer their exact output types by running SQL on the
        # merge engine at plan time; attach it so serialization can read schemas.
        # Execution still happens entirely in Rust.
        _attach_merge_engine(plan, self._get_merge_engine())
        try:
            return execute_via_rust(plan, datasources)
        except UnsupportedIR:
            return None

    def _routable_datasources(self, query_executor: Optional["QueryExecutor"]):
        """The datasources the Rust engine reads, or None when unavailable."""
        catalog = getattr(query_executor, "catalog", None)
        datasources = getattr(catalog, "datasources", None)
        return list(datasources.values()) if datasources else None

    def __repr__(self) -> str:
        return "Executor()"
