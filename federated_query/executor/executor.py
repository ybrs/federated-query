"""Query executor."""

from typing import Iterator, Optional, TYPE_CHECKING
import pyarrow as pa
from ..plan.physical import PhysicalPlanNode
from ..config.config import ExecutorConfig
from .merge_engine import MergeEngine

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


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
            config = ExecutorConfig()
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
        batches = list(self.execute(plan, query_executor=query_executor))
        if not batches:
            # Return empty table with schema
            return pa.Table.from_batches([], schema=plan.schema())
        return pa.Table.from_batches(batches)

    def __repr__(self) -> str:
        return "Executor()"
