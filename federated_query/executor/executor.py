"""Query executor."""

from typing import Iterator, List, Optional, TYPE_CHECKING
import pyarrow as pa
from ..plan.physical import PhysicalPlanNode, PhysicalExplain
from ..config.config import ExecutorConfig

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


class Executor:
    """Query executor that runs physical plans on the Rust engine.

    The Rust engine (fedqrs) is the one and only execution path. Physical plans
    are serialized to its IR and run natively; a plan the engine cannot
    represent raises UnsupportedIR, which propagates (there is no fallback and no
    silent wrong answer). EXPLAIN is plan introspection and renders directly.
    """

    def __init__(self, catalog=None, config: Optional[ExecutorConfig] = None):
        """Initialize executor.

        Args:
            catalog: Optional catalog whose datasources the Rust engine reads
                when a plan is executed through this executor directly (the
                decorrelation/e2e test harnesses build ``Executor(catalog)``).
            config: Executor configuration (uses defaults if not provided).
        """
        if config is None:
            # Default executor tuning when the caller supplies none, so every
            # downstream operator sees a concrete config rather than None.
            config = ExecutorConfig.create()
        self.config = config
        self.catalog = catalog

    def execute(
        self,
        plan: PhysicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a physical plan on the Rust engine, yielding result batches."""
        table = self.execute_to_table(plan, query_executor=query_executor)
        yield from table.to_batches()

    def execute_to_table(
        self,
        plan: PhysicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> pa.Table:
        """Execute a physical plan and materialize it as an Arrow table.

        EXPLAIN renders directly (plan introspection, not data). Every other plan
        runs in Rust; an UnsupportedIR raised there propagates.
        """
        if isinstance(plan, PhysicalExplain):
            return self._explain_to_table(plan)
        return self._execute_via_rust(plan, query_executor)

    def _explain_to_table(self, plan: PhysicalExplain) -> pa.Table:
        """Render an EXPLAIN plan directly to a single-column Arrow table."""
        batch = plan.build_explain_batch()
        return pa.Table.from_batches([batch])

    def _execute_via_rust(
        self, plan: PhysicalPlanNode, query_executor: Optional["QueryExecutor"]
    ) -> pa.Table:
        """Run the plan on the Rust engine over its resolved datasources."""
        datasources = self._resolve_datasources(plan, query_executor)
        if not datasources:
            raise RuntimeError("no datasources available to execute the plan")
        from .rust_ir import execute_via_rust

        return execute_via_rust(plan, datasources)

    def _resolve_datasources(
        self, plan: PhysicalPlanNode, query_executor: Optional["QueryExecutor"]
    ) -> List[object]:
        """The datasources the Rust engine reads for this plan.

        Prefers the executing query executor's catalog, then this executor's own
        catalog, and finally the datasource connections carried by the plan's
        scans (so a bare ``Executor().execute(plan)`` still routes to Rust).
        """
        from_catalog = self._catalog_datasources(query_executor)
        if from_catalog:
            return from_catalog
        return _datasources_from_plan(plan)

    def _catalog_datasources(
        self, query_executor: Optional["QueryExecutor"]
    ) -> List[object]:
        """Datasources from the query executor's catalog, or this executor's."""
        catalog = getattr(query_executor, "catalog", None) or self.catalog
        datasources = getattr(catalog, "datasources", None)
        if datasources:
            return list(datasources.values())
        return []

    def __repr__(self) -> str:
        return "Executor()"


def _datasources_from_plan(plan: PhysicalPlanNode) -> List[object]:
    """Collect the distinct datasource connections carried by a plan's scans."""
    found: List[object] = []
    seen = set()
    _collect_datasources(plan, found, seen)
    return found


def _collect_datasources(node: PhysicalPlanNode, found: List[object], seen: set) -> None:
    """Walk the plan tree, appending each new datasource connection once."""
    connection = getattr(node, "datasource_connection", None)
    if connection is not None and id(connection) not in seen:
        seen.add(id(connection))
        found.append(connection)
    for child in node.children():
        _collect_datasources(child, found, seen)
