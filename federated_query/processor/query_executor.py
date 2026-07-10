"""QueryExecutor orchestrates middleware and the query pipeline."""

from __future__ import annotations

from typing import List, Optional, Union, Protocol

import pyarrow as pa

from ..catalog import Catalog
from ..executor import Executor
from ..executor.plan_cache import PlanCache, plan_cache_from_env
from ..executor.profiling import QueryProfiler, profiling_enabled, stage_timer
from ..optimizer import PhysicalPlanner, RuleBasedOptimizer
from ..optimizer.decorrelation import Decorrelator
from ..parser import Binder, Parser
from ..plan import PhysicalExplain, ExplainFormat
from ..plan.logical import LogicalPlanNode
from ..plan.physical import PhysicalPlanNode
from .query_context import QueryContext


class QueryProcessor(Protocol):
    """Processor interface for both pre and post execution phases."""

    def before_execution(self, executor: "QueryExecutor") -> None:
        """Run before planning/execution to mutate the query."""
        ...

    def after_execution(
        self, executor: "QueryExecutor", result: Union[pa.Table, dict]
    ) -> Union[pa.Table, dict]:
        """Run after execution to transform the result."""
        ...


class QueryExecutor:
    """Coordinates processors with parser, binder, optimizer, and executor."""

    def __init__(
        self,
        catalog: Catalog,
        parser: Parser,
        binder: Binder,
        optimizer: RuleBasedOptimizer,
        planner: PhysicalPlanner,
        physical_executor: Executor,
        processors: Optional[List[QueryProcessor]] = None,
        decorrelator: Optional[Decorrelator] = None,
        plan_cache: Optional[PlanCache] = None,
    ):
        """Initialize dependencies."""
        self.catalog = catalog
        self.parser = parser
        self.binder = binder
        self.optimizer = optimizer
        self.planner = planner
        self.physical_executor = physical_executor
        if processors is None:
            processors = []
        self.processors = processors
        if decorrelator is None:
            decorrelator = Decorrelator()
        self.decorrelator = decorrelator
        # Cross-query plan cache (None = disabled): repeated statements skip
        # parse-through-physical and go straight to execution.
        if plan_cache is None:
            plan_cache = plan_cache_from_env()
        self.plan_cache = plan_cache
        self.input_query = ""
        self.query_context = QueryContext("")
        self.last_profile_report: Optional[str] = None

    def execute(self, sql: str) -> Union[pa.Table, dict]:
        """Run the full query pipeline with middleware hooks."""
        self.input_query = sql
        self.query_context = QueryContext(sql)
        profiler = QueryProfiler() if profiling_enabled() else None
        result = self._execute_pipeline(sql, profiler)
        self.last_profile_report = profiler.report() if profiler else None
        return result

    def _execute_pipeline(
        self, sql: str, profiler: Optional[QueryProfiler]
    ) -> Union[pa.Table, dict]:
        physical_plan = self._plan_pipeline(sql, profiler)
        if profiler is not None:
            profiler.instrument(physical_plan)
        with stage_timer(profiler, "execute"):
            raw_result = self._run_physical_plan(physical_plan)
        return self._run_after_processors(raw_result)

    def _plan_pipeline(
        self, sql: str, profiler: Optional[QueryProfiler]
    ) -> PhysicalPlanNode:
        """Run and time the planning stages, returning the physical plan.

        Before-processors run on EVERY execution, cache hit or not: the star
        expansion's after-hook renames result columns from state its
        before-hook builds per query. The cache keys on the REWRITTEN SQL and
        skips parse-through-physical."""
        with stage_timer(profiler, "before"):
            rewritten_sql = self._run_before_processors(sql)
        cached = self._cached_plan(rewritten_sql)
        if cached is not None:
            return cached
        with stage_timer(profiler, "parse"):
            logical_plan = self._parse_query(rewritten_sql)
        with stage_timer(profiler, "bind"):
            bound_plan = self._bind_plan(logical_plan)
        with stage_timer(profiler, "decorrelate"):
            decorrelated_plan = self._decorrelate_plan(bound_plan)
        with stage_timer(profiler, "optimize"):
            optimized_plan = self._optimize_plan(decorrelated_plan)
        with stage_timer(profiler, "plan"):
            physical_plan = self._build_physical_plan(optimized_plan)
        self._store_plan(rewritten_sql, physical_plan)
        return physical_plan

    def _cached_plan(self, rewritten_sql: str) -> Optional[PhysicalPlanNode]:
        """The cached physical plan for this rewritten SQL, or None (miss,
        expired, or the cache is disabled)."""
        if self.plan_cache is None:
            return None
        return self.plan_cache.get(rewritten_sql)

    def _store_plan(self, rewritten_sql: str, plan: PhysicalPlanNode) -> None:
        """Cache a freshly built plan. An EXPLAIN plan never caches: its
        execution mutates scan nodes (dynamic_filter_values) and it must
        present the CURRENT planning decisions, not last minute's."""
        if self.plan_cache is None or isinstance(plan, PhysicalExplain):
            return
        self.plan_cache.put(rewritten_sql, plan)

    def _run_before_processors(self, sql: str) -> str:
        """Execute processor hooks before planning."""
        self.query_context.rewritten_sql = sql
        for processor in self.processors:
            processor.before_execution(self)
        return self.query_context.rewritten_sql

    def _parse_query(self, sql: str) -> LogicalPlanNode:
        """Parse SQL into a logical plan."""
        return self.parser.parse_to_logical_plan(
            sql, catalog=self.catalog, query_executor=self
        )

    def _bind_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Bind logical plan with catalog metadata."""
        return self.binder.bind(plan, query_executor=self)

    def _decorrelate_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Run decorrelation pass on a bound plan."""
        return self.decorrelator.decorrelate(plan)

    def _optimize_plan(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Apply optimizer rules to the bound plan."""
        return self.optimizer.optimize(plan, query_executor=self)

    def _build_physical_plan(self, plan: LogicalPlanNode) -> PhysicalPlanNode:
        """Construct a physical plan."""
        return self.planner.plan(plan, query_executor=self)

    def _run_physical_plan(self, plan: PhysicalPlanNode) -> Union[pa.Table, dict]:
        """Execute the physical plan or build EXPLAIN output."""
        if isinstance(plan, PhysicalExplain):
            return self._run_explain(plan)
        return self.physical_executor.execute_to_table(plan, query_executor=self)

    def _run_explain(self, plan: PhysicalExplain) -> Union[pa.Table, dict]:
        """JSON explain returns a structured document; TEXT explain executes to rows.

        Every ExplainFormat is handled explicitly; an unhandled one raises rather
        than silently being executed as if it were a normal query.
        """
        if plan.format == ExplainFormat.JSON:
            return plan.build_document(stringify_queries=False)
        if plan.format == ExplainFormat.TEXT:
            return self.physical_executor.execute_to_table(plan, query_executor=self)
        raise ValueError(f"Unsupported EXPLAIN format: {plan.format}")

    def _run_after_processors(
        self, result: Union[pa.Table, dict]
    ) -> Union[pa.Table, dict]:
        """Execute processor hooks after execution."""
        for processor in reversed(self.processors):
            result = processor.after_execution(self, result)
        return result
