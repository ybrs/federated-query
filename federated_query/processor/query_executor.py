"""QueryExecutor orchestrates middleware and the query pipeline."""

from __future__ import annotations

from typing import List, Optional, Union, Protocol

import pyarrow as pa

from ..catalog import Catalog
from ..executor import Executor
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
        self.input_query = ""
        self.query_context = QueryContext("")

    def execute(self, sql: str) -> Union[pa.Table, dict]:
        """Run the full query pipeline with middleware hooks."""
        self.input_query = sql
        self.query_context = QueryContext(sql)
        rewritten_sql = self._run_before_processors(sql)
        logical_plan = self._parse_query(rewritten_sql)
        bound_plan = self._bind_plan(logical_plan)
        decorrelated_plan = self._decorrelate_plan(bound_plan)
        optimized_plan = self._optimize_plan(decorrelated_plan)
        physical_plan = self._build_physical_plan(optimized_plan)
        raw_result = self._run_physical_plan(physical_plan)
        return self._run_after_processors(raw_result)

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

    def _run_physical_plan(
        self, plan: PhysicalPlanNode
    ) -> Union[pa.Table, dict]:
        """Execute the physical plan or build EXPLAIN output."""
        if isinstance(plan, PhysicalExplain):
            if plan.format == ExplainFormat.JSON:
                return plan.build_document(stringify_queries=False)
        return self.physical_executor.execute_to_table(
            plan, query_executor=self
        )

    def _run_after_processors(
        self, result: Union[pa.Table, dict]
    ) -> Union[pa.Table, dict]:
        """Execute processor hooks after execution."""
        for processor in reversed(self.processors):
            result = processor.after_execution(self, result)
        return result
