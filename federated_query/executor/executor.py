"""Query executor."""

from typing import Iterator
import pyarrow as pa
from ..plan.physical import PhysicalPlanNode
from ..config.config import ExecutorConfig


class Executor:
    """Query executor that runs physical plans."""

    def __init__(self, config: ExecutorConfig):
        """Initialize executor.

        Args:
            config: Executor configuration
        """
        self.config = config

    def execute(self, plan: PhysicalPlanNode) -> Iterator[pa.RecordBatch]:
        """Execute a physical plan.

        Args:
            plan: Physical plan to execute

        Yields:
            Arrow record batches with results
        """
        # Execute the plan
        # The plan nodes themselves implement execute() which yields batches
        yield from plan.execute()

    def execute_to_table(self, plan: PhysicalPlanNode) -> pa.Table:
        """Execute a physical plan and materialize as Arrow table.

        Args:
            plan: Physical plan to execute

        Returns:
            Arrow table with all results
        """
        batches = list(self.execute(plan))
        if not batches:
            # Return empty table with schema
            return pa.Table.from_batches([], schema=plan.schema())
        return pa.Table.from_batches(batches)

    def __repr__(self) -> str:
        return "Executor()"
