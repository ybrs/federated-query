"""Physical planner converts logical plans to physical plans."""

from typing import Dict
from ..catalog.catalog import Catalog
from ..datasources.base import DataSource
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Limit,
)
from ..plan.physical import (
    PhysicalPlanNode,
    PhysicalScan,
    PhysicalProject,
    PhysicalFilter,
    PhysicalLimit,
)


class PhysicalPlanner:
    """Converts logical plans to physical plans."""

    def __init__(self, catalog: Catalog):
        """Initialize physical planner.

        Args:
            catalog: Catalog for looking up data sources
        """
        self.catalog = catalog

    def plan(self, logical_plan: LogicalPlanNode) -> PhysicalPlanNode:
        """Convert logical plan to physical plan.

        Args:
            logical_plan: Logical plan to convert

        Returns:
            Physical plan ready for execution
        """
        return self._plan_node(logical_plan)

    def _plan_node(self, node: LogicalPlanNode) -> PhysicalPlanNode:
        """Plan a single node."""
        if isinstance(node, Scan):
            return self._plan_scan(node)
        if isinstance(node, Filter):
            return self._plan_filter(node)
        if isinstance(node, Project):
            return self._plan_project(node)
        if isinstance(node, Limit):
            return self._plan_limit(node)

        raise ValueError(f"Unsupported logical plan node: {type(node)}")

    def _plan_scan(self, scan: Scan) -> PhysicalScan:
        """Plan a scan node."""
        datasource = self.catalog.get_datasource(scan.datasource)
        if datasource is None:
            raise ValueError(f"Data source not found: {scan.datasource}")

        return PhysicalScan(
            datasource=scan.datasource,
            schema_name=scan.schema_name,
            table_name=scan.table_name,
            columns=scan.columns,
            filters=scan.filters,
            datasource_connection=datasource,
        )

    def _plan_filter(self, filter_node: Filter) -> PhysicalFilter:
        """Plan a filter node."""
        input_plan = self._plan_node(filter_node.input)
        return PhysicalFilter(input=input_plan, predicate=filter_node.predicate)

    def _plan_project(self, project: Project) -> PhysicalProject:
        """Plan a project node."""
        input_plan = self._plan_node(project.input)
        return PhysicalProject(
            input=input_plan,
            expressions=project.expressions,
            output_names=project.aliases,
        )

    def _plan_limit(self, limit: Limit) -> PhysicalLimit:
        """Plan a limit node."""
        input_plan = self._plan_node(limit.input)
        return PhysicalLimit(
            input=input_plan, limit=limit.limit, offset=limit.offset
        )

    def __repr__(self) -> str:
        return "PhysicalPlanner()"
