"""Binder resolves references and validates types."""

from typing import Dict, List, Optional
from ..catalog.catalog import Catalog
from ..plan.logical import LogicalPlanNode
from ..plan.expressions import Expression, ColumnRef


class BindingError(Exception):
    """Exception raised during binding."""

    pass


class Binder:
    """Binder resolves table and column references."""

    def __init__(self, catalog: Catalog):
        """Initialize binder.

        Args:
            catalog: Catalog with metadata
        """
        self.catalog = catalog

    def bind(self, plan: LogicalPlanNode) -> LogicalPlanNode:
        """Bind a logical plan.

        This resolves all table and column references, validates types,
        and adds metadata to the plan.

        Args:
            plan: Unbound logical plan

        Returns:
            Bound logical plan with resolved references

        Raises:
            BindingError: If binding fails
        """
        # TODO: Implement binding logic
        # This will:
        # 1. Resolve table references using catalog
        # 2. Resolve column references
        # 3. Validate types
        # 4. Add schema information to plan nodes
        raise NotImplementedError("Binding not yet implemented")

    def resolve_column(
        self, col_ref: ColumnRef, available_columns: List[str]
    ) -> ColumnRef:
        """Resolve a column reference.

        Args:
            col_ref: Column reference to resolve
            available_columns: List of available column names

        Returns:
            Resolved column reference

        Raises:
            BindingError: If column cannot be resolved
        """
        # TODO: Implement column resolution
        raise NotImplementedError("Column resolution not yet implemented")

    def __repr__(self) -> str:
        return "Binder()"
