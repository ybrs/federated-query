"""Binder resolves references and validates types."""

from typing import Dict, List, Optional
from ..catalog.catalog import Catalog
from ..catalog.schema import Table, Column
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Limit,
)
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    DataType,
)


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
        if isinstance(plan, Scan):
            return self._bind_scan(plan)
        if isinstance(plan, Filter):
            return self._bind_filter(plan)
        if isinstance(plan, Project):
            return self._bind_project(plan)
        if isinstance(plan, Limit):
            return self._bind_limit(plan)

        raise BindingError(f"Unsupported plan node type: {type(plan)}")

    def _bind_scan(self, scan: Scan) -> Scan:
        """Bind a Scan node."""
        table = self._resolve_table(scan)
        self._validate_columns(scan, table)
        return scan

    def _resolve_table(self, scan: Scan) -> Table:
        """Resolve table reference."""
        table = self.catalog.get_table(
            scan.datasource, scan.schema_name, scan.table_name
        )
        if table is None:
            raise BindingError(
                f"Table not found: {scan.datasource}.{scan.schema_name}.{scan.table_name}"
            )
        return table

    def _validate_columns(self, scan: Scan, table: Table) -> None:
        """Validate that columns exist in table."""
        if "*" in scan.columns:
            return

        for col_name in scan.columns:
            column = table.get_column(col_name)
            if column is None:
                raise BindingError(
                    f"Column '{col_name}' not found in table {table.name}"
                )

    def _bind_filter(self, filter_node: Filter) -> Filter:
        """Bind a Filter node."""
        bound_input = self.bind(filter_node.input)
        table = self._get_table_from_plan(bound_input)
        bound_predicate = self._bind_expression(filter_node.predicate, table)
        return Filter(input=bound_input, predicate=bound_predicate)

    def _bind_project(self, project: Project) -> Project:
        """Bind a Project node."""
        bound_input = self.bind(project.input)
        table = self._get_table_from_plan(bound_input)
        bound_expressions = self._bind_expressions(project.expressions, table)
        return Project(
            input=bound_input,
            expressions=bound_expressions,
            aliases=project.aliases,
        )

    def _bind_limit(self, limit: Limit) -> Limit:
        """Bind a Limit node."""
        bound_input = self.bind(limit.input)
        return Limit(input=bound_input, limit=limit.limit, offset=limit.offset)

    def _get_table_from_plan(self, plan: LogicalPlanNode) -> Optional[Table]:
        """Extract table metadata from a plan node."""
        if isinstance(plan, Scan):
            return self.catalog.get_table(
                plan.datasource, plan.schema_name, plan.table_name
            )
        if hasattr(plan, "input"):
            return self._get_table_from_plan(plan.input)
        return None

    def _bind_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind a list of expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_expression(
        self, expr: Expression, table: Optional[Table]
    ) -> Expression:
        """Bind an expression."""
        if isinstance(expr, ColumnRef):
            return self._bind_column_ref(expr, table)
        if isinstance(expr, Literal):
            return expr
        if isinstance(expr, BinaryOp):
            return self._bind_binary_op(expr, table)
        if isinstance(expr, UnaryOp):
            return self._bind_unary_op(expr, table)

        return expr

    def _bind_column_ref(
        self, col_ref: ColumnRef, table: Optional[Table]
    ) -> ColumnRef:
        """Bind a column reference."""
        if col_ref.column == "*":
            return col_ref

        if table is None:
            raise BindingError(f"Cannot resolve column {col_ref.column}: no table context")

        column = table.get_column(col_ref.column)
        if column is None:
            raise BindingError(
                f"Column '{col_ref.column}' not found in table {table.name}"
            )

        return ColumnRef(
            table=col_ref.table,
            column=col_ref.column,
            data_type=column.data_type,
        )

    def _bind_binary_op(self, binary_op: BinaryOp, table: Optional[Table]) -> BinaryOp:
        """Bind a binary operation."""
        left = self._bind_expression(binary_op.left, table)
        right = self._bind_expression(binary_op.right, table)
        return BinaryOp(op=binary_op.op, left=left, right=right)

    def _bind_unary_op(self, unary_op: UnaryOp, table: Optional[Table]) -> UnaryOp:
        """Bind a unary operation."""
        operand = self._bind_expression(unary_op.operand, table)
        return UnaryOp(op=unary_op.op, operand=operand)

    def __repr__(self) -> str:
        return "Binder()"
