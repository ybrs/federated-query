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
    Join,
    Aggregate,
)
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    DataType,
    FunctionCall,
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
        if isinstance(plan, Join):
            return self._bind_join(plan)
        if isinstance(plan, Aggregate):
            return self._bind_aggregate(plan)

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

        if isinstance(bound_input, Join):
            tables = self._get_tables_from_join(bound_input.left, bound_input.right)
            bound_predicate = self._bind_join_condition(filter_node.predicate, tables)
        else:
            table = self._get_table_from_plan(bound_input)
            bound_predicate = self._bind_expression(filter_node.predicate, table)

        return Filter(input=bound_input, predicate=bound_predicate)

    def _bind_project(self, project: Project) -> Project:
        """Bind a Project node."""
        bound_input = self.bind(project.input)

        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_expressions = []
            for expr in project.expressions:
                bound_expr = self._bind_expression_multi_table(expr, tables)
                bound_expressions.append(bound_expr)
        else:
            table = self._get_table_from_plan(bound_input)
            bound_expressions = self._bind_expressions(project.expressions, table)

        return Project(
            input=bound_input,
            expressions=bound_expressions,
            aliases=project.aliases,
        )

    def _contains_join(self, plan: LogicalPlanNode) -> bool:
        """Check if plan tree contains a Join node."""
        if isinstance(plan, Join):
            return True
        if hasattr(plan, "input"):
            return self._contains_join(plan.input)
        return False

    def _extract_tables_from_tree(self, plan: LogicalPlanNode) -> Dict[Optional[str], Table]:
        """Extract all tables from plan tree."""
        if isinstance(plan, Join):
            return self._get_tables_from_join(plan.left, plan.right)
        if hasattr(plan, "input"):
            return self._extract_tables_from_tree(plan.input)
        return {}

    def _bind_expression_multi_table(
        self, expr: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind expression with multiple tables."""
        if isinstance(expr, ColumnRef):
            return self._bind_column_ref_multi_table(expr, tables)
        if isinstance(expr, Literal):
            return expr
        if isinstance(expr, BinaryOp):
            left = self._bind_expression_multi_table(expr.left, tables)
            right = self._bind_expression_multi_table(expr.right, tables)
            return BinaryOp(op=expr.op, left=left, right=right)
        if isinstance(expr, UnaryOp):
            operand = self._bind_expression_multi_table(expr.operand, tables)
            return UnaryOp(op=expr.op, operand=operand)

        return expr

    def _bind_limit(self, limit: Limit) -> Limit:
        """Bind a Limit node."""
        bound_input = self.bind(limit.input)
        return Limit(input=bound_input, limit=limit.limit, offset=limit.offset)

    def _bind_join(self, join: Join) -> Join:
        """Bind a Join node."""
        bound_left = self.bind(join.left)
        bound_right = self.bind(join.right)

        bound_condition = None
        if join.condition:
            tables = self._get_tables_from_join(bound_left, bound_right)
            bound_condition = self._bind_join_condition(join.condition, tables)

        return Join(
            left=bound_left,
            right=bound_right,
            join_type=join.join_type,
            condition=bound_condition,
        )

    def _bind_aggregate(self, aggregate: Aggregate) -> Aggregate:
        """Bind an Aggregate node."""
        bound_input = self.bind(aggregate.input)
        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_group_by = self._bind_group_by_multi_table(aggregate.group_by, tables)
            bound_aggregates = self._bind_aggregate_expressions_multi_table(
                aggregate.aggregates, tables
            )
        else:
            table = self._get_table_from_plan(bound_input)
            bound_group_by = self._bind_group_by_expressions(aggregate.group_by, table)
            bound_aggregates = self._bind_aggregate_expressions(
                aggregate.aggregates, table
            )

        return Aggregate(
            input=bound_input,
            group_by=bound_group_by,
            aggregates=bound_aggregates,
            output_names=aggregate.output_names,
        )

    def _bind_group_by_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind GROUP BY expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_aggregate_expressions(
        self, expressions: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind aggregate expressions."""
        bound = []
        for expr in expressions:
            bound_expr = self._bind_aggregate_expression(expr, table)
            bound.append(bound_expr)
        return bound

    def _bind_group_by_multi_table(
        self, expressions: List[Expression], tables: Dict[Optional[str], Table]
    ) -> List[Expression]:
        """Bind GROUP BY expressions referencing multiple tables."""
        return [self._bind_expression_multi_table(expr, tables) for expr in expressions]

    def _bind_aggregate_expressions_multi_table(
        self, expressions: List[Expression], tables: Dict[Optional[str], Table]
    ) -> List[Expression]:
        """Bind aggregate expressions when multiple tables are available."""
        bound = []
        for expr in expressions:
            bound.append(self._bind_aggregate_expression_multi_table(expr, tables))
        return bound

    def _bind_aggregate_expression(
        self, expr: Expression, table: Optional[Table]
    ) -> Expression:
        """Bind an aggregate expression."""
        if isinstance(expr, FunctionCall):
            bound_args = self._bind_expressions(expr.args, table)
            return FunctionCall(
                function_name=expr.function_name,
                args=bound_args,
                is_aggregate=expr.is_aggregate,
            )

        return self._bind_expression(expr, table)

    def _bind_aggregate_expression_multi_table(
        self, expr: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind aggregate expression referencing multiple tables."""
        if isinstance(expr, FunctionCall):
            bound_args = [
                self._bind_expression_multi_table(arg, tables) for arg in expr.args
            ]
            return FunctionCall(
                function_name=expr.function_name,
                args=bound_args,
                is_aggregate=expr.is_aggregate,
            )

        return self._bind_expression_multi_table(expr, tables)

    def _get_tables_from_join(
        self, left: LogicalPlanNode, right: LogicalPlanNode
    ) -> Dict[Optional[str], Table]:
        """Get tables from both sides of join.

        Returns:
            Dictionary mapping table names/aliases to Table objects
        """
        tables = {}

        left_tables = self._extract_tables(left)
        right_tables = self._extract_tables(right)

        tables.update(left_tables)
        tables.update(right_tables)

        return tables

    def _extract_tables(self, plan: LogicalPlanNode) -> Dict[Optional[str], Table]:
        """Extract all tables from a plan node."""
        tables = {}

        if isinstance(plan, Scan):
            table = self.catalog.get_table(
                plan.datasource, plan.schema_name, plan.table_name
            )
            if table:
                tables[plan.table_name] = table

        if isinstance(plan, Join):
            left_tables = self._extract_tables(plan.left)
            right_tables = self._extract_tables(plan.right)
            tables.update(left_tables)
            tables.update(right_tables)

        if hasattr(plan, "input"):
            return self._extract_tables(plan.input)

        return tables

    def _bind_join_condition(
        self, condition: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind join condition with multiple tables."""
        if isinstance(condition, ColumnRef):
            return self._bind_column_ref_multi_table(condition, tables)
        if isinstance(condition, Literal):
            return condition
        if isinstance(condition, BinaryOp):
            left = self._bind_join_condition(condition.left, tables)
            right = self._bind_join_condition(condition.right, tables)
            return BinaryOp(op=condition.op, left=left, right=right)
        if isinstance(condition, UnaryOp):
            operand = self._bind_join_condition(condition.operand, tables)
            return UnaryOp(op=condition.op, operand=operand)

        return condition

    def _bind_column_ref_multi_table(
        self, col_ref: ColumnRef, tables: Dict[Optional[str], Table]
    ) -> ColumnRef:
        """Bind column reference with multiple tables."""
        if col_ref.column == "*":
            return col_ref

        if col_ref.table:
            table = tables.get(col_ref.table)
            if table is None:
                for table_name, tbl in tables.items():
                    column = tbl.get_column(col_ref.column)
                    if column:
                        return ColumnRef(
                            table=col_ref.table,
                            column=col_ref.column,
                            data_type=column.data_type,
                        )
                raise BindingError(f"Column '{col_ref.column}' with qualifier '{col_ref.table}' not found")
            column = table.get_column(col_ref.column)
            if column is None:
                raise BindingError(
                    f"Column '{col_ref.column}' not found in table {col_ref.table}"
                )
            return ColumnRef(
                table=col_ref.table,
                column=col_ref.column,
                data_type=column.data_type,
            )

        found_table = None
        found_column = None
        for table_name, table in tables.items():
            column = table.get_column(col_ref.column)
            if column:
                if found_column:
                    raise BindingError(
                        f"Column '{col_ref.column}' is ambiguous (found in multiple tables)"
                    )
                found_table = table_name
                found_column = column

        if found_column is None:
            raise BindingError(f"Column '{col_ref.column}' not found in any table")

        return ColumnRef(
            table=found_table,
            column=col_ref.column,
            data_type=found_column.data_type,
        )

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
            raise BindingError(f"Can not resolve column '{col_ref.column}'': no table context")

        column = table.get_column(col_ref.column)
        if column is None:
            raise BindingError(
                f"Column '{col_ref.column}' not found in table '{table.name}'"
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
