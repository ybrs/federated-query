"""Binder resolves references and validates types."""

from typing import Dict, List, Optional, TYPE_CHECKING
from ..catalog.catalog import Catalog
from ..catalog.schema import Table
from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Limit,
    Sort,
    Join,
    Aggregate,
    Explain,
)
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    DataType,
    FunctionCall,
    InList,
    BetweenExpression,
    CaseExpr,
)

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


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

    def bind(
        self,
        plan: LogicalPlanNode,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> LogicalPlanNode:
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
        if isinstance(plan, Explain):
            return self._bind_explain(plan)
        if isinstance(plan, Scan):
            return self._bind_scan(plan)
        if isinstance(plan, Filter):
            return self._bind_filter(plan)
        if isinstance(plan, Project):
            return self._bind_project(plan)
        if isinstance(plan, Sort):
            return self._bind_sort(plan)
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

        if isinstance(bound_input, Aggregate):
            bound_predicate = self._bind_having_predicate(filter_node.predicate, bound_input)
        elif isinstance(bound_input, Join):
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
            distinct=project.distinct,
        )

    def _bind_sort(self, sort: Sort) -> Sort:
        """Bind a Sort node."""
        bound_input = self.bind(sort.input)

        if isinstance(bound_input, Project):
            bound_keys = self._bind_sort_keys_for_project(sort.sort_keys, bound_input)
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        if self._contains_join(bound_input):
            tables = self._extract_tables_from_tree(bound_input)
            bound_keys = self._bind_sort_keys_multi(sort.sort_keys, tables)
            return Sort(
                input=bound_input,
                sort_keys=bound_keys,
                ascending=sort.ascending,
                nulls_order=sort.nulls_order,
            )

        table = self._get_table_from_plan(bound_input)
        bound_keys = self._bind_sort_keys(sort.sort_keys, table)
        return Sort(
            input=bound_input,
            sort_keys=bound_keys,
            ascending=sort.ascending,
            nulls_order=sort.nulls_order,
        )

    def _bind_sort_keys(
        self, sort_keys: List[Expression], table: Optional[Table]
    ) -> List[Expression]:
        """Bind ORDER BY expressions for a single table."""
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_expression(key, table)
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_sort_keys_multi(
        self,
        sort_keys: List[Expression],
        tables: Dict[Optional[str], Table],
    ) -> List[Expression]:
        """Bind ORDER BY expressions referencing multiple tables."""
        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_expression_multi_table(key, tables)
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_sort_keys_for_project(
        self,
        sort_keys: List[Expression],
        project: Project,
    ) -> List[Expression]:
        """Bind ORDER BY keys when input is a Project (supports aliases)."""
        alias_map = self._build_alias_expression_map(project)
        tables = None
        if self._contains_join(project.input):
            tables = self._extract_tables_from_tree(project.input)
        table = self._get_table_from_plan(project.input)

        bound_keys: List[Expression] = []
        for key in sort_keys:
            bound_key = self._bind_project_sort_key(
                key=key,
                alias_map=alias_map,
                table=table,
                tables=tables,
            )
            bound_keys.append(bound_key)
        return bound_keys

    def _bind_project_sort_key(
        self,
        key: Expression,
        alias_map: Dict[str, Expression],
        table: Optional[Table],
        tables: Optional[Dict[Optional[str], Table]],
    ) -> Expression:
        """Bind a single ORDER BY expression that may reference a select alias."""
        from ..plan.expressions import ColumnRef

        if isinstance(key, ColumnRef) and key.table is None:
            if key.column in alias_map:
                source_expr = alias_map[key.column]
                from ..plan.expressions import ColumnRef as BoundColumnRef
                if isinstance(source_expr, BoundColumnRef):
                    return ColumnRef(
                        table=source_expr.table,
                        column=source_expr.column,
                        data_type=source_expr.data_type,
                    )
                return ColumnRef(
                    table=None,
                    column=key.column,
                    data_type=source_expr.get_type(),
                )

        if tables is not None:
            return self._bind_expression_multi_table(key, tables)

        return self._bind_expression(key, table)

    def _build_alias_expression_map(
        self, project: Project
    ) -> Dict[str, Expression]:
        """Map output aliases to their bound expressions."""
        alias_map: Dict[str, Expression] = {}
        for index in range(len(project.aliases)):
            alias = project.aliases[index]
            expression = project.expressions[index]
            alias_map[alias] = expression
        return alias_map

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
        if isinstance(expr, InList):
            return self._bind_in_list_multi(expr, tables)
        if isinstance(expr, BetweenExpression):
            return self._bind_between_multi(expr, tables)
        if isinstance(expr, CaseExpr):
            return self._bind_case_expr_multi(expr, tables)

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

    def _bind_explain(self, explain: Explain) -> Explain:
        """Bind an Explain node."""
        bound_child = self.bind(explain.input)
        return Explain(input=bound_child, format=explain.format)

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
        bound: List[Expression] = []
        for expr in expressions:
            bound_expr = self._bind_expression_multi_table(expr, tables)
            bound.append(bound_expr)
        return bound

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
                distinct=expr.distinct,
            )

        return self._bind_expression(expr, table)

    def _bind_aggregate_expression_multi_table(
        self, expr: Expression, tables: Dict[Optional[str], Table]
    ) -> Expression:
        """Bind aggregate expression referencing multiple tables."""
        if isinstance(expr, FunctionCall):
            bound_args = []
            for arg in expr.args:
                bound_arg = self._bind_expression_multi_table(arg, tables)
                bound_args.append(bound_arg)
            return FunctionCall(
                function_name=expr.function_name,
                args=bound_args,
                is_aggregate=expr.is_aggregate,
                distinct=expr.distinct,
            )

        return self._bind_expression_multi_table(expr, tables)

    def _bind_having_predicate(
        self, predicate: Expression, aggregate: Aggregate
    ) -> Expression:
        """Bind HAVING predicate against aggregate output schema.

        Args:
            predicate: HAVING predicate expression
            aggregate: Aggregate node providing output schema

        Returns:
            Bound predicate expression
        """
        return self._bind_having_expression(predicate, aggregate.output_names)

    def _bind_having_expression(
        self, expr: Expression, output_names: List[str]
    ) -> Expression:
        """Bind expression against aggregate output columns.

        Args:
            expr: Expression to bind
            output_names: List of output column names from aggregate

        Returns:
            Bound expression
        """
        if isinstance(expr, ColumnRef):
            if expr.column not in output_names:
                raise BindingError(
                    f"Column '{expr.column}' not found in aggregate output"
                )
            return expr

        if isinstance(expr, BinaryOp):
            left = self._bind_having_expression(expr.left, output_names)
            right = self._bind_having_expression(expr.right, output_names)
            return BinaryOp(op=expr.op, left=left, right=right)

        if isinstance(expr, UnaryOp):
            operand = self._bind_having_expression(expr.operand, output_names)
            return UnaryOp(op=expr.op, operand=operand)

        return expr

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
        if isinstance(expr, InList):
            return self._bind_in_list(expr, table)
        if isinstance(expr, BetweenExpression):
            return self._bind_between(expr, table)
        if isinstance(expr, CaseExpr):
            return self._bind_case_expr(expr, table)

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

    def _bind_in_list(
        self,
        expr: InList,
        table: Optional[Table]
    ) -> InList:
        bound_value = self._bind_expression(expr.value, table)
        bound_options: List[Expression] = []
        for option in expr.options:
            bound_option = self._bind_expression(option, table)
            bound_options.append(bound_option)
        return InList(value=bound_value, options=bound_options)

    def _bind_in_list_multi(
        self,
        expr: InList,
        tables: Dict[Optional[str], Table]
    ) -> InList:
        bound_value = self._bind_expression_multi_table(expr.value, tables)
        bound_options: List[Expression] = []
        for option in expr.options:
            bound_option = self._bind_expression_multi_table(option, tables)
            bound_options.append(bound_option)
        return InList(value=bound_value, options=bound_options)

    def _bind_between(
        self,
        expr: BetweenExpression,
        table: Optional[Table]
    ) -> BetweenExpression:
        bound_value = self._bind_expression(expr.value, table)
        bound_lower = self._bind_expression(expr.lower, table)
        bound_upper = self._bind_expression(expr.upper, table)
        return BetweenExpression(
            value=bound_value,
            lower=bound_lower,
            upper=bound_upper,
        )

    def _bind_between_multi(
        self,
        expr: BetweenExpression,
        tables: Dict[Optional[str], Table]
    ) -> BetweenExpression:
        bound_value = self._bind_expression_multi_table(expr.value, tables)
        bound_lower = self._bind_expression_multi_table(expr.lower, tables)
        bound_upper = self._bind_expression_multi_table(expr.upper, tables)
        return BetweenExpression(
            value=bound_value,
            lower=bound_lower,
            upper=bound_upper,
        )

    def _bind_case_expr(
        self,
        expr: CaseExpr,
        table: Optional[Table]
    ) -> CaseExpr:
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_condition = self._bind_expression(condition, table)
            bound_result = self._bind_expression(result, table)
            bound_when.append((bound_condition, bound_result))
        bound_else = None
        if expr.else_result is not None:
            bound_else = self._bind_expression(expr.else_result, table)
        return CaseExpr(when_clauses=bound_when, else_result=bound_else)

    def _bind_case_expr_multi(
        self,
        expr: CaseExpr,
        tables: Dict[Optional[str], Table]
    ) -> CaseExpr:
        bound_when = []
        for condition, result in expr.when_clauses:
            bound_condition = self._bind_expression_multi_table(condition, tables)
            bound_result = self._bind_expression_multi_table(result, tables)
            bound_when.append((bound_condition, bound_result))
        bound_else = None
        if expr.else_result is not None:
            bound_else = self._bind_expression_multi_table(expr.else_result, tables)
        return CaseExpr(when_clauses=bound_when, else_result=bound_else)

    def __repr__(self) -> str:
        return "Binder()"
