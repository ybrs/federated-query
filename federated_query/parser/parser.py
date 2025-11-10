"""SQL parser using sqlglot."""

import sqlglot
from sqlglot import exp
from typing import Optional, List, Tuple
from ..plan.logical import LogicalPlanNode, Scan, Project, Filter, Limit, Join, JoinType, Aggregate
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    DataType,
    FunctionCall,
)


class Parser:
    """SQL parser that converts SQL to logical plan."""

    def __init__(self):
        """Initialize parser."""
        self.dialect = "postgres"  # Default dialect

    def parse(self, sql: str) -> exp.Expression:
        """Parse SQL string to sqlglot AST.

        Args:
            sql: SQL query string

        Returns:
            sqlglot expression tree
        """
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        return parsed

    def ast_to_logical_plan(self, ast: exp.Expression) -> LogicalPlanNode:
        """Convert sqlglot AST to logical plan.

        Args:
            ast: sqlglot expression tree

        Returns:
            Logical plan root node
        """
        if isinstance(ast, exp.Select):
            return self._convert_select(ast)
        raise ValueError(f"Unsupported AST node type: {type(ast)}")

    def _convert_select(self, select: exp.Select) -> LogicalPlanNode:
        """Convert SELECT statement to logical plan.

        Args:
            select: sqlglot Select node

        Returns:
            Logical plan node
        """
        plan = self._build_from_clause(select)
        plan = self._build_where_clause(select, plan)
        plan = self._build_group_by_clause(select, plan)
        plan = self._build_having_clause(select, plan)
        plan = self._build_select_clause(select, plan)
        plan = self._build_limit_clause(select, plan)
        return plan

    def _build_from_clause(self, select: exp.Select) -> LogicalPlanNode:
        """Build scan node from FROM clause.

        Args:
            select: sqlglot Select node

        Returns:
            Scan node or Join node
        """
        from_clause = select.args.get("from")
        if not from_clause:
            raise ValueError("SELECT must have FROM clause")

        table_expr = from_clause.this

        joins = select.args.get("joins")
        if joins:
            return self._build_join_plan(select, table_expr, joins)

        table_parts = self._extract_table_parts(table_expr)
        all_columns = self._collect_needed_columns(select)

        datasource = table_parts[0]
        schema_name = table_parts[1]
        table_name = table_parts[2]

        return Scan(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            columns=all_columns,
        )

    def _build_join_plan(
        self, select: exp.Select, left_table: exp.Table, joins: List[exp.Join]
    ) -> LogicalPlanNode:
        """Build join plan from FROM and JOIN clauses.

        Args:
            select: sqlglot Select node
            left_table: Left table from FROM clause
            joins: List of JOIN clauses

        Returns:
            Join node
        """
        left_table_parts = self._extract_table_parts(left_table)
        left_table_alias = self._extract_table_alias(left_table)

        left_plan = Scan(
            datasource=left_table_parts[0],
            schema_name=left_table_parts[1],
            table_name=left_table_parts[2],
            columns=["*"],
        )

        current_plan = left_plan

        for join_clause in joins:
            right_table = join_clause.this
            right_table_parts = self._extract_table_parts(right_table)
            right_table_alias = self._extract_table_alias(right_table)

            right_plan = Scan(
                datasource=right_table_parts[0],
                schema_name=right_table_parts[1],
                table_name=right_table_parts[2],
                columns=["*"],
            )

            join_type = self._extract_join_type(join_clause)
            join_condition = None
            if join_clause.args.get("on"):
                join_condition = self._convert_expression(join_clause.args["on"])

            current_plan = Join(
                left=current_plan,
                right=right_plan,
                join_type=join_type,
                condition=join_condition,
            )

        return current_plan

    def _extract_table_alias(self, table_expr: exp.Table) -> Optional[str]:
        """Extract table alias if present.

        Args:
            table_expr: sqlglot Table node

        Returns:
            Table alias or table name if no alias
        """
        if table_expr.alias:
            return table_expr.alias
        return table_expr.name

    def _filter_columns_for_table(
        self, columns: List[str], table_alias: Optional[str]
    ) -> List[str]:
        """Filter columns that belong to a specific table.

        Args:
            columns: All column names
            table_alias: Table alias to filter by

        Returns:
            List of columns for this table
        """
        if "*" in columns:
            return ["*"]

        filtered = []
        for col in columns:
            if "." in col:
                table_part, col_part = col.split(".", 1)
                if table_part == table_alias:
                    filtered.append(col_part)
            else:
                filtered.append(col)

        return filtered

    def _extract_join_type(self, join_clause: exp.Join) -> JoinType:
        """Extract join type from JOIN clause.

        Args:
            join_clause: sqlglot Join node

        Returns:
            JoinType enum value
        """
        kind = join_clause.args.get("kind")
        side = join_clause.args.get("side")

        if not kind:
            return JoinType.INNER

        if kind.upper() == "CROSS":
            return JoinType.CROSS
        if side:
            side_upper = side.upper()
            if side_upper == "LEFT":
                return JoinType.LEFT
            if side_upper == "RIGHT":
                return JoinType.RIGHT
            if side_upper == "FULL":
                return JoinType.FULL

        return JoinType.INNER

    def _extract_table_parts(self, table_expr: exp.Table) -> Tuple[str, str, str]:
        """Extract datasource, schema, and table name.

        Args:
            table_expr: sqlglot Table node

        Returns:
            Tuple of (datasource, schema, table_name)
        """
        parts = []
        if table_expr.catalog:
            parts.append(table_expr.catalog)
        if table_expr.db:
            parts.append(table_expr.db)
        parts.append(table_expr.name)

        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "default", parts[0], parts[1]
        return "default", "public", parts[0]

    def _collect_needed_columns(self, select: exp.Select) -> List[str]:
        """Collect all columns needed from table.

        Args:
            select: sqlglot Select node

        Returns:
            List of column names
        """
        columns = []

        for expr in select.expressions:
            col_names = self._extract_column_names(expr)
            for col_name in col_names:
                if col_name not in columns:
                    columns.append(col_name)

        where = select.args.get("where")
        if where:
            col_names = self._extract_column_names(where.this)
            for col_name in col_names:
                if col_name not in columns:
                    columns.append(col_name)

        if not columns:
            columns = ["*"]

        return columns

    def _extract_column_names(self, expr: exp.Expression) -> List[str]:
        """Extract column names from expression.

        Args:
            expr: sqlglot expression

        Returns:
            List of column names
        """
        columns = []

        if isinstance(expr, exp.Column):
            columns.append(expr.name)
        elif isinstance(expr, exp.Star):
            columns.append("*")
        else:
            for child in expr.iter_expressions():
                child_cols = self._extract_column_names(child)
                columns.extend(child_cols)

        return columns

    def _build_where_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build filter node from WHERE clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with filter (or original if no WHERE)
        """
        where = select.args.get("where")
        if not where:
            return input_plan

        predicate = self._convert_expression(where.this)
        return Filter(input=input_plan, predicate=predicate)

    def _build_select_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build project node from SELECT clause."""
        if isinstance(input_plan, Aggregate):
            return input_plan

        expressions = []
        aliases = []

        for select_expr in select.expressions:
            expr = self._convert_expression(select_expr)
            expressions.append(expr)

            alias = self._get_alias(select_expr)
            aliases.append(alias)

        return Project(input=input_plan, expressions=expressions, aliases=aliases)

    def _get_alias(self, expr: exp.Expression) -> str:
        """Get alias for expression.

        Args:
            expr: sqlglot expression

        Returns:
            Alias name
        """
        if isinstance(expr, exp.Alias):
            return expr.alias
        if isinstance(expr, exp.Column):
            return expr.name
        return str(expr)

    def _build_limit_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build limit node from LIMIT clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with limit (or original if no LIMIT)
        """
        limit_expr = select.args.get("limit")
        if not limit_expr:
            return input_plan

        limit_value = int(limit_expr.expression.this)
        return Limit(input=input_plan, limit=limit_value)

    def _convert_expression(self, expr: exp.Expression) -> Expression:
        """Convert sqlglot expression to our Expression.

        Args:
            expr: sqlglot expression

        Returns:
            Our Expression object
        """
        if isinstance(expr, exp.Column):
            return self._convert_column(expr)
        if isinstance(expr, exp.Literal):
            return self._convert_literal(expr)
        if isinstance(expr, exp.Binary):
            return self._convert_binary(expr)
        if isinstance(expr, exp.Unary):
            return self._convert_unary(expr)
        if isinstance(expr, exp.Alias):
            return self._convert_expression(expr.this)
        if isinstance(expr, exp.Star):
            return ColumnRef(table=None, column="*")
        if self._is_aggregate_function(expr):
            return self._convert_function(expr)

        raise ValueError(f"Unsupported expression type: {type(expr)}")

    def _convert_column(self, col: exp.Column) -> ColumnRef:
        """Convert sqlglot Column to ColumnRef."""
        table = col.table if col.table else None
        return ColumnRef(table=table, column=col.name)

    def _convert_literal(self, lit: exp.Literal) -> Literal:
        """Convert sqlglot Literal to our Literal."""
        value_str = lit.this
        data_type = self._infer_literal_type(value_str)
        converted_value = self._convert_literal_value(value_str, data_type)
        return Literal(value=converted_value, data_type=data_type)

    def _infer_literal_type(self, value: str) -> DataType:
        """Infer data type from literal value."""
        if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
            return DataType.INTEGER
        if value.replace(".", "", 1).replace("-", "", 1).isdigit():
            return DataType.DOUBLE
        if value.lower() in ("true", "false"):
            return DataType.BOOLEAN
        return DataType.VARCHAR

    def _convert_literal_value(self, value_str: str, data_type: DataType):
        """Convert literal string to proper Python type."""
        if data_type == DataType.INTEGER:
            return int(value_str)
        if data_type == DataType.DOUBLE:
            return float(value_str)
        if data_type == DataType.BOOLEAN:
            return value_str.lower() == "true"
        return value_str

    def _convert_binary(self, binary: exp.Binary) -> BinaryOp:
        """Convert sqlglot binary operation."""
        left = self._convert_expression(binary.left)
        right = self._convert_expression(binary.right)
        op = self._map_binary_op(binary)
        return BinaryOp(op=op, left=left, right=right)

    def _map_binary_op(self, binary: exp.Binary) -> BinaryOpType:
        """Map sqlglot binary op to our BinaryOpType."""
        type_name = type(binary).__name__
        mapping = {
            "Add": BinaryOpType.ADD,
            "Sub": BinaryOpType.SUBTRACT,
            "Mul": BinaryOpType.MULTIPLY,
            "Div": BinaryOpType.DIVIDE,
            "Mod": BinaryOpType.MODULO,
            "EQ": BinaryOpType.EQ,
            "NEQ": BinaryOpType.NEQ,
            "LT": BinaryOpType.LT,
            "LTE": BinaryOpType.LTE,
            "GT": BinaryOpType.GT,
            "GTE": BinaryOpType.GTE,
            "And": BinaryOpType.AND,
            "Or": BinaryOpType.OR,
            "Like": BinaryOpType.LIKE,
        }
        return mapping.get(type_name, BinaryOpType.EQ)

    def _convert_unary(self, unary: exp.Unary) -> UnaryOp:
        """Convert sqlglot unary operation."""
        operand = self._convert_expression(unary.this)
        op = self._map_unary_op(unary)
        return UnaryOp(op=op, operand=operand)

    def _map_unary_op(self, unary: exp.Unary) -> UnaryOpType:
        """Map sqlglot unary op to our UnaryOpType."""
        type_name = type(unary).__name__
        mapping = {
            "Not": UnaryOpType.NOT,
            "Neg": UnaryOpType.NEGATE,
        }
        return mapping.get(type_name, UnaryOpType.NOT)

    def _build_group_by_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build aggregate node from GROUP BY clause or aggregate functions."""
        has_group_by = select.args.get("group") is not None
        has_aggregates = self._has_aggregate_functions(select)

        if not has_group_by and not has_aggregates:
            return input_plan

        group_exprs = []
        if has_group_by:
            group_exprs = self._extract_group_by_expressions(select.args["group"])

        agg_exprs, output_names = self._extract_aggregate_expressions(select)

        return Aggregate(
            input=input_plan,
            group_by=group_exprs,
            aggregates=agg_exprs,
            output_names=output_names,
        )

    def _extract_group_by_expressions(self, group_by: exp.Group) -> List[Expression]:
        """Extract grouping expressions from GROUP BY clause."""
        expressions = []
        for group_expr in group_by.expressions:
            expr = self._convert_expression(group_expr)
            expressions.append(expr)
        return expressions

    def _extract_aggregate_expressions(self, select: exp.Select) -> Tuple[List[Expression], List[str]]:
        """Extract aggregate expressions and output names."""
        agg_exprs = []
        output_names = []

        for select_expr in select.expressions:
            alias = self._get_alias(select_expr)
            output_names.append(alias)

            if isinstance(select_expr, exp.Alias):
                expr = self._convert_expression(select_expr.this)
            else:
                expr = self._convert_expression(select_expr)

            agg_exprs.append(expr)

        return agg_exprs, output_names

    def _build_having_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build filter node from HAVING clause."""
        having = select.args.get("having")
        if not having:
            return input_plan

        predicate = self._convert_expression(having.this)
        return Filter(input=input_plan, predicate=predicate)

    def _has_aggregate_functions(self, select: exp.Select) -> bool:
        """Check if SELECT clause contains aggregate functions."""
        for select_expr in select.expressions:
            if self._contains_aggregate(select_expr):
                return True
        return False

    def _contains_aggregate(self, expr: exp.Expression) -> bool:
        """Check if expression contains aggregate functions."""
        if self._is_aggregate_function(expr):
            return True

        for child in expr.iter_expressions():
            if self._contains_aggregate(child):
                return True

        return False

    def _is_aggregate_function(self, expr: exp.Expression) -> bool:
        """Check if expression is an aggregate function."""
        aggregate_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        return isinstance(expr, aggregate_types)

    def _convert_function(self, func_expr: exp.Expression) -> FunctionCall:
        """Convert aggregate function expression."""
        func_name = type(func_expr).__name__.upper()
        args = []

        if hasattr(func_expr, 'this') and func_expr.this:
            args.append(self._convert_expression(func_expr.this))
        elif isinstance(func_expr, exp.Count) and not func_expr.this:
            args.append(ColumnRef(table=None, column="*"))

        return FunctionCall(
            function_name=func_name,
            args=args,
            is_aggregate=True,
        )

    def parse_to_logical_plan(self, sql: str) -> LogicalPlanNode:
        """Parse SQL directly to logical plan.

        Args:
            sql: SQL query string

        Returns:
            Logical plan root node
        """
        ast = self.parse(sql)
        return self.ast_to_logical_plan(ast)

    def __repr__(self) -> str:
        return f"Parser(dialect={self.dialect})"
