"""SQL parser using sqlglot."""

from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import sqlglot
from sqlglot import exp

from ..plan.logical import (
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    Limit,
    Join,
    JoinType,
    Aggregate,
    Explain,
    ExplainFormat,
)
from ..catalog.catalog import Catalog
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
    InList,
    BetweenExpression,
    CaseExpr,
)

if TYPE_CHECKING:
    from ..processor.query_executor import QueryExecutor


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
        if isinstance(ast, exp.Command) and self._is_explain_command(ast):
            return self._convert_explain(ast)
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
        plan = self._build_order_by_clause(select, plan)
        plan = self._build_limit_clause(select, plan)
        return plan

    def _convert_explain(self, command: exp.Command) -> LogicalPlanNode:
        """Convert EXPLAIN statement to logical plan."""
        query_ast, explain_format = self._extract_explain_parts(command)
        child_plan = self.ast_to_logical_plan(query_ast)
        return Explain(input=child_plan, format=explain_format)

    def _extract_explain_parts(
        self, command: exp.Command
    ) -> Tuple[exp.Expression, ExplainFormat]:
        """Extract wrapped statement and format."""
        expression = command.args.get("expression")
        if expression is None:
            raise ValueError("EXPLAIN requires a statement to describe")
        if isinstance(expression, exp.Literal) and expression.is_string:
            sql_text = expression.this.strip()
            explain_format, sql_body = self._parse_explain_options(sql_text)
            parsed = sqlglot.parse_one(sql_body, dialect=self.dialect)
            return parsed, explain_format
        if isinstance(expression, exp.Expression):
            return expression, ExplainFormat.TEXT
        raise ValueError(f"Unsupported EXPLAIN expression: {type(expression)}")

    def _parse_explain_options(
        self, sql_text: str
    ) -> Tuple[ExplainFormat, str]:
        """Parse optional EXPLAIN options prefix."""
        if not sql_text.startswith("("):
            return ExplainFormat.TEXT, sql_text
        options, remainder = self._split_option_clause(sql_text)
        explain_format = self._resolve_explain_format(options)
        cleaned_sql = remainder.strip()
        return explain_format, cleaned_sql

    def _split_option_clause(self, sql_text: str) -> Tuple[str, str]:
        """Split '(...) rest' into option text and remainder."""
        depth = 0
        start_index = -1
        end_index = -1
        for position, char in enumerate(sql_text):
            if char == "(":
                if depth == 0:
                    start_index = position
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    end_index = position
                    break
        if end_index == -1 or start_index == -1:
            raise ValueError("EXPLAIN options missing closing parenthesis")
        options = sql_text[start_index + 1 : end_index]
        remainder = sql_text[end_index + 1 :]
        return options, remainder

    def _resolve_explain_format(self, options: str) -> ExplainFormat:
        """Determine requested EXPLAIN format."""
        normalized = options.replace("=", " ")
        normalized = normalized.replace(",", " ")
        tokens = normalized.split()
        index = 0
        while index < len(tokens):
            token = tokens[index].upper()
            if token in ("FORMAT", "AS") and index + 1 < len(tokens):
                value = tokens[index + 1].upper()
                if value == "JSON":
                    return ExplainFormat.JSON
                if value == "TEXT":
                    return ExplainFormat.TEXT
            index += 1
        return ExplainFormat.TEXT

    def _is_explain_command(self, command: exp.Command) -> bool:
        """Check if a sqlglot Command represents EXPLAIN."""
        keyword = command.args.get("this")
        if keyword is None:
            return False
        return str(keyword).upper() == "EXPLAIN"

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
        table_alias = self._extract_table_alias(table_expr)
        all_columns = self._collect_needed_columns(select)

        datasource = table_parts[0]
        schema_name = table_parts[1]
        table_name = table_parts[2]

        return Scan(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            columns=all_columns,
            alias=table_alias,
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
        column_usage = self._collect_join_column_usage(select)

        left_table_parts = self._extract_table_parts(left_table)
        left_table_alias = self._extract_table_alias(left_table)
        left_columns = self._columns_for_join_table(
            column_usage, left_table_alias
        )

        left_plan = Scan(
            datasource=left_table_parts[0],
            schema_name=left_table_parts[1],
            table_name=left_table_parts[2],
            columns=left_columns,
            alias=left_table_alias,
        )

        current_plan = left_plan

        for join_clause in joins:
            right_table = join_clause.this
            right_table_parts = self._extract_table_parts(right_table)
            right_table_alias = self._extract_table_alias(right_table)
            right_columns = self._columns_for_join_table(
                column_usage, right_table_alias
            )

            right_plan = Scan(
                datasource=right_table_parts[0],
                schema_name=right_table_parts[1],
                table_name=right_table_parts[2],
                columns=right_columns,
                alias=right_table_alias,
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
        alias = table_expr.alias
        if alias:
            return str(alias)
        return str(table_expr.alias_or_name)

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

    def _collect_join_column_usage(
        self, select: exp.Select
    ) -> Dict[Optional[str], List[str]]:
        """Collect column usage grouped by table alias for join planning."""
        usage: Dict[Optional[str], List[str]] = {}
        for column in select.find_all(exp.Column):
            table_name = column.table
            column_name = column.name
            if not column_name:
                continue
            if table_name is None:
                bucket = usage.setdefault(None, [])
                if column_name not in bucket:
                    bucket.append(column_name)
                continue
            bucket = usage.setdefault(table_name, [])
            if column_name not in bucket:
                bucket.append(column_name)
        return usage

    def _columns_for_join_table(
        self,
        usage: Dict[Optional[str], List[str]],
        table_alias: Optional[str],
    ) -> List[str]:
        """Return column list for a specific table in a join."""
        key = table_alias
        if key in usage:
            columns = usage[key]
            if "*" in columns:
                return ["*"]
            return list(columns)
        if None in usage and usage[None]:
            return ["*"]
        return ["*"]

    def _extract_join_type(self, join_clause: exp.Join) -> JoinType:
        """Extract join type from JOIN clause.

        Args:
            join_clause: sqlglot Join node

        Returns:
            JoinType enum value
        """
        kind = join_clause.args.get("kind")
        side = join_clause.args.get("side")

        if side:
            side_upper = side.upper()
            if side_upper == "LEFT":
                return JoinType.LEFT
            if side_upper == "RIGHT":
                return JoinType.RIGHT
            if side_upper == "FULL":
                return JoinType.FULL

        if kind:
            if kind.upper() == "CROSS":
                return JoinType.CROSS

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

    def _build_group_by_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build aggregate node from GROUP BY clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with aggregate (or original if no GROUP BY)
        """
        group = select.args.get("group")
        has_aggregates = self._has_aggregate_functions(select)

        if group:
            return self._create_aggregate_node(select, input_plan, group)

        if has_aggregates:
            return self._create_aggregate_node_without_grouping(select, input_plan)

        return input_plan

    def _has_aggregate_functions(self, select: exp.Select) -> bool:
        """Check if SELECT has aggregate functions.

        Args:
            select: sqlglot Select node

        Returns:
            True if SELECT contains aggregate functions
        """
        for expr in select.expressions:
            if self._contains_aggregate_function(expr):
                return True
        return False

    def _contains_aggregate_function(self, expr: exp.Expression) -> bool:
        """Check if expression contains aggregate function.

        Args:
            expr: sqlglot expression

        Returns:
            True if expression contains aggregate function
        """
        if self._is_aggregate_function(expr):
            return True

        for child in expr.iter_expressions():
            if self._contains_aggregate_function(child):
                return True

        return False

    def _is_aggregate_function(self, expr: exp.Expression) -> bool:
        """Check if an expression represents an aggregate function."""
        aggregate_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        return isinstance(expr, exp.AggFunc) or isinstance(expr, aggregate_types)

    def _create_aggregate_node_without_grouping(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> Aggregate:
        """Create aggregate node without GROUP BY.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Aggregate node with empty group_by
        """
        aggregates = self._extract_aggregates_from_select(select)
        output_names = self._extract_output_names(select)

        return Aggregate(
            input=input_plan,
            group_by=[],
            aggregates=aggregates,
            output_names=output_names,
        )

    def _create_aggregate_node(
        self, select: exp.Select, input_plan: LogicalPlanNode, group: exp.Group
    ) -> Aggregate:
        """Create aggregate node from SELECT and GROUP BY.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan
            group: sqlglot Group node

        Returns:
            Aggregate node
        """
        group_by_exprs = self._extract_group_by_exprs(group)
        aggregates = self._extract_aggregates_from_select(select)
        output_names = self._extract_output_names(select)

        return Aggregate(
            input=input_plan,
            group_by=group_by_exprs,
            aggregates=aggregates,
            output_names=output_names,
        )

    def _extract_group_by_exprs(self, group: exp.Group) -> List[Expression]:
        """Extract grouping expressions.

        Args:
            group: sqlglot Group node

        Returns:
            List of grouping expressions
        """
        expressions = []
        for expr in group.expressions:
            converted = self._convert_expression(expr)
            expressions.append(converted)
        return expressions

    def _extract_aggregates_from_select(self, select: exp.Select) -> List[Expression]:
        """Extract all expressions from SELECT for aggregation.

        Args:
            select: sqlglot Select node

        Returns:
            List of expressions (both group keys and aggregates)
        """
        expressions = []
        for expr in select.expressions:
            converted = self._convert_expression(expr)
            expressions.append(converted)
        return expressions

    def _extract_output_names(self, select: exp.Select) -> List[str]:
        """Extract output column names from SELECT.

        Args:
            select: sqlglot Select node

        Returns:
            List of output names
        """
        names = []
        for expr in select.expressions:
            alias = self._get_alias(expr)
            names.append(alias)
        return names

    def _build_having_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build filter node from HAVING clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with filter (or original if no HAVING)
        """
        having = select.args.get("having")
        if not having:
            return input_plan

        predicate = self._convert_expression(having.this)

        if isinstance(input_plan, Aggregate):
            predicate = self._rewrite_having_predicate(predicate, input_plan)

        return Filter(input=input_plan, predicate=predicate)

    def _build_select_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build projection node from SELECT clause."""
        if isinstance(input_plan, Aggregate):
            return input_plan

        if isinstance(input_plan, Filter) and isinstance(input_plan.input, Aggregate):
            return input_plan

        expressions = []
        aliases = []

        for select_expr in select.expressions:
            expr = self._convert_expression(select_expr)
            expressions.append(expr)

            alias = self._get_alias(select_expr)
            aliases.append(alias)

        distinct_arg = select.args.get("distinct")
        has_distinct = bool(distinct_arg)
        return Projection(
            input=input_plan,
            expressions=expressions,
            aliases=aliases,
            distinct=has_distinct,
        )

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

    def _build_order_by_clause(
        self, select: exp.Select, input_plan: LogicalPlanNode
    ) -> LogicalPlanNode:
        """Build Sort node from ORDER BY clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with Sort node (or original if no ORDER BY)
        """
        from ..plan.logical import Sort

        order = select.args.get("order")
        if not order:
            return input_plan

        sort_keys = []
        ascending = []
        nulls_order = []

        for ordered in order.expressions:
            expr = self._convert_expression(ordered.this)
            sort_keys.append(expr)

            is_desc = ordered.args.get("desc", False)
            ascending.append(not is_desc)

            nulls = ordered.args.get("nulls_first")
            if nulls is not None:
                nulls_order.append("FIRST" if nulls else "LAST")
            else:
                nulls_order.append(None)

        return Sort(
            input=input_plan,
            sort_keys=sort_keys,
            ascending=ascending,
            nulls_order=nulls_order
        )

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
        offset_value = 0
        offset_expr = select.args.get("offset")
        if offset_expr is not None:
            offset_value = int(offset_expr.expression.this)
        return Limit(input=input_plan, limit=limit_value, offset=offset_value)

    def _convert_expression(self, expr: exp.Expression) -> Expression:
        """Convert sqlglot expression to our Expression.

        Args:
            expr: sqlglot expression

        Returns:
            Our Expression object
        """
        if isinstance(expr, exp.In):
            return self._convert_in_expression(expr)
        if isinstance(expr, exp.Between):
            return self._convert_between_expression(expr)
        if isinstance(expr, exp.Column):
            return self._convert_column(expr)
        if isinstance(expr, exp.Is):
            return self._convert_is_expression(expr)
        if isinstance(expr, exp.Literal):
            return self._convert_literal(expr)
        if isinstance(expr, exp.Null):
            return Literal(value=None, data_type=DataType.NULL)
        if isinstance(expr, exp.Binary):
            return self._convert_binary(expr)
        if isinstance(expr, exp.Unary):
            return self._convert_unary(expr)
        if isinstance(expr, exp.Alias):
            return self._convert_expression(expr.this)
        if isinstance(expr, exp.Star):
            return ColumnRef(table=None, column="*")
        if isinstance(expr, exp.Case):
            return self._convert_case_expression(expr)
        if self._is_aggregate_function(expr):
            return self._convert_aggregate_function(expr)
        if isinstance(expr, (exp.Anonymous, exp.Func, exp.Upper)):
            return self._convert_function_call(expr)

        raise ValueError(f"Unsupported expression type: {type(expr)}")

    def _convert_in_expression(self, expr: exp.In) -> Expression:
        """Convert IN list to expression node."""
        value_expr = self._convert_expression(expr.this)
        options: List[Expression] = []
        for option in expr.expressions:
            converted = self._convert_expression(option)
            options.append(converted)
        return InList(value=value_expr, options=options)

    def _convert_between_expression(self, expr: exp.Between) -> Expression:
        """Convert BETWEEN to expression node."""
        value_expr = self._convert_expression(expr.this)
        low_expr = self._convert_expression(expr.args["low"])
        high_expr = self._convert_expression(expr.args["high"])
        return BetweenExpression(value=value_expr, lower=low_expr, upper=high_expr)

    def _convert_is_expression(self, expr: exp.Is) -> Expression:
        """Convert IS and IS NOT comparisons into unary operations."""
        operand = self._convert_expression(expr.this)
        comparison = expr.expression
        if isinstance(comparison, exp.Null):
            return UnaryOp(op=UnaryOpType.IS_NULL, operand=operand)
        if isinstance(comparison, exp.Not) and isinstance(comparison.this, exp.Null):
            return UnaryOp(op=UnaryOpType.IS_NOT_NULL, operand=operand)
        raise ValueError("IS comparison supports only NULL and NOT NULL")

    def _convert_case_expression(self, expr: exp.Case) -> Expression:
        """Convert CASE expression."""
        when_clauses = []
        for if_clause in expr.args.get("ifs") or []:
            condition = self._convert_expression(if_clause.this)
            result_expr = if_clause.args.get("true")
            result = self._convert_expression(result_expr) if result_expr is not None else Literal(value=None, data_type=DataType.NULL)
            when_clauses.append((condition, result))
        else_expr = None
        default_expr = expr.args.get("default")
        if default_expr is not None:
            else_expr = self._convert_expression(default_expr)
        return CaseExpr(when_clauses=when_clauses, else_result=else_expr)

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
        if isinstance(unary, exp.Paren):
            return self._convert_expression(unary.this)

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

    def _convert_aggregate_function(self, func: exp.AggFunc) -> FunctionCall:
        """Convert sqlglot aggregate function.

        Args:
            func: sqlglot AggFunc node

        Returns:
            FunctionCall expression
        """
        func_name = type(func).__name__.upper()
        distinct = bool(func.args.get("distinct"))
        if isinstance(func.this, exp.Distinct):
            distinct = True
        args = self._extract_function_args(func, distinct)
        return FunctionCall(
            function_name=func_name,
            args=args,
            is_aggregate=True,
            distinct=distinct,
        )

    def _extract_function_args(self, func: exp.AggFunc, distinct: bool) -> List[Expression]:
        """Extract arguments from function.

        Args:
            func: sqlglot AggFunc node
            distinct: True if DISTINCT modifier is present

        Returns:
            List of argument expressions
        """
        args = []
        value = getattr(func, "this", None)
        if isinstance(value, exp.Distinct):
            for child in value.expressions or []:
                converted = self._convert_expression(child)
                args.append(converted)
        elif value is not None:
            arg = self._convert_expression(value)
            args.append(arg)
        elif isinstance(func, exp.Count):
            args.append(ColumnRef(table=None, column="*"))
        return args

    def _convert_function_call(self, func: exp.Expression) -> FunctionCall:
        """Convert generic function expressions."""
        name = func.sql_name().upper()
        args: List[Expression] = []
        if hasattr(func, "this") and func.this is not None:
            args.append(self._convert_expression(func.this))
        for child in func.expressions or []:
            args.append(self._convert_expression(child))
        return FunctionCall(function_name=name, args=args, is_aggregate=False)

    def _rewrite_having_predicate(
        self, predicate: Expression, aggregate: Aggregate
    ) -> Expression:
        """Rewrite HAVING predicate to reference output columns.

        Args:
            predicate: HAVING predicate expression
            aggregate: Aggregate node

        Returns:
            Rewritten predicate with column references
        """
        agg_map = self._build_aggregate_mapping(aggregate)
        return self._rewrite_expression(predicate, agg_map)

    def _build_aggregate_mapping(self, aggregate: Aggregate) -> Dict[str, str]:
        """Build mapping from aggregate expressions to output names.

        Args:
            aggregate: Aggregate node

        Returns:
            Dictionary mapping aggregate expression SQL to column name
        """
        mapping = {}

        for i in range(len(aggregate.aggregates)):
            agg_expr = aggregate.aggregates[i]
            output_name = aggregate.output_names[i]

            if isinstance(agg_expr, FunctionCall) and agg_expr.is_aggregate:
                agg_key = self._expr_to_key(agg_expr)
                mapping[agg_key] = output_name

        return mapping

    def _expr_to_key(self, expr: Expression) -> str:
        """Convert expression to string key for matching.

        Args:
            expr: Expression to convert

        Returns:
            String key for this expression
        """
        if isinstance(expr, FunctionCall):
            args_keys = [self._expr_to_key(arg) for arg in expr.args]
            args_str = ",".join(args_keys)
            return f"{expr.function_name.upper()}({args_str})"
        if isinstance(expr, ColumnRef):
            return expr.column
        if isinstance(expr, Literal):
            return str(expr.value)
        return expr.to_sql()

    def _rewrite_expression(
        self, expr: Expression, agg_map: Dict[str, str]
    ) -> Expression:
        """Rewrite expression replacing aggregates with column refs.

        Args:
            expr: Expression to rewrite
            agg_map: Mapping from aggregate keys to column names

        Returns:
            Rewritten expression
        """
        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            expr_key = self._expr_to_key(expr)
            if expr_key in agg_map:
                col_name = agg_map[expr_key]
                return ColumnRef(table=None, column=col_name, data_type=expr.get_type())
            return expr

        if isinstance(expr, BinaryOp):
            left = self._rewrite_expression(expr.left, agg_map)
            right = self._rewrite_expression(expr.right, agg_map)
            return BinaryOp(op=expr.op, left=left, right=right)

        if isinstance(expr, UnaryOp):
            operand = self._rewrite_expression(expr.operand, agg_map)
            return UnaryOp(op=expr.op, operand=operand)

        return expr

    def parse_to_logical_plan(
        self,
        sql: str,
        catalog: Optional[Catalog] = None,
        query_executor: Optional["QueryExecutor"] = None,
    ) -> LogicalPlanNode:
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
