"""SQL parser using sqlglot."""

import sqlglot
from sqlglot import exp
from typing import Optional, List, Tuple
from ..plan.logical import LogicalPlanNode, Scan, Project, Filter, Limit
from ..plan.expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    BinaryOpType,
    UnaryOp,
    UnaryOpType,
    DataType,
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
        plan = self._build_select_clause(select, plan)
        plan = self._build_limit_clause(select, plan)
        return plan

    def _build_from_clause(self, select: exp.Select) -> LogicalPlanNode:
        """Build scan node from FROM clause.

        Args:
            select: sqlglot Select node

        Returns:
            Scan node
        """
        from_clause = select.args.get("from")
        if not from_clause:
            raise ValueError("SELECT must have FROM clause")

        table_expr = from_clause.this
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
        """Build project node from SELECT clause.

        Args:
            select: sqlglot Select node
            input_plan: Input logical plan

        Returns:
            Logical plan with projection
        """
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
