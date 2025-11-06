"""SQL parser using sqlglot."""

import sqlglot
from sqlglot import exp
from typing import Optional
from ..plan.logical import LogicalPlanNode


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
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            return parsed
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")

    def ast_to_logical_plan(self, ast: exp.Expression) -> LogicalPlanNode:
        """Convert sqlglot AST to logical plan.

        This will be implemented to walk the AST and build logical plan nodes.

        Args:
            ast: sqlglot expression tree

        Returns:
            Logical plan root node
        """
        # TODO: Implement AST to logical plan conversion
        # This is a major component that will be implemented in Phase 1
        raise NotImplementedError("AST to logical plan conversion not yet implemented")

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
