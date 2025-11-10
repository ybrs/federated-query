"""Expression nodes for query plans."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple
from enum import Enum


class DataType(Enum):
    """SQL data types."""

    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    NULL = "NULL"


class Expression(ABC):
    """Base class for all expressions."""

    @abstractmethod
    def get_type(self) -> DataType:
        """Get the data type of this expression."""
        pass

    @abstractmethod
    def accept(self, visitor):
        """Accept a visitor for the visitor pattern."""
        pass

    @abstractmethod
    def to_sql(self) -> str:
        """Convert expression to SQL string."""
        pass


@dataclass(frozen=True)
class ColumnRef(Expression):
    """Column reference expression."""

    table: Optional[str]  # Can be None for unqualified references
    column: str
    data_type: Optional[DataType] = None  # Set during binding

    def get_type(self) -> DataType:
        if self.data_type is None:
            raise NotImplementedError("Type must be set during binding")
        return self.data_type

    def accept(self, visitor):
        return visitor.visit_column_ref(self)

    def to_sql(self) -> str:
        if self.table:
            return f"{self.table}.{self.column}"
        return self.column

    def __repr__(self) -> str:
        return f"ColumnRef({self.table}.{self.column})" if self.table else f"ColumnRef({self.column})"


@dataclass(frozen=True)
class Literal(Expression):
    """Literal value expression."""

    value: Any
    data_type: DataType

    def get_type(self) -> DataType:
        return self.data_type

    def accept(self, visitor):
        return visitor.visit_literal(self)

    def to_sql(self) -> str:
        if self.value is None:
            return "NULL"
        if self.data_type in (DataType.VARCHAR, DataType.TEXT):
            return f"'{self.value}'"
        return str(self.value)

    def __repr__(self) -> str:
        return f"Literal({self.value})"


class BinaryOpType(Enum):
    """Binary operator types."""

    # Arithmetic
    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    MODULO = "%"

    # Comparison
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="

    # Logical
    AND = "AND"
    OR = "OR"

    # String
    CONCAT = "||"
    LIKE = "LIKE"


@dataclass(frozen=True)
class BinaryOp(Expression):
    """Binary operation expression."""

    op: BinaryOpType
    left: Expression
    right: Expression

    def get_type(self) -> DataType:
        # Logical and comparison operators return boolean
        if self.op in (BinaryOpType.AND, BinaryOpType.OR, BinaryOpType.EQ,
                       BinaryOpType.NEQ, BinaryOpType.LT, BinaryOpType.LTE,
                       BinaryOpType.GT, BinaryOpType.GTE, BinaryOpType.LIKE):
            return DataType.BOOLEAN

        # Arithmetic operators inherit type from operands
        # (simplified - real implementation needs type coercion)
        return self.left.get_type()

    def accept(self, visitor):
        return visitor.visit_binary_op(self)

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} {self.op.value} {self.right.to_sql()})"

    def __repr__(self) -> str:
        return f"BinaryOp({self.op.value}, {self.left}, {self.right})"


class UnaryOpType(Enum):
    """Unary operator types."""

    NOT = "NOT"
    NEGATE = "-"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass(frozen=True)
class UnaryOp(Expression):
    """Unary operation expression."""

    op: UnaryOpType
    operand: Expression

    def get_type(self) -> DataType:
        if self.op in (UnaryOpType.NOT, UnaryOpType.IS_NULL, UnaryOpType.IS_NOT_NULL):
            return DataType.BOOLEAN
        return self.operand.get_type()

    def accept(self, visitor):
        return visitor.visit_unary_op(self)

    def to_sql(self) -> str:
        if self.op in (UnaryOpType.IS_NULL, UnaryOpType.IS_NOT_NULL):
            return f"({self.operand.to_sql()} {self.op.value})"
        return f"({self.op.value} {self.operand.to_sql()})"

    def __repr__(self) -> str:
        return f"UnaryOp({self.op.value}, {self.operand})"


@dataclass(frozen=True)
class FunctionCall(Expression):
    """Function call expression."""

    function_name: str
    args: List[Expression]
    is_aggregate: bool = False

    def get_type(self) -> DataType:
        # Type depends on function - needs catalog lookup
        # Simplified for now
        if self.function_name.upper() in ("COUNT", "SUM"):
            return DataType.BIGINT
        if self.function_name.upper() in ("AVG"):
            return DataType.DOUBLE
        return DataType.VARCHAR  # Default

    def accept(self, visitor):
        return visitor.visit_function_call(self)

    def to_sql(self) -> str:
        args_sql = ", ".join(arg.to_sql() for arg in self.args)
        return f"{self.function_name}({args_sql})"

    def __repr__(self) -> str:
        return f"FunctionCall({self.function_name}, {self.args})"


@dataclass(frozen=True)
class CaseExpr(Expression):
    """CASE expression."""

    when_clauses: List[Tuple[Expression, Expression]]  # (condition, result)
    else_result: Optional[Expression]

    def get_type(self) -> DataType:
        # Return type is the type of the first result expression
        if self.when_clauses:
            return self.when_clauses[0][1].get_type()
        if self.else_result:
            return self.else_result.get_type()
        return DataType.NULL

    def accept(self, visitor):
        return visitor.visit_case_expr(self)

    def to_sql(self) -> str:
        sql = "CASE"
        for condition, result in self.when_clauses:
            sql += f" WHEN {condition.to_sql()} THEN {result.to_sql()}"
        if self.else_result:
            sql += f" ELSE {self.else_result.to_sql()}"
        sql += " END"
        return sql

    def __repr__(self) -> str:
        return f"CaseExpr(when_count={len(self.when_clauses)})"


class ExpressionVisitor(ABC):
    """Visitor interface for expressions."""

    @abstractmethod
    def visit_column_ref(self, expr: ColumnRef):
        pass

    @abstractmethod
    def visit_literal(self, expr: Literal):
        pass

    @abstractmethod
    def visit_binary_op(self, expr: BinaryOp):
        pass

    @abstractmethod
    def visit_unary_op(self, expr: UnaryOp):
        pass

    @abstractmethod
    def visit_function_call(self, expr: FunctionCall):
        pass

    @abstractmethod
    def visit_case_expr(self, expr: CaseExpr):
        pass
