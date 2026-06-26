"""Expression nodes for query plans."""

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, List, Optional, Tuple
from enum import Enum

from ..model import StateModel


@lru_cache(maxsize=None)
def _identifier_needs_quoting(name: str) -> bool:
    """Whether a bare identifier must be quoted to be valid SQL.

    A reserved word (e.g. ``select``, ``order``) does not round-trip as a plain
    column name, so it must be double-quoted. Checked once per name by
    re-parsing ``SELECT <name>`` and caching the result.
    """
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import ParseError

    try:
        parsed = sqlglot.parse_one(f"SELECT {name}")
    except ParseError:
        return True
    selected = parsed.expressions[0] if parsed.expressions else None
    return not (
        isinstance(selected, exp.Column) and selected.name.upper() == name.upper()
    )


def render_identifier(name: str) -> str:
    """Render a column/identifier name, quoting it when SQL requires it."""
    if name == "*":
        return name
    if _identifier_needs_quoting(name):
        return f'"{name}"'
    return name


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
    INTERVAL = "INTERVAL"
    NULL = "NULL"


class Expression(StateModel, ABC):
    """Base class for all expressions (a mutable Pydantic model, see
    :class:`~federated_query.model.StateModel`)."""

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
        column = render_identifier(self.column)
        if self.table:
            return f"{self.table}.{column}"
        return column

    def __repr__(self) -> str:
        if self.table:
            return f"ColumnRef({self.table}.{self.column})"
        return f"ColumnRef({self.column})"


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
            # Escape embedded single quotes (O'Brien -> 'O''Brien') so the
            # remote SQL stays valid and is not an injection vector.
            escaped = str(self.value).replace("'", "''")
            return f"'{escaped}'"
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

    # Null-safe comparison (NULL is treated as a comparable value)
    NULL_SAFE_EQ = "IS NOT DISTINCT FROM"
    NULL_SAFE_NEQ = "IS DISTINCT FROM"

    # Logical
    AND = "AND"
    OR = "OR"

    # String
    CONCAT = "||"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    REGEX_MATCH = "~"  # POSIX regex match
    REGEX_IMATCH = "~*"  # case-insensitive POSIX regex match


class BinaryOp(Expression):
    """Binary operation expression."""

    op: BinaryOpType
    left: Expression
    right: Expression

    def get_type(self) -> DataType:
        # Logical and comparison operators return boolean
        if self.op in (
            BinaryOpType.AND,
            BinaryOpType.OR,
            BinaryOpType.EQ,
            BinaryOpType.NEQ,
            BinaryOpType.LT,
            BinaryOpType.LTE,
            BinaryOpType.GT,
            BinaryOpType.GTE,
            BinaryOpType.LIKE,
            BinaryOpType.ILIKE,
            BinaryOpType.NULL_SAFE_EQ,
            BinaryOpType.NULL_SAFE_NEQ,
            BinaryOpType.REGEX_MATCH,
            BinaryOpType.REGEX_IMATCH,
        ):
            return DataType.BOOLEAN

        # String concatenation always produces text
        if self.op == BinaryOpType.CONCAT:
            return DataType.VARCHAR

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


class FunctionCall(Expression):
    """Function call expression."""

    function_name: str
    args: List[Expression]
    is_aggregate: bool = False
    distinct: bool = False
    # Ordered-set aggregates (e.g. PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x))
    # carry their sort key here; None for ordinary aggregates.
    within_group_key: Optional[Expression] = None
    within_group_desc: bool = False

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
        args_sql_parts = []
        for arg in self.args:
            args_sql_parts.append(arg.to_sql())
        args_sql = ", ".join(args_sql_parts)
        prefix = ""
        if self.distinct:
            prefix = "DISTINCT "
        call = f"{self.function_name}({prefix}{args_sql})"
        if self.within_group_key is None:
            return call
        return f"{call} WITHIN GROUP (ORDER BY {self._within_group_order_sql()})"

    def _within_group_order_sql(self) -> str:
        """Render the ORDER BY key of an ordered-set aggregate's WITHIN GROUP."""
        direction = " DESC" if self.within_group_desc else ""
        return f"{self.within_group_key.to_sql()}{direction}"

    def __repr__(self) -> str:
        return f"FunctionCall({self.function_name}, {self.args})"


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


class InList(Expression):
    """IN list expression."""

    value: Expression
    options: List[Expression]

    def get_type(self) -> DataType:
        return DataType.BOOLEAN

    def accept(self, visitor):
        return visitor.visit_in_list(self)

    def to_sql(self) -> str:
        values = []
        for option in self.options:
            values.append(option.to_sql())
        joined = ", ".join(values)
        return f"({self.value.to_sql()} IN ({joined}))"

    def __repr__(self) -> str:
        return f"InList(options={len(self.options)})"


class BetweenExpression(Expression):
    """BETWEEN expression."""

    value: Expression
    lower: Expression
    upper: Expression

    def get_type(self) -> DataType:
        return DataType.BOOLEAN

    def accept(self, visitor):
        return visitor.visit_between(self)

    def to_sql(self) -> str:
        value_sql = self.value.to_sql()
        lower_sql = self.lower.to_sql()
        upper_sql = self.upper.to_sql()
        return f"({value_sql} BETWEEN {lower_sql} AND {upper_sql})"

    def __repr__(self) -> str:
        return "BetweenExpression()"


class Cast(Expression):
    """``CAST(expr AS target_type)`` type conversion.

    ``target_type`` keeps the original SQL type text (e.g. ``VARCHAR`` or
    ``DECIMAL(10, 2)``) so the cast re-renders verbatim when pushed to a
    remote source. ``data_type`` holds the resolved engine type, set during
    binding for local evaluation.
    """

    expr: Expression
    target_type: str
    data_type: Optional[DataType] = None

    def get_type(self) -> DataType:
        if self.data_type is None:
            raise NotImplementedError("Cast type must be set during binding")
        return self.data_type

    def accept(self, visitor):
        return visitor.visit_cast(self)

    def to_sql(self) -> str:
        return f"CAST({self.expr.to_sql()} AS {self.target_type})"

    def __repr__(self) -> str:
        return f"Cast({self.expr} AS {self.target_type})"


class WindowExpr(Expression):
    """A window function: ``function OVER (PARTITION BY ... ORDER BY ... <frame>)``.

    Renders verbatim to SQL so it pushes to a source or runs in the merge engine
    unchanged (Postgres/ClickHouse/DuckDB all evaluate window SQL natively).
    Ordering mirrors the Sort node: parallel ``order_keys`` / ``order_ascending``
    / ``order_nulls`` lists. ``frame`` keeps the raw frame clause text (``ROWS
    BETWEEN ...``) for verbatim re-rendering, or None when absent.
    """

    function: Expression
    partition_by: List[Expression]
    order_keys: List[Expression]
    order_ascending: List[bool]
    order_nulls: List[Optional[str]]
    frame: Optional[str] = None

    def get_type(self) -> DataType:
        """The window's type is the type of its underlying function."""
        return self.function.get_type()

    def accept(self, visitor):
        """Dispatch to the visitor's window hook."""
        return visitor.visit_window_expr(self)

    def to_sql(self) -> str:
        """Render ``function OVER (PARTITION BY ... ORDER BY ... <frame>)``."""
        return f"{self.function.to_sql()} OVER ({self._over_clause()})"

    def _over_clause(self) -> str:
        """Assemble the PARTITION BY / ORDER BY / frame body of the OVER clause."""
        parts = []
        if self.partition_by:
            parts.append(f"PARTITION BY {self._partition_sql()}")
        if self.order_keys:
            parts.append(f"ORDER BY {self._order_sql()}")
        if self.frame:
            parts.append(self.frame)
        return " ".join(parts)

    def _partition_sql(self) -> str:
        """Render the comma-separated PARTITION BY expressions."""
        items = []
        for expr in self.partition_by:
            items.append(expr.to_sql())
        return ", ".join(items)

    def _order_sql(self) -> str:
        """Render the comma-separated ORDER BY items with direction and NULLS."""
        items = []
        for index, key in enumerate(self.order_keys):
            items.append(self._order_item_sql(index, key))
        return ", ".join(items)

    def _order_item_sql(self, index: int, key: Expression) -> str:
        """Render one ORDER BY item: ``expr [DESC] [NULLS FIRST|LAST]``."""
        text = key.to_sql()
        if not self.order_ascending[index]:
            text += " DESC"
        nulls = self.order_nulls[index]
        if nulls is not None:
            text += f" NULLS {nulls}"
        return text

    def __repr__(self) -> str:
        return f"WindowExpr({self.function})"


class Extract(Expression):
    """``EXTRACT(field FROM source)`` date/time field extraction.

    ``field`` is the unit keyword (``YEAR``, ``MONTH`` ...); ``source`` is the
    date/time expression it is taken from. The result is a numeric value.
    """

    field: str
    source: Expression

    def get_type(self) -> DataType:
        """EXTRACT yields a numeric component, modelled as a double."""
        return DataType.DOUBLE

    def accept(self, visitor):
        """Dispatch to the visitor's extract hook."""
        return visitor.visit_extract(self)

    def to_sql(self) -> str:
        """Render the keyword-argument EXTRACT syntax."""
        return f"EXTRACT({self.field} FROM {self.source.to_sql()})"

    def __repr__(self) -> str:
        return f"Extract({self.field} FROM {self.source})"


class Interval(Expression):
    """An ``INTERVAL 'value unit'`` literal such as ``INTERVAL '30 days'``.

    ``value`` is the magnitude text (``'30'``) and ``unit`` the optional unit
    keyword (``DAYS``); some intervals carry the whole specification in
    ``value`` and leave ``unit`` as ``None``.
    """

    value: str
    unit: Optional[str]

    def get_type(self) -> DataType:
        """Interval literals carry the INTERVAL type."""
        return DataType.INTERVAL

    def accept(self, visitor):
        """Dispatch to the visitor's interval hook."""
        return visitor.visit_interval(self)

    def to_sql(self) -> str:
        """Render the INTERVAL literal with its unit when present."""
        if self.unit:
            return f"INTERVAL '{self.value} {self.unit}'"
        return f"INTERVAL '{self.value}'"

    def __repr__(self) -> str:
        return f"Interval({self.value} {self.unit})"


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

    @abstractmethod
    def visit_in_list(self, expr: InList):
        pass

    @abstractmethod
    def visit_between(self, expr: BetweenExpression):
        pass

    @abstractmethod
    def visit_cast(self, expr: "Cast"):
        pass

    @abstractmethod
    def visit_extract(self, expr: "Extract"):
        pass

    @abstractmethod
    def visit_interval(self, expr: "Interval"):
        pass

    @abstractmethod
    def visit_subquery(self, expr: "SubqueryExpression"):
        pass

    @abstractmethod
    def visit_exists(self, expr: "ExistsExpression"):
        pass

    @abstractmethod
    def visit_in_subquery(self, expr: "InSubquery"):
        pass

    @abstractmethod
    def visit_quantified_comparison(self, expr: "QuantifiedComparison"):
        pass

    @abstractmethod
    def visit_tuple(self, expr: "TupleExpression"):
        pass


class Quantifier(Enum):
    """Quantifiers for quantified comparisons."""

    ANY = "ANY"
    SOME = "SOME"
    ALL = "ALL"


class SubqueryExpression(Expression):
    """Scalar subquery expression."""

    # LogicalPlanNode; typed Any because logical.py imports this module,
    # so the real type would be a runtime import cycle (it was never
    # validated as a dataclass field either).
    subquery: Any

    def get_type(self) -> DataType:
        return DataType.NULL

    def accept(self, visitor):
        return visitor.visit_subquery(self)

    def to_sql(self) -> str:
        return f"({self.subquery})"

    def __repr__(self) -> str:
        return "SubqueryExpression()"


class ExistsExpression(Expression):
    """EXISTS or NOT EXISTS predicate."""

    # LogicalPlanNode; typed Any because logical.py imports this module,
    # so the real type would be a runtime import cycle (it was never
    # validated as a dataclass field either).
    subquery: Any
    negated: bool = False

    def get_type(self) -> DataType:
        return DataType.BOOLEAN

    def accept(self, visitor):
        return visitor.visit_exists(self)

    def to_sql(self) -> str:
        prefix = "NOT " if self.negated else ""
        return f"{prefix}EXISTS({self.subquery})"

    def __repr__(self) -> str:
        prefix = "NOT " if self.negated else ""
        return f"{prefix}ExistsExpression()"


class InSubquery(Expression):
    """IN or NOT IN predicate with subquery."""

    value: Expression
    # LogicalPlanNode; typed Any because logical.py imports this module,
    # so the real type would be a runtime import cycle (it was never
    # validated as a dataclass field either).
    subquery: Any
    negated: bool = False

    def get_type(self) -> DataType:
        return DataType.BOOLEAN

    def accept(self, visitor):
        return visitor.visit_in_subquery(self)

    def to_sql(self) -> str:
        prefix = "NOT " if self.negated else ""
        return f"({self.value.to_sql()} {prefix}IN ({self.subquery}))"

    def __repr__(self) -> str:
        prefix = "NOT " if self.negated else ""
        return f"{prefix}InSubquery()"


class TupleExpression(Expression):
    """Row value constructor such as ``(u.city, u.country)``.

    Only meaningful as the left-hand side of a tuple IN subquery; the
    decorrelator expands it into per-column predicates. It is never
    evaluated directly.
    """

    items: Tuple[Expression, ...]

    def get_type(self) -> DataType:
        return DataType.NULL

    def accept(self, visitor):
        return visitor.visit_tuple(self)

    def to_sql(self) -> str:
        parts = []
        for item in self.items:
            parts.append(item.to_sql())
        joined = ", ".join(parts)
        return f"({joined})"

    def __repr__(self) -> str:
        return f"TupleExpression({len(self.items)} items)"


class QuantifiedComparison(Expression):
    """Quantified comparison such as > ANY or = ALL."""

    operator: BinaryOpType
    quantifier: Quantifier
    left: Expression
    # LogicalPlanNode; typed Any because logical.py imports this module,
    # so the real type would be a runtime import cycle (it was never
    # validated as a dataclass field either).
    subquery: Any

    def get_type(self) -> DataType:
        return DataType.BOOLEAN

    def accept(self, visitor):
        return visitor.visit_quantified_comparison(self)

    def to_sql(self) -> str:
        return (
            f"({self.left.to_sql()} {self.operator.value} "
            f"{self.quantifier.value} ({self.subquery}))"
        )

    def __repr__(self) -> str:
        return (
            f"QuantifiedComparison({self.operator.value}, " f"{self.quantifier.value})"
        )
