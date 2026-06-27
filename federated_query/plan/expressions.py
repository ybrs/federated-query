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


# ---------------------------------------------------------------------------
# Shared expression-tree utilities - the SINGLE home for these operations.
# Every walker / rebuilder / collector / predicate-combiner in the engine routes
# through these, so there is exactly one implementation of each. Do not add a
# second copy elsewhere (enforced by tests/test_no_duplicate_helpers.py).
# ---------------------------------------------------------------------------

# Expression nodes that carry a nested subquery plan. Generic expression walks
# do NOT descend into these: a subquery's references belong to its inner scope.
SUBQUERY_NODE_TYPES = (
    SubqueryExpression,
    ExistsExpression,
    InSubquery,
    QuantifiedComparison,
)


def is_aggregate_call(expr: Expression) -> bool:
    """Whether an expression is an aggregate function call (SUM/COUNT/...)."""
    return isinstance(expr, FunctionCall) and expr.is_aggregate


def expression_children(expr: Expression) -> List[Expression]:
    """Direct child expressions of a node.

    Leaves (ColumnRef/Literal/Interval) and subquery nodes return []; a generic
    walk never descends into a subquery's own plan.
    """
    if isinstance(expr, BinaryOp):
        return [expr.left, expr.right]
    if isinstance(expr, UnaryOp):
        return [expr.operand]
    if isinstance(expr, Cast):
        return [expr.expr]
    if isinstance(expr, Extract):
        return [expr.source]
    if isinstance(expr, FunctionCall):
        children = list(expr.args)
        if expr.within_group_key is not None:
            children.append(expr.within_group_key)
        return children
    if isinstance(expr, CaseExpr):
        return _case_children(expr)
    if isinstance(expr, InList):
        return [expr.value] + list(expr.options)
    if isinstance(expr, BetweenExpression):
        return [expr.value, expr.lower, expr.upper]
    if isinstance(expr, TupleExpression):
        return list(expr.items)
    if isinstance(expr, WindowExpr):
        return [expr.function] + list(expr.partition_by) + list(expr.order_keys)
    return []


def _case_children(expr: "CaseExpr") -> List[Expression]:
    """Condition/result expressions of a CASE, plus its ELSE."""
    children: List[Expression] = []
    for condition, result in expr.when_clauses:
        children.append(condition)
        children.append(result)
    if expr.else_result is not None:
        children.append(expr.else_result)
    return children


def map_children(expr: Expression, fn) -> Expression:
    """Rebuild ``expr`` with ``fn`` applied to each child expression.

    Uses model_copy so no field is ever dropped; leaves and subquery nodes are
    returned unchanged.
    """
    if isinstance(expr, BinaryOp):
        return expr.model_copy(update={"left": fn(expr.left), "right": fn(expr.right)})
    if isinstance(expr, UnaryOp):
        return expr.model_copy(update={"operand": fn(expr.operand)})
    if isinstance(expr, Cast):
        return expr.model_copy(update={"expr": fn(expr.expr)})
    if isinstance(expr, Extract):
        return expr.model_copy(update={"source": fn(expr.source)})
    if isinstance(expr, FunctionCall):
        return _map_function_children(expr, fn)
    if isinstance(expr, CaseExpr):
        return _map_case_children(expr, fn)
    if isinstance(expr, InList):
        return expr.model_copy(
            update={"value": fn(expr.value), "options": _map_list(expr.options, fn)}
        )
    if isinstance(expr, BetweenExpression):
        return expr.model_copy(
            update={
                "value": fn(expr.value),
                "lower": fn(expr.lower),
                "upper": fn(expr.upper),
            }
        )
    if isinstance(expr, TupleExpression):
        return expr.model_copy(update={"items": tuple(_map_list(expr.items, fn))})
    if isinstance(expr, WindowExpr):
        return expr.model_copy(
            update={
                "function": fn(expr.function),
                "partition_by": _map_list(expr.partition_by, fn),
                "order_keys": _map_list(expr.order_keys, fn),
            }
        )
    return expr


def _map_list(items, fn) -> List[Expression]:
    """Apply fn to each expression in a list."""
    result: List[Expression] = []
    for item in items:
        result.append(fn(item))
    return result


def _map_function_children(expr: "FunctionCall", fn) -> "FunctionCall":
    """Rebuild a FunctionCall's args and WITHIN GROUP key."""
    new_key = None
    if expr.within_group_key is not None:
        new_key = fn(expr.within_group_key)
    return expr.model_copy(
        update={"args": _map_list(expr.args, fn), "within_group_key": new_key}
    )


def _map_case_children(expr: "CaseExpr", fn) -> "CaseExpr":
    """Rebuild a CASE's branch conditions/results and ELSE."""
    new_when = []
    for condition, result in expr.when_clauses:
        new_when.append((fn(condition), fn(result)))
    new_else = None
    if expr.else_result is not None:
        new_else = fn(expr.else_result)
    return expr.model_copy(update={"when_clauses": new_when, "else_result": new_else})


def column_refs(expr: Expression) -> List[ColumnRef]:
    """Every ColumnRef in an expression tree (not descending into subqueries)."""
    refs: List[ColumnRef] = []
    if isinstance(expr, ColumnRef):
        refs.append(expr)
    for child in expression_children(expr):
        refs.extend(column_refs(child))
    return refs


def split_conjuncts(expr: Expression) -> List[Expression]:
    """Flatten the top-level AND chain of a predicate into its conjuncts."""
    if isinstance(expr, BinaryOp) and expr.op == BinaryOpType.AND:
        return split_conjuncts(expr.left) + split_conjuncts(expr.right)
    return [expr]


def split_disjuncts(expr: Expression) -> List[Expression]:
    """Flatten the top-level OR chain of a predicate into its disjuncts."""
    if isinstance(expr, BinaryOp) and expr.op == BinaryOpType.OR:
        return split_disjuncts(expr.left) + split_disjuncts(expr.right)
    return [expr]


def combine_and(terms: List[Expression]) -> Optional[Expression]:
    """AND a list of predicates into one expression, or None when empty."""
    return _combine(terms, BinaryOpType.AND)


def combine_or(terms: List[Expression]) -> Optional[Expression]:
    """OR a list of predicates into one expression, or None when empty."""
    return _combine(terms, BinaryOpType.OR)


def _combine(terms: List[Expression], op: "BinaryOpType") -> Optional[Expression]:
    """Left-fold a list of predicates with a binary operator."""
    if not terms:
        return None
    result = terms[0]
    for term in terms[1:]:
        result = BinaryOp(op=op, left=result, right=term)
    return result


def and_expressions(
    left: Optional[Expression], right: Optional[Expression]
) -> Optional[Expression]:
    """None-safe AND of two predicate expressions."""
    if left is None:
        return right
    if right is None:
        return left
    return BinaryOp(op=BinaryOpType.AND, left=left, right=right)


def contains_aggregate(expr: Expression) -> bool:
    """Whether an expression tree contains an aggregate function call."""
    if is_aggregate_call(expr):
        return True
    for child in expression_children(expr):
        if contains_aggregate(child):
            return True
    return False


def aggregate_output_map(output_names, aggregates) -> dict:
    """Map each aggregate-function output name to its aggregate expression.

    Relies on positional alignment of output_names and aggregates (the contract
    every aggregate-bearing scan/node uses).
    """
    mapping = {}
    for index in range(len(output_names)):
        expression = aggregates[index]
        if is_aggregate_call(expression):
            mapping[output_names[index]] = expression
    return mapping


def split_where_having(predicate, output_map):
    """Split a merged scan predicate into (where_predicate, having_predicate).

    This is the SINGLE source of truth for WHERE-vs-HAVING placement. When a
    GROUP BY query is folded into one scan, its WHERE and HAVING conjuncts live
    together in the scan filter. A conjunct is a HAVING term if it references an
    aggregate-output column (a key of ``output_map``, e.g. the alias ``total``
    the binder rewrote ``SUM(x)`` to) OR directly contains an aggregate function
    call; its aggregate-output refs are substituted back to the aggregate
    expression so the source sees ``HAVING SUM(x) > 10`` rather than an alias in
    WHERE. Every other conjunct is WHERE. Either side may be None.
    """
    where_terms = []
    having_terms = []
    for conjunct in split_conjuncts(predicate):
        if _is_having_conjunct(conjunct, output_map):
            having_terms.append(_substitute_aggregate_refs(conjunct, output_map))
        else:
            where_terms.append(conjunct)
    return combine_and(where_terms), combine_and(having_terms)


def _is_having_conjunct(expr: Expression, output_map) -> bool:
    """Whether a conjunct belongs in HAVING (references an aggregate)."""
    if contains_aggregate(expr):
        return True
    for ref in column_refs(expr):
        if ref.column in output_map:
            return True
    return False


def _substitute_aggregate_refs(expr: Expression, output_map) -> Expression:
    """Replace aggregate-output column refs with their aggregate expressions."""
    if isinstance(expr, ColumnRef):
        return output_map.get(expr.column, expr)
    return map_children(expr, lambda child: _substitute_aggregate_refs(child, output_map))
