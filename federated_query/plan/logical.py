"""Logical plan nodes."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from enum import Enum

from .expressions import Expression


class JoinType(Enum):
    """Join types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"
    SEMI = "SEMI"  # For EXISTS
    ANTI = "ANTI"  # For NOT EXISTS


class AggregateFunction(Enum):
    """Aggregate function types."""

    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class SetOpKind(Enum):
    """SQL set-operation kinds."""

    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"


class ExplainFormat(Enum):
    """Supported EXPLAIN output formats."""

    TEXT = "TEXT"
    JSON = "JSON"


class LogicalPlanNode(ABC):
    """Base class for logical plan nodes."""

    @abstractmethod
    def children(self) -> List["LogicalPlanNode"]:
        """Return child nodes."""
        pass

    @abstractmethod
    def with_children(self, children: List["LogicalPlanNode"]) -> "LogicalPlanNode":
        """Create a new node with different children (immutable)."""
        pass

    @abstractmethod
    def accept(self, visitor):
        """Accept a visitor for the visitor pattern."""
        pass

    @abstractmethod
    def schema(self) -> List[str]:
        """Return output column names."""
        pass

    def __repr__(self) -> str:
        return self.__class__.__name__


@dataclass(frozen=True)
class Scan(LogicalPlanNode):
    """Scan a table from a data source.

    Can represent:
    - Simple scan: SELECT columns FROM table
    - Scan with filter: SELECT columns FROM table WHERE filter
    - Scan with aggregates: SELECT group_by, agg_funcs FROM table WHERE filter GROUP BY group_by
    - Scan with order by: SELECT columns FROM table ORDER BY sort_keys
    """

    datasource: str
    schema_name: str
    table_name: str
    columns: List[str]  # Columns to read
    filters: Optional[Expression] = None  # Optional pushed-down filters
    alias: Optional[str] = None  # Table alias (e.g., "u" in "FROM users u")
    group_by: Optional[List[Expression]] = None  # Optional GROUP BY expressions
    aggregates: Optional[List[Expression]] = None  # Optional aggregate expressions
    output_names: Optional[List[str]] = (
        None  # Output column names when using aggregates
    )
    limit: Optional[int] = None
    offset: int = 0
    order_by_keys: Optional[List[Expression]] = None  # ORDER BY expressions
    order_by_ascending: Optional[List[bool]] = None  # ASC/DESC for each key
    order_by_nulls: Optional[List[Optional[str]]] = (
        None  # NULLS FIRST/LAST for each key
    )
    distinct: bool = False

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "Scan":
        assert len(children) == 0
        return self

    def accept(self, visitor):
        return visitor.visit_scan(self)

    def schema(self) -> List[str]:
        if self.output_names:
            return self.output_names
        return self.columns

    def __repr__(self) -> str:
        """Return string representation of Scan node."""
        table_ref = f"{self.datasource}.{self.schema_name}.{self.table_name}"
        filter_str = f" WHERE {self.filters}" if self.filters else ""
        agg_str = ""
        if self.aggregates:
            agg_str = f", aggs={len(self.aggregates)}"
        if self.group_by:
            agg_str += f", group_by={len(self.group_by)}"
        order_str = ""
        if self.order_by_keys:
            order_str = f", order_by={len(self.order_by_keys)} keys"
        limit_str = ""
        if self.limit is not None:
            limit_str = f", limit={self.limit}, offset={self.offset}"
        distinct_str = ""
        if self.distinct:
            distinct_str = ", distinct"
        return (
            f"Scan({table_ref}, cols={len(self.columns)}{filter_str}"
            f"{agg_str}{order_str}{limit_str}{distinct_str})"
        )


@dataclass(frozen=True)
class Projection(LogicalPlanNode):
    """Projection (select) specific expressions."""

    input: LogicalPlanNode
    expressions: List[Expression]
    aliases: List[str]  # Output column names
    distinct: bool = False

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Projection":
        assert len(children) == 1
        return Projection(children[0], self.expressions, self.aliases, self.distinct)

    def accept(self, visitor):
        return visitor.visit_projection(self)

    def schema(self) -> List[str]:
        # A ``*`` alias stands in for the input's columns; expand it so the
        # projection reports concrete names (joins above need them to orient
        # keys and to resolve qualified references).
        if "*" not in self.aliases:
            return self.aliases
        names: List[str] = []
        for alias in self.aliases:
            if alias == "*":
                names.extend(self.input.schema())
            else:
                names.append(alias)
        return names

    def __repr__(self) -> str:
        prefix = "Distinct " if self.distinct else ""
        return f"{prefix}Projection({len(self.expressions)} expressions)"


@dataclass(frozen=True)
class Filter(LogicalPlanNode):
    """Filter rows based on a predicate."""

    input: LogicalPlanNode
    predicate: Expression

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Filter":
        assert len(children) == 1
        return Filter(children[0], self.predicate)

    def accept(self, visitor):
        return visitor.visit_filter(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Filter({self.predicate})"


@dataclass(frozen=True)
class Join(LogicalPlanNode):
    """Join two inputs."""

    left: LogicalPlanNode
    right: LogicalPlanNode
    join_type: JoinType
    condition: Optional[Expression]  # None for cross/NATURAL/USING joins
    # NATURAL and USING joins carry no ON predicate: the source matches columns
    # by name. ``natural`` flags a NATURAL JOIN; ``using`` lists the shared
    # column names of a USING join.
    natural: bool = False
    using: Optional[List[str]] = None

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "Join":
        assert len(children) == 2
        return Join(
            children[0],
            children[1],
            self.join_type,
            self.condition,
            self.natural,
            self.using,
        )

    def accept(self, visitor):
        return visitor.visit_join(self)

    def schema(self) -> List[str]:
        # SEMI/ANTI joins are existential filters: they emit only the left
        # input's columns, never the right side's.
        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            return self.left.schema()
        return self.left.schema() + self.right.schema()

    def __repr__(self) -> str:
        return f"Join({self.join_type.value}, {self.condition})"


@dataclass(frozen=True)
class Aggregate(LogicalPlanNode):
    """Aggregate with grouping."""

    input: LogicalPlanNode
    group_by: List[Expression]  # Grouping expressions
    aggregates: List[Expression]  # Aggregate expressions
    output_names: List[str]  # Output column names

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Aggregate":
        assert len(children) == 1
        return Aggregate(children[0], self.group_by, self.aggregates, self.output_names)

    def accept(self, visitor):
        return visitor.visit_aggregate(self)

    def schema(self) -> List[str]:
        return self.output_names

    def __repr__(self) -> str:
        return f"Aggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"


@dataclass(frozen=True)
class Sort(LogicalPlanNode):
    """Sort rows."""

    input: LogicalPlanNode
    sort_keys: List[Expression]
    ascending: List[bool]  # One per sort key
    nulls_order: Optional[List[Optional[str]]] = None  # NULLS FIRST/LAST for each key

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Sort":
        assert len(children) == 1
        return Sort(children[0], self.sort_keys, self.ascending, self.nulls_order)

    def accept(self, visitor):
        return visitor.visit_sort(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Sort({len(self.sort_keys)} keys)"


@dataclass(frozen=True)
class Limit(LogicalPlanNode):
    """Limit number of rows.

    ``limit`` is None for an OFFSET without a row cap (``OFFSET n`` alone),
    which still skips rows and must not be discarded.
    """

    input: LogicalPlanNode
    limit: Optional[int]
    offset: int = 0

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Limit":
        assert len(children) == 1
        return Limit(children[0], self.limit, self.offset)

    def accept(self, visitor):
        return visitor.visit_limit(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Limit({self.limit}, offset={self.offset})"


@dataclass(frozen=True)
class Union(LogicalPlanNode):
    """Union of multiple inputs."""

    inputs: List[LogicalPlanNode]
    distinct: bool  # True for UNION, False for UNION ALL

    def children(self) -> List[LogicalPlanNode]:
        return self.inputs

    def with_children(self, children: List[LogicalPlanNode]) -> "Union":
        return Union(children, self.distinct)

    def accept(self, visitor):
        return visitor.visit_union(self)

    def schema(self) -> List[str]:
        # All inputs should have same schema
        return self.inputs[0].schema() if self.inputs else []

    def __repr__(self) -> str:
        union_type = "UNION" if self.distinct else "UNION ALL"
        return f"{union_type}({len(self.inputs)} inputs)"


@dataclass(frozen=True)
class SetOperation(LogicalPlanNode):
    """Binary SQL set operation (UNION / INTERSECT / EXCEPT).

    ``distinct`` is True for the bare form (which removes duplicates) and
    False for the ``ALL`` form (which preserves row multiplicity). Chained
    set operations nest left-associatively, mirroring the parser AST.
    """

    left: LogicalPlanNode
    right: LogicalPlanNode
    kind: SetOpKind
    distinct: bool

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "SetOperation":
        assert len(children) == 2
        return SetOperation(children[0], children[1], self.kind, self.distinct)

    def accept(self, visitor):
        return visitor.visit_set_operation(self)

    def schema(self) -> List[str]:
        # Both branches share a schema; the left branch names the result.
        return self.left.schema()

    def __repr__(self) -> str:
        suffix = "" if self.distinct else " ALL"
        return f"{self.kind.value}{suffix}(left, right)"


@dataclass(frozen=True)
class Explain(LogicalPlanNode):
    """Explain wrapper around another plan."""

    input: LogicalPlanNode
    format: ExplainFormat = ExplainFormat.TEXT

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Explain":
        assert len(children) == 1
        return Explain(children[0], self.format)

    def accept(self, visitor):
        return visitor.visit_explain(self)

    def schema(self) -> List[str]:
        return ["plan"]

    def __repr__(self) -> str:
        return f"Explain(format={self.format.value})"


@dataclass(frozen=True)
class CTE(LogicalPlanNode):
    """Common table expression wrapper.

    Holds a named subplan and a root plan that can reference it. ``recursive``
    marks a ``WITH RECURSIVE`` whose body references its own name;
    ``column_names`` carries an explicit output column list (``counter(n)``).
    """

    name: str
    cte_plan: LogicalPlanNode
    child: LogicalPlanNode
    recursive: bool = False
    column_names: Optional[List[str]] = None

    def children(self) -> List[LogicalPlanNode]:
        return [self.cte_plan, self.child]

    def with_children(self, children: List[LogicalPlanNode]) -> "CTE":
        assert len(children) == 2
        return CTE(
            name=self.name,
            cte_plan=children[0],
            child=children[1],
            recursive=self.recursive,
            column_names=self.column_names,
        )

    def accept(self, visitor):
        return visitor.visit_cte(self)

    def schema(self) -> List[str]:
        return self.child.schema()

    def __repr__(self) -> str:
        return f"CTE({self.name})"


@dataclass(frozen=True)
class CTERef(LogicalPlanNode):
    """A reference to a CTE by name, used in a FROM/JOIN position.

    A dedicated leaf node (not a ``Scan``) so optimizer passes can recognize a
    CTE reference distinctly from a catalog table. ``alias`` is how the
    reference is addressed in the query; ``columns`` are the referenced column
    names and ``output_names`` (filled by the binder) are the CTE's full output
    schema.
    """

    name: str
    alias: Optional[str] = None
    columns: Optional[List[str]] = None
    output_names: Optional[List[str]] = None

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "CTERef":
        assert len(children) == 0
        return self

    def accept(self, visitor):
        return visitor.visit_cte_ref(self)

    def schema(self) -> List[str]:
        return self.output_names or self.columns or []

    def __repr__(self) -> str:
        return f"CTERef({self.name})"


@dataclass(frozen=True)
class Values(LogicalPlanNode):
    """In-memory rows built from constant expressions.

    Represents a FROM-less SELECT such as ``SELECT 42``: one or more rows,
    each a list of expressions evaluated without input columns.
    """

    rows: List[List[Expression]]
    output_names: List[str]

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "Values":
        assert len(children) == 0
        return self

    def accept(self, visitor):
        return visitor.visit_values(self)

    def schema(self) -> List[str]:
        return self.output_names

    def __repr__(self) -> str:
        return f"Values({len(self.rows)} rows)"


@dataclass(frozen=True)
class SubqueryScan(LogicalPlanNode):
    """A derived table: a subplan exposed under an alias.

    ``FROM (SELECT ...) dt`` becomes SubqueryScan(input=subplan, alias="dt");
    the output columns are the subplan's output columns.
    """

    input: LogicalPlanNode
    alias: str

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "SubqueryScan":
        assert len(children) == 1
        return SubqueryScan(input=children[0], alias=self.alias)

    def accept(self, visitor):
        return visitor.visit_subquery_scan(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"SubqueryScan({self.alias})"


@dataclass(frozen=True)
class SingleRowGuard(LogicalPlanNode):
    """Runtime cardinality guard for decorrelated scalar subqueries.

    With no keys, the input may produce at most one row in total. With
    keys, each distinct key tuple may appear at most once. Violations
    raise at execution time, mirroring real engines' scalar subquery
    errors.
    """

    input: LogicalPlanNode
    keys: List[Expression]

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "SingleRowGuard":
        assert len(children) == 1
        return SingleRowGuard(input=children[0], keys=self.keys)

    def accept(self, visitor):
        return visitor.visit_single_row_guard(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"SingleRowGuard(keys={len(self.keys)})"


@dataclass(frozen=True)
class GroupedLimit(LogicalPlanNode):
    """Per-key LIMIT produced by decorrelating a correlated LIMIT.

    A correlated subquery's LIMIT applies once per outer row; after
    decorrelation it becomes "at most N rows per correlation key".
    """

    input: LogicalPlanNode
    keys: List[Expression]
    limit: int
    # Optional per-key ordering: when set, each key group is sorted by these
    # keys before its first ``limit`` rows are kept (the "first/latest row per
    # key" idiom). When None, rows are kept in input order.
    order_by_keys: Optional[List[Expression]] = None
    order_by_ascending: Optional[List[bool]] = None
    order_by_nulls: Optional[List[Optional[str]]] = None

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "GroupedLimit":
        assert len(children) == 1
        return GroupedLimit(
            input=children[0],
            keys=self.keys,
            limit=self.limit,
            order_by_keys=self.order_by_keys,
            order_by_ascending=self.order_by_ascending,
            order_by_nulls=self.order_by_nulls,
        )

    def accept(self, visitor):
        return visitor.visit_grouped_limit(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"GroupedLimit(limit={self.limit}, keys={len(self.keys)})"


@dataclass(frozen=True)
class LateralJoin(LogicalPlanNode):
    """A dependent (lateral) join: the right side may reference left columns.

    This is the decorrelation fallback for a correlated subquery that cannot be
    flattened into a set-based join — a non-equality correlation crossing an
    aggregate or LIMIT, or a skip-level correlation. The right side is a
    correlated subquery subtree (with the outer references kept) that is
    evaluated per left row; the executing engine — a pushed-to source, or the
    in-memory DuckDB merge engine — decorrelates and runs it. ``join_type`` is
    LEFT so an outer row with no subquery match still survives (value NULL).
    """

    left: LogicalPlanNode
    right: LogicalPlanNode
    join_type: JoinType

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "LateralJoin":
        assert len(children) == 2
        return LateralJoin(children[0], children[1], self.join_type)

    def accept(self, visitor):
        return visitor.visit_lateral_join(self)

    def schema(self) -> List[str]:
        return self.left.schema() + self.right.schema()

    def __repr__(self) -> str:
        return f"LateralJoin({self.join_type.name})"


class LogicalPlanVisitor(ABC):
    """Visitor interface for logical plan nodes."""

    @abstractmethod
    def visit_scan(self, node: Scan):
        pass

    @abstractmethod
    def visit_projection(self, node: Projection):
        pass

    @abstractmethod
    def visit_filter(self, node: Filter):
        pass

    @abstractmethod
    def visit_join(self, node: Join):
        pass

    @abstractmethod
    def visit_aggregate(self, node: Aggregate):
        pass

    @abstractmethod
    def visit_sort(self, node: Sort):
        pass

    @abstractmethod
    def visit_limit(self, node: Limit):
        pass

    @abstractmethod
    def visit_union(self, node: Union):
        pass

    @abstractmethod
    def visit_set_operation(self, node: "SetOperation"):
        pass

    @abstractmethod
    def visit_explain(self, node: Explain):
        pass

    @abstractmethod
    def visit_cte(self, node: "CTE"):
        pass

    @abstractmethod
    def visit_values(self, node: "Values"):
        pass

    @abstractmethod
    def visit_subquery_scan(self, node: "SubqueryScan"):
        pass

    @abstractmethod
    def visit_single_row_guard(self, node: "SingleRowGuard"):
        pass

    @abstractmethod
    def visit_grouped_limit(self, node: "GroupedLimit"):
        pass

    def visit_lateral_join(self, node: "LateralJoin"):
        pass

    def visit_cte_ref(self, node: "CTERef"):
        pass
