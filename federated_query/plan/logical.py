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
    output_names: Optional[List[str]] = None  # Output column names when using aggregates
    limit: Optional[int] = None
    offset: int = 0
    order_by_keys: Optional[List[Expression]] = None  # ORDER BY expressions
    order_by_ascending: Optional[List[bool]] = None  # ASC/DESC for each key
    order_by_nulls: Optional[List[Optional[str]]] = None  # NULLS FIRST/LAST for each key
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
class Project(LogicalPlanNode):
    """Project (select) specific expressions."""

    input: LogicalPlanNode
    expressions: List[Expression]
    aliases: List[str]  # Output column names
    distinct: bool = False

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Project":
        assert len(children) == 1
        return Project(children[0], self.expressions, self.aliases, self.distinct)

    def accept(self, visitor):
        return visitor.visit_project(self)

    def schema(self) -> List[str]:
        return self.aliases

    def __repr__(self) -> str:
        prefix = "Distinct " if self.distinct else ""
        return f"{prefix}Project({len(self.expressions)} expressions)"


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
    condition: Optional[Expression]  # None for cross join

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "Join":
        assert len(children) == 2
        return Join(children[0], children[1], self.join_type, self.condition)

    def accept(self, visitor):
        return visitor.visit_join(self)

    def schema(self) -> List[str]:
        # Combine schemas from both sides
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
    """Limit number of rows."""

    input: LogicalPlanNode
    limit: int
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


class LogicalPlanVisitor(ABC):
    """Visitor interface for logical plan nodes."""

    @abstractmethod
    def visit_scan(self, node: Scan):
        pass

    @abstractmethod
    def visit_project(self, node: Project):
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
    def visit_explain(self, node: Explain):
        pass
