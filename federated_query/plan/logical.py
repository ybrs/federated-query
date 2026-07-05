"""Logical plan nodes (Pydantic models)."""

from abc import ABC, abstractmethod
from typing import List, Optional
from enum import Enum

from ..model import StateModel
from .expressions import Expression
from .field_introspection import model_expression_values


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


class LogicalPlanNode(StateModel):
    """Base class for logical plan nodes (a mutable Pydantic model, see
    :class:`~federated_query.model.StateModel`)."""

    def children(self) -> List["LogicalPlanNode"]:
        """Return child nodes."""
        raise NotImplementedError

    def direct_expressions(self) -> List[Expression]:
        """Every expression attached directly to this node (not its children).

        Derived from the field type annotations (see
        :mod:`federated_query.plan.field_introspection`), so every node type is
        covered without a per-node list and a field whose type cannot be
        classified raises rather than being silently skipped. Walkers that must
        see all of a node's expressions (correlation analysis, validation) use
        this single source of truth.
        """
        return model_expression_values(self)

    def with_children(self, children: List["LogicalPlanNode"]) -> "LogicalPlanNode":
        """Return a copy of this node with different children."""
        raise NotImplementedError

    def _require_child_count(
        self, children: List["LogicalPlanNode"], expected: int
    ) -> None:
        """Raise when with_children is given the wrong arity.

        Uses an explicit raise rather than assert so the check is not stripped
        under ``python -O`` (which would let an invalid rebuild pass silently).
        """
        if len(children) != expected:
            raise ValueError(
                f"{type(self).__name__}.with_children expects {expected} "
                f"child(ren), got {len(children)}"
            )

    def accept(self, visitor):
        """Accept a visitor for the visitor pattern."""
        raise NotImplementedError

    def schema(self) -> List[str]:
        """Return output column names."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__


def transform_children(node, transform) -> "LogicalPlanNode":
    """Rebuild a plan node from its transformed children, or return it unchanged.

    The mechanical recurse-and-rebuild that every pushdown rule's pass-through
    arms share: each child is transformed, and the node is rebuilt via
    with_children only when a child actually changed, so an unchanged subtree
    keeps its identity (the rules' change-detection relies on that). The per-rule
    decision of WHICH node types to recurse into stays in the rule; this only
    removes the hand-written rebuild boilerplate.
    """
    new_children = []
    changed = False
    for child in node.children():
        new_child = transform(child)
        new_children.append(new_child)
        if new_child != child:
            changed = True
    if changed:
        return node.with_children(new_children)
    return node


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
    # TABLESAMPLE clause as Postgres-form SQL (e.g. "TABLESAMPLE BERNOULLI (10)");
    # transpiled to the source dialect when the scan is rendered.
    sample: Optional[str] = None
    group_by: Optional[List[Expression]] = None  # Optional GROUP BY expressions
    # GROUP BY ROLLUP/CUBE/GROUPING SETS folded onto the scan, as explicit sets.
    grouping_sets: Optional[List[List[Expression]]] = None
    aggregates: Optional[List[Expression]] = None  # Optional aggregate expressions
    output_names: Optional[List[str]] = None  # Output names when using aggregates
    limit: Optional[int] = None
    offset: int = 0
    order_by_keys: Optional[List[Expression]] = None  # ORDER BY expressions
    order_by_ascending: Optional[List[bool]] = None  # ASC/DESC for each key
    order_by_nulls: Optional[List[Optional[str]]] = None  # NULLS FIRST/LAST per key
    distinct: bool = False

    @classmethod
    def create(
        cls,
        *,
        datasource: str,
        schema_name: str,
        table_name: str,
        columns: List[str],
        filters: Optional[Expression] = None,
        alias: Optional[str] = None,
        sample: Optional[str] = None,
        group_by: Optional[List[Expression]] = None,
        grouping_sets: Optional[List[List[Expression]]] = None,
        aggregates: Optional[List[Expression]] = None,
        output_names: Optional[List[str]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
        distinct: bool = False,
    ) -> "Scan":
        """Build a Scan leaf reading columns from a source table.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            filters=filters,
            alias=alias,
            sample=sample,
            group_by=group_by,
            grouping_sets=grouping_sets,
            aggregates=aggregates,
            output_names=output_names,
            limit=limit,
            offset=offset,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
            distinct=distinct,
        )

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "Scan":
        self._require_child_count(children, 0)
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


class Projection(LogicalPlanNode):
    """Projection (select) specific expressions."""

    input: LogicalPlanNode
    expressions: List[Expression]
    aliases: List[str]  # Output column names
    distinct: bool = False
    # DISTINCT ON (keys): keep one row per key combination (chosen by ORDER BY).
    # None for a plain projection; an empty/non-empty list implies DISTINCT ON.
    distinct_on: Optional[List[Expression]] = None

    @classmethod
    def create(
        cls,
        *,
        input: LogicalPlanNode,
        expressions: List[Expression],
        aliases: List[str],
        distinct: bool = False,
        distinct_on: Optional[List[Expression]] = None,
    ) -> "Projection":
        """Build a Projection selecting the given expressions over an input.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            input=input,
            expressions=expressions,
            aliases=aliases,
            distinct=distinct,
            distinct_on=distinct_on,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Projection":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

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


class Filter(LogicalPlanNode):
    """Filter rows based on a predicate."""

    input: LogicalPlanNode
    predicate: Expression

    @classmethod
    def create(
        cls, *, input: LogicalPlanNode, predicate: Expression
    ) -> "Filter":
        """Build a Filter applying a predicate to its input rows.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(input=input, predicate=predicate)

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Filter":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_filter(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Filter({self.predicate})"


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
    # Set by the cost-based join-ordering rule: the estimated output rows of
    # this join and the provenance of every DEFAULTED statistic behind that
    # estimate (empty/None = fully statistics-backed). EXPLAIN prints both, so
    # a plan costed from guesses is visibly different from a costed one.
    estimated_rows: Optional[int] = None
    estimate_defaults: Optional[List[str]] = None

    @classmethod
    def create(
        cls,
        *,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        join_type: JoinType,
        condition: Optional[Expression],
        natural: bool = False,
        using: Optional[List[str]] = None,
        estimated_rows: Optional[int] = None,
        estimate_defaults: Optional[List[str]] = None,
    ) -> "Join":
        """Build a Join of two inputs with an optional ON condition.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            left=left,
            right=right,
            join_type=join_type,
            condition=condition,
            natural=natural,
            using=using,
            estimated_rows=estimated_rows,
            estimate_defaults=estimate_defaults,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "Join":
        self._require_child_count(children, 2)
        return self.model_copy(update={"left": children[0], "right": children[1]})

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


class Aggregate(LogicalPlanNode):
    """Aggregate with grouping."""

    input: LogicalPlanNode
    group_by: List[Expression]  # Grouping expressions (union of all grouping sets)
    aggregates: List[Expression]  # Aggregate expressions
    output_names: List[str]  # Output column names
    # GROUP BY ROLLUP/CUBE/GROUPING SETS, expanded to explicit grouping sets.
    # Each inner list is one set's expressions ([] is the grand total). None for
    # an ordinary single-level GROUP BY.
    grouping_sets: Optional[List[List[Expression]]] = None

    @classmethod
    def create(
        cls,
        *,
        input: LogicalPlanNode,
        group_by: List[Expression],
        aggregates: List[Expression],
        output_names: List[str],
        grouping_sets: Optional[List[List[Expression]]] = None,
    ) -> "Aggregate":
        """Build an Aggregate grouping its input and computing aggregates.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            input=input,
            group_by=group_by,
            aggregates=aggregates,
            output_names=output_names,
            grouping_sets=grouping_sets,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Aggregate":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_aggregate(self)

    def schema(self) -> List[str]:
        return self.output_names

    def __repr__(self) -> str:
        return f"Aggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"


class Sort(LogicalPlanNode):
    """Sort rows."""

    input: LogicalPlanNode
    sort_keys: List[Expression]
    ascending: List[bool]  # One per sort key
    nulls_order: Optional[List[Optional[str]]] = None  # NULLS FIRST/LAST per key

    @classmethod
    def create(
        cls,
        *,
        input: LogicalPlanNode,
        sort_keys: List[Expression],
        ascending: List[bool],
        nulls_order: Optional[List[Optional[str]]] = None,
    ) -> "Sort":
        """Build a Sort ordering its input by the given keys.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            input=input,
            sort_keys=sort_keys,
            ascending=ascending,
            nulls_order=nulls_order,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Sort":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_sort(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Sort({len(self.sort_keys)} keys)"


class Limit(LogicalPlanNode):
    """Limit number of rows.

    ``limit`` is None for an OFFSET without a row cap (``OFFSET n`` alone),
    which still skips rows and must not be discarded.
    """

    input: LogicalPlanNode
    limit: Optional[int]
    offset: int = 0

    @classmethod
    def create(
        cls, *, input: LogicalPlanNode, limit: Optional[int], offset: int = 0
    ) -> "Limit":
        """Build a Limit capping and/or offsetting its input rows.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(input=input, limit=limit, offset=offset)

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Limit":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_limit(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"Limit({self.limit}, offset={self.offset})"


class Union(LogicalPlanNode):
    """Union of multiple inputs."""

    inputs: List[LogicalPlanNode]
    distinct: bool  # True for UNION, False for UNION ALL

    @classmethod
    def create(
        cls, *, inputs: List[LogicalPlanNode], distinct: bool
    ) -> "Union":
        """Build a Union combining several inputs sharing a schema.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(inputs=inputs, distinct=distinct)

    def children(self) -> List[LogicalPlanNode]:
        return self.inputs

    def with_children(self, children: List[LogicalPlanNode]) -> "Union":
        return self.model_copy(update={"inputs": children})

    def accept(self, visitor):
        return visitor.visit_union(self)

    def schema(self) -> List[str]:
        # All inputs should have same schema
        return self.inputs[0].schema() if self.inputs else []

    def __repr__(self) -> str:
        union_type = "UNION" if self.distinct else "UNION ALL"
        return f"{union_type}({len(self.inputs)} inputs)"


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

    @classmethod
    def create(
        cls,
        *,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        kind: SetOpKind,
        distinct: bool,
    ) -> "SetOperation":
        """Build a binary SetOperation (UNION/INTERSECT/EXCEPT) over two inputs.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(left=left, right=right, kind=kind, distinct=distinct)

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "SetOperation":
        self._require_child_count(children, 2)
        return self.model_copy(update={"left": children[0], "right": children[1]})

    def accept(self, visitor):
        return visitor.visit_set_operation(self)

    def schema(self) -> List[str]:
        # Both branches share a schema; the left branch names the result.
        return self.left.schema()

    def __repr__(self) -> str:
        suffix = "" if self.distinct else " ALL"
        return f"{self.kind.value}{suffix}(left, right)"


class Explain(LogicalPlanNode):
    """Explain wrapper around another plan."""

    input: LogicalPlanNode
    format: ExplainFormat = ExplainFormat.TEXT

    @classmethod
    def create(
        cls,
        *,
        input: LogicalPlanNode,
        format: ExplainFormat = ExplainFormat.TEXT,
    ) -> "Explain":
        """Build an Explain wrapper rendering a plan in the given format.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(input=input, format=format)

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "Explain":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_explain(self)

    def schema(self) -> List[str]:
        return ["plan"]

    def __repr__(self) -> str:
        return f"Explain(format={self.format.value})"


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

    @classmethod
    def create(
        cls,
        *,
        name: str,
        cte_plan: LogicalPlanNode,
        child: LogicalPlanNode,
        recursive: bool = False,
        column_names: Optional[List[str]] = None,
    ) -> "CTE":
        """Build a CTE binding a named subplan for use by a child plan.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            name=name,
            cte_plan=cte_plan,
            child=child,
            recursive=recursive,
            column_names=column_names,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.cte_plan, self.child]

    def with_children(self, children: List[LogicalPlanNode]) -> "CTE":
        self._require_child_count(children, 2)
        return self.model_copy(update={"cte_plan": children[0], "child": children[1]})

    def accept(self, visitor):
        return visitor.visit_cte(self)

    def schema(self) -> List[str]:
        return self.child.schema()

    def __repr__(self) -> str:
        return f"CTE({self.name})"


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

    @classmethod
    def create(
        cls,
        *,
        name: str,
        alias: Optional[str] = None,
        columns: Optional[List[str]] = None,
        output_names: Optional[List[str]] = None,
    ) -> "CTERef":
        """Build a CTERef leaf referencing a named CTE in a FROM/JOIN position.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            name=name,
            alias=alias,
            columns=columns,
            output_names=output_names,
        )

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "CTERef":
        self._require_child_count(children, 0)
        return self

    def accept(self, visitor):
        return visitor.visit_cte_ref(self)

    def schema(self) -> List[str]:
        return self.output_names or self.columns or []

    def __repr__(self) -> str:
        return f"CTERef({self.name})"


class Values(LogicalPlanNode):
    """In-memory rows built from constant expressions.

    Represents a FROM-less SELECT such as ``SELECT 42``: one or more rows,
    each a list of expressions evaluated without input columns.
    """

    rows: List[List[Expression]]
    output_names: List[str]

    @classmethod
    def create(
        cls, *, rows: List[List[Expression]], output_names: List[str]
    ) -> "Values":
        """Build a Values leaf of constant rows evaluated without input columns.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(rows=rows, output_names=output_names)

    def children(self) -> List[LogicalPlanNode]:
        return []

    def with_children(self, children: List[LogicalPlanNode]) -> "Values":
        self._require_child_count(children, 0)
        return self

    def accept(self, visitor):
        return visitor.visit_values(self)

    def schema(self) -> List[str]:
        return self.output_names

    def __repr__(self) -> str:
        return f"Values({len(self.rows)} rows)"


class SubqueryScan(LogicalPlanNode):
    """A derived table: a subplan exposed under an alias.

    ``FROM (SELECT ...) dt`` becomes SubqueryScan(input=subplan, alias="dt");
    the output columns are the subplan's output columns. A column-alias list
    (``... AS dt(a, b)``) is carried in column_names and renames those outputs
    positionally; None means keep the subplan's own output names.
    """

    input: LogicalPlanNode
    alias: str
    column_names: Optional[List[str]] = None

    @classmethod
    def create(
        cls, *, input: LogicalPlanNode, alias: str,
        column_names: Optional[List[str]] = None,
    ) -> "SubqueryScan":
        """Build a SubqueryScan exposing a subplan under a derived-table alias.
        column_names carries a ``AS alias(col, ...)`` rename list, or None.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(input=input, alias=alias, column_names=column_names)

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "SubqueryScan":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_subquery_scan(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"SubqueryScan({self.alias})"


class SingleRowGuard(LogicalPlanNode):
    """Runtime cardinality guard for decorrelated scalar subqueries.

    With no keys, the input may produce at most one row in total. With
    keys, each distinct key tuple may appear at most once. Violations
    raise at execution time, mirroring real engines' scalar subquery
    errors.
    """

    input: LogicalPlanNode
    keys: List[Expression]

    @classmethod
    def create(
        cls, *, input: LogicalPlanNode, keys: List[Expression]
    ) -> "SingleRowGuard":
        """Build a SingleRowGuard enforcing at-most-one-row-per-key on its input.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(input=input, keys=keys)

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "SingleRowGuard":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_single_row_guard(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"SingleRowGuard(keys={len(self.keys)})"


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

    @classmethod
    def create(
        cls,
        *,
        input: LogicalPlanNode,
        keys: List[Expression],
        limit: int,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
    ) -> "GroupedLimit":
        """Build a GroupedLimit keeping at most N rows per correlation key.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(
            input=input,
            keys=keys,
            limit=limit,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
        )

    def children(self) -> List[LogicalPlanNode]:
        return [self.input]

    def with_children(self, children: List[LogicalPlanNode]) -> "GroupedLimit":
        self._require_child_count(children, 1)
        return self.model_copy(update={"input": children[0]})

    def accept(self, visitor):
        return visitor.visit_grouped_limit(self)

    def schema(self) -> List[str]:
        return self.input.schema()

    def __repr__(self) -> str:
        return f"GroupedLimit(limit={self.limit}, keys={len(self.keys)})"


class LateralJoin(LogicalPlanNode):
    """A dependent (lateral) join: the right side may reference left columns.

    This is the decorrelation fallback for a correlated subquery that cannot be
    flattened into a set-based join - a non-equality correlation crossing an
    aggregate or LIMIT, or a skip-level correlation. The right side is a
    correlated subquery subtree (with the outer references kept) that is
    evaluated per left row; the executing engine - a pushed-to source, or the
    in-memory DuckDB merge engine - decorrelates and runs it. ``join_type`` is
    LEFT so an outer row with no subquery match still survives (value NULL).
    """

    left: LogicalPlanNode
    right: LogicalPlanNode
    join_type: JoinType

    @classmethod
    def create(
        cls,
        *,
        left: LogicalPlanNode,
        right: LogicalPlanNode,
        join_type: JoinType,
    ) -> "LateralJoin":
        """Build a LateralJoin whose right side may reference left columns.
        Sanctioned construction path; prefer model_copy when deriving from an existing node."""
        return cls(left=left, right=right, join_type=join_type)

    def children(self) -> List[LogicalPlanNode]:
        return [self.left, self.right]

    def with_children(self, children: List[LogicalPlanNode]) -> "LateralJoin":
        self._require_child_count(children, 2)
        return self.model_copy(update={"left": children[0], "right": children[1]})

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
