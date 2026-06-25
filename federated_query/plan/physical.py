"""Physical plan nodes."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    List,
    Optional,
    Iterator,
    Any,
    TYPE_CHECKING,
    Dict,
    Callable,
    Set,
    Tuple,
)
from enum import Enum

import pyarrow as pa
from sqlglot import exp

from .expressions import Expression
from .logical import JoinType, ExplainFormat, SetOpKind
from ..utils.logging import get_logger

if TYPE_CHECKING:
    from .expressions import FunctionCall


_LOGGER = get_logger(__name__)

# Cap on distinct build-side keys for which a hash join pushes a dynamic IN
# filter into the probe side. Above this the IN list is not worth the round
# trip, so the probe is fetched in full (logged, never silently dropped). A
# cost-based choice would replace this constant later.
_DYNAMIC_FILTER_MAX_KEYS = 2000

# Names under which a hash join registers its two inputs with the merge engine.
# Each join runs on its own DuckDB cursor, so these fixed names never collide
# across nested joins.
_MERGE_LEFT_RELATION = "in_left"
_MERGE_RIGHT_RELATION = "in_right"
# Name under which an aggregate registers its single input with the merge engine.
_MERGE_AGG_RELATION = "in_agg"
# Name under which a sort registers its single input with the merge engine.
_MERGE_SORT_RELATION = "in_sort"


class PhysicalPlanNode(ABC):
    """Base class for physical plan nodes."""

    @abstractmethod
    def children(self) -> List["PhysicalPlanNode"]:
        """Return child nodes."""
        pass

    @abstractmethod
    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute this operator and yield record batches."""
        pass

    @abstractmethod
    def schema(self) -> pa.Schema:
        """Return output schema."""
        pass

    @abstractmethod
    def estimated_cost(self) -> float:
        """Estimated cost of executing this plan."""
        pass

    def __repr__(self) -> str:
        return self.__class__.__name__

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Optional mapping from (table, column) to physical column name."""
        return {}

    def set_merge_engine(self, engine: Any) -> None:
        """Attach the per-query DuckDB merge engine used for local execution."""
        self._merge_engine = engine

    def merge_engine(self) -> Any:
        """Return the attached merge engine, or None when none was attached."""
        return getattr(self, "_merge_engine", None)

    def apply_dynamic_filter(
        self, key_columns: List[Expression], value_tuples: List[tuple]
    ) -> bool:
        """Constrain this input to rows whose join keys appear in ``value_tuples``.

        Used by a hash join to push the build side's distinct key values into
        the probe side as a runtime ``IN`` filter (semi-join reduction). Most
        nodes cannot accept such a filter; they return False and the join probes
        in full. ``PhysicalScan`` overrides this.
        """
        return False


def _row_key_tuple(key_values, row_index: int) -> tuple:
    """Build a hashable key tuple from per-column arrays at one row index."""
    values = []
    for array in key_values:
        values.append(array[row_index].as_py())
    return tuple(values)


def _key_column_names(num_keys: int) -> List[str]:
    """Stable column names k0..k(n-1) for a join's key columns."""
    names = []
    for index in range(num_keys):
        names.append(f"k{index}")
    return names


def _within_dynamic_filter_cap(num_keys: int) -> bool:
    """Whether a build-key count is non-empty and within the IN-list cap.

    Logs at INFO when an over-cap set is skipped so the decision is observable.
    """
    if num_keys == 0:
        return False
    if num_keys > _DYNAMIC_FILTER_MAX_KEYS:
        _LOGGER.info(
            "dynamic filter skipped: %d build keys exceed cap %d",
            num_keys,
            _DYNAMIC_FILTER_MAX_KEYS,
        )
        return False
    return True


def _key_tuples_from_table(table: "pa.Table") -> List[tuple]:
    """Convert a distinct-key table (already within the cap) to value tuples."""
    columns = []
    for name in table.column_names:
        columns.append(table.column(name).to_pylist())
    return _zip_columns_to_tuples(columns, table.num_rows)


def _zip_columns_to_tuples(columns: List[list], num_rows: int) -> List[tuple]:
    """Row-wise zip of per-column value lists into hashable key tuples."""
    tuples = []
    for row_index in range(num_rows):
        values = []
        for column in columns:
            values.append(column[row_index])
        tuples.append(tuple(values))
    return tuples


def _and_filters(existing: Optional[Expression], new_filter: Expression) -> Expression:
    """Combine an existing scan filter with an additional predicate via AND."""
    from .expressions import BinaryOp, BinaryOpType

    if existing is None:
        return new_filter
    return BinaryOp(op=BinaryOpType.AND, left=existing, right=new_filter)


def _literal_for_value(value):
    """Wrap a Python join-key value as a typed Literal, or None if unsupported.

    Keyed on the exact type so ``bool`` (a subclass of ``int``) is not misread
    as an integer. Unsupported types (date, decimal, bytes) decline.
    """
    from .expressions import Literal, DataType

    if value is None:
        return None
    type_map = {
        bool: DataType.BOOLEAN,
        int: DataType.INTEGER,
        float: DataType.DOUBLE,
        str: DataType.VARCHAR,
    }
    data_type = type_map.get(type(value))
    if data_type is None:
        return None
    return Literal(value=value, data_type=data_type)


def _table_from_batches(batches: List[pa.RecordBatch], schema: pa.Schema) -> pa.Table:
    """Build an Arrow table from batches, falling back to an empty typed table."""
    if not batches:
        return schema.empty_table()
    return pa.Table.from_batches(batches)


@dataclass
class PhysicalCTE(PhysicalPlanNode):
    """Materializes a cross-source CTE body once, shared by every reference.

    A CTE is a named relation: its body is executed a single time into an Arrow
    table that every reference reads, so the work is never repeated even when
    the CTE is referenced many times. Single-source CTEs never reach here — they
    are pushed whole as a remote ``WITH``; this is the cross-source path.
    """

    name: str
    body: PhysicalPlanNode
    column_names: Optional[List[str]] = None

    def children(self) -> List[PhysicalPlanNode]:
        return [self.body]

    def materialize(self) -> pa.Table:
        """Execute the body once and cache its rows as an Arrow table."""
        cached = getattr(self, "_cached", None)
        if cached is not None:
            return cached
        table = _table_from_batches(list(self.body.execute()), self.body.schema())
        self._cached = self._relabel(table)
        return self._cached

    def _relabel(self, table: pa.Table) -> pa.Table:
        """Apply an explicit CTE column list to the output column names."""
        if not self.column_names or list(table.column_names) == self.column_names:
            return table
        return table.rename_columns(self.column_names)

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield the materialized rows (computed once)."""
        yield from self.materialize().to_batches()

    def schema(self) -> pa.Schema:
        """Output schema, taken from the body and relabeled if a list is set."""
        base = self.body.schema()
        if not self.column_names or list(base.names) == self.column_names:
            return base
        fields = []
        for index in range(len(self.column_names)):
            fields.append(pa.field(self.column_names[index], base.field(index).type))
        return pa.schema(fields)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTE({self.name})"


@dataclass
class PhysicalCTEScan(PhysicalPlanNode):
    """Reads a materialized CTE's rows; one per reference, shares the producer."""

    producer: PhysicalCTE
    alias: Optional[str] = None

    def children(self) -> List[PhysicalPlanNode]:
        return [self.producer]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield the producer's materialized rows."""
        yield from self.producer.materialize().to_batches()

    def schema(self) -> pa.Schema:
        return self.producer.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTEScan({self.producer.name})"


@dataclass
class PhysicalCTEMergeQuery(PhysicalPlanNode):
    """Runs a whole (possibly recursive) WITH inside the merge engine.

    Every base source relation in the CTE is materialized into Arrow and
    registered under a generated name; the rendered WITH references those names
    so the in-memory DuckDB runs the recursion (and any multi-reference)
    locally. Used when a recursive CTE spans data sources — the fixpoint needs a
    single engine.
    """

    sql: str
    inputs: Dict[str, PhysicalPlanNode]
    output_names: List[str]

    def children(self) -> List[PhysicalPlanNode]:
        return list(self.inputs.values())

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Materialize the registered sources, then run the WITH in DuckDB."""
        engine = self.merge_engine()
        yield from engine.run(self.sql, self._materialized_inputs())

    def _materialized_inputs(self) -> Dict[str, pa.Table]:
        """Execute each base-source subtree into a registered Arrow table."""
        tables = {}
        for name, node in self.inputs.items():
            tables[name] = _table_from_batches(list(node.execute()), node.schema())
        return tables

    def schema(self) -> pa.Schema:
        """Probe the WITH with no rows to learn the output schema."""
        engine = self.merge_engine()
        probe = f"SELECT * FROM ({self.sql}) AS _cte_schema LIMIT 0"
        for batch in engine.run(probe, self._materialized_inputs()):
            return batch.schema
        return pa.schema([])

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTEMergeQuery({len(self.inputs)} sources)"


_MERGE_JOIN_KEYWORDS = {
    JoinType.INNER: "INNER JOIN",
    JoinType.LEFT: "LEFT JOIN",
    JoinType.RIGHT: "RIGHT JOIN",
    JoinType.FULL: "FULL JOIN",
    JoinType.SEMI: "SEMI JOIN",
    JoinType.ANTI: "ANTI JOIN",
}


def _qualify_join_condition(condition, left_names, right_names):
    """Rebuild a join condition qualifying each column ref by its side.

    A column whose name belongs to the left input is qualified ``l.``, one on
    the right ``r.``; this lets an arbitrary (non-equi, NULL-aware) predicate be
    rendered against the two registered merge-engine relations without ambiguity.
    """
    from .expressions import ColumnRef, BinaryOp, UnaryOp

    if isinstance(condition, ColumnRef):
        if condition.column in left_names:
            return ColumnRef(table="l", column=condition.column)
        if condition.column in right_names:
            return ColumnRef(table="r", column=condition.column)
        return condition
    if isinstance(condition, BinaryOp):
        return BinaryOp(
            op=condition.op,
            left=_qualify_join_condition(condition.left, left_names, right_names),
            right=_qualify_join_condition(condition.right, left_names, right_names),
        )
    if isinstance(condition, UnaryOp):
        return UnaryOp(
            op=condition.op,
            operand=_qualify_join_condition(condition.operand, left_names, right_names),
        )
    return condition


def _merge_join_select_list(join_type, left_schema, right_schema) -> str:
    """SELECT list reproducing the join output: left only for SEMI/ANTI, else both."""
    parts = []
    for name in left_schema.names:
        parts.append(f'l."{name}" AS "{name}"')
    if join_type in (JoinType.SEMI, JoinType.ANTI):
        return ", ".join(parts)
    left_name_set = set(left_schema.names)
    for field in right_schema:
        output_name = field.name
        if output_name in left_name_set:
            output_name = f"right_{output_name}"
        parts.append(f'r."{field.name}" AS "{output_name}"')
    return ", ".join(parts)


def _cast_batch_to_schema(batch: pa.RecordBatch, target: pa.Schema) -> pa.RecordBatch:
    """Cast each column of a batch to the target schema's types.

    DuckDB may return a different-but-compatible type for an aggregate (e.g.
    a wide SUM); aligning to the operator's declared schema keeps the output
    contract stable for parent operators.
    """
    import pyarrow.compute as pc

    arrays = []
    for index, field in enumerate(target):
        column = batch.column(index)
        if column.type == field.type:
            arrays.append(column)
        else:
            arrays.append(pc.cast(column, field.type))
    return pa.RecordBatch.from_arrays(arrays, names=list(target.names))


def _prepend_batch(first: pa.RecordBatch, rest) -> Iterator[pa.RecordBatch]:
    """Yield an already-pulled first batch, then the remaining batches."""
    yield first
    yield from rest


def _streaming_reader(node: "PhysicalPlanNode") -> pa.RecordBatchReader:
    """Expose a node's output as a lazy reader without materializing it.

    DuckDB pulls batches from this reader on demand, so the (often large) probe
    side is streamed, never drained into a table by us. A single batch is peeked
    to learn the real output schema (a node's declared ``schema()`` can drift
    from what it actually yields); the rest stay lazy.
    """
    batches = node.execute()
    first = next(batches, None)
    if first is None:
        return pa.RecordBatchReader.from_batches(node.schema(), iter(()))
    return pa.RecordBatchReader.from_batches(
        first.schema, _prepend_batch(first, batches)
    )


def _join_output_aliases(
    left: "PhysicalPlanNode", right: "PhysicalPlanNode"
) -> Dict[Tuple[Optional[str], str], str]:
    """Build a (table, column) -> output-name map for a binary join.

    Left columns keep their names; right columns whose names collide with a
    left column are renamed with a ``right_`` prefix, mirroring the renaming
    in ``_join_rows`` so qualified references resolve to the correct side.
    """
    alias_map: Dict[Tuple[Optional[str], str], str] = dict(left.column_aliases())
    left_names = set(left.schema().names)
    for key, physical_name in right.column_aliases().items():
        if physical_name in left_names:
            alias_map[key] = f"right_{physical_name}"
        else:
            alias_map[key] = physical_name
    return alias_map


@dataclass
class PhysicalScan(PhysicalPlanNode):
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
    columns: List[str]
    filters: Optional[Expression] = None
    datasource_connection: Any = None  # Set during planning
    _schema: Optional[pa.Schema] = None  # Cached schema
    group_by: Optional[List[Expression]] = None  # Optional GROUP BY expressions
    aggregates: Optional[List[Expression]] = None  # Optional aggregate expressions
    output_names: Optional[List[str]] = (
        None  # Output column names when using aggregates
    )
    alias: Optional[str] = None  # Table alias (if any)
    limit: Optional[int] = None
    offset: int = 0
    order_by_keys: Optional[List[Expression]] = None  # ORDER BY expressions
    order_by_ascending: Optional[List[bool]] = None  # ASC/DESC for each key
    order_by_nulls: Optional[List[Optional[str]]] = (
        None  # NULLS FIRST/LAST for each key
    )
    distinct: bool = False
    # Probe-side join keys that a hash join will constrain with the build side's
    # values at runtime (semi-join reduction). Set at plan time so EXPLAIN can
    # show the dynamic filter; the actual IN list is injected during execution.
    dynamic_filter_keys: Optional[List[Expression]] = None
    # Distinct build-side key values fetched during EXPLAIN so the dynamic
    # filter renders with real values; None outside EXPLAIN.
    dynamic_filter_values: Optional[List[tuple]] = None

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def apply_dynamic_filter(
        self, key_columns: List[Expression], value_tuples: List[tuple]
    ) -> bool:
        """Add a runtime ``key IN (...)`` filter from a join's build-side keys.

        Only single-column keys with renderable literal values are handled; a
        composite key or an untypable value declines (the join probes in full).
        The filter mutates this scan in place — its SQL is rebuilt at execute
        time — and does not change the output schema.
        """
        if len(key_columns) != 1:
            return False
        in_filter = self._build_in_filter(key_columns[0], value_tuples)
        if in_filter is None:
            return False
        self.filters = _and_filters(self.filters, in_filter)
        return True

    def _build_in_filter(self, key_column: Expression, value_tuples: List[tuple]):
        """Build ``key_column IN (<distinct values>)`` or None if any is untypable."""
        from .expressions import InList

        options = []
        for value_tuple in value_tuples:
            literal = _literal_for_value(value_tuple[0])
            if literal is None:
                return None
            options.append(literal)
        if not options:
            return None
        return InList(value=key_column, options=options)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose this scan's columns under its table alias.

        Lets a join above resolve qualified references (``o.id``) even when
        another relation in the join exposes a column of the same name.
        """
        if self.alias is None:
            return {}
        mapping: Dict[Tuple[Optional[str], str], str] = {}
        for name in self.schema().names:
            mapping[(self.alias, name)] = name
        return mapping

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute scan on remote data source."""
        if self.datasource_connection is None:
            raise RuntimeError("Data source connection not set")

        query = self._build_query()

        self.datasource_connection.ensure_connected()
        batches = self.datasource_connection.execute_query(query)

        for batch in batches:
            yield batch

    def _build_query(self) -> str:
        """Build SQL query for this scan."""
        select_kw = "SELECT DISTINCT" if self.distinct else "SELECT"
        query = f"{select_kw} {self._format_columns()} FROM {self._format_table_ref()}"
        where_pred, having_pred = self._split_where_having()
        if where_pred is not None:
            query = f"{query} WHERE {where_pred.to_sql()}"
        if self.group_by:
            query = f"{query} GROUP BY {self._format_group_by()}"
        if having_pred is not None:
            query = f"{query} HAVING {having_pred.to_sql()}"
        return self._append_order_limit(query)

    def _append_order_limit(self, query: str) -> str:
        """Append ORDER BY / LIMIT / OFFSET clauses to a query."""
        if self.order_by_keys:
            query = f"{query} ORDER BY {self._format_order_by()}"
        if self.limit is not None:
            query = f"{query} LIMIT {self.limit}"
        # OFFSET is independent of LIMIT: ``OFFSET n`` alone is valid SQL and
        # must not be dropped just because there is no row cap.
        if self.offset:
            query = f"{query} OFFSET {self.offset}"
        return query

    def _split_where_having(self):
        """Partition the filter into pre-aggregate WHERE and HAVING predicates.

        A predicate merged from WHERE and HAVING must be split by conjunct:
        aggregate-bearing conjuncts belong in HAVING, the rest in WHERE.
        Routing the whole conjunction to one clause produces invalid SQL.
        """
        if not self.filters:
            return None, None
        if not (self.group_by or self.aggregates):
            return self.filters, None
        return self._partition_conjuncts(self.filters)

    def _partition_conjuncts(self, predicate: Expression):
        """Split top-level conjuncts into (WHERE predicate, HAVING predicate)."""
        where_terms = []
        having_terms = []
        for conjunct in self._split_and(predicate):
            if self._predicate_uses_aggregates(conjunct):
                having_terms.append(conjunct)
            else:
                where_terms.append(conjunct)
        return self._join_and(where_terms), self._join_and(having_terms)

    def _split_and(self, predicate: Expression):
        """Flatten a predicate into its top-level AND conjuncts."""
        from .expressions import BinaryOp, BinaryOpType

        if isinstance(predicate, BinaryOp) and predicate.op == BinaryOpType.AND:
            return self._split_and(predicate.left) + self._split_and(predicate.right)
        return [predicate]

    def _join_and(self, terms):
        """Recombine conjuncts into one AND expression, or None when empty."""
        from .expressions import BinaryOp, BinaryOpType

        if not terms:
            return None
        result = terms[0]
        for term in terms[1:]:
            result = BinaryOp(op=BinaryOpType.AND, left=result, right=term)
        return result

    def _format_columns(self) -> str:
        """Format column list for SQL.

        If aggregates are present, use them directly.
        The aggregates list contains ALL SELECT expressions in order:
        - GROUP BY columns first
        - Then aggregate functions (COUNT, SUM, etc.)

        Otherwise format simple column list.
        """
        if self.aggregates:
            select_items = []
            index = 0
            while index < len(self.aggregates):
                expr = self.aggregates[index]
                expr_sql = expr.to_sql()
                alias_name = None
                if self.output_names and index < len(self.output_names):
                    alias_name = self.output_names[index]
                if alias_name:
                    select_items.append(f'{expr_sql} AS "{alias_name}"')
                else:
                    select_items.append(expr_sql)
                index += 1
            return ", ".join(select_items)

        if "*" in self.columns:
            return "*"

        quoted_cols = []
        for col in self.columns:
            quoted_cols.append(f'"{col}"')

        return ", ".join(quoted_cols)

    def _format_table_ref(self) -> str:
        """Format table reference for SQL."""
        ref = f'"{self.schema_name}"."{self.table_name}"'
        if self.alias:
            return f"{ref} AS {self.alias}"
        return ref

    def _format_group_by(self) -> str:
        """Format GROUP BY clause."""
        if not self.group_by:
            return ""

        group_items = []
        for expr in self.group_by:
            group_items.append(expr.to_sql())

        return ", ".join(group_items)

    def _format_order_by(self) -> str:
        """Format ORDER BY clause for SQL."""
        if not self.order_by_keys:
            return ""

        items = []
        for i in range(len(self.order_by_keys)):
            expr = self.order_by_keys[i]
            item = expr.to_sql()

            if self.order_by_ascending and i < len(self.order_by_ascending):
                if not self.order_by_ascending[i]:
                    item = f"{item} DESC"

            if self.order_by_nulls and i < len(self.order_by_nulls):
                nulls_spec = self.order_by_nulls[i]
                if nulls_spec:
                    item = f"{item} NULLS {nulls_spec}"

            items.append(item)

        return ", ".join(items)

    def _predicate_uses_aggregates(self, expr: Expression) -> bool:
        """Check if predicate references aggregate functions."""
        from .expressions import (
            FunctionCall,
            BinaryOp,
            UnaryOp,
            InList,
            BetweenExpression,
            ColumnRef,
        )

        if isinstance(expr, FunctionCall):
            if expr.is_aggregate:
                return True
            for arg in expr.args:
                if self._predicate_uses_aggregates(arg):
                    return True
            return False

        if isinstance(expr, BinaryOp):
            return self._predicate_uses_aggregates(
                expr.left
            ) or self._predicate_uses_aggregates(expr.right)

        if isinstance(expr, UnaryOp):
            return self._predicate_uses_aggregates(expr.operand)

        if isinstance(expr, InList):
            if self._predicate_uses_aggregates(expr.value):
                return True
            for option in expr.options:
                if self._predicate_uses_aggregates(option):
                    return True
            return False

        if isinstance(expr, BetweenExpression):
            return (
                self._predicate_uses_aggregates(expr.value)
                or self._predicate_uses_aggregates(expr.lower)
                or self._predicate_uses_aggregates(expr.upper)
            )

        return False

    def schema(self) -> pa.Schema:
        """Get output schema."""
        if self._schema is not None:
            return self._schema

        if self.datasource_connection is None:
            raise RuntimeError("Data source connection not set")

        query = self._build_query()
        self._schema = self.datasource_connection.get_query_schema(query)
        return self._schema

    def estimated_cost(self) -> float:
        # Cost based on table statistics
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        table_ref = f"{self.datasource}.{self.schema_name}.{self.table_name}"
        if self.alias:
            return f"PhysicalScan({table_ref} AS {self.alias})"
        return f"PhysicalScan({table_ref})"


@dataclass
class PhysicalProjection(PhysicalPlanNode):
    """Projection expressions."""

    input: PhysicalPlanNode
    expressions: List[Expression]
    output_names: List[str]
    distinct: bool = False

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute projection, de-duplicating rows when SELECT DISTINCT."""
        if not self.distinct:
            for batch in self.input.execute():
                yield self._project_batch(batch)
            return
        yield from self._execute_distinct()

    def _execute_distinct(self) -> Iterator[pa.RecordBatch]:
        """Project all input, then emit only distinct rows.

        Local DISTINCT used to be a no-op (the flag was decorative), so a
        query executed locally returned duplicates; grouping by every output
        column collapses them while preserving each column's type.
        """
        batches = []
        for batch in self.input.execute():
            batches.append(self._project_batch(batch))
        if len(batches) == 0:
            return
        table = pa.Table.from_batches(batches)
        deduped = table.group_by(table.column_names).aggregate([])
        for batch in deduped.select(table.column_names).to_batches():
            yield batch

    def _project_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Project a single batch."""
        from .expressions import ColumnRef

        columns = []
        output_names = []
        alias_map = self.column_aliases()

        for i, expr in enumerate(self.expressions):
            if isinstance(expr, ColumnRef):
                if expr.column == "*":
                    for col_idx in range(batch.num_columns):
                        columns.append(batch.column(col_idx))
                        output_names.append(batch.schema.field(col_idx).name)
                else:
                    column_data = self._resolve_column(expr, batch, alias_map)
                    columns.append(column_data)
                    output_names.append(self.output_names[i])
            else:
                evaluated = self._evaluate_expression(expr, batch)
                columns.append(evaluated)
                output_names.append(self.output_names[i])

        return pa.RecordBatch.from_arrays(columns, names=output_names)

    def _evaluate_expression(self, expr: Expression, batch: pa.RecordBatch) -> pa.Array:
        """Evaluate expression on batch."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        evaluator = ExpressionEvaluator(batch, alias_map=self.column_aliases())
        return evaluator.evaluate(expr)

    def schema(self) -> pa.Schema:
        """Get output schema."""
        from .expressions import ColumnRef

        input_schema = self.input.schema()
        fields = []

        for i, expr in enumerate(self.expressions):
            if isinstance(expr, ColumnRef):
                if expr.column == "*":
                    for field in input_schema:
                        fields.append(field)
                else:
                    field_index = input_schema.get_field_index(expr.column)
                    field = input_schema.field(field_index)
                    new_field = pa.field(self.output_names[i], field.type)
                    fields.append(new_field)
            else:
                inferred = self._infer_expression_type(expr, input_schema)
                fields.append(pa.field(self.output_names[i], inferred))

        return pa.schema(fields)

    def _infer_expression_type(
        self, expr: Expression, input_schema: pa.Schema
    ) -> pa.DataType:
        """Infer a computed expression's Arrow type by evaluating it
        against an empty batch with the input schema."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        empty_batch = pa.RecordBatch.from_pylist([], schema=input_schema)
        evaluator = ExpressionEvaluator(empty_batch, alias_map=self.column_aliases())
        return evaluator.evaluate(expr).type

    def estimated_cost(self) -> float:
        # Cost is input cost + projection cost
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalProjection({len(self.expressions)} exprs)"

    def _resolve_column(
        self,
        expr: Expression,
        batch: pa.RecordBatch,
        alias_map: Dict[Tuple[Optional[str], str], str],
    ) -> pa.Array:
        """Resolve column, including alias fallbacks."""
        from .expressions import ColumnRef

        if not isinstance(expr, ColumnRef):
            raise ValueError("Expected ColumnRef")

        mapped = None
        if expr.table is not None:
            mapped = self._lookup_alias(expr, alias_map)
            if mapped is not None:
                return batch.column(mapped)

        try:
            return batch.column(expr.column)
        except KeyError:
            if mapped is None:
                mapped = self._lookup_alias(expr, alias_map)
            if mapped is None:
                raise
            return batch.column(mapped)

    def _lookup_alias(
        self, expr: Expression, alias_map: Dict[Tuple[Optional[str], str], str]
    ) -> Optional[str]:
        from .expressions import ColumnRef

        if not isinstance(expr, ColumnRef):
            return None

        if expr.table is None:
            return None

        key = (expr.table, expr.column)
        return alias_map.get(key)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalFilter(PhysicalPlanNode):
    """Filter rows."""

    input: PhysicalPlanNode
    predicate: Expression

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute filter."""
        import pyarrow.compute as pc

        for batch in self.input.execute():
            mask = self._evaluate_predicate(batch)
            filtered = batch.filter(mask)
            if filtered.num_rows > 0:
                yield filtered

    def _evaluate_predicate(self, batch: pa.RecordBatch) -> pa.Array:
        """Evaluate predicate on batch and return boolean mask."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        evaluator = ExpressionEvaluator(batch, alias_map=self.column_aliases())
        return evaluator.evaluate(self.predicate)

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalFilter({self.predicate})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalHashJoin(PhysicalPlanNode):
    """Hash join implementation."""

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    join_type: JoinType
    left_keys: List[Expression]
    right_keys: List[Expression]
    build_side: str  # "left" or "right"

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute the hash join, via the merge engine when one is attached."""
        engine = self.merge_engine()
        if engine is not None:
            yield from self._execute_merge(engine)
            return
        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            yield from self._execute_semi_anti()
            return
        yield from self._execute_row_loop()

    def _execute_row_loop(self) -> Iterator[pa.RecordBatch]:
        """Row-at-a-time hash join (fallback when no merge engine is attached)."""
        from collections import defaultdict

        if self.build_side == "right":
            build_node = self.right
            probe_node = self.left
            build_keys = self.right_keys
            probe_keys = self.left_keys
            probe_is_left = True
        else:
            build_node = self.left
            probe_node = self.right
            build_keys = self.left_keys
            probe_keys = self.right_keys
            probe_is_left = False

        hash_table = defaultdict(list)
        build_batches = []
        matched_build_rows = set()

        build_aliases = build_node.column_aliases()
        probe_aliases = probe_node.column_aliases()
        for batch in build_node.execute():
            build_batches.append(batch)
            key_values = self._extract_key_values(batch, build_keys, build_aliases)

            for row_idx in range(batch.num_rows):
                key = _row_key_tuple(key_values, row_idx)
                # SQL equality never matches NULL keys, so rows with a NULL
                # key component are not indexed for matching.
                if None not in key:
                    hash_table[key].append((len(build_batches) - 1, row_idx))

        if len(build_batches) == 0:
            need_probe_outer = (
                (self.join_type == JoinType.LEFT and probe_is_left)
                or (self.join_type == JoinType.RIGHT and not probe_is_left)
                or (self.join_type == JoinType.FULL)
            )
            if need_probe_outer:
                for batch in probe_node.execute():
                    if probe_is_left:
                        yield self._create_left_outer_batch(batch)
                    else:
                        yield self._create_right_outer_batch(batch)
            return

        self._maybe_reduce_probe(probe_node, probe_keys, hash_table)

        for probe_batch in probe_node.execute():
            probe_key_values = self._extract_key_values(
                probe_batch, probe_keys, probe_aliases
            )

            for probe_row_idx in range(probe_batch.num_rows):
                probe_key = _row_key_tuple(probe_key_values, probe_row_idx)

                # A NULL key component can never satisfy SQL equality.
                if None not in probe_key and probe_key in hash_table:
                    build_rows = hash_table[probe_key]
                    for build_batch_idx, build_row_idx in build_rows:
                        build_batch = build_batches[build_batch_idx]
                        matched_build_rows.add((build_batch_idx, build_row_idx))
                        yield self._join_matched_rows(
                            probe_batch,
                            probe_row_idx,
                            build_batch,
                            build_row_idx,
                            probe_is_left,
                        )
                else:
                    need_probe_outer = (
                        (self.join_type == JoinType.LEFT and probe_is_left)
                        or (self.join_type == JoinType.RIGHT and not probe_is_left)
                        or (self.join_type == JoinType.FULL)
                    )
                    if need_probe_outer:
                        if probe_is_left:
                            yield self._create_left_outer_row(
                                probe_batch, probe_row_idx
                            )
                        else:
                            yield self._create_right_outer_row(
                                probe_batch, probe_row_idx
                            )

        need_build_outer = (
            (self.join_type == JoinType.RIGHT and probe_is_left)
            or (self.join_type == JoinType.LEFT and not probe_is_left)
            or (self.join_type == JoinType.FULL)
        )
        if need_build_outer:
            for build_batch_idx, build_batch in enumerate(build_batches):
                for build_row_idx in range(build_batch.num_rows):
                    if (build_batch_idx, build_row_idx) not in matched_build_rows:
                        if probe_is_left:
                            yield self._create_right_outer_row(
                                build_batch, build_row_idx
                            )
                        else:
                            yield self._create_left_outer_row_from_batch(
                                build_batch, build_row_idx
                            )

    def _execute_merge(self, engine) -> Iterator[pa.RecordBatch]:
        """Run the join through DuckDB; INNER keeps the G9 build-materialize path."""
        if self.join_type == JoinType.INNER:
            return self._execute_inner_merge(engine)
        return self._execute_streamed_merge(engine)

    def _execute_streamed_merge(self, engine) -> Iterator[pa.RecordBatch]:
        """Run a non-INNER join (LEFT/RIGHT/FULL/SEMI/ANTI) through DuckDB.

        Outer joins (LEFT/RIGHT/FULL) and ANTI keep non-matching rows, so the
        probe reduction does not apply; a SEMI join keeps only matching left
        rows, so the right's keys are pushed into the left as a semi-join
        reduction. The right side is drained into a table first — exactly as the
        row-at-a-time path drains the build side — so its source connection is
        released before the left side streams; otherwise deeply nested joins
        hold a connection open at every level and exhaust the pool. The left
        side streams.
        """
        right_batches = list(self.right.execute())
        if self.join_type == JoinType.SEMI:
            self._reduce_left_for_semi(right_batches)
        right_table = _table_from_batches(right_batches, self.right.schema())
        left_reader = _streaming_reader(self.left)
        inputs = {
            _MERGE_LEFT_RELATION: left_reader,
            _MERGE_RIGHT_RELATION: right_table,
        }
        sql = self._join_sql(left_reader.schema, right_table.schema)
        return engine.run(sql, inputs)

    def _reduce_left_for_semi(self, right_batches: List[pa.RecordBatch]) -> None:
        """Push the right's distinct keys into the left scan (semi-join reduction).

        A SEMI join keeps only left rows whose key matches a right key, so
        constraining the left to those keys drops no result row and avoids
        fetching unmatched rows from the left's (often remote) source. ANTI is
        excluded — it keeps non-matching rows, which this filter would remove.
        """
        value_tuples = self._distinct_build_keys(
            right_batches, self.right, self.right_keys
        )
        if value_tuples is None:
            return
        if self.left.apply_dynamic_filter(self.left_keys, value_tuples):
            _LOGGER.debug(
                "semi-join reduction: %d keys pushed to left", len(value_tuples)
            )

    def _execute_inner_merge(self, engine) -> Iterator[pa.RecordBatch]:
        """Run an INNER equi-join through the DuckDB merge engine.

        The build side is materialized once (its keys feed the G9 dynamic probe
        filter, and a hash join must read all build rows anyway). The output is
        rendered from the actual materialized input schemas, so its column names
        and types match the row-at-a-time path the engine relies on. The result
        streams back from DuckDB.
        """
        build_batches = self._materialize_build()
        if not build_batches:
            return iter(())
        self._reduce_probe_from_build(build_batches)
        inputs = self._merge_inputs(build_batches)
        left_schema = inputs[_MERGE_LEFT_RELATION].schema
        right_schema = inputs[_MERGE_RIGHT_RELATION].schema
        sql = self._join_sql(left_schema, right_schema)
        return engine.run(sql, inputs)

    def _build_probe_sides(self):
        """Return (build_node, probe_node, build_keys, probe_keys) per build_side."""
        if self.build_side == "right":
            return self.right, self.left, self.right_keys, self.left_keys
        return self.left, self.right, self.left_keys, self.right_keys

    def _materialize_build(self) -> List[pa.RecordBatch]:
        """Read the build side fully into a list of batches."""
        build_node = self._build_probe_sides()[0]
        batches = []
        for batch in build_node.execute():
            batches.append(batch)
        return batches

    def _maybe_reduce_probe(self, probe_node, probe_keys, hash_table) -> None:
        """Push an already-built hash table's keys into the probe (row-loop path).

        The row-at-a-time join has already materialized ``hash_table`` (its keys
        are the distinct non-NULL build keys), so reading them back is free here
        — no extra pass. INNER only; outer joins keep non-matching probe rows.
        The vectorized merge path uses ``_distinct_build_keys`` instead.
        """
        if self.join_type != JoinType.INNER:
            return
        keys = list(hash_table.keys())
        if not _within_dynamic_filter_cap(len(keys)):
            return
        if probe_node.apply_dynamic_filter(probe_keys, keys):
            _LOGGER.debug("dynamic filter applied to probe: %d keys", len(keys))

    def _reduce_probe_from_build(self, build_batches: List[pa.RecordBatch]) -> None:
        """Push the build side's distinct keys into the probe as an IN filter.

        Semi-join reduction: an INNER join only emits probe rows whose key
        matches a build key, so constraining the probe to those keys is safe and
        avoids fetching unmatched rows (the win for a remote probe). Limited to
        INNER joins; the distinct-key set is computed vectorized and skipped when
        empty or above the cap, so an over-cap build pays no per-row Python cost.
        """
        if self.join_type != JoinType.INNER:
            return
        build_node, probe_node, build_keys, probe_keys = self._build_probe_sides()
        value_tuples = self._distinct_build_keys(build_batches, build_node, build_keys)
        if value_tuples is None:
            return
        if probe_node.apply_dynamic_filter(probe_keys, value_tuples):
            _LOGGER.debug("dynamic filter applied to probe: %d keys", len(value_tuples))

    def _distinct_build_keys(
        self, build_batches: List[pa.RecordBatch], build_node, build_keys
    ) -> Optional[List[tuple]]:
        """Distinct non-NULL build-key tuples, or None if empty or above the cap.

        Distinct values are computed with pyarrow, so the cap is enforced on the
        vectorized distinct count: an over-cap build — which the probe would
        reject anyway — never pays the per-row Python boxing the row-at-a-time
        path would. Only the surviving (<= cap) keys are turned into tuples.
        """
        aliases = build_node.column_aliases()
        table = self._build_key_table(build_batches, build_keys, aliases)
        if not _within_dynamic_filter_cap(table.num_rows):
            return None
        return _key_tuples_from_table(table)

    def _build_key_table(self, build_batches, build_keys, aliases) -> "pa.Table":
        """Distinct non-NULL build-key combinations as a table (vectorized).

        Each batch's key columns are collected as-is and concatenated, then
        DuckDB-style deduplicated via ``drop_null().group_by(...).aggregate([])``
        — all in pyarrow's C++ kernels, no Python per row.
        """
        names = _key_column_names(len(build_keys))
        key_tables = []
        for batch in build_batches:
            arrays = self._extract_key_values(batch, build_keys, aliases)
            key_tables.append(pa.table(arrays, names=names))
        combined = pa.concat_tables(key_tables)
        return combined.drop_null().group_by(names).aggregate([])

    def _merge_inputs(self, build_batches: List[pa.RecordBatch]) -> Dict[str, object]:
        """Map self.left/self.right to the DuckDB relations under stable names.

        ``self.left`` always registers as the left relation and ``self.right``
        as the right, regardless of which side is built, so the output column
        order does not depend on the build/probe choice. The build side is
        already materialized (a hash join must read all build rows, and its keys
        feed the G9 probe filter); the probe side is handed to DuckDB as a lazy
        streaming reader and is never drained into a table by us.
        """
        build_node, probe_node, _, _ = self._build_probe_sides()
        build_table = _table_from_batches(build_batches, build_node.schema())
        probe_reader = _streaming_reader(probe_node)
        if self.build_side == "right":
            return {
                _MERGE_LEFT_RELATION: probe_reader,
                _MERGE_RIGHT_RELATION: build_table,
            }
        return {
            _MERGE_LEFT_RELATION: build_table,
            _MERGE_RIGHT_RELATION: probe_reader,
        }

    def _join_sql(self, left_schema: pa.Schema, right_schema: pa.Schema) -> str:
        """Render the join SELECT for this join type over the registered relations."""
        select_list = self._merge_select_list_for_type(left_schema, right_schema)
        return (
            f"SELECT {select_list} "
            f"FROM {_MERGE_LEFT_RELATION} AS l "
            f"{self._merge_join_keyword()} {_MERGE_RIGHT_RELATION} AS r "
            f"ON {self._merge_on_clause()}"
        )

    def _merge_join_keyword(self) -> str:
        """Map the join type to its DuckDB SQL join keyword."""
        keywords = {
            JoinType.INNER: "INNER JOIN",
            JoinType.LEFT: "LEFT JOIN",
            JoinType.RIGHT: "RIGHT JOIN",
            JoinType.FULL: "FULL JOIN",
            JoinType.SEMI: "SEMI JOIN",
            JoinType.ANTI: "ANTI JOIN",
        }
        return keywords[self.join_type]

    def _merge_select_list_for_type(
        self, left_schema: pa.Schema, right_schema: pa.Schema
    ) -> str:
        """SEMI/ANTI emit only the left columns; other joins emit both sides."""
        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            return self._merge_left_select_list(left_schema)
        return self._merge_select_list(left_schema, right_schema)

    def _merge_left_select_list(self, left_schema: pa.Schema) -> str:
        """Alias only the left columns, for SEMI/ANTI output."""
        parts = []
        for name in left_schema.names:
            parts.append(f'l."{name}" AS "{name}"')
        return ", ".join(parts)

    def _merge_select_list(
        self, left_schema: pa.Schema, right_schema: pa.Schema
    ) -> str:
        """Alias left then right columns to reproduce the join output schema.

        The actual materialized input schemas are used (not ``schema()``) so the
        output matches the row-at-a-time path, which reads real batch fields;
        a right column whose name collides with a left one is renamed with the
        ``right_`` prefix, exactly as ``_join_rows`` does.
        """
        left_names = left_schema.names
        left_name_set = set(left_names)
        parts = []
        for name in left_names:
            parts.append(f'l."{name}" AS "{name}"')
        for field in right_schema:
            output_name = field.name
            if output_name in left_name_set:
                output_name = f"right_{output_name}"
            parts.append(f'r."{field.name}" AS "{output_name}"')
        return ", ".join(parts)

    def _merge_on_clause(self) -> str:
        """Render the equi-join condition as l.key = r.key conjuncts."""
        left_aliases = self.left.column_aliases()
        right_aliases = self.right.column_aliases()
        conjuncts = []
        for left_key, right_key in zip(self.left_keys, self.right_keys):
            left_name = self._resolve_key_name(left_key, left_aliases)
            right_name = self._resolve_key_name(right_key, right_aliases)
            conjuncts.append(f'l."{left_name}" = r."{right_name}"')
        return " AND ".join(conjuncts)

    def _extract_key_values(
        self, batch: pa.RecordBatch, keys: List[Expression], aliases=None
    ) -> List[pa.Array]:
        """Extract key column values from a batch, resolving qualified names.

        When the input is a pushed multi-table query, two source columns may
        share a name; ``aliases`` maps a key's (table, column) to the unique
        output column so the right one is selected instead of failing on an
        ambiguous bare name.
        """
        from .expressions import ColumnRef

        key_arrays = []
        for key in keys:
            if not isinstance(key, ColumnRef):
                raise NotImplementedError(
                    f"Key extraction not implemented for {type(key)}"
                )
            name = self._resolve_key_name(key, aliases)
            key_arrays.append(batch.column(name))

        return key_arrays

    def _resolve_key_name(self, key, aliases) -> str:
        """Map a key column reference to its physical output column name."""
        if aliases:
            resolved = aliases.get((key.table, key.column))
            if resolved is not None:
                return resolved
        return key.column

    def _execute_semi_anti(self) -> Iterator[pa.RecordBatch]:
        """Execute SEMI or ANTI hash join."""
        from collections import defaultdict

        right_aliases = self.right.column_aliases()
        left_aliases = self.left.column_aliases()
        hash_table = defaultdict(bool)
        for batch in self.right.execute():
            key_values = self._extract_key_values(batch, self.right_keys, right_aliases)
            for row_idx in range(batch.num_rows):
                key = _row_key_tuple(key_values, row_idx)
                # SQL equality never matches NULL keys.
                if None not in key:
                    hash_table[key] = True

        for left_batch in self.left.execute():
            left_keys = self._extract_key_values(
                left_batch, self.left_keys, left_aliases
            )
            for left_idx in range(left_batch.num_rows):
                key = _row_key_tuple(left_keys, left_idx)
                match = None not in key and key in hash_table
                if self.join_type == JoinType.SEMI and match:
                    yield self._left_row_batch(left_batch, left_idx)
                if self.join_type == JoinType.ANTI and not match:
                    yield self._left_row_batch(left_batch, left_idx)

    def _join_matched_rows(
        self,
        probe_batch: pa.RecordBatch,
        probe_row_idx: int,
        build_batch: pa.RecordBatch,
        build_row_idx: int,
        probe_is_left: bool,
    ) -> pa.RecordBatch:
        """Join a matched probe/build row pair, always in left-then-right order.

        The build side may be either input (chosen for performance), so order
        the output by which input is logically the left one to keep the column
        order independent of the build/probe choice.
        """
        if probe_is_left:
            return self._join_rows(
                probe_batch, probe_row_idx, build_batch, build_row_idx
            )
        return self._join_rows(build_batch, build_row_idx, probe_batch, probe_row_idx)

    def _join_rows(
        self,
        left_batch: pa.RecordBatch,
        left_row_idx: int,
        right_batch: pa.RecordBatch,
        right_row_idx: int,
    ) -> pa.RecordBatch:
        """Join two rows into a single batch."""
        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            col = left_batch.column(i)
            field = left_batch.schema.field(i)
            scalar_val = col[left_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)

        for i in range(right_batch.num_columns):
            col = right_batch.column(i)
            field = right_batch.schema.field(i)
            name = field.name

            if name in names:
                name = f"right_{name}"

            scalar_val = col[right_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_left_outer_batch(self, left_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Create batch with NULLs for right side (for LEFT OUTER JOIN)."""
        right_schema = self.right.schema()

        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            arrays.append(left_batch.column(i))
            field = left_batch.schema.field(i)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            null_array = pa.array([None] * left_batch.num_rows, type=field.type)
            arrays.append(null_array)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_left_outer_row(
        self, left_batch: pa.RecordBatch, left_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for right side."""
        right_schema = self.right.schema()

        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            col = left_batch.column(i)
            field = left_batch.schema.field(i)
            scalar_val = col[left_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            null_array = pa.array([None], type=field.type)
            arrays.append(null_array)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_right_outer_row(
        self, right_batch: pa.RecordBatch, right_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for left side and data from right side."""
        left_schema = self.left.schema()

        arrays = []
        names = []

        for field in left_schema:
            null_array = pa.array([None], type=field.type)
            arrays.append(null_array)
            names.append(field.name)

        for i in range(right_batch.num_columns):
            col = right_batch.column(i)
            field = right_batch.schema.field(i)
            name = field.name

            if name in names:
                name = f"right_{name}"

            scalar_val = col[right_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_right_outer_batch(self, right_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Create batch with NULLs for left side (for RIGHT OUTER JOIN)."""
        left_schema = self.left.schema()

        arrays = []
        names = []

        for field in left_schema:
            null_array = pa.array([None] * right_batch.num_rows, type=field.type)
            arrays.append(null_array)
            names.append(field.name)

        for i in range(right_batch.num_columns):
            field = right_batch.schema.field(i)
            name = field.name
            if name in names:
                name = f"right_{name}"
            arrays.append(right_batch.column(i))
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_left_outer_row_from_batch(
        self, left_batch: pa.RecordBatch, left_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row from left batch with NULLs for right side."""
        right_schema = self.right.schema()

        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            col = left_batch.column(i)
            field = left_batch.schema.field(i)
            scalar_val = col[left_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            null_array = pa.array([None], type=field.type)
            arrays.append(null_array)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _left_row_batch(
        self,
        batch: pa.RecordBatch,
        row_idx: int,
    ) -> pa.RecordBatch:
        """Create single-row batch with only left columns."""
        arrays = []
        names = []
        for i in range(batch.num_columns):
            col = batch.column(i)
            field = batch.schema.field(i)
            scalar_val = col[row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def schema(self) -> pa.Schema:
        """Combine schemas from both sides."""
        left_schema = self.left.schema()
        right_schema = self.right.schema()

        fields = []
        names = []

        for field in left_schema:
            fields.append(field)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            fields.append(pa.field(name, field.type, nullable=field.nullable))
            names.append(name)

        return pa.schema(fields)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Map qualified columns to their (possibly renamed) output names."""
        return _join_output_aliases(self.left, self.right)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashJoin({self.join_type.value}, build={self.build_side})"


@dataclass
class PhysicalRemoteJoin(PhysicalPlanNode):
    """Join executed directly on a single data source.

    NOT dead code — intentionally retained for future selective pushdown.
    The G1 single-source generator (``optimizer/single_source_pushdown.py``,
    surfaced as ``PhysicalRemoteQuery``) currently pushes the *whole* maximal
    same-source subtree, so the planner no longer constructs this node (verified
    by instrumentation: 0 hits across the suite). It stays because that greedy
    "push everything" policy breaks down once part of the data lives on the
    coordinator — most importantly when we run as a query accelerator with a
    large table cached locally and want to push only the remote portion of a
    join (e.g. a 3-table join where one table is cached on our side). Reviving
    selective/partial pushdown will reuse this node's per-side SQL building.
    See ``selective-pushdown.md`` for the cases and the planned machinery.
    Do not delete without sign-off.
    """

    left: PhysicalScan
    right: PhysicalScan
    join_type: JoinType
    condition: Expression
    datasource_connection: Any
    group_by: Optional[List[Expression]] = None
    aggregates: Optional[List[Expression]] = None
    output_names: Optional[List[str]] = None
    distinct: bool = False
    order_by_keys: Optional[List[Expression]] = None
    order_by_ascending: Optional[List[bool]] = None
    order_by_nulls: Optional[List[Optional[str]]] = None
    _schema: Optional[pa.Schema] = None
    _column_alias_map: Dict[Tuple[Optional[str], str], str] = field(
        default_factory=dict, init=False
    )

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def execute(self) -> Iterator[pa.RecordBatch]:
        query = self._build_query()
        self.datasource_connection.ensure_connected()
        batches = self.datasource_connection.execute_query(query)
        for batch in batches:
            yield batch

    def schema(self) -> pa.Schema:
        if self._schema is None:
            query = self._build_query()
            self._schema = self.datasource_connection.get_query_schema(query)
        return self._schema

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalRemoteJoin({self.join_type.value})"

    def _build_query(self) -> str:
        """Construct SQL for remote join, preserving per-side filters."""
        self._column_alias_map = {}
        base_query = self._build_join_query()
        grouped_query = self._apply_group_by(base_query)
        return self._apply_order_by(grouped_query)

    def _build_join_query(self) -> str:
        """Build SELECT and JOIN clauses for remote execution."""
        select_clause = self._build_select_clause()
        left_source = self._build_source(self.left)
        right_source = self._build_source(self.right)
        join_keyword = self._join_keyword()
        condition_sql = self.condition.to_sql()
        select_kw = "SELECT DISTINCT" if self.distinct else "SELECT"
        return (
            f"{select_kw} {select_clause} "
            f"FROM {left_source} {join_keyword} {right_source} "
            f"ON {condition_sql}"
        )

    def _apply_group_by(self, query: str) -> str:
        """Append GROUP BY clause when needed."""
        if not self.group_by:
            return query
        group_clause = self._build_group_by_clause()
        return f"{query} GROUP BY {group_clause}"

    def _apply_order_by(self, query: str) -> str:
        """Append ORDER BY clause when present."""
        if not self.order_by_keys:
            return query
        order_items = []
        for index in range(len(self.order_by_keys)):
            item = self._build_order_item(index)
            order_items.append(item)
        order_clause = ", ".join(order_items)
        return f"{query} ORDER BY {order_clause}"

    def _build_order_item(self, index: int) -> str:
        """Build a single ORDER BY item with direction and NULLS handling."""
        item = self.order_by_keys[index].to_sql()
        item = self._apply_order_direction(item, index)
        return self._apply_nulls_order(item, index)

    def _apply_order_direction(self, item: str, index: int) -> str:
        """Add DESC keyword when sorting direction is descending."""
        if self.order_by_ascending is None:
            return item
        if index >= len(self.order_by_ascending):
            return item
        if self.order_by_ascending[index]:
            return item
        return f"{item} DESC"

    def _apply_nulls_order(self, item: str, index: int) -> str:
        """Append NULLS FIRST/LAST when specified."""
        if self.order_by_nulls is None:
            return item
        if index >= len(self.order_by_nulls):
            return item
        nulls_spec = self.order_by_nulls[index]
        if not nulls_spec:
            return item
        return f"{item} NULLS {nulls_spec}"

    def _build_select_clause(self) -> str:
        if self.aggregates:
            return self._build_aggregate_select_clause()
        items = []
        seen = set()
        self._append_select_items(self.left, items, seen, False)
        self._append_select_items(self.right, items, seen, True)
        return ", ".join(items)

    def _build_aggregate_select_clause(self) -> str:
        items = []
        names = self.output_names or []
        expressions = self.aggregates or []
        index = 0
        while index < len(expressions):
            expr_sql = expressions[index].to_sql()
            alias = names[index] if index < len(names) else None
            if alias:
                items.append(f'{expr_sql} AS "{alias}"')
            else:
                items.append(expr_sql)
            index += 1
        return ", ".join(items)

    def _build_group_by_clause(self) -> str:
        parts = []
        for expr in self.group_by or []:
            parts.append(expr.to_sql())
        return ", ".join(parts)

    def _append_select_items(
        self, scan: PhysicalScan, items: List[str], seen: Set[str], is_right: bool
    ) -> None:
        columns = self._resolve_columns(scan)
        alias = self._scan_alias(scan)
        for column in columns:
            source = self._format_column(alias, column)
            output = self._select_output_name(column, seen, is_right)
            items.append(f'{source} AS "{output}"')
            seen.add(output)
            self._register_column_alias(scan, column, output)

    def _resolve_columns(self, scan: PhysicalScan) -> List[str]:
        if scan.columns and "*" in scan.columns:
            schema = scan.schema()
            names = []
            for field in schema:
                names.append(field.name)
            return names
        return scan.columns

    def _format_column(self, alias: str, column: str) -> str:
        return f'{alias}."{column}"'

    def _select_output_name(self, column: str, seen: Set[str], is_right: bool) -> str:
        name = column
        if is_right and name in seen:
            name = f"right_{name}"
        return name

    def _join_keyword(self) -> str:
        return f"{self.join_type.value} JOIN"

    def _scan_alias(self, scan: PhysicalScan) -> str:
        if scan.alias:
            return scan.alias
        return scan.table_name

    def _build_source(self, scan: PhysicalScan) -> str:
        """Build a FROM source for a scan, applying side-local filters.

        A side filter may be written against the join alias (``p.category``).
        The alias must therefore be applied to the table INSIDE the derived
        table as well, since the outer alias is not in scope within the
        subquery — otherwise the remote engine raises a binder error.
        """
        table_ref = f'"{scan.schema_name}"."{scan.table_name}"'
        alias = scan.alias if scan.alias else scan.table_name
        if not scan.filters:
            return self._aliased(table_ref, alias)
        inner = self._aliased(table_ref, alias)
        filter_sql = scan.filters.to_sql()
        derived = f"(SELECT * FROM {inner} WHERE {filter_sql})"
        return self._aliased(derived, alias)

    def _aliased(self, source: str, alias: Optional[str]) -> str:
        """Append an ``AS alias`` suffix when an alias is present."""
        if alias:
            return f"{source} AS {alias}"
        return source

    def _register_column_alias(
        self, scan: PhysicalScan, column: str, output: str
    ) -> None:
        identifiers = set()
        if scan.alias:
            identifiers.add(scan.alias)
        identifiers.add(scan.table_name)

        for ident in identifiers:
            if ident:
                self._column_alias_map[(ident, column)] = output

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        if self.aggregates and self.output_names:
            alias_map: Dict[Tuple[Optional[str], str], str] = {}
            for name in self.output_names:
                alias_map[(None, name)] = name
            return alias_map
        return self._column_alias_map


@dataclass
class PhysicalNestedLoopJoin(PhysicalPlanNode):
    """Nested loop join implementation."""

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    join_type: JoinType
    condition: Optional[Expression]

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Run the join in the merge engine (DuckDB), or a Python loop if absent.

        The arbitrary ``condition`` (in practice the null-aware SEMI/ANTI/LEFT
        predicate decorrelation produces, ``x = v OR x IS NULL OR v IS NULL``) is
        rendered to DuckDB with each column resolved to the left or right input.
        DuckDB materializes each side once and joins set-based — so the inner
        side is never re-scanned per outer row (which, cross-source, would be a
        remote query per row). The Python loop remains only as a no-engine path.
        """
        engine = self.merge_engine()
        if engine is not None:
            yield from self._execute_merge(engine)
            return
        yield from self._execute_python()

    def _execute_merge(self, engine) -> Iterator[pa.RecordBatch]:
        """Materialize the right, stream the left, and join in DuckDB."""
        right_table = _table_from_batches(
            list(self.right.execute()), self.right.schema()
        )
        left_reader = _streaming_reader(self.left)
        inputs = {
            _MERGE_LEFT_RELATION: left_reader,
            _MERGE_RIGHT_RELATION: right_table,
        }
        sql = self._merge_sql(left_reader.schema, right_table.schema)
        return engine.run(sql, inputs)

    def _merge_sql(self, left_schema: pa.Schema, right_schema: pa.Schema) -> str:
        """Render the join SELECT over the two registered merge relations."""
        select_list = _merge_join_select_list(self.join_type, left_schema, right_schema)
        on_clause = "TRUE"
        if self.condition is not None:
            qualified = _qualify_join_condition(
                self.condition, set(left_schema.names), set(right_schema.names)
            )
            on_clause = qualified.to_sql()
        return (
            f"SELECT {select_list} "
            f"FROM {_MERGE_LEFT_RELATION} AS l "
            f"{_MERGE_JOIN_KEYWORDS[self.join_type]} {_MERGE_RIGHT_RELATION} AS r "
            f"ON {on_clause}"
        )

    def _execute_python(self) -> Iterator[pa.RecordBatch]:
        """Row-at-a-time nested loop, used only when no merge engine is attached."""
        import pyarrow.compute as pc

        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            for batch in self._execute_semi_anti():
                yield batch
            return

        right_batches = []
        for batch in self.right.execute():
            right_batches.append(batch)

        if len(right_batches) == 0:
            if self.join_type in (JoinType.LEFT, JoinType.FULL):
                for batch in self.left.execute():
                    yield self._create_left_outer_batch(batch)
            return

        matched_right_rows = set()

        for left_batch in self.left.execute():
            for left_row_idx in range(left_batch.num_rows):
                matched = False

                for right_batch_idx, right_batch in enumerate(right_batches):
                    for right_row_idx in range(right_batch.num_rows):
                        if self.condition is None or self._evaluate_condition(
                            left_batch, left_row_idx, right_batch, right_row_idx
                        ):
                            yield self._join_rows(
                                left_batch, left_row_idx, right_batch, right_row_idx
                            )
                            matched = True
                            matched_right_rows.add((right_batch_idx, right_row_idx))

                if not matched and self.join_type in (JoinType.LEFT, JoinType.FULL):
                    yield self._create_left_outer_row(left_batch, left_row_idx)

        if self.join_type in (JoinType.RIGHT, JoinType.FULL):
            for right_batch_idx, right_batch in enumerate(right_batches):
                for right_row_idx in range(right_batch.num_rows):
                    if (right_batch_idx, right_row_idx) not in matched_right_rows:
                        yield self._create_right_outer_row(right_batch, right_row_idx)

    def _execute_semi_anti(self) -> Iterator[pa.RecordBatch]:
        """Execute SEMI or ANTI nested loop join."""
        for left_batch in self.left.execute():
            for left_row_idx in range(left_batch.num_rows):
                matched = False
                for right_batch in self.right.execute():
                    for right_row_idx in range(right_batch.num_rows):
                        if self.condition is None or self._evaluate_condition(
                            left_batch,
                            left_row_idx,
                            right_batch,
                            right_row_idx,
                        ):
                            matched = True
                            break
                    if matched:
                        break
                if self.join_type == JoinType.SEMI and matched:
                    yield self._left_row_batch(left_batch, left_row_idx)
                if self.join_type == JoinType.ANTI and not matched:
                    yield self._left_row_batch(left_batch, left_row_idx)

    def _evaluate_condition(
        self,
        left_batch: pa.RecordBatch,
        left_row_idx: int,
        right_batch: pa.RecordBatch,
        right_row_idx: int,
    ) -> bool:
        """Evaluate join condition for two rows."""
        from .expressions import BinaryOp, BinaryOpType, ColumnRef, Literal
        import pyarrow.compute as pc

        if self.condition is None:
            return True

        joined = self._join_rows(left_batch, left_row_idx, right_batch, right_row_idx)
        result = self._evaluate_expression_on_batch(self.condition, joined)

        return result[0].as_py() if len(result) > 0 else False

    def _evaluate_expression_on_batch(
        self, expr: Expression, batch: pa.RecordBatch
    ) -> pa.Array:
        """Evaluate expression on a batch."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        evaluator = ExpressionEvaluator(batch)
        return evaluator.evaluate(expr)

    def _join_rows(
        self,
        left_batch: pa.RecordBatch,
        left_row_idx: int,
        right_batch: pa.RecordBatch,
        right_row_idx: int,
    ) -> pa.RecordBatch:
        """Join two rows into a single batch."""
        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            col = left_batch.column(i)
            field = left_batch.schema.field(i)
            scalar_val = col[left_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)

        for i in range(right_batch.num_columns):
            col = right_batch.column(i)
            field = right_batch.schema.field(i)
            name = field.name

            if name in names:
                name = f"right_{name}"

            scalar_val = col[right_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_left_outer_batch(self, left_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Create batch with NULLs for right side."""
        right_schema = self.right.schema()

        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            arrays.append(left_batch.column(i))
            field = left_batch.schema.field(i)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            null_array = pa.array([None] * left_batch.num_rows, type=field.type)
            arrays.append(null_array)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_left_outer_row(
        self, left_batch: pa.RecordBatch, left_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for right side."""
        right_schema = self.right.schema()

        arrays = []
        names = []

        for i in range(left_batch.num_columns):
            col = left_batch.column(i)
            field = left_batch.schema.field(i)
            scalar_val = col[left_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            null_array = pa.array([None], type=field.type)
            arrays.append(null_array)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _create_right_outer_row(
        self, right_batch: pa.RecordBatch, right_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for left side and data from right side."""
        left_schema = self.left.schema()

        arrays = []
        names = []

        for field in left_schema:
            null_array = pa.array([None], type=field.type)
            arrays.append(null_array)
            names.append(field.name)

        for i in range(right_batch.num_columns):
            col = right_batch.column(i)
            field = right_batch.schema.field(i)
            name = field.name

            if name in names:
                name = f"right_{name}"

            scalar_val = col[right_row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(name)

        return pa.RecordBatch.from_arrays(arrays, names=names)

    def _left_row_batch(
        self,
        batch: pa.RecordBatch,
        row_idx: int,
    ) -> pa.RecordBatch:
        """Create single-row batch with only left columns."""
        arrays = []
        names = []
        for i in range(batch.num_columns):
            col = batch.column(i)
            field = batch.schema.field(i)
            scalar_val = col[row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)
        return pa.RecordBatch.from_arrays(arrays, names=names)

    def schema(self) -> pa.Schema:
        """Combine schemas from both sides."""
        left_schema = self.left.schema()
        right_schema = self.right.schema()

        fields = []
        names = []

        for field in left_schema:
            fields.append(field)
            names.append(field.name)

        for field in right_schema:
            name = field.name
            if name in names:
                name = f"right_{name}"
            fields.append(pa.field(name, field.type, nullable=field.nullable))
            names.append(name)

        return pa.schema(fields)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Map qualified columns to their (possibly renamed) output names."""
        return _join_output_aliases(self.left, self.right)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalNestedLoopJoin({self.join_type.value})"

    def _left_row_batch(
        self,
        batch: pa.RecordBatch,
        row_idx: int,
    ) -> pa.RecordBatch:
        """Create single-row batch with only left columns."""
        arrays = []
        names = []
        for i in range(batch.num_columns):
            col = batch.column(i)
            field = batch.schema.field(i)
            scalar_val = col[row_idx]
            arr = pa.array([scalar_val.as_py()], type=field.type)
            arrays.append(arr)
            names.append(field.name)
        return pa.RecordBatch.from_arrays(arrays, names=names)


@dataclass
class PhysicalHashAggregate(PhysicalPlanNode):
    """Hash-based aggregation."""

    input: PhysicalPlanNode
    group_by: List[Expression]
    aggregates: List[Expression]
    output_names: List[str]

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute hash aggregation, via the merge engine when one is attached."""
        engine = self.merge_engine()
        if engine is not None and self._can_merge_aggregate():
            yield from self._execute_merge_aggregate(engine)
            return

        if self._supports_streaming():
            result_batch = self._execute_streaming()
        else:
            result_batch = self._execute_materialized()

        if result_batch.num_rows > 0:
            yield result_batch

    def _can_merge_aggregate(self) -> bool:
        """Whether group-bys and aggregates are simple enough to render to SQL."""
        return self._group_by_renderable() and self._aggregates_renderable()

    def _group_by_renderable(self) -> bool:
        """True when every group-by key is a plain column reference."""
        from .expressions import ColumnRef

        for expr in self.group_by:
            if not isinstance(expr, ColumnRef):
                return False
        return True

    def _aggregates_renderable(self) -> bool:
        """True when every SELECT expression is a column or a column aggregate."""
        for expr in self.aggregates:
            if not self._agg_expr_renderable(expr):
                return False
        return True

    def _agg_expr_renderable(self, expr: Expression) -> bool:
        """A group column, or an aggregate function over plain column arguments."""
        from .expressions import ColumnRef, FunctionCall

        if isinstance(expr, ColumnRef):
            return True
        return (
            isinstance(expr, FunctionCall)
            and expr.is_aggregate
            and self._args_are_columns(expr)
        )

    def _args_are_columns(self, func: "FunctionCall") -> bool:
        """True when every aggregate argument is a plain column reference."""
        from .expressions import ColumnRef

        for arg in func.args:
            if not isinstance(arg, ColumnRef):
                return False
        return True

    def _execute_merge_aggregate(self, engine) -> Iterator[pa.RecordBatch]:
        """Run the GROUP BY through DuckDB, casting output to the declared schema."""
        reader = _streaming_reader(self.input)
        sql = self._aggregate_sql(self.input.column_aliases())
        target = self.schema()
        for batch in engine.run(sql, {_MERGE_AGG_RELATION: reader}):
            yield _cast_batch_to_schema(batch, target)

    def _aggregate_sql(self, aliases) -> str:
        """Render ``SELECT <aggs> FROM in_agg [GROUP BY <keys>]``."""
        select_list = self._aggregate_select_list(aliases)
        sql = f"SELECT {select_list} FROM {_MERGE_AGG_RELATION}"
        group_list = self._aggregate_group_list(aliases)
        if group_list:
            sql += f" GROUP BY {group_list}"
        return sql

    def _aggregate_select_list(self, aliases) -> str:
        """Alias each SELECT expression to its output name."""
        parts = []
        for expr, name in zip(self.aggregates, self.output_names):
            parts.append(f'{self._render_agg_expr(expr, aliases)} AS "{name}"')
        return ", ".join(parts)

    def _aggregate_group_list(self, aliases) -> str:
        """Render the GROUP BY keys, or empty for a global aggregate."""
        parts = []
        for expr in self.group_by:
            parts.append(self._render_agg_expr(expr, aliases))
        return ", ".join(parts)

    def _render_agg_expr(self, expr: Expression, aliases) -> str:
        """Render a column or aggregate against the input's physical column names."""
        from .expressions import ColumnRef

        if isinstance(expr, ColumnRef):
            if expr.column == "*":
                return "*"
            return f'"{self._resolve_agg_column(expr, aliases)}"'
        return self._render_agg_function(expr, aliases)

    def _resolve_agg_column(self, expr, aliases) -> str:
        """Resolve a column reference to its physical name in the input relation."""
        if aliases:
            resolved = aliases.get((expr.table, expr.column))
            if resolved is not None:
                return resolved
        return expr.column

    def _render_agg_function(self, func: "FunctionCall", aliases) -> str:
        """Render an aggregate call, preserving DISTINCT and COUNT(*)."""
        name = func.function_name.upper()
        distinct = "DISTINCT " if func.distinct else ""
        inner = "*"
        if func.args:
            inner = self._render_agg_expr(func.args[0], aliases)
        return f"{name}({distinct}{inner})"

    def _supports_streaming(self) -> bool:
        """Return True when streaming aggregation can evaluate all expressions."""
        from .expressions import ColumnRef, FunctionCall

        def is_supported(expr: Expression) -> bool:
            if isinstance(expr, ColumnRef):
                return True
            if isinstance(expr, FunctionCall) and expr.is_aggregate:
                return all(is_supported(arg) for arg in expr.args)
            return False

        return all(is_supported(expr) for expr in self.aggregates)

    def _execute_streaming(self) -> pa.RecordBatch:
        from collections import defaultdict

        hash_table = defaultdict(lambda: self._create_accumulator())

        for batch in self.input.execute():
            self._accumulate_batch(batch, hash_table)

        return self._finalize_aggregates(hash_table)

    def _create_accumulator(self) -> dict:
        """Create accumulator for aggregate functions."""
        accumulator = {}
        for i, expr in enumerate(self.aggregates):
            accumulator[i] = self._create_single_accumulator(expr)
        return accumulator

    def _create_single_accumulator(self, expr: Expression) -> dict:
        """Create accumulator for a single aggregate."""
        from .expressions import FunctionCall

        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            func_name = expr.function_name.upper()
            return {
                "type": func_name,
                "count": 0,
                "sum": 0,
                "min": None,
                "max": None,
                "distinct": expr.distinct,
                "seen": set(),
            }

        return {"type": "VALUE", "value": None}

    def _accumulate_batch(self, batch: pa.RecordBatch, hash_table: dict):
        """Accumulate values from a batch."""
        for row_idx in range(batch.num_rows):
            group_key = self._extract_group_key(batch, row_idx)
            self._accumulate_row(batch, row_idx, hash_table[group_key])

    def _extract_group_key(self, batch: pa.RecordBatch, row_idx: int) -> tuple:
        """Extract grouping key from a row."""
        from .expressions import ColumnRef

        if len(self.group_by) == 0:
            return ()

        key_values = []
        for expr in self.group_by:
            if isinstance(expr, ColumnRef):
                col = batch.column(expr.column)
                value = col[row_idx].as_py()
                key_values.append(value)
        return tuple(key_values)

    def _accumulate_row(self, batch: pa.RecordBatch, row_idx: int, accumulator: dict):
        """Accumulate values from a single row."""
        for i, expr in enumerate(self.aggregates):
            value = self._extract_value_from_row(batch, row_idx, expr)
            self._update_accumulator(accumulator[i], value)

    def _extract_value_from_row(
        self, batch: pa.RecordBatch, row_idx: int, expr: Expression
    ):
        """Extract value for an expression from a row."""
        from .expressions import ColumnRef, FunctionCall

        if isinstance(expr, ColumnRef):
            if expr.column == "*":
                return 1
            col = batch.column(expr.column)
            return col[row_idx].as_py()

        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            if len(expr.args) == 0:
                return 1
            arg = expr.args[0]
            return self._extract_value_from_row(batch, row_idx, arg)

        return self._evaluate_scalar(batch, row_idx, expr)

    def _evaluate_scalar(self, batch: pa.RecordBatch, row_idx: int, expr: Expression):
        """Evaluate a non-trivial aggregate argument (e.g. ``price * qty``).

        Computing it through the shared evaluator avoids silently dropping
        the value to None, which would corrupt SUM/AVG over expressions.
        """
        from ..executor.expression_evaluator import ExpressionEvaluator

        row = batch.slice(row_idx, 1)
        result = ExpressionEvaluator(row).evaluate(expr)
        return result[0].as_py()

    def _update_accumulator(self, acc: dict, value):
        """Update accumulator with a value."""
        acc_type = acc["type"]

        if acc_type == "VALUE":
            acc["value"] = value
            return

        if not self._distinct_admits(acc, value):
            return

        if acc_type == "COUNT":
            # SQL COUNT skips NULLs; COUNT(*) always accumulates a literal 1.
            if value is not None:
                acc["count"] += 1
            return

        if value is None:
            return

        self._update_numeric_accumulator(acc, value, acc_type)

    def _distinct_admits(self, acc: dict, value) -> bool:
        """For a DISTINCT aggregate, accept each non-NULL value only once.

        NULLs are passed through here and dropped by the per-aggregate NULL
        handling, matching SQL's exclusion of NULLs from DISTINCT aggregates.
        """
        if not acc.get("distinct"):
            return True
        if value is None:
            return True
        if value in acc["seen"]:
            return False
        acc["seen"].add(value)
        return True

    def _update_numeric_accumulator(self, acc: dict, value, acc_type: str):
        """Update a SUM/AVG (numeric) or MIN/MAX (any comparable) accumulator.

        MIN/MAX keep the value's native type so string columns (``MIN(status)``)
        compare correctly; SUM keeps integer inputs integral rather than
        forcing float64.
        """
        acc["count"] += 1
        if acc_type in ("SUM", "AVG"):
            acc["sum"] += value
        if acc_type in ("MIN", "MAX"):
            self._update_min_max(acc, value, acc_type)

    def _update_min_max(self, acc: dict, value, acc_type: str):
        """Update min/max accumulator over natively comparable values."""
        if acc["min"] is None:
            acc["min"] = value
            acc["max"] = value
        else:
            if acc_type == "MIN":
                acc["min"] = min(acc["min"], value)
            if acc_type == "MAX":
                acc["max"] = max(acc["max"], value)

    def _finalize_aggregates(self, hash_table: dict) -> pa.RecordBatch:
        """Convert hash table to record batch."""
        if len(hash_table) == 0 and len(self.group_by) == 0:
            # SQL: a global aggregate over empty input yields exactly one
            # row (COUNT = 0, other aggregates NULL).
            hash_table[()] = self._create_accumulator()
        if len(hash_table) == 0:
            return self._create_empty_batch()

        columns = []
        for i in range(len(self.output_names)):
            col_values = self._extract_column_values(hash_table, i)
            columns.append(col_values)

        return pa.RecordBatch.from_arrays(columns, names=self.output_names)

    def _extract_column_values(self, hash_table: dict, col_idx: int) -> pa.Array:
        """Extract values for a column.

        The aggregates list contains ALL expressions from SELECT clause:
        - Non-aggregate columns (ColumnRef) - these match group_by expressions
        - Aggregate functions (FunctionCall with is_aggregate=True)

        Accumulators are indexed by position in aggregates (accumulator[i] for aggregates[i]).
        """
        from .expressions import FunctionCall

        expr = self.aggregates[col_idx]

        values = []
        for group_key, accumulators in hash_table.items():
            # Check if this is an aggregate function
            if isinstance(expr, FunctionCall) and expr.is_aggregate:
                # Accumulators are indexed by position in aggregates
                value = self._finalize_accumulator(accumulators[col_idx])
                values.append(value)
            else:
                # This is a group-by column - find which one
                group_idx = self._find_group_by_index(expr)
                if group_idx is not None:
                    values.append(group_key[group_idx] if group_key else None)
                else:
                    # Fallback: might be a constant or expression
                    values.append(None)

        return pa.array(values)

    def _find_group_by_index(self, expr: Expression) -> int:
        """Find which group_by expression this matches."""
        from .expressions import ColumnRef

        # For now, simple match by column name
        if isinstance(expr, ColumnRef):
            for i, group_expr in enumerate(self.group_by):
                if (
                    isinstance(group_expr, ColumnRef)
                    and group_expr.column == expr.column
                ):
                    return i

        return None

    def _finalize_accumulator(self, acc: dict):
        """Finalize accumulator to get final value."""
        acc_type = acc["type"]

        if acc_type == "VALUE":
            return acc["value"]
        if acc_type == "COUNT":
            return acc["count"]
        if acc_type == "SUM":
            # SQL SUM over zero accumulated values is NULL, not 0.
            return acc["sum"] if acc["count"] > 0 else None
        if acc_type == "AVG":
            return acc["sum"] / acc["count"] if acc["count"] > 0 else None
        if acc_type == "MIN":
            return acc["min"]
        if acc_type == "MAX":
            return acc["max"]

        raise NotImplementedError(f"Local aggregate function not supported: {acc_type}")

    def _create_empty_batch(self) -> pa.RecordBatch:
        """Create empty batch with proper schema."""
        arrays = []
        for _ in self.output_names:
            arrays.append(pa.array([]))
        return pa.RecordBatch.from_arrays(arrays, names=self.output_names)

    def _execute_materialized(self) -> pa.RecordBatch:
        """Fallback aggregation that materializes input and uses pyarrow compute."""
        input_table = self._materialize_input()
        groups = self._build_groups(input_table)
        return self._compute_result(input_table, groups)

    def _materialize_input(self) -> pa.Table:
        """Materialize all input batches into a pyarrow.Table."""
        batches = [batch for batch in self.input.execute()]
        if not batches:
            return pa.Table.from_arrays([], names=[])
        return pa.Table.from_batches(batches)

    def _build_groups(self, table: pa.Table) -> dict:
        """Build hash table of group keys to row indices."""
        from collections import defaultdict

        groups = defaultdict(list)
        for row_idx in range(table.num_rows):
            key = self._extract_group_key(table, row_idx)
            groups[key].append(row_idx)
        return groups

    def _compute_result(self, table: pa.Table, groups: dict) -> pa.RecordBatch:
        """Compute aggregate results for all groups."""
        if len(groups) == 0 and len(self.group_by) == 0:
            # SQL: a global aggregate over empty input yields one row.
            groups = {(): []}
        result_rows = []
        for group_key, row_indices in groups.items():
            row = self._compute_group_row(table, group_key, row_indices)
            result_rows.append(row)
        return self._arrays_to_batch(result_rows)

    def _compute_group_row(
        self, table: pa.Table, group_key: tuple, row_indices: List[int]
    ) -> List:
        """Compute one output row for a single group."""
        from .expressions import ColumnRef, FunctionCall

        row_data = []
        for agg_expr in self.aggregates:
            if isinstance(agg_expr, ColumnRef):
                idx = self._find_group_by_index(agg_expr)
                if idx is not None and len(group_key) > 0:
                    row_data.append(group_key[idx])
                else:
                    row_data.append(None)
            elif isinstance(agg_expr, FunctionCall):
                value = self._compute_aggregate(table, agg_expr, row_indices)
                row_data.append(value)
            else:
                row_data.append(None)
        return row_data

    def _compute_aggregate(
        self, table: pa.Table, func: "FunctionCall", row_indices: List[int]
    ):
        """Compute an aggregate function using pyarrow compute helpers."""
        import pyarrow.compute as pc
        from .expressions import ColumnRef

        func_name = func.function_name.upper()

        if func_name == "COUNT" and (
            len(func.args) == 0 or getattr(func.args[0], "column", "*") == "*"
        ):
            return len(row_indices)

        if len(func.args) == 0:
            return len(row_indices)

        if len(row_indices) == 0:
            # No input rows: COUNT(col) is 0, every other aggregate is NULL.
            return 0 if func_name == "COUNT" else None

        column = self._aggregate_input_column(table, func.args[0])
        subset = column.take(row_indices)

        if func_name == "COUNT":
            return len(subset) - subset.null_count
        if func_name == "SUM":
            return pc.sum(subset).as_py()
        if func_name == "AVG":
            return pc.mean(subset).as_py()
        if func_name == "MIN":
            return pc.min(subset).as_py()
        if func_name == "MAX":
            return pc.max(subset).as_py()

        raise ValueError(f"Unsupported aggregate function: {func_name}")

    def _aggregate_input_column(self, table: pa.Table, arg: Expression):
        """Resolve an aggregate's argument to a column, evaluating expressions.

        A non-column argument (``SUM(price * quantity)``) is computed through
        the shared evaluator rather than silently dropped to None.
        """
        from .expressions import ColumnRef
        from ..executor.expression_evaluator import ExpressionEvaluator

        if isinstance(arg, ColumnRef):
            return table.column(arg.column)
        batch = table.combine_chunks().to_batches()[0]
        return ExpressionEvaluator(batch).evaluate(arg)

    def _arrays_to_batch(self, rows: List[List]) -> pa.RecordBatch:
        """Convert list of row data to a RecordBatch with correct names."""
        if len(rows) == 0:
            return self._create_empty_batch()

        num_cols = len(rows[0])
        arrays = []
        for col_idx in range(num_cols):
            col_data = [row[col_idx] for row in rows]
            arrays.append(pa.array(col_data))

        return pa.RecordBatch.from_arrays(arrays, names=self.output_names)

    def schema(self) -> pa.Schema:
        """Get output schema."""
        from .expressions import FunctionCall, ColumnRef

        input_schema = self.input.schema()
        fields = []

        for i, expr in enumerate(self.aggregates):
            name = self.output_names[i]
            field_type = self._infer_output_type(expr, input_schema)
            fields.append(pa.field(name, field_type))

        return pa.schema(fields)

    def _infer_output_type(
        self, expr: Expression, input_schema: pa.Schema
    ) -> pa.DataType:
        """Infer output type for an expression."""
        from .expressions import ColumnRef, FunctionCall

        if isinstance(expr, ColumnRef):
            field_index = input_schema.get_field_index(expr.column)
            return input_schema.field(field_index).type

        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            return self._infer_aggregate_type(expr, input_schema)

        return pa.int64()

    def _infer_aggregate_type(
        self, func: "FunctionCall", input_schema: pa.Schema
    ) -> pa.DataType:
        """Infer output type for aggregate function."""
        from .expressions import ColumnRef

        func_name = func.function_name.upper()
        arg_type = pa.float64()
        if func.args and isinstance(func.args[0], ColumnRef):
            field_index = input_schema.get_field_index(func.args[0].column)
            arg_type = input_schema.field(field_index).type

        if func_name == "COUNT":
            return pa.int64()
        if func_name in ("SUM", "AVG", "MIN", "MAX"):
            # Numeric accumulators compute in float64; the declared type
            # must match what execution actually produces.
            return pa.float64()

        return pa.float64()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashAggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalSort(PhysicalPlanNode):
    """Sort rows."""

    input: PhysicalPlanNode
    sort_keys: List[Expression]
    ascending: List[bool]
    nulls_order: Optional[List[Optional[str]]] = None

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute sort, via the merge engine when one is attached."""
        engine = self.merge_engine()
        if engine is not None and self._can_merge_sort():
            yield from self._execute_merge_sort(engine)
            return
        yield from self._execute_pyarrow_sort()

    def _can_merge_sort(self) -> bool:
        """True when every sort key is a plain column reference."""
        from .expressions import ColumnRef

        for key in self.sort_keys:
            if not isinstance(key, ColumnRef):
                return False
        return True

    def _execute_merge_sort(self, engine) -> Iterator[pa.RecordBatch]:
        """Sort through DuckDB, with per-key NULLS FIRST/LAST placement."""
        reader = _streaming_reader(self.input)
        order = self._sort_order_clause(self.input.column_aliases())
        sql = f"SELECT * FROM {_MERGE_SORT_RELATION} ORDER BY {order}"
        return engine.run(sql, {_MERGE_SORT_RELATION: reader})

    def _sort_order_clause(self, aliases) -> str:
        """Render every ORDER BY key with its direction and NULLS placement."""
        parts = []
        for index, key in enumerate(self.sort_keys):
            parts.append(self._sort_key_sql(key, index, aliases))
        return ", ".join(parts)

    def _sort_key_sql(self, key, index: int, aliases) -> str:
        """Render one sort key: ``"col" ASC|DESC NULLS FIRST|LAST``."""
        name = self._resolve_sort_column(key, aliases)
        direction = "ASC" if self.ascending[index] else "DESC"
        return f'"{name}" {direction} NULLS {self._nulls_keyword(index)}'

    def _nulls_keyword(self, index: int) -> str:
        """NULLS placement: explicit if given, else Postgres default by direction."""
        explicit = None
        if self.nulls_order and index < len(self.nulls_order):
            explicit = self.nulls_order[index]
        if explicit in ("FIRST", "LAST"):
            return explicit
        return "LAST" if self.ascending[index] else "FIRST"

    def _resolve_sort_column(self, key, aliases) -> str:
        """Resolve a sort column to its physical name in the input relation."""
        if aliases:
            resolved = aliases.get((key.table, key.column))
            if resolved is not None:
                return resolved
        return key.column

    def _execute_pyarrow_sort(self) -> Iterator[pa.RecordBatch]:
        """Materialize the input and sort it with pyarrow (fallback path)."""
        batches = []
        for batch in self.input.execute():
            batches.append(batch)

        if not batches:
            return

        table = pa.Table.from_batches(batches)

        sort_keys = []
        for i in range(len(self.sort_keys)):
            col_name = self._extract_column_name(self.sort_keys[i])
            order = "ascending" if self.ascending[i] else "descending"
            sort_keys.append((col_name, order))

        indices = pa.compute.sort_indices(
            table, sort_keys=sort_keys, null_placement=self._null_placement()
        )
        sorted_table = table.take(indices)

        for batch in sorted_table.to_batches():
            yield batch

    def _null_placement(self) -> str:
        """NULL placement for the first key, defaulting to Postgres semantics.

        Postgres orders NULLS LAST for ascending and NULLS FIRST for
        descending unless an explicit ``NULLS FIRST/LAST`` is given.
        """
        nulls = self.nulls_order[0] if self.nulls_order else None
        if nulls == "FIRST":
            return "at_start"
        if nulls == "LAST":
            return "at_end"
        return "at_end" if self.ascending[0] else "at_start"

    def _extract_column_name(self, expr: Expression) -> str:
        """Extract column name from expression."""
        from .expressions import ColumnRef

        if isinstance(expr, ColumnRef):
            return expr.column
        raise NotImplementedError(
            f"ORDER BY on complex expressions not yet supported: {expr}"
        )

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalSort({len(self.sort_keys)} keys)"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalLimit(PhysicalPlanNode):
    """Limit rows. ``limit`` is None for an OFFSET with no row cap."""

    input: PhysicalPlanNode
    limit: Optional[int]
    offset: int = 0

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute limit, skipping the offset then capping at the limit."""
        rows_emitted = 0
        rows_skipped = 0
        for batch in self.input.execute():
            batch, rows_skipped = self._skip_offset(batch, rows_skipped)
            if batch.num_rows == 0 or self._limit_reached(rows_emitted):
                continue
            out = self._take(batch, rows_emitted)
            rows_emitted += out.num_rows
            yield out

    def _skip_offset(self, batch: pa.RecordBatch, rows_skipped: int):
        """Drop leading rows until the offset has been consumed."""
        if rows_skipped >= self.offset:
            return batch, rows_skipped
        skip = min(self.offset - rows_skipped, batch.num_rows)
        return batch.slice(skip), rows_skipped + skip

    def _limit_reached(self, rows_emitted: int) -> bool:
        """True once the row cap (if any) has been met."""
        return self.limit is not None and rows_emitted >= self.limit

    def _take(self, batch: pa.RecordBatch, rows_emitted: int) -> pa.RecordBatch:
        """Slice a batch down to the remaining row allowance."""
        if self.limit is None:
            return batch
        remaining = self.limit - rows_emitted
        if batch.num_rows <= remaining:
            return batch
        return batch.slice(0, remaining)

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        return self.input.estimated_cost()

    def __repr__(self) -> str:
        return f"PhysicalLimit({self.limit})"


class CardinalityViolationError(Exception):
    """Raised when a scalar subquery produces more than one row."""


@dataclass
class PhysicalValues(PhysicalPlanNode):
    """Emits in-memory rows built from constant expressions.

    Used for FROM-less SELECTs such as ``SELECT 42``: each row is a list of
    expressions evaluated without any input columns.
    """

    rows: List[List[Expression]]
    output_names: List[str]

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Evaluate the constant rows into a single batch."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        columns = []
        for column_index in range(len(self.output_names)):
            values = []
            for row in self.rows:
                evaluator = ExpressionEvaluator(_one_row_dummy_batch())
                values.append(evaluator.evaluate(row[column_index])[0].as_py())
            columns.append(pa.array(values))
        yield pa.RecordBatch.from_arrays(columns, names=self.output_names)

    def schema(self) -> pa.Schema:
        """Infer schema by evaluating the first row."""
        from ..executor.expression_evaluator import ExpressionEvaluator

        fields = []
        for column_index in range(len(self.output_names)):
            evaluator = ExpressionEvaluator(_one_row_dummy_batch())
            value = evaluator.evaluate(self.rows[0][column_index])
            fields.append(pa.field(self.output_names[column_index], value.type))
        return pa.schema(fields)

    def estimated_cost(self) -> float:
        return 0.0

    def __repr__(self) -> str:
        return f"PhysicalValues({len(self.rows)} rows)"


def _one_row_dummy_batch() -> pa.RecordBatch:
    """Build a one-row batch so constant expressions broadcast to one row."""
    return pa.RecordBatch.from_pydict({"__dummy": pa.array([0])})


@dataclass
class PhysicalRemoteQuery(PhysicalPlanNode):
    """A pushable single-source subtree rendered as one remote SQL query.

    Carries a fully built sqlglot AST (joins, filters, projection, grouping,
    ordering, and limit all included) that the data source executes in one
    round trip. ``output_names`` records the result column order so an operator
    above a cross-source boundary can resolve the produced columns.
    """

    datasource: str
    datasource_connection: Any
    query_ast: Any
    output_names: List[str]
    column_alias_map: Dict[Tuple[Optional[str], str], str] = field(default_factory=dict)
    _schema: Optional[pa.Schema] = None

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Resolve a qualified reference to this query's unique output name."""
        return self.column_alias_map

    def execute(self) -> Iterator[pa.RecordBatch]:
        self.datasource_connection.ensure_connected()
        for batch in self.datasource_connection.execute_query(self._sql()):
            yield batch

    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._schema = self.datasource_connection.get_query_schema(self._sql())
        return self._schema

    def _sql(self) -> str:
        """Render the carried AST as a Postgres-dialect SQL string."""
        return self.query_ast.sql(dialect="postgres")

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalRemoteQuery({self.datasource})"


@dataclass
class PhysicalUnion(PhysicalPlanNode):
    """Concatenates input streams; optionally removes duplicate rows."""

    inputs: List[PhysicalPlanNode]
    distinct: bool

    def children(self) -> List[PhysicalPlanNode]:
        return self.inputs

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield all input batches, deduplicating rows when distinct."""
        if self.distinct:
            engine = self.merge_engine()
            if engine is not None:
                yield from self._execute_merge_distinct(engine)
            else:
                yield from self._execute_distinct()
            return
        for input_node in self.inputs:
            yield from input_node.execute()

    def _execute_merge_distinct(self, engine) -> Iterator[pa.RecordBatch]:
        """Deduplicate the unioned inputs via DuckDB UNION (vectorized, type-aware).

        Each branch is drained into a table first so its source connection is
        released before the next branch is read (UNION DISTINCT must see every
        row anyway, so DuckDB would buffer them regardless).
        """
        inputs = {}
        selects = []
        for index, node in enumerate(self.inputs):
            name = f"in_union_{index}"
            inputs[name] = _table_from_batches(list(node.execute()), node.schema())
            selects.append(f"SELECT * FROM {name}")
        return engine.run(" UNION ".join(selects), inputs)

    def _execute_distinct(self) -> Iterator[pa.RecordBatch]:
        """Yield input batches with duplicate rows removed."""
        seen_rows: Set[tuple] = set()
        for input_node in self.inputs:
            for batch in input_node.execute():
                deduplicated = self._drop_seen_rows(batch, seen_rows)
                if deduplicated.num_rows > 0:
                    yield deduplicated

    def _drop_seen_rows(
        self, batch: pa.RecordBatch, seen_rows: Set[tuple]
    ) -> pa.RecordBatch:
        """Filter out rows whose full tuple was already emitted."""
        keep_mask = []
        for row_index in range(batch.num_rows):
            row = _batch_row_tuple(batch, row_index)
            if row in seen_rows:
                keep_mask.append(False)
            else:
                seen_rows.add(row)
                keep_mask.append(True)
        return batch.filter(pa.array(keep_mask))

    def schema(self) -> pa.Schema:
        return self.inputs[0].schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        kind = "DISTINCT" if self.distinct else "ALL"
        return f"PhysicalUnion({kind}, {len(self.inputs)} inputs)"


def _batch_row_tuple(batch: pa.RecordBatch, row_index: int) -> tuple:
    """Extract one row of a batch as a hashable Python tuple."""
    values = []
    for column_index in range(batch.num_columns):
        values.append(batch.column(column_index)[row_index].as_py())
    return tuple(values)


_SET_OP_EXP = {
    SetOpKind.UNION: exp.Union,
    SetOpKind.INTERSECT: exp.Intersect,
    SetOpKind.EXCEPT: exp.Except,
}


@dataclass
class PhysicalRemoteSetOp(PhysicalPlanNode):
    """Set operation pushed to a single data source as one remote query.

    Both branches resolve to the same datasource, so the whole
    UNION/INTERSECT/EXCEPT is sent remotely. A trailing ORDER BY / LIMIT
    applies to the combined result and is rendered by wrapping the set
    operation in an outer ``SELECT * FROM (...)``.
    """

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    kind: SetOpKind
    distinct: bool
    datasource: str
    datasource_connection: Any
    order_by_keys: Optional[List[Expression]] = None
    order_by_ascending: Optional[List[bool]] = None
    order_by_nulls: Optional[List[Optional[str]]] = None
    limit: Optional[int] = None
    offset: int = 0
    _schema: Optional[pa.Schema] = None

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def execute(self) -> Iterator[pa.RecordBatch]:
        query = self._build_query()
        self.datasource_connection.ensure_connected()
        for batch in self.datasource_connection.execute_query(query):
            yield batch

    def schema(self) -> pa.Schema:
        if self._schema is None:
            self._schema = self.datasource_connection.get_query_schema(
                self._build_query()
            )
        return self._schema

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        suffix = "" if self.distinct else " ALL"
        return f"PhysicalRemoteSetOp({self.kind.value}{suffix})"

    def _build_query(self) -> str:
        """Render the remote SQL string for this set operation."""
        return self.build_remote_ast().sql(dialect="postgres")

    def build_remote_ast(self) -> exp.Expression:
        """Build the sqlglot AST sent to the data source.

        Set operations are left-associative in sqlglot, so combining the
        branch ASTs directly produces the same nesting as the original query
        without needing parentheses.
        """
        set_op_ast = self._combine(
            self._branch_ast(self.left), self._branch_ast(self.right)
        )
        if not self.order_by_keys and self.limit is None and not self.offset:
            return set_op_ast
        return self._wrap_order_limit(set_op_ast)

    def _branch_ast(self, branch: PhysicalPlanNode) -> exp.Expression:
        """Build the AST for one branch (a scan or a nested set operation)."""
        if isinstance(branch, PhysicalRemoteSetOp):
            return branch.build_remote_ast()
        return self.datasource_connection.parse_query(branch._build_query())

    def _combine(
        self, left_ast: exp.Expression, right_ast: exp.Expression
    ) -> exp.Expression:
        """Wrap two branch ASTs in the matching set-operation node."""
        node_cls = _SET_OP_EXP[self.kind]
        return node_cls(this=left_ast, expression=right_ast, distinct=self.distinct)

    def _wrap_order_limit(self, set_op_ast: exp.Expression) -> exp.Expression:
        """Wrap the set operation in an outer SELECT carrying ORDER BY/LIMIT."""
        inner_sql = set_op_ast.sql(dialect="postgres")
        wrapped = f"SELECT * FROM ({inner_sql}) AS _setop"
        wrapped = self._append_order_by(wrapped)
        wrapped = self._append_limit_offset(wrapped)
        return self.datasource_connection.parse_query(wrapped)

    def _append_order_by(self, query: str) -> str:
        """Append the ORDER BY clause built from the folded sort keys."""
        if not self.order_by_keys:
            return query
        items = []
        for index in range(len(self.order_by_keys)):
            items.append(self._order_item(index))
        return f"{query} ORDER BY {', '.join(items)}"

    def _order_item(self, index: int) -> str:
        """Render one ORDER BY item with direction and NULLS handling."""
        item = self.order_by_keys[index].to_sql()
        item = self._with_direction(item, index)
        return self._with_nulls(item, index)

    def _with_direction(self, item: str, index: int) -> str:
        """Add DESC when the sort direction for this key is descending."""
        if not self.order_by_ascending:
            return item
        if index < len(self.order_by_ascending) and not self.order_by_ascending[index]:
            return f"{item} DESC"
        return item

    def _with_nulls(self, item: str, index: int) -> str:
        """Append NULLS FIRST/LAST when specified for this key."""
        if not self.order_by_nulls:
            return item
        if index >= len(self.order_by_nulls):
            return item
        spec = self.order_by_nulls[index]
        if not spec:
            return item
        return f"{item} NULLS {spec}"

    def _append_limit_offset(self, query: str) -> str:
        """Append LIMIT and/or OFFSET clauses when present."""
        if self.limit is not None:
            query = f"{query} LIMIT {self.limit}"
        if self.offset:
            query = f"{query} OFFSET {self.offset}"
        return query


@dataclass
class PhysicalSetOperation(PhysicalPlanNode):
    """Locally evaluated INTERSECT / EXCEPT over two materialized inputs.

    Used when the branches do not share one data source. Multiset semantics
    follow SQL: the ``distinct`` form deduplicates the result, while the
    ``ALL`` form keeps min/difference multiplicities per distinct row.
    """

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    kind: SetOpKind
    distinct: bool

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield the combined rows honoring INTERSECT/EXCEPT semantics."""
        engine = self.merge_engine()
        if engine is not None:
            yield from self._execute_merge_setop(engine)
            return
        yield from self._execute_row_loop()

    def _execute_merge_setop(self, engine) -> Iterator[pa.RecordBatch]:
        """Run INTERSECT/EXCEPT (distinct or ALL) through DuckDB.

        Both inputs are drained into tables first (releasing their source
        connections); DuckDB applies the multiset semantics natively.
        """
        left_table = _table_from_batches(list(self.left.execute()), self.left.schema())
        right_table = _table_from_batches(
            list(self.right.execute()), self.right.schema()
        )
        sql = (
            f"SELECT * FROM {_MERGE_LEFT_RELATION} {self._setop_keyword()} "
            f"SELECT * FROM {_MERGE_RIGHT_RELATION}"
        )
        return engine.run(
            sql,
            {_MERGE_LEFT_RELATION: left_table, _MERGE_RIGHT_RELATION: right_table},
        )

    def _setop_keyword(self) -> str:
        """DuckDB keyword for this set operation and multiset mode."""
        base = "INTERSECT" if self.kind == SetOpKind.INTERSECT else "EXCEPT"
        return base if self.distinct else f"{base} ALL"

    def _execute_row_loop(self) -> Iterator[pa.RecordBatch]:
        """Row-at-a-time INTERSECT/EXCEPT (fallback when no engine is attached)."""
        right_counts = self._row_counts(self.right)
        emitted: Set[tuple] = set()
        for batch in self.left.execute():
            kept = self._select_rows(batch, right_counts, emitted)
            if kept.num_rows > 0:
                yield kept

    def _select_rows(
        self,
        batch: pa.RecordBatch,
        right_counts: Dict[tuple, int],
        emitted: Set[tuple],
    ) -> pa.RecordBatch:
        """Build the keep-mask for one left batch under the set-op rules."""
        keep_mask = []
        for row_index in range(batch.num_rows):
            row = _batch_row_tuple(batch, row_index)
            keep_mask.append(self._keep_row(row, right_counts, emitted))
        return batch.filter(pa.array(keep_mask))

    def _keep_row(
        self, row: tuple, right_counts: Dict[tuple, int], emitted: Set[tuple]
    ) -> bool:
        """Decide whether one left row belongs in the result."""
        if self.distinct:
            return self._keep_distinct(row, right_counts, emitted)
        return self._keep_all(row, right_counts)

    def _keep_distinct(
        self, row: tuple, right_counts: Dict[tuple, int], emitted: Set[tuple]
    ) -> bool:
        """Distinct INTERSECT/EXCEPT: emit each qualifying row once."""
        if row in emitted:
            return False
        present_on_right = right_counts.get(row, 0) > 0
        qualifies = present_on_right
        if self.kind == SetOpKind.EXCEPT:
            qualifies = not present_on_right
        if not qualifies:
            return False
        emitted.add(row)
        return True

    def _keep_all(self, row: tuple, right_counts: Dict[tuple, int]) -> bool:
        """ALL form: consume right multiplicity per matched row."""
        available = right_counts.get(row, 0)
        if self.kind == SetOpKind.INTERSECT:
            if available > 0:
                right_counts[row] = available - 1
                return True
            return False
        if available > 0:
            right_counts[row] = available - 1
            return False
        return True

    def _row_counts(self, node: PhysicalPlanNode) -> Dict[tuple, int]:
        """Materialize one input as a multiset of row tuples."""
        counts: Dict[tuple, int] = {}
        for batch in node.execute():
            for row_index in range(batch.num_rows):
                row = _batch_row_tuple(batch, row_index)
                counts[row] = counts.get(row, 0) + 1
        return counts

    def schema(self) -> pa.Schema:
        return self.left.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        suffix = "" if self.distinct else " ALL"
        return f"PhysicalSetOperation({self.kind.value}{suffix})"


@dataclass
class PhysicalSingleRowGuard(PhysicalPlanNode):
    """Enforces scalar-subquery cardinality at execution time.

    With no keys, more than one input row in total raises. With keys, a
    repeated key value raises — this guards decorrelated correlated scalar
    subqueries, where each key admits at most one row.
    """

    input: PhysicalPlanNode
    keys: List[Expression]

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Stream input batches, raising on any cardinality violation."""
        seen_keys: Set[tuple] = set()
        total_rows = 0
        for batch in self.input.execute():
            total_rows += batch.num_rows
            if len(self.keys) == 0 and total_rows > 1:
                raise CardinalityViolationError(
                    "Scalar subquery returned more than one row"
                )
            if len(self.keys) > 0:
                self._check_key_uniqueness(batch, seen_keys)
            yield batch

    def _check_key_uniqueness(
        self, batch: pa.RecordBatch, seen_keys: Set[tuple]
    ) -> None:
        """Raise when a non-NULL guard key value appears more than once.

        A key containing NULL can never satisfy SQL equality, so duplicate
        NULL-keyed inner rows can never match an outer row and must not be
        treated as a cardinality violation.
        """
        key_arrays = []
        for key in self.keys:
            key_arrays.append(batch.column(key.column))
        for row_index in range(batch.num_rows):
            self._check_row_key(key_arrays, row_index, seen_keys)

    def _check_row_key(self, key_arrays, row_index: int, seen_keys: Set[tuple]) -> None:
        """Record one row's key and raise on a repeated non-NULL key."""
        key_values = []
        for array in key_arrays:
            key_values.append(array[row_index].as_py())
        key = tuple(key_values)
        if None in key:
            return
        if key in seen_keys:
            raise CardinalityViolationError(
                "Scalar subquery returned more than one row for a "
                f"correlation key {key}"
            )
        seen_keys.add(key)

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalSingleRowGuard(keys={len(self.keys)})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalGroupedLimit(PhysicalPlanNode):
    """Emits at most ``limit`` rows per distinct key tuple.

    This implements a correlated subquery LIMIT after decorrelation: the
    original per-outer-row LIMIT becomes a per-correlation-key limit.
    """

    input: PhysicalPlanNode
    keys: List[Expression]
    limit: int
    # Optional per-key ordering; when set, each key group is sorted by these
    # keys before its first ``limit`` rows are kept.
    order_by_keys: Optional[List[Expression]] = None
    order_by_ascending: Optional[List[bool]] = None
    order_by_nulls: Optional[List[Optional[str]]] = None

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Keep the first ``limit`` rows of each key.

        Without an ordering the input row order decides which rows survive
        (row-at-a-time streaming). With an ordering the input is materialized
        and each key group is sorted first, implementing the first/latest-row-
        per-key idiom without depending on a child operator's order.
        """
        if self.order_by_keys:
            yield from self._execute_ordered()
            return
        emitted_counts: Dict[tuple, int] = {}
        for batch in self.input.execute():
            filtered = self._limit_batch(batch, emitted_counts)
            if filtered.num_rows > 0:
                yield filtered

    def _execute_ordered(self) -> Iterator[pa.RecordBatch]:
        """Materialize, sort within each key, and emit each key's first rows."""
        table = self._sorted_input_table()
        emitted_counts: Dict[tuple, int] = {}
        for batch in table.to_batches():
            filtered = self._limit_batch(batch, emitted_counts)
            if filtered.num_rows > 0:
                yield filtered

    def _sorted_input_table(self) -> pa.Table:
        """Collect the input and sort it by key then by the ordering keys."""
        batches = list(self.input.execute())
        table = pa.Table.from_batches(batches, schema=self.input.schema())
        return table.sort_by(self._sort_directives())

    def _sort_directives(self) -> List[Tuple[str, str]]:
        """Sort by every key (to group rows) then by each ordering key."""
        directives: List[Tuple[str, str]] = []
        for key in self.keys:
            directives.append((key.column, "ascending"))
        for index in range(len(self.order_by_keys)):
            directives.append(
                (self.order_by_keys[index].column, self._direction(index))
            )
        return directives

    def _direction(self, index: int) -> str:
        """The pyarrow sort direction for one ordering key."""
        ascending = self.order_by_ascending
        if ascending is None or ascending[index]:
            return "ascending"
        return "descending"

    def _limit_batch(
        self, batch: pa.RecordBatch, emitted_counts: Dict[tuple, int]
    ) -> pa.RecordBatch:
        """Build the per-key limited slice of one batch."""
        key_arrays = []
        for key in self.keys:
            key_arrays.append(batch.column(key.column))
        keep_mask = []
        for row_index in range(batch.num_rows):
            key_values = []
            for array in key_arrays:
                key_values.append(array[row_index].as_py())
            key = tuple(key_values)
            count = emitted_counts.get(key, 0)
            keep_mask.append(count < self.limit)
            emitted_counts[key] = count + 1
        return batch.filter(pa.array(keep_mask))

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalGroupedLimit(limit={self.limit}, keys={len(self.keys)})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


@dataclass
class PhysicalLateralJoin(PhysicalPlanNode):
    """Cross-source dependent (LATERAL) join, run in the merge engine.

    The left side and the right's base relation are materialized into Arrow and
    registered in the in-memory DuckDB, which decorrelates and runs the LATERAL
    SQL. The base relation is first reduced to the left's correlation *domain*
    (a dynamic filter pushed to the base's source) so only relevant rows cross
    the network; correctness is unaffected because the merge engine re-applies
    the exact correlation.
    """

    left: PhysicalPlanNode
    left_name: str
    left_alias: str
    base_scan: PhysicalPlanNode
    base_name: str
    lateral_sql: str
    output_names: List[str]
    join_type: JoinType
    # (inner column, comparison op, outer column) correlation terms used to
    # derive the dynamic filter pushed into the base relation.
    correlations: List[Tuple[str, Any, str]]

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.base_scan]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Materialize both sides, register them, run the LATERAL in DuckDB."""
        engine = self.merge_engine()
        yield from engine.run(self.lateral_sql, self._inputs())

    def _inputs(self) -> Dict[str, pa.Table]:
        """Build the registered Arrow inputs: the left and the reduced base."""
        left_table = _table_from_batches(list(self.left.execute()), self.left.schema())
        base_table = self._reduced_base(left_table)
        return {self.left_name: left_table, self.base_name: base_table}

    def _reduced_base(self, left_table: pa.Table) -> pa.Table:
        """Execute the base relation, restricted to the left's correlation domain."""
        scan = self._apply_domain_filter(left_table)
        return _table_from_batches(list(scan.execute()), scan.schema())

    def _apply_domain_filter(self, left_table: pa.Table) -> PhysicalPlanNode:
        """Add a sound dynamic filter (domain restriction) to the base scan.

        The filter may be loose (a superset) — the merge engine re-checks the
        exact correlation — so it only shrinks the rows the base source returns.
        """
        from dataclasses import replace

        if not isinstance(self.base_scan, PhysicalScan):
            return self.base_scan
        term = _derive_domain_filter(self.correlations, left_table)
        if term is None:
            return self.base_scan
        combined = term
        if self.base_scan.filters is not None:
            combined = _and_expr(self.base_scan.filters, term)
        return replace(self.base_scan, filters=combined)

    def schema(self) -> pa.Schema:
        """Run an empty LATERAL to learn the (left + value) output schema."""
        engine = self.merge_engine()
        probe = f"SELECT * FROM ({self.lateral_sql}) AS _s LIMIT 0"
        for batch in engine.run(probe, self._inputs()):
            return batch.schema
        return pa.schema([])

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return {}

    def __repr__(self) -> str:
        return f"PhysicalLateralJoin({self.join_type.name}, base={self.base_name})"


def _and_expr(left: Expression, right: Expression) -> Expression:
    """Combine two predicates with AND."""
    from .expressions import BinaryOp, BinaryOpType

    return BinaryOp(op=BinaryOpType.AND, left=left, right=right)


def _derive_domain_filter(correlations, left_table: pa.Table) -> Optional[Expression]:
    """Derive a sound base-relation filter from the left's correlation domain.

    ``=`` becomes ``inner IN (domain)``; ``<``/``<=`` a ``< max(domain)`` bound;
    ``>``/``>=`` a ``> min(domain)`` bound. Any other shape contributes no term
    (the base is fetched in full — still correct, just not reduced).
    """
    from .expressions import BinaryOp, BinaryOpType, InList, ColumnRef

    terms: List[Expression] = []
    for inner_col, op, outer_col in correlations:
        if outer_col not in left_table.schema.names:
            continue
        domain = left_table.column(outer_col).combine_chunks().unique().drop_null()
        values = domain.to_pylist()
        if not values:
            continue
        column = ColumnRef(table=None, column=inner_col)
        term = _domain_term(column, op, values)
        if term is not None:
            terms.append(term)
    if not terms:
        return None
    combined = terms[0]
    for term in terms[1:]:
        combined = _and_expr(combined, term)
    return combined


def _domain_term(column, op, values) -> Optional[Expression]:
    """Build one sound dynamic-filter term for a correlation operator."""
    from .expressions import BinaryOp, BinaryOpType, InList

    if op == BinaryOpType.EQ:
        if not _within_dynamic_filter_cap(len(values)):
            return None
        return InList(value=column, options=[_literal(v) for v in values])
    if op in (BinaryOpType.LT, BinaryOpType.LTE):
        return BinaryOp(op=op, left=column, right=_literal(max(values)))
    if op in (BinaryOpType.GT, BinaryOpType.GTE):
        return BinaryOp(op=op, left=column, right=_literal(min(values)))
    return None


def _literal(value) -> Expression:
    """Wrap a Python domain value as a typed literal."""
    from .expressions import Literal, DataType

    if isinstance(value, bool):
        return Literal(value=value, data_type=DataType.BOOLEAN)
    if isinstance(value, int):
        return Literal(value=value, data_type=DataType.BIGINT)
    if isinstance(value, float):
        return Literal(value=value, data_type=DataType.DOUBLE)
    return Literal(value=value, data_type=DataType.VARCHAR)


@dataclass
class PhysicalExplain(PhysicalPlanNode):
    """Explain a physical plan without executing it."""

    child: PhysicalPlanNode
    format: ExplainFormat = ExplainFormat.TEXT

    def children(self) -> List[PhysicalPlanNode]:
        return [self.child]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Return representation of the plan."""
        if self.format == ExplainFormat.JSON:
            batch = self._build_json_batch()
            yield batch
            return
        batch = self._build_text_batch()
        yield batch

    def build_document(self, stringify_queries: bool = False) -> Dict[str, Any]:
        """Build structured plan + query metadata."""
        formatter = _PlanFormatter()
        plan_lines = formatter.format(self.child)
        entries = self._build_query_entries(stringify_queries)
        document = {
            "plan": plan_lines,
            "queries": entries,
            "datasources": self._group_by_datasource(entries),
        }
        return document

    def _group_by_datasource(self, entries: List[Dict[str, Any]]) -> Dict[str, list]:
        """Group remote query values by the datasource that runs them."""
        grouped: Dict[str, list] = {}
        for entry in entries:
            grouped.setdefault(entry["datasource_name"], []).append(entry["query"])
        return grouped

    def schema(self) -> pa.Schema:
        return pa.schema([pa.field("plan", pa.string())])

    def estimated_cost(self) -> float:
        return -1.0

    def __repr__(self) -> str:
        return f"PhysicalExplain(format={self.format.value})"

    def _build_text_batch(self) -> pa.RecordBatch:
        formatter = _PlanFormatter()
        lines = formatter.format(self.child)
        self._append_query_lines(lines)
        array = pa.array(lines)
        return pa.RecordBatch.from_arrays([array], names=["plan"])

    def _build_json_batch(self) -> pa.RecordBatch:
        document = self.build_document(stringify_queries=True)
        json_text = json.dumps(document, indent=2)
        array = pa.array([json_text])
        return pa.RecordBatch.from_arrays([array], names=["plan"])

    def _append_query_lines(self, lines: List[str]) -> None:
        queries = self._collect_queries()
        if not queries:
            return
        lines.append("")
        lines.append("Queries:")
        for snapshot in queries:
            text = f"- {snapshot.datasource_name}: {snapshot.sql}"
            lines.append(text)

    def _build_query_entries(self, stringify_queries: bool) -> List[Dict[str, Any]]:
        queries = self._collect_queries()
        entries: List[Dict[str, Any]] = []
        for snapshot in queries:
            query_value: Any = snapshot.query_ast
            if stringify_queries:
                if snapshot.query_ast is not None:
                    query_value = snapshot.query_ast.sql(dialect="postgres")
                else:
                    query_value = snapshot.sql
            entry = {
                "datasource_name": snapshot.datasource_name,
                "query": query_value,
            }
            entries.append(entry)
        return entries

    def _collect_queries(self) -> List["_DatasourceQuerySnapshot"]:
        collector = _DatasourceQueryCollector()
        return collector.collect(self.child)


@dataclass
class _DatasourceQuerySnapshot:
    """Captured query metadata for a single datasource call."""

    datasource_name: str
    sql: str
    query_ast: Any


@dataclass
class Gather(PhysicalPlanNode):
    """Gather data from a remote data source.

    This operator fetches data from a remote source and materializes it locally.
    It's used when we need to bring data to the coordinator for local processing.
    """

    datasource: str
    query: str  # SQL query to execute on remote source
    datasource_connection: Any = None

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute query on remote source and stream results."""
        raise NotImplementedError("Execute not yet implemented")

    def schema(self) -> pa.Schema:
        raise NotImplementedError("Schema resolution not yet implemented")

    def estimated_cost(self) -> float:
        # Includes network transfer cost
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"Gather(from={self.datasource})"


class _PlanFormatter:
    """Utility to format physical plans as text."""

    def __init__(self):
        self._detail_builders: Dict[type, Callable[[PhysicalPlanNode], str]] = {}
        self._detail_builders[PhysicalScan] = self._scan_detail
        self._detail_builders[PhysicalProjection] = self._projection_detail
        self._detail_builders[PhysicalFilter] = self._filter_detail
        self._detail_builders[PhysicalLimit] = self._limit_detail
        self._detail_builders[PhysicalHashJoin] = self._hash_join_detail
        self._detail_builders[PhysicalRemoteJoin] = self._remote_join_detail
        self._detail_builders[PhysicalNestedLoopJoin] = self._nested_loop_join_detail
        self._detail_builders[PhysicalHashAggregate] = self._aggregate_detail
        self._detail_builders[PhysicalSort] = self._sort_detail

    def format(self, node: PhysicalPlanNode) -> List[str]:
        lines: List[str] = []
        self._append_node_line(node, 0, lines)
        return lines

    def _append_node_line(
        self, node: PhysicalPlanNode, depth: int, lines: List[str]
    ) -> None:
        indent = self._build_indent(depth)
        header = self._build_header(node)
        lines.append(f"{indent}{header}")
        children = node.children()
        for child in children:
            self._append_node_line(child, depth + 1, lines)

    def _build_indent(self, depth: int) -> str:
        if depth == 0:
            return ""
        return f"{'  ' * depth}-> "

    def _build_header(self, node: PhysicalPlanNode) -> str:
        name = node.__class__.__name__
        cost = self._safe_cost(node)
        detail = self._detail_for(node)
        if detail:
            return f"{name} cost={cost} rows=-1 details={detail}"
        return f"{name} cost={cost} rows=-1"

    def _detail_for(self, node: PhysicalPlanNode) -> str:
        builder = self._detail_builders.get(type(node))
        if builder is None:
            return ""
        return builder(node)

    def _safe_cost(self, node: PhysicalPlanNode) -> float:
        try:
            return node.estimated_cost()
        except NotImplementedError:
            return -1.0

    def _scan_detail(self, node: PhysicalScan) -> str:
        table = f"{node.datasource}.{node.schema_name}.{node.table_name}"
        columns = self._format_column_list(node.columns)
        detail = f"table={table}"
        if columns:
            detail = f"{detail} columns={columns}"
        if node.filters:
            detail = f"{detail} filter={node.filters}"
        return detail

    def _projection_detail(self, node: PhysicalProjection) -> str:
        names = self._format_column_list(node.output_names)
        if names:
            return f"columns={names}"
        return ""

    def _filter_detail(self, node: PhysicalFilter) -> str:
        return f"predicate={node.predicate}"

    def _limit_detail(self, node: PhysicalLimit) -> str:
        return f"limit={node.limit} offset={node.offset}"

    def _hash_join_detail(self, node: PhysicalHashJoin) -> str:
        left_keys = self._format_expression_list(node.left_keys)
        right_keys = self._format_expression_list(node.right_keys)
        info = f"type={node.join_type.value}"
        if left_keys:
            info = f"{info} left_keys={left_keys}"
        if right_keys:
            info = f"{info} right_keys={right_keys}"
        return f"{info} build_side={node.build_side}"

    def _remote_join_detail(self, node: PhysicalRemoteJoin) -> str:
        left = f"{node.left.schema_name}.{node.left.table_name}"
        right = f"{node.right.schema_name}.{node.right.table_name}"
        return f"type={node.join_type.value} sources={left}⨝{right}"

    def _nested_loop_join_detail(self, node: PhysicalNestedLoopJoin) -> str:
        info = f"type={node.join_type.value}"
        if node.condition:
            return f"{info} condition={node.condition}"
        return info

    def _aggregate_detail(self, node: PhysicalHashAggregate) -> str:
        groups = self._format_expression_list(node.group_by)
        aggs = self._format_expression_list(node.aggregates)
        info = ""
        if groups:
            info = f"group_by={groups}"
        if aggs:
            if info:
                info = f"{info} aggregates={aggs}"
            else:
                info = f"aggregates={aggs}"
        return info

    def _sort_detail(self, node: PhysicalSort) -> str:
        keys = self._format_expression_list(node.sort_keys)
        return f"keys={keys}"

    def _format_expression_list(self, expressions: List[Expression]) -> str:
        parts: List[str] = []
        for expr in expressions:
            parts.append(str(expr))
        return ",".join(parts)

    def _format_column_list(self, columns: List[str]) -> str:
        if not columns:
            return ""
        buffer: List[str] = []
        for column in columns:
            buffer.append(column)
        return ",".join(buffer)


class _DatasourceQueryCollector:
    """Collect queries issued by physical plan nodes."""

    def collect(self, node: PhysicalPlanNode) -> List[_DatasourceQuerySnapshot]:
        snapshots: List[_DatasourceQuerySnapshot] = []
        self._visit(node, snapshots)
        return snapshots

    def _visit(
        self, node: PhysicalPlanNode, snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        self._record_node(node, snapshots)
        children = node.children()
        for child in children:
            self._visit(child, snapshots)

    def _record_node(
        self, node: PhysicalPlanNode, snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if isinstance(node, PhysicalScan):
            self._record_scan(node, snapshots)
        elif isinstance(node, PhysicalRemoteJoin):
            self._record_remote_join(node, snapshots)
        elif isinstance(node, PhysicalRemoteSetOp):
            self._record_remote_set_op(node, snapshots)
        elif isinstance(node, PhysicalRemoteQuery):
            self._record_remote_query(node, snapshots)
        elif isinstance(node, PhysicalHashJoin):
            self._prefetch_dynamic_filter(node)

    def _prefetch_dynamic_filter(self, hash_join: "PhysicalHashJoin") -> None:
        """Fetch the build side's distinct keys so EXPLAIN shows real values.

        The build side is executed and read until a few distinct keys are
        gathered, which are stashed on the marked probe scan. Visited before the
        probe child, so the probe's query renders with the values.
        """
        if hash_join.build_side == "right":
            probe, build_node, build_keys = (
                hash_join.left,
                hash_join.right,
                hash_join.right_keys,
            )
        else:
            probe, build_node, build_keys = (
                hash_join.right,
                hash_join.left,
                hash_join.left_keys,
            )
        if not isinstance(probe, PhysicalScan) or not probe.dynamic_filter_keys:
            return
        probe.dynamic_filter_values = self._collect_build_values(
            build_node, build_keys, build_node.column_aliases()
        )

    def _collect_build_values(
        self,
        build_node: PhysicalPlanNode,
        build_keys: List[Expression],
        aliases,
    ) -> List[tuple]:
        """Read distinct, NULL-free build-key tuples, stopping once past five."""
        distinct: List[tuple] = []
        seen: Set[tuple] = set()
        for batch in build_node.execute():
            key_arrays = self._build_key_arrays(batch, build_keys, aliases)
            for row_index in range(batch.num_rows):
                self._add_distinct_key(key_arrays, row_index, seen, distinct)
                if len(distinct) > 5:
                    return distinct
        return distinct

    def _build_key_arrays(self, batch, build_keys, aliases):
        """Collect the build-key column arrays, resolving qualified names."""
        from .expressions import ColumnRef

        arrays = []
        for key in build_keys:
            if not isinstance(key, ColumnRef):
                return []
            name = key.column
            if aliases:
                name = aliases.get((key.table, key.column), key.column)
            arrays.append(batch.column(name))
        return arrays

    def _add_distinct_key(self, key_arrays, row_index, seen, distinct) -> None:
        """Append a row's key tuple to ``distinct`` if new and free of NULLs."""
        if not key_arrays:
            return
        key = _row_key_tuple(key_arrays, row_index)
        if None in key or key in seen:
            return
        seen.add(key)
        distinct.append(key)

    def _record_scan(
        self, scan: PhysicalScan, snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        datasource = scan.datasource_connection
        if datasource is None:
            return
        base_sql = scan._build_query()
        query_ast = datasource.parse_query(base_sql)
        sql = self._annotate_dynamic_filter(scan, base_sql)
        snapshot = _DatasourceQuerySnapshot(scan.datasource, sql, query_ast)
        snapshots.append(snapshot)

    def _annotate_dynamic_filter(self, scan: PhysicalScan, sql: str) -> str:
        """Render the runtime IN filter as a real predicate in the EXPLAIN SQL.

        The build-side values are only known at execution time, so the value
        list shows as ``(...)``. Only single-column keys are marked, and the
        probe scan is a plain ``SELECT ... FROM ... [WHERE ...]``, so the
        predicate appends cleanly to the WHERE clause.
        """
        keys = scan.dynamic_filter_keys
        if not keys:
            return sql
        predicate = f"{keys[0].to_sql()} IN ({self._render_in_values(scan)})"
        if " WHERE " in sql:
            return f"{sql} AND {predicate}"
        return f"{sql} WHERE {predicate}"

    def _render_in_values(self, scan: PhysicalScan) -> str:
        """Render up to five real build values, then ``...`` if more exist.

        Falls back to ``...`` when the values were not fetched (no EXPLAIN
        prefetch) so the predicate stays readable.
        """
        values = scan.dynamic_filter_values
        if not values:
            return "..."
        items = []
        for value_tuple in values[:5]:
            items.append(self._render_value(value_tuple[0]))
        rendered = ", ".join(items)
        if len(values) > 5:
            return f"{rendered}, ..."
        return rendered

    def _render_value(self, value) -> str:
        """Render a single value as a SQL literal (quoted/escaped as needed)."""
        literal = _literal_for_value(value)
        if literal is None:
            return str(value)
        return literal.to_sql()

    def _record_remote_join(
        self, join: PhysicalRemoteJoin, snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        datasource = join.datasource_connection
        if datasource is None:
            return
        sql = join._build_query()
        query_ast = datasource.parse_query(sql)
        snapshot = _DatasourceQuerySnapshot(join.left.datasource, sql, query_ast)
        snapshots.append(snapshot)

    def _record_remote_set_op(
        self, set_op: "PhysicalRemoteSetOp", snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if set_op.datasource_connection is None:
            return
        query_ast = set_op.build_remote_ast()
        sql = query_ast.sql(dialect="postgres")
        snapshot = _DatasourceQuerySnapshot(set_op.datasource, sql, query_ast)
        snapshots.append(snapshot)

    def _record_remote_query(
        self, node: "PhysicalRemoteQuery", snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if node.datasource_connection is None:
            return
        sql = node.query_ast.sql(dialect="postgres")
        snapshot = _DatasourceQuerySnapshot(node.datasource, sql, node.query_ast)
        snapshots.append(snapshot)
