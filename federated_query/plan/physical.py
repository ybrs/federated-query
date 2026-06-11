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

if TYPE_CHECKING:
    from .expressions import FunctionCall


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


def _row_key_tuple(key_values, row_index: int) -> tuple:
    """Build a hashable key tuple from per-column arrays at one row index."""
    values = []
    for array in key_values:
        values.append(array[row_index].as_py())
    return tuple(values)


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

    def children(self) -> List[PhysicalPlanNode]:
        return []

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
        """Execute hash join."""
        from collections import defaultdict
        from .expressions import ColumnRef

        if self.join_type in (JoinType.SEMI, JoinType.ANTI):
            for batch in self._execute_semi_anti():
                yield batch
            return

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

        for batch in build_node.execute():
            build_batches.append(batch)
            key_values = self._extract_key_values(batch, build_keys)

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

        for probe_batch in probe_node.execute():
            probe_key_values = self._extract_key_values(probe_batch, probe_keys)

            for probe_row_idx in range(probe_batch.num_rows):
                probe_key = _row_key_tuple(probe_key_values, probe_row_idx)

                # A NULL key component can never satisfy SQL equality.
                if None not in probe_key and probe_key in hash_table:
                    build_rows = hash_table[probe_key]
                    for build_batch_idx, build_row_idx in build_rows:
                        build_batch = build_batches[build_batch_idx]
                        matched_build_rows.add((build_batch_idx, build_row_idx))
                        joined = self._join_rows(
                            probe_batch, probe_row_idx, build_batch, build_row_idx
                        )
                        yield joined
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

    def _extract_key_values(
        self, batch: pa.RecordBatch, keys: List[Expression]
    ) -> List[pa.Array]:
        """Extract key column values from batch."""
        from .expressions import ColumnRef

        key_arrays = []
        for key in keys:
            if isinstance(key, ColumnRef):
                key_arrays.append(batch.column(key.column))
            else:
                raise NotImplementedError(
                    f"Key extraction not implemented for {type(key)}"
                )

        return key_arrays

    def _execute_semi_anti(self) -> Iterator[pa.RecordBatch]:
        """Execute SEMI or ANTI hash join."""
        from collections import defaultdict

        hash_table = defaultdict(bool)
        for batch in self.right.execute():
            key_values = self._extract_key_values(batch, self.right_keys)
            for row_idx in range(batch.num_rows):
                key = _row_key_tuple(key_values, row_idx)
                # SQL equality never matches NULL keys.
                if None not in key:
                    hash_table[key] = True

        for left_batch in self.left.execute():
            left_keys = self._extract_key_values(left_batch, self.left_keys)
            for left_idx in range(left_batch.num_rows):
                key = _row_key_tuple(left_keys, left_idx)
                match = None not in key and key in hash_table
                if self.join_type == JoinType.SEMI and match:
                    yield self._left_row_batch(left_batch, left_idx)
                if self.join_type == JoinType.ANTI and not match:
                    yield self._left_row_batch(left_batch, left_idx)

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
    """Join executed directly on a single data source."""

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
        """Execute nested loop join."""
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
        """Execute hash aggregation."""
        if self._supports_streaming():
            result_batch = self._execute_streaming()
        else:
            result_batch = self._execute_materialized()

        if result_batch.num_rows > 0:
            yield result_batch

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
        """Execute sort by materializing input and sorting."""
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
class PhysicalUnion(PhysicalPlanNode):
    """Concatenates input streams; optionally removes duplicate rows."""

    inputs: List[PhysicalPlanNode]
    distinct: bool

    def children(self) -> List[PhysicalPlanNode]:
        return self.inputs

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield all input batches, deduplicating rows when distinct."""
        if self.distinct:
            yield from self._execute_distinct()
            return
        for input_node in self.inputs:
            yield from input_node.execute()

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

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Stream input, keeping the first ``limit`` rows of each key."""
        emitted_counts: Dict[tuple, int] = {}
        for batch in self.input.execute():
            filtered = self._limit_batch(batch, emitted_counts)
            if filtered.num_rows > 0:
                yield filtered

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

    def _record_scan(
        self, scan: PhysicalScan, snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        datasource = scan.datasource_connection
        if datasource is None:
            return
        sql = scan._build_query()
        query_ast = datasource.parse_query(sql)
        snapshot = _DatasourceQuerySnapshot(scan.datasource, sql, query_ast)
        snapshots.append(snapshot)

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
