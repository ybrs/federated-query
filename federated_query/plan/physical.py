"""Physical plan nodes."""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Iterator, Any, TYPE_CHECKING, Dict, Callable, Set, Tuple
from enum import Enum

import pyarrow as pa
from sqlglot import exp

from .expressions import Expression
from .logical import JoinType, ExplainFormat

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
    output_names: Optional[List[str]] = None  # Output column names when using aggregates
    alias: Optional[str] = None  # Table alias (if any)
    limit: Optional[int] = None
    offset: int = 0
    order_by_keys: Optional[List[Expression]] = None  # ORDER BY expressions
    order_by_ascending: Optional[List[bool]] = None  # ASC/DESC for each key
    order_by_nulls: Optional[List[Optional[str]]] = None  # NULLS FIRST/LAST for each key
    distinct: bool = False

    def children(self) -> List[PhysicalPlanNode]:
        return []

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
        cols = self._format_columns()
        table_ref = self._format_table_ref()
        select_kw = "SELECT"
        if self.distinct:
            select_kw = "SELECT DISTINCT"
        query = f"{select_kw} {cols} FROM {table_ref}"

        use_having = False
        if self.filters:
            use_having = (self.group_by or self.aggregates) and self._predicate_uses_aggregates(
                self.filters
            )
            if not use_having:
                where_clause = self.filters.to_sql()
                query = f"{query} WHERE {where_clause}"

        if self.group_by:
            group_clause = self._format_group_by()
            query = f"{query} GROUP BY {group_clause}"

        if self.filters and use_having:
            clause_sql = self.filters.to_sql()
            query = f"{query} HAVING {clause_sql}"

        if self.order_by_keys:
            order_clause = self._format_order_by()
            query = f"{query} ORDER BY {order_clause}"

        if self.limit is not None:
            query = f"{query} LIMIT {self.limit}"
            if self.offset:
                query = f"{query} OFFSET {self.offset}"

        return query

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
            return f'{ref} AS {self.alias}'
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
        from .expressions import FunctionCall, BinaryOp, UnaryOp, InList, BetweenExpression, ColumnRef

        if isinstance(expr, FunctionCall):
            if expr.is_aggregate:
                return True
            for arg in expr.args:
                if self._predicate_uses_aggregates(arg):
                    return True
            return False

        if isinstance(expr, BinaryOp):
            return self._predicate_uses_aggregates(expr.left) or self._predicate_uses_aggregates(expr.right)

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
class PhysicalProject(PhysicalPlanNode):
    """Project expressions."""

    input: PhysicalPlanNode
    expressions: List[Expression]
    output_names: List[str]
    distinct: bool = False

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute projection."""
        for batch in self.input.execute():
            projected = self._project_batch(batch)
            yield projected

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
        raise NotImplementedError(f"Expression evaluation not yet implemented for {type(expr)}")

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
                raise NotImplementedError(f"Schema inference not implemented for {type(expr)}")

        return pa.schema(fields)

    def estimated_cost(self) -> float:
        # Cost is input cost + projection cost
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalProject({len(self.expressions)} exprs)"

    def _resolve_column(
        self,
        expr: Expression,
        batch: pa.RecordBatch,
        alias_map: Dict[Tuple[Optional[str], str], str]
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
        self,
        expr: Expression,
        alias_map: Dict[Tuple[Optional[str], str], str]
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
        import pyarrow.compute as pc
        from .expressions import BinaryOp, BinaryOpType, ColumnRef, Literal

        if isinstance(self.predicate, BinaryOp):
            return self._evaluate_binary_op(self.predicate, batch)

        raise NotImplementedError(f"Filter evaluation not implemented for {type(self.predicate)}")

    def _evaluate_binary_op(self, op: "BinaryOp", batch: pa.RecordBatch) -> pa.Array:
        """Evaluate binary operation."""
        import pyarrow.compute as pc
        from .expressions import BinaryOpType, ColumnRef, Literal

        left_val = self._evaluate_value(op.left, batch)
        right_val = self._evaluate_value(op.right, batch)

        op_map = {
            BinaryOpType.EQ: pc.equal,
            BinaryOpType.NEQ: pc.not_equal,
            BinaryOpType.LT: pc.less,
            BinaryOpType.LTE: pc.less_equal,
            BinaryOpType.GT: pc.greater,
            BinaryOpType.GTE: pc.greater_equal,
            BinaryOpType.AND: pc.and_,
            BinaryOpType.OR: pc.or_,
        }

        compute_func = op_map.get(op.op)
        if compute_func is None:
            raise NotImplementedError(f"Operator {op.op} not supported in filter")

        return compute_func(left_val, right_val)

    def _evaluate_value(self, expr: Expression, batch: pa.RecordBatch):
        """Evaluate an expression to a value."""
        from .expressions import ColumnRef, Literal, BinaryOp

        if isinstance(expr, ColumnRef):
            return batch.column(expr.column)
        if isinstance(expr, Literal):
            return pa.scalar(expr.value)
        if isinstance(expr, BinaryOp):
            return self._evaluate_binary_op(expr, batch)

        raise NotImplementedError(f"Value evaluation not implemented for {type(expr)}")

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
                key = tuple(val[row_idx].as_py() for val in key_values)
                hash_table[key].append((len(build_batches) - 1, row_idx))

        if len(hash_table) == 0:
            need_probe_outer = (
                (self.join_type == JoinType.LEFT and probe_is_left) or
                (self.join_type == JoinType.RIGHT and not probe_is_left) or
                (self.join_type == JoinType.FULL)
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
                probe_key = tuple(val[probe_row_idx].as_py() for val in probe_key_values)

                if probe_key in hash_table:
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
                        (self.join_type == JoinType.LEFT and probe_is_left) or
                        (self.join_type == JoinType.RIGHT and not probe_is_left) or
                        (self.join_type == JoinType.FULL)
                    )
                    if need_probe_outer:
                        if probe_is_left:
                            yield self._create_left_outer_row(probe_batch, probe_row_idx)
                        else:
                            yield self._create_right_outer_row(probe_batch, probe_row_idx)

        need_build_outer = (
            (self.join_type == JoinType.RIGHT and probe_is_left) or
            (self.join_type == JoinType.LEFT and not probe_is_left) or
            (self.join_type == JoinType.FULL)
        )
        if need_build_outer:
            for build_batch_idx, build_batch in enumerate(build_batches):
                for build_row_idx in range(build_batch.num_rows):
                    if (build_batch_idx, build_row_idx) not in matched_build_rows:
                        if probe_is_left:
                            yield self._create_right_outer_row(build_batch, build_row_idx)
                        else:
                            yield self._create_left_outer_row_from_batch(build_batch, build_row_idx)

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
                raise NotImplementedError(f"Key extraction not implemented for {type(key)}")

        return key_arrays

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
    _column_alias_map: Dict[Tuple[Optional[str], str], str] = field(default_factory=dict, init=False)

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
        self,
        scan: PhysicalScan,
        items: List[str],
        seen: Set[str],
        is_right: bool
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

    def _select_output_name(
        self,
        column: str,
        seen: Set[str],
        is_right: bool
    ) -> str:
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
        """Build FROM source for a scan, applying side-local filters."""
        table_ref = f'"{scan.schema_name}"."{scan.table_name}"'
        source_sql = table_ref
        if scan.filters:
            filter_sql = scan.filters.to_sql()
            source_sql = f"(SELECT * FROM {table_ref} WHERE {filter_sql})"
        alias = scan.alias if scan.alias else scan.table_name
        if alias:
            return f"{source_sql} AS {alias}"
        return source_sql

    def _register_column_alias(
        self,
        scan: PhysicalScan,
        column: str,
        output: str
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
        from .expressions import BinaryOp, BinaryOpType, ColumnRef, Literal
        import pyarrow.compute as pc

        if isinstance(expr, ColumnRef):
            return batch.column(expr.column)
        if isinstance(expr, Literal):
            return pa.array([expr.value])
        if isinstance(expr, BinaryOp):
            left_val = self._evaluate_expression_on_batch(expr.left, batch)
            right_val = self._evaluate_expression_on_batch(expr.right, batch)

            op_map = {
                BinaryOpType.EQ: pc.equal,
                BinaryOpType.NEQ: pc.not_equal,
                BinaryOpType.LT: pc.less,
                BinaryOpType.LTE: pc.less_equal,
                BinaryOpType.GT: pc.greater,
                BinaryOpType.GTE: pc.greater_equal,
                BinaryOpType.AND: pc.and_,
                BinaryOpType.OR: pc.or_,
            }

            compute_func = op_map.get(expr.op)
            if compute_func:
                return compute_func(left_val, right_val)

        raise NotImplementedError(f"Expression evaluation not implemented for {type(expr)}")

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

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalNestedLoopJoin({self.join_type.value})"


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
            return {"type": func_name, "count": 0, "sum": 0, "min": None, "max": None}

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

    def _extract_value_from_row(self, batch: pa.RecordBatch, row_idx: int, expr: Expression):
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

        return None

    def _update_accumulator(self, acc: dict, value):
        """Update accumulator with a value."""
        acc_type = acc["type"]

        if acc_type == "VALUE":
            acc["value"] = value
            return

        if acc_type == "COUNT":
            acc["count"] += 1
            return

        if value is None:
            return

        self._update_numeric_accumulator(acc, value, acc_type)

    def _update_numeric_accumulator(self, acc: dict, value, acc_type: str):
        """Update numeric accumulator (SUM, AVG, MIN, MAX)."""
        acc["count"] += 1
        numeric_val = float(value) if value is not None else 0

        if acc_type in ("SUM", "AVG"):
            acc["sum"] += numeric_val

        if acc_type in ("MIN", "MAX"):
            self._update_min_max(acc, numeric_val, acc_type)

    def _update_min_max(self, acc: dict, value: float, acc_type: str):
        """Update min/max accumulator."""
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
                if isinstance(group_expr, ColumnRef) and group_expr.column == expr.column:
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
            return acc["sum"]
        if acc_type == "AVG":
            return acc["sum"] / acc["count"] if acc["count"] > 0 else None
        if acc_type == "MIN":
            return acc["min"]
        if acc_type == "MAX":
            return acc["max"]

        return None

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
        result_rows = []
        for group_key, row_indices in groups.items():
            row = self._compute_group_row(table, group_key, row_indices)
            result_rows.append(row)
        return self._arrays_to_batch(result_rows)

    def _compute_group_row(self, table: pa.Table, group_key: tuple, row_indices: List[int]) -> List:
        """Compute one output row for a single group."""
        from .expressions import ColumnRef, FunctionCall

        row_data = []
        for agg_expr in self.aggregates:
            if isinstance(agg_expr, ColumnRef):
                if len(group_key) > 0:
                    idx = self._find_group_by_index(agg_expr)
                    row_data.append(group_key[idx])
                else:
                    row_data.append(None)
            elif isinstance(agg_expr, FunctionCall):
                value = self._compute_aggregate(table, agg_expr, row_indices)
                row_data.append(value)
            else:
                row_data.append(None)
        return row_data

    def _find_group_by_index(self, col_ref: "ColumnRef") -> int:
        """Find the offset of a ColumnRef inside group_by expressions."""
        from .expressions import ColumnRef

        for idx, group_expr in enumerate(self.group_by):
            if isinstance(group_expr, ColumnRef) and group_expr.column == col_ref.column:
                return idx
        return 0

    def _compute_aggregate(self, table: pa.Table, func: "FunctionCall", row_indices: List[int]):
        """Compute an aggregate function using pyarrow compute helpers."""
        import pyarrow.compute as pc
        from .expressions import ColumnRef

        func_name = func.function_name.upper()

        if func_name == "COUNT" and (len(func.args) == 0 or getattr(func.args[0], "column", "*") == "*"):
            return len(row_indices)

        if len(func.args) == 0:
            return len(row_indices)

        arg = func.args[0]
        if not isinstance(arg, ColumnRef):
            return None

        col_name = arg.column
        column = table.column(col_name)
        subset = column.take(row_indices)

        if func_name == "SUM":
            return pc.sum(subset).as_py()
        if func_name == "AVG":
            return pc.mean(subset).as_py()
        if func_name == "MIN":
            return pc.min(subset).as_py()
        if func_name == "MAX":
            return pc.max(subset).as_py()

        return None

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

    def _infer_output_type(self, expr: Expression, input_schema: pa.Schema) -> pa.DataType:
        """Infer output type for an expression."""
        from .expressions import ColumnRef, FunctionCall

        if isinstance(expr, ColumnRef):
            field_index = input_schema.get_field_index(expr.column)
            return input_schema.field(field_index).type

        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            return self._infer_aggregate_type(expr, input_schema)

        return pa.int64()

    def _infer_aggregate_type(self, func: "FunctionCall", input_schema: pa.Schema) -> pa.DataType:
        """Infer output type for aggregate function."""
        from .expressions import ColumnRef

        func_name = func.function_name.upper()
        arg_type = pa.float64()
        if func.args and isinstance(func.args[0], ColumnRef):
            field_index = input_schema.get_field_index(func.args[0].column)
            arg_type = input_schema.field(field_index).type

        if func_name == "COUNT":
            return pa.int64()
        if func_name == "SUM":
            return arg_type if pa.types.is_integer(arg_type) else pa.float64()
        if func_name == "AVG":
            return pa.float64()
        if func_name in ("MIN", "MAX"):
            return arg_type

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

        indices = pa.compute.sort_indices(table, sort_keys=sort_keys)
        sorted_table = table.take(indices)

        for batch in sorted_table.to_batches():
            yield batch

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
    """Limit rows."""

    input: PhysicalPlanNode
    limit: int
    offset: int = 0

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute limit."""
        rows_emitted = 0
        rows_skipped = 0

        for batch in self.input.execute():
            batch_size = batch.num_rows

            if rows_skipped < self.offset:
                skip_count = min(self.offset - rows_skipped, batch_size)
                rows_skipped += skip_count
                if rows_skipped < self.offset:
                    continue
                batch = batch.slice(skip_count)

            if rows_emitted >= self.limit:
                break

            remaining = self.limit - rows_emitted
            if batch.num_rows <= remaining:
                yield batch
                rows_emitted += batch.num_rows
            else:
                yield batch.slice(0, remaining)
                rows_emitted += remaining
                break

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        return self.input.estimated_cost()

    def __repr__(self) -> str:
        return f"PhysicalLimit({self.limit})"

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
        document = {"plan": plan_lines, "queries": entries}
        return document

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
        self._detail_builders[PhysicalProject] = self._project_detail
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

    def _append_node_line(self, node: PhysicalPlanNode, depth: int, lines: List[str]) -> None:
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

    def _project_detail(self, node: PhysicalProject) -> str:
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
        self,
        node: PhysicalPlanNode,
        snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        self._record_node(node, snapshots)
        children = node.children()
        for child in children:
            self._visit(child, snapshots)

    def _record_node(
        self,
        node: PhysicalPlanNode,
        snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if isinstance(node, PhysicalScan):
            self._record_scan(node, snapshots)
        elif isinstance(node, PhysicalRemoteJoin):
            self._record_remote_join(node, snapshots)

    def _record_scan(
        self,
        scan: PhysicalScan,
        snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        datasource = scan.datasource_connection
        if datasource is None:
            return
        sql = scan._build_query()
        query_ast = datasource.parse_query(sql)
        self._normalize_count_distinct(query_ast)
        snapshot = _DatasourceQuerySnapshot(scan.datasource, sql, query_ast)
        snapshots.append(snapshot)

    def _record_remote_join(
        self,
        join: PhysicalRemoteJoin,
        snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        datasource = join.datasource_connection
        if datasource is None:
            return
        sql = join._build_query()
        query_ast = datasource.parse_query(sql)
        self._normalize_count_distinct(query_ast)
        snapshot = _DatasourceQuerySnapshot(join.left.datasource, sql, query_ast)
        snapshots.append(snapshot)

    def _normalize_count_distinct(self, node: exp.Expression) -> None:
        """Mark COUNT(DISTINCT ...) nodes with distinct flag for assertions."""
        if isinstance(node, exp.Count):
            if isinstance(node.this, exp.Distinct):
                values = node.this.expressions or []
                if values:
                    node.set("this", values[0])
                node.set("distinct", True)
        for key, value in list(node.args.items()):
            if isinstance(value, exp.Expression):
                self._normalize_count_distinct(value)
                continue
            if isinstance(value, list):
                for child in value:
                    if isinstance(child, exp.Expression):
                        self._normalize_count_distinct(child)
