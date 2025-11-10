"""Physical plan nodes."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Iterator, Any
from enum import Enum

import pyarrow as pa

from .expressions import Expression
from .logical import JoinType


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


@dataclass
class PhysicalScan(PhysicalPlanNode):
    """Scan a table from a data source."""

    datasource: str
    schema_name: str
    table_name: str
    columns: List[str]
    filters: Optional[Expression] = None
    datasource_connection: Any = None  # Set during planning
    _schema: Optional[pa.Schema] = None  # Cached schema

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
        query = f"SELECT {cols} FROM {table_ref}"

        if self.filters:
            where_clause = self.filters.to_sql()
            query = f"{query} WHERE {where_clause}"

        return query

    def _format_columns(self) -> str:
        """Format column list for SQL."""
        if "*" in self.columns:
            return "*"

        quoted_cols = []
        for col in self.columns:
            quoted_cols.append(f'"{col}"')

        return ", ".join(quoted_cols)

    def _format_table_ref(self) -> str:
        """Format table reference for SQL."""
        return f'"{self.schema_name}"."{self.table_name}"'

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
        return f"PhysicalScan({table_ref})"


@dataclass
class PhysicalProject(PhysicalPlanNode):
    """Project expressions."""

    input: PhysicalPlanNode
    expressions: List[Expression]
    output_names: List[str]

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
        for expr in self.expressions:
            if isinstance(expr, ColumnRef):
                if expr.column == "*":
                    return batch
                column_data = batch.column(expr.column)
                columns.append(column_data)
            else:
                evaluated = self._evaluate_expression(expr, batch)
                columns.append(evaluated)

        return pa.RecordBatch.from_arrays(columns, names=self.output_names)

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
                    return input_schema

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
        else:
            build_node = self.left
            probe_node = self.right
            build_keys = self.left_keys
            probe_keys = self.right_keys

        hash_table = defaultdict(list)
        build_batches = []

        for batch in build_node.execute():
            build_batches.append(batch)
            key_values = self._extract_key_values(batch, build_keys)

            for row_idx in range(batch.num_rows):
                key = tuple(val[row_idx].as_py() for val in key_values)
                hash_table[key].append((len(build_batches) - 1, row_idx))

        if len(hash_table) == 0:
            if self.join_type in (JoinType.LEFT, JoinType.FULL):
                for batch in probe_node.execute():
                    yield self._create_left_outer_batch(batch)
            return

        for probe_batch in probe_node.execute():
            probe_key_values = self._extract_key_values(probe_batch, probe_keys)

            for probe_row_idx in range(probe_batch.num_rows):
                probe_key = tuple(val[probe_row_idx].as_py() for val in probe_key_values)

                if probe_key in hash_table:
                    build_rows = hash_table[probe_key]
                    for build_batch_idx, build_row_idx in build_rows:
                        build_batch = build_batches[build_batch_idx]
                        joined = self._join_rows(
                            probe_batch, probe_row_idx, build_batch, build_row_idx
                        )
                        yield joined
                else:
                    if self.join_type in (JoinType.LEFT, JoinType.FULL):
                        yield self._create_left_outer_row(probe_batch, probe_row_idx)

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
        raise NotImplementedError("LEFT OUTER JOIN not yet fully implemented")

    def _create_left_outer_row(
        self, left_batch: pa.RecordBatch, left_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for right side."""
        raise NotImplementedError("LEFT OUTER JOIN not yet fully implemented")

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

        for left_batch in self.left.execute():
            for left_row_idx in range(left_batch.num_rows):
                matched = False

                for right_batch in right_batches:
                    for right_row_idx in range(right_batch.num_rows):
                        if self.condition is None or self._evaluate_condition(
                            left_batch, left_row_idx, right_batch, right_row_idx
                        ):
                            yield self._join_rows(
                                left_batch, left_row_idx, right_batch, right_row_idx
                            )
                            matched = True

                if not matched and self.join_type in (JoinType.LEFT, JoinType.FULL):
                    yield self._create_left_outer_row(left_batch, left_row_idx)

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

        return result[0].as_py() if result.length() > 0 else False

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
        raise NotImplementedError("LEFT OUTER JOIN not yet fully implemented")

    def _create_left_outer_row(
        self, left_batch: pa.RecordBatch, left_row_idx: int
    ) -> pa.RecordBatch:
        """Create single row with NULLs for right side."""
        raise NotImplementedError("LEFT OUTER JOIN not yet fully implemented")

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
        from collections import defaultdict
        from .expressions import ColumnRef, FunctionCall

        input_table = self._materialize_input()
        groups = self._build_groups(input_table)
        result_batch = self._compute_result(input_table, groups)
        yield result_batch

    def _materialize_input(self) -> pa.Table:
        """Materialize all input batches into a table."""
        batches = []
        for batch in self.input.execute():
            batches.append(batch)
        return pa.Table.from_batches(batches)

    def _build_groups(self, table: pa.Table) -> dict:
        """Build hash table of group keys to row indices."""
        from collections import defaultdict
        from .expressions import ColumnRef

        groups = defaultdict(list)

        for row_idx in range(table.num_rows):
            key = self._extract_group_key(table, row_idx)
            groups[key].append(row_idx)

        return groups

    def _extract_group_key(self, table: pa.Table, row_idx: int) -> tuple:
        """Extract grouping key for a row."""
        from .expressions import ColumnRef

        if len(self.group_by) == 0:
            return ()

        key_values = []
        for group_expr in self.group_by:
            if isinstance(group_expr, ColumnRef):
                col_data = table.column(group_expr.column)
                value = col_data[row_idx].as_py()
                key_values.append(value)

        return tuple(key_values)

    def _compute_result(self, table: pa.Table, groups: dict) -> pa.RecordBatch:
        """Compute aggregate results for all groups."""
        import pyarrow.compute as pc
        from .expressions import FunctionCall, ColumnRef

        result_arrays = []

        for group_key, row_indices in groups.items():
            row_data = self._compute_group_row(table, group_key, row_indices)
            result_arrays.append(row_data)

        return self._arrays_to_batch(result_arrays)

    def _compute_group_row(self, table: pa.Table, group_key: tuple, row_indices: List[int]) -> List:
        """Compute one result row for a group."""
        from .expressions import FunctionCall, ColumnRef
        import pyarrow.compute as pc

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

        return row_data

    def _find_group_by_index(self, col_ref: "ColumnRef") -> int:
        """Find index of column in group_by list."""
        from .expressions import ColumnRef

        for idx, group_expr in enumerate(self.group_by):
            if isinstance(group_expr, ColumnRef):
                if group_expr.column == col_ref.column:
                    return idx

        return 0

    def _compute_aggregate(self, table: pa.Table, func: "FunctionCall", row_indices: List[int]) -> Any:
        """Compute aggregate function value."""
        import pyarrow.compute as pc
        from .expressions import ColumnRef

        func_name = func.function_name.upper()

        if func_name == "COUNT":
            return len(row_indices)

        if len(func.args) == 0:
            return len(row_indices)

        arg = func.args[0]
        if not isinstance(arg, ColumnRef):
            return None

        col_name = arg.column
        if col_name == "*":
            return len(row_indices)

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

    def _arrays_to_batch(self, result_arrays: List[List]) -> pa.RecordBatch:
        """Convert list of row data to RecordBatch."""
        if len(result_arrays) == 0:
            return pa.RecordBatch.from_arrays([], names=self.output_names)

        num_cols = len(result_arrays[0])
        columns = []

        for col_idx in range(num_cols):
            col_data = []
            for row in result_arrays:
                col_data.append(row[col_idx])
            columns.append(col_data)

        arrays = []
        for col_data in columns:
            arrays.append(pa.array(col_data))

        return pa.RecordBatch.from_arrays(arrays, names=self.output_names)

    def schema(self) -> pa.Schema:
        """Get output schema."""
        from .expressions import FunctionCall, ColumnRef

        input_schema = self.input.schema()
        fields = []

        for idx, agg_expr in enumerate(self.aggregates):
            name = self.output_names[idx]

            if isinstance(agg_expr, ColumnRef):
                field_index = input_schema.get_field_index(agg_expr.column)
                field = input_schema.field(field_index)
                fields.append(pa.field(name, field.type))
            elif isinstance(agg_expr, FunctionCall):
                func_name = agg_expr.function_name.upper()
                if func_name in ("COUNT", "SUM"):
                    fields.append(pa.field(name, pa.int64()))
                elif func_name == "AVG":
                    fields.append(pa.field(name, pa.float64()))
                else:
                    fields.append(pa.field(name, pa.float64()))

        return pa.schema(fields)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashAggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"


@dataclass
class PhysicalSort(PhysicalPlanNode):
    """Sort rows."""

    input: PhysicalPlanNode
    sort_keys: List[Expression]
    ascending: List[bool]

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute sort."""
        raise NotImplementedError("Execute not yet implemented")

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalSort({len(self.sort_keys)} keys)"


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
