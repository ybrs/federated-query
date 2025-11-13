"""Physical plan nodes."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Iterator, Any, TYPE_CHECKING, Dict, Callable
from enum import Enum

import pyarrow as pa

from .expressions import Expression
from .logical import JoinType

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
        output_names = []

        for i, expr in enumerate(self.expressions):
            if isinstance(expr, ColumnRef):
                if expr.column == "*":
                    for col_idx in range(batch.num_columns):
                        columns.append(batch.column(col_idx))
                        output_names.append(batch.schema.field(col_idx).name)
                else:
                    column_data = batch.column(expr.column)
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
        """Extract values for a column."""
        values = []
        for group_key, accumulators in hash_table.items():
            if col_idx < len(self.group_by):
                values.append(group_key[col_idx] if group_key else None)
            else:
                agg_idx = col_idx
                value = self._finalize_accumulator(accumulators[agg_idx])
                values.append(value)

        return pa.array(values)

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
class PhysicalExplain(PhysicalPlanNode):
    """Explain a physical plan without executing it."""

    child: PhysicalPlanNode

    def children(self) -> List[PhysicalPlanNode]:
        return [self.child]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Return textual representation of the plan."""
        formatter = _PlanFormatter()
        lines = formatter.format(self.child)
        array = pa.array(lines)
        batch = pa.RecordBatch.from_arrays([array], names=["plan"])
        yield batch

    def schema(self) -> pa.Schema:
        return pa.schema([pa.field("plan", pa.string())])

    def estimated_cost(self) -> float:
        return -1.0

    def __repr__(self) -> str:
        return "PhysicalExplain()"


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
