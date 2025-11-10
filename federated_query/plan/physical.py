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
        raise NotImplementedError("Execute not yet implemented")

    def schema(self) -> pa.Schema:
        # Combine schemas from both sides
        raise NotImplementedError("Schema resolution not yet implemented")

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
        raise NotImplementedError("Execute not yet implemented")

    def schema(self) -> pa.Schema:
        raise NotImplementedError("Schema resolution not yet implemented")

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
        raise NotImplementedError("Execute not yet implemented")

    def schema(self) -> pa.Schema:
        raise NotImplementedError("Schema resolution not yet implemented")

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
