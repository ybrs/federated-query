"""Physical plan nodes."""

import json
from abc import ABC, abstractmethod
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

from pydantic import Field

from ..model import StateModel
from .expressions import Expression, and_expressions
from .emit import expression_to_ast, CANONICAL_SOURCE_RESOLVER, MergeResolver, clauses
from .logical import JoinType, ExplainFormat, SetOpKind
from ..utils.logging import get_logger


def _source_ast(expr: Expression) -> exp.Expression:
    """Lower an engine predicate to a sqlglot AST in the canonical source form."""
    return expression_to_ast(expr, CANONICAL_SOURCE_RESOLVER)


def _parse_table_sample(sample_text: str) -> exp.Expression:
    """Parse a stored Postgres-form TABLESAMPLE fragment into a sqlglot node.

    The clause is held as raw text; recover the structured node once here so
    sqlglot renders the source dialect's own sampling syntax.
    """
    import sqlglot

    parsed = sqlglot.parse_one(f"SELECT * FROM _t {sample_text}", read="postgres")
    return parsed.find(exp.Table).args.get("sample")


def build_table_ref(name, schema=None, alias=None, sample=None) -> exp.Table:
    """Build a FROM table reference ``["schema".]"name"`` with optional parts.

    The one scan-to-exp.Table builder, shared by the remote scan and the
    single-source pushdown: a quoted name (and optional quoted schema), an
    optional quoted alias, and an optional TABLESAMPLE recovered from its stored
    text. Each caller supplies its own alias policy; the node construction is
    here so it exists once.
    """
    table = exp.Table(this=exp.to_identifier(name, quoted=True))
    if schema is not None:
        table.set("db", exp.to_identifier(schema, quoted=True))
    if alias:
        table.set("alias", exp.TableAlias(this=exp.to_identifier(alias, quoted=True)))
    if sample is not None:
        table.set("sample", _parse_table_sample(sample))
    return table


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
# Name under which a window operator registers its single input with the merge engine.
_MERGE_WINDOW_RELATION = "in_window"
# Name under which a DISTINCT projection registers its projected input.
_MERGE_DISTINCT_RELATION = "in_distinct"
# Name under which a grouped-limit registers its single input, plus the synthetic
# columns it adds: a row-order index (stable tiebreak so the merge path keeps the
# same rows as the Python streaming path) and the per-key row number.
_MERGE_GROUPED_LIMIT_RELATION = "in_grouped_limit"
_GROUPED_LIMIT_INDEX_COL = "__gl_idx"
_GROUPED_LIMIT_RN_COL = "__gl_rn"


class PhysicalPlanNode(StateModel, ABC):
    """Base class for physical plan nodes."""

    # The per-query DuckDB merge engine, attached imperatively after planning
    # (a private attr, not a field: execution state, not plan structure).
    _merge_engine: Any = None

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


def to_source_sql(connection, postgres_sql: str) -> str:
    """Transpile the engine's Postgres-form SQL into the source's own dialect.

    String-building nodes (scans, remote joins) generate Postgres-flavored SQL;
    re-parsing and rendering it through the source dialect transpiles
    dialect-divergent syntax (function names, TABLESAMPLE, ordered-set
    aggregates) to what the source actually accepts.
    """
    ast = connection.parse_query(postgres_sql)
    return ast.sql(dialect=connection.render_dialect)


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
    # Typed constant standing in for a raw Python scalar in a predicate.
    # Built from the runtime value once its Python type maps to a DataType.
    return Literal.create(value=value, data_type=data_type)


def _table_from_batches(batches: List[pa.RecordBatch], schema: pa.Schema) -> pa.Table:
    """Build an Arrow table from batches, falling back to an empty typed table."""
    if not batches:
        return schema.empty_table()
    return pa.Table.from_batches(batches)


class PhysicalCTE(PhysicalPlanNode):
    """Materializes a cross-source CTE body once, shared by every reference.

    A CTE is a named relation: its body is executed a single time into an Arrow
    table that every reference reads, so the work is never repeated even when
    are pushed whole as a remote ``WITH``; this is the cross-source path.
    """

    name: str
    body: PhysicalPlanNode
    column_names: Optional[List[str]] = None
    # Lazily-filled cache of the materialized body (private attr, not a field).
    _cached: Optional[Any] = None

    @classmethod
    def create(
        cls,
        *,
        name: str,
        body: PhysicalPlanNode,
        column_names: Optional[List[str]] = None,
    ) -> "PhysicalCTE":
        """Sanctioned fresh-construction path for PhysicalCTE.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            name=name,
            body=body,
            column_names=column_names,
        )

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


def _alias_column_map(
    alias: Optional[str], names
) -> Dict[Tuple[Optional[str], str], str]:
    """Map every output column to itself under a relation alias.

    A relation that introduces an alias (a scan, a derived table, a CTE
    reference) exposes ``(alias, column) -> physical_name`` so a qualified
    reference resolves to a specific physical column instead of a bare name that
    could be shared across relations. Returns {} when there is no alias.
    """
    if alias is None:
        return {}
    mapping: Dict[Tuple[Optional[str], str], str] = {}
    for name in names:
        mapping[(alias, name)] = name
    return mapping


class PhysicalCTEScan(PhysicalPlanNode):
    """Reads a materialized CTE's rows; one per reference, shares the producer."""

    producer: PhysicalCTE
    alias: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        producer: PhysicalCTE,
        alias: Optional[str] = None,
    ) -> "PhysicalCTEScan":
        """Sanctioned fresh-construction path for PhysicalCTEScan.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            producer=producer,
            alias=alias,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.producer]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield the producer's materialized rows."""
        yield from self.producer.materialize().to_batches()

    def schema(self) -> pa.Schema:
        return self.producer.schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose the CTE's columns under the reference alias (or the CTE name)."""
        return _alias_column_map(self.alias or self.producer.name, self.schema().names)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTEScan({self.producer.name})"


class PhysicalAliasedRelation(PhysicalPlanNode):
    """Re-exposes a derived relation's columns under its subquery alias.

    A SubqueryScan introduces an alias - ``(SELECT ...) AS u`` or
    ``(VALUES ...) AS v(a, b)`` - and outer references bind as ``u.col``. This
    wrapper carries that alias and maps each output column to itself under it, so
    a qualified reference resolves to a specific physical column rather than
    falling back to a bare name. Inner relation aliases are intentionally hidden
    (SQL scoping): only ``alias.col`` is visible above the subquery.
    """

    input: PhysicalPlanNode
    alias: str

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        alias: str,
    ) -> "PhysicalAliasedRelation":
        """Sanctioned fresh-construction path for PhysicalAliasedRelation.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            alias=alias,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Pass the input's batches through unchanged."""
        yield from self.input.execute()

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose every output column under the subquery alias."""
        return _alias_column_map(self.alias, self.schema().names)

    def estimated_cost(self) -> float:
        return self.input.estimated_cost()

    def __repr__(self) -> str:
        return f"PhysicalAliasedRelation(alias={self.alias})"


class PhysicalCTEMergeQuery(PhysicalPlanNode):
    """Runs a whole (possibly recursive) WITH inside the merge engine.

    Every base source relation in the CTE is materialized into Arrow and
    registered under a generated name; the rendered WITH references those names
    so the in-memory DuckDB runs the recursion (and any multi-reference)
    single engine.
    """

    sql: str
    inputs: Dict[str, PhysicalPlanNode]
    output_names: List[str]

    @classmethod
    def create(
        cls,
        *,
        sql: str,
        inputs: Dict[str, PhysicalPlanNode],
        output_names: List[str],
    ) -> "PhysicalCTEMergeQuery":
        """Sanctioned fresh-construction path for PhysicalCTEMergeQuery.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            sql=sql,
            inputs=inputs,
            output_names=output_names,
        )

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
        """Output schema of the WITH, read from the result without fetching rows."""
        engine = self.merge_engine()
        return engine.schema(self.sql, self._materialized_inputs())

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
            return condition.model_copy(update={"table": "l"})
        if condition.column in right_names:
            return condition.model_copy(update={"table": "r"})
        return condition
    if isinstance(condition, BinaryOp):
        # Same operator with both operands recursively side-qualified.
        # A new node is needed because each child gets an l./r. rewrite.
        return BinaryOp.create(
            op=condition.op,
            left=_qualify_join_condition(condition.left, left_names, right_names),
            right=_qualify_join_condition(condition.right, left_names, right_names),
        )
    if isinstance(condition, UnaryOp):
        # Same unary operator wrapping its side-qualified operand.
        # Rebuilt so the negated/inner column carries the l./r. prefix.
        return UnaryOp.create(
            op=condition.op,
            operand=_qualify_join_condition(condition.operand, left_names, right_names),
        )
    return condition


def _right_output_name(physical_name: str, left_names) -> str:
    """Right-side output name under the left-wins collision rule.

    A right column whose name collides with a left column is renamed
    ``right_<name>``; otherwise it keeps its name. The one rule shared by the
    join SELECT list, the output schema, and the parent-resolution alias map, so
    all three agree on the join's columns.
    """
    if physical_name in left_names:
        return f"right_{physical_name}"
    return physical_name


def _merge_join_select_list(join_type, left_schema, right_schema) -> str:
    """SELECT list reproducing the join output: left only for SEMI/ANTI, else both."""
    parts = []
    for name in left_schema.names:
        parts.append(f'l."{name}" AS "{name}"')
    if join_type in (JoinType.SEMI, JoinType.ANTI):
        return ", ".join(parts)
    left_name_set = set(left_schema.names)
    for field in right_schema:
        output_name = _right_output_name(field.name, left_name_set)
        parts.append(f'r."{field.name}" AS "{output_name}"')
    return ", ".join(parts)


def _merge_join_sql(
    join_type, on_clause: str, left_schema: pa.Schema, right_schema: pa.Schema
) -> str:
    """Render a merge-engine join SELECT over the two registered relations.

    Shared by the hash join (equi-key ON clause) and the nested-loop join
    (arbitrary ON clause); only the ON clause differs between them.
    """
    select_list = _merge_join_select_list(join_type, left_schema, right_schema)
    return (
        f"SELECT {select_list} "
        f"FROM {_MERGE_LEFT_RELATION} AS l "
        f"{_MERGE_JOIN_KEYWORDS[join_type]} {_MERGE_RIGHT_RELATION} AS r "
        f"ON {on_clause}"
    )


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


def _evaluate_expression_type(expr, input_schema, alias_map) -> pa.DataType:
    """Infer an expression's Arrow type by evaluating it against an empty batch.

    The authoritative way to type a computed expression: run the actual kernels
    on a zero-row batch with the input schema and read the result type, instead
    of guessing from the operator. Shared by projection output typing and
    aggregate-argument typing so both agree.
    """
    from ..executor.expression_evaluator import ExpressionEvaluator

    empty_batch = pa.RecordBatch.from_pylist([], schema=input_schema)
    return ExpressionEvaluator(empty_batch, alias_map=alias_map).evaluate(expr).type


def _sum_result_type(arg_type: pa.DataType) -> pa.DataType:
    """The Arrow type a SUM produces: integers widen to a 64-bit accumulator,
    floating and decimal arguments keep their own type."""
    if pa.types.is_integer(arg_type):
        return pa.int64()
    return arg_type


def _physical_column_name(col_ref, alias_map) -> str:
    """Physical output-column name for a column reference - the one resolver.

    The qualifier is resolved through the alias map so a qualified key reads the
    intended column rather than a bare-name match (which, with two same-named
    columns, would pick the wrong one). A reference the map does not carry - a
    column of a flat scan, whose namespace is unambiguous - uses its own name.
    Both the type path (_column_ref_type) and the array path (_resolve_column)
    go through here, so a declared schema and the executed output resolve every
    column identically.
    """
    return alias_map.get((col_ref.table, col_ref.column), col_ref.column)


def _column_ref_type(col_ref, input_schema: pa.Schema, alias_map) -> pa.DataType:
    """Input Arrow type of a column reference, resolved through the alias map."""
    physical = _physical_column_name(col_ref, alias_map)
    index = input_schema.get_field_index(physical)
    return input_schema.field(index).type


def _key_column_arrays(batch, keys, aliases) -> List[pa.Array]:
    """Collect each key column's array from a batch, resolving qualified names.

    The one build-key extractor for the hash join and the EXPLAIN dynamic-filter
    collector. A non-ColumnRef key is unsupported and RAISES - never a silent
    empty result, which would hide the unsupported key and render an empty
    dynamic filter as if there were no values. ``aliases`` maps a key's (table,
    column) to its physical output name; a key the map does not carry uses its
    own name.
    """
    from .expressions import ColumnRef

    arrays = []
    for key in keys:
        if not isinstance(key, ColumnRef):
            raise NotImplementedError(
                f"Key extraction not supported for {type(key).__name__}"
            )
        arrays.append(batch.column(_physical_column_name(key, aliases or {})))
    return arrays


def _prepend_batch(first: pa.RecordBatch, rest) -> Iterator[pa.RecordBatch]:
    """Yield an already-pulled first batch, then the remaining batches."""
    yield first
    yield from rest


def _require_engine(node: "PhysicalPlanNode"):
    """Return the node's attached DuckDB merge engine, or raise if none.

    Local execution always runs through the merge engine (DuckDB is a hard
    dependency the executor attaches to every plan); a missing engine is a bug,
    so it fails loudly rather than silently taking a slow Python path.
    """
    engine = node.merge_engine()
    if engine is None:
        raise RuntimeError(
            f"{type(node).__name__} requires the DuckDB merge engine; none attached"
        )
    return engine


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
    join_type, left: "PhysicalPlanNode", right: "PhysicalPlanNode"
) -> Dict[Tuple[Optional[str], str], str]:
    """Build a (table, column) -> output-name map for a binary join.

    A SEMI/ANTI join outputs left columns only (matching the execution SELECT
    list), so no right column enters the map. For an INNER/OUTER join, left
    columns keep their names and a colliding right column is renamed with a
    ``right_`` prefix (the left-wins rule). When both sides expose the SAME
    (table, column) key - e.g. a subquery that scans the same table the outer
    query does - the LEFT side wins the key (the outer reference is what the user
    wrote); the right's renamed column is reachable by its physical name but
    never overwrites the left, so a qualified outer reference can never silently
    resolve to the inner relation.
    """
    alias_map: Dict[Tuple[Optional[str], str], str] = dict(left.column_aliases())
    if join_type in (JoinType.SEMI, JoinType.ANTI):
        return alias_map
    left_names = set(left.schema().names)
    for key, physical_name in right.column_aliases().items():
        if key in alias_map:
            continue
        alias_map[key] = _right_output_name(physical_name, left_names)
    return alias_map


def _join_output_schema(
    join_type, left: "PhysicalPlanNode", right: "PhysicalPlanNode"
) -> pa.Schema:
    """Build the output schema of a binary join.

    A SEMI/ANTI join outputs left columns only, so the right side adds nothing.
    For an INNER/OUTER join, left columns keep their names and a colliding right
    column is renamed ``right_<name>`` - the same left-wins rule used by the
    execution SELECT list (_merge_join_select_list) and the parent-resolution map
    (_join_output_aliases), so the declared schema, the actual output, and
    qualified-reference resolution all agree.
    """
    left_schema = left.schema()
    fields = []
    for field in left_schema:
        fields.append(field)
    if join_type in (JoinType.SEMI, JoinType.ANTI):
        return pa.schema(fields)
    left_names = set(left_schema.names)
    for field in right.schema():
        name = _right_output_name(field.name, left_names)
        fields.append(pa.field(name, field.type, nullable=field.nullable))
    return pa.schema(fields)


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
    sample: Optional[str] = None  # TABLESAMPLE clause (Postgres-form SQL)
    group_by: Optional[List[Expression]] = None  # Optional GROUP BY expressions
    grouping_sets: Optional[List[List[Expression]]] = None  # ROLLUP/CUBE/GROUPING SETS
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

    @classmethod
    def create(
        cls,
        *,
        datasource: str,
        schema_name: str,
        table_name: str,
        columns: List[str],
        filters: Optional[Expression] = None,
        datasource_connection: Any = None,
        sample: Optional[str] = None,
        group_by: Optional[List[Expression]] = None,
        grouping_sets: Optional[List[List[Expression]]] = None,
        aggregates: Optional[List[Expression]] = None,
        output_names: Optional[List[str]] = None,
        alias: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
        distinct: bool = False,
        dynamic_filter_keys: Optional[List[Expression]] = None,
        dynamic_filter_values: Optional[List[tuple]] = None,
    ) -> "PhysicalScan":
        """Sanctioned fresh-construction path for PhysicalScan.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            datasource=datasource,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            filters=filters,
            datasource_connection=datasource_connection,
            sample=sample,
            group_by=group_by,
            grouping_sets=grouping_sets,
            aggregates=aggregates,
            output_names=output_names,
            alias=alias,
            limit=limit,
            offset=offset,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
            distinct=distinct,
            dynamic_filter_keys=dynamic_filter_keys,
            dynamic_filter_values=dynamic_filter_values,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def apply_dynamic_filter(
        self, key_columns: List[Expression], value_tuples: List[tuple]
    ) -> bool:
        """Add a runtime ``key IN (...)`` filter from a join's build-side keys.

        Only single-column keys with renderable literal values are handled; a
        composite key or an untypable value declines (the join probes in full).
        """
        if len(key_columns) != 1:
            return False
        in_filter = self._build_in_filter(key_columns[0], value_tuples)
        if in_filter is None:
            return False
        self.filters = and_expressions(self.filters, in_filter)
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
        # Runtime membership test restricting the scan to the join's build keys.
        # Assembled from the collected key literals for dynamic filter pushdown.
        return InList.create(value=key_column, options=options)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose this scan's columns under its table alias.

        Lets a join above resolve qualified references (``o.id``) even when
        another relation in the join exposes a column of the same name.
        """
        return _alias_column_map(self.alias, self.schema().names)

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute scan on remote data source."""
        if self.datasource_connection is None:
            raise RuntimeError("Data source connection not set")

        query = self._render_source_sql()

        self.datasource_connection.ensure_connected()
        batches = self.datasource_connection.execute_query(query)

        for batch in batches:
            yield batch

    def _render_source_sql(self) -> str:
        """Render this scan in the source dialect via the transpile boundary.

        The single emitter builds the canonical Postgres AST; to_source_sql
        parses it once and renders the source dialect, which is where function
        names and other dialect-divergent syntax are translated.
        """
        return to_source_sql(self.datasource_connection, self._build_query())

    def _build_query(self) -> str:
        """Render this scan as canonical Postgres-form SQL from its AST."""
        return self._build_ast().sql(dialect="postgres")

    def _build_ast(self) -> exp.Select:
        """Build the sqlglot SELECT for this scan via the shared assembler.

        A scan is the N=1 case of a same-source query, so it composes the same
        skeleton (clauses.assemble_select) that the N-table single-source
        pushdown builder uses; only the FROM clause (one table) differs.
        """
        where_pred, having_pred = self._split_where_having()
        return clauses.assemble_select(
            self._table_ref(),
            self._select_items(),
            where=_source_ast(where_pred) if where_pred is not None else None,
            group=clauses.group_by(
                self.group_by, self.grouping_sets, CANONICAL_SOURCE_RESOLVER
            ),
            having=_source_ast(having_pred) if having_pred is not None else None,
            distinct=self.distinct,
            order=clauses.order_by(
                self.order_by_keys,
                self.order_by_ascending,
                self.order_by_nulls,
                CANONICAL_SOURCE_RESOLVER,
            ),
            limit=self.limit,
            offset=self.offset,
        )

    def _split_where_having(self):
        """Partition the scan filter into (WHERE, HAVING) via the shared splitter.

        A folded GROUP BY query carries its WHERE and HAVING conjuncts together;
        the shared split_where_having routes aggregate-output references (the
        binder rewrote ``SUM(x)`` to its alias) into HAVING and substitutes them
        back to the aggregate, rather than leaving an aggregate/alias in WHERE.
        """
        from .expressions import aggregate_output_map, split_where_having

        if not self.filters:
            return None, None
        if not (self.group_by or self.aggregates):
            return self.filters, None
        output_map = aggregate_output_map(self.output_names, self.aggregates)
        return split_where_having(self.filters, output_map)

    def _select_items(self) -> list:
        """Build the SELECT projection: aggregates, a star, or plain columns.

        With aggregates present, the aggregates list is the full ordered SELECT
        (GROUP BY keys first, then aggregate functions) aliased by output_names.
        """
        if self.aggregates:
            return clauses.select_expressions(
                self.aggregates, self.output_names, CANONICAL_SOURCE_RESOLVER
            )
        if "*" in self.columns:
            return [exp.Star()]
        items = []
        for col in self.columns:
            items.append(exp.column(col, quoted=True))
        return items

    def _table_ref(self) -> exp.Table:
        """Build the FROM table with optional alias and TABLESAMPLE."""
        return build_table_ref(
            self.table_name, self.schema_name, self.alias, self.sample
        )

    def schema(self) -> pa.Schema:
        """Get output schema."""
        if self._schema is not None:
            return self._schema

        if self.datasource_connection is None:
            raise RuntimeError("Data source connection not set")

        query = self._render_source_sql()
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


class PhysicalProjection(PhysicalPlanNode):
    """Projection expressions."""

    input: PhysicalPlanNode
    expressions: List[Expression]
    output_names: List[str]
    distinct: bool = False
    distinct_on: Optional[List[Expression]] = None

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        expressions: List[Expression],
        output_names: List[str],
        distinct: bool = False,
        distinct_on: Optional[List[Expression]] = None,
    ) -> "PhysicalProjection":
        """Sanctioned fresh-construction path for PhysicalProjection.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            expressions=expressions,
            output_names=output_names,
            distinct=distinct,
            distinct_on=distinct_on,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute projection, de-duplicating rows when SELECT DISTINCT.

        DISTINCT ON is only correct when pushed to a source (or the merge
        engine), where ORDER BY chooses the surviving row; the local pyarrow
        path cannot honor that, so it fails fast rather than guess.
        """
        if self.distinct_on is not None:
            from ..parser.errors import UnsupportedSQLError

            raise UnsupportedSQLError(
                "DISTINCT ON is only supported when the query pushes to a single source"
            )
        if not self.distinct:
            for batch in self.input.execute():
                yield self._project_batch(batch)
            return
        yield from self._execute_distinct()

    def _execute_distinct(self) -> Iterator[pa.RecordBatch]:
        """Project all input, then emit only distinct rows via the merge engine.

        Deduplication runs as ``SELECT DISTINCT * `` in DuckDB - the one local
        set/dedup engine that PhysicalUnion, PhysicalSort, and the aggregate
        operators also use - instead of a separate pyarrow group-by path.
        """
        engine = _require_engine(self)
        projected = []
        for batch in self.input.execute():
            projected.append(self._project_batch(batch))
        table = _table_from_batches(projected, self.schema())
        sql = f"SELECT DISTINCT * FROM {_MERGE_DISTINCT_RELATION}"
        yield from engine.run(sql, {_MERGE_DISTINCT_RELATION: table})

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
                    field_type = _column_ref_type(
                        expr, input_schema, self.column_aliases()
                    )
                    fields.append(pa.field(self.output_names[i], field_type))
            else:
                inferred = self._infer_expression_type(expr, input_schema)
                fields.append(pa.field(self.output_names[i], inferred))

        return pa.schema(fields)

    def _infer_expression_type(
        self, expr: Expression, input_schema: pa.Schema
    ) -> pa.DataType:
        """Infer a computed expression's Arrow type via the shared evaluator."""
        return _evaluate_expression_type(expr, input_schema, self.column_aliases())

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
        """Resolve a column to its array via the shared physical-name rule.

        Uses the same resolver as schema typing (_physical_column_name), so the
        executed output and the declared schema read every column identically.
        """
        from .expressions import ColumnRef

        if not isinstance(expr, ColumnRef):
            raise ValueError("Expected ColumnRef")
        return batch.column(_physical_column_name(expr, alias_map))

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


class PhysicalWindow(PhysicalPlanNode):
    """Evaluate a window-bearing projection in the merge engine.

    Runs ``SELECT <projection exprs> FROM input`` in DuckDB, which computes the
    window functions natively. Used on the cross-source path, when a projection
    containing a window sits over a non-pushable input; the single-source path
    renders the window straight into the remote query instead, so this operator
    is never built there. Window functions cannot be evaluated row-at-a-time, so
    a merge engine is required.
    """

    input: PhysicalPlanNode
    expressions: List[Expression]
    output_names: List[str]

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        expressions: List[Expression],
        output_names: List[str],
    ) -> "PhysicalWindow":
        """Sanctioned fresh-construction path for PhysicalWindow.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            expressions=expressions,
            output_names=output_names,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Stream the input through DuckDB, which evaluates the window SQL."""
        engine = _require_engine(self)
        reader = _streaming_reader(self.input)
        sql = self._window_sql(self.input.column_aliases())
        yield from engine.run(sql, {_MERGE_WINDOW_RELATION: reader})

    def _window_sql(self, aliases) -> str:
        """Render ``SELECT <exprs> AS <names> FROM in_window``."""
        select_list = self._window_select_list(aliases)
        return f"SELECT {select_list} FROM {_MERGE_WINDOW_RELATION}"

    def _window_select_list(self, aliases) -> str:
        """Alias each projection expression via the shared clause builder.

        Columns resolve to their physical merge-relation names (MergeResolver) and
        the whole expression (window function included) lowers through the one
        emitter to DuckDB SQL.
        """
        if len(self.expressions) != len(self.output_names):
            raise ValueError(
                f"expressions ({len(self.expressions)}) and output_names "
                f"({len(self.output_names)}) length mismatch"
            )
        return clauses.select_expressions_fragment(
            self.expressions, self.output_names, MergeResolver(aliases), dialect="duckdb"
        )

    def schema(self) -> pa.Schema:
        """Output schema, read from DuckDB so window result types are exact.

        Uses an empty table built from the input schema rather than executing
        the input, so schema()/EXPLAIN never materializes the upstream query.
        """
        engine = self.merge_engine()
        empty_input = self.input.schema().empty_table()
        sql = self._window_sql(self.input.column_aliases())
        return engine.schema(sql, {_MERGE_WINDOW_RELATION: empty_input})

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Each output column is exposed by its bare output name."""
        result = {}
        for name in self.output_names:
            result[(None, name)] = name
        return result

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalWindow({len(self.expressions)} exprs)"


class PhysicalFilter(PhysicalPlanNode):
    """Filter rows."""

    input: PhysicalPlanNode
    predicate: Expression

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        predicate: Expression,
    ) -> "PhysicalFilter":
        """Sanctioned fresh-construction path for PhysicalFilter.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            predicate=predicate,
        )

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


class PhysicalHashJoin(PhysicalPlanNode):
    """Hash join implementation."""

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    join_type: JoinType
    left_keys: List[Expression]
    right_keys: List[Expression]
    build_side: str  # "left" or "right"

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        join_type: JoinType,
        left_keys: List[Expression],
        right_keys: List[Expression],
        build_side: str,
    ) -> "PhysicalHashJoin":
        """Sanctioned fresh-construction path for PhysicalHashJoin.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            join_type=join_type,
            left_keys=left_keys,
            right_keys=right_keys,
            build_side=build_side,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute the hash join in the DuckDB merge engine."""
        yield from self._execute_merge(_require_engine(self))

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
        """
        names = _key_column_names(len(build_keys))
        key_tables = []
        for batch in build_batches:
            arrays = _key_column_arrays(batch, build_keys, aliases)
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
        return _merge_join_sql(
            self.join_type, self._merge_on_clause(), left_schema, right_schema
        )

    def _merge_on_clause(self) -> str:
        """Render the equi-join condition as l.key = r.key conjuncts."""
        if len(self.left_keys) != len(self.right_keys):
            raise ValueError(
                f"join key count mismatch: {len(self.left_keys)} left vs "
                f"{len(self.right_keys)} right"
            )
        left_aliases = self.left.column_aliases()
        right_aliases = self.right.column_aliases()
        conjuncts = []
        for left_key, right_key in zip(self.left_keys, self.right_keys):
            left_name = _physical_column_name(left_key, left_aliases or {})
            right_name = _physical_column_name(right_key, right_aliases or {})
            conjuncts.append(f'l."{left_name}" = r."{right_name}"')
        return " AND ".join(conjuncts)

    def schema(self) -> pa.Schema:
        """Combine both sides' schemas (left-wins on name collision)."""
        return _join_output_schema(self.join_type, self.left, self.right)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Map qualified columns to their (possibly renamed) output names."""
        return _join_output_aliases(self.join_type, self.left, self.right)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashJoin({self.join_type.value}, build={self.build_side})"


class PhysicalRemoteJoin(PhysicalPlanNode):
    """Join executed directly on a single data source.

    The planner builds this only when both sides are plain same-source scans
    (_try_plan_remote_join / _is_remote_join_candidate). Single-source pushdown
    (PhysicalRemoteQuery) currently collapses the whole maximal same-source
    subtree first, so the candidate check does not fire in practice today; the
    node remains for selective/partial pushdown, where only the remote portion
    of a join is pushed while part of the data is on the coordinator.
    """

    left: PhysicalScan
    right: PhysicalScan
    join_type: JoinType
    condition: Expression
    datasource_connection: Any
    group_by: Optional[List[Expression]] = None
    grouping_sets: Optional[List[List[Expression]]] = None  # ROLLUP/CUBE/GROUPING SETS
    aggregates: Optional[List[Expression]] = None
    output_names: Optional[List[str]] = None
    distinct: bool = False
    order_by_keys: Optional[List[Expression]] = None
    order_by_ascending: Optional[List[bool]] = None
    order_by_nulls: Optional[List[Optional[str]]] = None
    _schema: Optional[pa.Schema] = None
    _column_alias_map: Dict[Tuple[Optional[str], str], str] = {}

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalScan,
        right: PhysicalScan,
        join_type: JoinType,
        condition: Expression,
        datasource_connection: Any,
        group_by: Optional[List[Expression]] = None,
        grouping_sets: Optional[List[List[Expression]]] = None,
        aggregates: Optional[List[Expression]] = None,
        output_names: Optional[List[str]] = None,
        distinct: bool = False,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
    ) -> "PhysicalRemoteJoin":
        """Sanctioned fresh-construction path for PhysicalRemoteJoin.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            join_type=join_type,
            condition=condition,
            datasource_connection=datasource_connection,
            group_by=group_by,
            grouping_sets=grouping_sets,
            aggregates=aggregates,
            output_names=output_names,
            distinct=distinct,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def execute(self) -> Iterator[pa.RecordBatch]:
        query = to_source_sql(self.datasource_connection, self._build_query())
        self.datasource_connection.ensure_connected()
        batches = self.datasource_connection.execute_query(query)
        for batch in batches:
            yield batch

    def schema(self) -> pa.Schema:
        if self._schema is None:
            query = to_source_sql(self.datasource_connection, self._build_query())
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
        if not self.group_by and self.grouping_sets is None:
            return query
        group_clause = self._build_group_by_clause()
        return f"{query} GROUP BY {group_clause}"

    def _apply_order_by(self, query: str) -> str:
        """Append the ORDER BY clause via the shared clause builder when present."""
        if not self.order_by_keys:
            return query
        order_clause = clauses.order_by_fragment(
            self.order_by_keys,
            self.order_by_ascending,
            self.order_by_nulls,
            CANONICAL_SOURCE_RESOLVER,
        )
        return f"{query} ORDER BY {order_clause}"

    def _build_select_clause(self) -> str:
        if self.aggregates:
            return self._build_aggregate_select_clause()
        items = []
        seen = set()
        self._append_select_items(self.left, items, seen, False)
        self._append_select_items(self.right, items, seen, True)
        return ", ".join(items)

    def _build_aggregate_select_clause(self) -> str:
        """Render the aggregate SELECT list via the shared clause builder."""
        return clauses.select_expressions_fragment(
            self.aggregates or [],
            self.output_names or [],
            CANONICAL_SOURCE_RESOLVER,
            dialect="postgres",
        )

    def _build_group_by_clause(self) -> str:
        """Render the GROUP BY keys via the shared clause builder."""
        return clauses.group_by_fragment(
            self.group_by, self.grouping_sets, CANONICAL_SOURCE_RESOLVER
        )

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
        return f'"{alias}"."{column}"'

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
        """Append a quoted ``AS "alias"`` suffix when an alias is present.

        The alias is quoted to match the emitter's quoted column qualifiers
        (``"alias"."col"``); an unquoted alias would case-fold and miss a
        case-sensitive qualifier.
        """
        if alias:
            return f'{source} AS "{alias}"'
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


class PhysicalNestedLoopJoin(PhysicalPlanNode):
    """Nested loop join implementation."""

    left: PhysicalPlanNode
    right: PhysicalPlanNode
    join_type: JoinType
    condition: Optional[Expression]

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        join_type: JoinType,
        condition: Optional[Expression],
    ) -> "PhysicalNestedLoopJoin":
        """Sanctioned fresh-construction path for PhysicalNestedLoopJoin.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            join_type=join_type,
            condition=condition,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Run the join in the merge engine (DuckDB); raise if none is attached.

        The arbitrary ``condition`` (in practice the null-aware SEMI/ANTI/LEFT
        predicate decorrelation produces, ``x = v OR x IS NULL OR v IS NULL``) is
        rendered to DuckDB with each column resolved to the left or right input.
        DuckDB materializes each side once and joins set-based, so the inner side
        is never re-scanned per outer row (which, cross-source, would be a remote
        query per row). There is no Python row-by-row fallback: a missing engine
        is a bug, so _require_engine raises rather than silently running slow.
        """
        yield from self._execute_merge(_require_engine(self))

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
        on_clause = "TRUE"
        if self.condition is not None:
            qualified = _qualify_join_condition(
                self.condition, set(left_schema.names), set(right_schema.names)
            )
            on_clause = qualified.to_sql()
        return _merge_join_sql(self.join_type, on_clause, left_schema, right_schema)

    def schema(self) -> pa.Schema:
        """Combine both sides' schemas (left-wins on name collision)."""
        return _join_output_schema(self.join_type, self.left, self.right)

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Map qualified columns to their (possibly renamed) output names."""
        return _join_output_aliases(self.join_type, self.left, self.right)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalNestedLoopJoin({self.join_type.value})"


class PhysicalHashAggregate(PhysicalPlanNode):
    """Hash-based aggregation."""

    input: PhysicalPlanNode
    group_by: List[Expression]
    aggregates: List[Expression]
    output_names: List[str]
    grouping_sets: Optional[List[List[Expression]]] = None

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        group_by: List[Expression],
        aggregates: List[Expression],
        output_names: List[str],
        grouping_sets: Optional[List[List[Expression]]] = None,
    ) -> "PhysicalHashAggregate":
        """Sanctioned fresh-construction path for PhysicalHashAggregate.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            group_by=group_by,
            aggregates=aggregates,
            output_names=output_names,
            grouping_sets=grouping_sets,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute the hash aggregation in the DuckDB merge engine.

        Group keys and aggregate arguments (plain columns or any expression)
        render to DuckDB SQL, so DuckDB computes the whole GROUP BY; there is no
        separate Python aggregation.
        """
        yield from self._execute_merge_aggregate(_require_engine(self))

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
        """Alias each SELECT expression to its output name.

        Uses the shared alias-and-join skeleton (clauses.aliased_select_fragment);
        the per-expression renderer is the one emitter, except an ordered-set
        aggregate's WITHIN GROUP form, which DuckDB needs literal.
        """
        if len(self.aggregates) != len(self.output_names):
            raise ValueError(
                f"aggregates ({len(self.aggregates)}) and output_names "
                f"({len(self.output_names)}) length mismatch"
            )
        return clauses.aliased_select_fragment(
            self.aggregates,
            self.output_names,
            lambda expr: self._render_agg_expr(expr, aliases),
        )

    def _aggregate_group_list(self, aliases) -> str:
        """Render GROUP BY keys / GROUPING SETS via the shared clause builder."""
        return clauses.group_by_fragment(
            self.group_by, self.grouping_sets, MergeResolver(aliases), dialect="duckdb"
        )

    def _render_agg_expr(self, expr: Expression, aliases) -> str:
        """Render a SELECT/group expression against the input's physical columns.

        Everything lowers through the one emitter except an ordered-set
        aggregate's WITHIN GROUP form: the emitter's typed node transpiles to an
        inline ``f(arg ORDER BY key)`` that DuckDB rejects, so that one case is
        wrapped here from emitter-rendered parts. COUNT(*), DISTINCT, and
        multi-argument calls the emitter already renders identically.
        """
        from .expressions import FunctionCall

        if isinstance(expr, FunctionCall) and expr.within_group_key is not None:
            return self._render_ordered_set_aggregate(expr, aliases)
        return expression_to_ast(expr, MergeResolver(aliases)).sql(dialect="duckdb")

    def _render_ordered_set_aggregate(self, func: "FunctionCall", aliases) -> str:
        """Render ``f(args) WITHIN GROUP (ORDER BY key [DESC])`` for DuckDB.

        DuckDB accepts the standard WITHIN GROUP form but rejects the inline
        ``f(args ORDER BY key)`` that sqlglot's DuckDB dialect transpiles an
        ordered-set Anonymous call to; the call and the sort key are rendered by
        the one emitter, and only the WITHIN GROUP wrapper is built here.
        """
        resolver = MergeResolver(aliases)
        call = func.model_copy(update={"within_group_key": None})
        call_sql = expression_to_ast(call, resolver).sql(dialect="duckdb")
        key_sql = expression_to_ast(func.within_group_key, resolver).sql(dialect="duckdb")
        direction = " DESC" if func.within_group_desc else ""
        return f"{call_sql} WITHIN GROUP (ORDER BY {key_sql}{direction})"

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
        """Output type of one aggregate-list item: a group key or an aggregate.

        A column group key is typed from the input schema (resolving its
        qualifier via the alias map); an aggregate uses the function's
        result-type rule. Any other expression key (e.g. ``a % b``, evaluated by
        the merge engine, not the local kernels) keeps the integer default.
        """
        from .expressions import FunctionCall, ColumnRef

        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            return self._infer_aggregate_type(expr, input_schema)
        if isinstance(expr, ColumnRef):
            return _column_ref_type(expr, input_schema, self.column_aliases())
        return pa.int64()

    def _infer_aggregate_type(
        self, func: "FunctionCall", input_schema: pa.Schema
    ) -> pa.DataType:
        """Result type of an aggregate from the function and its argument type.

        COUNT is always an integer; MIN/MAX preserve the argument type (so
        MIN(name) is varchar, not float64 - the previous bug that made the
        result cast corrupt non-numeric columns); AVG is double; SUM widens
        integers but keeps float/decimal. Any other aggregate falls back to its
        argument type, the safest declaration when the rule is unknown.
        """
        func_name = func.function_name.upper()
        if func_name in ("COUNT", "COUNT_DISTINCT"):
            return pa.int64()
        arg_type = self._aggregate_arg_type(func, input_schema)
        if func_name in ("MIN", "MAX"):
            return arg_type
        if func_name == "AVG":
            return pa.float64()
        if func_name == "SUM":
            return _sum_result_type(arg_type)
        return arg_type

    def _aggregate_arg_type(
        self, func: "FunctionCall", input_schema: pa.Schema
    ) -> pa.DataType:
        """Arrow type of an aggregate's first argument.

        A column argument (the usual case, e.g. ``MIN(name)``) is typed from the
        input schema; a computed argument (``SUM(a + b)``) by the shared
        empty-batch evaluator.
        """
        from .expressions import ColumnRef

        if not func.args:
            return pa.int64()
        arg = func.args[0]
        if isinstance(arg, ColumnRef):
            return _column_ref_type(arg, input_schema, self.column_aliases())
        return _evaluate_expression_type(arg, input_schema, self.column_aliases())

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashAggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


class PhysicalSort(PhysicalPlanNode):
    """Sort rows."""

    input: PhysicalPlanNode
    sort_keys: List[Expression]
    ascending: List[bool]
    nulls_order: Optional[List[Optional[str]]] = None

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        sort_keys: List[Expression],
        ascending: List[bool],
        nulls_order: Optional[List[Optional[str]]] = None,
    ) -> "PhysicalSort":
        """Sanctioned fresh-construction path for PhysicalSort.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            sort_keys=sort_keys,
            ascending=ascending,
            nulls_order=nulls_order,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Execute the sort in the DuckDB merge engine.

        Every sort key (a plain column or any expression) renders to DuckDB SQL,
        so DuckDB does all the ordering; there is no separate Python sort.
        """
        yield from self._execute_merge_sort(_require_engine(self))

    def _execute_merge_sort(self, engine) -> Iterator[pa.RecordBatch]:
        """Sort through DuckDB, with per-key NULLS FIRST/LAST placement."""
        reader = _streaming_reader(self.input)
        order = self._sort_order_clause(self.input.column_aliases())
        sql = f"SELECT * FROM {_MERGE_SORT_RELATION} ORDER BY {order}"
        return engine.run(sql, {_MERGE_SORT_RELATION: reader})

    def _sort_order_clause(self, aliases) -> str:
        """Render every ORDER BY key via the shared clause builder.

        The single ``emit.clauses.order_by`` resolves keys to their physical
        merge-relation names (MergeResolver) and fills the Postgres NULLS default
        so the merge engine's ordering matches the source's; each key renders to
        a DuckDB fragment for the merge query.
        """
        return clauses.order_by_fragment(
            self.sort_keys,
            self.ascending,
            self.nulls_order,
            MergeResolver(aliases),
            dialect="duckdb",
        )

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalSort({len(self.sort_keys)} keys)"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


class PhysicalLimit(PhysicalPlanNode):
    """Limit rows. ``limit`` is None for an OFFSET with no row cap."""

    input: PhysicalPlanNode
    limit: Optional[int]
    offset: int = 0

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        limit: Optional[int],
        offset: int = 0,
    ) -> "PhysicalLimit":
        """Sanctioned fresh-construction path for PhysicalLimit.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            limit=limit,
            offset=offset,
        )

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


class PhysicalValues(PhysicalPlanNode):
    """Emits in-memory rows built from constant expressions.

    Used for FROM-less SELECTs such as ``SELECT 42``: each row is a list of
    expressions evaluated without any input columns.
    """

    rows: List[List[Expression]]
    output_names: List[str]

    @classmethod
    def create(
        cls,
        *,
        rows: List[List[Expression]],
        output_names: List[str],
    ) -> "PhysicalValues":
        """Sanctioned fresh-construction path for PhysicalValues.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            rows=rows,
            output_names=output_names,
        )

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
    column_alias_map: Dict[Tuple[Optional[str], str], str] = Field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        datasource: str,
        datasource_connection: Any,
        query_ast: Any,
        output_names: List[str],
        column_alias_map: Dict[Tuple[Optional[str], str], str],
    ) -> "PhysicalRemoteQuery":
        """Sanctioned fresh-construction path for PhysicalRemoteQuery.
        column_alias_map is explicit (the default_factory cannot be a parameter
        default); pass {} when no alias remapping is needed."""
        return cls(
            datasource=datasource,
            datasource_connection=datasource_connection,
            query_ast=query_ast,
            output_names=output_names,
            column_alias_map=column_alias_map,
        )
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
        """Render the carried canonical AST in the target source's dialect.

        The AST is built in the canonical Postgres form by the single emitter;
        to_source_sql is the one transpile boundary that translates
        dialect-divergent syntax for the source.
        """
        return to_source_sql(
            self.datasource_connection, self.query_ast.sql(dialect="postgres")
        )

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalRemoteQuery({self.datasource})"


class PhysicalUnion(PhysicalPlanNode):
    """Concatenates input streams; optionally removes duplicate rows."""

    inputs: List[PhysicalPlanNode]
    distinct: bool

    @classmethod
    def create(
        cls,
        *,
        inputs: List[PhysicalPlanNode],
        distinct: bool,
    ) -> "PhysicalUnion":
        """Sanctioned fresh-construction path for PhysicalUnion.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            inputs=inputs,
            distinct=distinct,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return self.inputs

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield all input batches, deduplicating rows when distinct."""
        if self.distinct:
            yield from self._execute_merge_distinct(_require_engine(self))
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

    def schema(self) -> pa.Schema:
        return self.inputs[0].schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose the first branch's aliases; a union's output is its columns."""
        return self.inputs[0].column_aliases()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        kind = "DISTINCT" if self.distinct else "ALL"
        return f"PhysicalUnion({kind}, {len(self.inputs)} inputs)"


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

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        kind: SetOpKind,
        distinct: bool,
        datasource: str,
        datasource_connection: Any,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> "PhysicalRemoteSetOp":
        """Sanctioned fresh-construction path for PhysicalRemoteSetOp.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            kind=kind,
            distinct=distinct,
            datasource=datasource,
            datasource_connection=datasource_connection,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
            limit=limit,
            offset=offset,
        )

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
        """Render the set operation in the source dialect via the transpile boundary.

        The branch ASTs are built in the canonical Postgres form by the single
        emitter; to_source_sql translates dialect-divergent syntax (function
        names, etc.) when rendering the source dialect.
        """
        return to_source_sql(
            self.datasource_connection, self.build_remote_ast().sql(dialect="postgres")
        )

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
        return branch._build_ast()

    def _combine(
        self, left_ast: exp.Expression, right_ast: exp.Expression
    ) -> exp.Expression:
        """Wrap two branch ASTs in the matching set-operation node."""
        node_cls = clauses.SET_OP_EXP[self.kind]
        return node_cls(this=left_ast, expression=right_ast, distinct=self.distinct)

    def _wrap_order_limit(self, set_op_ast: exp.Expression) -> exp.Expression:
        """Wrap the set operation in an outer SELECT carrying ORDER BY/LIMIT/OFFSET."""
        alias = exp.TableAlias(this=exp.to_identifier("_setop", quoted=True))
        subquery = exp.Subquery(this=set_op_ast, alias=alias)
        select = exp.Select(expressions=[exp.Star()]).from_(subquery)
        order = clauses.order_by(
            self.order_by_keys,
            self.order_by_ascending,
            self.order_by_nulls,
            CANONICAL_SOURCE_RESOLVER,
        )
        if order is not None:
            select.set("order", order)
        return clauses.apply_limit_offset(select, self.limit, self.offset)


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

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        kind: SetOpKind,
        distinct: bool,
    ) -> "PhysicalSetOperation":
        """Sanctioned fresh-construction path for PhysicalSetOperation.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            kind=kind,
            distinct=distinct,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Yield the combined rows honoring INTERSECT/EXCEPT semantics."""
        yield from self._execute_merge_setop(_require_engine(self))

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

    def schema(self) -> pa.Schema:
        return self.left.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        suffix = "" if self.distinct else " ALL"
        return f"PhysicalSetOperation({self.kind.value}{suffix})"


class PhysicalSingleRowGuard(PhysicalPlanNode):
    """Enforces scalar-subquery cardinality at execution time.

    With no keys, more than one input row in total raises. With keys, a
    subqueries, where each key admits at most one row.
    """

    input: PhysicalPlanNode
    keys: List[Expression]

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        keys: List[Expression],
    ) -> "PhysicalSingleRowGuard":
        """Sanctioned fresh-construction path for PhysicalSingleRowGuard.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            keys=keys,
        )

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
        key_arrays = _key_column_arrays(batch, self.keys, self.input.column_aliases())
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

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        keys: List[Expression],
        limit: int,
        order_by_keys: Optional[List[Expression]] = None,
        order_by_ascending: Optional[List[bool]] = None,
        order_by_nulls: Optional[List[Optional[str]]] = None,
    ) -> "PhysicalGroupedLimit":
        """Sanctioned fresh-construction path for PhysicalGroupedLimit.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            input=input,
            keys=keys,
            limit=limit,
            order_by_keys=order_by_keys,
            order_by_ascending=order_by_ascending,
            order_by_nulls=order_by_nulls,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Keep at most ``limit`` rows per key, via a DuckDB ROW_NUMBER window.

        Partition and ordering keys (plain columns or any expression) render to
        DuckDB SQL, so DuckDB applies the per-key limit; there is no Python path.
        """
        yield from self._execute_merge(_require_engine(self))

    def _execute_merge(self, engine) -> Iterator[pa.RecordBatch]:
        """Run the per-key limit as a windowed query in the merge engine."""
        table = self._indexed_input_table()
        sql = self._grouped_limit_sql(self.input.schema().names)
        return engine.run(sql, {_MERGE_GROUPED_LIMIT_RELATION: table})

    def _indexed_input_table(self) -> pa.Table:
        """Materialize the input and append a stable row-order index column."""
        table = pa.Table.from_batches(
            list(self.input.execute()), schema=self.input.schema()
        )
        index = pa.array(range(table.num_rows), type=pa.int64())
        return table.append_column(_GROUPED_LIMIT_INDEX_COL, index)

    def _grouped_limit_sql(self, names: List[str]) -> str:
        """Render the ``ROW_NUMBER`` window and keep each key's first rows."""
        window = (
            f"ROW_NUMBER() OVER (PARTITION BY {self._partition_clause()} "
            f"ORDER BY {self._merge_order_clause()})"
        )
        inner = (
            f"SELECT *, {window} AS {_GROUPED_LIMIT_RN_COL} "
            f"FROM {_MERGE_GROUPED_LIMIT_RELATION}"
        )
        select_list = self._merge_select_list(names)
        return (
            f"SELECT {select_list} FROM ({inner}) AS _t "
            f"WHERE {_GROUPED_LIMIT_RN_COL} <= {self.limit} "
            f'ORDER BY "{_GROUPED_LIMIT_INDEX_COL}"'
        )

    def _merge_select_list(self, names: List[str]) -> str:
        """Quote the original output columns (drops the synthetic columns)."""
        parts = []
        for name in names:
            parts.append(f'"{name}"')
        return ", ".join(parts)

    def _partition_clause(self) -> str:
        """Render the PARTITION BY list, lowering each key (column or expression)."""
        parts = []
        for key in self.keys:
            parts.append(
                expression_to_ast(key, MergeResolver({})).sql(dialect="duckdb")
            )
        return ", ".join(parts)

    def _merge_order_clause(self) -> str:
        """Order each partition by the ordering keys, then the row index.

        The ordering keys render through the shared ``emit.clauses.order_by``
        (explicit direction, NULLS only when supplied); the trailing row-index
        keeps ties in input order, so the same rows survive as the streaming
        Python path.
        """
        parts = []
        if self.order_by_keys:
            parts.append(
                clauses.order_by_fragment(
                    self.order_by_keys,
                    self.order_by_ascending,
                    self.order_by_nulls,
                    MergeResolver({}),
                    dialect="duckdb",
                )
            )
        parts.append(f'"{_GROUPED_LIMIT_INDEX_COL}" ASC')
        return ", ".join(parts)

    def schema(self) -> pa.Schema:
        return self.input.schema()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalGroupedLimit(limit={self.limit}, keys={len(self.keys)})"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return self.input.column_aliases()


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

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        left_name: str,
        left_alias: str,
        base_scan: PhysicalPlanNode,
        base_name: str,
        lateral_sql: str,
        output_names: List[str],
        join_type: JoinType,
        correlations: List[Tuple[str, Any, str]],
    ) -> "PhysicalLateralJoin":
        """Sanctioned fresh-construction path for PhysicalLateralJoin.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            left_name=left_name,
            left_alias=left_alias,
            base_scan=base_scan,
            base_name=base_name,
            lateral_sql=lateral_sql,
            output_names=output_names,
            join_type=join_type,
            correlations=correlations,
        )

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

        """
        if not isinstance(self.base_scan, PhysicalScan):
            return self.base_scan
        term = _derive_domain_filter(self.correlations, left_table)
        if term is None:
            return self.base_scan
        combined = term
        if self.base_scan.filters is not None:
            combined = and_expressions(self.base_scan.filters, term)
        return self.base_scan.model_copy(update={"filters": combined})

    def schema(self) -> pa.Schema:
        """The (left + value) output schema, read without fetching rows.

        Taken from the LATERAL result's Arrow reader, which exposes the schema
        ``LIMIT 0`` probe produces zero batches and would lose the schema).
        """
        engine = self.merge_engine()
        return engine.schema(self.lateral_sql, self._inputs())

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return {}

    def __repr__(self) -> str:
        return f"PhysicalLateralJoin({self.join_type.name}, base={self.base_name})"


def _derive_domain_filter(correlations, left_table: pa.Table) -> Optional[Expression]:
    """Derive a sound base-relation filter from the left's correlation domain.

    ``=`` becomes ``inner IN (domain)``; ``<``/``<=`` a ``< max(domain)`` bound;
    ``>``/``>=`` a ``> min(domain)`` bound. Any other shape contributes no term
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
        # Unqualified reference to the base relation's correlated inner column.
        # Names the column the derived domain term will constrain.
        column = ColumnRef.create(table=None, column=inner_col)
        term = _domain_term(column, op, values)
        if term is not None:
            terms.append(term)
    if not terms:
        return None
    combined = terms[0]
    for term in terms[1:]:
        combined = and_expressions(combined, term)
    return combined


def _domain_term(column, op, values) -> Optional[Expression]:
    """Build one sound dynamic-filter term for a correlation operator."""
    from .expressions import BinaryOp, BinaryOpType, InList

    if op == BinaryOpType.EQ:
        if not _within_dynamic_filter_cap(len(values)):
            return None
        # Membership term reducing the base to the equality domain's values.
        # An equality correlation maps to IN over the full distinct domain.
        return InList.create(value=column, options=[_literal(v) for v in values])
    if op in (BinaryOpType.LT, BinaryOpType.LTE):
        # Upper-bound term keeping only rows below the domain's maximum.
        # A less-than correlation is soundly reduced to a single max bound.
        return BinaryOp.create(op=op, left=column, right=_literal(max(values)))
    if op in (BinaryOpType.GT, BinaryOpType.GTE):
        # Lower-bound term keeping only rows above the domain's minimum.
        # A greater-than correlation is soundly reduced to a single min bound.
        return BinaryOp.create(op=op, left=column, right=_literal(min(values)))
    return None


def _literal(value) -> Expression:
    """Wrap a Python domain value as a typed literal."""
    from .expressions import Literal, DataType

    if isinstance(value, bool):
        # Boolean constant carrying a domain value into the filter tree.
        # Chosen because the Python value is a bool.
        return Literal.create(value=value, data_type=DataType.BOOLEAN)
    if isinstance(value, int):
        # Integer constant carrying a domain value into the filter tree.
        # Widened to BIGINT since the Python value is an int.
        return Literal.create(value=value, data_type=DataType.BIGINT)
    if isinstance(value, float):
        # Floating-point constant carrying a domain value into the filter tree.
        # Chosen because the Python value is a float.
        return Literal.create(value=value, data_type=DataType.DOUBLE)
    # String constant carrying any remaining domain value into the filter tree.
    # The fallback for values that are not bool, int, or float.
    return Literal.create(value=value, data_type=DataType.VARCHAR)


class PhysicalExplain(PhysicalPlanNode):
    """Explain a physical plan without executing it."""

    child: PhysicalPlanNode
    format: ExplainFormat = ExplainFormat.TEXT

    @classmethod
    def create(
        cls,
        *,
        child: PhysicalPlanNode,
        format: ExplainFormat = ExplainFormat.TEXT,
    ) -> "PhysicalExplain":
        """Sanctioned fresh-construction path for PhysicalExplain.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            child=child,
            format=format,
        )

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
                # snapshot.sql is already rendered in the source's dialect by the
                # recorder, so EXPLAIN shows exactly what is sent to the source.
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


class _DatasourceQuerySnapshot(StateModel):
    """Captured query metadata for a single datasource call."""

    datasource_name: str
    sql: str
    query_ast: Any

    @classmethod
    def create(
        cls,
        *,
        datasource_name: str,
        sql: str,
        query_ast: Any,
    ) -> "_DatasourceQuerySnapshot":
        """Sanctioned fresh-construction path for _DatasourceQuerySnapshot.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            datasource_name=datasource_name,
            sql=sql,
            query_ast=query_ast,
        )


class Gather(PhysicalPlanNode):
    """Gather data from a remote data source.

    This operator fetches data from a remote source and materializes it locally.
    It's used when we need to bring data to the coordinator for local processing.
    """

    datasource: str
    query: str  # SQL query to execute on remote source
    datasource_connection: Any = None

    @classmethod
    def create(
        cls,
        *,
        datasource: str,
        query: str,
        datasource_connection: Any = None,
    ) -> "Gather":
        """Sanctioned fresh-construction path for Gather.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            datasource=datasource,
            query=query,
            datasource_connection=datasource_connection,
        )

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
        return f"type={node.join_type.value} sources={left}|X|{right}"

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
            key_arrays = _key_column_arrays(batch, build_keys, aliases)
            for row_index in range(batch.num_rows):
                self._add_distinct_key(key_arrays, row_index, seen, distinct)
                if len(distinct) > 5:
                    return distinct
        return distinct

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
        native_sql = to_source_sql(datasource, base_sql)
        sql = self._annotate_dynamic_filter(scan, native_sql)
        # EXPLAIN record of the per-source query this scan will issue.
        # Captured from the scan's rendered native SQL and parsed AST.
        snapshot = _DatasourceQuerySnapshot.create(
            datasource_name=scan.datasource, sql=sql, query_ast=query_ast
        )
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
        postgres_sql = join._build_query()
        query_ast = datasource.parse_query(postgres_sql)
        native_sql = to_source_sql(datasource, postgres_sql)
        # EXPLAIN record of the pushed-down remote join query.
        # Attributed to the left input's datasource that runs the join.
        snapshot = _DatasourceQuerySnapshot.create(
            datasource_name=join.left.datasource, sql=native_sql, query_ast=query_ast
        )
        snapshots.append(snapshot)

    def _record_remote_set_op(
        self, set_op: "PhysicalRemoteSetOp", snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if set_op.datasource_connection is None:
            return
        query_ast = set_op.build_remote_ast()
        sql = to_source_sql(
            set_op.datasource_connection, query_ast.sql(dialect="postgres")
        )
        # EXPLAIN record of the pushed-down remote set operation query.
        # Taken from the set op's remote AST rendered to the source dialect.
        snapshot = _DatasourceQuerySnapshot.create(
            datasource_name=set_op.datasource, sql=sql, query_ast=query_ast
        )
        snapshots.append(snapshot)

    def _record_remote_query(
        self, node: "PhysicalRemoteQuery", snapshots: List[_DatasourceQuerySnapshot]
    ) -> None:
        if node.datasource_connection is None:
            return
        # node.query_ast is the emitter's canonical form (function calls are
        # exp.Anonymous). For the EXPLAIN snapshot, normalize it through the
        # source's own dialect and parser - as _record_scan does - so the
        # displayed AST is the dialect's typed form (and SEMI/ANTI joins, which a
        # Postgres render would rewrite to EXISTS, are preserved). Execution
        # still renders node.query_ast directly; this is display-only.
        sql = node.query_ast.sql(dialect=node.datasource_connection.render_dialect)
        display_ast = node.datasource_connection.parse_query(sql)
        # EXPLAIN record of the remote query, normalized for display only.
        # Uses the dialect-reparsed AST so the shown form matches the source.
        snapshot = _DatasourceQuerySnapshot.create(
            datasource_name=node.datasource, sql=sql, query_ast=display_ast
        )
        snapshots.append(snapshot)
