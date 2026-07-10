"""Physical plan nodes."""

import functools
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

from pydantic import Field, PrivateAttr

from ..model import StateModel
from .expressions import Expression
from .emit import expression_to_ast, CANONICAL_SOURCE_RESOLVER, MergeResolver, clauses
from .logical import JoinType, ExplainFormat, SetOpKind


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


# Cap on distinct build-side keys for which a hash join pushes a dynamic IN
# filter into the probe side. Above this the IN list is not worth the round
# trip, so the Rust engine fetches the probe in full. A cost-based choice would
# replace this constant later.
_DYNAMIC_FILTER_MAX_KEYS = 2000

# Name under which a window operator's input is registered when its rendered SQL
# runs as a Rust ``raw_sql`` fragment.
_MERGE_WINDOW_RELATION = "in_window"
# Name under which a window-bearing aggregate registers its single input; its
# rendered SELECT ... GROUP BY runs as a Rust ``raw_sql`` fragment. Matches the
# input key rust_ir registers for the aggregate.
_MERGE_AGGREGATE_RELATION = "in_0"


def _expression_contains_window(expr) -> bool:
    """Whether an expression tree contains a WindowExpr at any depth."""
    from .expressions import WindowExpr, expression_children

    if isinstance(expr, WindowExpr):
        return True
    for child in expression_children(expr):
        if _expression_contains_window(child):
            return True
    return False


def _expression_contains_grouping(expr) -> bool:
    """Whether an expression tree contains a GROUPING() call at any depth."""
    from .expressions import FunctionCall, expression_children

    if isinstance(expr, FunctionCall) and expr.function_name.lower() == "grouping":
        return True
    for child in expression_children(expr):
        if _expression_contains_grouping(child):
            return True
    return False


# Name under which a grouped-limit registers its single input, plus the synthetic
# columns its rendered SQL adds: a row-order index (stable tiebreak) and the
# per-key row number.
_MERGE_GROUPED_LIMIT_RELATION = "in_grouped_limit"
_GROUPED_LIMIT_INDEX_COL = "__gl_idx"
_GROUPED_LIMIT_RN_COL = "__gl_rn"


def _derived_once(method):
    """Cache a node's derived value (schema / column aliases) per instance.

    Both derivations recurse the whole subtree, and every PARENT re-derives
    its children's results, which is exponential in tree depth: TPC-DS q59
    spent 4.6s of a 4.6s run re-deriving schemas during IR build against
    ~0.3s of engine execution. The first call computes; every later call
    returns the cached value. See PhysicalPlanNode for the invariant that
    makes this safe.
    """
    name = method.__name__

    @functools.wraps(method)
    def wrapper(self):
        cache = self._derived_cache
        if name not in cache:
            cache[name] = method(self)
        return cache[name]

    return wrapper


class PhysicalPlanNode(StateModel, ABC):
    """Base class for physical plan nodes.

    Physical nodes are a pure plan representation: they carry schema,
    column-alias, and SQL-rendering information that ``rust_ir`` serializes into
    the Rust engine's IR. They are never executed in Python; the Rust engine is
    the one execution path.

    ``schema()`` and ``column_aliases()`` are computed ONCE per node: every
    subclass's implementation is wrapped by ``_derived_once`` (see
    ``__init_subclass__``). The cache relies on one invariant: the fields
    those methods derive from (inputs, expressions, output names) are never
    mutated after construction - plan rewrites replace nodes via
    ``model_copy`` (which starts the copy with an EMPTY cache), never by
    assigning structural fields in place. The in-place mutations that do
    exist (a scan's ``distinct``, a remote set-op's ``limit``/``offset``,
    ``estimated_rows`` threading) do not affect schema or aliases.
    """

    # Per-instance cache for the wrapped derivations; private so pydantic
    # keeps it out of fields, equality, and construction.
    _derived_cache: Dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init_subclass__(cls, **kwargs) -> None:
        """Wrap the subclass's own schema()/column_aliases() in the cache.

        Wrapping happens HERE, once per subclass, so no node class can forget
        the cache and silently reintroduce the exponential recomputation.
        Only methods defined by the subclass itself are wrapped (inherited
        ones are already wrapped on the class that defined them).
        """
        super().__init_subclass__(**kwargs)
        for name in ("schema", "column_aliases"):
            method = cls.__dict__.get(name)
            if method is not None and not getattr(
                method, "__isabstractmethod__", False
            ):
                setattr(cls, name, _derived_once(method))

    def model_copy(
        self, *, update: Optional[Dict[str, Any]] = None, deep: bool = False
    ) -> "PhysicalPlanNode":
        """Copy with an EMPTY derived cache: the copy's fields may differ."""
        copied = super().model_copy(update=update, deep=deep)
        copied._derived_cache = {}
        return copied

    @abstractmethod
    def children(self) -> List["PhysicalPlanNode"]:
        """Return child nodes."""
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


class PhysicalCTE(PhysicalPlanNode):
    """Materializes a cross-source CTE body once, shared by every reference.

    A CTE is a named relation: its body is executed a single time into an Arrow
    table that every reference reads, so the work is never repeated even when
    are pushed whole as a remote ``WITH``; this is the cross-source path.
    """

    name: str
    body: PhysicalPlanNode
    column_names: Optional[List[str]] = None

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

    def schema(self) -> pa.Schema:
        return self.producer.schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose the CTE's columns under the reference alias (or the CTE name)."""
        return _alias_column_map(self.alias or self.producer.name, self.schema().names)

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTEScan({self.producer.name})"


class PhysicalShipment(PhysicalPlanNode):
    """Ships a foreign relation INTO a target source, then runs the child there.

    Dim shipping: a small foreign dimension (``body``) is materialized once and
    written to ``datasource`` as a TEMP TABLE named ``table``; ``child`` - an
    island on ``datasource`` whose SQL references that temp table - then runs
    with the whole fact join+aggregate collapsed into one source, so only the
    aggregate OUTPUT crosses the boundary instead of the fact rows.

    Mirrors PhysicalCTE (materialize a body once, consume it above), but the
    materialization lands on ANOTHER source. The emitter emits the body binding,
    then a ``ship`` step, then the child - so the temp table exists before the
    island reads it. The child island carries a seeded schema (the temp table
    cannot be probed from the python-side connection).
    """

    table: str
    datasource: str
    body: PhysicalPlanNode
    child: PhysicalPlanNode

    @classmethod
    def create(
        cls,
        *,
        table: str,
        datasource: str,
        body: PhysicalPlanNode,
        child: PhysicalPlanNode,
    ) -> "PhysicalShipment":
        """Sanctioned fresh-construction path for PhysicalShipment.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            table=table,
            datasource=datasource,
            body=body,
            child=child,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.body, self.child]

    def schema(self) -> pa.Schema:
        """The output is the island's output; the shipped body is a side effect."""
        return self.child.schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Resolve references against the island that produces the output."""
        return self.child.column_aliases()

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalShipment(table={self.table}, ds={self.datasource})"


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

    def schema(self) -> pa.Schema:
        """The WITH's output schema is computed natively by the Rust engine.

        The result types of a (possibly recursive) WITH depend on running the
        SQL, which only the execution engine does; this node is a pure plan
        representation whose columns the Rust engine types. A caller that reaches
        here wants a type the plan cannot supply, so it fails loudly rather than
        return a guessed schema.
        """
        raise NotImplementedError(
            "PhysicalCTEMergeQuery output schema is computed by the Rust engine"
        )

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalCTEMergeQuery({len(self.inputs)} sources)"


def _right_output_name(physical_name: str, left_names) -> str:
    """Right-side output name under the left-wins collision rule.

    A right column whose name collides with a left column is renamed
    ``right_<name>``; if THAT also collides - which happens in a left-deep
    self-join that references the same relation three or more times, because an
    earlier join in the chain already produced ``right_<name>`` - it is suffixed
    ``right_<name>_1``, ``_2`` ... until unique. (Concretely, the TPC-DS q31
    benchmark self-joins one aggregate CTE as ss1/ss2/ss3, so its ``store_sales``
    column appears from three relations.) Two distinct right columns never map to
    the same output (their bases differ), so this depends only on ``left_names``
    and stays the ONE rule the join SELECT list, the output schema, and the
    parent alias map all share.
    """
    if physical_name not in left_names:
        return physical_name
    candidate = f"right_{physical_name}"
    unique = candidate
    suffix = 1
    while unique in left_names:
        unique = f"{candidate}_{suffix}"
        suffix += 1
    return unique


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
    # The cost model's row estimate (threaded from the logical Scan), so the
    # execution-IR reduction can orient by size; None when not cost-estimated.
    estimated_rows: Optional[int] = None
    # Source-catalog NDV per join-key column (threaded from the logical Scan),
    # so the reduction can refuse a dynamic filter whose keys cover the probe
    # column's whole value domain; None when not cost-estimated.
    column_ndv: Optional[Dict[str, int]] = None
    # A known output schema seeded at construction, used INSTEAD of probing the
    # source. Set only for the synthetic scan of a shipped dimension: that temp
    # table lives on the engine's pinned connection, so the python-side probe
    # cannot see it. None everywhere else, so every real table still probes.
    seeded_schema: Optional[pa.Schema] = None

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
        estimated_rows: Optional[int] = None,
        column_ndv: Optional[Dict[str, int]] = None,
        seeded_schema: Optional[pa.Schema] = None,
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
            estimated_rows=estimated_rows,
            column_ndv=column_ndv,
            seeded_schema=seeded_schema,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return []

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """Expose this scan's columns under its table alias.

        Lets a join above resolve qualified references (``o.id``) even when
        another relation in the join exposes a column of the same name.
        """
        return _alias_column_map(self.alias, self.schema().names)

    def execute(self) -> Iterator[pa.RecordBatch]:
        """Read this scan's rows from its data source.

        The Rust engine runs whole plans; this direct read is used only by
        EXPLAIN, which samples a scan's build-side keys to show the dynamic
        ``IN`` filter with real values.
        """
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
        """Get output schema.

        A seeded schema (a shipped-dimension temp table) is returned directly:
        that table exists only on the engine's pinned connection, so the
        python-side probe cannot see it. Every real table still probes.
        """
        if self._schema is not None:
            return self._schema

        if self.seeded_schema is not None:
            self._schema = self.seeded_schema
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
                        expr, input_schema, self.input.column_aliases()
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
        return _evaluate_expression_type(
            expr, input_schema, self.input.column_aliases()
        )

    def estimated_cost(self) -> float:
        # Cost is input cost + projection cost
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalProjection({len(self.expressions)} exprs)"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """This projection's OUTPUT columns: a reference resolves to an output by
        its name, or by the source column of a passthrough expression.

        NOT the input's aliases: a projection that DROPS columns must not expose
        them, and a passthrough of a renamed column - a self-join collision
        column physically named right_customer_id, projected under the clean
        output name customer_id - must resolve to the OUTPUT name. A sort above
        the projection otherwise named the vanished input column and failed
        (q04). The projection's OWN expressions are typed against
        self.input.column_aliases(), since they reference input columns.
        """
        from .expressions import ColumnRef

        aliases: Dict[Tuple[Optional[str], str], str] = {}
        for index, expr in enumerate(self.expressions):
            name = self.output_names[index]
            aliases[(None, name)] = name
            if isinstance(expr, ColumnRef) and expr.column != "*":
                aliases[(expr.table, expr.column)] = name
        return aliases


class PhysicalWindow(PhysicalPlanNode):
    """Evaluate a window-bearing projection as one ``SELECT ... OVER (...)``.

    Renders ``SELECT <projection exprs> FROM input`` and runs it where the plan
    executes: the Rust engine emits it as a SQL fragment, and the DuckDB merge
    path runs it directly. Used on the cross-source path, when a projection
    containing a window sits over a non-pushable input; the single-source path
    renders the window straight into the remote query instead, so this operator
    is never built there. Window functions cannot be evaluated row-at-a-time, so
    execution needs a SQL engine, but the output schema is derived directly from
    the input schema and expression types with no engine call.
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
        self._check_output_arity()
        return clauses.select_expressions_fragment(
            self.expressions,
            self.output_names,
            MergeResolver(aliases),
            dialect="duckdb",
        )

    def schema(self) -> pa.Schema:
        """Output schema computed directly from the input schema and expression
        types, with no merge-engine round trip.

        One output column per projection expression: a plain column reference
        keeps its input Arrow type, a window function takes the Arrow type of its
        underlying function, and any other scalar expression is typed by the
        shared evaluator against the input schema.
        """
        self._check_output_arity()
        input_schema = self.input.schema()
        fields = []
        for index, expr in enumerate(self.expressions):
            field_type = self._output_type(expr, input_schema)
            fields.append(pa.field(self.output_names[index], field_type))
        return pa.schema(fields)

    def _output_type(self, expr: Expression, input_schema: pa.Schema) -> pa.DataType:
        """Arrow type of one output expression, routed by its kind."""
        from .expressions import ColumnRef, WindowExpr

        if isinstance(expr, ColumnRef):
            return _column_ref_type(expr, input_schema, self.column_aliases())
        if isinstance(expr, WindowExpr):
            return self._window_type(expr)
        return _evaluate_expression_type(expr, input_schema, self.column_aliases())

    def _window_type(self, expr: Expression) -> pa.DataType:
        """Arrow type of a window function, from its underlying function's type.

        A ranking window (ROW_NUMBER, RANK, ...) yields an integer; a value or
        aggregate window keeps its function's declared type. A window nested
        inside a larger scalar expression is not routed here - the shared
        evaluator has no window kernel, so such a case fails loudly there.
        """
        from .arrow_types import arrow_type_for

        return arrow_type_for(expr.get_type())

    def _check_output_arity(self) -> None:
        """Raise when expressions and output_names disagree in length."""
        if len(self.expressions) != len(self.output_names):
            raise ValueError(
                f"expressions ({len(self.expressions)}) and output_names "
                f"({len(self.output_names)}) length mismatch"
            )

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
    # Carried over from the logical Join when the cost-based join-ordering
    # rule ran: the estimated output rows and the provenance of any DEFAULTED
    # statistics behind them. EXPLAIN prints both.
    estimated_rows: Optional[int] = None
    estimate_defaults: Optional[List[str]] = None

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
        estimated_rows: Optional[int] = None,
        estimate_defaults: Optional[List[str]] = None,
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
            estimated_rows=estimated_rows,
            estimate_defaults=estimate_defaults,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

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
    # Carried over from the logical Join when the cost-based join-ordering
    # rule ran: the estimated output rows and the provenance of any DEFAULTED
    # statistics behind them. EXPLAIN prints both.
    estimated_rows: Optional[int] = None
    estimate_defaults: Optional[List[str]] = None

    @classmethod
    def create(
        cls,
        *,
        left: PhysicalPlanNode,
        right: PhysicalPlanNode,
        join_type: JoinType,
        condition: Optional[Expression],
        estimated_rows: Optional[int] = None,
        estimate_defaults: Optional[List[str]] = None,
    ) -> "PhysicalNestedLoopJoin":
        """Sanctioned fresh-construction path for PhysicalNestedLoopJoin.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            left=left,
            right=right,
            join_type=join_type,
            condition=condition,
            estimated_rows=estimated_rows,
            estimate_defaults=estimate_defaults,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.left, self.right]

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
    # Learned-stats group provenance, stamped at planning from the logical
    # aggregate: {"subject": subplan signature, "columns": group column names}.
    # The engine measures this aggregate's group count (the collapse); the write
    # path keys it in the catalog by this subject. None = not observable.
    group_observation: Optional[Dict[str, object]] = None

    @classmethod
    def create(
        cls,
        *,
        input: PhysicalPlanNode,
        group_by: List[Expression],
        aggregates: List[Expression],
        output_names: List[str],
        grouping_sets: Optional[List[List[Expression]]] = None,
        group_observation: Optional[Dict[str, object]] = None,
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
            group_observation=group_observation,
        )

    def children(self) -> List[PhysicalPlanNode]:
        return [self.input]

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
            return _column_ref_type(expr, input_schema, self.input.column_aliases())
        from .expressions import contains_aggregate
        from .arrow_types import arrow_type_for

        if contains_aggregate(expr):
            # A scalar over aggregates (100.0 * SUM(x) / SUM(y)): its output type
            # is not an aggregate's and is not int by default. Type it
            # structurally from the engine DataType (its aggregate leaves are
            # typed at binding), so a decimal/double result is not mis-cast to
            # int64 (TPC-H q14).
            return arrow_type_for(expr.get_type())
        # A plain expression group key (a % b): DuckDB computes it and returns an
        # integer for the integer keys these are in practice; keep the integer
        # default rather than the value's structural type, which would need
        # every operator's local kernel just to infer a type.
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
            return _column_ref_type(arg, input_schema, self.input.column_aliases())
        return _evaluate_expression_type(arg, input_schema, self.input.column_aliases())

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def __repr__(self) -> str:
        return f"PhysicalHashAggregate(groups={len(self.group_by)}, aggs={len(self.aggregates)})"

    def has_window_output(self) -> bool:
        """Whether an output expression carries a window function.

        A window over grouped aggregates (sum(sum(x)) OVER (...) ... GROUP BY ...)
        is valid SQL but the structured aggregate fragment cannot express it, so
        such an aggregate is emitted through _aggregate_sql instead.
        """
        for expr in self.aggregates:
            if _expression_contains_window(expr):
                return True
        return False

    def _aggregate_sql(self, aliases) -> str:
        """Render ``SELECT <outputs> FROM in_0 [GROUP BY <keys>]`` for the merge
        engine, used when an output carries a window (see has_window_output).

        DataFusion evaluates the window over the grouped aggregates directly;
        columns resolve to their physical merge-relation names via MergeResolver,
        the same lowering PhysicalWindow uses.
        """
        return self._render_aggregate_sql(self.aggregates, self.output_names, aliases)

    def _render_aggregate_sql(self, exprs, names, aliases) -> str:
        """Render one grouped SELECT over the merge input for the given outputs."""
        resolver = MergeResolver(aliases)
        select_list = clauses.select_expressions_fragment(
            exprs, names, resolver, dialect="duckdb"
        )
        sql = f"SELECT {select_list} FROM {_MERGE_AGGREGATE_RELATION}"
        group = clauses.group_by_fragment(
            self.group_by, self.grouping_sets, resolver, dialect="duckdb"
        )
        if group is not None:
            sql = f"{sql} GROUP BY {group}"
        return sql

    def window_split_needed(self) -> bool:
        """Whether the fused window-aggregate SQL must split into two stages.

        DataFusion cannot plan GROUPING() referenced inside a window expression
        (TPC-DS q70/q86: ``rank() OVER (PARTITION BY grouping(a)+grouping(b)``),
        but evaluates it fine as a plain aggregate output - so the aggregate
        materializes every window operand and the window runs over columns.
        """
        for expr in self.aggregates:
            if _expression_contains_grouping(expr):
                return True
        return False

    def split_window_aggregate_sqls(self, aliases) -> Tuple[str, str]:
        """The two-stage SQL for a window over a GROUPING() aggregate.

        Stage 1 is the GROUP BY, its outputs the non-window outputs plus every
        window operand (grouping()/aggregate calls and key columns) materialized
        under a hidden name. Stage 2 is the window SELECT over stage 1's result,
        every operand replaced by its stage-1 output column, presenting exactly
        the declared output names (hidden columns dropped).
        """
        stage1_exprs: List[Expression] = []
        stage1_names: List[str] = []
        for expr, name in zip(self.aggregates, self.output_names):
            if not _expression_contains_window(expr):
                stage1_exprs.append(expr)
                stage1_names.append(name)
        stage2_items: List[Expression] = []
        for expr, name in zip(self.aggregates, self.output_names):
            stage2_items.append(
                self._stage2_item(expr, name, stage1_exprs, stage1_names)
            )
        return (
            self._render_aggregate_sql(stage1_exprs, stage1_names, aliases),
            self._stage2_sql(stage2_items, stage1_names),
        )

    def _stage2_item(self, expr, name, stage1_exprs, stage1_names) -> Expression:
        """One stage-2 SELECT item: a pass-through of an already-aggregated
        output by name, or the window expression over materialized operands."""
        from .expressions import ColumnRef

        if not _expression_contains_window(expr):
            # The stage-1 output passes through stage 2 unchanged, by name.
            return ColumnRef.create(table=None, column=name)
        return self._materialize_window_operands(expr, stage1_exprs, stage1_names)

    def _materialize_window_operands(self, expr, exprs, names) -> Expression:
        """Rewrite a window-bearing expression to read stage-1 output columns.

        Every grouping()/aggregate call and plain column reference becomes a
        reference to the stage-1 output computing it (appended as a hidden
        output when none does yet); the window structure itself is preserved.
        """
        from .expressions import WindowExpr, map_children

        if isinstance(expr, WindowExpr):
            return self._rebuild_window(expr, exprs, names)
        if self._is_stage1_operand(expr):
            return self._stage1_output_ref(expr, exprs, names)
        return map_children(
            expr,
            lambda child: self._materialize_window_operands(child, exprs, names),
        )

    def _rebuild_window(self, window, exprs, names):
        """Rebuild a WindowExpr with materialized operands: the function's
        arguments, PARTITION BY items, and ORDER BY keys all read stage-1
        columns; direction lists and the frame are preserved by model_copy."""
        from .expressions import map_children

        def rewrite(e):
            """Materialize one window operand against the stage-1 outputs."""
            return self._materialize_window_operands(e, exprs, names)

        partition = []
        for item in window.partition_by:
            partition.append(rewrite(item))
        order = []
        for key in window.order_keys:
            order.append(rewrite(key))
        return window.model_copy(
            update={
                "function": map_children(window.function, rewrite),
                "partition_by": partition,
                "order_keys": order,
            }
        )

    def _is_stage1_operand(self, expr) -> bool:
        """A leaf stage 1 must compute: a column, an aggregate call, or
        GROUPING() (an aggregate-scoped function the window stage cannot run)."""
        from .expressions import ColumnRef, FunctionCall

        if isinstance(expr, ColumnRef):
            return True
        return isinstance(expr, FunctionCall) and (
            expr.is_aggregate or expr.function_name.lower() == "grouping"
        )

    def _stage1_output_ref(self, expr, exprs, names):
        """The stage-1 output column computing ``expr``, appending a hidden
        output (``__w<n>``) when no existing output matches structurally."""
        from .expressions import ColumnRef

        for index, existing in enumerate(exprs):
            if existing == expr:
                # A bare reference to the stage-1 output that already computes
                # this operand, typed as the operand itself.
                return ColumnRef.create(
                    table=None, column=names[index], data_type=expr.get_type()
                )
        name = f"__w{len(names)}"
        exprs.append(expr)
        names.append(name)
        # A bare reference to the hidden stage-1 output just appended for this
        # operand, typed as the operand itself.
        return ColumnRef.create(table=None, column=name, data_type=expr.get_type())

    def _stage2_sql(self, items, stage1_names) -> str:
        """Render the stage-2 window SELECT over the stage-1 relation."""
        stage2_aliases: Dict[Tuple[Optional[str], str], str] = {}
        for name in stage1_names:
            stage2_aliases[(None, name)] = name
        resolver = MergeResolver(stage2_aliases)
        select_list = clauses.select_expressions_fragment(
            items, self.output_names, resolver, dialect="duckdb"
        )
        return f"SELECT {select_list} FROM {_MERGE_AGGREGATE_RELATION}"

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """This aggregate's OUTPUT columns, keyed by qualifier -> output name, so
        a parent resolves against what the aggregate produces. A group key that
        is a plain column keeps its source qualifier; every output is also
        exposed unqualified by its output name (a computed result has no source
        column). The aggregate's own type inference uses input.column_aliases()
        directly, so this no longer needs to pass the input columns through."""
        from .expressions import ColumnRef

        aliases: Dict[Tuple[Optional[str], str], str] = {}
        for expr, name in zip(self.aggregates, self.output_names):
            aliases[(None, name)] = name
            if isinstance(expr, ColumnRef):
                aliases[(expr.table, expr.column)] = name
        return aliases


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
    # The deliberate max-base OVER-estimate (largest interior base scan): the
    # orientation FLOOR used when no real output estimate exists, so a
    # fact-carrying remote can never look small on a missing estimate.
    estimated_rows: Optional[int] = None
    # The subtree root's own cost estimate - the rows this query actually
    # returns. Orientation and the reduction-usefulness gate prefer it.
    output_estimated_rows: Optional[int] = None
    # Source-catalog NDV per OUTPUT column that passes through unchanged from
    # an interior base scan (join keys do). The reduction uses it to refuse a
    # dynamic filter whose keys cover the probe column's whole value domain.
    column_ndv: Optional[Dict[str, int]] = None
    # A known output schema seeded at construction, used INSTEAD of probing the
    # source. Set only for the island that reads a shipped dimension: it
    # references a temp table that lives on the engine's pinned connection, so
    # the python-side probe cannot see it. None everywhere else.
    seeded_schema: Optional[pa.Schema] = None
    # Learned-stats group provenance {subject, columns} stamped on a SHIPPED
    # island whose materialized row count IS the pre-ship aggregate's group
    # count (only row-count-preserving wrappers above it). The subject is
    # computed from the PRE-SHIP plan so it matches the cost model's read key.
    group_observation: Optional[Dict[str, object]] = None

    @classmethod
    def create(
        cls,
        *,
        datasource: str,
        datasource_connection: Any,
        query_ast: Any,
        output_names: List[str],
        column_alias_map: Dict[Tuple[Optional[str], str], str],
        estimated_rows: Optional[int] = None,
        output_estimated_rows: Optional[int] = None,
        column_ndv: Optional[Dict[str, int]] = None,
        seeded_schema: Optional[pa.Schema] = None,
        group_observation: Optional[Dict[str, object]] = None,
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
            estimated_rows=estimated_rows,
            output_estimated_rows=output_estimated_rows,
            column_ndv=column_ndv,
            seeded_schema=seeded_schema,
            group_observation=group_observation,
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
        """Output schema, seeded when this island reads a shipped dimension.

        A shipped-dimension temp table lives only on the engine's pinned
        connection, so the python-side probe cannot see it; the shipping rule
        seeds the known schema here. Every other island still probes.
        """
        if self._schema is None:
            if self.seeded_schema is not None:
                self._schema = self.seeded_schema
            else:
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

    def schema(self) -> pa.Schema:
        return self.inputs[0].schema()

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        """A union's output is exactly its columns: one alias per output column,
        keyed by its output name.

        NOT the first branch's aliases - those also expose that branch's SOURCE
        column names (e.g. `('customer', 'c_customer_id')` alongside the
        `(None, 'customer_id')` output), two keys mapping to one physical column.
        Self-joining the union then renamed that column twice (right_customer_id
        and right_customer_id_1), so a parent's right_customer_id reference did
        not resolve (q04).
        """
        aliases: Dict[Tuple[Optional[str], str], str] = {}
        for name in self.schema().names:
            aliases[(None, name)] = name
        return aliases

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

    def _grouped_limit_sql(self, names: List[str]) -> str:
        """Render the ``ROW_NUMBER`` window and keep each key's first rows.

        Rendered by the Rust engine's grouped-limit fragment, which wraps this
        input in a CTE adding the row-order index column this SQL orders by.
        """
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

    def schema(self) -> pa.Schema:
        """A cross-source LATERAL is not representable in the Rust engine IR.

        The output types of the dependent join depend on running the LATERAL
        query, which the removed local coordinator did. The Rust engine has no
        native connector for this shape yet, so a plan containing this node fails
        loudly at IR build (UnsupportedIR) rather than producing a wrong answer;
        its schema is likewise not computable here.
        """
        raise NotImplementedError(
            "cross-source LATERAL is not supported by the Rust engine"
        )

    def estimated_cost(self) -> float:
        raise NotImplementedError("Cost estimation not yet implemented")

    def column_aliases(self) -> Dict[Tuple[Optional[str], str], str]:
        return {}

    def __repr__(self) -> str:
        return f"PhysicalLateralJoin({self.join_type.name}, base={self.base_name})"


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

    def build_explain_batch(self) -> pa.RecordBatch:
        """Render the plan as a single-column Arrow batch (no data execution).

        EXPLAIN is plan introspection: the executor renders it directly instead
        of running it through the Rust engine, so it never needs an execution
        engine of its own.
        """
        if self.format == ExplainFormat.JSON:
            return self._build_json_batch()
        return self._build_text_batch()

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
        rows = self._estimated_rows(node)
        detail = self._detail_for(node)
        header = f"{name} cost={cost} rows={rows}"
        if detail:
            header = f"{header} details={detail}"
        return header + self._defaulted_note(node)

    def _estimated_rows(self, node: PhysicalPlanNode) -> int:
        """The join-ordering estimate when the node carries one; -1 means the
        node was never cost-estimated."""
        value = getattr(node, "estimated_rows", None)
        if value is None:
            return -1
        return value

    def _defaulted_note(self, node: PhysicalPlanNode) -> str:
        """The defaulted-statistics marker: which estimates came from named
        defaults rather than real source statistics. Never silent."""
        defaults = getattr(node, "estimate_defaults", None)
        if not defaults:
            return ""
        return f" stats=defaulted[{', '.join(defaults)}]"

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
