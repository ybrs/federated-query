"""Serialize a physical plan into the fedqrs execution IR.

The Rust engine (``fedqrs``) executes the whole plan natively: it reads every
source over a native driver, runs joins/aggregates on DataFusion, computes and
injects semi-join filters in Rust, and returns only the final Arrow result.
This module walks a ``PhysicalPlanNode`` tree into the JSON IR that engine
consumes.

Column references are resolved to their physical Arrow column names HERE (in
Python), so the Rust side stays mechanical and never needs the engine's alias
model. Anything not yet supported raises ``UnsupportedIR`` rather than emitting
a plan that would silently produce wrong rows.
"""

import json
import os

from sqlglot import exp

from ..optimizer.estimate_defaults import useless_key_reduction

from ..plan.expressions import (
    BetweenExpression,
    BinaryOp,
    BinaryOpType,
    CaseExpr,
    Cast,
    ColumnRef,
    DataType,
    Extract,
    FunctionCall,
    InList,
    Literal,
    UnaryOp,
    UnaryOpType,
    expression_children,
)
from ..plan.logical import JoinType, SetOpKind
from ..optimizer.decorrelation import _replace_column_refs
from ..optimizer.estimate_defaults import larger_estimated_side, orientation_rows
from ..plan.physical import (
    PhysicalAliasedRelation,
    PhysicalCTEMergeQuery,
    PhysicalCTEScan,
    PhysicalFilter,
    PhysicalGroupedLimit,
    PhysicalHashAggregate,
    PhysicalLimit,
    PhysicalHashJoin,
    PhysicalNestedLoopJoin,
    PhysicalProjection,
    PhysicalRemoteQuery,
    PhysicalRemoteSetOp,
    PhysicalScan,
    PhysicalSetOperation,
    PhysicalShipment,
    PhysicalSingleRowGuard,
    PhysicalSort,
    PhysicalUnion,
    PhysicalValues,
    PhysicalWindow,
    _DYNAMIC_FILTER_MAX_KEYS,
    _GROUPED_LIMIT_INDEX_COL,
    _MERGE_GROUPED_LIMIT_RELATION,
    _physical_column_name,
)


class UnsupportedIR(Exception):
    """A physical node or expression the Rust IR does not yet represent."""


# Binary operators the Rust engine understands, keyed by the engine's enum.
_BINARY_OP_TOKENS = {
    BinaryOpType.ADD: "+",
    BinaryOpType.SUBTRACT: "-",
    BinaryOpType.MULTIPLY: "*",
    BinaryOpType.DIVIDE: "/",
    BinaryOpType.MODULO: "%",
    BinaryOpType.EQ: "=",
    BinaryOpType.NEQ: "!=",
    BinaryOpType.LT: "<",
    BinaryOpType.LTE: "<=",
    BinaryOpType.GT: ">",
    BinaryOpType.GTE: ">=",
    BinaryOpType.AND: "and",
    BinaryOpType.OR: "or",
    BinaryOpType.LIKE: "like",
    BinaryOpType.ILIKE: "ilike",
    BinaryOpType.NULL_SAFE_EQ: "is_not_distinct_from",
    BinaryOpType.NULL_SAFE_NEQ: "is_distinct_from",
}

# Typed-literal tags keyed by the value's exact Python type. `bool` is a
# subclass of `int`, so exact-type lookup keeps them distinct.
_LITERAL_KINDS = {bool: "bool", int: "int", float: "float", str: "str"}

# Unary ops the engine represents as an IR `unary` node, and as an `is_null` node.
_UNARY_OPS = {UnaryOpType.NOT: "not", UnaryOpType.NEGATE: "neg"}
_NULL_TESTS = {UnaryOpType.IS_NULL: False, UnaryOpType.IS_NOT_NULL: True}


# --- expression serialization -------------------------------------------------


def expr_to_ir(expr):
    """Serialize a source-side expression; columns keep their own qualifier."""
    return _serialize_expr(expr, _plain_column)


def _plain_column(col_ref):
    """A column node carrying the reference's own relation qualifier."""
    node = {"node": "column", "name": col_ref.column}
    if col_ref.table is not None:
        node["relation"] = col_ref.table
    return node


def _serialize_expr(expr, column_fn):
    """Serialize one expression via a type-dispatched serializer; `column_fn`
    renders each ColumnRef."""
    serializer = _EXPR_SERIALIZERS.get(type(expr))
    if serializer is None:
        raise UnsupportedIR(f"expression {type(expr).__name__} not supported in IR")
    return serializer(expr, column_fn)


def _serialize_column(expr, column_fn):
    """A column reference, rendered by the caller's column function."""
    return column_fn(expr)


def _serialize_literal(expr, column_fn):
    """A typed literal value."""
    return {"node": "literal", "value": _literal_value(expr.value)}


def _serialize_cast(expr, column_fn):
    """A CAST to the engine's Arrow type name (decimals become float64)."""
    arrow_type = _CAST_TYPES.get(expr.data_type)
    if arrow_type is None:
        raise UnsupportedIR(f"cast to {expr.target_type} not supported in IR")
    return {
        "node": "cast",
        "expr": _serialize_expr(expr.expr, column_fn),
        "to": arrow_type,
    }


def _serialize_in_list(expr, column_fn):
    """An IN-list membership test (NOT IN arrives wrapped in a NOT unary op)."""
    options = []
    for option in expr.options:
        options.append(_serialize_expr(option, column_fn))
    return {
        "node": "in_list",
        "expr": _serialize_expr(expr.value, column_fn),
        "list": options,
        "negated": False,
    }


def _serialize_case(expr, column_fn):
    """A searched CASE: a list of WHEN/THEN plus an optional ELSE."""
    whens = []
    for condition, result in expr.when_clauses:
        whens.append(
            {
                "when": _serialize_expr(condition, column_fn),
                "then": _serialize_expr(result, column_fn),
            }
        )
    case = {"node": "case", "whens": whens}
    if expr.else_result is not None:
        case["else"] = _serialize_expr(expr.else_result, column_fn)
    return case


def _serialize_extract(expr, column_fn):
    """EXTRACT(field FROM source) lowers to date_part('field', source)."""
    field = {"node": "literal", "value": {"lit": "str", "value": expr.field.lower()}}
    source = _serialize_expr(expr.source, column_fn)
    return {"node": "function", "name": "date_part", "args": [field, source]}


def _serialize_function(expr, column_fn):
    """A function call resolved by name in the engine's registry. Aggregate calls
    are allowed so an aggregate can sit inside a larger expression (e.g.
    `100 * sum(x) / sum(y)` in an aggregate select)."""
    args = []
    for arg in expr.args:
        args.append(_serialize_expr(arg, column_fn))
    return {"node": "function", "name": expr.function_name.lower(), "args": args}


def _serialize_between(expr, column_fn):
    """BETWEEN as (value >= lower) AND (value <= upper)."""
    value = _serialize_expr(expr.value, column_fn)
    lower = _serialize_expr(expr.lower, column_fn)
    upper = _serialize_expr(expr.upper, column_fn)
    ge = {"node": "binary", "op": ">=", "left": value, "right": lower}
    le = {"node": "binary", "op": "<=", "left": value, "right": upper}
    return {"node": "binary", "op": "and", "left": ge, "right": le}


def _serialize_unary(expr, column_fn):
    """Serialize a unary op: NOT / negate as `unary`, IS [NOT] NULL as `is_null`."""
    operand = _serialize_expr(expr.operand, column_fn)
    unary = _UNARY_OPS.get(expr.op)
    if unary is not None:
        return {"node": "unary", "op": unary, "operand": operand}
    negated = _NULL_TESTS.get(expr.op)
    if negated is None:
        raise UnsupportedIR(f"unary operator {expr.op} not supported in IR")
    return {"node": "is_null", "expr": operand, "negated": negated}


def _serialize_binary(expr, column_fn):
    """Serialize a binary operation, refusing operators the engine lacks."""
    token = _BINARY_OP_TOKENS.get(expr.op)
    if token is None:
        raise UnsupportedIR(f"binary operator {expr.op} not supported in IR")
    left = _serialize_expr(expr.left, column_fn)
    right = _serialize_expr(expr.right, column_fn)
    return {"node": "binary", "op": token, "left": left, "right": right}


def _literal_value(value):
    """Map a Python literal to the IR's typed-literal form."""
    if value is None:
        return {"lit": "null"}
    kind = _LITERAL_KINDS.get(type(value))
    if kind is None:
        raise UnsupportedIR(f"literal of type {type(value).__name__} not supported")
    return {"lit": kind, "value": value}


def _expr_over(expr, input_name, child_aliases):
    """Serialize an expression whose columns come from one input relation.

    Each ColumnRef resolves through `child_aliases` to the physical column name
    in that input's binding, tagged with `input_name` (e.g. `in_0`).
    """

    def column(col_ref):
        return {
            "node": "column",
            "relation": input_name,
            "name": _physical_column_name(col_ref, child_aliases),
        }

    return _serialize_expr(expr, column)


# Engine DataType -> Arrow type name for a CAST target. The engine treats
# decimals as float64 (PG numeric already arrives as float), matching the
# DuckDB path within float precision.
_CAST_TYPES = {
    DataType.INTEGER: "int32",
    DataType.BIGINT: "int64",
    DataType.FLOAT: "float32",
    DataType.DOUBLE: "float64",
    DataType.DECIMAL: "float64",
    DataType.VARCHAR: "utf8",
    DataType.TEXT: "utf8",
    DataType.BOOLEAN: "boolean",
    DataType.DATE: "date32",
}

# Expression type -> its serializer. Defined after the serializers it names.
_EXPR_SERIALIZERS = {
    ColumnRef: _serialize_column,
    Literal: _serialize_literal,
    BinaryOp: _serialize_binary,
    UnaryOp: _serialize_unary,
    Cast: _serialize_cast,
    InList: _serialize_in_list,
    CaseExpr: _serialize_case,
    Extract: _serialize_extract,
    FunctionCall: _serialize_function,
    BetweenExpression: _serialize_between,
}


# --- scan specs ---------------------------------------------------------------


def raw_scan_spec(node):
    """A scan spec carrying pre-rendered source SQL (Python's emitter).

    Used for a single-source subtree or any scan we do not inject a dynamic
    filter into; Rust runs the SQL natively.
    """
    if isinstance(node, PhysicalScan):
        return {"raw_sql": node._render_source_sql()}
    renderer = getattr(node, "_sql", None) or getattr(node, "_build_query", None)
    if renderer is None:
        raise UnsupportedIR(f"{type(node).__name__} does not render source SQL")
    return {"raw_sql": renderer()}


def structured_scan_spec(scan):
    """A structured scan spec for a plain single-table scan.

    Required when Rust must inject a dynamic ``col IN (...)`` filter: the scan
    must be a bare table read so the injected predicate composes correctly.
    """
    _reject_nonplain_scan(scan)
    spec = {"table": scan.table_name, "columns": list(scan.columns)}
    _add_scan_qualifiers(scan, spec)
    _add_scan_predicates(scan, spec)
    return spec


def _reject_nonplain_scan(scan):
    """Refuse a scan that already folds aggregation/ordering/limits."""
    if any((scan.aggregates, scan.group_by, scan.grouping_sets)):
        raise UnsupportedIR("cannot inject a dynamic filter into an aggregate scan")
    if any((scan.order_by_keys, scan.limit is not None, scan.offset)):
        raise UnsupportedIR(
            "cannot inject a dynamic filter into an ordered/limited scan"
        )


def _add_scan_qualifiers(scan, spec):
    """Copy the optional schema and alias onto the scan spec."""
    if scan.schema_name:
        spec["schema"] = scan.schema_name
    if scan.alias:
        spec["alias"] = scan.alias


def _add_scan_predicates(scan, spec):
    """Copy the optional filter and DISTINCT flag onto the scan spec."""
    if scan.filters is not None:
        spec["filter"] = expr_to_ir(scan.filters)
    if scan.distinct:
        spec["distinct"] = True


# --- plan walker --------------------------------------------------------------


class _Names:
    """Hands out unique binding and fragment names within one IR."""

    def __init__(self):
        """Start the binding and fragment counters at zero."""
        self._b = 0
        self._f = 0

    def binding(self):
        """Return the next unique binding name."""
        self._b += 1
        return f"b{self._b}"

    def fragment(self):
        """Return the next unique fragment name."""
        self._f += 1
        return f"f{self._f}"


class _Ctx:
    """Accumulates IR steps and fragments while walking a plan bottom-up."""

    def __init__(self):
        """Start with empty steps/fragments and fresh name counters."""
        self.steps = []
        self.fragments = {}
        self.names = _Names()
        # One binding per DISTINCT step content (source scans, key
        # collections, injected scans keyed by everything but the output
        # name): two plan branches reading the same data share one read
        # (q70 fetched the same reduced fact twice for its main rollup and
        # its ranked-states subquery).
        self.step_cache = {}
        # Bindings of base scans already emitted with an injected dynamic
        # filter, keyed by node id: when the probe of a reduction is a
        # composite (wrapper layers over a single-source base), the base is
        # injected first and its binding cached, then the wrappers emit
        # normally and reach the base through this cache instead of re-reading.
        self.injected = {}
        # CTE body bindings keyed by id(producer): a multi-referenced CTE is
        # emitted (and executed) ONCE; every later reference reuses the same
        # binding (the engine clones a shared binding until its last consumer).
        self.cte_bindings = {}
        # Base relation bindings keyed by id(scan node), recorded as scans
        # emit: a later reduction can collect its build keys from the base
        # relation that ORIGINATES the key column instead of forcing a lazy
        # join region into memory (see _collect_anchor).
        self.scan_bindings = {}
        # Per traced-base-id, the reduction candidate donating the FEWEST
        # keys (built by _injection_winners before emission): when several
        # joins trace to one base, first-emitted must not beat most
        # selective (q33 injected 90k address keys and left January's 31
        # date keys unused).
        self.injection_winners = {}
        # A winner's emitted keys, keyed by id(build node), so a winner
        # feeding several bases collects once.
        self.winner_keys = {}
        # Build-side bindings keyed by id(build node), so a winner's early
        # emission and its own join's emission share one read.
        self.build_bindings = {}
        # binding name -> catalog provenance for the learned-stats write path:
        # what this binding's measured output-row count MEANS (a base table's
        # row count, a column's NDV). The engine reports one number per binding;
        # this map assigns it meaning. See adaptive-catalog-plan.md.
        self.observations = {}


def build_ir(plan):
    """Serialize a physical plan root into the execution IR.

    Walks the tree bottom-up: each node emits its steps and yields a binding
    whose Arrow columns are named by that node's output schema, so a parent
    resolves its expressions against ``child.column_aliases()``. Unsupported
    shapes raise so we never emit a plan that could produce wrong rows.
    """
    ir, _ = build_ir_with_observations(plan)
    return ir


def build_ir_with_observations(plan):
    """Serialize a plan and ALSO return the observation provenance map (binding
    name -> what its measured row count means for the learned-stats catalog).

    Kept separate from build_ir so the provenance never enters the JSON sent to
    the engine (it is a python-side write-path concern), and so build_ir's many
    callers keep their single-value return.
    """
    ctx = _Ctx()
    ctx.injection_winners = _injection_winners(plan)
    binding = _emit(plan, ctx)
    ctx.steps.append({"op": "return", "input": binding})
    ir = {
        "outputs": _plan_outputs(plan),
        "steps": ctx.steps,
        "fragments": ctx.fragments,
    }
    return ir, ctx.observations


def _plan_outputs(plan):
    """The result column names, from output_names or the node schema."""
    names = getattr(plan, "output_names", None)
    if names:
        return list(names)
    return list(plan.schema().names)


def _emit(node, ctx):
    """Emit steps for `node`, returning the binding that holds its output."""
    cached = ctx.injected.get(id(node))
    if cached is not None:
        # This base scan was already emitted with an injected dynamic filter;
        # the wrappers above it read that reduced binding.
        return cached
    emitter = _NODE_EMITTERS.get(type(node))
    if emitter is None:
        raise UnsupportedIR(f"physical node {type(node).__name__} not supported yet")
    return emitter(node, ctx)


# A plain PostgreSQL scan at least this many estimated rows reads through the
# engine's ctid-partitioned parallel path: one connection is bottlenecked on a
# single PG backend's scan plus the ADBC driver's sequential decode (measured:
# customer SF1 wide read 136ms single stream vs 37.5ms at 8 partitions; the
# oracle's postgres_scanner does the same read in 76ms). Below this size the
# per-partition round trips cost more than they save.
PARALLEL_SCAN_MIN_ROWS = 50_000


def _emit_source(node, ctx):
    """A source scan run natively (structured+parallel when it qualifies)."""
    step = {
        "op": "source_scan",
        "datasource": node.datasource,
        "scan": _source_scan_spec(node),
        "binding": ctx.names.binding(),
    }
    binding = _emit_step_once(ctx, step, key=_scan_share_key(node, step))
    ctx.scan_bindings[id(node)] = binding
    _record_scan_observation(ctx, node, binding)
    return binding


def _record_scan_observation(ctx, node, binding):
    """Record that this scan's measured output rows are a table's BASE row count.
    Only an unfiltered, un-aggregated base scan qualifies: a filter makes the
    count a predicate selectivity and a pushed aggregate makes it a group count,
    both of which are v1.5 (they come from lazy merge fragments)."""
    if not isinstance(node, PhysicalScan):
        return
    if node.filters is not None or node.group_by or node.aggregates or node.grouping_sets:
        return
    ctx.observations[binding] = {
        "target": "table_rows",
        "datasource": node.datasource,
        "schema": node.schema_name,
        "table": node.table_name,
    }


def _scan_share_key(node, step):
    """A scan step's sharing identity, alias-neutral when possible.

    Two identical reads under different aliases render different SQL (the
    alias qualifies the WHERE columns), so the key renders a CLONE under a
    fixed alias - the alias inside a self-contained scan is invisible to
    consumers. q70's two date_dim reads differed only this way, which kept
    their key collections and injected fact reads apart. Falls back to the
    literal step identity when the shape does not canonicalize."""
    if not isinstance(node, PhysicalScan) or not _injectable_scan(node):
        return _step_cache_key(step)
    fields = {
        "op": step["op"],
        "datasource": node.datasource,
        "schema": node.schema_name,
        "table": node.table_name,
        "materialize": step.get("materialize", False),
        "sql": _canonical_scan_sql(node),
    }
    return json.dumps(fields, sort_keys=True)


def _canonical_scan_sql(node):
    """The scan rendered under a fixed alias, with its filter references
    requalified to match; used only as a sharing key."""
    filters = node.filters
    if filters is not None:
        mapping = {}
        for ref in _expression_refs(filters):
            mapping[(ref.table, ref.column)] = ref.model_copy(
                update={"table": "__cse__"}
            )
        filters = _replace_column_refs(filters, mapping)
    clone = node.model_copy(update={"alias": "__cse__", "filters": filters})
    return clone._render_source_sql()


def _expression_refs(expr):
    """Every ColumnRef in an expression tree."""
    refs = []
    if isinstance(expr, ColumnRef):
        refs.append(expr)
        return refs
    for child in expression_children(expr):
        refs.extend(_expression_refs(child))
    return refs


def _emit_step_once(ctx, step, key=None):
    """Append a producing step unless an identical one (same op and inputs,
    ignoring the output name) already ran; return the binding that holds
    the data either way. The engine's use counting handles the sharing.

    For structured scans the COLUMN LIST is excluded from the identity and
    widened to the union on a hit: two branches reading different columns
    of the same rows share one read (q70's rollup and its ranked-states
    subquery scan the same reduced fact), and consumers resolve their
    columns by name, so extra columns are invisible to them."""
    if key is None:
        key = _step_cache_key(step)
    cached = ctx.step_cache.get(key)
    if cached is not None:
        _widen_cached_columns(cached, step)
        return cached["binding"]
    ctx.steps.append(step)
    ctx.step_cache[key] = step
    return step["binding"]


def _step_cache_key(step) -> str:
    """A step's identity: every field except its output name and, for a
    structured scan spec, its mergeable column list."""
    key_fields = {}
    for field, value in step.items():
        if field == "binding":
            continue
        if field == "scan" and isinstance(value, dict) and "columns" in value:
            narrowed = dict(value)
            narrowed.pop("columns")
            key_fields[field] = narrowed
            continue
        key_fields[field] = value
    return json.dumps(key_fields, sort_keys=True)


def _widen_cached_columns(cached, step) -> None:
    """Widen an already-emitted scan's columns to cover a new consumer's."""
    cached_scan = cached.get("scan")
    new_scan = step.get("scan")
    if not isinstance(cached_scan, dict) or "columns" not in cached_scan:
        return
    for column in new_scan["columns"]:
        if column not in cached_scan["columns"]:
            cached_scan["columns"].append(column)


def _source_scan_spec(node):
    """The scan spec for a plain source read: a structured spec marked
    ``parallel`` for a big plain PostgreSQL scan (the engine reads it with
    ctid-partitioned parallel connections), pre-rendered SQL otherwise."""
    if not _parallel_scan_eligible(node):
        return raw_scan_spec(node)
    try:
        spec = structured_scan_spec(node)
    except UnsupportedIR:
        # A capability probe, not error hiding: a filter the expression IR
        # cannot represent reads over the raw-SQL single-stream path, which
        # is equally correct - only the parallel speedup is forfeited.
        return raw_scan_spec(node)
    spec["parallel"] = True
    return spec


def _parallel_scan_eligible(node) -> bool:
    """Whether a scan is a big, plain, partition-safe PostgreSQL table read."""
    if not _is_big_postgres_scan(node):
        return False
    return _partition_safe_scan(node)


def _is_big_postgres_scan(node) -> bool:
    """A PhysicalScan on a PostgreSQL source whose cost estimate clears the
    parallel threshold; an unestimated scan stays on the single-stream path."""
    from ..datasources.postgresql import PostgreSQLDataSource

    if not isinstance(node, PhysicalScan):
        return False
    if not isinstance(node.datasource_connection, PostgreSQLDataSource):
        return False
    rows = node.estimated_rows
    return rows is not None and rows >= PARALLEL_SCAN_MIN_ROWS


def _partition_safe_scan(node) -> bool:
    """Whether ctid-partitioned reads return exactly this scan's rows.
    DISTINCT and TABLESAMPLE are not partition-safe (per-partition DISTINCT
    keeps duplicates across partitions; sampling composes wrongly); aggregates,
    ordering, and limits are excluded by the plain-scan check."""
    if node.distinct or node.sample is not None:
        return False
    return _injectable_scan(node)


def _merge_step(ctx, fragment, inputs):
    """Append a merge step running `fragment` over `inputs`; return its binding."""
    result = ctx.names.binding()
    ctx.steps.append(
        {
            "op": "merge",
            "fragment": fragment,
            "inputs": inputs,
            "binding": result,
        }
    )
    return result


def _emit_projection(node, ctx):
    """A projection over its single input, as a `project` fragment.

    DISTINCT ON needs the source's ORDER BY semantics to pick the surviving row.
    A single-source DISTINCT ON is pushed whole to its source; one reaching here
    is cross-source, which the project fragment cannot honor, so it fails loudly
    rather than silently dropping the DISTINCT ON and returning wrong rows.
    """
    if node.distinct_on is not None:
        raise UnsupportedIR(
            "DISTINCT ON is only supported when the query pushes to a single source"
        )
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
    project = _projection_items(
        node.expressions, node.output_names, "in_0", aliases, node.input
    )
    fragment = ctx.names.fragment()
    # `distinct` MUST reach the engine: a plain project fragment for a
    # DISTINCT projection would silently return duplicate rows.
    ctx.fragments[fragment] = {
        "kind": "project",
        "project": project,
        "distinct": node.distinct,
    }
    return _merge_step(ctx, fragment, {"in_0": child})


def _projection_items(expressions, names, input_name, aliases, input_node):
    """Serialize each output expression aliased to its output name.

    A ``*`` column reference expands to one item per input column (keeping each
    column's own name), the same expansion the node's ``schema()`` performs, so
    an unexpanded ``SELECT *`` projection resolves against real columns.
    """
    items = []
    for expr, name in zip(expressions, names):
        if _is_star_column(expr):
            _append_star_items(items, input_node, input_name)
        else:
            items.append({"expr": _expr_over(expr, input_name, aliases), "alias": name})
    return items


def _is_star_column(expr):
    """True when the expression is the ``*`` wildcard column reference."""
    return isinstance(expr, ColumnRef) and expr.column == "*"


def _append_star_items(items, input_node, input_name):
    """Append one projection item per input column, keeping its own name."""
    for name in input_node.schema().names:
        items.append(_side_column(input_name, name, name))


def _emit_aggregate(node, ctx):
    """A GROUP BY (or GROUPING SETS) over its single input, as an `aggregate`
    fragment - or, when an output carries a window function (which the structured
    fragment cannot express), as a rendered `SELECT ... GROUP BY` raw_sql."""
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
    if node.has_window_output():
        if node.window_split_needed():
            # GROUPING() inside a window cannot run fused (DataFusion planner
            # gap): stage 1 materializes the window operands as aggregate
            # outputs, stage 2 runs the window over those columns.
            stage1_sql, stage2_sql = node.split_window_aggregate_sqls(aliases)
            grouped = _raw_sql_step(ctx, stage1_sql, {"in_0": child})
            return _raw_sql_step(ctx, stage2_sql, {"in_0": grouped})
        return _raw_sql_step(ctx, node._aggregate_sql(aliases), {"in_0": child})
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = _aggregate_fragment(node, aliases)
    return _merge_step(ctx, fragment, {"in_0": child})


def _aggregate_fragment(node, aliases):
    """Build the aggregate fragment: select list, group-by, optional grouping sets."""
    fragment = {
        "kind": "aggregate",
        "select": _aggregate_select(node.aggregates, node.output_names, aliases),
        "group_by": _serialize_group_by(node.group_by, aliases),
    }
    if node.grouping_sets:
        fragment["grouping_sets"] = _serialize_grouping_sets(
            node.grouping_sets, aliases
        )
    return fragment


def _serialize_grouping_sets(grouping_sets, aliases):
    """Serialize each grouping set's expressions over the single input."""
    result = []
    for group_set in grouping_sets:
        exprs = []
        for expr in group_set:
            exprs.append(_expr_over(expr, "in_0", aliases))
        result.append(exprs)
    return result


def _serialize_group_by(group_by, aliases):
    """Serialize each grouping key over the `in_0` input."""
    items = []
    for key in group_by:
        items.append(_expr_over(key, "in_0", aliases))
    return items


def _aggregate_select(aggregates, names, aliases):
    """Serialize the aggregate SELECT list (agg calls and grouping columns)."""
    items = []
    for expr, name in zip(aggregates, names):
        items.append(_aggregate_item(expr, name, aliases))
    return items


def _aggregate_item(expr, name, aliases):
    """One aggregate output: an aggregate call or a plain grouping expression."""
    if isinstance(expr, FunctionCall) and expr.is_aggregate:
        return {"agg": _agg_call(expr, aliases), "alias": name}
    return {"expr": _expr_over(expr, "in_0", aliases), "alias": name}


def _agg_call(fn, aliases):
    """Serialize an aggregate FunctionCall; `count(*)` is the star form, and an
    ordered-set aggregate carries its `WITHIN GROUP (ORDER BY ...)`."""
    star = _is_star_arg(fn.args)
    args = _agg_args(fn.args, aliases, star)
    call = {
        "func": fn.function_name,
        "distinct": fn.distinct,
        "star": star,
        "args": args,
    }
    if fn.within_group_key is not None:
        call["within_group"] = {
            "key": _expr_over(fn.within_group_key, "in_0", aliases),
            "desc": fn.within_group_desc,
        }
    return call


def _is_star_arg(args):
    """True when the single argument is the `*` wildcard column."""
    if len(args) != 1:
        return False
    first = args[0]
    return isinstance(first, ColumnRef) and first.column == "*"


def _agg_args(args, aliases, star):
    """Serialize aggregate arguments; the star form takes none."""
    if star:
        return []
    items = []
    for arg in args:
        items.append(_expr_over(arg, "in_0", aliases))
    return items


def _emit_passthrough(node, ctx):
    """A node that only re-qualifies its input's columns (e.g. a subquery alias):
    the data is unchanged, so emit the child and let column resolution handle it."""
    return _emit(node.input, ctx)


def _emit_filter(node, ctx):
    """A boolean filter over its single input, as a `filter` fragment."""
    child = _emit(node.input, ctx)
    predicate = _expr_over(node.predicate, "in_0", node.input.column_aliases())
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "filter", "predicate": predicate}
    return _merge_step(ctx, fragment, {"in_0": child})


def _emit_single_row_guard(node, ctx):
    """The scalar-subquery cardinality guard over its single input, as a
    `single_row_guard` fragment. The engine errors when the input holds more
    than one row (per distinct key tuple when keys are given), else passes the
    input through unchanged - dropping the guard would let the join above
    silently duplicate outer rows."""
    child = _emit(node.input, ctx)
    keys = []
    for key in node.keys:
        keys.append(_expr_over(key, "in_0", node.input.column_aliases()))
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "single_row_guard", "keys": keys}
    return _merge_step(ctx, fragment, {"in_0": child})


def _emit_cte_scan(node, ctx):
    """A CTE reference: the producer's body is emitted - and executed - ONCE.

    Every reference to the same producer reuses the first reference's binding
    (the engine clones a shared binding Arc-shallow until its last consumer,
    so nothing re-runs; TPC-DS q04 executed its 6-times-referenced 3-branch
    year_total body 18 times before this). An explicit CTE column list
    relabels the body's output columns to the declared names, once as well.
    """
    producer = node.producer
    binding = ctx.cte_bindings.get(id(producer))
    if binding is not None:
        return binding
    binding = _emit(producer.body, ctx)
    if producer.column_names:
        binding = _relabel_columns(binding, producer.body, producer.column_names, ctx)
    ctx.cte_bindings[id(producer)] = binding
    return binding


def _emit_shipment(node, ctx):
    """Ship a foreign relation into a target source, then run the island.

    Emits the body binding, then a `ship` step that materializes it as a temp
    table on `node.datasource`, then the child island (whose source_scan reads
    that temp table). The ORDER is load-bearing: the ship step must precede the
    island's scan so the table exists when the engine reads it. The engine
    routes every later read of that source through the pinned connection the
    ship created, and drops the temp table at query end.
    """
    body_binding = _emit(node.body, ctx)
    ctx.steps.append(
        {
            "op": "ship",
            "datasource": node.datasource,
            "input": body_binding,
            "table": node.table,
        }
    )
    return _emit(node.child, ctx)


def _relabel_columns(binding, body, names, ctx):
    """Project a binding's columns to new output names (a CTE column list),
    positionally: the i-th body column becomes the i-th declared name."""
    project = []
    for source, target in zip(body.schema().names, names):
        project.append(_side_column("in_0", source, target))
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "project", "project": project}
    result = ctx.names.binding()
    ctx.steps.append(
        {
            "op": "merge",
            "fragment": fragment,
            "inputs": {"in_0": binding},
            "binding": result,
        }
    )
    return result


def _emit_grouped_limit(node, ctx):
    """Keep at most `limit` rows per group. The node's SQL expects a synthetic
    row-index column (added by the merge engine to preserve input order for the
    final ORDER BY); our binding lacks it, so wrap the input in a CTE named for
    the merge relation that adds it, then run the node's rendered SQL."""
    child = _emit(node.input, ctx)
    node_sql = node._grouped_limit_sql(node.input.schema().names)
    sql = (
        f"WITH {_MERGE_GROUPED_LIMIT_RELATION} AS "
        f'(SELECT *, ROW_NUMBER() OVER () AS "{_GROUPED_LIMIT_INDEX_COL}" '
        "FROM __gl_input) " + node_sql
    )
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append(
        {
            "op": "merge",
            "fragment": fragment,
            "inputs": {"__gl_input": child},
            "binding": result,
        }
    )
    return result


def _emit_window(node, ctx):
    """A window-bearing projection: register the input as `in_window` and run the
    rendered `SELECT <exprs> OVER (...) FROM in_window` as a raw_sql fragment."""
    child = _emit(node.input, ctx)
    sql = node._window_sql(node.input.column_aliases())
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append(
        {
            "op": "merge",
            "fragment": fragment,
            "inputs": {"in_window": child},
            "binding": result,
        }
    )
    return result


def _raw_sql_step(ctx, sql, inputs):
    """Append a `raw_sql` fragment running `sql` over the registered `inputs`
    (each merge input is registered under its name in a fresh DataFusion
    context, which parses and executes the SQL); return its binding."""
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append(
        {
            "op": "merge",
            "fragment": fragment,
            "inputs": inputs,
            "binding": result,
        }
    )
    return result


def _emit_cte_merge(node, ctx):
    """A whole WITH/CTE rendered as SQL over named inputs: emit each input to a
    binding registered under its name, and run the SQL as a `raw_sql` fragment."""
    inputs = {}
    for name, subtree in node.inputs.items():
        inputs[name] = _emit(subtree, ctx)
    return _raw_sql_step(ctx, node.sql, inputs)


def _branch_inputs(ctx, branches):
    """Emit each set-operation branch to an `in_<i>` binding; return the inputs
    map and the per-branch `SELECT * FROM in_<i>` statements."""
    inputs = {}
    selects = []
    for index, child in enumerate(branches):
        name = "in_{0}".format(index)
        inputs[name] = _emit(child, ctx)
        selects.append("SELECT * FROM {0}".format(name))
    return inputs, selects


def _emit_union(node, ctx):
    """A cross-source UNION [ALL] run as a raw_sql fragment over its branches.

    Each branch is registered as in_0..in_N; DataFusion's UNION aligns by
    position and takes the FIRST branch's column names - exactly the node's
    output (its schema is inputs[0]). `distinct` picks UNION vs UNION ALL.
    """
    inputs, selects = _branch_inputs(ctx, node.inputs)
    keyword = "UNION" if node.distinct else "UNION ALL"
    return _raw_sql_step(ctx, (" " + keyword + " ").join(selects), inputs)


def _emit_set_operation(node, ctx):
    """A cross-source INTERSECT / EXCEPT [ALL] run as a raw_sql fragment over its
    two branches (in_0, in_1); `distinct` picks the plain vs the ALL form."""
    inputs, selects = _branch_inputs(ctx, [node.left, node.right])
    keyword = node.kind.value if node.distinct else node.kind.value + " ALL"
    return _raw_sql_step(ctx, (" " + keyword + " ").join(selects), inputs)


def _emit_limit(node, ctx):
    """A LIMIT/OFFSET over its single input, as a `limit` fragment."""
    child = _emit(node.input, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {
        "kind": "limit",
        "limit": node.limit,
        "offset": node.offset,
    }
    return _merge_step(ctx, fragment, {"in_0": child})


def _emit_values(node, ctx):
    """A VALUES relation of constant rows: render it as `SELECT * FROM (VALUES
    ...) AS v(names)` and run it as a `raw_sql` fragment (no inputs)."""
    sql = _render_values_sql(node.rows, node.output_names)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append(
        {"op": "merge", "fragment": fragment, "inputs": {}, "binding": result}
    )
    return result


def _render_values_sql(rows, names):
    """Render a constant VALUES relation to DataFusion-compatible SQL."""
    tuples = []
    for row in rows:
        tuples.append(exp.Tuple(expressions=_values_row(row)))
    columns = []
    for name in names:
        columns.append(exp.to_identifier(name))
    alias = exp.TableAlias(this=exp.to_identifier("v"), columns=columns)
    values = exp.Values(expressions=tuples, alias=alias)
    return exp.select("*").from_(values).sql()


def _values_row(row):
    """Convert one row of constant cells to sqlglot literal expressions."""
    cells = []
    for cell in row:
        cells.append(_values_cell(cell))
    return cells


def _values_cell(cell):
    """A single VALUES cell; only constant literals are supported."""
    if not isinstance(cell, Literal):
        raise UnsupportedIR(f"non-literal VALUES cell {type(cell).__name__}")
    return exp.convert(cell.value)


def _emit_sort(node, ctx):
    """An ORDER BY over its single input, as a `sort` fragment."""
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
    keys = _sort_keys(node, aliases)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "sort", "keys": keys}
    return _merge_step(ctx, fragment, {"in_0": child})


def _sort_keys(node, aliases):
    """Serialize each ORDER BY key with direction and NULL placement."""
    keys = []
    for index, expr in enumerate(node.sort_keys):
        keys.append(_sort_key(node, index, expr, aliases))
    return keys


def _sort_key(node, index, expr, aliases):
    """One sort key: expression, direction, and NULL placement."""
    ascending = node.ascending[index]
    nulls_first = _nulls_first(node.nulls_order, index, ascending)
    return {
        "expr": _expr_over(expr, "in_0", aliases),
        "ascending": ascending,
        "nulls_first": nulls_first,
    }


def _nulls_first(nulls_order, index, ascending):
    """NULL placement: explicit if given, else SQL default (DESC first)."""
    if not nulls_order:
        return not ascending
    setting = nulls_order[index]
    if setting is None:
        return not ascending
    return str(setting).upper() == "FIRST"


# JoinType enum -> IR join_kind string. Any type absent here is refused.
_JOIN_KINDS = {
    JoinType.INNER: "inner",
    JoinType.LEFT: "left",
    JoinType.RIGHT: "right",
    JoinType.FULL: "full",
    JoinType.SEMI: "semi",
    JoinType.ANTI: "anti",
}


def _emit_join(join, ctx):
    """Emit a hash join, reducing the probe by the build keys when possible.

    Semi-join reduction (read build, collect its distinct key, inject it into
    the probe's SQL) applies only to an INNER single-key join between two plain
    scans, and only when the cost model does not prove the filter useless;
    otherwise both children are emitted in full (recursively, so they may
    be any subtree) and joined. The join's canonical output columns are emitted
    so a parent can reference them.
    """
    kind = _join_kind(join)
    if _can_reduce(join) and _probe_base_resolvable(join) and _reduction_filters(join):
        left_binding, right_binding = _emit_reduced_join(join, ctx)
    else:
        left_binding = _emit(join.left, ctx)
        right_binding = _emit(join.right, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = _join_fragment(join, kind)
    return _merge_step(
        ctx, fragment, {"in_left": left_binding, "in_right": right_binding}
    )


def _join_kind(join):
    """The IR join_kind for a physical join, or raise if the type is unmapped."""
    kind = _JOIN_KINDS.get(join.join_type)
    if kind is None:
        raise UnsupportedIR(f"join type {join.join_type} not yet supported")
    return kind


def _can_reduce(join):
    """True when the semi-join reduction applies: INNER, single key, and at
    least one side is a plain scan the keys can be injected into. The OTHER
    side (the build) may be ANY subtree - it is materialized for the join
    anyway, so collecting its distinct keys costs nothing extra and the
    adaptive strategy in the engine (IN list / temp table / full-scan guard)
    decides how the keys reach the probe's source."""
    if len(join.left_keys) != 1 or len(join.right_keys) != 1:
        return False
    if join.join_type == JoinType.SEMI:
        # For a SEMI join the injected IN filter IS the join condition: keys
        # always come from the existential right side and pre-filter the
        # preserved left probe; the coordinator semi join stays for
        # exactness. ANTI must never inject (it would keep the wrong rows).
        return _probe_preference(join.left) > 0
    if join.join_type == JoinType.LEFT:
        # A LEFT join preserves every left row, so only the NULLABLE right side
        # may be reduced - by the preserved left's keys. Every left row keeps
        # all its matches (its key is in the injected set) and unmatched left
        # rows still get NULL, so the result is identical. The right may be a
        # composite (an aggregate subquery) whose injectable base survives.
        return _left_reducible(join)
    if join.join_type != JoinType.INNER:
        return False
    return _injection_rank(join.left, join.left_keys[0]) > 0 or (
        _injection_rank(join.right, join.right_keys[0]) > 0
    )


def _injection_rank(node, key_expr) -> int:
    """_probe_preference extended to composite probes: a subtree whose join
    key traces to injectable base scans ranks like a plain scan."""
    rank = _probe_preference(node)
    if rank > 0:
        return rank
    if _probe_injection_bases(node, key_expr) is not None:
        return 1
    return 0


def _left_reducible(join) -> bool:
    """True when a LEFT join's nullable right side can be key-reduced: a single
    equi key and a right probe whose injectable base preserves the key."""
    if len(join.left_keys) != 1 or len(join.right_keys) != 1:
        return False
    inject_col = _physical_column_name(join.right_keys[0], join.right.column_aliases())
    return _reducible_probe_base(join.right, inject_col) is not None


def _probe_base_resolvable(join) -> bool:
    """True when the oriented probe descends to a single injectable base for the
    inject column - which the reduced-join emission requires.

    _can_reduce accepts an INNER join on probe-preference alone, but the cost-
    oriented probe can be the LARGER side, and that side may itself be a join
    (a fact already joined to another dim, as in q96) with no single base. The
    emission would then find no base and crash on None.datasource; gate on this
    so the join instead emits as a normal (unreduced) join.
    """
    _, probe, _, probe_key = _orient_join(join)
    return _probe_injection_bases(probe, probe_key) is not None


def _reduction_filters(join) -> bool:
    """The cost-based usefulness gate: False when the planned reduction
    provably filters nothing - the build side's distinct keys cover the probe
    column's whole value domain (an unfiltered dimension's keys are the FK
    domain, so the injected IN keeps every probe row and costs pure overhead).
    Decided from the cost model's NDVs threaded onto the plan; when either
    side's statistic is missing this keeps today's reduce-by-default."""
    build, probe, build_key, probe_key = _orient_join(join)
    if _build_donates_whole_domain(build):
        return False
    bases = _probe_injection_bases(probe, probe_key)
    probe_ndv = None
    if bases:
        # The fetched-fraction price is keyed to the base actually injected;
        # multi-base probes price by their first base (same key domain).
        base, inject_col = bases[0]
        probe_ndv = _node_column_ndv(base, inject_col)
    keys_ndv = _node_column_ndv(build, _build_key_name(build, build_key))
    build_rows = _output_rows(build)
    if isinstance(build, PhysicalRemoteQuery) and build_rows is None:
        # A remote's column_ndv is the unfiltered base domain; without its
        # real output estimate there is no evidence about its actual key set.
        return True
    return not useless_key_reduction(keys_ndv, build_rows, probe_ndv)


def _build_donates_whole_domain(build) -> bool:
    """An UNFILTERED plain base scan donates its entire key domain, so under
    FK containment the injection keeps every probe row and costs pure
    overhead (q39's whole-warehouse and q06's every-item injections). This
    structural check needs no statistics; filtered scans and composite
    builds fall through to the NDV pricing."""
    if not isinstance(build, PhysicalScan):
        return False
    if build.filters is not None or build.sample is not None:
        return False
    return not any((build.aggregates, build.group_by, build.grouping_sets))


def _output_rows(node):
    """The rows a node actually produces: a remote query's real output
    estimate (its estimated_rows is the deliberate max-base orientation
    floor, and its column_ndv the unfiltered base domain - judging keys by
    those killed q11's useful 400-key island reduction), any other node's
    threaded estimate. None abstains."""
    if isinstance(node, PhysicalRemoteQuery):
        return node.output_estimated_rows
    return getattr(node, "estimated_rows", None)


def _build_key_name(build, build_key_expr) -> str:
    """The physical output name of the build side's join-key column."""
    return _physical_column_name(build_key_expr, build.column_aliases())


def _node_column_ndv(node, column):
    """A node's cost-model NDV for one of its output columns, or None when the
    node carries no threaded statistics (a computed subtree, an unestimated
    scan) - the gate then abstains rather than guessing."""
    ndv_map = getattr(node, "column_ndv", None)
    if not ndv_map:
        return None
    return ndv_map.get(column)


def _reducible_probe_base(probe, inject_col):
    """The single-source base scan/remote a probe reduces to - descending
    through pure single-input wrappers - IF the inject column survives
    unchanged to that base; else None (the reduction would mis-target)."""
    node = probe
    while not _is_probe_base(node):
        node = _single_wrapper_input(node)
        if node is None:
            return None
    if inject_col in _base_output_names(node):
        return node
    return None


def _is_probe_base(node) -> bool:
    """A node the engine can inject a dynamic filter into directly: a plain or
    aggregate scan (wrapped as a derived table) or a pushed remote query."""
    return isinstance(node, (PhysicalScan, PhysicalRemoteQuery))


def _single_wrapper_input(node):
    """The sole input of a column-passthrough wrapper (alias/projection), or
    None for anything that reshapes or combines rows."""
    if isinstance(node, (PhysicalAliasedRelation, PhysicalProjection)):
        return node.input
    return None


def _base_output_names(node):
    """The output column names of a probe base."""
    names = set()
    for name in node.column_aliases().values():
        names.add(name)
    return names


def _injectable_scan(node):
    """A plain single-table scan the engine can inject ``col IN (...)`` into
    (mirrors _reject_nonplain_scan, so an emitted spec can never raise)."""
    if not isinstance(node, PhysicalScan):
        return False
    if any((node.aggregates, node.group_by, node.grouping_sets)):
        return False
    return not any((node.order_by_keys, node.limit is not None, node.offset))


def _probe_preference(node) -> int:
    """Rank a node as the injection target: a pushed remote query first (it
    is usually the collapsed fact island - the big side the reduction exists
    to cut; the engine wraps its SQL as a derived table and filters its
    output columns), then a plain scan, then nothing."""
    if isinstance(node, PhysicalRemoteQuery):
        return 2
    if _injectable_scan(node):
        return 1
    return 0


def _probe_injection_bases(probe, key_expr):
    """[(base, inject column)] for a probe: the legacy single-base resolution
    (which also covers aggregate bases) first, else the traced descent
    through join cascades and decorrelation wrappers. None = untraceable.

    The traced path is why the SF10 outlier cluster existed: only plain-scan
    probes were injectable, so a fact under a join cascade (q39: (inventory
    x item) x date_dim), a decorrelated wrapper (q58, q06) or a union (q05)
    shipped WHOLE across the wire while the filtered dimension received a
    useless injection."""
    inject_col = _physical_column_name(key_expr, probe.column_aliases())
    base = _reducible_probe_base(probe, inject_col)
    if base is not None:
        return [(base, inject_col)]
    return _traced_injection_bases(probe, (key_expr.table, key_expr.column))


def _traced_injection_bases(node, key_pair):
    """The base scan(s) that ORIGINATE the probe key column, descending only
    where removing base rows lacking the keys cannot change what the
    reduction join above sees. `key_pair` is (qualifier, column) AT THIS
    NODE'S OUTPUT and is translated through renames on the way down."""
    if isinstance(node, (PhysicalScan, PhysicalRemoteQuery)):
        return _traced_base_entry(node, key_pair)
    if isinstance(node, PhysicalAliasedRelation):
        inner = _alias_inner_pair(node, key_pair)
        return None if inner is None else _traced_injection_bases(node.input, inner)
    if isinstance(node, PhysicalProjection):
        inner = _projection_inner_pair(node, key_pair)
        return None if inner is None else _traced_injection_bases(node.input, inner)
    if isinstance(node, PhysicalFilter):
        # A filter passes columns through untouched; filtering its input by a
        # key superset commutes with it.
        return _traced_injection_bases(node.input, key_pair)
    if isinstance(node, PhysicalHashJoin):
        return _join_injection_bases(node, key_pair)
    if isinstance(node, PhysicalUnion):
        return _union_injection_bases(node, key_pair)
    if isinstance(node, PhysicalSetOperation):
        return _setop_injection_bases(node, key_pair)
    return None


def _setop_injection_bases(setop, key_pair):
    """Descend into both branches of a binary UNION, exactly as the n-ary union
    case does (q54's catalog UNION ALL web sits in a SetOperation, not a
    PhysicalUnion). INTERSECT/EXCEPT are not traced: filtering an EXCEPT's right
    branch could ADD result rows, and an intersect only reduces via its smaller
    side, so neither reduces safely by a key superset here. A branch that does
    not trace stays unfiltered - the reduction join above still drops keyless
    rows exactly; None only when NEITHER branch traces."""
    if setop.kind != SetOpKind.UNION or key_pair not in setop.column_aliases():
        return None
    position = setop.schema().names.index(key_pair[1])
    bases = []
    for branch in (setop.left, setop.right):
        pair = _branch_key_pair(branch, position)
        if pair is None:
            continue
        traced = _traced_injection_bases(branch, pair)
        if traced:
            bases.extend(traced)
    return bases or None


def _union_injection_bases(union, key_pair):
    """Descend into every union branch that traces (q05's probe is a UNION
    ALL of sales and returns scans per channel). Branches that do not trace
    stay unfiltered - correct either way, because the reduction join above
    still drops keyless union rows exactly; None only when NO branch traces
    (nothing would be reduced). Filtering a branch by a key superset
    commutes with UNION's dedup too: the key is part of every row, so the
    removed rows are whole dedup groups the join above drops anyway."""
    if key_pair not in union.column_aliases():
        return None
    position = union.schema().names.index(key_pair[1])
    bases = []
    for branch in union.inputs:
        pair = _branch_key_pair(branch, position)
        if pair is None:
            continue
        traced = _traced_injection_bases(branch, pair)
        if traced:
            bases.extend(traced)
    return bases or None


def _branch_key_pair(branch, position):
    """The (qualifier, column) pair naming a branch's output column at a
    union position, or None when hidden or ambiguous."""
    physical = branch.schema().names[position]
    matches = []
    for pair, name in branch.column_aliases().items():
        if name == physical:
            matches.append(pair)
    if len(matches) == 1:
        return matches[0]
    return _branch_identity_pair(matches, physical)


def _branch_identity_pair(matches, physical):
    """Resolve an ambiguous branch column. A renaming projection exposes BOTH
    its output-alias identity ``(None, out)`` and the base passthrough
    ``(base, col)`` under the same physical name; prefer the output-alias pair
    (its column IS the physical name), which the downstream projection tracer
    resolves through its output names. None when that is not unique either."""
    identity = []
    for pair in matches:
        if pair[1] == physical:
            identity.append(pair)
    return identity[0] if len(identity) == 1 else None


def _traced_base_entry(base, key_pair):
    """[(base, physical column)] when a PLAIN injectable base exposes the
    key; aggregate/limited scans only reduce via the legacy vetted shape."""
    if isinstance(base, PhysicalScan) and not _injectable_scan(base):
        return None
    physical = base.column_aliases().get(key_pair)
    if physical is None:
        return None
    return [(base, physical)]


def _alias_inner_pair(node, key_pair):
    """Translate the key through an alias wrapper: the single input column
    the aliased output re-exposes, or None when hidden or ambiguous."""
    physical = node.column_aliases().get(key_pair)
    if physical is None:
        return None
    matches = []
    for pair, name in node.input.column_aliases().items():
        if name == physical:
            matches.append(pair)
    if len(matches) != 1:
        return None
    return matches[0]


def _projection_inner_pair(node, key_pair):
    """Translate the key through a projection: the input column the key
    output re-exposes, or None (computed expression, ambiguous name, or
    DISTINCT ON - which picks a survivor per group, so removing input rows
    can change which row a kept group keeps)."""
    if node.distinct_on:
        return None
    if key_pair not in node.column_aliases():
        return None
    matches = []
    for i, name in enumerate(node.output_names):
        if name == key_pair[1]:
            matches.append(node.expressions[i])
    if len(matches) != 1:
        return None
    expr = matches[0]
    if not isinstance(expr, ColumnRef) or expr.column == "*":
        return None
    return (expr.table, expr.column)


def _join_injection_bases(join, key_pair):
    """Descend into the join side that carries the key, when that side is not
    null-extended by this join: a removed keyless row then only removes
    output rows whose key the reduction join above drops anyway. A
    null-extended side is unsafe - removing its rows turns matches into
    NULL extensions, changing kept rows' contents."""
    sides = []
    if key_pair in join.left.column_aliases():
        sides.append("left")
    if key_pair in join.right.column_aliases():
        sides.append("right")
    if len(sides) != 1:
        return None
    side = sides[0]
    if not _injection_side_safe(join.join_type, side):
        return None
    child = join.left if side == "left" else join.right
    return _traced_injection_bases(child, key_pair)


def _injection_side_safe(join_type, side) -> bool:
    """Whether a join side is preserved (not null-extended) so its base may
    be pre-filtered by a key superset. FULL extends both sides: never."""
    if join_type == JoinType.INNER:
        return True
    if join_type == JoinType.LEFT:
        return side == "left"
    if join_type == JoinType.RIGHT:
        return side == "right"
    if join_type in (JoinType.SEMI, JoinType.ANTI):
        return side == "left"
    return False


def _emit_reduced_join(join, ctx):
    """The build/collect/inject reduction path; returns (left, right) bindings."""
    build_child, probe_child, build_key_expr, probe_key_expr = _orient_join(join)
    build_binding, probe_binding = _emit_reduced_inputs(
        ctx, build_child, probe_child, build_key_expr, probe_key_expr
    )
    left_binding = build_binding if build_child is join.left else probe_binding
    right_binding = build_binding if build_child is join.right else probe_binding
    return left_binding, right_binding


def _orient_join(join):
    """Pick build/probe sides and their join keys.

    A SEMI join's sides have fixed roles: the existential right side donates
    keys and the preserved left side is the probe. For INNER joins: when both
    sides are plain scans the planner's build_side choice stands; otherwise
    the side ranking higher as an injection target is the PROBE (the
    reduction exists to cut the big read) and the other donates keys."""
    if join.join_type == JoinType.SEMI:
        return join.right, join.left, join.right_keys[0], join.left_keys[0]
    if join.join_type == JoinType.LEFT:
        # Fixed roles: the preserved left donates keys, the nullable right is
        # the reduced probe (reducing the left would drop preserved rows).
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    # Cost-based: reduce the LARGER side (the cost model's estimate reaches
    # here via estimated_rows), instead of the structural remote>scan guess
    # that mis-sizes a small dim collapsed into a remote query.
    probe = _cardinality_probe(join)
    if probe is join.right:
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    if probe is join.left:
        return join.right, join.left, join.right_keys[0], join.left_keys[0]
    if _injectable_scan(join.left) and _injectable_scan(join.right):
        if join.build_side == "left":
            return join.left, join.right, join.left_keys[0], join.right_keys[0]
        return join.right, join.left, join.right_keys[0], join.left_keys[0]
    left_rank = _injection_rank(join.left, join.left_keys[0])
    right_rank = _injection_rank(join.right, join.right_keys[0])
    if left_rank == right_rank:
        # Rank tie (e.g. both sides trace): probe the side with the larger
        # KNOWN estimate - the reduction exists to cut the big read, and an
        # unknown side cannot be known big (q58: fact-x-item est 28.8M vs
        # date_dim-x-subquery est None flipped to the useless direction).
        left_urgency = _side_urgency(join.left, join.left_keys[0])
        if left_urgency > _side_urgency(join.right, join.right_keys[0]):
            return join.right, join.left, join.right_keys[0], join.left_keys[0]
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    if right_rank > left_rank:
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    return join.right, join.left, join.right_keys[0], join.left_keys[0]


def _known_rows(node) -> int:
    """estimated_rows when threaded, else -1: an unknown side cannot be
    known big, so a known-big side wins the probe role.

    A remote query with no threaded output estimate still exposes its base
    domains: the widest column NDV is its size floor (an island exposing a
    1.5M-NDV order key cannot be small - TPC-H q21's lineitem self-join
    island lost its estimate and a 10k-supplier island out-sized it,
    flipping the reduction backwards)."""
    rows = getattr(node, "estimated_rows", None)
    if rows is None and isinstance(node, PhysicalRemoteQuery):
        ndv_map = getattr(node, "column_ndv", None)
        if ndv_map:
            rows = max(ndv_map.values())
    return -1 if rows is None else rows


def _probe_urgency(node) -> int:
    """How much a side needs to be the reduced probe: its known size floor,
    or effectively infinite for a remote island carrying NO statistics at
    all - the cost model could not see through it, and an unjudgeable
    island read whole is exactly the risk the reduction exists to remove
    (TPC-H q21's lineitem self-join island has no stats; the judgeable
    10k-supplier island must not out-rank it)."""
    rows = _known_rows(node)
    if rows < 0 and isinstance(node, PhysicalRemoteQuery):
        return 1 << 62
    return rows


def _side_urgency(node, key_expr) -> int:
    """A join side's probe urgency, derived from the node itself or - when
    the node is an unjudgeable composite - from the base relations its key
    traces to: a union of statless fact islands must out-rank a filtered
    dimension scan (q05's channel unions), and a composite over a known-big
    fact scan carries that fact's size (q58)."""
    urgency = _probe_urgency(node)
    if urgency >= 0:
        return urgency
    bases = _probe_injection_bases(node, key_expr)
    if not bases:
        return urgency
    best = urgency
    for base, _ in bases:
        candidate = _probe_urgency(base)
        if candidate > best:
            best = candidate
    return best


def _passthrough_input(node):
    """The single input a SIZE-PRESERVING wrapper exposes unchanged (a derived
    table, a filter, or a non-DISTINCT projection), or None. A DISTINCT
    projection collapses rows, so it is not size-preserving."""
    if isinstance(node, (PhysicalAliasedRelation, PhysicalFilter)):
        return node.input
    if isinstance(node, PhysicalProjection) and not node.distinct and not node.distinct_on:
        return node.input
    return None


def _derived_side_rows(node):
    """A join side's size for orientation when its own estimate is absent:
    descend through size-preserving wrappers and sum a union's branches down to
    the leaf scans (every scan now carries an estimate). Abstains (None) on a
    collapsing node - an aggregate or DISTINCT - whose output size the input
    rows do not predict, so orientation keeps its rank-based fallback there
    rather than trust an over-estimate."""
    direct = orientation_rows(node)
    if direct is not None:
        return direct
    passthrough = _passthrough_input(node)
    if passthrough is not None:
        return _derived_side_rows(passthrough)
    if isinstance(node, (PhysicalUnion, PhysicalSetOperation)):
        return _summed_side_rows(node)
    return None


def _summed_side_rows(node):
    """The summed derived size of a union's branches, or None if any branch is
    unjudgeable (a partial sum would understate the side and mis-orient)."""
    total = 0
    for child in node.children():
        rows = _derived_side_rows(child)
        if rows is None:
            return None
        total += rows
    return total


def _larger_derived_side(left, right):
    """The bigger of two join sides by derived size, or None when either is
    unjudgeable or they tie - the same abstention contract as
    larger_estimated_side, but seeing through derived-table / union wrappers."""
    left_rows = _derived_side_rows(left)
    right_rows = _derived_side_rows(right)
    if left_rows is None or right_rows is None or left_rows == right_rows:
        return None
    return right if right_rows > left_rows else left


def _cardinality_probe(join):
    """The larger injectable side to reduce, or None to fall back.

    Uses the cost estimate threaded onto both children, seen THROUGH derived-
    table and union wrappers (a big fact wrapped in a derived table still
    out-sizes a small dimension - q54's 21.6M sales union vs a 30-row date
    filter). Declines (None) when either estimate is missing, they tie (both
    likely defaulted), or the larger side is not an injectable probe -
    guaranteeing the same reduction set as the structural heuristic, just
    better-oriented when sizes differ."""
    larger = _larger_derived_side(join.left, join.right)
    if larger is None:
        return None
    key_expr = join.left_keys[0] if larger is join.left else join.right_keys[0]
    if _injection_rank(larger, key_expr) > 0:
        return larger
    return None


def _emit_reduced_inputs(ctx, build_child, probe_child, build_key_expr, probe_key_expr):
    """Emit the build side, its distinct keys, and the injected probe scan.

    The keys are collected from the base relation that ORIGINATES the build
    key column when possible (a superset of the joined build side's keys -
    the injection is only a reduction, so a superset is always correct); the
    build subtree's own binding then stays LAZY and its region keeps
    streaming. Only when the key column is not traceable to an emitted base
    (renamed through projections) does the collection read the build binding
    itself, forcing it.
    """
    build_binding = _emit_build_side(ctx, build_child)
    keys_binding, anchor_key = _emit_keys_for(
        ctx, build_child, build_binding, build_key_expr
    )
    reduction = {
        "build": build_child,
        "keys_binding": keys_binding,
        "build_key": anchor_key,
    }
    probe_binding = _emit_probe(ctx, probe_child, probe_key_expr, reduction)
    return build_binding, probe_binding


def _emit_keys_for(ctx, build_child, build_binding, build_key_expr):
    """(keys binding, key name) of a build side's distinct join keys,
    anchored to the originating base scan when traceable."""
    anchor = _collect_anchor(ctx, build_child, build_key_expr)
    if anchor is None:
        anchor_binding = build_binding
        anchor_key = _physical_column_name(build_key_expr, build_child.column_aliases())
    else:
        anchor_binding, anchor_key = anchor
    keys_binding = _emit_collect_distinct(
        ctx, anchor_binding, anchor_key, ctx.names.binding()
    )
    _record_ndv_observation(ctx, build_child, build_key_expr, keys_binding)
    return keys_binding, anchor_key


def _record_ndv_observation(ctx, build_child, key_expr, keys_binding):
    """Record that a collect_distinct's measured row count is a base COLUMN's
    NDV - only when the collected key IS a column of an UNREDUCED base scan, so
    the distinct count is that column's real domain, not a joined-or-reduced
    cardinality."""
    base = _ndv_base_scan(ctx, build_child, key_expr)
    if base is None:
        return
    ctx.observations[keys_binding] = {
        "target": "column_ndv",
        "datasource": base.datasource,
        "schema": base.schema_name,
        "table": base.table_name,
        "column": key_expr.column,
    }


def _ndv_base_scan(ctx, build_child, key_expr):
    """The base PhysicalScan whose real column the collected key IS, or None. A
    scan carrying a filter/aggregate, or one REDUCED by an injected dynamic
    filter (its collected distinct count is over the reduced rows, not the base
    column's domain), is excluded."""
    if isinstance(build_child, PhysicalScan):
        scan = build_child
    else:
        scan = _originating_relation(build_child, key_expr)
    if not isinstance(scan, PhysicalScan):
        return None
    if scan.filters is not None or scan.group_by or scan.aggregates:
        return None
    if scan.dynamic_filter_keys or id(scan) in ctx.injected:
        return None
    if (key_expr.table, key_expr.column) not in scan.column_aliases():
        return None
    return scan


def _collect_anchor(ctx, build_child, build_key_expr):
    """(binding, physical column) of the base relation originating the build
    key, or None when the build side IS a base relation already (its binding
    serves directly) or the key is not traceable to an emitted base."""
    if isinstance(build_child, (PhysicalScan, PhysicalRemoteQuery)):
        return None
    scan = _originating_relation(build_child, build_key_expr)
    if scan is None:
        return None
    binding = ctx.scan_bindings.get(id(scan))
    if binding is None:
        return None
    return binding, _physical_column_name(build_key_expr, scan.column_aliases())


def _originating_relation(node, key_expr):
    """The base scan / remote relation exposing the key column under the
    key's own qualifier, or None when renames hide it. A relation alias is
    unique within a query, so the first match is the only match."""
    if isinstance(node, (PhysicalScan, PhysicalRemoteQuery)):
        if (key_expr.table, key_expr.column) in node.column_aliases():
            return node
        return None
    for child in node.children():
        found = _originating_relation(child, key_expr)
        if found is not None:
            return found
    return None


def _emit_probe(ctx, probe_child, probe_key_expr, reduction):
    """Emit the probe with the build keys injected into its base scan(s).

    A plain scan / remote probe is injected directly. A composite probe (a
    join cascade, an aggregate subquery, decorrelation wrappers) has the
    keys injected into every base scan the key traces to; each base binding
    is cached so the wrappers - emitted normally as coordinator fragments -
    read the reduced bases. Per base, the PLANNED WINNER's keys (fewest
    donated) inject rather than this join's own when they differ."""
    bases = _probe_injection_bases(probe_child, probe_key_expr)
    if bases is None:
        # _probe_base_resolvable gated on this before choosing the reduced
        # path; reaching here means the gate and the emission disagree.
        raise UnsupportedIR("reduced join probe lost its injection base")
    for base, inject_col in bases:
        cached = ctx.injected.get(id(base))
        if cached is not None:
            # This base is already injected; reading the fact a SECOND time
            # for another filter costs more than it saves (q39 shipped
            # inventory twice). The join above still enforces its keys
            # exactly - injection is only a reduction.
            if base is probe_child:
                return cached
            continue
        base_binding = _emit_base_injection(ctx, base, inject_col, reduction)
        ctx.injected[id(base)] = base_binding
        if base is probe_child:
            return base_binding
    return _emit(probe_child, ctx)


def _emit_base_injection(ctx, base, inject_col, reduction):
    """One injected read of a base: the most selective planned candidate is
    the PRIMARY injection (full delivery-strategy machinery), and every
    further candidate's keys ride along as bounded extra IN lists on the
    same read (q46's store AND date keys now both apply)."""
    candidates = ctx.injection_winners.get(id(base), [])
    keys_binding, build_key = reduction["keys_binding"], reduction["build_key"]
    column = inject_col
    if candidates and candidates[0]["build"] is not reduction["build"]:
        keys_binding, build_key = _winner_keys(ctx, candidates[0])
        column = candidates[0]["columns"][id(base)]
    extras = _extra_injections(ctx, base, column, candidates)
    return _emit_injected_scan(
        ctx,
        base,
        column,
        keys_binding,
        ctx.names.binding(),
        build_key,
        extras=extras,
    )


def _extra_injections(ctx, base, primary_column, candidates):
    """The runner-up candidates as extra (column, keys) injections, capped
    at two - each is a bonus filter, and past the best few the selectivity
    gains vanish while the IN lists lengthen the SQL."""
    extras = []
    for candidate in candidates[1:]:
        if len(extras) == 2:
            break
        column = candidate["columns"][id(base)]
        if column == primary_column:
            # A second key set on the SAME column adds nothing: the primary
            # already restricts it, and the join enforces exactness.
            continue
        keys_binding, _ = _winner_keys(ctx, candidate)
        extras.append({"column": column, "keys_from": keys_binding})
    return extras


def _winner_keys(ctx, winner):
    """The winning candidate's distinct-keys binding, emitted once."""
    cached = ctx.winner_keys.get(id(winner["build"]))
    if cached is not None:
        return cached
    build_binding = _emit_build_side(ctx, winner["build"])
    keys = _emit_keys_for(ctx, winner["build"], build_binding, winner["build_key"])
    ctx.winner_keys[id(winner["build"])] = keys
    return keys


def _injection_winners(plan):
    """Per traced-base-id, the reduction candidate donating the fewest keys.

    Emission is bottom-up, so without this plan-level pass the INNERMOST
    join's injection claims a shared base first regardless of selectivity
    (q33: 90k address keys won over January's 31 date keys and the fact
    shipped whole). Candidates whose build subtree CONTAINS the base are
    skipped - their keys cannot exist before the base is read."""
    winners = {}
    for node in _walk_plan_nodes(plan):
        if not isinstance(node, PhysicalHashJoin):
            continue
        if not _reduction_applies(node):
            continue
        _note_candidate(winners, node)
    return winners


def _walk_plan_nodes(node):
    """Yield every node of a physical plan tree."""
    yield node
    for child in node.children():
        yield from _walk_plan_nodes(child)


def _reduction_applies(join) -> bool:
    """Whether the emitter will take the reduced path for this join."""
    if not _can_reduce(join) or not _probe_base_resolvable(join):
        return False
    return _reduction_filters(join)


def _note_candidate(winners, join):
    """Record this join's reduction against every base it traces to. All
    candidates are kept, ordered by build output (fewest donated keys
    first): the best one becomes the PRIMARY injection, the rest ride
    along as bounded extra IN lists on the same read."""
    build, probe, build_key, probe_key = _orient_join(join)
    bases = _probe_injection_bases(probe, probe_key)
    if bases is None:
        return
    columns = {}
    for base, inject_col in bases:
        columns[id(base)] = inject_col
    score = _candidate_score(build)
    for base, _ in bases:
        if _subtree_contains(build, base):
            continue
        candidate = {
            "build": build,
            "build_key": build_key,
            "columns": columns,
            "score": score,
        }
        entries = winners.setdefault(id(base), [])
        if not _candidate_known(entries, candidate):
            entries.append(candidate)
            entries.sort(key=lambda entry: entry["score"])


def _candidate_known(entries, candidate) -> bool:
    """Whether an equivalent candidate (same build node) is already listed."""
    for entry in entries:
        if entry["build"] is candidate["build"]:
            return True
    return False


def _candidate_score(build):
    """A candidate's key-count proxy: the build's output estimate, or
    effectively infinite when unknown (an unjudgeable candidate must not
    beat a judged selective one)."""
    rows = _output_rows(build)
    if rows is None:
        rows = getattr(build, "estimated_rows", None)
    return (1 << 62) if rows is None else rows


def _subtree_contains(node, target) -> bool:
    """Whether `target` (by identity) appears in `node`'s subtree."""
    for candidate in _walk_plan_nodes(node):
        if candidate is target:
            return True
    return False


def _emit_build_side(ctx, build_child):
    """The build input's binding: the dedicated materialized read for a plain
    scan, the subtree's own emission for anything else (every step output is
    a materialized binding in the engine, so the key collection and the join
    can both read it). Cached by node identity, so a winner's early keys
    emission and the join's own emission share one read."""
    cached = ctx.build_bindings.get(id(build_child))
    if cached is not None:
        return cached
    if isinstance(build_child, PhysicalScan):
        binding = _emit_build_scan(ctx, build_child, ctx.names.binding())
        ctx.build_bindings[id(build_child)] = binding
        return binding
    binding = _emit(build_child, ctx)
    ctx.build_bindings[id(build_child)] = binding
    return binding


def _emit_build_scan(ctx, build_child, binding):
    """A fully materialized build-side scan (it is also distinct-scanned).
    Uses the same spec choice as a plain source scan, so a big PostgreSQL
    build side also gets the ctid-parallel read. Returns the binding that
    holds the data (a shared one when an identical scan already ran)."""
    step = {
        "op": "source_scan",
        "datasource": build_child.datasource,
        "scan": _source_scan_spec(build_child),
        "binding": binding,
        "materialize": True,
    }
    shared = _emit_step_once(ctx, step, key=_scan_share_key(build_child, step))
    ctx.scan_bindings[id(build_child)] = shared
    return shared


def _emit_collect_distinct(ctx, input_binding, key, binding):
    """Collect the build side's distinct join-key values, capped. Returns
    the binding holding the keys (shared when identical)."""
    return _emit_step_once(
        ctx,
        {
            "op": "collect_distinct",
            "input": input_binding,
            "key": key,
            "cap": _DYNAMIC_FILTER_MAX_KEYS,
            "binding": binding,
        },
    )


def _emit_injected_scan(
    ctx, base, inject_col, keys_binding, binding, build_key, extras=None
):
    """A probe base with the build's keys pushed in as `col IN (...)`. The
    probe column's NDV rides along so the engine's delivery-strategy guard
    prices the fetched fraction as keys/NDV instead of keys/row-count.
    Returns the binding holding the rows (shared when identical)."""
    step = {
        "op": "injected_scan",
        "datasource": base.datasource,
        "scan": _injected_probe_spec(base, inject_col, build_key),
        "inject_column": inject_col,
        "keys_from": keys_binding,
        "binding": binding,
    }
    ndv = _node_column_ndv(base, inject_col)
    if ndv is not None:
        step["inject_column_ndv"] = ndv
    if extras:
        step["extra_injections"] = extras
    return _emit_step_once(ctx, step)


def _injected_probe_spec(base, inject_col, build_key):
    """A structured spec for a plain scan; pre-rendered SQL for a pushed remote
    query OR an aggregate scan (wrapped as a derived table so the key filter
    applies to their OUTPUT columns - a plain scan takes the filter in its own
    WHERE). A remote island additionally carries injected_sql: the same query
    with the key filter placed on its owning base relation INSIDE, because
    sources do not push a semi-join through the derived-table wrapper."""
    if isinstance(base, PhysicalRemoteQuery):
        spec = raw_scan_spec(base)
        injected_sql = _island_injected_sql(base, inject_col, build_key)
        if injected_sql is not None:
            spec["injected_sql"] = injected_sql
        return spec
    if any((base.aggregates, base.group_by, base.grouping_sets)):
        spec = raw_scan_spec(base)
        injected_sql = _aggregate_injected_sql(base, inject_col, build_key)
        if injected_sql is not None:
            spec["injected_sql"] = injected_sql
        return spec
    return structured_scan_spec(base)


# The connection-scoped temp table the engine fills with the build side's
# distinct keys (must match fedqrs's DYN_KEYS_TEMP_TABLE).
_DYN_KEYS_TEMP_TABLE = "fedq_dyn_keys"


def _aggregate_injected_sql(base, inject_col, build_key):
    """An aggregate-subquery base (the decorrelated `AVG(x) GROUP BY key`
    probe) with `group_key IN (SELECT key FROM fedq_dyn_keys)` placed in the
    scan's WHERE - BEFORE the grouping - instead of wrapping its output.

    The inject column is a GROUP KEY (the reduction joins on it), so filtering
    the input by it is identical to filtering the groups, and sources do not
    push a semi-join through the aggregate wrapper (measured q17 SF10: 1508ms
    wrapped vs 93ms pushed inside). None when the inject column is not a plain
    group-key base column, or the scan carries GROUPING SETS (a filtered input
    changes which sets a row contributes to)."""
    if base.grouping_sets:
        return None
    owner = _aggregate_key_owner(base, inject_col)
    if owner is None:
        return None
    from ..plan.physical import to_source_sql

    keys_query = exp.select(exp.column(build_key, quoted=True)).from_(
        _DYN_KEYS_TEMP_TABLE
    )
    key_column = exp.column(owner.column, table=owner.table, quoted=True)
    injected = base._build_ast().where(key_column.isin(query=keys_query), copy=True)
    return to_source_sql(base.datasource_connection, injected.sql(dialect="postgres"))


def _aggregate_key_owner(base, inject_col):
    """The base ColumnRef of the group key whose output name is the inject
    column; None when that output is a computed aggregate (not a plain group
    key) or is ambiguous - filtering a non-group-key would change the result."""
    group_cols = set()
    for key in base.group_by or []:
        if isinstance(key, ColumnRef):
            group_cols.add((key.table, key.column))
    owners = []
    for expr, name in zip(base.aggregates or [], base.output_names or []):
        if name != inject_col or not isinstance(expr, ColumnRef):
            continue
        if (expr.table, expr.column) in group_cols:
            owners.append(expr)
    if len(owners) == 1:
        return owners[0]
    return None


def _island_injected_sql(base, inject_col, build_key):
    """The island's SQL with `owner.col IN (SELECT key FROM fedq_dyn_keys)`
    placed on the owning base relation inside the query (q03 measured the
    wrapper at 65ms vs 21ms with the filter inside). None when the owner is
    ambiguous or the island's shape would change meaning under an added WHERE
    conjunct - the engine then falls back to the output wrapper."""
    owner = _inject_owner(base, inject_col)
    if owner is None or not _injectable_island_ast(base.query_ast):
        return None
    qualifier, column = owner
    keys_query = exp.select(exp.column(build_key, quoted=True)).from_(
        _DYN_KEYS_TEMP_TABLE
    )
    conjunct = exp.column(column, table=qualifier, quoted=True).isin(query=keys_query)
    injected = base.query_ast.copy().where(conjunct, copy=False)
    from ..plan.physical import to_source_sql

    return to_source_sql(base.datasource_connection, injected.sql(dialect="postgres"))


def _inject_owner(base, inject_col):
    """The unique (relation alias, base column) whose output name is the
    inject column; None when the column is computed (no qualifier) or when
    more than one relation claims it (never inject on a guess)."""
    owners = []
    for (qualifier, column), output in base.column_alias_map.items():
        if output == inject_col and qualifier is not None:
            owners.append((qualifier, column))
    if len(owners) == 1:
        return owners[0]
    return None


def _injectable_island_ast(ast) -> bool:
    """Whether adding a WHERE conjunct preserves the island's meaning: a plain
    SELECT (no set-operation root, no WITH whose scopes the conjunct cannot
    see), no LIMIT/OFFSET (the filter would change which rows the limit
    keeps), and no window functions (frames would change). GROUP BY is fine:
    the inject column maps to a selected base column, which under grouping is
    a group key, and filtering by a group key before or after aggregation is
    identical."""
    if not isinstance(ast, exp.Select):
        return False
    if ast.args.get("with") is not None:
        return False
    if ast.args.get("limit") is not None or ast.args.get("offset") is not None:
        return False
    for _window in ast.find_all(exp.Window):
        return False
    return True


def _join_fragment(join, kind):
    """Build the hash-join fragment: keys (possibly several) plus the canonical
    output projection."""
    return {
        "kind": "hash_join",
        "join_type": kind,
        "left_keys": _key_names(join.left_keys, join.left),
        "right_keys": _key_names(join.right_keys, join.right),
        "project": _join_output_projection(join),
    }


def _key_names(keys, child):
    """Physical column names of the join keys against a child's aliases."""
    aliases = child.column_aliases()
    names = []
    for key in keys:
        names.append(_physical_column_name(key, aliases))
    return names


def _nested_loop_kind(node):
    """The IR join_kind for a nested-loop join. A CROSS join is a conditionless
    cartesian product; the engine expresses it as an INNER nested-loop join with
    NO condition (run_nested_loop_join reads an absent condition as the cross
    product), so CROSS maps to inner here rather than being refused."""
    if node.join_type == JoinType.CROSS:
        return "inner"
    return _join_kind(node)


def _emit_nested_loop_join(node, ctx):
    """A non-equi (nested-loop) join: emit both sides, join on the condition
    (or a cross join when there is none), then the canonical output columns."""
    kind = _nested_loop_kind(node)
    left_binding = _emit(node.left, ctx)
    right_binding = _emit(node.right, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = _nested_loop_fragment(node, kind)
    return _merge_step(
        ctx, fragment, {"in_left": left_binding, "in_right": right_binding}
    )


def _nested_loop_fragment(node, kind):
    """Build the nested-loop-join fragment: condition + canonical projection."""
    fragment = {
        "kind": "nested_loop_join",
        "join_type": kind,
        "project": _join_output_projection(node),
    }
    if node.condition is not None:
        fragment["condition"] = _serialize_expr(
            node.condition, _two_sided_column_fn(node)
        )
    return fragment


def _two_sided_column_fn(join):
    """A column renderer resolving each ref to in_left/in_right by owning side."""
    left_aliases = join.left.column_aliases()
    right_aliases = join.right.column_aliases()
    left_tables = _relation_names(left_aliases)

    def column(col_ref):
        key = (col_ref.table, col_ref.column)
        if col_ref.table in left_tables:
            return {"node": "column", "relation": "in_left", "name": left_aliases[key]}
        return {"node": "column", "relation": "in_right", "name": right_aliases[key]}

    return column


def _join_output_projection(join):
    """Every join output column, from whichever side owns it, canonically named."""
    left_aliases = join.left.column_aliases()
    right_aliases = join.right.column_aliases()
    left_tables = _relation_names(left_aliases)
    project = []
    for (table, column), out_name in join.column_aliases().items():
        item = _join_projection_item(
            table, column, out_name, left_tables, left_aliases, right_aliases
        )
        project.append(item)
    _uniquify_aliases(project)
    return project


def _uniquify_aliases(project):
    """Suffix any repeated output alias so the join result has unique column
    names. A self-join (both sides the same relation) can name two columns
    identically (`right_customer_id` twice); DataFusion rejects a duplicate
    qualified name when that result is registered as a downstream input. Only a
    repeat is renamed, so a column a parent actually references keeps its name.
    """
    seen = {}
    for item in project:
        name = item["alias"]
        if name not in seen:
            seen[name] = 0
            continue
        seen[name] += 1
        item["alias"] = f"{name}_{seen[name]}"


def _relation_names(aliases):
    """The set of relation aliases present in a column-alias map."""
    names = set()
    for table, _column in aliases:
        names.add(table)
    return names


def _join_projection_item(
    table, column, out_name, left_tables, left_aliases, right_aliases
):
    """One join output column, resolved to its input side and physical name."""
    if table in left_tables:
        return _side_column("in_left", left_aliases[(table, column)], out_name)
    return _side_column("in_right", right_aliases[(table, column)], out_name)


def _side_column(relation, name, alias):
    """A projection item selecting `relation`.`name` aliased to `alias`."""
    return {
        "expr": {"node": "column", "relation": relation, "name": name},
        "alias": alias,
    }


# Physical node type -> its emitter. Defined after the emitters it references.
_NODE_EMITTERS = {
    PhysicalRemoteQuery: _emit_source,
    PhysicalScan: _emit_source,
    PhysicalRemoteSetOp: _emit_source,
    PhysicalHashJoin: _emit_join,
    PhysicalNestedLoopJoin: _emit_nested_loop_join,
    PhysicalProjection: _emit_projection,
    PhysicalHashAggregate: _emit_aggregate,
    PhysicalSort: _emit_sort,
    PhysicalFilter: _emit_filter,
    PhysicalLimit: _emit_limit,
    PhysicalValues: _emit_values,
    PhysicalCTEMergeQuery: _emit_cte_merge,
    PhysicalCTEScan: _emit_cte_scan,
    PhysicalShipment: _emit_shipment,
    PhysicalAliasedRelation: _emit_passthrough,
    PhysicalWindow: _emit_window,
    PhysicalGroupedLimit: _emit_grouped_limit,
    PhysicalSingleRowGuard: _emit_single_row_guard,
    PhysicalUnion: _emit_union,
    PhysicalSetOperation: _emit_set_operation,
}


# --- datasource registration bridge -------------------------------------------


def _pg_adbc_driver_path():
    """Locate the ADBC Postgres driver shared library shipped with the venv."""
    import adbc_driver_postgresql

    return os.path.join(
        os.path.dirname(adbc_driver_postgresql.__file__),
        "libadbc_driver_postgresql.so",
    )


def register_datasources(datasources):
    """Register each Python datasource with the Rust engine (by name)."""
    import fedqrs

    from ..datasources.duckdb import DuckDBDataSource
    from ..datasources.postgresql import PostgreSQLDataSource

    for datasource in datasources:
        _register_one(fedqrs, datasource, PostgreSQLDataSource, DuckDBDataSource)


def _register_one(fedqrs, datasource, postgres_cls, duckdb_cls):
    """Register a single datasource, dispatching on its connector type."""
    from ..datasources.parquet import ParquetDataSource

    if isinstance(datasource, ParquetDataSource):
        fedqrs.register_datasource(
            datasource.name, "parquet", {"dir": datasource.parquet_dir}
        )
        return
    if isinstance(datasource, postgres_cls):
        params = {"uri": datasource._adbc_uri(), "adbc_driver": _pg_adbc_driver_path()}
        fedqrs.register_datasource(datasource.name, "postgres", params)
        return
    if isinstance(datasource, duckdb_cls):
        fedqrs.register_datasource(
            datasource.name, "duckdb", {"path": datasource.db_path}
        )
        return
    raise UnsupportedIR(
        f"datasource {type(datasource).__name__} has no native Rust connector yet"
    )


def execute_via_rust(plan, datasources):
    """Register datasources, serialize `plan` to IR, run it in Rust, return a
    pyarrow Table. The whole query executes in Rust; only the result crosses."""
    import json

    import fedqrs
    import pyarrow as pa

    register_datasources(datasources)
    ir = build_ir(plan)
    # The engine returns per-step (binding, measured rows) observations for the
    # learned-stats catalog; the write path (join with build_ir provenance,
    # persist) is wired by the caller. Discarded here until then.
    stream, _observations = fedqrs.execute_ir(json.dumps(ir))
    return pa.RecordBatchReader.from_stream(stream).read_all()
