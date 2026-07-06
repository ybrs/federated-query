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
)
from ..plan.logical import JoinType
from ..optimizer.estimate_defaults import larger_estimated_side
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
    PhysicalSort,
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
    return {"node": "cast", "expr": _serialize_expr(expr.expr, column_fn), "to": arrow_type}


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
        whens.append({
            "when": _serialize_expr(condition, column_fn),
            "then": _serialize_expr(result, column_fn),
        })
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
        raise UnsupportedIR("cannot inject a dynamic filter into an ordered/limited scan")


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
        # Bindings of base scans already emitted with an injected dynamic
        # filter, keyed by node id: when the probe of a reduction is a
        # composite (wrapper layers over a single-source base), the base is
        # injected first and its binding cached, then the wrappers emit
        # normally and reach the base through this cache instead of re-reading.
        self.injected = {}


def build_ir(plan):
    """Serialize a physical plan root into the execution IR.

    Walks the tree bottom-up: each node emits its steps and yields a binding
    whose Arrow columns are named by that node's output schema, so a parent
    resolves its expressions against ``child.column_aliases()``. Unsupported
    shapes raise so we never emit a plan that could produce wrong rows.
    """
    ctx = _Ctx()
    binding = _emit(plan, ctx)
    ctx.steps.append({"op": "return", "input": binding})
    return {"outputs": _plan_outputs(plan), "steps": ctx.steps, "fragments": ctx.fragments}


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
    binding = ctx.names.binding()
    ctx.steps.append({
        "op": "source_scan", "datasource": node.datasource,
        "scan": _source_scan_spec(node), "binding": binding,
    })
    return binding


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
    ctx.steps.append({
        "op": "merge", "fragment": fragment, "inputs": inputs, "binding": result,
    })
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
    project = _projection_items(node.expressions, node.output_names, "in_0", aliases, node.input)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "project", "project": project}
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
    fragment."""
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
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
        fragment["grouping_sets"] = _serialize_grouping_sets(node.grouping_sets, aliases)
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
    call = {"func": fn.function_name, "distinct": fn.distinct, "star": star, "args": args}
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


def _emit_cte_scan(node, ctx):
    """A CTE reference: emit the producer's body. A CTE referenced N times
    re-emits the body per reference (correct for deterministic bodies; sharing
    a single materialization is a perf follow-up). An explicit CTE column list
    relabels the body's output columns to the declared names."""
    binding = _emit(node.producer.body, ctx)
    if not node.producer.column_names:
        return binding
    return _relabel_columns(binding, node.producer.body, node.producer.column_names, ctx)


def _relabel_columns(binding, body, names, ctx):
    """Project a binding's columns to new output names (a CTE column list),
    positionally: the i-th body column becomes the i-th declared name."""
    project = []
    for source, target in zip(body.schema().names, names):
        project.append(_side_column("in_0", source, target))
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "project", "project": project}
    result = ctx.names.binding()
    ctx.steps.append({"op": "merge", "fragment": fragment, "inputs": {"in_0": binding}, "binding": result})
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
        "FROM __gl_input) "
        + node_sql
    )
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append({"op": "merge", "fragment": fragment, "inputs": {"__gl_input": child}, "binding": result})
    return result


def _emit_window(node, ctx):
    """A window-bearing projection: register the input as `in_window` and run the
    rendered `SELECT <exprs> OVER (...) FROM in_window` as a raw_sql fragment."""
    child = _emit(node.input, ctx)
    sql = node._window_sql(node.input.column_aliases())
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append({"op": "merge", "fragment": fragment, "inputs": {"in_window": child}, "binding": result})
    return result


def _emit_cte_merge(node, ctx):
    """A whole WITH/CTE rendered as SQL over named inputs: emit each input to a
    binding, register it under its name, and run the SQL as a `raw_sql` fragment."""
    inputs = {}
    for name, subtree in node.inputs.items():
        inputs[name] = _emit(subtree, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": node.sql}
    result = ctx.names.binding()
    ctx.steps.append({"op": "merge", "fragment": fragment, "inputs": inputs, "binding": result})
    return result


def _emit_limit(node, ctx):
    """A LIMIT/OFFSET over its single input, as a `limit` fragment."""
    child = _emit(node.input, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "limit", "limit": node.limit, "offset": node.offset}
    return _merge_step(ctx, fragment, {"in_0": child})


def _emit_values(node, ctx):
    """A VALUES relation of constant rows: render it as `SELECT * FROM (VALUES
    ...) AS v(names)` and run it as a `raw_sql` fragment (no inputs)."""
    sql = _render_values_sql(node.rows, node.output_names)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "raw_sql", "sql": sql}
    result = ctx.names.binding()
    ctx.steps.append({"op": "merge", "fragment": fragment, "inputs": {}, "binding": result})
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
    if _can_reduce(join) and _reduction_filters(join):
        left_binding, right_binding = _emit_reduced_join(join, ctx)
    else:
        left_binding = _emit(join.left, ctx)
        right_binding = _emit(join.right, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = _join_fragment(join, kind)
    return _merge_step(ctx, fragment, {"in_left": left_binding, "in_right": right_binding})


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
    return _probe_preference(join.left) > 0 or _probe_preference(join.right) > 0


def _left_reducible(join) -> bool:
    """True when a LEFT join's nullable right side can be key-reduced: a single
    equi key and a right probe whose injectable base preserves the key."""
    if len(join.left_keys) != 1 or len(join.right_keys) != 1:
        return False
    inject_col = _physical_column_name(join.right_keys[0], join.right.column_aliases())
    return _reducible_probe_base(join.right, inject_col) is not None


def _reduction_filters(join) -> bool:
    """The cost-based usefulness gate: False when the planned reduction
    provably filters nothing - the build side's distinct keys cover the probe
    column's whole value domain (an unfiltered dimension's keys are the FK
    domain, so the injected IN keeps every probe row and costs pure overhead).
    Decided from the cost model's NDVs threaded onto the plan; when either
    side's statistic is missing this keeps today's reduce-by-default."""
    build, probe, build_key, probe_key = _orient_join(join)
    if not isinstance(build, PhysicalScan):
        # Only a plain scan's estimate reflects its own filters. A collapsed
        # remote's estimated_rows is the deliberate max-base OVER-estimate and
        # its column_ndv the unfiltered base domain, so judging its keys here
        # would kill useful reductions (q11: a 400-row filtered island reads
        # as 10k keys). Abstain until remotes carry a real output estimate.
        return True
    inject_col = _physical_column_name(probe_key, probe.column_aliases())
    probe_ndv = _node_column_ndv(_reducible_probe_base(probe, inject_col), inject_col)
    keys_ndv = _node_column_ndv(build, _build_key_name(build, build_key))
    return not useless_key_reduction(keys_ndv, build.estimated_rows, probe_ndv)


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


def _emit_reduced_join(join, ctx):
    """The build/collect/inject reduction path; returns (left, right) bindings."""
    build_child, probe_child, build_key_expr, probe_key_expr = _orient_join(join)
    build_binding, probe_binding = _emit_reduced_inputs(
        ctx, build_child, probe_child, build_key_expr, probe_key_expr)
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
    if _probe_preference(join.right) >= _probe_preference(join.left):
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    return join.right, join.left, join.right_keys[0], join.left_keys[0]


def _cardinality_probe(join):
    """The larger injectable side to reduce, or None to fall back.

    Uses the cost estimate threaded onto both children (via the shared
    larger_estimated_side helper). Declines (None) when either estimate is
    missing, they tie (both likely defaulted), or the larger side is not an
    injectable probe - guaranteeing the same reduction set as the structural
    heuristic, just better-oriented when sizes differ."""
    larger = larger_estimated_side(join.left, join.right)
    if larger is not None and _probe_preference(larger) > 0:
        return larger
    return None


def _emit_reduced_inputs(ctx, build_child, probe_child, build_key_expr, probe_key_expr):
    """Emit the build side, its distinct keys, and the injected probe scan."""
    build_key = _physical_column_name(build_key_expr, build_child.column_aliases())
    inject_col = _physical_column_name(probe_key_expr, probe_child.column_aliases())
    build_binding = _emit_build_side(ctx, build_child)
    keys_binding = ctx.names.binding()
    _emit_collect_distinct(ctx, build_binding, build_key, keys_binding)
    probe_binding = _emit_probe(ctx, probe_child, inject_col, keys_binding)
    return build_binding, probe_binding


def _emit_probe(ctx, probe_child, inject_col, keys_binding):
    """Emit the probe with the build keys injected into its base scan.

    A plain scan / remote probe is injected directly. A composite probe (an
    aggregate subquery under alias/projection wrappers) has the keys injected
    into its base, whose binding is cached so the wrappers - emitted normally
    as coordinator fragments - read the reduced base."""
    base = _reducible_probe_base(probe_child, inject_col)
    base_binding = ctx.names.binding()
    _emit_injected_scan(ctx, base, inject_col, keys_binding, base_binding)
    if base is probe_child:
        return base_binding
    ctx.injected[id(base)] = base_binding
    return _emit(probe_child, ctx)


def _emit_build_side(ctx, build_child):
    """The build input's binding: the dedicated materialized read for a plain
    scan, the subtree's own emission for anything else (every step output is
    a materialized binding in the engine, so the key collection and the join
    can both read it)."""
    if isinstance(build_child, PhysicalScan):
        binding = ctx.names.binding()
        _emit_build_scan(ctx, build_child, binding)
        return binding
    return _emit(build_child, ctx)


def _emit_build_scan(ctx, build_child, binding):
    """A fully materialized build-side scan (it is also distinct-scanned).
    Uses the same spec choice as a plain source scan, so a big PostgreSQL
    build side also gets the ctid-parallel read."""
    ctx.steps.append({
        "op": "source_scan", "datasource": build_child.datasource,
        "scan": _source_scan_spec(build_child), "binding": binding, "materialize": True,
    })


def _emit_collect_distinct(ctx, input_binding, key, binding):
    """Collect the build side's distinct join-key values, capped."""
    ctx.steps.append({
        "op": "collect_distinct", "input": input_binding, "key": key,
        "cap": _DYNAMIC_FILTER_MAX_KEYS, "binding": binding,
    })


def _emit_injected_scan(ctx, base, inject_col, keys_binding, binding):
    """A probe base with the build's keys pushed in as `col IN (...)`."""
    ctx.steps.append({
        "op": "injected_scan", "datasource": base.datasource,
        "scan": _injected_probe_spec(base), "inject_column": inject_col,
        "keys_from": keys_binding, "binding": binding,
    })


def _injected_probe_spec(base):
    """A structured spec for a plain scan; pre-rendered SQL for a pushed remote
    query OR an aggregate scan (both wrapped as a derived table so the key
    filter applies to their OUTPUT columns - a plain scan takes the filter in
    its own WHERE)."""
    if isinstance(base, PhysicalRemoteQuery):
        return raw_scan_spec(base)
    if any((base.aggregates, base.group_by, base.grouping_sets)):
        return raw_scan_spec(base)
    return structured_scan_spec(base)


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


def _emit_nested_loop_join(node, ctx):
    """A non-equi (nested-loop) join: emit both sides, join on the condition
    (or a cross join when there is none), then the canonical output columns."""
    kind = _join_kind(node)
    left_binding = _emit(node.left, ctx)
    right_binding = _emit(node.right, ctx)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = _nested_loop_fragment(node, kind)
    return _merge_step(ctx, fragment, {"in_left": left_binding, "in_right": right_binding})


def _nested_loop_fragment(node, kind):
    """Build the nested-loop-join fragment: condition + canonical projection."""
    fragment = {
        "kind": "nested_loop_join",
        "join_type": kind,
        "project": _join_output_projection(node),
    }
    if node.condition is not None:
        fragment["condition"] = _serialize_expr(node.condition, _two_sided_column_fn(node))
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
            table, column, out_name, left_tables, left_aliases, right_aliases)
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
    for (table, _column) in aliases:
        names.add(table)
    return names


def _join_projection_item(table, column, out_name, left_tables, left_aliases, right_aliases):
    """One join output column, resolved to its input side and physical name."""
    if table in left_tables:
        return _side_column("in_left", left_aliases[(table, column)], out_name)
    return _side_column("in_right", right_aliases[(table, column)], out_name)


def _side_column(relation, name, alias):
    """A projection item selecting `relation`.`name` aliased to `alias`."""
    return {"expr": {"node": "column", "relation": relation, "name": name}, "alias": alias}


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
    PhysicalAliasedRelation: _emit_passthrough,
    PhysicalWindow: _emit_window,
    PhysicalGroupedLimit: _emit_grouped_limit,
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
        fedqrs.register_datasource(datasource.name, "parquet", {"dir": datasource.parquet_dir})
        return
    if isinstance(datasource, postgres_cls):
        params = {"uri": datasource._adbc_uri(), "adbc_driver": _pg_adbc_driver_path()}
        fedqrs.register_datasource(datasource.name, "postgres", params)
        return
    if isinstance(datasource, duckdb_cls):
        fedqrs.register_datasource(datasource.name, "duckdb", {"path": datasource.db_path})
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
    stream = fedqrs.execute_ir(json.dumps(ir))
    return pa.RecordBatchReader.from_stream(stream).read_all()
