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

from ..plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    FunctionCall,
    Literal,
    UnaryOp,
    UnaryOpType,
)
from ..plan.logical import JoinType
from ..plan.physical import (
    PhysicalAliasedRelation,
    PhysicalCTEMergeQuery,
    PhysicalCTEScan,
    PhysicalFilter,
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
    """Serialize one expression; `column_fn` renders each ColumnRef."""
    if isinstance(expr, ColumnRef):
        return column_fn(expr)
    if isinstance(expr, Literal):
        return {"node": "literal", "value": _literal_value(expr.value)}
    if isinstance(expr, BinaryOp):
        return _serialize_binary(expr, column_fn)
    if isinstance(expr, UnaryOp):
        return _serialize_unary(expr, column_fn)
    raise UnsupportedIR(f"expression {type(expr).__name__} not supported in IR")


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
    emitter = _NODE_EMITTERS.get(type(node))
    if emitter is None:
        raise UnsupportedIR(f"physical node {type(node).__name__} not supported yet")
    return emitter(node, ctx)


def _emit_source(node, ctx):
    """A source scan run natively, as pre-rendered SQL."""
    binding = ctx.names.binding()
    ctx.steps.append({
        "op": "source_scan", "datasource": node.datasource,
        "scan": raw_scan_spec(node), "binding": binding,
    })
    return binding


def _merge_step(ctx, fragment, inputs):
    """Append a merge step running `fragment` over `inputs`; return its binding."""
    result = ctx.names.binding()
    ctx.steps.append({
        "op": "merge", "fragment": fragment, "inputs": inputs, "binding": result,
    })
    return result


def _emit_projection(node, ctx):
    """A projection over its single input, as a `project` fragment."""
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
    project = _projection_items(node.expressions, node.output_names, "in_0", aliases)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "project", "project": project}
    return _merge_step(ctx, fragment, {"in_0": child})


def _projection_items(expressions, names, input_name, aliases):
    """Serialize each output expression aliased to its output name."""
    items = []
    for expr, name in zip(expressions, names):
        items.append({"expr": _expr_over(expr, input_name, aliases), "alias": name})
    return items


def _emit_aggregate(node, ctx):
    """A GROUP BY over its single input, as an `aggregate` fragment."""
    if node.grouping_sets:
        raise UnsupportedIR("GROUPING SETS/ROLLUP/CUBE not yet supported")
    child = _emit(node.input, ctx)
    aliases = node.input.column_aliases()
    group_by = _serialize_group_by(node.group_by, aliases)
    select = _aggregate_select(node.aggregates, node.output_names, aliases)
    fragment = ctx.names.fragment()
    ctx.fragments[fragment] = {"kind": "aggregate", "select": select, "group_by": group_by}
    return _merge_step(ctx, fragment, {"in_0": child})


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
    """Serialize an aggregate FunctionCall; `count(*)` is the star form."""
    if fn.within_group_key is not None:
        raise UnsupportedIR("ordered-set aggregates (WITHIN GROUP) not yet supported")
    star = _is_star_arg(fn.args)
    args = _agg_args(fn.args, aliases, star)
    return {"func": fn.function_name, "distinct": fn.distinct, "star": star, "args": args}


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
    scans; otherwise both children are emitted in full (recursively, so they may
    be any subtree) and joined. The join's canonical output columns are emitted
    so a parent can reference them.
    """
    kind = _join_kind(join)
    if _can_reduce(join):
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
    """True when the semi-join reduction applies: INNER, single key, both plain
    scans (so the build renders source SQL and the probe accepts an IN filter)."""
    if join.join_type != JoinType.INNER:
        return False
    if len(join.left_keys) != 1 or len(join.right_keys) != 1:
        return False
    return isinstance(join.left, PhysicalScan) and isinstance(join.right, PhysicalScan)


def _emit_reduced_join(join, ctx):
    """The build/collect/inject reduction path; returns (left, right) bindings."""
    build_child, probe_child, build_key_expr, probe_key_expr = _orient_join(join)
    build_binding, probe_binding = _emit_reduced_inputs(
        ctx, build_child, probe_child, build_key_expr, probe_key_expr)
    left_binding = build_binding if build_child is join.left else probe_binding
    right_binding = build_binding if build_child is join.right else probe_binding
    return left_binding, right_binding


def _orient_join(join):
    """Pick build/probe sides and their join keys from the build_side choice."""
    if join.build_side == "left":
        return join.left, join.right, join.left_keys[0], join.right_keys[0]
    return join.right, join.left, join.right_keys[0], join.left_keys[0]


def _emit_reduced_inputs(ctx, build_child, probe_child, build_key_expr, probe_key_expr):
    """Emit the build scan, its distinct keys, and the injected probe scan."""
    build_key = _physical_column_name(build_key_expr, build_child.column_aliases())
    inject_col = _physical_column_name(probe_key_expr, probe_child.column_aliases())
    build_binding = ctx.names.binding()
    keys_binding = ctx.names.binding()
    probe_binding = ctx.names.binding()
    _emit_build_scan(ctx, build_child, build_binding)
    _emit_collect_distinct(ctx, build_binding, build_key, keys_binding)
    _emit_injected_scan(ctx, probe_child, inject_col, keys_binding, probe_binding)
    return build_binding, probe_binding


def _emit_build_scan(ctx, build_child, binding):
    """A fully materialized build-side scan (it is also distinct-scanned)."""
    ctx.steps.append({
        "op": "source_scan", "datasource": build_child.datasource,
        "scan": raw_scan_spec(build_child), "binding": binding, "materialize": True,
    })


def _emit_collect_distinct(ctx, input_binding, key, binding):
    """Collect the build side's distinct join-key values, capped."""
    ctx.steps.append({
        "op": "collect_distinct", "input": input_binding, "key": key,
        "cap": _DYNAMIC_FILTER_MAX_KEYS, "binding": binding,
    })


def _emit_injected_scan(ctx, probe_child, inject_col, keys_binding, binding):
    """A probe scan with the build's keys pushed in as `col IN (...)`."""
    ctx.steps.append({
        "op": "injected_scan", "datasource": probe_child.datasource,
        "scan": structured_scan_spec(probe_child), "inject_column": inject_col,
        "keys_from": keys_binding, "binding": binding,
    })


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
