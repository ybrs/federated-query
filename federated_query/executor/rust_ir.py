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

from ..plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    Expression,
    FunctionCall,
    Literal,
)
from ..plan.logical import JoinType
from ..plan.physical import (
    PhysicalHashAggregate,
    PhysicalHashJoin,
    PhysicalProjection,
    PhysicalRemoteQuery,
    PhysicalScan,
    PhysicalSort,
    _DYNAMIC_FILTER_MAX_KEYS,
    _physical_column_name,
)


class UnsupportedIR(Exception):
    """A physical node or expression the Rust IR does not yet represent."""


# Binary operators the Rust engine understands, keyed by the engine's enum.
# The Rust side accepts arithmetic/comparison symbols verbatim plus lowercase
# ``and``/``or``; anything absent here is refused loudly.
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


def expr_to_ir(expr: Expression) -> dict:
    """Serialize one engine expression into an IR expression node.

    Supports the shapes a simple scan filter needs (column / literal / binary).
    Unsupported node types raise, never degrade.
    """
    if isinstance(expr, ColumnRef):
        node = {"node": "column", "name": expr.column}
        if expr.table is not None:
            node["relation"] = expr.table
        return node

    if isinstance(expr, Literal):
        return {"node": "literal", "value": _literal_value(expr.value)}

    if isinstance(expr, BinaryOp):
        token = _BINARY_OP_TOKENS.get(expr.op)
        if token is None:
            raise UnsupportedIR(f"binary operator {expr.op} not supported in IR")
        return {
            "node": "binary",
            "op": token,
            "left": expr_to_ir(expr.left),
            "right": expr_to_ir(expr.right),
        }

    raise UnsupportedIR(f"expression {type(expr).__name__} not supported in IR")


def _literal_value(value) -> dict:
    """Map a Python literal value to the IR's typed-literal form.

    ``bool`` is checked before ``int`` because ``bool`` is an ``int`` subclass.
    """
    if value is None:
        return {"lit": "null"}
    if isinstance(value, bool):
        return {"lit": "bool", "value": value}
    if isinstance(value, int):
        return {"lit": "int", "value": value}
    if isinstance(value, float):
        return {"lit": "float", "value": value}
    if isinstance(value, str):
        return {"lit": "str", "value": value}
    raise UnsupportedIR(f"literal value of type {type(value).__name__} not supported")


def raw_scan_spec(node) -> dict:
    """A scan spec that carries pre-rendered source SQL.

    Used for a single-source subtree (or any scan we don't need to inject a
    dynamic filter into): Python's emitter already renders dialect-correct SQL,
    and Rust runs it natively. ``node`` must expose the engine's source-SQL
    renderer (``PhysicalScan`` / ``PhysicalRemoteQuery``).
    """
    if isinstance(node, PhysicalScan):
        return {"raw_sql": node._render_source_sql()}
    # PhysicalRemoteQuery and any other single-source leaf render through _sql().
    renderer = getattr(node, "_sql", None)
    if renderer is None:
        raise UnsupportedIR(
            f"{type(node).__name__} does not render source SQL for a raw scan"
        )
    return {"raw_sql": renderer()}


def structured_scan_spec(scan: PhysicalScan) -> dict:
    """A structured scan spec for a plain single-table scan.

    Required when Rust must inject a dynamic ``col IN (...)`` filter: the scan
    must be a bare table read (no pushed aggregate / grouping / order-by), so
    the injected predicate composes correctly. Raises otherwise.
    """
    if scan.aggregates or scan.group_by or scan.grouping_sets:
        raise UnsupportedIR("cannot inject a dynamic filter into an aggregate scan")
    if scan.order_by_keys or scan.limit is not None or scan.offset:
        raise UnsupportedIR("cannot inject a dynamic filter into an ordered/limited scan")

    spec: dict = {"table": scan.table_name, "columns": list(scan.columns)}
    if scan.schema_name:
        spec["schema"] = scan.schema_name
    if scan.alias:
        spec["alias"] = scan.alias
    if scan.filters is not None:
        spec["filter"] = expr_to_ir(scan.filters)
    if scan.distinct:
        spec["distinct"] = True
    return spec


class _Names:
    """Hands out unique binding and fragment names within one IR."""

    def __init__(self):
        self._b = 0
        self._f = 0

    def binding(self) -> str:
        self._b += 1
        return f"b{self._b}"

    def fragment(self) -> str:
        self._f += 1
        return f"f{self._f}"


class _Ctx:
    """Accumulates IR steps and fragments while walking a plan bottom-up."""

    def __init__(self):
        self.steps: list = []
        self.fragments: dict = {}
        self.names = _Names()


def build_ir(plan) -> dict:
    """Serialize a physical plan root into the execution IR.

    Walks the tree bottom-up: each node emits its steps and yields a binding
    whose Arrow columns are named by that node's output schema, so a parent
    resolves its expressions against ``child.column_aliases()`` (the same
    contract the engine's own operators use). Unsupported shapes raise so we
    never emit a plan that could produce wrong rows.
    """
    ctx = _Ctx()
    binding = _emit(plan, ctx)
    ctx.steps.append({"op": "return", "input": binding})
    outputs = list(getattr(plan, "output_names", None) or plan.schema().names)
    return {"outputs": outputs, "steps": ctx.steps, "fragments": ctx.fragments}


def _emit(node, ctx: _Ctx) -> str:
    """Emit steps for `node`, returning the binding that holds its output."""
    if isinstance(node, (PhysicalRemoteQuery, PhysicalScan)):
        binding = ctx.names.binding()
        ctx.steps.append({
            "op": "source_scan",
            "datasource": node.datasource,
            "scan": raw_scan_spec(node),
            "binding": binding,
        })
        return binding
    if isinstance(node, PhysicalHashJoin):
        return _emit_join(node, ctx)
    if isinstance(node, PhysicalProjection):
        return _emit_projection(node, ctx)
    if isinstance(node, PhysicalHashAggregate):
        return _emit_aggregate(node, ctx)
    if isinstance(node, PhysicalSort):
        return _emit_sort(node, ctx)
    raise UnsupportedIR(f"physical node {type(node).__name__} not supported yet")


def _emit_sort(node, ctx: _Ctx) -> str:
    """An ORDER BY over its single input, as a `sort` fragment."""
    child = _emit(node.input, ctx)
    child_aliases = node.input.column_aliases()
    keys = []
    for i, (expr, asc) in enumerate(zip(node.sort_keys, node.ascending)):
        nulls = node.nulls_order[i] if node.nulls_order else None
        # SQL default NULL placement: DESC -> NULLS FIRST, ASC -> NULLS LAST.
        nulls_first = (str(nulls).upper() == "FIRST") if nulls is not None else (not asc)
        keys.append({
            "expr": _expr_over(expr, "in_0", child_aliases),
            "ascending": asc,
            "nulls_first": nulls_first,
        })
    frag = ctx.names.fragment()
    ctx.fragments[frag] = {"kind": "sort", "keys": keys}
    result = ctx.names.binding()
    ctx.steps.append({
        "op": "merge", "fragment": frag,
        "inputs": {"in_0": child}, "binding": result,
    })
    return result


def _emit_aggregate(node, ctx: _Ctx) -> str:
    """A GROUP BY over its single input, as an `aggregate` fragment.

    The node's `aggregates` list is the output SELECT list: each item is either
    an aggregate FunctionCall or a plain grouping expression, aliased by
    `output_names`. Columns resolve against the child's aliases (relation `in_0`).
    """
    if node.grouping_sets:
        raise UnsupportedIR("GROUPING SETS/ROLLUP/CUBE not yet supported")
    child = _emit(node.input, ctx)
    child_aliases = node.input.column_aliases()

    group_by = [_expr_over(g, "in_0", child_aliases) for g in node.group_by]
    select = []
    for expr, name in zip(node.aggregates, node.output_names):
        if isinstance(expr, FunctionCall) and expr.is_aggregate:
            select.append({"agg": _agg_call(expr, child_aliases), "alias": name})
        else:
            select.append({"expr": _expr_over(expr, "in_0", child_aliases), "alias": name})

    frag = ctx.names.fragment()
    ctx.fragments[frag] = {"kind": "aggregate", "select": select, "group_by": group_by}
    result = ctx.names.binding()
    ctx.steps.append({
        "op": "merge", "fragment": frag,
        "inputs": {"in_0": child}, "binding": result,
    })
    return result


def _agg_call(fn: FunctionCall, child_aliases: dict) -> dict:
    """Serialize an aggregate FunctionCall. `count(*)` is the star form."""
    if fn.within_group_key is not None:
        raise UnsupportedIR("ordered-set aggregates (WITHIN GROUP) not yet supported")
    star = (
        len(fn.args) == 1
        and isinstance(fn.args[0], ColumnRef)
        and fn.args[0].column == "*"
    )
    args = [] if star else [_expr_over(a, "in_0", child_aliases) for a in fn.args]
    return {"func": fn.function_name, "distinct": fn.distinct, "star": star, "args": args}


def _emit_projection(node, ctx: _Ctx) -> str:
    """A projection over its single input, as a `project` fragment."""
    child = _emit(node.input, ctx)
    child_aliases = node.input.column_aliases()
    project = [
        {"expr": _expr_over(expr, "in_0", child_aliases), "alias": name}
        for expr, name in zip(node.expressions, node.output_names)
    ]
    frag = ctx.names.fragment()
    ctx.fragments[frag] = {"kind": "project", "project": project}
    result = ctx.names.binding()
    ctx.steps.append({
        "op": "merge", "fragment": frag,
        "inputs": {"in_0": child}, "binding": result,
    })
    return result


def _emit_join(join, ctx: _Ctx) -> str:
    """Emit build/collect/inject/merge for an INNER hash join, producing the
    join's canonical output columns (so a parent can reference them).

    Semi-join reduction: read the build side, collect its distinct join key,
    inject those values into the probe's SQL as `probe_key IN (...)` (all in
    Rust), then join.
    """
    if join.join_type != JoinType.INNER:
        raise UnsupportedIR(f"join type {join.join_type} not yet supported")
    if len(join.left_keys) != 1 or len(join.right_keys) != 1:
        raise UnsupportedIR("multi-key join not yet supported")
    left, right = join.left, join.right
    if not isinstance(left, PhysicalScan) or not isinstance(right, PhysicalScan):
        raise UnsupportedIR("join children must be plain scans for now")

    if join.build_side == "left":
        build_child, probe_child = left, right
        build_key_expr, probe_key_expr = join.left_keys[0], join.right_keys[0]
    else:
        build_child, probe_child = right, left
        build_key_expr, probe_key_expr = join.right_keys[0], join.left_keys[0]

    build_key = _physical_column_name(build_key_expr, build_child.column_aliases())
    inject_col = _physical_column_name(probe_key_expr, probe_child.column_aliases())

    build_binding = ctx.names.binding()
    keys_binding = ctx.names.binding()
    probe_binding = ctx.names.binding()

    ctx.steps.append({
        "op": "source_scan", "datasource": build_child.datasource,
        "scan": raw_scan_spec(build_child), "binding": build_binding,
        "materialize": True,
    })
    ctx.steps.append({
        "op": "collect_distinct", "input": build_binding, "key": build_key,
        "cap": _DYNAMIC_FILTER_MAX_KEYS, "binding": keys_binding,
    })
    ctx.steps.append({
        "op": "injected_scan", "datasource": probe_child.datasource,
        "scan": structured_scan_spec(probe_child), "inject_column": inject_col,
        "keys_from": keys_binding, "binding": probe_binding,
    })

    left_binding = build_binding if build_child is left else probe_binding
    right_binding = build_binding if build_child is right else probe_binding
    left_key = _physical_column_name(join.left_keys[0], left.column_aliases())
    right_key = _physical_column_name(join.right_keys[0], right.column_aliases())

    # Canonical output: every join output column, from whichever side owns it.
    left_aliases = left.column_aliases()
    right_aliases = right.column_aliases()
    left_tables = {tbl for (tbl, _c) in left_aliases}
    project = []
    for (tbl, col), out_name in join.column_aliases().items():
        side = "in_left" if tbl in left_tables else "in_right"
        aliases = left_aliases if tbl in left_tables else right_aliases
        phys = aliases[(tbl, col)]
        project.append({
            "expr": {"node": "column", "relation": side, "name": phys},
            "alias": out_name,
        })

    frag = ctx.names.fragment()
    ctx.fragments[frag] = {
        "kind": "hash_join", "join_type": "inner",
        "left_keys": [left_key], "right_keys": [right_key], "project": project,
    }
    result = ctx.names.binding()
    ctx.steps.append({
        "op": "merge", "fragment": frag,
        "inputs": {"in_left": left_binding, "in_right": right_binding},
        "binding": result,
    })
    return result


def _expr_over(expr: Expression, input_name: str, child_aliases: dict) -> dict:
    """Serialize an expression whose columns come from one input relation.

    Each ColumnRef resolves through `child_aliases` to the physical column name
    in that input's binding, tagged with `input_name` (e.g. `in_0`)."""
    if isinstance(expr, ColumnRef):
        return {
            "node": "column",
            "relation": input_name,
            "name": _physical_column_name(expr, child_aliases),
        }
    if isinstance(expr, Literal):
        return {"node": "literal", "value": _literal_value(expr.value)}
    if isinstance(expr, BinaryOp):
        token = _BINARY_OP_TOKENS.get(expr.op)
        if token is None:
            raise UnsupportedIR(f"binary operator {expr.op} not supported in IR")
        return {
            "node": "binary",
            "op": token,
            "left": _expr_over(expr.left, input_name, child_aliases),
            "right": _expr_over(expr.right, input_name, child_aliases),
        }
    raise UnsupportedIR(f"expression {type(expr).__name__} not supported")


# --- datasource registration bridge -------------------------------------------

def _pg_adbc_driver_path() -> str:
    """Locate the ADBC Postgres driver shared library shipped with the venv."""
    import adbc_driver_postgresql

    return os.path.join(
        os.path.dirname(adbc_driver_postgresql.__file__),
        "libadbc_driver_postgresql.so",
    )


def register_datasources(datasources) -> None:
    """Register each Python datasource with the Rust engine (by name)."""
    import fedqrs

    from ..datasources.postgresql import PostgreSQLDataSource
    from ..datasources.duckdb import DuckDBDataSource

    for ds in datasources:
        if isinstance(ds, PostgreSQLDataSource):
            fedqrs.register_datasource(
                ds.name,
                "postgres",
                {"uri": ds._adbc_uri(), "adbc_driver": _pg_adbc_driver_path()},
            )
        elif isinstance(ds, DuckDBDataSource):
            fedqrs.register_datasource(ds.name, "duckdb", {"path": ds.db_path})
        else:
            raise UnsupportedIR(
                f"datasource {type(ds).__name__} has no native Rust connector yet"
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
