"""Dump the OLD (Python) engine's pushed SQL per query, statically.

Walks the physical plan (no execution) under the pg-dims placement and records,
per query: each source island (its datasource, whether it is a full pushdown or
a raw table pull, and its SQL), each dim shipment (temp table into a source),
and the coordinator-side operator node types (work NOT pushed to a source).

Usage: python dump_old.py <tpch|tpcds>
Writes benchmarks/plan_compare/old/<suite>/<q>.txt
"""

import glob
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, ROOT)

# Physical node types that carry a pushed source query (islands) vs. the
# coordinator operators that run work locally.
ISLAND_PUSHDOWN = "PhysicalRemoteQuery"
ISLAND_RAWSCAN = "PhysicalScan"
ISLAND_REMOTE_JOIN = "PhysicalRemoteJoin"
ISLAND_REMOTE_SETOP = "PhysicalRemoteSetOp"
SHIPMENT = "PhysicalShipment"

# Coordinator operators that indicate real work left above the source islands.
COORD_WORK = {
    "PhysicalHashJoin",
    "PhysicalNestedLoopJoin",
    "PhysicalHashAggregate",
    "PhysicalSort",
    "PhysicalLimit",
    "PhysicalUnion",
    "PhysicalSetOperation",
    "PhysicalLateralJoin",
    "PhysicalWindow",
    "PhysicalFilter",
    "PhysicalSingleRowGuard",
    "PhysicalGroupedLimit",
    "PhysicalCTEMergeQuery",
}


def _suite_module(suite):
    """Import the suite's run_federated helpers (build_fedq, _qualify, ...).

    Only tpch has a Python-engine runner; tpcds does not, so old-engine dumps
    exist only for tpch and a tpcds request exits loudly.
    """
    if suite == "tpcds":
        raise SystemExit(
            "no Python-engine tpcds runner exists; old-engine dumps are tpch-only")
    bench_dir = os.path.join(ROOT, "benchmarks", suite)
    sys.path.insert(0, bench_dir)
    import run_federated as rf
    from generate import _db_path, DEFAULT_DATA_DIR
    return rf, _db_path, DEFAULT_DATA_DIR, bench_dir


def _options(suite):
    """A minimal options namespace for build_fedq at the smallest scale."""
    scale = "0.01" if suite == "tpch" else "0.1"
    database = "duckpoc" if suite == "tpch" else None
    return types.SimpleNamespace(
        scale_factor=scale, pg_host="localhost", pg_port="5432",
        pg_user="postgres", pg_password="postgres", pg_database=database,
        memory_limit=0,
    )


def _render_island_sql(node):
    """Render a source island node's pushed SQL in canonical Postgres form."""
    cls = type(node).__name__
    if cls == ISLAND_PUSHDOWN:
        return node.query_ast.sql(dialect="postgres")
    if cls == ISLAND_RAWSCAN:
        return node._build_query()
    if cls == ISLAND_REMOTE_JOIN:
        return node._build_query()
    if cls == ISLAND_REMOTE_SETOP:
        return node.build_remote_ast().sql(dialect="postgres")
    raise ValueError("not an island node: {0}".format(cls))


def _island_datasource(node):
    """The source that runs an island node."""
    cls = type(node).__name__
    if cls == ISLAND_REMOTE_JOIN:
        return node.left.datasource
    return node.datasource


def _classify_sql(sql):
    """Classify a pushed SQL by CONTENT (fair across engines): a single-source
    OLD PhysicalScan can still carry an aggregate/filter, so the node type is
    not a reliable pushdown signal - the SQL is."""
    upper = sql.upper()
    if " JOIN " in upper:
        return "join"
    aggregates = ("GROUP BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "STDDEV")
    for marker in aggregates:
        if marker in upper:
            return "agg"
    return "scan"


def _walk(node, islands, shipments, coord_ops):
    """Recurse the physical tree, sorting nodes into islands/shipments/coord."""
    cls = type(node).__name__
    if cls in (ISLAND_PUSHDOWN, ISLAND_RAWSCAN, ISLAND_REMOTE_JOIN, ISLAND_REMOTE_SETOP):
        sql = _render_island_sql(node)
        islands.append((_island_datasource(node), _classify_sql(sql), sql))
        return
    if cls == SHIPMENT:
        shipments.append((node.table, node.datasource))
    if cls in COORD_WORK:
        coord_ops.append(cls)
    for child in node.children():
        _walk(child, islands, shipments, coord_ops)


def _build_physical(rf, qe, engine_sql):
    """Run the executor pipeline stages to the physical plan (no execution)."""
    rewritten = qe._run_before_processors(engine_sql)
    logical = qe._parse_query(rewritten)
    logical = qe._bind_plan(logical)
    logical = qe._decorrelate_plan(logical)
    logical = qe._optimize_plan(logical)
    return qe._build_physical_plan(logical)


def _pushes_agg_join(islands):
    """Whether any island pushes an aggregate or a join (structural pushdown)."""
    for _datasource, kind, _sql in islands:
        if kind in ("join", "agg"):
            return True
    return False


def _write_report(out_path, name, islands, shipments, coord_ops, error):
    """Write one query's old-engine pushed-SQL report."""
    lines = ["QUERY: {0}".format(name), "ENGINE: old (python)", ""]
    if error is not None:
        lines.append("ERROR: {0}".format(error))
        _write(out_path, lines)
        return
    lines.append("ISLANDS: {0}".format(len(islands)))
    lines.append("PUSHES_AGG_OR_JOIN: {0}".format(_pushes_agg_join(islands)))
    lines.append("SHIPMENTS: {0}".format(
        ", ".join("{0}->{1}".format(t, d) for t, d in shipments) or "none"))
    lines.append("COORDINATOR_OPS: {0}".format(", ".join(coord_ops) or "none"))
    lines.append("")
    for index, (datasource, kind, sql) in enumerate(islands):
        lines.append("--- island {0} source={1} kind={2} ---".format(
            index, datasource, kind))
        lines.append(sql)
        lines.append("")
    _write(out_path, lines)


def _write(out_path, lines):
    """Persist a report's lines."""
    with open(out_path, "w") as handle:
        handle.write("\n".join(lines))


def _dump_one(rf, qe, placement, path, out_dir):
    """Dump one query's old-engine plan, recording any error as a finding."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = open(path).read()
    out_path = os.path.join(out_dir, name + ".txt")
    islands, shipments, coord_ops, error = [], [], [], None
    try:
        engine_sql = rf._qualify(raw, placement, rf.FEDQ_SOURCES, "postgres")
        physical = _build_physical(rf, qe, engine_sql)
        _walk(physical, islands, shipments, coord_ops)
    except BaseException as caught:
        error = "{0}: {1}".format(type(caught).__name__,
                                  str(caught).strip().splitlines()[:1])
    _write_report(out_path, name, islands, shipments, coord_ops, error)
    status = "ERROR" if error else "islands={0} ship={1}".format(
        len(islands), len(shipments))
    print("{0:5} {1}".format(name, status), flush=True)


def main():
    """Dump every query in a suite through the old engine's physical planner."""
    suite = sys.argv[1]
    rf, _db_path, data_dir, bench_dir = _suite_module(suite)
    options = _options(suite)
    db_path = _db_path(data_dir, options.scale_factor)
    runtime = rf.build_fedq(db_path, options)
    qe = runtime.query_executor
    placement = rf.PLACEMENTS["pg-dims"]
    out_dir = os.path.join(HERE, "old", suite)
    paths = sorted(glob.glob(os.path.join(bench_dir, "queries", "q*.sql")))
    for path in paths:
        _dump_one(rf, qe, placement, path, out_dir)


if __name__ == "__main__":
    main()
