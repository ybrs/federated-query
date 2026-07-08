"""Dump the federated logical/physical plan for one TPC-DS query under pg-dims.

Usage: python dump_plan.py q39
"""
import sys
import types

from run_federated import (
    build_fedq,
    _qualify,
    FEDQ_SOURCES,
    PLACEMENTS,
    _db_path,
    DEFAULT_DATA_DIR,
)
from run import _read_query


def _options():
    return types.SimpleNamespace(
        scale_factor=(sys.argv[2] if len(sys.argv)>2 else "0.1"),
        pg_host="localhost",
        pg_port="5432",
        pg_user="postgres",
        pg_password="postgres",
        pg_database=None,
    )


def main():
    name = sys.argv[1]
    options = _options()
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    runtime = build_fedq(db_path, options)
    placement = PLACEMENTS["pg-dims"]
    raw = _read_query(f"queries/{name}.sql")
    engine_sql = _qualify(raw, placement, FEDQ_SOURCES, "postgres")

    # Walk the executor's own pipeline stages to reach the optimized logical plan.
    qe = runtime.query_executor
    rewritten = qe._run_before_processors(engine_sql)
    logical = qe._parse_query(rewritten)
    logical = qe._bind_plan(logical)
    logical = qe._decorrelate_plan(logical)
    logical = qe._optimize_plan(logical)
    print("===== OPTIMIZED LOGICAL PLAN =====")
    _print_logical(logical, 0)
    physical = qe._build_physical_plan(logical)
    print("===== PHYSICAL PLAN =====")
    _print_physical(physical, 0)


def _print_physical(node, depth):
    indent = "  " * depth
    cls = type(node).__name__
    extra = ""
    if cls == "PhysicalShipment":
        extra = f" table={node.table} ds={node.datasource}"
    if cls == "PhysicalRemoteQuery":
        seeded = node.seeded_schema is not None
        extra = f" ds={node.datasource} seeded={seeded} out={node.output_names}"
    if cls == "PhysicalScan":
        extra = f" {node.datasource}.{node.schema_name}.{node.table_name}"
    if cls == "PhysicalCTE":
        extra = f" name={node.name}"
    print(f"{indent}{cls}{extra}")
    for child in node.children():
        _print_physical(child, depth + 1)


def _print_logical(node, depth):
    indent = "  " * depth
    extra = ""
    cls = type(node).__name__
    if cls == "Scan":
        extra = (
            f" {node.datasource}.{node.schema_name}.{node.table_name}"
            f" alias={node.alias} cols={node.columns}"
            f" est={node.estimated_rows} filt={node.filters is not None}"
        )
    if cls == "Join":
        extra = f" {node.join_type} ON {node.condition} est={node.estimated_rows}"
    if cls == "Aggregate":
        extra = f" out={getattr(node, 'output_names', None)}"
    if cls in ("CTE", "CTERef"):
        extra = f" name={getattr(node, 'name', None)}"
    print(f"{indent}{cls}{extra}")
    for child in node.children():
        _print_logical(child, depth + 1)


if __name__ == "__main__":
    main()
