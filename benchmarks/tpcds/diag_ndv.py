"""Trace NDV resolution for a query's dim-shipping-relevant aggregates.

Usage: python diag_ndv.py q39 [scale]

For every Aggregate in the optimized logical plan it prints, per group key:
the owner relation the cost model resolves the qualifier to, the NDV it gets
(or the default it falls back to), and the resulting group-count estimate vs
the input estimate. This is the empirical picture behind the q23/q39 gating
problem: WHY each key resolves or defaults.
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

from federated_query.plan.logical import Aggregate
from federated_query.plan.expressions import ColumnRef


def _options(scale):
    return types.SimpleNamespace(
        scale_factor=scale,
        pg_host="localhost",
        pg_port="5432",
        pg_user="postgres",
        pg_password="postgres",
        pg_database=None,
    )


def _collect_aggregates(node, out):
    if isinstance(node, Aggregate):
        out.append(node)
    for child in node.children():
        _collect_aggregates(child, out)


def _describe_key(cost, key, input_node, input_rows):
    """One group key: owner, NDV, defaulted?"""
    if isinstance(key, ColumnRef) and key.table is not None:
        owner = cost._find_relation(input_node, key.table)
        owner_name = type(owner).__name__ if owner is not None else "NONE"
        ndv = cost._owner_column_ndv(owner, key.column) if owner is not None else None
        if ndv is not None:
            return f"{key.table}.{key.column}: owner={owner_name} ndv={ndv}"
        return f"{key.table}.{key.column}: owner={owner_name} ndv=DEFAULT"
    return f"{type(key).__name__}(non-column): ndv=DEFAULT"


def _report_aggregate(cost, agg, idx):
    if not agg.group_by:
        return
    input_est = cost.estimate(agg.input)
    print(f"\n--- Aggregate #{idx}  input_rows={input_est.rows} "
          f"input_defaulted={bool(input_est.defaults_used)} ---")
    print(f"    output_names={agg.output_names}")
    for key in agg.group_by:
        print("    " + _describe_key(cost, key, agg.input, input_est.rows))
    est = cost.estimate(agg)
    ratio = input_est.rows / max(1, est.rows)
    print(f"    => group_count est={est.rows}  collapse_ratio={ratio:.1f}x  "
          f"defaulted={bool(est.defaults_used)}")


def main():
    name = sys.argv[1]
    scale = sys.argv[2] if len(sys.argv) > 2 else "10"
    options = _options(scale)
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    runtime = build_fedq(db_path, options)
    placement = PLACEMENTS["pg-dims"]
    raw = _read_query(f"queries/{name}.sql")
    engine_sql = _qualify(raw, placement, FEDQ_SOURCES, "postgres")

    qe = runtime.query_executor
    rewritten = qe._run_before_processors(engine_sql)
    logical = qe._parse_query(rewritten)
    logical = qe._bind_plan(logical)
    logical = qe._decorrelate_plan(logical)
    logical = qe._optimize_plan(logical)

    cost = runtime.query_executor.planner.cost_model
    aggregates = []
    _collect_aggregates(logical, aggregates)
    print(f"===== {name} scale={scale}: {len(aggregates)} aggregate(s) =====")
    idx = 0
    for agg in aggregates:
        _report_aggregate(cost, agg, idx)
        idx += 1


if __name__ == "__main__":
    main()
