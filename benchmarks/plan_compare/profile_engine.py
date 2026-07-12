"""Profile ONE engine on same-plan TPC-H queries: plan vs fetch vs coordinator.

Runs in its own process (old and new each link their own duckdb; loading both in
one process clashes). For each query it reports, at SF1:
  plan_ms       - planning only (parse+bind+optimize+physical)
  fetch_ms      - sum of source_scan times from the FEDQRS_PROFILE trace
  total_ms      - wall time of execute() materialized to rows (warm median of 3)
  other_ms      - total_ms - fetch_ms  (coordinator + marshal + planning-in-exec)

Usage: python profile_engine.py <old|new> q01 q06 q12 ...
"""

import os
import re
import sys
import time
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
TPCH = os.path.join(ROOT, "benchmarks", "tpch")
sys.path.insert(0, TPCH)
sys.path.insert(0, HERE)

SCALE = "1"
PG_DB = "duckpoc_sf1"

PROFILE_RE = re.compile(r"\[fedqrs\]\s+([0-9.]+)ms\s+source_scan\s+ds=(\w+)")


def _capture_fd2(thunk):
    """Run thunk with fd 2 redirected to a temp file; return (result, stderr)."""
    tmp = tempfile.NamedTemporaryFile("w+", delete=False)
    path = tmp.name
    tmp.close()
    saved = os.dup(2)
    target = os.open(path, os.O_WRONLY | os.O_TRUNC)
    os.dup2(target, 2)
    os.close(target)
    try:
        result = thunk()
    finally:
        os.dup2(saved, 2)
        os.close(saved)
    with open(path) as handle:
        text = handle.read()
    os.remove(path)
    return result, text


def _fetch_ms(trace):
    """Sum of source_scan milliseconds across all sources in one trace."""
    total = 0.0
    for match in PROFILE_RE.finditer(trace):
        total += float(match.group(1))
    return total


def _median(values):
    """Median of a small list."""
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def _profile(make_fns, queries):
    """Report COLD plan per query, each on a FRESH runtime (no cached stats).

    make_fns() returns (execute, plan_only) bound to a newly built runtime, so
    every query's first plan is genuinely cold - the number that matters, since a
    cold plan that scans data is O(data) (minutes at SF10). WARM = median of 3.
    """
    print("query   plan_cold  plan_warm  total_cold")
    for name in queries:
        raw = open(os.path.join(TPCH, "queries", name + ".sql")).read()
        execute, plan_only = make_fns()  # fresh runtime => cold stats
        start = time.time()
        plan_only(raw)
        plan_cold = (time.time() - start) * 1000
        warm = []
        for _ in range(3):
            start = time.time()
            plan_only(raw)
            warm.append((time.time() - start) * 1000)
        plan_warm = _median(warm)
        start = time.time()
        execute(raw)
        total_cold = (time.time() - start) * 1000
        print(
            "{0:6}  {1:9.1f}  {2:9.1f}  {3:10.1f}".format(
                name, plan_cold, plan_warm, total_cold
            )
        )


def _run_old(queries):
    """Profile the OLD (Python-planner -> fedqrs) engine."""
    import run_federated as rf
    from generate import _db_path, DEFAULT_DATA_DIR
    import types

    options = types.SimpleNamespace(
        scale_factor=SCALE, pg_host="localhost", pg_port="5432",
        pg_user="postgres", pg_password="postgres", pg_database=PG_DB,
    )
    db_path = _db_path(DEFAULT_DATA_DIR, SCALE)
    placement = rf.PLACEMENTS["pg-dims"]

    def qualify(raw):
        return rf._qualify(raw, placement, rf.FEDQ_SOURCES, rf.ENGINE_DIALECT)

    def make_fns():
        runtime = rf.build_fedq(db_path, options)  # fresh catalog => cold stats
        qe = runtime.query_executor

        def plan_only(raw):
            logical = qe._run_before_processors(qualify(raw))
            logical = qe._parse_query(logical)
            logical = qe._bind_plan(logical)
            logical = qe._decorrelate_plan(logical)
            logical = qe._optimize_plan(logical)
            return qe._build_physical_plan(logical)

        def execute(raw):
            return runtime.execute(qualify(raw)).num_rows

        return execute, plan_only

    _profile(make_fns, queries)


def _run_new(queries):
    """Profile the NEW (Rust) engine."""
    import fedq
    import pyarrow as pa
    from dump_new import _write_config, _adbc_driver_path
    from generate import _db_path, DEFAULT_DATA_DIR
    import run_federated as rf

    db_path = _db_path(DEFAULT_DATA_DIR, SCALE)
    config = _write_config(db_path, PG_DB, _adbc_driver_path())
    placement = rf.PLACEMENTS["pg-dims"]

    def qualify(raw):
        return rf._qualify(raw, placement, rf.FEDQ_SOURCES, rf.ENGINE_DIALECT)

    def make_fns():
        runtime = fedq.Runtime(config)  # fresh runtime => cold stats collector

        def plan_only(raw):
            return runtime.execute("EXPLAIN " + qualify(raw))

        def execute(raw):
            return pa.table(runtime.execute(qualify(raw))).num_rows

        return execute, plan_only

    _profile(make_fns, queries)


def main():
    """Dispatch to the requested engine's profiler."""
    engine = sys.argv[1]
    queries = sys.argv[2:] or ["q01", "q06", "q12", "q04"]
    os.environ["FEDQRS_PROFILE"] = "1"
    if engine == "old":
        _run_old(queries)
    elif engine == "new":
        _run_new(queries)
    else:
        raise SystemExit("engine must be old|new")


if __name__ == "__main__":
    main()
