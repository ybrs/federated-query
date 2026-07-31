"""One perf-compare measurement worker: ONE engine, ONE source, ONE process.

Invoked only by compare.py (see README.md). The old and new engines each link
their own DuckDB, so an engine NEVER shares a process with the other. Cold
modes measure exactly one query in this fresh process; warm mode reuses one
runtime across all queries (a live session).

Modes:
  cold-plan   time ONE planning pass on a fresh runtime in a fresh process
  cold-total  time ONE execute() (includes its own planning) likewise
  warm        per query: warmup runs, then median of N plans and N executes

Output: one JSON object per line on stdout. Diagnostics go to stderr. A blown
planning budget is status "killed"; an engine that cannot build the source is
status "unsupported"; any other failure is "error". Nothing is swallowed - the
message always carries the engine's own text.
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
TPCH = os.path.join(ROOT, "benchmarks", "tpch")
# HERE first: the fedq.so symlink in this directory points at the RELEASE
# build (compare.py maintains it); TPCH supplies qualify + run_federated_rust.
sys.path.insert(0, TPCH)
sys.path.insert(0, HERE)

ENGINE_DIALECT = "postgres"
PROFILE_RE = re.compile(r"\[fedqrs\]\s+([0-9.]+)ms\s+source_scan\s+ds=(\w+)")


class UnsupportedSource(Exception):
    """The engine cannot build a runtime for this source (a real coverage gap)."""


def _duck_path(scale_factor):
    """The TPC-H DuckDB file for a scale factor; missing data fails loudly."""
    path = os.path.join(TPCH, "data", "tpch_sf{0}.duckdb".format(scale_factor))
    if not os.path.exists(path):
        raise SystemExit("missing duck data: {0} (run generate.py)".format(path))
    return path


def _parquet_dir(scale_factor):
    """The TPC-H parquet export dir for a scale factor; missing fails loudly."""
    path = os.path.join(TPCH, "data", "parquet_sf{0}".format(scale_factor))
    if not os.path.isdir(path):
        raise SystemExit(
            "missing parquet data: {0} (run export_parquet.py --scale-factor "
            "{1} --target-dir {0})".format(path, scale_factor)
        )
    return path


def _read_query(name):
    """The raw SQL of one TPC-H query by name (q01..q22)."""
    with open(os.path.join(TPCH, "queries", name + ".sql")) as handle:
        return handle.read()


def _qualify_single(sql, source_name):
    """Qualify bare TPC-H tables onto one source's `main` schema."""
    from qualify import qualify_query

    return qualify_query(sql, source_name, "main", ENGINE_DIALECT)


def _capture_fd2(thunk):
    """Run thunk with fd 2 redirected to a temp file; return (result, stderr).

    Native code (the Rust engine, fedqrs) writes its FEDQRS_PROFILE trace to
    file descriptor 2 directly, so a sys.stderr swap would not catch it.
    """
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
    # Re-emit the captured stderr so nothing is hidden from the parent log.
    sys.stderr.write(text)
    return result, text


def _fetch_ms(trace):
    """Sum of source_scan milliseconds across all sources in one trace."""
    total = 0.0
    for match in PROFILE_RE.finditer(trace):
        total += float(match.group(1))
    return total


def _median(values):
    """Median of a small list (upper median for even lengths)."""
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def _adbc_driver_path():
    """Return the ADBC Postgres driver shipped with the installed package.

    Derived from the imported ``adbc_driver_postgresql`` so it follows whichever
    interpreter runs this worker, rather than naming a venv path and Python
    version that go stale the moment either changes.
    """
    import adbc_driver_postgresql

    package_dir = os.path.dirname(adbc_driver_postgresql.__file__)
    driver = os.path.join(package_dir, "libadbc_driver_postgresql.so")
    if not os.path.exists(driver):
        raise SystemExit("adbc_driver_postgresql has no driver library at " + driver)
    return driver


def _new_config(source, args):
    """Write the NEW engine's temp YAML config for a source; return its path.

    A source kind the Rust runtime does not register raises UnsupportedSource
    when the runtime is built - reported, never skipped silently.
    """
    if source == "duck":
        text = (
            "datasources:\n"
            "  duck:\n"
            "    type: duckdb\n"
            "    path: {0}\n".format(_duck_path(args.scale_factor))
        )
    elif source == "parquet":
        text = (
            "datasources:\n"
            "  pq:\n"
            "    type: parquet\n"
            "    dir: {0}\n".format(_parquet_dir(args.scale_factor))
        )
    elif source == "pgduck":
        text = (
            "datasources:\n"
            "  duck:\n"
            "    type: duckdb\n"
            "    path: {0}\n"
            "  pg:\n"
            "    type: postgres\n"
            "    host: localhost\n"
            "    port: 5432\n"
            "    user: postgres\n"
            "    password: postgres\n"
            "    database: {1}\n"
            "    schemas:\n"
            "      - public\n"
            "    adbc_driver: {2}\n".format(
                _duck_path(args.scale_factor), args.pg_database, _adbc_driver_path()
            )
        )
    else:
        raise SystemExit("unknown source: " + source)
    if args.planning_budget_ms is not None:
        text += "optimizer:\n  planning_budget_ms: {0}\n".format(
            args.planning_budget_ms
        )
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def _build_new(source, args):
    """Runtime factory for the NEW (Rust) engine on a source."""
    import fedq

    config_path = _new_config(source, args)

    def make():
        try:
            runtime = fedq.Runtime(config_path)
        except Exception as error:
            if "unsupported type" in str(error):
                raise UnsupportedSource(str(error)) from error
            raise
        if source == "pgduck":
            import run_federated_rust as rf

            def qualify(sql):
                """Qualify sql for the pg-dims federated placement.

                The runner carries the placement as its own PG_DIMS constant,
                so the mapping is not passed in here.
                """
                return rf._qualify(sql, rf.FEDQ_SOURCES, rf.ENGINE_DIALECT)
        else:
            name = "duck" if source == "duck" else "pq"

            def qualify(sql):
                """Qualify sql for the single-source placement."""
                return _qualify_single(sql, name)
        return runtime, qualify

    return make


def _new_fns(runtime, qualify):
    """(plan_only, execute) bound to a NEW-engine runtime.

    plan_only is EXPLAIN: the Rust runtime plans fully and never executes.
    """
    import pyarrow as pa

    def plan_only(sql):
        return runtime.execute("EXPLAIN " + qualify(sql))

    def execute(sql):
        return pa.table(runtime.execute(qualify(sql))).num_rows

    return plan_only, execute


def _classify(error):
    """One measurement failure -> (status, single-line message)."""
    message = " ".join(str(error).strip().split())
    if isinstance(error, UnsupportedSource):
        return "unsupported", message
    if "planning budget exceeded" in message:
        return "killed", message
    return "error", message


def _timed(thunk):
    """Wall-clock milliseconds of one call (unrounded), with the trace."""
    start = time.perf_counter()
    _, trace = _capture_fd2(thunk)
    return (time.perf_counter() - start) * 1000.0, trace


def _emit(record):
    """Write one JSON result line to stdout, flushed for live consumption."""
    sys.stdout.write(json.dumps(record) + "\n")
    sys.stdout.flush()


def _run_cold(make, fns, mode, query):
    """One cold measurement (this whole process exists for this one number)."""
    sql = _read_query(query)
    record = {"query": query, "mode": mode}
    try:
        runtime, qualify = make()
        plan_only, execute = fns(runtime, qualify)
        if mode == "cold-plan":
            record["plan_ms"], _ = _timed(lambda: plan_only(sql))
        else:
            record["total_ms"], trace = _timed(lambda: execute(sql))
            record["fetch_ms"] = _fetch_ms(trace)
        record["status"] = "ok"
    except Exception as error:  # classified and REPORTED, never swallowed
        record["status"], record["message"] = _classify(error)
    _emit(record)


def _run_warm(make, fns, queries, warmups, runs):
    """Warm medians per query on ONE shared runtime (a live session)."""
    try:
        runtime, qualify = make()
        plan_only, execute = fns(runtime, qualify)
    except Exception as error:
        status, message = _classify(error)
        for query in queries:
            _emit(
                {"query": query, "mode": "warm", "status": status, "message": message}
            )
        return
    for query in queries:
        _emit(_warm_one(plan_only, execute, query, warmups, runs))


def _warm_one(plan_only, execute, query, warmups, runs):
    """Warm plan/execute medians for one query; failures classified per query."""
    sql = _read_query(query)
    record = {"query": query, "mode": "warm"}
    try:
        for _ in range(warmups):
            _capture_fd2(lambda: execute(sql))
        plans = []
        for _ in range(runs):
            elapsed, _ = _timed(lambda: plan_only(sql))
            plans.append(elapsed)
        totals = []
        trace = ""
        for _ in range(runs):
            elapsed, trace = _timed(lambda: execute(sql))
            totals.append(elapsed)
        record["plan_ms"] = _median(plans)
        record["total_ms"] = _median(totals)
        record["fetch_ms"] = _fetch_ms(trace)
        record["status"] = "ok"
    except Exception as error:
        record["status"], record["message"] = _classify(error)
    return record


def main():
    """Parse arguments, build the engine adapter, run the requested mode."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=["new"])
    parser.add_argument(
        "--source", required=True, choices=["duck", "parquet", "pgduck"]
    )
    parser.add_argument(
        "--mode", required=True, choices=["cold-plan", "cold-total", "warm"]
    )
    parser.add_argument(
        "--queries", required=True, help="comma-separated, e.g. q01,q06"
    )
    parser.add_argument("--scale-factor", default="1")
    parser.add_argument("--pg-database", default="duckpoc_sf1")
    parser.add_argument("--planning-budget-ms", type=int, default=None)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    os.environ["FEDQRS_PROFILE"] = "1"
    queries = args.queries.split(",")
    make, fns = _build_new(args.source, args), _new_fns
    if args.mode == "warm":
        _run_warm(make, fns, queries, args.warmups, args.runs)
    else:
        if len(queries) != 1:
            raise SystemExit("cold modes take exactly one query per process")
        _run_cold(make, fns, args.mode, queries[0])


if __name__ == "__main__":
    main()
