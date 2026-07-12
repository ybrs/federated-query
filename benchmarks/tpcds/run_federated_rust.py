"""Run the FEDERATED TPC-DS benchmark (PostgreSQL + DuckDB split) through the
Rust engine (fedq-py), check correctness against cached references, and time it
against a pure-DuckDB baseline the same disciplined way the TPC-H runner does.

This adapts run_federated.py's pg-dims placement and qualification but swaps the
engine: instead of the Python FedQRuntime it drives the pyo3 ``fedq.Runtime``,
built from a temporary YAML config that registers a DuckDB source ("duck",
schema "main") and a PostgreSQL source ("pg", schema "public", read over the
ADBC driver), exactly as run_federated_rust.py does for TPC-H.

Every dimension is placed on PostgreSQL and every fact on DuckDB, so each
fact-dimension join crosses sources. Each query's federated result is compared,
row by row, against the cached pure-DuckDB truth in references_sf<sf>.duckdb
(the canonical single-source answer written by run_federated.py --mode
save-refs), using compare_results so the tolerances match the Python runner.

Timing follows benchmarks/perf_compare's discipline: both a cold and a warm
number are reported, unrounded, next to a DuckDB baseline. The DuckDB baseline
times the ORIGINAL query text against the pure-DuckDB fact+dim file (every table
local); the references db caches RESULTS, this baseline is the TIME reference.
The ratio and the tpch-style totals compare ours against that baseline.

  --warm-runs N (default 0): with N == 0 each query runs once on a shared
    runtime (the historical behavior), reported as the cold-ISH column. With
    N > 0 the first run stays the cold-ish column and the median of N subsequent
    runs on the live runtime is the warm column.
  --cold-process: run every query in a fresh child process with a fresh runtime,
    the strict cold definition of benchmarks/perf_compare (no plan cache, pooled
    connection, or cached statistic can leak between queries).

Each query is classified OK (matches the reference), WRONG (runs but differs),
or ERROR (raised in the engine). Every per-query exception is caught and
recorded so one failure cannot stop the whole run. The Rust engine's 100ms
planning budget surfaces as an ERROR ("planning budget exceeded") for the
heaviest queries; that is reported as-is, never worked around.
"""

import argparse
import datetime
import glob
import multiprocessing
import os
import re
import sys
import tempfile
import time

import duckdb
import pyarrow as pa
import sqlglot
from sqlglot import exp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fedq

from run_federated import _oracle_ms

from compare import compare_results
from generate import (
    _db_path,
    pg_database_name,
    DEFAULT_DATA_DIR,
    DEFAULT_QUERIES_DIR,
)
from qualify import TPCDS_TABLES


HERE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(HERE_DIR, "reports")

# The seven TPC-DS fact tables; everything else is a dimension. This mirrors
# run_federated.py's FACT_TABLES for the pg-dims placement.
FACT_TABLES = frozenset(
    {
        "store_sales",
        "store_returns",
        "catalog_sales",
        "catalog_returns",
        "web_sales",
        "web_returns",
        "inventory",
    }
)

ENGINE_DIALECT = "postgres"

# Engine three-part names: pg tables as pg.public.X, duck tables as duck.main.X.
FEDQ_SOURCES = {"pg": ("pg", "public"), "duck": ("duck", "main")}

# ADBC Postgres driver locations searched in order; first existing path wins.
ADBC_CANDIDATES = [
    "/workspace/venv-fedq/lib/python3.13/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    "/workspace/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
]


def _pg_dims_placement():
    """Map every dimension to "pg" and every fact to "duck"."""
    placement = {}
    for table in TPCDS_TABLES:
        placement[table] = "duck" if table in FACT_TABLES else "pg"
    return placement


PG_DIMS = _pg_dims_placement()


def _adbc_driver_path():
    """Return the first ADBC Postgres driver shared library that exists."""
    for candidate in ADBC_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    raise SystemExit("ADBC Postgres driver not found in: {0}".format(ADBC_CANDIDATES))


def write_config(db_path, database, adbc_driver, options):
    """Write a temp YAML config: a DuckDB source and a PostgreSQL source."""
    text = (
        "datasources:\n"
        "  duck:\n"
        "    type: duckdb\n"
        "    path: {db_path}\n"
        "    read_only: true\n"
        "  pg:\n"
        "    type: postgres\n"
        "    host: {host}\n"
        "    port: {port}\n"
        "    user: {user}\n"
        "    password: {password}\n"
        "    database: {database}\n"
        "    schemas:\n"
        "      - public\n"
        "    adbc_driver: {adbc_driver}\n"
    ).format(
        db_path=db_path, host=options.pg_host, port=options.pg_port,
        user=options.pg_user, password=options.pg_password,
        database=database, adbc_driver=adbc_driver,
    )
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def _qualify(sql, source_map, dialect):
    """Qualify each base TPC-DS table to the source that holds it under pg-dims."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in TPCDS_TABLES or table.args.get("db"):
            continue
        catalog, schema = source_map[PG_DIMS[name]]
        table.set("db", exp.to_identifier(schema))
        if catalog is not None:
            table.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect=dialect)


def arrow_to_rows(stream):
    """Consume an Arrow C-stream export into a list of positional row tuples."""
    table = pa.table(stream)
    columns = []
    for index in range(table.num_columns):
        columns.append(table.column(index).to_pylist())
    rows = []
    for row in zip(*columns):
        rows.append(tuple(row))
    return rows


def _read_query(path):
    """Read the original DuckDB query text from a .sql file."""
    with open(path) as handle:
        return handle.read()


def _reference_rows(refs, name):
    """Read the cached pure-DuckDB truth rows for a query, in ORDER BY order."""
    query = 'SELECT * EXCLUDE (__ord) FROM "{0}" ORDER BY __ord'.format(name)
    return refs.execute(query).fetchall()


def _median_ms(thunk, warm_runs):
    """Time one COLD-ish run then WARM repeats; return (cold_ms, warm_ms, result).

    The first call is the cold-ish run reported as the cold column. When
    warm_runs is 0 no repeats run and warm_ms is None. Otherwise warm_ms is the
    median of warm_runs subsequent runs (plan-cache hits on the live runtime).
    The last executed result is returned for the correctness comparison.
    """
    start = time.perf_counter()
    result = thunk()
    cold_ms = (time.perf_counter() - start) * 1000.0
    times = []
    for _ in range(warm_runs):
        start = time.perf_counter()
        result = thunk()
        times.append((time.perf_counter() - start) * 1000.0)
    warm_ms = None
    if times:
        times.sort()
        warm_ms = times[len(times) // 2]
    return cold_ms, warm_ms, result


def _metric_ms(cold_ms, warm_ms):
    """The comparable time for a run: the warm median when repeats ran, else the
    single cold-ish run. This is the number that feeds the ratio and totals."""
    if warm_ms is not None:
        return warm_ms
    return cold_ms


def _error_result(name, error):
    """Assemble an ERROR record for a query that raised in the engine (a planning
    budget kill surfaces here carrying the engine's own message)."""
    detail = str(error).strip().splitlines()
    message = detail[0] if detail else type(error).__name__
    return {
        "name": name, "status": "ERROR",
        "reason": "{0}: {1}".format(type(error).__name__, message),
        "engine_rows": None, "truth_rows": None,
        "cold_ms": None, "warm_ms": None, "duck_ms": None, "ratio": None,
    }


def _compared_result(name, engine_rows, truth_rows, decimals, cold_ms, warm_ms, duck_ms):
    """Compare engine rows against the reference truth, classify OK/WRONG, and
    attach the cold/warm engine timings, the DuckDB baseline, and the ratio."""
    match, reason = compare_results(engine_rows, truth_rows, decimals)
    ours_ms = _metric_ms(cold_ms, warm_ms)
    ratio = ours_ms / duck_ms if duck_ms else None
    return {
        "name": name, "status": "OK" if match else "WRONG", "reason": reason,
        "engine_rows": len(engine_rows), "truth_rows": len(truth_rows),
        "cold_ms": cold_ms, "warm_ms": warm_ms, "duck_ms": duck_ms,
        "ratio": ratio,
    }


def evaluate_query(runtime, refs, path, decimals, warm_runs):
    """Run one query through the Rust engine, time it (cold-ish plus warm
    median), verify against the reference, classify it.

    The DuckDB baseline time is READ from the `oracle_timings` table the staged
    reference build saved into the references db (run_federated.py --mode
    save-refs measures every query ONCE per dataset) - the baseline is a
    function of the data, never re-measured per run."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    engine_sql = _qualify(raw, FEDQ_SOURCES, ENGINE_DIALECT)
    try:
        cold_ms, warm_ms, engine_rows = _median_ms(
            lambda: arrow_to_rows(runtime.execute(engine_sql)), warm_runs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        # A Rust panic surfaces as pyo3_runtime.PanicException, which subclasses
        # BaseException (not Exception), so catch BaseException here to record a
        # panic as a per-query ERROR instead of aborting the whole run.
        return _error_result(name, error)
    truth_rows = _reference_rows(refs, name)
    return _compared_result(
        name, engine_rows, truth_rows, decimals, cold_ms, warm_ms,
        _oracle_ms(refs, name))


def _ms_text(value):
    """A milliseconds cell like '123.4', or '-' when there is no timing."""
    if value is None:
        return "-"
    return "{0:.1f}".format(value)


def _ratio_text(value):
    """An ours/DuckDB ratio cell like '1.51x', or '-' when unavailable."""
    if value is None:
        return "-"
    return "{0:.2f}x".format(value)


def _print_result(result):
    """Print one query's outcome row: status, cold/warm/duck ms, ratio, reason."""
    reason = result["reason"]
    if len(reason) > 40:
        reason = reason[:40] + "..."
    print("{0:5} {1:6} {2:>9} {3:>9} {4:>9} {5:>7}  {6}".format(
        result["name"], result["status"], _ms_text(result["cold_ms"]),
        _ms_text(result["warm_ms"]), _ms_text(result["duck_ms"]),
        _ratio_text(result["ratio"]), reason), flush=True)


def _tally(results):
    """Count OK / WRONG / ERROR across a result list."""
    tally = {"OK": 0, "WRONG": 0, "ERROR": 0}
    for result in results:
        tally[result["status"]] += 1
    return tally


def _geomean(ratios):
    """Geometric mean of positive ratios (0.0 for an empty list)."""
    if not ratios:
        return 0.0
    product = 1.0
    for ratio in ratios:
        product *= ratio
    return product ** (1.0 / len(ratios))


def _timing_totals(results):
    """Sum ours and DuckDB time and collect ratios over OK queries that have a
    DuckDB baseline; returns (ours_total_ms, duck_total_ms, ratios)."""
    ours_total = 0.0
    duck_total = 0.0
    ratios = []
    for result in results:
        if result["status"] != "OK" or not result["duck_ms"]:
            continue
        ours_total += _metric_ms(result["cold_ms"], result["warm_ms"])
        duck_total += result["duck_ms"]
        ratios.append(result["ratio"])
    return ours_total, duck_total, ratios


def _ratio_summary_line(ours_total, duck_total, ratios):
    """The tpch-style ours-vs-DuckDB totals line (seconds), or a note when no
    query carried a baseline timing."""
    if not ratios:
        return "ours -  duckdb -  ->  no DuckDB baseline timings measured"
    total_ratio = ours_total / duck_total if duck_total else float("nan")
    return ("ours {0:.1f}s  duckdb {1:.1f}s  ->  total {2:.2f}x  geomean {3:.2f}x  "
            "({4} OK queries measured)".format(
                ours_total / 1000.0, duck_total / 1000.0, total_ratio,
                _geomean(ratios), len(ratios)))


def _print_summary(results, tally, total_ms):
    """Print the OK/WRONG/ERROR tally and the tpch-style timing ratio line."""
    ours_total, duck_total, ratios = _timing_totals(results)
    print("-" * 72)
    print("[pg-dims] {0} ok | {1} wrong | {2} error   (total {3:.1f}s)".format(
        tally["OK"], tally["WRONG"], tally["ERROR"], total_ms / 1000.0))
    print("[pg-dims] " + _ratio_summary_line(ours_total, duck_total, ratios))


def _cluster_key(result):
    """A coarse grouping key: the reason with identifiers and numbers collapsed.

    Quoted identifiers and bare numbers are replaced with placeholders so that
    the same failure over different tables/columns clusters into one bucket.
    """
    reason = result["reason"]
    reason = re.sub(r"'[^']*'", "'X'", reason)
    reason = re.sub(r'"[^"]*"', '"X"', reason)
    reason = re.sub(r"\bq\d+\b", "qNN", reason)
    reason = re.sub(r"\d+", "N", reason)
    return reason


def _cluster_failures(results):
    """Group non-OK results by cluster key, WRONG buckets before ERROR ones."""
    clusters = {}
    for result in results:
        if result["status"] == "OK":
            continue
        key = _cluster_key(result)
        clusters.setdefault(key, {"status": result["status"], "members": []})
        clusters[key]["members"].append(result["name"])
    return clusters


def _cluster_sort_key(item):
    """Sort clusters: WRONG before ERROR, then by descending size, then label."""
    key, bucket = item
    status_rank = 0 if bucket["status"] == "WRONG" else 1
    return (status_rank, -len(bucket["members"]), key)


def _grouped_lines(results):
    """Build the grouped non-OK section, WRONG clusters first, largest first."""
    ordered = sorted(_cluster_failures(results).items(), key=_cluster_sort_key)
    lines = []
    for key, bucket in ordered:
        names = ", ".join(bucket["members"])
        lines.append("### {0} ({1}) [{2}]".format(
            key, len(bucket["members"]), bucket["status"]))
        lines.append("Queries: {0}".format(names))
        lines.append("")
    return lines


def _matrix_row(result):
    """Render one query's per-query markdown table row."""
    message = result["reason"] if result["reason"] else "rows and values match"
    rows_cell = "-"
    if result["engine_rows"] is not None:
        rows_cell = "{0} / {1}".format(result["engine_rows"], result["truth_rows"])
    return "| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} |".format(
        result["name"], result["status"], _ms_text(result["cold_ms"]),
        _ms_text(result["warm_ms"]), _ms_text(result["duck_ms"]),
        _ratio_text(result["ratio"]), rows_cell, message.replace("|", "\\|"))


def _matrix_lines(results):
    """Build the per-query markdown table (status, timings, rows, message)."""
    lines = [
        "| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio "
        "| Rows engine/truth | Message |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        lines.append(_matrix_row(result))
    return lines


def _timing_summary_lines(results):
    """The tpch-style timing totals table over OK queries with a DuckDB baseline."""
    ours_total, duck_total, ratios = _timing_totals(results)
    if not ratios:
        return []
    total_ratio = ours_total / duck_total if duck_total else float("nan")
    return [
        "## Timing summary (OK queries with a DuckDB baseline)",
        "",
        "| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |",
        "| --- | --- | --- | --- | --- |",
        "| {0:.1f} | {1:.1f} | {2:.2f}x | {3:.2f}x | {4} |".format(
            ours_total / 1000.0, duck_total / 1000.0, total_ratio,
            _geomean(ratios), len(ratios)),
        "",
    ]


def _report_lines(results, tally, total_ms, scale_factor):
    """Assemble the full markdown report for the run."""
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    ours_total, duck_total, ratios = _timing_totals(results)
    lines = [
        "# TPC-DS federated benchmark report (Rust engine)",
        "",
        "Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.",
        "Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).",
        "Truth: cached pure-DuckDB references in references_sf{0}.duckdb.".format(
            scale_factor),
        "Baseline: pure DuckDB over the fact+dim file (every table local).",
        "Generated: {0}".format(stamp),
        "",
        "Tally: {0} ok | {1} wrong | {2} error   (total {3} queries, {4:.1f}s)".format(
            tally["OK"], tally["WRONG"], tally["ERROR"], len(results),
            total_ms / 1000.0),
        "Timing: " + _ratio_summary_line(ours_total, duck_total, ratios),
        "",
        "## Non-OK queries grouped by reason",
        "",
    ]
    lines.extend(_grouped_lines(results))
    lines.extend(_timing_summary_lines(results))
    lines.append("## Per-query matrix")
    lines.append("")
    lines.extend(_matrix_lines(results))
    lines.append("")
    return lines


def write_report(results, tally, total_ms, options, path):
    """Write the per-query markdown report for this run."""
    lines = _report_lines(results, tally, total_ms, options.scale_factor)
    with open(path, "w") as handle:
        handle.write("\n".join(lines))
    print("Wrote report: {0}".format(path))


def _select_query_files(queries_dir, only):
    """Return the sorted query files, optionally filtered by --only numbers."""
    paths = sorted(glob.glob(os.path.join(queries_dir, "q*.sql")))
    if not only:
        return paths
    wanted = set()
    for token in only.split(","):
        wanted.add("q{0:02d}".format(int(token.strip())))
    selected = []
    for path in paths:
        if os.path.splitext(os.path.basename(path))[0] in wanted:
            selected.append(path)
    return selected


def _refs_path(scale_factor):
    """The per-scale cached references database file."""
    return os.path.join(
        DEFAULT_DATA_DIR, "references_sf{0}.duckdb".format(scale_factor))


def _build_context(options):
    """Assemble the shared benchmark context both run modes need: the config
    path, the DuckDB fact+dim file, the pg database, and the ADBC driver."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    database = options.pg_database or pg_database_name(options.scale_factor)
    adbc_driver = _adbc_driver_path()
    config_path = write_config(db_path, database, adbc_driver, options)
    return config_path, db_path, database, adbc_driver


def _print_header(db_path, database, adbc_driver):
    """Print the run banner and the per-query column headings."""
    print("\n==== TPC-DS pg-dims (ours=fedq-rust vs pure-DuckDB baseline) ====")
    print("db={0} pg={1} adbc={2}".format(db_path, database, adbc_driver))
    print("{0:5} {1:6} {2:>9} {3:>9} {4:>9} {5:>7}".format(
        "query", "status", "cold", "warm", "duckdb", "ratio"))


def _finish(results, total_ms, options):
    """Print the summary, write the report, and return the results."""
    tally = _tally(results)
    _print_summary(results, tally, total_ms)
    report_path = os.path.join(
        REPORTS_DIR, "rust-fed-sf{0}.md".format(options.scale_factor))
    os.makedirs(REPORTS_DIR, exist_ok=True)
    write_report(results, tally, total_ms, options, report_path)
    return results


def _run_shared(options, paths):
    """Run every query on ONE shared runtime and DuckDB baseline connection.

    The runtime persists across queries, so a query's first run is only
    cold-ISH (connections pooled, statistics session-cached by earlier
    queries). Use --cold-process for the strict cold definition.
    """
    config_path, db_path, database, adbc_driver = _build_context(options)
    runtime = fedq.Runtime(config_path)
    refs = duckdb.connect(_refs_path(options.scale_factor), read_only=True)
    _print_header(db_path, database, adbc_driver)
    print("[pg-dims] {0} queries x {1} engine run(s) each; baseline CACHED "
          "(oracle_timings in the references db)".format(
              len(paths), 1 + options.warm_runs))
    results = []
    started = time.perf_counter()
    for path in paths:
        result = evaluate_query(
            runtime, refs, path, options.decimals, options.warm_runs)
        results.append(result)
        _print_result(result)
    return _finish(results, (time.perf_counter() - started) * 1000.0, options)


def _cold_worker(config_path, db_path, refs_path, path, decimals, warm_runs, result_queue):
    """Child entry: build a FRESH runtime and DuckDB baseline, evaluate one
    query, and send its result back. A fresh runtime per query is the strict
    cold definition of benchmarks/perf_compare - no plan cache, pooled
    connection, or cached statistic can leak in from an earlier query."""
    runtime = fedq.Runtime(config_path)
    refs = duckdb.connect(refs_path, read_only=True)
    result = evaluate_query(runtime, refs, path, decimals, warm_runs)
    result_queue.put(result)


def _evaluate_cold_process(config_path, db_path, refs_path, path, options):
    """Fork a fresh child to evaluate one query under the strict cold definition."""
    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()
    process = context.Process(
        target=_cold_worker,
        args=(config_path, db_path, refs_path, path, options.decimals,
              options.warm_runs, result_queue))
    process.start()
    result = result_queue.get()
    process.join()
    return result


def _run_cold_process(options, paths):
    """Run every query in a fresh child with a fresh runtime - the strict cold
    definition of benchmarks/perf_compare. Slower than shared mode; each query
    pays the full connect + statistics + planning cost with nothing cached."""
    config_path, db_path, database, adbc_driver = _build_context(options)
    refs_path = _refs_path(options.scale_factor)
    _print_header(db_path, database, adbc_driver)
    results = []
    started = time.perf_counter()
    for path in paths:
        result = _evaluate_cold_process(config_path, db_path, refs_path, path, options)
        results.append(result)
        _print_result(result)
    return _finish(results, (time.perf_counter() - started) * 1000.0, options)


def run(options):
    """Run every selected query under the pg-dims federated split and report."""
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    if options.cold_process:
        return _run_cold_process(options, paths)
    return _run_shared(options, paths)


def _parse_args():
    """Parse command-line arguments for the federated Rust TPC-DS runner."""
    parser = argparse.ArgumentParser(description="Run federated TPC-DS on the Rust engine.")
    parser.add_argument("--scale-factor", default="0.1")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
    parser.add_argument("--warm-runs", type=int, default=0)
    parser.add_argument("--cold-process", action="store_true")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default=None)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    return parser.parse_args()


def main():
    """Entry point: run the federated TPC-DS benchmark on the Rust engine."""
    run(_parse_args())


if __name__ == "__main__":
    main()
