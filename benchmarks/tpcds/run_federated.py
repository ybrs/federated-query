"""Run the TPC-DS queries across PostgreSQL + DuckDB and check correctness.

The single-source benchmark (run.py) puts every table on one DuckDB source.
This runner splits the 24 base tables across a PostgreSQL source and a DuckDB
source (a ``placement``) and runs each query through the federated engine,
exercising its cross-source paths (cross-source joins, aggregations,
semi-joins).

Truth for the federated run is DuckDB reading the SAME split through its
``postgres`` connector: it attaches PostgreSQL and reads the pg-placed tables
via the connector and the duck-placed tables natively, so it computes the exact
federated answer fedq should produce. Both read identical data (load_postgres.py
loaded every table into both sources), so a mismatch is a real engine bug.

Correctness compares the engine's federated result against PURE DuckDB over the
same file (the canonical single-source answer); the federated DuckDB oracle
(Postgres attached) is used only for the timing baseline, since its postgres
scanner has its own quirks (dropped rows, avg-of-decimal drift) that are not our
bugs.

Each engine run happens in an isolated child process bounded by a wall-clock
timeout and a memory cap, so a cross-source query that never terminates or blows
up intermediate results becomes a clean ERROR row (Timeout / Killed) instead of
hanging or OOM-ing the whole run.
"""

import argparse
import datetime
import glob
import math
import multiprocessing
import os
import queue as queue_module
import subprocess
import threading
import time

import duckdb
import sqlglot
from sqlglot import exp

from federated_query.catalog.catalog import Catalog
from federated_query.cli.fedq import FedQRuntime
from federated_query.config.config import Config
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource

from compare import compare_results
from generate import (
    _db_path,
    pg_database_name,
    DEFAULT_DATA_DIR,
    DEFAULT_QUERIES_DIR,
)
from qualify import TPCDS_TABLES
from run import arrow_to_rows, _read_query

HERE_DIR = os.path.dirname(os.path.abspath(__file__))
# Reports are commit-named under reports/, like the TPC-H benchmark's.
REPORTS_DIR = os.path.join(HERE_DIR, "reports")

# The seven TPC-DS fact tables; everything else is a dimension.
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


def _pg_dims_placement():
    """Every dimension on PostgreSQL, every fact on DuckDB.

    Each fact-dimension join then crosses sources, which is the common TPC-DS
    shape (a large fact scanned against many small dimensions).
    """
    placement = {}
    for table in TPCDS_TABLES:
        placement[table] = "duck" if table in FACT_TABLES else "pg"
    return placement


# Each placement maps every base table to the source that holds it. "pg" is the
# PostgreSQL source, "duck" the DuckDB source.
PLACEMENTS = {
    "pg-dims": _pg_dims_placement(),
    # Split sales facts from their matching returns and alternate the dimensions
    # so fact-fact and fact-dimension joins are forced across sources.
    "adversarial": {
        "store_sales": "duck",
        "store_returns": "pg",
        "catalog_sales": "pg",
        "catalog_returns": "duck",
        "web_sales": "duck",
        "web_returns": "pg",
        "inventory": "pg",
        "call_center": "pg",
        "catalog_page": "duck",
        "customer": "pg",
        "customer_address": "duck",
        "customer_demographics": "pg",
        "date_dim": "pg",
        "household_demographics": "duck",
        "income_band": "pg",
        "item": "duck",
        "promotion": "pg",
        "reason": "duck",
        "ship_mode": "pg",
        "store": "duck",
        "time_dim": "pg",
        "warehouse": "duck",
        "web_page": "pg",
        "web_site": "duck",
    },
}

ENGINE_DIALECT = "postgres"

# How each source kind names its schema for the engine (three-part references)
# and for the DuckDB oracle (pg tables via the attached "pgdb", duck tables
# left bare so they resolve against the local DuckDB file).
FEDQ_SOURCES = {"pg": ("pg", "public"), "duck": ("duck", "main")}
ORACLE_SOURCES = {"pg": ("pgdb", "public"), "duck": (None, "main")}


def _database(options):
    """The PostgreSQL database to read: an explicit --pg-database, otherwise the
    scale's dedicated TPC-DS database (kept separate from the TPC-H benchmark)."""
    return options.pg_database or pg_database_name(options.scale_factor)


def _pg_config(options):
    """PostgreSQL connection config for the engine's connector."""
    return {
        "host": options.pg_host,
        "port": int(options.pg_port),
        "database": _database(options),
        "user": options.pg_user,
        "password": options.pg_password,
        "schemas": ["public"],
    }


def _pg_dsn(options):
    """The libpq DSN DuckDB's postgres extension attaches through."""
    return (
        f"dbname={_database(options)} user={options.pg_user} "
        f"password={options.pg_password} host={options.pg_host} "
        f"port={options.pg_port}"
    )


def build_fedq(db_path, options):
    """Build the engine runtime with a PostgreSQL and a DuckDB source."""
    duck = DuckDBDataSource("duck", {"path": db_path, "read_only": True})
    duck.connect()
    postgres = PostgreSQLDataSource("pg", _pg_config(options))
    postgres.connect()
    catalog = Catalog()
    catalog.register_datasource(duck)
    catalog.register_datasource(postgres)
    catalog.load_metadata()
    return FedQRuntime(catalog, Config())


def build_oracle(db_path, options):
    """Open DuckDB over the dataset with PostgreSQL attached via the connector.

    Disable the postgres extension's filter pushdown: it is buggy (and flagged
    'experimental') - on q59 it mis-pushes a filter to Postgres and the scan
    returns 0 rows, so the oracle produced a wrong 0-row answer (verified: pure
    DuckDB and our engine both return 100). The oracle is the correctness
    reference, so it must be right even at the cost of reading unfiltered dim
    rows and filtering locally.
    """
    connection = _reference_connection(db_path, options)
    connection.execute("LOAD postgres")
    connection.execute(
        f"ATTACH '{_pg_dsn(options)}' AS pgdb (TYPE postgres, READ_ONLY)"
    )
    connection.execute("SET pg_experimental_filter_pushdown=false")
    return connection


def _reference_connection(db_path, options):
    """A DuckDB reference connection CAPPED below the child's RSS watchdog.

    The engine and both references share one watchdog-limited child; an
    uncapped reference computing a heavy SF10 truth (q64 joins three facts)
    can blow the child's RSS limit and be misattributed as an ENGINE error.
    DuckDB degrades gracefully under its own memory_limit (it spills), so the
    references get ~60 percent of the watchdog budget.
    """
    connection = duckdb.connect(db_path, read_only=True)
    if options.memory_limit > 0:
        cap_mb = max(2048, int(options.memory_limit * 0.6))
        connection.execute(f"SET memory_limit='{cap_mb}MB'")
    return connection


def _error_text(error):
    """Render an exception as a single-line ``Type: message`` string."""
    lines = str(error).strip().splitlines()
    detail = lines[0] if lines else type(error).__name__
    return "{0}: {1}".format(type(error).__name__, detail)


def _current_rss():
    """This process's resident memory in bytes (Linux /proc)."""
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


def _start_memory_watchdog(memory_limit_mb):
    """Kill this child (exit 137) the moment its REAL resident memory crosses
    the cap, checked on a background thread.

    RSS, not RLIMIT_AS: the Rust engine reserves a large VIRTUAL address space
    (DataFusion/tokio/DuckDB arenas) at low RSS, so an address-space cap fires
    on queries whose real memory is well within budget - e.g. q72's oracle uses
    6 GB RSS but enough virtual, alongside the engine, to trip a 12 GB AS cap.
    """
    if memory_limit_mb <= 0:
        return
    limit_bytes = memory_limit_mb * 1024 * 1024

    def watch():
        while True:
            if _current_rss() > limit_bytes:
                os.write(2, b"MEMORY LIMIT EXCEEDED - killing query\n")
                os._exit(137)
            time.sleep(0.2)

    threading.Thread(target=watch, daemon=True).start()


def _run_engine(runtime, engine_sql):
    """Time the COLD and the WARM execution; return (cold_ms, warm_ms, rows).

    Each query runs in a fresh child, so the FIRST execution is genuinely
    COLD: source connections, statistics fetches (and the statless-table
    probe), the full planning pipeline, and the Rust engine's setup all pay
    here. The SECOND execution is WARM: connections pooled, stats
    session-cached, the plan cache HIT. Reporting BOTH keeps cold-path
    regressions visible - a feature that only helps warm runs must not
    silently tax first-sight queries."""
    start = time.perf_counter()
    runtime.execute(engine_sql)
    cold_ms = (time.perf_counter() - start) * 1000.0
    start = time.perf_counter()
    result = runtime.execute(engine_sql)
    warm_ms = (time.perf_counter() - start) * 1000.0
    return cold_ms, warm_ms, arrow_to_rows(result)


def _run_oracle(oracle, oracle_sql):
    """Warm the DuckDB-over-Postgres oracle, then time one execution."""
    oracle.execute(oracle_sql).fetchall()
    start = time.perf_counter()
    rows = oracle.execute(oracle_sql).fetchall()
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return elapsed_ms, rows


def _evaluate_worker(
    engine_sql, oracle_sql, truth_sql, db_path, options, decimals, result_queue
):
    """Child-process entry: run the query, verify it, and time the DuckDB
    baseline - all in ONE process (so the engine's read-write DuckDB open never
    collides with a read-only lock across processes).

    CORRECTNESS compares the engine's federated result against PURE DuckDB over
    the same file (truth_sql): pure DuckDB reads every table locally, so it
    cannot hit the postgres-scanner quirks the federated oracle does (q59's
    dropped rows, q18's avg-of-decimal drift). TIMING uses the federated oracle
    (DuckDB with Postgres attached) as the "DuckDB over federated Postgres"
    baseline; its rows are NOT trusted for correctness and a failure there only
    drops the timing number, never the correctness verdict.
    """
    _start_memory_watchdog(options.memory_limit)
    try:
        runtime = build_fedq(db_path, options)
        engine_cold_ms, engine_ms, engine_rows = _run_engine(runtime, engine_sql)
    except Exception as error:
        result_queue.put(("error", _error_text(error)))
        return
    try:
        truth_rows = (
            _reference_connection(db_path, options).execute(truth_sql).fetchall()
        )
    except Exception as error:
        result_queue.put(("error", "ground-truth: " + _error_text(error)))
        return
    oracle_ms = _time_federated_oracle(db_path, options, oracle_sql)
    match, reason = compare_results(engine_rows, truth_rows, decimals)
    result_queue.put(
        (
            "done",
            (
                "PASS" if match else "MISMATCH",
                reason,
                len(engine_rows),
                len(truth_rows),
                engine_ms,
                oracle_ms,
                engine_cold_ms,
            ),
        )
    )


# The federated oracle's share of the child timeout: past it the oracle is
# INTERRUPTED (duckdb Connection.interrupt is thread-safe) and the query keeps
# its correctness verdict with no oracle timing - the summary already renders
# a PASS with no timing.
ORACLE_TIMEOUT_FRACTION = 0.5


def _time_federated_oracle(db_path, options, oracle_sql):
    """Time DuckDB-over-Postgres for the baseline, or None if it cannot run.

    The federated oracle is only a timing reference (its result is not trusted
    for correctness), so any failure - an OOM, a scanner error, an unsupported
    shape - must not fail an otherwise-correct query; it just yields no timing.
    """
    try:
        oracle = build_oracle(db_path, options)
        # The oracle gets its OWN time budget: a reference that neither errors
        # nor finishes (duck's postgres scanner grinding through a mis-ordered
        # spilling join - q85 adversarial) must yield "no timing", not burn the
        # whole child timeout and misreport the ENGINE as an ERROR.
        timer = threading.Timer(
            options.timeout * ORACLE_TIMEOUT_FRACTION, oracle.interrupt
        )
        timer.start()
        try:
            oracle_ms, _ = _run_oracle(oracle, oracle_sql)
        finally:
            timer.cancel()
        return oracle_ms
    except Exception:
        return None


def _finish_stalled_worker(process, timeout_s):
    """Handle a worker that sent no result: distinguish timeout from a hard kill."""
    process.join(1)
    if process.is_alive():
        process.terminate()
        process.join()
        return None, "Timeout: exceeded {0}s".format(timeout_s)
    reason = "Killed: worker exited with code {0} (likely memory limit)".format(
        process.exitcode
    )
    return None, reason


def _run_isolated(engine_sql, oracle_sql, truth_sql, db_path, options):
    """Run one query's engine evaluation (verified against pure DuckDB, timed
    against the federated oracle) in a child process bounded by timeout and
    memory. Returns (outcome_tuple, None) or (None, error_text)."""
    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()
    process = context.Process(
        target=_evaluate_worker,
        args=(
            engine_sql,
            oracle_sql,
            truth_sql,
            db_path,
            options,
            options.decimals,
            result_queue,
        ),
    )
    process.start()
    return _await_result(process, result_queue, options.timeout)


def _await_result(process, result_queue, timeout_s):
    """Wait for the worker's result, detecting a memory-watchdog kill promptly.

    The watchdog exits the child (137) without queuing anything, so a plain
    blocking get would wait the whole timeout; poll liveness between short gets
    and report the kill as soon as the child is gone."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        outcome = _poll_worker(process, result_queue, timeout_s)
        if outcome is not None:
            return outcome
    return _finish_stalled_worker(process, timeout_s)


def _poll_worker(process, result_queue, timeout_s):
    """One short poll: the classified outcome, a stall verdict if the child
    died, or None to keep waiting."""
    try:
        status, payload = result_queue.get(timeout=0.25)
    except queue_module.Empty:
        if process.is_alive():
            return None
        return _finish_stalled_worker(process, timeout_s)
    process.join()
    if status == "done":
        return payload, None
    return None, payload


def _qualify(sql, placement, source_map, dialect):
    """Qualify each base table to the source that holds it under a placement."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in TPCDS_TABLES or table.args.get("db"):
            continue
        catalog, schema = source_map[placement[name]]
        table.set("db", exp.to_identifier(schema))
        if catalog is not None:
            table.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect=dialect)


def _query_sources(sql, placement):
    """The set of source kinds the base tables of a query resolve to."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    sources = set()
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name in TPCDS_TABLES:
            sources.add(placement[name])
    return sources


def evaluate_query(path, placement, db_path, options):
    """Run one query, verify the engine's federated result against pure DuckDB,
    time it against the federated oracle, and classify it.

    truth_sql is the raw query run against pure DuckDB (unqualified names resolve
    to the file's main schema) - the correctness reference. engine_sql / oracle_sql
    are qualified to the federated split for the engine and the timing oracle.
    """
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    cross = "cross" if len(_query_sources(raw, placement)) > 1 else "single"
    engine_sql = _qualify(raw, placement, FEDQ_SOURCES, ENGINE_DIALECT)
    oracle_sql = _qualify(raw, placement, ORACLE_SOURCES, "duckdb")
    outcome, error = _run_isolated(engine_sql, oracle_sql, raw, db_path, options)
    if error is not None:
        return _result(name, "ERROR", error, cross, None, None)
    (status, reason, engine_rows, oracle_rows,
     engine_ms, oracle_ms, engine_cold_ms) = outcome
    return _result(
        name, status, reason, cross, engine_rows, oracle_rows,
        engine_ms, oracle_ms, engine_cold_ms,
    )


def _result(
    name,
    status,
    reason,
    cross,
    engine_rows,
    oracle_rows,
    engine_ms=None,
    oracle_ms=None,
    engine_cold_ms=None,
):
    """Assemble one query's outcome record (timings are None for ERROR rows).
    engine_ms is the WARM run (plan cache hit, pooled connections, cached
    stats); engine_cold_ms is the fresh child's FIRST execution - the
    cold-path number a warm-only report would let regress silently."""
    return {
        "name": name,
        "status": status,
        "reason": reason,
        "span": cross,
        "engine_rows": engine_rows,
        "oracle_rows": oracle_rows,
        "engine_ms": engine_ms,
        "oracle_ms": oracle_ms,
        "engine_cold_ms": engine_cold_ms,
    }


def _print_result(result):
    """Print one query's outcome row."""
    reason = result["reason"]
    if len(reason) > 80:
        reason = reason[:80] + "..."
    print(
        "{0:5} {1:8} {2:7} {3}".format(
            result["name"], result["status"], result["span"], reason
        ),
        flush=True,
    )


def _tally(results):
    """Count PASS / MISMATCH / ERROR and cross-source queries in a result list."""
    tally = {"PASS": 0, "MISMATCH": 0, "ERROR": 0}
    cross = 0
    for result in results:
        tally[result["status"]] += 1
        if result["span"] == "cross":
            cross += 1
    return tally, cross


def _summary_line(placement_name, results):
    """Return the one-line tally for a placement as text."""
    tally, cross = _tally(results)
    return (
        "[{0}] Total {1} | PASS {2} | MISMATCH {3} | ERROR {4} | "
        "cross-source {5}".format(
            placement_name,
            len(results),
            tally["PASS"],
            tally["MISMATCH"],
            tally["ERROR"],
            cross,
        )
    )


def _print_summary(placement_name, results):
    """Print the pass/mismatch/error tally and cross-source count."""
    print("-" * 60)
    print(_summary_line(placement_name, results))


def _timing_rows(results):
    """PASS queries that have both timings, as (name, engine_ms, duck_ms, ratio).

    duck_ms is the DuckDB-over-Postgres oracle; ratio = engine_ms / duck_ms, so
    >1 means the engine is slower than DuckDB federating the same split.
    """
    rows = []
    for result in results:
        if result["status"] != "PASS" or result["engine_ms"] is None:
            continue
        duck_ms = result["oracle_ms"]
        if duck_ms is None or duck_ms <= 0:
            # A PASS whose TIMING oracle failed (e.g. it exceeded its own
            # memory cap): correctness stands, there is just no baseline to
            # compare against, so the row is excluded from the timing table.
            continue
        engine_ms = result["engine_ms"]
        rows.append((result["name"], engine_ms, duck_ms, engine_ms / duck_ms))
    return rows


def _geomean(values):
    """Geometric mean of positive values (0.0 if empty)."""
    if not values:
        return 0.0
    log_sum = 0.0
    for value in values:
        log_sum += math.log(value)
    return math.exp(log_sum / len(values))


def _timing_table_lines(placement_name, results):
    """Markdown lines: the timing totals for PASS queries with both timings.

    Per-query timings live in the per-query matrix; this is the tpch-style
    summary row - totals, ratio, geomean, and how many queries were measured.
    """
    rows = _timing_rows(results)
    if not rows:
        return []
    ours_total = 0.0
    duck_total = 0.0
    ratios = []
    for name, engine_ms, duck_ms, ratio in rows:
        ours_total += engine_ms
        duck_total += duck_ms
        ratios.append(ratio)
    cold_total = _cold_total(results)
    cold_cell = "-" if cold_total is None else "{0:.1f}".format(cold_total)
    return [
        "",
        "### Timing summary (PASS only): engine vs "
        "DuckDB-over-Postgres [{0}]".format(placement_name),
        "",
        "| Warm (ms) | Cold (ms) | DuckDB (ms) | Ratio | Geomean | Measured |",
        "| --- | --- | --- | --- | --- | --- |",
        "| {0:.1f} | {1} | {2:.1f} | {3:.2f}x | {4:.2f}x | {5} |".format(
            ours_total, cold_cell, duck_total, ours_total / duck_total,
            _geomean(ratios), len(rows)
        ),
        "",
    ]


def _cold_total(results):
    """The summed COLD engine time over PASS queries, or None when no result
    carries one (older records). Reported next to the warm total so a change
    helping only warm runs cannot silently tax first-sight queries."""
    total = 0.0
    seen = False
    for result in results:
        if result["status"] != "PASS":
            continue
        cold_ms = result.get("engine_cold_ms")
        if cold_ms is not None:
            total += cold_ms
            seen = True
    return total if seen else None


def _print_timing_summary(placement_name, results):
    """Print the engine-vs-DuckDB-over-Postgres timing table to stdout."""
    for line in _timing_table_lines(placement_name, results):
        print(line)


# Ordered failure signatures for the federated run: the first substring found in
# a reason names the cluster. Includes cross-source-only causes (Postgres text
# decode, cross-source column resolution) on top of the engine-limitation ones.
# An unmatched failure falls into "Other" so it is never hidden.
ERROR_CATEGORIES = [
    ("Out of Memory", "Out of memory"),
    ("arrow_scan", "Out of memory"),
    ("Timeout", "Timeout"),
    ("Killed", "Memory limit (killed)"),
    ("UnicodeDecodeError", "Postgres text decode (UnicodeDecodeError)"),
    ("ColumnResolutionError", "Cross-source column resolution"),
    ("Unresolved column reference", "Cross-source column resolution"),
    ("orient join keys", "Join-key orientation"),
    ("DecorrelationError", "Decorrelation limitation"),
    ("BinderException", "DuckDB binder (oracle side)"),
    ("BindingError", "Binding: reference not in scope"),
    ("StarExpansion", "Star over subquery/CTE"),
    ("NULLIF", "Unsupported function NULLIF"),
    ("CardinalityViolation", "Scalar subquery > 1 row"),
    ("Decimal value does not fit", "Decimal precision"),
    ("simple CASE", "Simple CASE unsupported"),
    ("window functions are not", "Window in WHERE"),
    ("InternalException", "Internal exception"),
    ("row count differs", "Wrong result: row count"),
    ("order differs at row", "Wrong result: row order"),
    ("differs: engine=", "Wrong result: row values"),
]


def _categorize(reason):
    """Map a failure reason to a cluster label; unknown reasons are 'Other'."""
    for needle, label in ERROR_CATEGORIES:
        if needle in reason:
            return label
    return "Other"


def _cluster_failures(results):
    """Group non-PASS results by failure cluster, preserving encounter order."""
    clusters = {}
    for result in results:
        if result["status"] == "PASS":
            continue
        label = _categorize(result["reason"])
        clusters.setdefault(label, []).append(result)
    return clusters


def _cluster_sort_key(item):
    """Sort clusters by descending size, then by label for stable output."""
    label, members = item
    return (-len(members), label)


def _escape(text):
    """Escape markdown table cell separators in a reason string."""
    return text.replace("|", "\\|")


def _cluster_lines(clusters):
    """Build the failure-cluster section, largest cluster first."""
    ordered = sorted(clusters.items(), key=_cluster_sort_key)
    lines = []
    for label, members in ordered:
        names = ", ".join(member["name"] for member in members)
        lines.append("### {0} ({1})".format(label, len(members)))
        lines.append("Queries: {0}".format(names))
        lines.append("")
        lines.append("- " + _escape(members[0]["reason"]))
        lines.append("")
    return lines


def _rows_cell(result):
    """Render the engine/oracle row-count cell, or '-' when the query errored."""
    if result["engine_rows"] is None:
        return "-"
    return "{0} / {1}".format(result["engine_rows"], result["oracle_rows"])


def _ms_cell(value):
    """A milliseconds cell, or '-' when the query produced no timing."""
    if value is None:
        return "-"
    return "{0:.1f}".format(value)


def _ratio_cell(result):
    """The ours/DuckDB ratio cell, or '-' without both timings."""
    if result["engine_ms"] is None or not result["oracle_ms"]:
        return "-"
    return "{0:.2f}x".format(result["engine_ms"] / result["oracle_ms"])


def _matrix_lines(results):
    """Build the per-query markdown table (timings, status, rows, detail)."""
    lines = [
        "| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span "
        "| Rows engine/oracle | Detail |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        detail = result["reason"] if result["reason"] else "rows and values match"
        lines.append(
            "| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} |".format(
                result["name"],
                _ms_cell(result["engine_ms"]),
                _ms_cell(result["oracle_ms"]),
                _ratio_cell(result),
                result["status"],
                result["span"],
                _rows_cell(result),
                _escape(detail),
            )
        )
    return lines


def _placement_section(placement_name, results):
    """Build the report section for one placement: summary, clusters, matrix."""
    lines = [
        "## Placement: {0}".format(placement_name),
        "",
        _summary_line(placement_name, results),
        "",
        "### Failure clusters",
        "",
    ]
    lines.extend(_cluster_lines(_cluster_failures(results)))
    lines.append("### Per-query matrix")
    lines.append("")
    lines.extend(_matrix_lines(results))
    lines.append("")
    return lines


def _git(*args):
    """Run a git command in this repo and return its stripped stdout."""
    done = subprocess.run(("git",) + args, cwd=HERE_DIR, capture_output=True, text=True)
    return done.stdout.strip()


def _commit_info():
    """Return (short-hash, subject, dirty) for the current git checkout."""
    short = _git("rev-parse", "--short", "HEAD")
    subject = _git("log", "-1", "--format=%s")
    dirty = _git("status", "--porcelain") != ""
    return short, subject, dirty


def _host_line():
    """A one-line description of the CPU the benchmark ran on."""
    model = "unknown CPU"
    with open("/proc/cpuinfo") as handle:
        for line in handle:
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    return "{0} ({1} cores)".format(model, os.cpu_count())


def _report_header(options):
    """Build the report preamble: commit, host, engines, parameters."""
    short, subject, dirty = _commit_info()
    dirty_note = ""
    if dirty:
        dirty_note = "  (working tree DIRTY - results not from a clean commit)"
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    return [
        "# TPC-DS federated benchmark report",
        "",
        "Commit: `{0}` - {1}{2}".format(short, subject, dirty_note),
        "Generated: {0}".format(stamp),
        "Host: {0}".format(_host_line()),
        "Engine: fedqrs (Rust / DataFusion) - the only execution path.",
        "Oracle: DuckDB {0}.".format(duckdb.__version__),
        "",
        "Scale factor {0}, PostgreSQL + DuckDB split, per-query timeout {1}s, "
        "memory cap {2} MB. Each query's engine and DuckDB oracle (with "
        "PostgreSQL attached) run together in one isolated child process; "
        "timings are steady-state (one warm-up run discarded).".format(
            options.scale_factor, options.timeout, options.memory_limit
        ),
        "",
        "Correctness compares fedq's federated result against PURE DuckDB over "
        "the same file (every table read locally), the canonical answer - so a "
        "MISMATCH is a real engine bug, not a federation quirk of the DuckDB "
        "postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal "
        "on q18). The federated DuckDB oracle is used only for the timing "
        "baseline. Rows are compared in order, values rounded to {0} "
        "decimals.".format(options.decimals),
        "",
    ]


def write_report(sections, options, path):
    """Write the federated markdown report across every placement run."""
    lines = _report_header(options)
    for placement_name, results in sections:
        lines.extend(_placement_section(placement_name, results))
        lines.extend(_timing_table_lines(placement_name, results))
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


def run_placement(placement_name, paths, db_path, options):
    """Run every selected query under one placement and print the matrix."""
    placement = PLACEMENTS[placement_name]
    print("\n==== placement: {0} ====".format(placement_name))
    results = []
    for path in paths:
        result = evaluate_query(path, placement, db_path, options)
        results.append(result)
        _print_result(result)
    _print_summary(placement_name, results)
    _print_timing_summary(placement_name, results)
    return results


def run(options):
    """Run each requested placement over the query set.

    Each query's engine and oracle run together in one isolated child process
    (see _evaluate_worker), so nothing holds the DuckDB file open in the parent.
    ``--mode`` picks the staged variants: save-refs / engine / compare (see the
    staged-runs section at the bottom of this module); ``full`` is this
    all-in-one behavior.
    """
    if options.mode != "full":
        run_staged(options)
        return
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    sections = []
    for placement_name in options.placements.split(","):
        results = run_placement(placement_name.strip(), paths, db_path, options)
        sections.append((placement_name.strip(), results))
    write_report(sections, options, options.report or _default_report_path())


def run_staged(options):
    """Dispatch one staged step for a single placement."""
    placement_name = options.placements.split(",")[0].strip()
    if options.mode == "save-refs":
        run_save_references(options, placement_name)
    elif options.mode == "engine":
        run_engine_only(options, placement_name)
    elif options.mode == "compare":
        run_compare(options, placement_name)
    else:
        raise SystemExit("unknown --mode {0}".format(options.mode))


def _default_report_path():
    """reports/report-result-<commit>.md, like the TPC-H benchmark."""
    short, _, _ = _commit_info()
    os.makedirs(REPORTS_DIR, exist_ok=True)
    return os.path.join(REPORTS_DIR, "report-result-{0}.md".format(short))


def _parse_args():
    """Parse command-line arguments for the federated benchmark runner."""
    parser = argparse.ArgumentParser(description="Run TPC-DS across PG + DuckDB.")
    parser.add_argument("--scale-factor", default="1")
    parser.add_argument("--placements", default="pg-dims,adversarial")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--memory-limit", type=int, default=12288)
    parser.add_argument("--report", default=None)
    parser.add_argument(
        "--mode",
        default="full",
        choices=["full", "save-refs", "engine", "compare"],
    )
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default=None)
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    return parser.parse_args()


def main():
    """Entry point: run the federated TPC-DS benchmark."""
    run(_parse_args())


# --- staged runs: cached references, engine-only, timed compare ---------------
#
# The truth and oracle results are pure functions of the DATA, not of the
# engine under test - so a full three-engine run per tally wastes most of its
# wall clock re-deriving them. `--mode save-refs` runs them ONCE per dataset
# into a references DuckDB file; `--mode engine` runs only our engine and
# saves each result to CSV; `--mode compare` checks the CSVs against the
# cached truth (and reports how long comparing took). `--mode full` is the
# original all-in-one behavior.


def _refs_path(options):
    """The per-scale references database file."""
    return os.path.join(
        DEFAULT_DATA_DIR, "references_sf{0}.duckdb".format(options.scale_factor)
    )


def _engine_dir(options):
    """The per-scale directory of engine result CSVs."""
    return os.path.join(
        DEFAULT_DATA_DIR, "engine_results_sf{0}".format(options.scale_factor)
    )


def _staged_queries(options, placement):
    """(name, engine_sql, oracle_sql, truth_sql) for every selected query."""
    prepared = []
    for path in _select_query_files(options.queries_dir, options.only):
        name = os.path.splitext(os.path.basename(path))[0]
        raw = _read_query(path)
        prepared.append(
            (
                name,
                _qualify(raw, placement, FEDQ_SOURCES, ENGINE_DIALECT),
                _qualify(raw, placement, ORACLE_SOURCES, "duckdb"),
                raw,
            )
        )
    return prepared


def _refuse_existing_references(options):
    """Refuse to re-run the oracle when its results already exist. The
    references (truth + oracle_timings) are functions of the DATA, measured
    once per dataset; re-measuring them burns minutes-to-hours for nothing.
    DETERMINISTIC: presence of a populated oracle_timings table refuses,
    always - rebuilding requires DELETING the references file first, an
    explicit human act."""
    path = _refs_path(options)
    if not os.path.exists(path):
        return
    existing = duckdb.connect(path, read_only=True)
    try:
        count = existing.execute("SELECT count(*) FROM oracle_timings").fetchone()[0]
    except duckdb.CatalogException:
        return
    finally:
        existing.close()
    if count > 0:
        raise SystemExit(
            "REFUSED: {0} already holds {1} oracle timings. The oracle is "
            "never re-run while its results exist; delete the file first if "
            "the DATA changed.".format(path, count))


def run_save_references(options, placement_name):
    """Step 1: persist truth rows and oracle timings for every query."""
    _refuse_existing_references(options)
    placement = PLACEMENTS[placement_name]
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    refs = duckdb.connect(_refs_path(options))
    refs.execute(
        "CREATE OR REPLACE TABLE oracle_timings (query VARCHAR, oracle_ms DOUBLE)"
    )
    for name, _, oracle_sql, truth_sql in _staged_queries(options, placement):
        started = time.perf_counter()
        truth = _reference_connection(db_path, options).execute(truth_sql).arrow()
        refs.register("truth_tmp", truth)
        # __ord pins the truth's row order through table storage, so the
        # in-order comparison still sees the ORDER BY output order.
        refs.execute(
            'CREATE OR REPLACE TABLE "{0}" AS '
            "SELECT row_number() OVER () AS __ord, * FROM truth_tmp".format(name)
        )
        refs.unregister("truth_tmp")
        truth_ms = (time.perf_counter() - started) * 1000.0
        oracle_ms = _time_federated_oracle(db_path, options, oracle_sql)
        refs.execute("INSERT INTO oracle_timings VALUES (?, ?)", [name, oracle_ms])
        oracle_text = "-" if oracle_ms is None else "{0:.0f}ms".format(oracle_ms)
        print(
            "{0:5} truth {1:7.0f}ms  oracle {2}".format(name, truth_ms, oracle_text),
            flush=True,
        )
    print("references written: {0}".format(_refs_path(options)))


def _engine_worker(engine_sql, db_path, options, csv_path, result_queue):
    """Child: time the COLD then the WARM execution, saving the result CSV.
    Cold = the fresh child's first run (connections, stats fetches, probe,
    full planning); warm = the second (plan-cache hit, cached stats)."""
    _start_memory_watchdog(options.memory_limit)
    try:
        runtime = build_fedq(db_path, options)
        started = time.perf_counter()
        runtime.execute(engine_sql)
        cold_ms = (time.perf_counter() - started) * 1000.0
        started = time.perf_counter()
        result = runtime.execute(engine_sql)
        engine_ms = (time.perf_counter() - started) * 1000.0
        _write_engine_csv(result, csv_path)
    except Exception as error:
        result_queue.put(("error", _error_text(error)))
        return
    result_queue.put(("done", (engine_ms, result.num_rows, cold_ms)))


def _write_engine_csv(table, csv_path):
    """Write an Arrow result to CSV; NULL is the explicit sentinel ``\\N``."""
    import csv as csv_module

    with open(csv_path, "w", newline="") as handle:
        writer = csv_module.writer(handle)
        writer.writerow(table.column_names)
        for row in table.to_pylist():
            cells = []
            for column in table.column_names:
                value = row[column]
                cells.append("\\N" if value is None else str(value))
            writer.writerow(cells)


def run_engine_only(options, placement_name):
    """Step 2: run ONLY the engine per query, saving results and timings."""
    placement = PLACEMENTS[placement_name]
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    out_dir = _engine_dir(options)
    os.makedirs(out_dir, exist_ok=True)
    timings = []
    for name, engine_sql, _, _ in _staged_queries(options, placement):
        csv_path = os.path.join(out_dir, name + ".csv")
        context = multiprocessing.get_context("fork")
        result_queue = context.Queue()
        process = context.Process(
            target=_engine_worker,
            args=(engine_sql, db_path, options, csv_path, result_queue),
        )
        process.start()
        outcome, error = _await_result(process, result_queue, options.timeout)
        if error is not None:
            print("{0:5} ERROR {1}".format(name, error[:90]), flush=True)
            timings.append((name, None, None))
            continue
        engine_ms, rows, cold_ms = outcome
        timings.append((name, engine_ms, cold_ms))
        print("{0:5} warm={1:7.0f}ms cold={2:7.0f}ms  rows={3}".format(
            name, engine_ms, cold_ms, rows), flush=True)
    _write_engine_timings(out_dir, timings)
    print("engine results written: {0}".format(out_dir))


def _write_engine_timings(out_dir, timings):
    """Persist per-query engine timings next to the result CSVs.

    MERGED with any existing file, so an ``--only`` engine run refreshes just
    its queries instead of clobbering the full run's timings.
    """
    import csv as csv_module

    path = os.path.join(out_dir, "engine_timings.csv")
    merged = _load_all_engine_timings(out_dir) if os.path.exists(path) else {}
    for name, engine_ms, cold_ms in timings:
        merged[name] = (engine_ms, cold_ms)
    with open(path, "w", newline="") as handle:
        writer = csv_module.writer(handle)
        writer.writerow(["query", "engine_ms", "engine_cold_ms"])
        for name in sorted(merged):
            engine_ms, cold_ms = merged[name]
            writer.writerow([
                name,
                "" if engine_ms is None else engine_ms,
                "" if cold_ms is None else cold_ms,
            ])


def _typed_cell(value, dtype):
    """Convert one CSV cell back to the truth column's comparable type."""
    if value == "\\N":
        return None
    upper = dtype.upper()
    if upper.startswith(("DECIMAL", "DOUBLE", "FLOAT", "REAL")):
        return float(value)
    if upper.startswith(("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT")):
        return int(value)
    return value


def _read_engine_csv(csv_path, column_types):
    """Load an engine result CSV as typed rows (truth types drive parsing)."""
    import csv as csv_module

    rows = []
    with open(csv_path, newline="") as handle:
        reader = csv_module.reader(handle)
        next(reader)
        for record in reader:
            cells = []
            for value, dtype in zip(record, column_types):
                cells.append(_typed_cell(value, dtype))
            rows.append(tuple(cells))
    return rows


def run_compare(options, placement_name):
    """Step 3: compare engine CSVs against cached truth; time the comparing."""
    placement = PLACEMENTS[placement_name]
    refs = duckdb.connect(_refs_path(options), read_only=True)
    out_dir = _engine_dir(options)
    engine_ms_by_name = _load_all_engine_timings(out_dir)
    results = []
    compare_started = time.perf_counter()
    for name, _, _, _ in _staged_queries(options, placement):
        results.append(_compare_one(refs, out_dir, name, engine_ms_by_name, options))
    compare_ms = (time.perf_counter() - compare_started) * 1000.0
    for result in results:
        _print_result(result)
    _print_summary(placement_name, results)
    _print_timing_summary(placement_name, results)
    print("compare took {0:.0f}ms for {1} queries".format(compare_ms, len(results)))


def _compare_one(refs, out_dir, name, engine_ms_by_name, options):
    """Compare one query's engine CSV against its cached truth table."""
    csv_path = os.path.join(out_dir, name + ".csv")
    engine_ms, engine_cold_ms = engine_ms_by_name.get(name, (None, None))
    if engine_ms is None or not os.path.exists(csv_path):
        return _result(name, "ERROR", "no engine result saved", "cross", None, None)
    described = refs.execute('DESCRIBE "{0}"'.format(name)).fetchall()
    column_types = []
    for column in described[1:]:
        column_types.append(column[1])
    truth_rows = refs.execute(
        'SELECT * EXCLUDE (__ord) FROM "{0}" ORDER BY __ord'.format(name)
    ).fetchall()
    engine_rows = _read_engine_csv(csv_path, column_types)
    oracle_ms = _oracle_ms(refs, name)
    match, reason = compare_results(engine_rows, truth_rows, options.decimals)
    status = "PASS" if match else "MISMATCH"
    return _result(
        name,
        status,
        reason,
        "cross",
        len(engine_rows),
        len(truth_rows),
        engine_ms,
        oracle_ms,
        engine_cold_ms,
    )


def _oracle_ms(refs, name):
    """The cached federated-oracle timing for a query, or None."""
    row = refs.execute(
        "SELECT oracle_ms FROM oracle_timings WHERE query = ?", [name]
    ).fetchone()
    return None if row is None else row[0]


def _load_engine_timings(out_dir):
    """query -> WARM engine_ms from the engine step's timing file."""
    warm = {}
    for name, (engine_ms, _cold_ms) in _load_all_engine_timings(out_dir).items():
        warm[name] = engine_ms
    return warm


def _load_all_engine_timings(out_dir):
    """query -> (warm engine_ms, cold engine_ms) from the timing file. A file
    from before the cold column reads back with cold = None."""
    import csv as csv_module

    timings = {}
    path = os.path.join(out_dir, "engine_timings.csv")
    with open(path, newline="") as handle:
        reader = csv_module.reader(handle)
        next(reader)
        for row in reader:
            name = row[0]
            engine_ms = float(row[1]) if row[1] else None
            cold_ms = float(row[2]) if len(row) > 2 and row[2] else None
            timings[name] = (engine_ms, cold_ms)
    return timings


if __name__ == "__main__":
    main()
