"""Reproducible TPC-H benchmark: our engine vs DuckDB, timing + correctness.

One committed entry point that measures the engine across three methodologies
and two scale factors and writes a single markdown report named after the git
commit it was run against (``reports/report-result-<commit>.md``):

  single       All eight tables in ONE Parquet source: the whole query pushes to
               the Rust engine as one plan. Pure engine vs DuckDB over the same
               Parquet. No federation.

  fedparquet   Tables split across TWO Parquet sources (facts vs dims), so most
               queries join across sources: our federation layer. DuckDB reads
               all eight files monolithically and does NOT federate, so the gap
               here overstates our federation cost - kept for reference only.

  fedpgduck    Facts on DuckDB, dims on PostgreSQL. The DuckDB oracle federates
               the SAME split via its postgres connector, so both engines cross
               the source boundary. This is the FAIR federated comparison.

Each query runs in its own subprocess bounded by a wall-clock timeout and an RSS
cap (``--memlimit-gb``): a query that hangs or blows past memory becomes a
TIMEOUT / KILLED row and the run continues. Timing is the median of three warm
runs; correctness is a differential check against DuckDB on the same data.

Usage:
    python benchmarks/tpch/bench.py                       # all cells, SF0.1 + SF1
    python benchmarks/tpch/bench.py --scales 0.1          # one scale
    python benchmarks/tpch/bench.py --cells single        # one methodology
"""

import argparse
import datetime
import json
import math
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
for _path in (REPO_ROOT, HERE):
    if _path not in sys.path:
        sys.path.insert(0, _path)

QUERY_DIR = os.path.join(HERE, "queries")
REPORT_DIR = os.path.join(HERE, "reports")

# Scale factor -> the fixed inputs it is benchmarked against. The Parquet dirs
# feed the single/fedparquet cells; the PostgreSQL database feeds fedpgduck
# (facts read from the DuckDB file, dims from this database).
PARQUET_DIRS = {"0.1": "/workspace/tpch_parquet", "1": "/workspace/tpch_parquet_sf1"}
PG_DATABASES = {"0.1": "duckpoc", "1": "duckpoc_sf1", "10": "duckpoc_sf10"}
LINEITEM_ROWS = {"0.1": "600K", "1": "6M"}

CELLS = ("single", "fedparquet", "fedpgduck")
CELL_TITLES = {
    "single": "Single source (pure engine, Parquet)",
    "fedparquet": "Federated, 2 Parquet sources (DuckDB monolithic - unfair)",
    "fedpgduck": "Federated, PostgreSQL + DuckDB (both federate - fair)",
}
# The run_tpch.py mode name backing each Parquet-based cell.
PARQUET_CELL_MODE = {"single": "single", "fedparquet": "federated"}


# ------------------------------ child: one query ---------------------------

def _bench_parquet(cell, scale, query):
    """Measure one Parquet-backed query (single / fedparquet) as a record."""
    from run_tpch import measure_one

    record = measure_one(PARQUET_CELL_MODE[cell], PARQUET_DIRS[scale], query)
    record["status"] = "OK" if record["correct"] else "MISMATCH"
    record["reason"] = ""
    return record


def _fed_options(scale):
    """The argument namespace run_federated's builders read for a scale."""
    return argparse.Namespace(
        pg_host="localhost", pg_port="5432", pg_database=PG_DATABASES[scale],
        pg_user="postgres", pg_password="postgres", scale_factor=scale,
    )


def _fed_to_record(outcome):
    """Normalize run_federated's outcome dict to the common record shape."""
    correct = outcome["status"] == "PASS"
    status = "OK" if correct else outcome["status"]
    return {"name": outcome["name"], "ours_ms": outcome["ours_ms"],
            "ours_cold_ms": outcome.get("ours_cold_ms"),
            "duck_ms": outcome["duck_ms"], "correct": correct,
            "ours_rows": outcome["engine_rows"], "duck_rows": outcome["oracle_rows"],
            "status": status, "reason": "" if correct else outcome["reason"]}


def _bench_fedpgduck(scale, query):
    """Measure one fair PG+DuckDB federated query as a record."""
    from generate import DEFAULT_DATA_DIR, _db_path
    from run_federated import PLACEMENTS, build_fedq, build_oracle, evaluate_query

    options = _fed_options(scale)
    db_path = _db_path(DEFAULT_DATA_DIR, scale)
    runtime = build_fedq(db_path, options)
    oracle = build_oracle(db_path, options)
    path = os.path.join(QUERY_DIR, query + ".sql")
    outcome = evaluate_query(runtime, oracle, path, PLACEMENTS["pg-dims"], 2)
    return _fed_to_record(outcome)


def _child_record(cell, scale, query):
    """Dispatch one query to its cell's measurement path."""
    if cell == "fedpgduck":
        return _bench_fedpgduck(scale, query)
    return _bench_parquet(cell, scale, query)


def _run_child(args):
    """Child entry: measure one query under the RSS cap, emit one JSON line."""
    from run_tpch import _start_memory_watchdog

    _start_memory_watchdog(int(args.memlimit_gb * 1024 ** 3))
    record = _child_record(args.cell, args.scale, args.query)
    print("##JSON## " + json.dumps(record), flush=True)


# ------------------------------ parent: matrix -----------------------------

def _status_record(query, status, reason=""):
    """A record for a query that produced no measurement (timeout/killed/error)."""
    return {"name": query, "ours_ms": None, "duck_ms": None, "correct": False,
            "ours_rows": None, "duck_rows": None, "status": status, "reason": reason}


def _last_stderr(stderr):
    """The last non-empty stderr line, used as an ERROR record's reason."""
    reason = ""
    for line in stderr.splitlines():
        if line.strip():
            reason = line.strip()
    return reason


def _parse_child(query, done):
    """Turn a finished child process into a record (status on non-JSON exits)."""
    if done.returncode in (137, -9):
        return _status_record(query, "KILLED")
    for line in done.stdout.splitlines():
        if line.startswith("##JSON## "):
            return json.loads(line[len("##JSON## "):])
    return _status_record(query, "ERROR", _last_stderr(done.stderr))


def _spawn(cell, scale, query, args):
    """Run one (cell, scale, query) in a capped subprocess; return a record."""
    cmd = [sys.executable, "-u", os.path.abspath(__file__), "--child",
           "--cell", cell, "--scale", scale, "--query", query,
           "--memlimit-gb", str(args.memlimit_gb)]
    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return _status_record(query, "TIMEOUT")
    return _parse_child(query, done)


def _print_progress(record):
    """Print one query's outcome line as the matrix fills in."""
    ours = _ms(record["ours_ms"])
    duck = _ms(record["duck_ms"])
    flag = "OK" if record["correct"] else record["status"]
    print("  {0:5} {1:>10} {2:>10}  {3}".format(
        record["name"], ours, duck, flag), flush=True)


def _collect_cell(cell, scale, args):
    """Run all 22 queries for one (cell, scale) and return the records."""
    print("\n== {0}  SF{1} ==".format(cell, scale), flush=True)
    records = []
    for number in range(1, 23):
        query = "q{0:02d}".format(number)
        records.append(_spawn(cell, scale, query, args))
        _print_progress(records[-1])
    return records


def _collect(args):
    """Run every requested (cell, scale) and return results[cell][scale]."""
    results = {}
    for cell in args.cells:
        results[cell] = {}
        for scale in args.scales:
            results[cell][scale] = _collect_cell(cell, scale, args)
    return results


# ------------------------------ report writing -----------------------------

def _ms(value):
    """Render a millisecond value, or '-' when there is no measurement."""
    if value is None:
        return "-"
    return "{0:.1f}".format(value)


def _ratio_value(record):
    """ours/duck ratio as a float, or None when either side is missing/zero."""
    ours = record["ours_ms"]
    duck = record["duck_ms"]
    if ours is None or duck is None or duck == 0:
        return None
    return ours / duck


def _ratio_cell(record):
    """Render the ours/duck ratio, or '-' when it cannot be computed."""
    ratio = _ratio_value(record)
    if ratio is None:
        return "-"
    return "{0:.2f}x".format(ratio)


def _rows_cell(record):
    """Render the engine/oracle row-count cell, or '-' when unmeasured."""
    if record["ours_rows"] is None and record["duck_rows"] is None:
        return "-"
    return "{0} / {1}".format(record["ours_rows"], record["duck_rows"])


def _correct_cell(record):
    """Render correctness: 'yes' when matched, else the failing status."""
    if record["correct"]:
        return "yes"
    return record["status"]


def _cell_totals(records):
    """Sum ours (warm + cold) / duck ms over measured queries; count correct
    and measured. Cold is None when no record carries it (parquet cells or
    older records)."""
    ours = duck = cold = 0.0
    correct = measured = cold_seen = 0
    for record in records:
        correct += 1 if record["correct"] else 0
        if record["ours_ms"] is not None and record["duck_ms"] is not None:
            ours += record["ours_ms"]
            duck += record["duck_ms"]
            measured += 1
            cold_ms = record.get("ours_cold_ms")
            if cold_ms is not None:
                cold += cold_ms
                cold_seen += 1
    return ours, duck, correct, measured, (cold if cold_seen else None)


def _summary_row(cell, scale, records):
    """One row of the top-level summary matrix for a (cell, scale)."""
    ours, duck, correct, measured, cold = _cell_totals(records)
    ratio = ours / duck if duck else math.nan
    cold_cell = "-" if cold is None else "{0:.1f}".format(cold)
    return "| {0} | {1} | {2}/22 | {3:.1f} | {4} | {5:.1f} | {6:.2f}x | {7} |".format(
        cell, scale, correct, ours, cold_cell, duck, ratio, measured)


def _summary_section(results, scales):
    """Build the summary matrix across every (cell, scale)."""
    lines = ["## Summary", "",
             "Totals cover only queries that produced a measurement; TIMEOUT / "
             "KILLED / ERROR queries are excluded from the ms totals but counted "
             "as not-correct. `measured` is how many of the 22 timed cleanly.",
             "",
             "| Cell | SF | Correct | Warm (ms) | Cold (ms) | DuckDB (ms) "
             "| Ratio | Measured |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for cell in results:
        for scale in scales:
            if scale in results[cell]:
                lines.append(_summary_row(cell, scale, results[cell][scale]))
    lines.append("")
    return lines


def _failures(results, scales):
    """Collect every non-correct record as (cell, scale, record) tuples."""
    found = []
    for cell in results:
        for scale in scales:
            for record in results[cell].get(scale, []):
                if not record["correct"]:
                    found.append((cell, scale, record))
    return found


def _issues_section(results, scales):
    """Build the issues section: every failure with its status and detail."""
    failures = _failures(results, scales)
    lines = ["## Issues", ""]
    if not failures:
        lines.append("No failures: every query matched DuckDB in every cell.")
        lines.append("")
        return lines
    lines.append("| Cell | SF | Query | Status | Detail |")
    lines.append("| --- | --- | --- | --- | --- |")
    for cell, scale, record in failures:
        detail = _escape(record["reason"]) if record["reason"] else record["status"]
        lines.append("| {0} | {1} | {2} | {3} | {4} |".format(
            cell, scale, record["name"], record["status"], detail))
    lines.append("")
    return lines


def _escape(text):
    """Escape markdown table separators in a free-text detail string."""
    return text.replace("|", "\\|").replace("\n", " ")


def _detail_table(records):
    """Build the per-query table for one (cell, scale)."""
    lines = ["| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |",
             "| --- | --- | --- | --- | --- | --- |"]
    for record in records:
        lines.append("| {0} | {1} | {2} | {3} | {4} | {5} |".format(
            record["name"], _ms(record["ours_ms"]), _ms(record["duck_ms"]),
            _ratio_cell(record), _correct_cell(record), _rows_cell(record)))
    return lines


def _detail_section(results, scales):
    """Build one detailed per-query table per (cell, scale)."""
    lines = ["## Per-query detail", ""]
    for cell in results:
        for scale in scales:
            if scale in results[cell]:
                lines.append("### {0} - SF{1}".format(CELL_TITLES[cell], scale))
                lines.append("")
                lines.extend(_detail_table(results[cell][scale]))
                lines.append("")
    return lines


def _host_line():
    """A one-line description of the CPU the benchmark ran on."""
    model = "unknown CPU"
    with open("/proc/cpuinfo") as handle:
        for line in handle:
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    return "{0} ({1} cores)".format(model, os.cpu_count())


def _duckdb_version():
    """The DuckDB library version used as the correctness oracle."""
    import duckdb

    return duckdb.__version__


def _commit_info():
    """Return (short-hash, subject, dirty) for the current git checkout."""
    short = _git("rev-parse", "--short", "HEAD")
    subject = _git("log", "-1", "--format=%s")
    dirty = _git("status", "--porcelain") != ""
    return short, subject, dirty


def _git(*args):
    """Run a git command in the repo and return its stripped stdout."""
    done = subprocess.run(("git",) + args, cwd=REPO_ROOT,
                          capture_output=True, text=True)
    return done.stdout.strip()


def _header(short, subject, dirty, scales):
    """Build the report preamble: commit, host, engines, scales, methodology."""
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    dirty_note = "  (working tree DIRTY - results not from a clean commit)" if dirty else ""
    lines = ["# TPC-H benchmark report", "",
             "Commit: `{0}` - {1}{2}".format(short, subject, dirty_note),
             "Generated: {0}".format(stamp), "Host: {0}".format(_host_line()),
             "Engine: fedqrs (Rust / DataFusion) - the only execution path.",
             "Oracle: DuckDB {0}.".format(_duckdb_version()),
             "Scale factors: " + _scales_note(scales), ""]
    lines.extend(_methodology_lines())
    return lines


def _scales_note(scales):
    """Render the scale factors with their lineitem row counts."""
    parts = []
    for scale in scales:
        parts.append("SF{0} ({1} lineitem)".format(scale, LINEITEM_ROWS.get(scale, "?")))
    return ", ".join(parts) + "."


def _methodology_lines():
    """The fixed methodology description block."""
    return ["## Methodology", "",
            "Timing is the median of three warm runs per query; correctness is a "
            "differential row-by-row check against DuckDB on the same data "
            "(numbers rounded to 2 decimals). Each query runs in its own "
            "subprocess with an RSS cap, so a blow-up is a KILLED row, not a lost "
            "run. Ratio is ours/DuckDB (higher = we are slower).", "",
            "- **single** - " + CELL_TITLES["single"] + ".",
            "- **fedparquet** - " + CELL_TITLES["fedparquet"]
            + "; DuckDB reads all files as one, so this overstates our cost.",
            "- **fedpgduck** - " + CELL_TITLES["fedpgduck"]
            + "; the DuckDB oracle federates the same split via its postgres "
            "connector. This is the honest federated number.", ""]


def _build_report(results, scales):
    """Assemble the full markdown report as a single string."""
    short, subject, dirty = _commit_info()
    lines = _header(short, subject, dirty, scales)
    lines.extend(_summary_section(results, scales))
    lines.extend(_issues_section(results, scales))
    lines.extend(_detail_section(results, scales))
    return short, "\n".join(lines) + "\n"


def _write_report(results, scales):
    """Write the report to reports/report-result-<commit>.md and return its path."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    short, text = _build_report(results, scales)
    path = os.path.join(REPORT_DIR, "report-result-{0}.md".format(short))
    with open(path, "w") as handle:
        handle.write(text)
    return path


# ------------------------------ entry point --------------------------------

def _parse_args():
    """Parse the benchmark orchestrator's command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", default="0.1,1")
    parser.add_argument("--cells", default=",".join(CELLS))
    parser.add_argument("--memlimit-gb", type=float, default=12.0)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--child", action="store_true", help="internal: one query")
    parser.add_argument("--cell", default=None)
    parser.add_argument("--scale", default=None)
    parser.add_argument("--query", default=None)
    return parser.parse_args()


def _run_parent(args):
    """Parent entry: run the whole matrix and write the commit-named report."""
    args.scales = args.scales.split(",")
    args.cells = args.cells.split(",")
    results = _collect(args)
    path = _write_report(results, args.scales)
    print("\nWrote report: {0}".format(path))


def main():
    """Dispatch to the child (one query) or the parent (full matrix)."""
    args = _parse_args()
    if args.child:
        _run_child(args)
    else:
        _run_parent(args)


if __name__ == "__main__":
    main()
