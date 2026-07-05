"""Experiment: does manual join reordering fix our biggest fedpgduck offenders?

The engine executes joins in the user's FROM order with no cost-based
reordering. This runs the four join-order-sensitive TPC-H offenders (q05, q07,
q08, q09) two ways through the SAME fair PostgreSQL + DuckDB federated engine -
once exactly as written, once with the joins hand-reordered into a good
left-deep order (experiments/joinorder/qNN.sql) - times both, and checks each
result against the DuckDB oracle. The speedup is a concrete lower bound on what
a real join-ordering optimizer would buy us before we build one.

Usage:
    python benchmarks/tpch/experiments/joinorder_experiment.py --scale 1
"""

import argparse
import datetime
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
TPCH_DIR = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(os.path.dirname(TPCH_DIR))
for _path in (REPO_ROOT, TPCH_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from compare import compare_results
from generate import DEFAULT_DATA_DIR, DEFAULT_QUERIES_DIR, _db_path
from run import arrow_to_rows
from run_federated import (
    ENGINE_DIALECT, FEDQ_SOURCES, ORACLE_SOURCES, PLACEMENTS,
    _median_ms, _qualify, build_fedq, build_oracle,
)

QUERIES = ("q05", "q07", "q08", "q09")
REORDER_DIR = os.path.join(HERE, "joinorder")
REPORT_DIR = os.path.join(HERE, "reports")
PG_DATABASES = {"0.1": "duckpoc", "1": "duckpoc_sf1"}


def _options(scale):
    """The argument namespace run_federated's builders read for a scale."""
    return argparse.Namespace(
        pg_host="localhost", pg_port="5432", pg_database=PG_DATABASES[scale],
        pg_user="postgres", pg_password="postgres", scale_factor=scale,
    )


def _read(path):
    """Read a query file, stripped of trailing whitespace and semicolon."""
    with open(path) as handle:
        return handle.read().strip().rstrip(";")


def _time_fedq(runtime, sql):
    """Median ms and result rows for one query through the federated engine."""
    engine_sql = _qualify(sql, PLACEMENTS["pg-dims"], FEDQ_SOURCES, ENGINE_DIALECT)
    return _median_ms(lambda: arrow_to_rows(runtime.execute(engine_sql)))


def _time_oracle(oracle, sql):
    """Median ms and result rows for the original query through DuckDB."""
    oracle_sql = _qualify(sql, PLACEMENTS["pg-dims"], ORACLE_SOURCES, "duckdb")
    return _median_ms(lambda: oracle.execute(oracle_sql).fetchall())


def _both_correct(orig_rows, reord_rows, oracle_rows):
    """True only if both the original and reordered results match DuckDB."""
    orig_ok, _ = compare_results(orig_rows, oracle_rows, 2)
    reord_ok, _ = compare_results(reord_rows, oracle_rows, 2)
    return orig_ok and reord_ok


def _run_query(runtime, oracle, name):
    """Measure the original and reordered variant of one query; return a record."""
    original = _read(os.path.join(DEFAULT_QUERIES_DIR, name + ".sql"))
    reordered = _read(os.path.join(REORDER_DIR, name + ".sql"))
    orig_ms, orig_rows = _time_fedq(runtime, original)
    reord_ms, reord_rows = _time_fedq(runtime, reordered)
    duck_ms, oracle_rows = _time_oracle(oracle, original)
    return {"name": name, "orig_ms": orig_ms, "reord_ms": reord_ms,
            "duck_ms": duck_ms,
            "correct": _both_correct(orig_rows, reord_rows, oracle_rows)}


def _speedup(record):
    """How many times faster the reordered variant is than the original."""
    if record["reord_ms"] == 0:
        return float("nan")
    return record["orig_ms"] / record["reord_ms"]


def _print_row(record):
    """Print one query's before/after line to the console."""
    print("{0:5} {1:11.1f} {2:11.1f} {3:8.1f}x {4:8.2f}x {5:8.2f}x  {6}".format(
        record["name"], record["orig_ms"], record["reord_ms"], _speedup(record),
        record["orig_ms"] / record["duck_ms"], record["reord_ms"] / record["duck_ms"],
        "OK" if record["correct"] else "WRONG"), flush=True)


def _detail_row(record):
    """One markdown table row for a query's before/after measurement."""
    return "| {0} | {1:.1f} | {2:.1f} | {3:.1f}x | {4:.1f} | {5:.2f}x | {6:.2f}x | {7} |".format(
        record["name"], record["orig_ms"], record["reord_ms"], _speedup(record),
        record["duck_ms"], record["orig_ms"] / record["duck_ms"],
        record["reord_ms"] / record["duck_ms"], "yes" if record["correct"] else "NO")


def _git_short():
    """The current short git commit hash."""
    done = subprocess.run(("git", "rev-parse", "--short", "HEAD"),
                          cwd=REPO_ROOT, capture_output=True, text=True)
    return done.stdout.strip()


def _report_lines(records, scale):
    """Build the markdown report body."""
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = ["# Join-order experiment (fedpgduck, SF{0})".format(scale), "",
             "Commit: `{0}`  Generated: {1}".format(_git_short(), stamp), "",
             "Each offender run two ways through the fair PostgreSQL + DuckDB "
             "engine: as written (user FROM order) and with joins hand-reordered "
             "into a good left-deep order. Median of three warm runs; both "
             "variants checked against the DuckDB oracle. Ratio is ours/DuckDB.",
             "",
             "| Query | Orig (ms) | Reordered (ms) | Speedup | DuckDB (ms) | "
             "Orig/Duck | Reord/Duck | Correct |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for record in records:
        lines.append(_detail_row(record))
    lines.append("")
    return lines


def _write_report(records, scale):
    """Write the experiment report and return its path."""
    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, "joinorder-{0}-sf{1}.md".format(_git_short(), scale))
    with open(path, "w") as handle:
        handle.write("\n".join(_report_lines(records, scale)) + "\n")
    return path


def run(scale):
    """Build the federated engine, run every experiment query, write the report."""
    db_path = _db_path(DEFAULT_DATA_DIR, scale)
    options = _options(scale)
    runtime = build_fedq(db_path, options)
    oracle = build_oracle(db_path, options)
    print("query        orig(ms)  reorder(ms)  speedup  o/duck   r/duck  correct")
    records = []
    for name in QUERIES:
        records.append(_run_query(runtime, oracle, name))
        _print_row(records[-1])
    print("\nWrote report: {0}".format(_write_report(records, scale)))


def main():
    """Entry point: run the join-order experiment at the given scale factor."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", default="1")
    run(parser.parse_args().scale)


if __name__ == "__main__":
    main()
