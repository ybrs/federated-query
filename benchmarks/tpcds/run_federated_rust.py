"""Run the FEDERATED TPC-DS benchmark (PostgreSQL + DuckDB split) through the
Rust engine (fedq-py) and check correctness against cached references.

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

Each query is classified OK (matches the reference), WRONG (runs but differs),
or ERROR (raised in the engine). Every per-query exception is caught and
recorded so one failure cannot stop the whole run. The Rust engine's 100ms
planning budget surfaces as an ERROR ("planning budget exceeded") for the
heaviest queries; that is reported as-is, never worked around.
"""

import argparse
import datetime
import glob
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


def _error_result(name, error, elapsed_ms):
    """Assemble an ERROR record for a query that raised in the engine."""
    detail = str(error).strip().splitlines()
    message = detail[0] if detail else type(error).__name__
    return {
        "name": name, "status": "ERROR",
        "reason": "{0}: {1}".format(type(error).__name__, message),
        "ms": elapsed_ms, "engine_rows": None, "truth_rows": None,
    }


def _compared_result(name, engine_rows, truth_rows, elapsed_ms, decimals):
    """Compare engine rows against the reference truth and classify the query."""
    match, reason = compare_results(engine_rows, truth_rows, decimals)
    status = "OK" if match else "WRONG"
    return {
        "name": name, "status": status, "reason": reason, "ms": elapsed_ms,
        "engine_rows": len(engine_rows), "truth_rows": len(truth_rows),
    }


def evaluate_query(runtime, refs, path, decimals):
    """Run one query through the Rust engine, verify it, time it, and classify."""
    name = os.path.splitext(os.path.basename(path))[0]
    engine_sql = _qualify(_read_query(path), FEDQ_SOURCES, ENGINE_DIALECT)
    start = time.perf_counter()
    try:
        engine_rows = arrow_to_rows(runtime.execute(engine_sql))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        # A Rust panic surfaces as pyo3_runtime.PanicException, which subclasses
        # BaseException (not Exception), so catch BaseException here to record a
        # panic as a per-query ERROR instead of aborting the whole run.
        return _error_result(name, error, (time.perf_counter() - start) * 1000.0)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    truth_rows = _reference_rows(refs, name)
    return _compared_result(name, engine_rows, truth_rows, elapsed_ms, decimals)


def _print_result(result):
    """Print one query's outcome row with its wall time and reason."""
    reason = result["reason"]
    if len(reason) > 70:
        reason = reason[:70] + "..."
    print("{0:5} {1:6} {2:9.1f}ms  {3}".format(
        result["name"], result["status"], result["ms"], reason), flush=True)


def _tally(results):
    """Count OK / WRONG / ERROR across a result list."""
    tally = {"OK": 0, "WRONG": 0, "ERROR": 0}
    for result in results:
        tally[result["status"]] += 1
    return tally


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


def _matrix_lines(results):
    """Build the per-query markdown table (status, ms, message)."""
    lines = [
        "| Query | Status | ms | Rows engine/truth | Message |",
        "| --- | --- | --- | --- | --- |",
    ]
    for result in results:
        message = result["reason"] if result["reason"] else "rows and values match"
        rows_cell = "-"
        if result["engine_rows"] is not None:
            rows_cell = "{0} / {1}".format(result["engine_rows"], result["truth_rows"])
        lines.append("| {0} | {1} | {2:.1f} | {3} | {4} |".format(
            result["name"], result["status"], result["ms"], rows_cell,
            message.replace("|", "\\|")))
    return lines


def _report_lines(results, tally, total_ms, scale_factor):
    """Assemble the full markdown report for the run."""
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "# TPC-DS federated benchmark report (Rust engine)",
        "",
        "Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.",
        "Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).",
        "Truth: cached pure-DuckDB references in references_sf{0}.duckdb.".format(
            scale_factor),
        "Generated: {0}".format(stamp),
        "",
        "Tally: {0} ok | {1} wrong | {2} error   (total {3} queries, {4:.1f}s)".format(
            tally["OK"], tally["WRONG"], tally["ERROR"], len(results),
            total_ms / 1000.0),
        "",
        "## Non-OK queries grouped by reason",
        "",
    ]
    lines.extend(_grouped_lines(results))
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


def _build_runtime(options):
    """Write the temp config and build the Rust engine runtime and references."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    database = options.pg_database or pg_database_name(options.scale_factor)
    adbc_driver = _adbc_driver_path()
    config_path = write_config(db_path, database, adbc_driver, options)
    runtime = fedq.Runtime(config_path)
    refs = duckdb.connect(_refs_path(options.scale_factor), read_only=True)
    return db_path, database, adbc_driver, runtime, refs


def run(options):
    """Run every selected query under the pg-dims federated split and report."""
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    db_path, database, adbc_driver, runtime, refs = _build_runtime(options)
    print("\n==== TPC-DS pg-dims (ours=fedq-rust vs cached DuckDB truth) ====")
    print("db={0} pg={1} adbc={2}".format(db_path, database, adbc_driver))
    results = []
    started = time.perf_counter()
    for path in paths:
        result = evaluate_query(runtime, refs, path, options.decimals)
        results.append(result)
        _print_result(result)
    total_ms = (time.perf_counter() - started) * 1000.0
    tally = _tally(results)
    print("-" * 72)
    print("[pg-dims] {0} ok | {1} wrong | {2} error   (total {3:.1f}s)".format(
        tally["OK"], tally["WRONG"], tally["ERROR"], total_ms / 1000.0))
    report_path = os.path.join(
        REPORTS_DIR, "rust-fed-sf{0}.md".format(options.scale_factor))
    os.makedirs(REPORTS_DIR, exist_ok=True)
    write_report(results, tally, total_ms, options, report_path)
    return results


def _parse_args():
    """Parse command-line arguments for the federated Rust TPC-DS runner."""
    parser = argparse.ArgumentParser(description="Run federated TPC-DS on the Rust engine.")
    parser.add_argument("--scale-factor", default="0.1")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
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
