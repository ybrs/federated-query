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

Each engine run happens in an isolated child process bounded by a wall-clock
timeout and a memory cap, so a cross-source query that never terminates or blows
up intermediate results becomes a clean ERROR row (Timeout / Killed) instead of
hanging or OOM-ing the whole run. The DuckDB oracle runs in the parent.
"""

import argparse
import glob
import multiprocessing
import os
import queue as queue_module
import resource

import duckdb
import sqlglot
from sqlglot import exp

from federated_query.catalog.catalog import Catalog
from federated_query.cli.fedq import FedQRuntime
from federated_query.config.config import Config
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource

from compare import compare_results
from generate import _db_path, DEFAULT_DATA_DIR, DEFAULT_QUERIES_DIR
from qualify import TPCDS_TABLES
from run import arrow_to_rows, _read_query


# The seven TPC-DS fact tables; everything else is a dimension.
FACT_TABLES = frozenset(
    {
        "store_sales", "store_returns", "catalog_sales", "catalog_returns",
        "web_sales", "web_returns", "inventory",
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
        "store_sales": "duck", "store_returns": "pg",
        "catalog_sales": "pg", "catalog_returns": "duck",
        "web_sales": "duck", "web_returns": "pg",
        "inventory": "pg",
        "call_center": "pg", "catalog_page": "duck", "customer": "pg",
        "customer_address": "duck", "customer_demographics": "pg",
        "date_dim": "pg", "household_demographics": "duck", "income_band": "pg",
        "item": "duck", "promotion": "pg", "reason": "duck", "ship_mode": "pg",
        "store": "duck", "time_dim": "pg", "warehouse": "duck",
        "web_page": "pg", "web_site": "duck",
    },
}

ENGINE_DIALECT = "postgres"

# How each source kind names its schema for the engine (three-part references)
# and for the DuckDB oracle (pg tables via the attached "pgdb", duck tables
# left bare so they resolve against the local DuckDB file).
FEDQ_SOURCES = {"pg": ("pg", "public"), "duck": ("duck", "main")}
ORACLE_SOURCES = {"pg": ("pgdb", "public"), "duck": (None, "main")}


def _pg_config(options):
    """PostgreSQL connection config for the engine's connector."""
    return {
        "host": options.pg_host,
        "port": int(options.pg_port),
        "database": options.pg_database,
        "user": options.pg_user,
        "password": options.pg_password,
        "schemas": ["public"],
    }


def _pg_dsn(options):
    """The libpq DSN DuckDB's postgres extension attaches through."""
    return (
        f"dbname={options.pg_database} user={options.pg_user} "
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
    """Open DuckDB over the dataset with PostgreSQL attached via the connector."""
    connection = duckdb.connect(db_path, read_only=True)
    connection.execute("LOAD postgres")
    connection.execute(f"ATTACH '{_pg_dsn(options)}' AS pgdb (TYPE postgres, READ_ONLY)")
    return connection


def _error_text(error):
    """Render an exception as a single-line ``Type: message`` string."""
    lines = str(error).strip().splitlines()
    detail = lines[0] if lines else type(error).__name__
    return "{0}: {1}".format(type(error).__name__, detail)


def _apply_memory_limit(memory_limit_mb):
    """Cap the child process address space so a runaway query fails loudly."""
    if memory_limit_mb <= 0:
        return
    limit_bytes = memory_limit_mb * 1024 * 1024
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))


def _engine_worker(engine_sql, db_path, options, result_queue):
    """Child-process entry: build a fresh federated runtime, run one query."""
    _apply_memory_limit(options.memory_limit)
    try:
        runtime = build_fedq(db_path, options)
        result = runtime.execute(engine_sql)
        result_queue.put(("ok", arrow_to_rows(result)))
    except Exception as error:
        result_queue.put(("error", _error_text(error)))


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


def run_engine_isolated(engine_sql, db_path, options):
    """Run one federated query in a child process bounded by timeout and memory."""
    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()
    process = context.Process(
        target=_engine_worker, args=(engine_sql, db_path, options, result_queue)
    )
    process.start()
    try:
        status, payload = result_queue.get(timeout=options.timeout)
    except queue_module.Empty:
        return _finish_stalled_worker(process, options.timeout)
    process.join()
    if status == "ok":
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


def evaluate_query(oracle, path, placement, db_path, options):
    """Run one query through the isolated engine and the oracle; classify it."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    cross = "cross" if len(_query_sources(raw, placement)) > 1 else "single"
    engine_sql = _qualify(raw, placement, FEDQ_SOURCES, ENGINE_DIALECT)
    engine_rows, error = run_engine_isolated(engine_sql, db_path, options)
    if error is not None:
        return _result(name, "ERROR", error, cross, None, None)
    oracle_sql = _qualify(raw, placement, ORACLE_SOURCES, "duckdb")
    oracle_rows = oracle.execute(oracle_sql).fetchall()
    match, reason = compare_results(engine_rows, oracle_rows, options.decimals)
    status = "PASS" if match else "MISMATCH"
    return _result(name, status, reason, cross, len(engine_rows), len(oracle_rows))


def _result(name, status, reason, cross, engine_rows, oracle_rows):
    """Assemble one query's outcome record."""
    return {"name": name, "status": status, "reason": reason, "span": cross,
            "engine_rows": engine_rows, "oracle_rows": oracle_rows}


def _print_result(result):
    """Print one query's outcome row."""
    reason = result["reason"]
    if len(reason) > 80:
        reason = reason[:80] + "..."
    print("{0:5} {1:8} {2:7} {3}".format(
        result["name"], result["status"], result["span"], reason), flush=True)


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
            placement_name, len(results), tally["PASS"], tally["MISMATCH"],
            tally["ERROR"], cross,
        )
    )


def _print_summary(placement_name, results):
    """Print the pass/mismatch/error tally and cross-source count."""
    print("-" * 60)
    print(_summary_line(placement_name, results))


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


def _matrix_lines(results):
    """Build the per-query markdown table (status, span, row counts, detail)."""
    lines = ["| Query | Status | Span | Rows engine/oracle | Detail |",
             "| --- | --- | --- | --- | --- |"]
    for result in results:
        detail = result["reason"] if result["reason"] else "rows and values match"
        lines.append("| {0} | {1} | {2} | {3} | {4} |".format(
            result["name"], result["status"], result["span"],
            _rows_cell(result), _escape(detail)))
    return lines


def _placement_section(placement_name, results):
    """Build the report section for one placement: summary, clusters, matrix."""
    lines = ["## Placement: {0}".format(placement_name), "", _summary_line(
        placement_name, results), "", "### Failure clusters", ""]
    lines.extend(_cluster_lines(_cluster_failures(results)))
    lines.append("### Per-query matrix")
    lines.append("")
    lines.extend(_matrix_lines(results))
    lines.append("")
    return lines


def _report_header(options):
    """Build the report preamble: parameters and methodology."""
    return [
        "# TPC-DS federated benchmark report",
        "",
        "Scale factor {0}, PostgreSQL + DuckDB split, per-query timeout {1}s, "
        "memory cap {2} MB. Each engine run is an isolated child process; the "
        "DuckDB oracle (with PostgreSQL attached) runs in the parent.".format(
            options.scale_factor, options.timeout, options.memory_limit),
        "",
        "Correctness is differential: fedq reads each table from its placed "
        "source while DuckDB reads the SAME split through its postgres "
        "connector, so both compute the exact federated answer and a MISMATCH "
        "is a real cross-source engine bug. Rows are compared in order, values "
        "rounded to {0} decimals.".format(options.decimals),
        "",
    ]


def write_report(sections, options, path):
    """Write the federated markdown report across every placement run."""
    lines = _report_header(options)
    for placement_name, results in sections:
        lines.extend(_placement_section(placement_name, results))
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


def run_placement(placement_name, paths, oracle, db_path, options):
    """Run every selected query under one placement and print the matrix."""
    placement = PLACEMENTS[placement_name]
    print("\n==== placement: {0} ====".format(placement_name))
    results = []
    for path in paths:
        result = evaluate_query(oracle, path, placement, db_path, options)
        results.append(result)
        _print_result(result)
    _print_summary(placement_name, results)
    return results


def run(options):
    """Load the oracle, then run each requested placement over the query set."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    oracle = build_oracle(db_path, options)
    sections = []
    for placement_name in options.placements.split(","):
        results = run_placement(placement_name.strip(), paths, oracle, db_path, options)
        sections.append((placement_name.strip(), results))
    if options.report:
        write_report(sections, options, options.report)


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
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default="duckpoc")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    return parser.parse_args()


def main():
    """Entry point: run the federated TPC-DS benchmark."""
    run(_parse_args())


if __name__ == "__main__":
    main()
