"""Run the TPC-H queries through the federated engine and check correctness.

For each of the 22 queries this:
  1. qualifies the TPC-H tables to ``source.schema.table`` and transpiles the
     query to the engine's dialect, then runs it through the full engine
     pipeline (parse -> bind -> decorrelate -> optimize -> plan -> execute);
  2. runs the original query directly in DuckDB as the correctness oracle;
  3. compares the two result sets as multisets of rounded rows.

Each engine run happens in an isolated child process with a wall-clock timeout
and a memory cap, so a query that never terminates or tries to allocate huge
amounts of memory becomes a clean ERROR row instead of hanging or OOM-ing the
whole benchmark.

The output is a per-query support matrix (PASS / MISMATCH / ERROR) and a
summary. A query the engine cannot handle is reported as ERROR with the
exception (or Timeout / Killed); a query that runs but returns wrong rows is a
MISMATCH.
"""

import argparse
import glob
import multiprocessing
import os
import queue as queue_module
import resource

import duckdb

from federated_query.catalog.catalog import Catalog
from federated_query.cli.fedq import FedQRuntime
from federated_query.config.config import Config
from federated_query.datasources.duckdb import DuckDBDataSource

from compare import compare_results
from generate import DEFAULT_QUERIES_DIR, _db_path, DEFAULT_DATA_DIR
from qualify import qualify_query


ENGINE_DIALECT = "postgres"


def arrow_to_rows(table):
    """Convert a pyarrow Table into a list of positional row tuples."""
    columns = []
    for index in range(table.num_columns):
        columns.append(table.column(index).to_pylist())
    rows = []
    for row in zip(*columns):
        rows.append(tuple(row))
    return rows


def build_runtime(db_path, source_name):
    """Register the DuckDB file as a source and build the engine runtime."""
    datasource = DuckDBDataSource(source_name, {"path": db_path, "read_only": True})
    datasource.connect()
    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()
    return FedQRuntime(catalog, Config())


def _read_query(path):
    """Read the original DuckDB query text from a .sql file."""
    with open(path) as handle:
        return handle.read()


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


def _engine_worker(engine_sql, db_path, source_name, memory_limit_mb, result_queue):
    """Child-process entry: build a fresh runtime, run one query, send it back."""
    _apply_memory_limit(memory_limit_mb)
    try:
        runtime = build_runtime(db_path, source_name)
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
    """Run one query in a child process bounded by timeout and memory cap."""
    context = multiprocessing.get_context("fork")
    result_queue = context.Queue()
    process = context.Process(
        target=_engine_worker,
        args=(engine_sql, db_path, options.source, options.memory_limit, result_queue),
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


def evaluate_query(oracle, path, db_path, options):
    """Run one query through engine and oracle and classify the outcome."""
    name = os.path.splitext(os.path.basename(path))[0]
    original_sql = _read_query(path)
    engine_sql = qualify_query(original_sql, options.source, options.schema, ENGINE_DIALECT)
    engine_rows, error = run_engine_isolated(engine_sql, db_path, options)
    if error is not None:
        return {"name": name, "status": "ERROR", "reason": error,
                "engine_rows": None, "oracle_rows": None}
    oracle_rows = oracle.execute(original_sql).fetchall()
    match, reason = compare_results(engine_rows, oracle_rows, options.decimals)
    return {"name": name, "status": "PASS" if match else "MISMATCH", "reason": reason,
            "engine_rows": len(engine_rows), "oracle_rows": len(oracle_rows)}


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


def _print_result(result):
    """Print one query's outcome row, truncating a long reason."""
    reason = result["reason"]
    if len(reason) > 100:
        reason = reason[:100] + "..."
    print("{0:5} {1:9} {2}".format(result["name"], result["status"], reason), flush=True)


def _print_summary(results):
    """Print the pass/mismatch/error tally across all queries."""
    tally = {"PASS": 0, "MISMATCH": 0, "ERROR": 0}
    for result in results:
        tally[result["status"]] += 1
    print("-" * 60)
    print(
        "Total {0} | PASS {1} | MISMATCH {2} | ERROR {3}".format(
            len(results), tally["PASS"], tally["MISMATCH"], tally["ERROR"]
        )
    )


# Ordered failure signatures: the first substring found in a reason names the
# cluster. An unmatched failure falls into "Other" so it is never hidden.
ERROR_CATEGORIES = [
    ("orient join keys", "Join-key orientation"),
    ("Out of Memory", "Out of memory"),
    ("ArrowBuffer", "Out of memory"),
    ("arrow_scan", "Out of memory"),
    ("RecursionError", "Recursion"),
    ("DecorrelationError", "Decorrelation limitation"),
    ("BindingError", "Binding: reference not in scope"),
    ("Timeout", "Timeout"),
    ("Killed", "Memory limit (killed)"),
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


def _escape(text):
    """Escape markdown table cell separators in a reason string."""
    return text.replace("|", "\\|")


def _rows_cell(result):
    """Render the engine/oracle row-count cell, or '-' when the query errored."""
    if result["engine_rows"] is None:
        return "-"
    return "{0} / {1}".format(result["engine_rows"], result["oracle_rows"])


def _matrix_lines(results):
    """Build the per-query markdown table (status, row counts, detail)."""
    lines = ["| Query | Status | Rows engine/oracle | Detail |",
             "| --- | --- | --- | --- |"]
    for result in results:
        detail = result["reason"] if result["reason"] else "rows and values match"
        lines.append("| {0} | {1} | {2} | {3} |".format(
            result["name"], result["status"], _rows_cell(result), _escape(detail)))
    return lines


def _member_names(members):
    """Join the query names of a cluster into a comma-separated string."""
    names = []
    for member in members:
        names.append(member["name"])
    return ", ".join(names)


def _cluster_lines(clusters):
    """Build the failure-cluster section, largest cluster first."""
    ordered = sorted(clusters.items(), key=_cluster_sort_key)
    lines = []
    for label, members in ordered:
        names = _member_names(members)
        lines.append("### {0} ({1})".format(label, len(members)))
        lines.append("Queries: {0}".format(names))
        lines.append("")
        lines.append("- " + members[0]["reason"])
        lines.append("")
    return lines


def _cluster_sort_key(item):
    """Sort clusters by descending size, then by label for stable output."""
    label, members = item
    return (-len(members), label)


def _summary_line(results):
    """Return the one-line PASS/MISMATCH/ERROR tally as text."""
    tally = {"PASS": 0, "MISMATCH": 0, "ERROR": 0}
    for result in results:
        tally[result["status"]] += 1
    return "Total {0} | PASS {1} | MISMATCH {2} | ERROR {3}".format(
        len(results), tally["PASS"], tally["MISMATCH"], tally["ERROR"])


def _report_header(results, options):
    """Build the report preamble: parameters, methodology, and summary."""
    return [
        "# TPC-H benchmark report",
        "",
        "Scale factor {0}, single DuckDB source, per-query timeout {1}s, "
        "memory cap {2} MB.".format(options.scale_factor, options.timeout,
                                    options.memory_limit),
        "",
        "Correctness is differential against DuckDB: each query runs through the "
        "engine and directly in DuckDB, and the two result sets are compared as "
        "a multiset of rows with every column value normalized (numbers rounded "
        "to {0} decimals, CHAR padding stripped). PASS means every row and every "
        "value matches; row order is not compared.".format(options.decimals),
        "",
        "## Summary",
        "",
        _summary_line(results),
    ]


def write_report(results, options, path):
    """Write the markdown report: summary, failure clusters, per-query matrix."""
    lines = _report_header(results, options)
    lines.append("")
    lines.append("## Failure clusters")
    lines.append("")
    lines.extend(_cluster_lines(_cluster_failures(results)))
    lines.append("## Per-query matrix")
    lines.append("")
    lines.extend(_matrix_lines(results))
    lines.append("")
    with open(path, "w") as handle:
        handle.write("\n".join(lines))
    print("Wrote report: {0}".format(path))


def run(options):
    """Run every selected query and print the support matrix and summary."""
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    oracle = duckdb.connect(options.db, read_only=True)
    results = []
    for path in paths:
        result = evaluate_query(oracle, path, options.db, options)
        results.append(result)
        _print_result(result)
    _print_summary(results)
    if options.report:
        write_report(results, options, options.report)
    return results


def _parse_args():
    """Parse command-line arguments for the benchmark runner."""
    parser = argparse.ArgumentParser(description="Run TPC-H queries through fedq.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--db", default=None)
    parser.add_argument("--source", default="tpch")
    parser.add_argument("--schema", default="main")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--memory-limit", type=int, default=12288)
    parser.add_argument("--report", default=None)
    return parser.parse_args()


def main():
    """Entry point: resolve the database path and run the benchmark."""
    args = _parse_args()
    if args.db is None:
        args.db = _db_path(DEFAULT_DATA_DIR, args.scale_factor)
    run(args)


if __name__ == "__main__":
    main()
