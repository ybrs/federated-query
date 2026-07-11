"""Run the TPC-H queries through the Rust engine (fedq-py) and check correctness.

This mirrors run.py's evaluate loop but swaps the engine: instead of the Python
FedQRuntime it drives the pyo3 `fedq.Runtime`, built from a temporary YAML config
that registers the single DuckDB source under the same name run.py uses ("tpch",
schema "main", dialect "postgres"). For each query it qualifies + transpiles the
SQL exactly as run.py does, runs it through the Rust engine, runs the raw SQL in
DuckDB as the oracle, and compares the two result sets with compare_results.

Any per-query exception is caught and recorded as an ERROR so one failure cannot
stop the whole run.
"""

import glob
import os
import sys
import tempfile

import duckdb
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fedq

from compare import compare_results
from qualify import qualify_query


ENGINE_DIALECT = "postgres"
SOURCE = "tpch"
SCHEMA = "main"
DECIMALS = 2
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "tpch_sf0.01.duckdb")
QUERIES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries")


def write_config(db_path):
    """Write a temp YAML config with one DuckDB datasource named 'tpch'."""
    text = (
        "datasources:\n"
        "  {0}:\n"
        "    type: duckdb\n"
        "    path: {1}\n"
        "    read_only: true\n"
    ).format(SOURCE, db_path)
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


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


def evaluate_query(runtime, oracle, path):
    """Run one query through the Rust engine and the oracle and classify it."""
    name = os.path.splitext(os.path.basename(path))[0]
    original_sql = _read_query(path)
    engine_sql = qualify_query(original_sql, SOURCE, SCHEMA, ENGINE_DIALECT)
    try:
        engine_rows = arrow_to_rows(runtime.execute(engine_sql))
    except Exception as error:
        detail = str(error).strip().splitlines()
        message = detail[0] if detail else type(error).__name__
        return {"name": name, "status": "ERROR",
                "reason": "{0}: {1}".format(type(error).__name__, message),
                "engine_rows": None, "oracle_rows": None}
    oracle_rows = oracle.execute(original_sql).fetchall()
    match, reason = compare_results(engine_rows, oracle_rows, DECIMALS)
    return {"name": name, "status": "PASS" if match else "MISMATCH", "reason": reason,
            "engine_rows": len(engine_rows), "oracle_rows": len(oracle_rows)}


def _rows_cell(result):
    """Render the engine/oracle row-count cell, or '-' when the query errored."""
    if result["engine_rows"] is None:
        return "-"
    return "{0} / {1}".format(result["engine_rows"], result["oracle_rows"])


def main():
    """Run every query and print the per-query table and final tally."""
    config_path = write_config(DB_PATH)
    runtime = fedq.Runtime(config_path)
    oracle = duckdb.connect(DB_PATH, read_only=True)
    paths = sorted(glob.glob(os.path.join(QUERIES_DIR, "q*.sql")))
    results = []
    print("{0:5} {1:9} {2:16} {3}".format("query", "status", "rows eng/ora", "detail"))
    print("-" * 90)
    for path in paths:
        result = evaluate_query(runtime, oracle, path)
        results.append(result)
        reason = result["reason"]
        if len(reason) > 60:
            reason = reason[:60] + "..."
        print("{0:5} {1:9} {2:16} {3}".format(
            result["name"], result["status"], _rows_cell(result), reason), flush=True)
    passed = 0
    for result in results:
        if result["status"] == "PASS":
            passed += 1
    print("-" * 90)
    print("TPC-H (Rust, single-source duck, sf0.01): {0}/22 correct".format(passed))


if __name__ == "__main__":
    main()
