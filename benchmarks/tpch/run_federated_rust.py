"""Run the FEDERATED TPC-H benchmark (PostgreSQL + DuckDB split) through the
Rust engine (fedq-py) and check correctness and timing.

This adapts run_federated.py's evaluate loop but swaps the engine: instead of
the Python FedQRuntime it drives the pyo3 ``fedq.Runtime``, built from a
temporary YAML config that registers a DuckDB source ("duck", schema "main")
and a PostgreSQL source ("pg", schema "public", read over the ADBC driver).

The placement, the three-part qualification (pg-dims: dims on pg, facts on
duck), the DuckDB-with-pg-attached oracle, and compare_results are kept exactly
as run_federated.py uses them, so the correctness truth and the timing method
match the old-architecture baseline.

Each per-query exception is caught and recorded as an ERROR so one failure
cannot stop the whole run.
"""

import argparse
import glob
import os
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
from generate import _db_path, DEFAULT_DATA_DIR, DEFAULT_QUERIES_DIR
from qualify import TPCH_TABLES


# Dimensions on PostgreSQL, fact tables on DuckDB: every fact-dimension join
# crosses sources. This mirrors run_federated.py's "pg-dims" placement.
PG_DIMS = {
    "nation": "pg", "region": "pg", "part": "pg", "supplier": "pg",
    "customer": "pg", "orders": "duck", "lineitem": "duck", "partsupp": "duck",
}

ENGINE_DIALECT = "postgres"

# Engine three-part names: pg tables as pg.public.X, duck tables as duck.main.X.
# The oracle reads pg tables through the attached "pgdb" and duck tables bare.
FEDQ_SOURCES = {"pg": ("pg", "public"), "duck": ("duck", "main")}
ORACLE_SOURCES = {"pg": ("pgdb", "public"), "duck": (None, "main")}

# ADBC Postgres driver locations searched in order (the exec plane reads pg
# over ADBC). First existing path wins.
ADBC_CANDIDATES = [
    "/workspace/venv-fedq/lib/python3.13/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    "/workspace/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
    "/workspace/federated-query/venv/lib/python3.12/site-packages/adbc_driver_postgresql/libadbc_driver_postgresql.so",
]


def _adbc_driver_path():
    """Return the first ADBC Postgres driver shared library that exists."""
    for candidate in ADBC_CANDIDATES:
        if os.path.exists(candidate):
            return candidate
    raise SystemExit("ADBC Postgres driver not found in: {0}".format(ADBC_CANDIDATES))


def write_config(db_path, options, adbc_driver):
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
        database=options.pg_database, adbc_driver=adbc_driver,
    )
    handle = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    handle.write(text)
    handle.close()
    return handle.name


def _pg_dsn(options):
    """The libpq DSN DuckDB's postgres extension attaches through."""
    return (
        f"dbname={options.pg_database} user={options.pg_user} "
        f"password={options.pg_password} host={options.pg_host} "
        f"port={options.pg_port}"
    )


def build_oracle(db_path, options):
    """Open DuckDB over the dataset with PostgreSQL attached via the connector."""
    connection = duckdb.connect(db_path, read_only=True)
    connection.execute("LOAD postgres")
    connection.execute(f"ATTACH '{_pg_dsn(options)}' AS pgdb (TYPE postgres, READ_ONLY)")
    return connection


def _qualify(sql, source_map, dialect):
    """Qualify each base table to the source that holds it under pg-dims."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in TPCH_TABLES or table.args.get("db"):
            continue
        catalog, schema = source_map[PG_DIMS[name]]
        table.set("db", exp.to_identifier(schema))
        if catalog is not None:
            table.set("catalog", exp.to_identifier(catalog))
    return tree.sql(dialect=dialect)


def _query_sources(sql):
    """The set of source kinds the base tables of a query resolve to."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    sources = set()
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name in TPCH_TABLES:
            sources.add(PG_DIMS[name])
    return sources


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


def _median_ms(thunk, warm_runs):
    """(cold ms, warm median ms, last result): first run is COLD, the warm
    median covers ``warm_runs`` subsequent runs (plan-cache hits)."""
    start = time.time()
    thunk()
    cold_ms = (time.time() - start) * 1000
    times = []
    result = None
    for _ in range(warm_runs):
        start = time.time()
        result = thunk()
        times.append((time.time() - start) * 1000)
    times.sort()
    return cold_ms, times[len(times) // 2], result


def evaluate_query(runtime, oracle, path, decimals, warm_runs):
    """Run one query through the Rust engine and the oracle; time + classify."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    cross = "cross" if len(_query_sources(raw)) > 1 else "single"
    engine_sql = _qualify(raw, FEDQ_SOURCES, ENGINE_DIALECT)
    try:
        ours_cold_ms, ours_ms, engine_rows = _median_ms(
            lambda: arrow_to_rows(runtime.execute(engine_sql)), warm_runs
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        # A Rust panic surfaces as pyo3_runtime.PanicException, which subclasses
        # BaseException (not Exception), so catch BaseException here to record a
        # panic as a per-query ERROR instead of aborting the whole run.
        return _error_result(name, error, cross)
    oracle_sql = _qualify(raw, ORACLE_SOURCES, "duckdb")
    _duck_cold, duck_ms, oracle_rows = _median_ms(
        lambda: oracle.execute(oracle_sql).fetchall(), warm_runs
    )
    match, reason = compare_results(engine_rows, oracle_rows, decimals)
    status = "PASS" if match else "MISMATCH"
    return {"name": name, "status": status, "reason": reason, "span": cross,
            "engine_rows": len(engine_rows), "oracle_rows": len(oracle_rows),
            "ours_ms": ours_ms, "ours_cold_ms": ours_cold_ms, "duck_ms": duck_ms}


def _error_result(name, error, cross):
    """Assemble an ERROR record for a query that raised in the engine."""
    detail = str(error).strip().splitlines()
    message = detail[0] if detail else type(error).__name__
    return {"name": name, "status": "ERROR",
            "reason": "{0}: {1}".format(type(error).__name__, message),
            "span": cross, "engine_rows": None, "oracle_rows": None,
            "ours_ms": 0.0, "ours_cold_ms": 0.0, "duck_ms": 0.0}


def _print_result(result):
    """Print one query's outcome row with timing and slowness (ours / duckdb)."""
    reason = result["reason"]
    if len(reason) > 46:
        reason = reason[:46] + "..."
    slower = result["ours_ms"] / result["duck_ms"] if result["duck_ms"] else 0.0
    print("{0:5} {1:8} {2:6} {3:9.1f}m {4:8.1f}m {5:6.2f}x  {6}".format(
        result["name"], result["status"], result["span"],
        result["ours_ms"], result["duck_ms"], slower, reason), flush=True)


def _geomean(ratios):
    """Geometric mean of a list of positive ratios (0.0 if the list is empty)."""
    if not ratios:
        return 0.0
    product = 1.0
    for ratio in ratios:
        product *= ratio
    return product ** (1.0 / len(ratios))


def _print_summary(results):
    """Print the tally, cross-source count, totals and the timing ratios."""
    tally = {"PASS": 0, "MISMATCH": 0, "ERROR": 0}
    cross = 0
    ours_total = duck_total = 0.0
    ratios = []
    for result in results:
        tally[result["status"]] += 1
        cross += 1 if result["span"] == "cross" else 0
        ours_total += result["ours_ms"]
        duck_total += result["duck_ms"]
        if result["status"] != "ERROR" and result["duck_ms"]:
            ratios.append(result["ours_ms"] / result["duck_ms"])
    total_ratio = ours_total / duck_total if duck_total else float("nan")
    print("-" * 72)
    print("[pg-dims] Total {0} | PASS {1} | MISMATCH {2} | ERROR {3} | cross-source {4}".format(
        len(results), tally["PASS"], tally["MISMATCH"], tally["ERROR"], cross))
    print("[pg-dims] ours {0:.0f}m  duckdb {1:.0f}m  ->  total {2:.2f}x  geomean {3:.2f}x  "
          "(both federated over the same PG+DuckDB split)".format(
              ours_total, duck_total, total_ratio, _geomean(ratios)))


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


def run(options):
    """Load sources, then run every selected query under the pg-dims placement."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    adbc_driver = _adbc_driver_path()
    config_path = write_config(db_path, options, adbc_driver)
    runtime = fedq.Runtime(config_path)
    oracle = build_oracle(db_path, options)
    print("\n==== placement: pg-dims (ours=fedq-rust, duckdb=via postgres connector) ====")
    print("db={0} pg={1} adbc={2}".format(db_path, options.pg_database, adbc_driver))
    print("{0:5} {1:8} {2:6} {3:>10} {4:>9} {5:>7}".format(
        "query", "status", "span", "ours", "duckdb", "slower"))
    results = []
    for path in paths:
        result = evaluate_query(runtime, oracle, path, options.decimals, options.warm_runs)
        results.append(result)
        _print_result(result)
    _print_summary(results)
    return results


def _parse_args():
    """Parse command-line arguments for the federated Rust benchmark runner."""
    parser = argparse.ArgumentParser(description="Run federated TPC-H on the Rust engine.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default="duckpoc")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    return parser.parse_args()


def main():
    """Entry point: run the federated TPC-H benchmark on the Rust engine."""
    run(_parse_args())


if __name__ == "__main__":
    main()
