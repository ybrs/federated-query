"""Run the TPC-H queries across PostgreSQL + DuckDB and check correctness.

The single-source benchmark (run.py) puts every table on one DuckDB source.
This runner splits the eight base tables across a PostgreSQL source and a
DuckDB source (a ``placement``) and runs each query through the federated
engine, exercising its cross-source paths (cross-source joins, aggregations,
semi-joins).

Truth for the federated run is DuckDB reading the SAME split through its
``postgres`` connector: it attaches PostgreSQL and reads the pg-placed tables
via the connector and the duck-placed tables natively, so it computes the exact
federated answer fedq should produce. Both read identical data (load_postgres.py
loaded every table into both sources), so a mismatch is a real engine bug.
"""

import argparse
import glob
import os
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
from generate import _db_path, DEFAULT_DATA_DIR, DEFAULT_QUERIES_DIR
from qualify import TPCH_TABLES
from run import arrow_to_rows, _read_query


# Each placement maps every base table to the source that holds it. "pg" is the
# PostgreSQL source, "duck" the DuckDB source.
PLACEMENTS = {
    # Dimensions on PostgreSQL, fact tables on DuckDB: every fact-dimension
    # join crosses sources.
    "pg-dims": {
        "nation": "pg", "region": "pg", "part": "pg", "supplier": "pg",
        "customer": "pg", "orders": "duck", "lineitem": "duck",
        "partsupp": "duck",
    },
    # Split the heaviest joins across sources (orders vs lineitem, part vs
    # partsupp, supplier vs customer) to force maximum cross-source work.
    "adversarial": {
        "orders": "pg", "lineitem": "duck", "part": "pg", "partsupp": "duck",
        "supplier": "pg", "customer": "duck", "nation": "pg", "region": "duck",
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


def _qualify(sql, placement, source_map, dialect):
    """Qualify each base table to the source that holds it under a placement."""
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if name not in TPCH_TABLES or table.args.get("db"):
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
        if name in TPCH_TABLES:
            sources.add(placement[name])
    return sources


def _median_ms(thunk):
    """(cold ms, warm median ms, last result): the FIRST run on the fresh
    runtime is COLD (stats fetches, planning, connections) and times
    separately; the warm median covers 3 subsequent runs (plan-cache hits).
    Reporting both keeps cold-path regressions visible."""
    start = time.time()
    thunk()
    cold_ms = (time.time() - start) * 1000
    times = []
    for _ in range(3):
        start = time.time()
        result = thunk()
        times.append((time.time() - start) * 1000)
    times.sort()
    return cold_ms, times[1], result


def evaluate_query(runtime, oracle, path, placement, decimals):
    """Run one query through the engine and the connector oracle; time + classify."""
    name = os.path.splitext(os.path.basename(path))[0]
    raw = _read_query(path)
    cross = "cross" if len(_query_sources(raw, placement)) > 1 else "single"
    engine_sql = _qualify(raw, placement, FEDQ_SOURCES, ENGINE_DIALECT)
    try:
        ours_cold_ms, ours_ms, engine_rows = _median_ms(
            lambda: arrow_to_rows(runtime.execute(engine_sql))
        )
    except Exception as error:
        return _result(name, "ERROR", _error_text(error), cross, None, None, 0.0, 0.0)
    oracle_sql = _qualify(raw, placement, ORACLE_SOURCES, "duckdb")
    _duck_cold, duck_ms, oracle_rows = _median_ms(
        lambda: oracle.execute(oracle_sql).fetchall()
    )
    match, reason = compare_results(engine_rows, oracle_rows, decimals)
    status = "PASS" if match else "MISMATCH"
    return _result(name, status, reason, cross, len(engine_rows), len(oracle_rows),
                   ours_ms, duck_ms, ours_cold_ms)


def _result(name, status, reason, cross, engine_rows, oracle_rows, ours_ms, duck_ms,
            ours_cold_ms=None):
    """Assemble one query's outcome record. ours_ms is the WARM median;
    ours_cold_ms the fresh runtime's first execution."""
    return {"name": name, "status": status, "reason": reason, "span": cross,
            "engine_rows": engine_rows, "oracle_rows": oracle_rows,
            "ours_ms": ours_ms, "duck_ms": duck_ms, "ours_cold_ms": ours_cold_ms}


def _error_text(error):
    """Render an exception as a single-line ``Type: message`` string."""
    lines = str(error).strip().splitlines()
    detail = lines[0] if lines else type(error).__name__
    return "{0}: {1}".format(type(error).__name__, detail)


def _print_result(result):
    """Print one query's outcome row with timing and slowness (ours / duckdb)."""
    reason = result["reason"]
    if len(reason) > 46:
        reason = reason[:46] + "..."
    slower = result["ours_ms"] / result["duck_ms"] if result["duck_ms"] else 0.0
    print("{0:5} {1:8} {2:6} {3:9.1f}m {4:8.1f}m {5:6.2f}x  {6}".format(
        result["name"], result["status"], result["span"],
        result["ours_ms"], result["duck_ms"], slower, reason), flush=True)


def _print_summary(placement_name, results):
    """Print the tally, cross-source count, and the total timing ratio."""
    tally = {"PASS": 0, "MISMATCH": 0, "ERROR": 0}
    cross = 0
    ours_total = duck_total = 0.0
    for result in results:
        tally[result["status"]] += 1
        cross += 1 if result["span"] == "cross" else 0
        ours_total += result["ours_ms"]
        duck_total += result["duck_ms"]
    ratio = ours_total / duck_total if duck_total else float("nan")
    print("-" * 72)
    print("[{0}] Total {1} | PASS {2} | MISMATCH {3} | ERROR {4} | cross-source {5}".format(
        placement_name, len(results), tally["PASS"], tally["MISMATCH"],
        tally["ERROR"], cross))
    print("[{0}] ours {1:.0f}m  duckdb {2:.0f}m  ->  {3:.2f}x slower  "
          "(both federated over the same PG+DuckDB split)".format(
              placement_name, ours_total, duck_total, ratio))


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


def run_placement(placement_name, paths, runtime, oracle, decimals):
    """Run every selected query under one placement and print the matrix."""
    placement = PLACEMENTS[placement_name]
    print("\n==== placement: {0} (ours=fedq, duckdb=via postgres connector) ====".format(
        placement_name))
    print("{0:5} {1:8} {2:6} {3:>10} {4:>9} {5:>7}".format(
        "query", "status", "span", "ours", "duckdb", "slower"))
    results = []
    for path in paths:
        result = evaluate_query(runtime, oracle, path, placement, decimals)
        results.append(result)
        _print_result(result)
    _print_summary(placement_name, results)
    return results


def run(options):
    """Load sources, then run each requested placement over the query set."""
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    paths = _select_query_files(options.queries_dir, options.only)
    if not paths:
        raise SystemExit("No query files found in {0}".format(options.queries_dir))
    runtime = build_fedq(db_path, options)
    oracle = build_oracle(db_path, options)
    for placement_name in options.placements.split(","):
        run_placement(placement_name.strip(), paths, runtime, oracle, options.decimals)


def _parse_args():
    """Parse command-line arguments for the federated benchmark runner."""
    parser = argparse.ArgumentParser(description="Run TPC-H across PG + DuckDB.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--placements", default="pg-dims,adversarial")
    parser.add_argument("--queries-dir", default=DEFAULT_QUERIES_DIR)
    parser.add_argument("--only", default=None)
    parser.add_argument("--decimals", type=int, default=2)
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default="duckpoc")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    return parser.parse_args()


def main():
    """Entry point: run the federated TPC-H benchmark."""
    run(_parse_args())


if __name__ == "__main__":
    main()
