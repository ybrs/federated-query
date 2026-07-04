"""TPC-H benchmark: the Rust engine vs DuckDB over the SAME Parquet files.

Two modes (``--mode``):

  single     All 8 tables in ONE Parquet source, so the whole query pushes to the
             Rust engine as a single plan. Measures the ENGINE (DataFusion) vs
             DuckDB - no federation.

  federated  Tables split across TWO Parquet sources - facts (lineitem, orders,
             partsupp) and dims (part, supplier, customer, nation, region) - so
             most queries JOIN across sources. Measures OUR federation layer on
             top of the engine. (DuckDB always reads all 8 files monolithically;
             it never federates. So the extra cost here vs `single` is entirely
             ours.)

Both modes read the identical Parquet files; DuckDB reading them directly is the
correctness + speed reference, identical for both modes.

Each query runs in its OWN subprocess with a hard RSS cap (``--memlimit-gb``,
default 12): a query whose memory crosses the cap (e.g. a cross-join blow-up) is
KILLED and reported as such, and the run continues. A per-query wall-clock cap
(``--timeout``) catches hangs the same way.

Usage:
    python benchmarks/tpch/run_tpch.py --mode federated --data /workspace/tpch_parquet_sf1
    python benchmarks/tpch/run_tpch.py --mode single --data /workspace/tpch_parquet
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from decimal import Decimal

QUERY_DIR = os.path.join(os.path.dirname(__file__), "queries")
ALL_TABLES = (
    "lineitem", "orders", "partsupp", "part", "supplier", "customer",
    "nation", "region",
)
FACTS = ("lineitem", "orders", "partsupp")
DIMS = ("part", "supplier", "customer", "nation", "region")


def _source_layout(mode):
    """Map (source_name -> its tables) for a mode, plus each table's source."""
    groups = {"tpch": ALL_TABLES} if mode == "single" else {"facts": FACTS, "dims": DIMS}
    source_of = {}
    for source, tables in groups.items():
        for table in tables:
            source_of[table] = source
    return groups, source_of


# ----------------------------- child: one query -----------------------------

def _current_rss():
    """This process's resident memory in bytes (Linux /proc)."""
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0


def _start_memory_watchdog(limit_bytes):
    """Kill this process (exit 137) the moment its RSS crosses limit_bytes."""
    def watch():
        while True:
            if _current_rss() > limit_bytes:
                os.write(2, b"MEMORY LIMIT EXCEEDED - killing query\n")
                os._exit(137)
            time.sleep(0.2)
    thread = threading.Thread(target=watch, daemon=True)
    thread.start()


def _link(parquet_dir, target_dir, tables):
    """Symlink the named Parquet files from parquet_dir into a fresh target_dir."""
    os.makedirs(target_dir, exist_ok=True)
    for table in tables:
        os.symlink(
            os.path.join(parquet_dir, f"{table}.parquet"),
            os.path.join(target_dir, f"{table}.parquet"),
        )
    return target_dir


def _build_sources(parquet_dir, work_dir, groups):
    """Create one connected ParquetDataSource per source group."""
    from federated_query.datasources.parquet import ParquetDataSource

    sources = []
    for name, tables in groups.items():
        directory = _link(parquet_dir, os.path.join(work_dir, name), tables)
        datasource = ParquetDataSource(name, {"dir": directory})
        datasource.connect()
        sources.append(datasource)
    return sources


def _build_runtime(sources):
    """A QueryExecutor over the given sources with the standard rule set."""
    from federated_query.catalog import Catalog
    from federated_query.config.config import ExecutorConfig
    from federated_query.executor.executor import Executor
    from federated_query.optimizer import (
        AggregatePushdownRule, LimitPushdownRule, OrderByPushdownRule,
        PredicatePushdownRule, ProjectionPushdownRule, RuleBasedOptimizer,
    )
    from federated_query.optimizer.decorrelation import Decorrelator
    from federated_query.optimizer.physical_planner import PhysicalPlanner
    from federated_query.parser import Binder, Parser
    from federated_query.processor import QueryExecutor, StarExpansionProcessor

    catalog = Catalog()
    for datasource in sources:
        catalog.register_datasource(datasource)
    catalog.load_metadata()
    parser = Parser()
    optimizer = RuleBasedOptimizer(catalog)
    for rule in (PredicatePushdownRule(), ProjectionPushdownRule(),
                 AggregatePushdownRule(), OrderByPushdownRule(), LimitPushdownRule()):
        optimizer.add_rule(rule)
    return QueryExecutor(
        catalog=catalog, parser=parser, binder=Binder(catalog), optimizer=optimizer,
        planner=PhysicalPlanner(catalog), physical_executor=Executor(ExecutorConfig()),
        processors=[StarExpansionProcessor(catalog, dialect=parser.dialect)],
        decorrelator=Decorrelator(),
    )


def _qualify(sql, source_of):
    """Qualify each TPC-H table to `<source>.main.<table>` for its source."""
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        source = source_of.get(table.name.lower())
        if source and not table.args.get("db"):
            table.set("db", exp.to_identifier("main"))
            table.set("catalog", exp.to_identifier(source))
    return tree.sql(dialect="postgres")


def _norm(rows):
    """Order-insensitive rows with every number coerced to 2-decimal float."""
    out = []
    for row in rows:
        cleaned = []
        for value in row:
            if isinstance(value, (float, Decimal)):
                cleaned.append(round(float(value), 2))
            else:
                cleaned.append(value)
        out.append(tuple(cleaned))
    return sorted(out, key=lambda r: tuple(str(x) for x in r))


def _arrow_rows(table):
    """Normalized rows from an Arrow table."""
    rows = []
    for i in range(table.num_rows):
        rows.append(tuple(column[i].as_py() for column in table.columns))
    return _norm(rows)


def _median3(thunk):
    """Median wall time (ms) over 3 runs after one warm-up, plus the result."""
    thunk()
    times = []
    for _ in range(3):
        start = time.time()
        result = thunk()
        times.append((time.time() - start) * 1000)
    times.sort()
    return times[1], result


def _reference(parquet_dir):
    """A DuckDB connection with a view per table over the raw Parquet files."""
    import duckdb

    connection = duckdb.connect()
    for table in ALL_TABLES:
        path = os.path.join(parquet_dir, f"{table}.parquet")
        connection.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    return connection


def _run_one(args):
    """Run a single query under the RSS cap; print `RESULT ours_ms duck_ms match`."""
    from federated_query.executor.rust_ir import execute_via_rust

    _start_memory_watchdog(int(args.memlimit_gb * 1024 ** 3))
    groups, source_of = _source_layout(args.mode)
    work = tempfile.mkdtemp(prefix="tpch_one_")
    try:
        sources = _build_sources(args.data, work, groups)
        qe = _build_runtime(sources)
        duck = _reference(args.data)
        raw = open(os.path.join(QUERY_DIR, f"{args.one}.sql")).read().strip().rstrip(";")
        plan = qe._plan_pipeline(_qualify(raw, source_of), profiler=None)
        ours_ms, ours = _median3(lambda: _arrow_rows(execute_via_rust(plan, sources)))
        duck_ms, ref = _median3(lambda: _norm(duck.execute(raw).fetchall()))
        print(f"RESULT {ours_ms:.1f} {duck_ms:.1f} {'OK' if ours == ref else 'DIFF'}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ----------------------------- parent: all 22 ------------------------------

def _run_subprocess(args, name):
    """Run one query in a capped subprocess; return (ours_ms, duck_ms, match) or a status."""
    cmd = [
        sys.executable, "-u", os.path.abspath(__file__), "--one", name,
        "--mode", args.mode, "--data", args.data,
        "--memlimit-gb", str(args.memlimit_gb),
    ]
    try:
        done = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return None
    if done.returncode == 137:
        return "KILLED >mem"
    if done.returncode != 0:
        return "ERROR"
    for line in done.stdout.splitlines():
        if line.startswith("RESULT"):
            _, ours_ms, duck_ms, match = line.split()
            return (float(ours_ms), float(duck_ms), match)
    return "NO-RESULT"


def _print_row(name, outcome):
    """Print one result row; return (ours_ms, duck_ms, matched) or (0, 0, False)."""
    if outcome is None:
        print(f"{name:6}  KILLED (timeout)")
        return 0.0, 0.0, False
    if isinstance(outcome, str):
        print(f"{name:6}  {outcome}")
        return 0.0, 0.0, False
    ours_ms, duck_ms, match = outcome
    print(f"{name:6} {ours_ms:9.1f}m {duck_ms:8.1f}m "
          f"{ours_ms / duck_ms:6.2f}x  {match}")
    return ours_ms, duck_ms, match == "OK"


def _run_all(args):
    """Run every query in its own capped subprocess and print the table."""
    print(f"mode={args.mode}  data={args.data}  "
          f"memlimit={args.memlimit_gb}GB  timeout={args.timeout}s\n")
    print(f"{'query':6} {'ours(rust)':>11} {'duckdb':>9} {'slower':>8}  match")
    ours_total = duck_total = 0.0
    correct = 0
    for i in range(1, 23):
        name = f"q{i:02d}"
        ours_ms, duck_ms, matched = _print_row(name, _run_subprocess(args, name))
        ours_total += ours_ms
        duck_total += duck_ms
        correct += 1 if matched else 0
    ratio = ours_total / duck_total if duck_total else float("nan")
    print(f"\n{'TOTAL':6} {ours_total:9.1f}m {duck_total:8.1f}m {ratio:6.2f}x")
    print(f"correct: {correct}/22 (exact match vs DuckDB; killed/errored count as not-correct)")


def main():
    """Dispatch to child (--one) or parent (run all 22)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("single", "federated"), default="federated")
    parser.add_argument("--data", default="/workspace/tpch_parquet")
    parser.add_argument("--memlimit-gb", type=float, default=12.0)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--one", default=None, help="internal: run one query qNN")
    args = parser.parse_args()
    if args.one:
        _run_one(args)
    else:
        _run_all(args)


if __name__ == "__main__":
    main()
