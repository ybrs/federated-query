"""TPC-H benchmark: the Rust engine vs DuckDB over the SAME Parquet files.

Two modes (``--mode``):

  single     All 8 tables live in ONE Parquet source, so the whole query pushes
             to the Rust engine as a single plan. This measures the ENGINE
             (DataFusion) against DuckDB - no federation, no cross-source joins.

  federated  Tables split across TWO Parquet sources - facts (lineitem, orders,
             partsupp) and dims (part, supplier, customer, nation, region) - so
             most queries JOIN across sources. This measures OUR federation layer
             (per-source scans + cross-source join + our join ordering) stacked
             on top of the engine.

Both modes read the identical Parquet files. DuckDB reading those files directly
is the correctness + speed reference, IDENTICAL for both modes - so the gap in
`single` is pure engine, and the extra gap in `federated` is our planner.

Usage:
    python benchmarks/tpch/run_tpch.py --mode single    --data /workspace/tpch_parquet
    python benchmarks/tpch/run_tpch.py --mode federated --data /workspace/tpch_parquet
"""

import argparse
import os
import shutil
import tempfile
import time
from decimal import Decimal

import duckdb
import sqlglot
from sqlglot import exp

from federated_query.catalog import Catalog
from federated_query.config.config import ExecutorConfig
from federated_query.datasources.parquet import ParquetDataSource
from federated_query.executor.executor import Executor
from federated_query.executor.rust_ir import execute_via_rust
from federated_query.optimizer import (
    AggregatePushdownRule, LimitPushdownRule, OrderByPushdownRule,
    PredicatePushdownRule, ProjectionPushdownRule, RuleBasedOptimizer,
)
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser
from federated_query.processor import QueryExecutor, StarExpansionProcessor

QUERY_DIR = os.path.join(os.path.dirname(__file__), "queries")
ALL_TABLES = (
    "lineitem", "orders", "partsupp", "part", "supplier", "customer",
    "nation", "region",
)
FACTS = ("lineitem", "orders", "partsupp")
DIMS = ("part", "supplier", "customer", "nation", "region")


def _link(parquet_dir, target_dir, tables):
    """Symlink the named Parquet files from parquet_dir into a fresh target_dir."""
    os.makedirs(target_dir, exist_ok=True)
    for table in tables:
        os.symlink(
            os.path.join(parquet_dir, f"{table}.parquet"),
            os.path.join(target_dir, f"{table}.parquet"),
        )
    return target_dir


def _source_layout(mode):
    """Map (source_name -> its tables) for a mode, plus each table's source."""
    if mode == "single":
        groups = {"tpch": ALL_TABLES}
    else:
        groups = {"facts": FACTS, "dims": DIMS}
    source_of = {}
    for source, tables in groups.items():
        for table in tables:
            source_of[table] = source
    return groups, source_of


def _build_sources(parquet_dir, work_dir, groups):
    """Create one connected ParquetDataSource per source group."""
    sources = []
    for name, tables in groups.items():
        directory = _link(parquet_dir, os.path.join(work_dir, name), tables)
        datasource = ParquetDataSource(name, {"dir": directory})
        datasource.connect()
        sources.append(datasource)
    return sources


def _build_runtime(sources):
    """A QueryExecutor over the given sources with the standard rule set."""
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
    connection = duckdb.connect()
    for table in ALL_TABLES:
        path = os.path.join(parquet_dir, f"{table}.parquet")
        connection.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    return connection


def _run_query(qe, sources, duck, source_of, name):
    """Run one query on both engines; return (ours_ms, duck_ms, ours, ref) or None."""
    raw = open(os.path.join(QUERY_DIR, f"{name}.sql")).read().strip().rstrip(";")
    plan = qe._plan_pipeline(_qualify(raw, source_of), profiler=None)
    ours_ms, ours = _median3(lambda: _arrow_rows(execute_via_rust(plan, sources)))
    duck_ms, ref = _median3(lambda: _norm(duck.execute(raw).fetchall()))
    return ours_ms, duck_ms, ours, ref


def _report(qe, sources, duck, source_of):
    """Run all 22 queries, print the per-query table, return totals + correct count."""
    print(f"{'query':6} {'ours(rust)':>11} {'duckdb':>9} {'ratio':>7}  match")
    ours_total = duck_total = 0.0
    correct = 0
    for i in range(1, 23):
        name = f"q{i:02d}"
        try:
            ours_ms, duck_ms, ours, ref = _run_query(qe, sources, duck, source_of, name)
        except Exception as error:  # noqa: BLE001 - a failing query must be visible, not fatal
            print(f"{name:6}  ours ERROR: {str(error).splitlines()[0][:66]}")
            continue
        ours_total += ours_ms
        duck_total += duck_ms
        matched = ours == ref
        correct += 1 if matched else 0
        print(f"{name:6} {ours_ms:9.1f}m {duck_ms:8.1f}m "
              f"{duck_ms / ours_ms:6.2f}x  {'OK' if matched else 'DIFF'}")
    return ours_total, duck_total, correct


def main():
    """Parse args, build both engines for the chosen mode, run, and summarize."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("single", "federated"), default="federated")
    parser.add_argument("--data", default="/workspace/tpch_parquet")
    args = parser.parse_args()

    groups, source_of = _source_layout(args.mode)
    work_dir = tempfile.mkdtemp(prefix="tpch_bench_")
    try:
        sources = _build_sources(args.data, work_dir, groups)
        qe = _build_runtime(sources)
        duck = _reference(args.data)
        print(f"mode={args.mode}  sources={list(groups)}  data={args.data}\n")
        ours_total, duck_total, correct = _report(qe, sources, duck, source_of)
        print(f"\n{'TOTAL':6} {ours_total:9.1f}m {duck_total:8.1f}m "
              f"{duck_total / ours_total:6.2f}x")
        print(f"correct: {correct}/22 (exact match vs DuckDB)")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
