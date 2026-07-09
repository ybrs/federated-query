"""Perf comparison of three federated engines over the SAME remote Postgres:

  1. our engine, DuckDB merge (current)
  2. our engine, Rust/DataFusion (new)
  3. DuckDB reading Postgres via postgres_scanner (the apples-to-apples rival)

Cross-source is simulated by registering the PG under two datasource names
(srcA/srcB) and assigning tables to each. Timing is execution-only (the plan and
IR are built once, outside the timed loop).

Prerequisites:
  - TPC-H tables loaded into PostgreSQL (see load_postgres.py), reachable via the
    standard POSTGRES_* env vars (defaults: localhost:5432, db test_db).
  - The `fedqrs` extension built into the active venv (maturin develop).
  - DuckDB with the `postgres` extension available (auto-installed on load).

Run:  POSTGRES_DB=<db> python benchmarks/tpch/run_engine_perf.py
"""

import json
import os
import time

import duckdb
import pyarrow as pa

import fedqrs
from federated_query.catalog import Catalog
from federated_query.config.config import Config
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.executor import Executor
from federated_query.executor.rust_ir import (
    UnsupportedIR,
    build_ir,
    register_datasources,
)
from federated_query.optimizer import build_optimizer
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser
from federated_query.processor import QueryExecutor, StarExpansionProcessor


def pg_config():
    """PostgreSQL connection config from the standard POSTGRES_* env vars."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": ["public"],
        "driver": "adbc",
    }


def build_executor(datasources):
    """Construct the full query pipeline over the given (connected) sources."""
    catalog = Catalog()
    for datasource in datasources:
        datasource.connect()
        catalog.register_datasource(datasource)
    catalog.load_metadata()
    parser = Parser()
    config = Config()
    optimizer = build_optimizer(catalog, config.optimizer, config.cost)
    return QueryExecutor(
        catalog=catalog, parser=parser, binder=Binder(catalog), optimizer=optimizer,
        planner=PhysicalPlanner(catalog), physical_executor=Executor(config.executor),
        processors=[StarExpansionProcessor(catalog, dialect=parser.dialect)],
        decorrelator=Decorrelator(),
    )


def attach_postgres(cfg):
    """A DuckDB connection with the same Postgres attached as schema `pg`."""
    conn = duckdb.connect()
    conn.execute("INSTALL postgres; LOAD postgres;")
    dsn = (
        f"dbname={cfg['database']} user={cfg['user']} password={cfg['password']} "
        f"host={cfg['host']} port={cfg['port']}"
    )
    conn.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES, READ_ONLY)")
    return conn


def best(fn, n=5):
    """Best (min) wall-clock over n runs, in ms, after one warmup."""
    fn()
    samples = []
    for _ in range(n):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    return min(samples)


def _build_ir_or_none(plan):
    """Serialize to IR on the pristine plan, or None if the shape is unsupported.

    The DuckDB merge path mutates the plan at execution (apply_dynamic_filter
    bakes the IN filter into the probe scan), so this must run before it.
    """
    try:
        return json.dumps(build_ir(plan))
    except UnsupportedIR:
        return None


def _time_rust(ir):
    """Execution-only time of the Rust engine for a prepared IR."""
    return best(
        lambda: pa.RecordBatchReader.from_stream(fedqrs.execute_ir(ir)[0]).read_all()
    )


def run(qe, duck, label, fed_sql, duck_pg_sql):
    """Time all three engines on one query and print a comparison block."""
    plan = qe._plan_pipeline(fed_sql, profiler=None)
    ir = _build_ir_or_none(plan)
    current = best(lambda: qe.physical_executor.execute_to_table(plan))
    duck_pg = best(lambda: duck.execute(duck_pg_sql).arrow())

    print(f"\n{label}")
    print(f"  our engine (DuckDB merge)  : {current:7.1f} ms")
    _print_rust(current, duck_pg, ir)
    print(f"  DuckDB over Postgres       : {duck_pg:7.1f} ms")


def _print_rust(current, duck_pg, ir):
    """Print the Rust-engine line, or note the shape is unsupported."""
    if ir is None:
        print("  our engine (Rust/DataFus.) : unsupported shape")
        return
    rust = _time_rust(ir)
    print(
        f"  our engine (Rust/DataFus.) : {rust:7.1f} ms   "
        f"({current / rust:.2f}x vs current, {duck_pg / rust:.2f}x vs DuckDB)"
    )


def main():
    """Set up the engines and run the benchmark queries."""
    cfg = pg_config()
    datasources = [
        PostgreSQLDataSource("srcA", dict(cfg)),
        PostgreSQLDataSource("srcB", dict(cfg)),
    ]
    qe = build_executor(datasources)
    register_datasources(datasources)
    duck = attach_postgres(cfg)
    for label, fed_sql, duck_pg_sql in _QUERIES:
        run(qe, duck, label, fed_sql, duck_pg_sql)


_QUERIES = (
    (
        "Q_xcount  lineitem JOIN part(p_size=15), count",
        "SELECT count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15",
        "SELECT count(*) FROM pg.public.lineitem "
        "JOIN pg.public.part ON l_partkey = p_partkey WHERE p_size = 15",
    ),
    (
        "Q_xrev    lineitem JOIN part, sum revenue (decimal)",
        "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15",
        "SELECT sum(l_extendedprice * (1 - l_discount)) FROM pg.public.lineitem "
        "JOIN pg.public.part ON l_partkey = p_partkey WHERE p_size = 15",
    ),
    (
        "Q_xgroup  lineitem JOIN part, count by brand",
        "SELECT p_brand, count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15 "
        "GROUP BY p_brand ORDER BY n DESC",
        "SELECT p_brand, count(*) FROM pg.public.lineitem "
        "JOIN pg.public.part ON l_partkey = p_partkey WHERE p_size = 15 "
        "GROUP BY p_brand ORDER BY count(*) DESC",
    ),
    (
        "Q_xorders lineitem JOIN orders(status=F), count",
        "SELECT count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.orders ON l_orderkey = o_orderkey WHERE o_orderstatus = 'F'",
        "SELECT count(*) FROM pg.public.lineitem "
        "JOIN pg.public.orders ON l_orderkey = o_orderkey WHERE o_orderstatus = 'F'",
    ),
)


if __name__ == "__main__":
    main()
