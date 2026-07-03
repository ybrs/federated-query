"""Perf comparison: the DuckDB merge engine vs the Rust/DataFusion engine.

Both engines read the SAME TPC-H data from PostgreSQL; cross-source is simulated
by registering the PG under two datasource names (srcA/srcB) and assigning
tables to each, so a join across them is genuinely federated. Timing is
execution-only (the plan and IR are built once, outside the timed loop). A
DuckDB-native run over the tpch_sf<SF>.duckdb file is shown as a no-federation
floor.

Prerequisites:
  - TPC-H tables loaded into PostgreSQL (see load_postgres.py), reachable via the
    standard POSTGRES_* env vars (defaults: localhost:5432, db test_db).
  - The `fedqrs` extension built into the active venv (maturin develop).
  - data/tpch_sf<SF>.duckdb present (see generate.py) for the floor.

Run:  POSTGRES_DB=<db> python benchmarks/tpch/run_engine_perf.py
"""

import json
import os
import statistics as st
import time

import duckdb
import pyarrow as pa

import fedqrs
from federated_query.catalog import Catalog
from federated_query.config.config import ExecutorConfig
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.executor import Executor
from federated_query.executor.rust_ir import (
    UnsupportedIR,
    build_ir,
    register_datasources,
)
from federated_query.optimizer import (
    AggregatePushdownRule,
    LimitPushdownRule,
    OrderByPushdownRule,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    RuleBasedOptimizer,
)
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.parser import Binder, Parser
from federated_query.processor import QueryExecutor, StarExpansionProcessor

SF = os.environ.get("TPCH_SF", "0.1")
DUCKFILE = os.path.join(os.path.dirname(__file__), "data", f"tpch_sf{SF}.duckdb")


def pg_config():
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
    catalog = Catalog()
    for ds in datasources:
        ds.connect()
        catalog.register_datasource(ds)
    catalog.load_metadata()
    parser = Parser()
    optimizer = RuleBasedOptimizer(catalog)
    for rule in (
        PredicatePushdownRule(), ProjectionPushdownRule(), AggregatePushdownRule(),
        OrderByPushdownRule(), LimitPushdownRule(),
    ):
        optimizer.add_rule(rule)
    return QueryExecutor(
        catalog=catalog, parser=parser, binder=Binder(catalog), optimizer=optimizer,
        planner=PhysicalPlanner(catalog), physical_executor=Executor(ExecutorConfig()),
        processors=[StarExpansionProcessor(catalog, dialect=parser.dialect)],
        decorrelator=Decorrelator(),
    )


def best(fn, n=5):
    fn()  # warmup
    samples = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t) * 1000)
    return min(samples), st.median(samples)


def run(qe, duck, label, sql, duck_sql=None, kind="cross"):
    plan = qe._plan_pipeline(sql, profiler=None)
    executor = qe.physical_executor

    # Build the IR on the PRISTINE plan: the DuckDB path mutates the plan at
    # execution (apply_dynamic_filter bakes the IN filter into the probe scan),
    # so serialize before running it.
    try:
        ir = json.dumps(build_ir(plan))
        rust_ok = True
    except UnsupportedIR as exc:
        rust_ok, rust = False, f"unsupported ({exc})"

    a_rows = executor.execute_to_table(plan).num_rows
    a_min, a_med = best(lambda: executor.execute_to_table(plan))

    speed = ""
    if rust_ok:
        b_tbl = pa.RecordBatchReader.from_stream(fedqrs.execute_ir(ir)).read_all()
        b_min, b_med = best(
            lambda: pa.RecordBatchReader.from_stream(fedqrs.execute_ir(ir)).read_all()
        )
        match = "same-rows" if a_rows == b_tbl.num_rows else f"ROWS {a_rows}!={b_tbl.num_rows}"
        rust = f"{b_min:8.1f} ms (median {b_med:.1f})  [{match}]"
        speed = f"  -> Rust {a_min / b_min:.2f}x" if b_min else ""

    print(f"\n{label}  [{kind}]  rows={a_rows}")
    print(f"  DuckDB-merge (current) : {a_min:8.1f} ms (median {a_med:.1f})")
    print(f"  Rust/DataFusion (new)  : {rust}{speed}")
    if duck_sql:
        d_min, _ = best(lambda: duck.execute(duck_sql).arrow())
        print(f"  DuckDB-native (floor)  : {d_min:8.1f} ms  (no federation)")


def main():
    cfg = pg_config()
    datasources = [
        PostgreSQLDataSource("srcA", dict(cfg)),
        PostgreSQLDataSource("srcB", dict(cfg)),
    ]
    qe = build_executor(datasources)
    register_datasources(datasources)
    duck = duckdb.connect(DUCKFILE, read_only=True) if os.path.exists(DUCKFILE) else None

    run(qe, duck, "Q_single  one-table sum (pushed to PG)",
        "SELECT sum(l_extendedprice * l_discount) AS revenue FROM srcA.public.lineitem "
        "WHERE l_quantity < 24 AND l_discount >= 0.05 AND l_discount <= 0.07",
        kind="single-source")

    run(qe, duck, "Q_xcount  lineitem JOIN part(p_size=15), count",
        "SELECT count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15",
        "SELECT count(*) FROM lineitem JOIN part ON l_partkey = p_partkey WHERE p_size = 15")

    run(qe, duck, "Q_xrev    lineitem JOIN part, sum revenue (decimal)",
        "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15",
        "SELECT sum(l_extendedprice * (1 - l_discount)) FROM lineitem "
        "JOIN part ON l_partkey = p_partkey WHERE p_size = 15")

    run(qe, duck, "Q_xgroup  lineitem JOIN part, count by brand",
        "SELECT p_brand, count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.part ON l_partkey = p_partkey WHERE p_size = 15 "
        "GROUP BY p_brand ORDER BY n DESC",
        "SELECT p_brand, count(*) FROM lineitem JOIN part ON l_partkey = p_partkey "
        "WHERE p_size = 15 GROUP BY p_brand ORDER BY count(*) DESC")

    run(qe, duck, "Q_xorders lineitem JOIN orders(status=F), count",
        "SELECT count(*) AS n FROM srcA.public.lineitem "
        "JOIN srcB.public.orders ON l_orderkey = o_orderkey WHERE o_orderstatus = 'F'",
        "SELECT count(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey "
        "WHERE o_orderstatus = 'F'")


if __name__ == "__main__":
    main()
