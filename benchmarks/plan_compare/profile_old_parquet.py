"""COLD-plan profile of the OLD engine over a PARQUET source (facts on parquet).

Builds a catalog with a single ParquetDataSource (the SF1 tpch parquet dir) and
times the cold plan of single-source fact queries. Isolates whether the OLD
planner scans the parquet data at plan time or uses cheap file/duckdb metadata.
"""

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "benchmarks", "tpch"))

from federated_query.datasources.parquet import ParquetDataSource
from federated_query.catalog.catalog import Catalog
from federated_query.config.config import Config

PARQUET_DIR = os.path.join(ROOT, "benchmarks", "tpch", "data", "parquet_sf1")
QUERIES = os.path.join(ROOT, "benchmarks", "tpch", "queries")


def _build_runtime():
    """A FedQRuntime whose only source is the SF1 tpch parquet directory."""
    from federated_query.cli.fedq import FedQRuntime

    source = ParquetDataSource("pq", {"dir": PARQUET_DIR})
    source.connect()
    catalog = Catalog()
    catalog.register_datasource(source)
    catalog.load_metadata()
    return FedQRuntime(catalog, Config())


def _qualify(raw):
    """Point every bare tpch table at pq.main.<table>."""
    import sqlglot
    from sqlglot import exp
    from qualify import TPCH_TABLES

    tree = sqlglot.parse_one(raw, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        if table.name.lower() in TPCH_TABLES and not table.args.get("db"):
            table.set("db", exp.to_identifier("main"))
            table.set("catalog", exp.to_identifier("pq"))
    return tree.sql(dialect="duckdb")


def main():
    """Time cold + warm plan of single-source parquet queries."""
    queries = sys.argv[1:] or ["q01", "q06", "q12"]
    print("query   plan_cold  plan_warm  (OLD engine, parquet source, SF1)")
    for name in queries:
        raw = open(os.path.join(QUERIES, name + ".sql")).read()
        runtime = _build_runtime()  # fresh => cold stats
        qe = runtime.query_executor
        sql = _qualify(raw)

        def plan_once():
            logical = qe._run_before_processors(sql)
            logical = qe._parse_query(logical)
            logical = qe._bind_plan(logical)
            logical = qe._decorrelate_plan(logical)
            logical = qe._optimize_plan(logical)
            return qe._build_physical_plan(logical)

        start = time.time()
        plan_once()
        cold = (time.time() - start) * 1000
        warm = []
        for _ in range(3):
            start = time.time()
            plan_once()
            warm.append((time.time() - start) * 1000)
        warm.sort()
        print("{0:6}  {1:9.1f}  {2:9.1f}".format(name, cold, warm[1]))


if __name__ == "__main__":
    main()
