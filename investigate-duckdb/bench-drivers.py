#!/usr/bin/env python
"""COPY vs direct SQL — compare the two PostgreSQL fetch drivers.

Finding: DuckDB reads PostgreSQL with binary ``COPY (SELECT …) TO STDOUT``.
fedq's *default* psycopg2 driver instead runs a plain ``SELECT`` and converts
psycopg2's Python row tuples into Arrow chunk by chunk — per-cell Python work.
fedq's ADBC driver (``driver: adbc``) already uses the SAME binary COPY DuckDB
does, streaming Postgres binary straight into Arrow buffers in C.

So "use COPY instead of direct SQL" is not something to hand-roll — it's already
there behind ``driver: adbc``, and it is ~2-3x faster than the psycopg2 path.
Run from investigate-duckdb/ with PG up on :5432 (duckpoc).
"""
import time

from federated_query.datasources.postgresql import PostgreSQLDataSource

CONFIG = dict(
    host="localhost", port=5432, database="duckpoc",
    user="postgres", password="", schemas=["public"],
)
QUERIES = [
    ("SELECT id,category_id,size_bytes FROM files WHERE size_bytes>9000000",
     "files filtered (~40k)"),
    ("SELECT id,category_id FROM files", "files full (400k)"),
    ("SELECT * FROM categories", "categories (50k)"),
]


def _best_drain_ms(driver, query):
    """Median-of-best wall time to drain a query to Arrow via one driver."""
    config = dict(CONFIG)
    if driver:
        config["driver"] = driver
    source = PostgreSQLDataSource("bench", config)
    source.connect()
    for _ in range(2):  # warm
        sum(batch.num_rows for batch in source.execute_query(query))
    best = float("inf")
    rows = 0
    for _ in range(5):
        started = time.time()
        rows = sum(batch.num_rows for batch in source.execute_query(query))
        best = min(best, (time.time() - started) * 1000)
    source.disconnect()
    return best, rows


def main():
    """Print the psycopg2-vs-ADBC drain time for each probe query."""
    print(f"{'query':24} {'psycopg2 SELECT':>16} {'ADBC binary COPY':>18} {'speedup':>9}")
    for query, label in QUERIES:
        psyco, rows = _best_drain_ms(None, query)
        adbc, _ = _best_drain_ms("adbc", query)
        print(f"{label:24} {psyco:13.1f} ms {adbc:15.1f} ms {psyco / adbc:8.1f}x")


if __name__ == "__main__":
    main()
