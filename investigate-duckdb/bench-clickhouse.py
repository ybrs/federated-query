#!/usr/bin/env python
"""The FAIR benchmark: fact table in ClickHouse, dimensions in PostgreSQL.

Neither engine gets the fact table "for free" — both must pull access_logs from
ClickHouse over the wire and the dimensions from PostgreSQL.

  fedq:   native ClickHouse connector (Arrow stream) + ADBC Postgres, with the
          G9 dynamic IN-list pushed from CH keys into Postgres.
  DuckDB: read access_logs from CH via httpfs+Parquet over HTTP (the static
          filter is pushed into the CH query — the only DuckDB->ClickHouse path
          that works; mysql_scanner fails because CH has no transactions) +
          postgres_scanner for the dimensions, joined locally.

Times the four cross-source queries (the PG-only queries Q0/Q2-Q5/Q9/Q11/Q12 are
unchanged and live in bench-all.sh). Run from investigate-duckdb/.
"""
import statistics
import time
import urllib.parse

import duckdb

from federated_query.cli.fedq import _prepare_runtime

CH_HTTP = "http://127.0.0.1:8123/?query="
CH_CONFIG = "fedq-poc-config-ch.yaml"

# Each query: the access_logs scan pushed to ClickHouse (DuckDB reads its Parquet),
# the rest of the DuckDB SQL, and the equivalent fedq SQL (3-part names).
QUERIES = [
    ("Q7", "selective (day+user → 10 keys)",
     "SELECT file_id FROM access_logs WHERE day='2026-02-01' AND user_id=7 FORMAT Parquet",
     "SELECT f.category_id, count(*) n FROM ch_logs a JOIN pg.files f ON f.id=a.file_id "
     "GROUP BY f.category_id ORDER BY n DESC LIMIT 5",
     "SELECT f.category_id, count(*) n FROM ch.default.access_logs a "
     "JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 "
     "GROUP BY f.category_id ORDER BY n DESC LIMIT 5;"),
    ("Q1", "headline week (308k keys)",
     "SELECT file_id, day FROM access_logs WHERE day BETWEEN '2026-02-01' AND '2026-02-07' FORMAT Parquet",
     "SELECT a.day, c.name, count(*) n FROM ch_logs a JOIN pg.files f ON f.id=a.file_id "
     "JOIN pg.categories c ON c.id=f.category_id GROUP BY a.day, c.name",
     "SELECT a.day, c.name, count(*) n FROM ch.default.access_logs a "
     "JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id "
     "WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day, c.name;"),
    ("Q6", "download, non-selective (394k keys)",
     "SELECT file_id FROM access_logs WHERE action='download' FORMAT Parquet",
     "SELECT c.name, count(*) n FROM ch_logs a JOIN pg.files f ON f.id=a.file_id "
     "JOIN pg.categories c ON c.id=f.category_id GROUP BY c.name ORDER BY n DESC LIMIT 5",
     "SELECT c.name, count(*) n FROM ch.default.access_logs a "
     "JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id "
     "WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;"),
    ("Q8", "no fact filter, dim filters (10M scanned)",
     "SELECT file_id FROM access_logs FORMAT Parquet",
     "SELECT c.name, count(*) n FROM ch_logs a JOIN pg.files f ON f.id=a.file_id "
     "JOIN pg.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true "
     "GROUP BY c.name ORDER BY n DESC LIMIT 5",
     "SELECT c.name, count(*) n FROM ch.default.access_logs a "
     "JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id "
     "WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"),
]


def _median_ms(run, reps=3):
    """Median wall time (ms) over `reps` warm runs of a zero-arg callable."""
    times = []
    for _ in range(reps):
        started = time.time()
        run()
        times.append((time.time() - started) * 1000)
    return statistics.median(times)


def _duck_runner(con, ch_scan, tail_sql):
    """A callable that runs one DuckDB CH+PG federated query."""
    url = CH_HTTP + urllib.parse.quote(ch_scan)
    sql = tail_sql.replace("ch_logs", f"read_parquet('{url}')")
    return lambda: con.execute(sql).fetchall()


def main():
    """Print the fair CH+PG benchmark for fedq vs DuckDB."""
    con = duckdb.connect()
    con.execute("LOAD httpfs; LOAD postgres; SET allow_asterisks_in_http_paths=true;")
    con.execute("ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' "
                "AS pg (TYPE postgres, READ_ONLY);")
    runtime, _, _ = _prepare_runtime(CH_CONFIG)

    print(f"{'Q':4} {'shape':36} {'DuckDB(CH+PG)':>14} {'fedq(CH+PG)':>13}")
    print(f"{'--':4} {'-'*36:36} {'-'*13:>14} {'-'*12:>13}")
    for qid, shape, ch_scan, tail_sql, fedq_sql in QUERIES:
        run_duck = _duck_runner(con, ch_scan, tail_sql)
        run_duck()  # warm
        duck_ms = _median_ms(run_duck)
        runtime.execute(fedq_sql)  # warm
        fedq_ms = _median_ms(lambda: runtime.execute(fedq_sql))
        print(f"{qid:4} {shape:36} {duck_ms:11.0f} ms {fedq_ms:10.0f} ms")


if __name__ == "__main__":
    main()
