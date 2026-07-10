"""Cold-sources benchmark: does the learned catalog recover performance lost to
MISSING remote statistics?

TPC-DS is fully ANALYZEd, so learning has no gaps to fill there. This harness
simulates the realistic FRESH-LOAD state instead: it CLEARS Postgres column
statistics (pg_statistic) and row counts (reltuples) for the TPC-DS tables - the
state a never-ANALYZEd federated source is actually in - runs a stat-sensitive
query subset two ways, and restores stats at the end:

  - cold, learning OFF  : the plan a fresh federated source produces (no stats).
  - cold, learning ON    : after the catalog has measured a warm-up run.

The per-query delta is the value the learned catalog adds when sources have no
statistics of their own. Correctness is checked too (both must still be right).

Usage: PGUSER=postgres python cold_sources.py [scale]   (default scale 10)
"""
import os
import sys
import time
import types

import psycopg2

from run import _read_query
from run_federated import (
    build_fedq, _qualify, FEDQ_SOURCES, PLACEMENTS, _db_path, DEFAULT_DATA_DIR,
    pg_database_name,
)
from qualify import TPCDS_TABLES

# Stat-sensitive queries: dim-shipping gating and reduction orientation both key
# off dimension row counts / NDVs, so they are where missing stats bite hardest.
SUBSET = ["q07", "q21", "q22", "q23", "q39", "q54"]
WARMUPS = 2       # learning-on passes before the timed warm run
TIMED_RUNS = 4    # best-of, per query


def _options(scale):
    return types.SimpleNamespace(
        scale_factor=scale, pg_host="localhost", pg_port="5432",
        pg_user="postgres", pg_password="postgres", pg_database=None,
    )


def _pg(scale):
    """A superuser connection to the scale's TPC-DS Postgres database."""
    return psycopg2.connect(
        host="localhost", port=5432, user="postgres", password="postgres",
        dbname=pg_database_name(scale),
    )


def _set_stats(conn, cold):
    """Clear (cold=True) or restore (cold=False) the TPC-DS tables' statistics.
    Cold = no pg_statistic rows and reltuples=-1: exactly a never-ANALYZEd load.
    Going cold also DISABLES per-table autovacuum so a background autoanalyze
    cannot silently re-derive statistics mid-benchmark (reads never trigger it,
    but the experiment must not depend on that). Restore = re-enable autovacuum
    and ANALYZE, re-deriving real stats so the DB is left as found."""
    conn.autocommit = True
    cursor = conn.cursor()
    for table in sorted(TPCDS_TABLES):
        if cold:
            cursor.execute(
                f'ALTER TABLE "{table}" SET (autovacuum_enabled = false)'
            )
            cursor.execute(
                "DELETE FROM pg_statistic WHERE starelid = to_regclass(%s)",
                (f"public.{table}",),
            )
            cursor.execute(
                "UPDATE pg_class SET reltuples = -1, relpages = 0 "
                "WHERE oid = to_regclass(%s)",
                (f"public.{table}",),
            )
        else:
            cursor.execute(f'ALTER TABLE "{table}" RESET (autovacuum_enabled)')
            cursor.execute(f'ANALYZE "{table}"')


def _verify_still_cold(conn):
    """Assert no TPC-DS table regained statistics during the cold phases; a
    benchmark that silently measured the ANALYZED path would report a lie."""
    cursor = conn.cursor()
    for table in sorted(TPCDS_TABLES):
        cursor.execute(
            "SELECT count(*), (SELECT reltuples FROM pg_class"
            " WHERE oid = to_regclass(%s))"
            " FROM pg_statistic WHERE starelid = to_regclass(%s)",
            (f"public.{table}", f"public.{table}"),
        )
        stat_rows, reltuples = cursor.fetchone()
        if stat_rows != 0 or reltuples >= 0:
            raise RuntimeError(
                f"cold benchmark invalidated: {table} regained pg statistics "
                f"mid-run (pg_statistic rows={stat_rows}, reltuples={reltuples})"
            )


def _sqls(scale):
    """The qualified engine SQL for each subset query."""
    placement = PLACEMENTS["pg-dims"]
    sqls = {}
    for name in SUBSET:
        raw = _read_query(f"queries/{name}.sql")
        sqls[name] = _qualify(raw, placement, FEDQ_SOURCES, "postgres")
    return sqls


def _timed(runtime, sql):
    """Best-of engine time (ms) for one query, plus its row count."""
    best = float("inf")
    rows = 0
    for _ in range(TIMED_RUNS):
        started = time.perf_counter()
        result = runtime.execute(sql)
        best = min(best, (time.perf_counter() - started) * 1000.0)
        rows = result.num_rows
    return best, rows


def _run(scale, sqls, catalog_path):
    """Run every subset query on a fresh runtime; return {name: (ms, rows)}.
    A catalog_path enables learning and is warmed before timing."""
    if catalog_path:
        os.environ["FEDQ_STATS_CATALOG"] = catalog_path
    else:
        os.environ.pop("FEDQ_STATS_CATALOG", None)
    runtime = build_fedq(_db_path(DEFAULT_DATA_DIR, scale), _options(scale))
    warm = WARMUPS if catalog_path else 0
    for _ in range(warm):
        for name in SUBSET:
            runtime.execute(sqls[name])
    results = {}
    for name in SUBSET:
        results[name] = _timed(runtime, sqls[name])
    return results


def main():
    scale = sys.argv[1] if len(sys.argv) > 1 else "10"
    sqls = _sqls(scale)
    conn = _pg(scale)
    try:
        # ANALYZED runs FIRST, with the source's real statistics intact: it is
        # the convergence target - perfect learning reproduces this time.
        print(f"=== ANALYZED sources (real pg stats), scale {scale} ===")
        analyzed = _run(scale, sqls, catalog_path=None)
        _set_stats(conn, cold=True)
        print(f"=== COLD sources (no pg stats), scale {scale} ===")
        off = _run(scale, sqls, catalog_path=None)
        warm = _run(scale, sqls, catalog_path="/tmp/cold_learn.sqlite")
        # The measurements above are only meaningful if the source stayed
        # statless the whole time - verify before restoring anything.
        _verify_still_cold(conn)
    finally:
        _set_stats(conn, cold=False)  # always restore
        conn.close()
        if os.path.exists("/tmp/cold_learn.sqlite"):
            os.remove("/tmp/cold_learn.sqlite")
    _report(analyzed, off, warm)


def _report(analyzed, off, warm):
    """Print the scoreboard: the criterion is WARM CONVERGING to ANALYZED
    (the plan real statistics buy), not merely beating OFF."""
    header = (f"\n{'query':7} {'ANALYZED':>9} {'OFF(ms)':>9} {'WARM(ms)':>9} "
              f"{'W-OFF':>8} {'W-ANLZ':>8}  rows(anlz/off/warm)")
    print(header)
    for name in SUBSET:
        anlz_ms, anlz_rows = analyzed[name]
        off_ms, off_rows = off[name]
        warm_ms, warm_rows = warm[name]
        match = "OK" if off_rows == warm_rows == anlz_rows else "ROWS DIFFER"
        print(f"{name:7} {anlz_ms:9.0f} {off_ms:9.0f} {warm_ms:9.0f} "
              f"{warm_ms-off_ms:+8.0f} {warm_ms-anlz_ms:+8.0f}  "
              f"{anlz_rows}/{off_rows}/{warm_rows} {match}")


if __name__ == "__main__":
    main()
