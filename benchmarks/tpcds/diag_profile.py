"""Ground-truth execution multiplicity: run q39 cold-off vs cold-warm under
FEDQRS_PROFILE and count how many times each scan step actually executes."""
import os
import types
import subprocess
import sys

os.environ["FEDQRS_PROFILE"] = "1"

import psycopg2

from run import _read_query
from run_federated import (
    build_fedq, _qualify, FEDQ_SOURCES, PLACEMENTS, _db_path, DEFAULT_DATA_DIR,
    pg_database_name)
from qualify import TPCDS_TABLES

SCALE = os.environ.get("DIAG_SCALE", "10")


def _opts():
    return types.SimpleNamespace(
        scale_factor=SCALE, pg_host="localhost", pg_port="5432",
        pg_user="postgres", pg_password="postgres", pg_database=None)


def _set_stats(conn, cold):
    """Clear/restore pg statistics; autovacuum is disabled per table while
    cold so a background autoanalyze cannot re-derive stats mid-probe."""
    conn.autocommit = True
    cur = conn.cursor()
    for t in sorted(TPCDS_TABLES):
        if cold:
            cur.execute(f'ALTER TABLE "{t}" SET (autovacuum_enabled = false)')
            cur.execute("DELETE FROM pg_statistic WHERE starelid=to_regclass(%s)",
                        (f"public.{t}",))
            cur.execute("UPDATE pg_class SET reltuples=-1,relpages=0 "
                        "WHERE oid=to_regclass(%s)", (f"public.{t}",))
        else:
            cur.execute(f'ALTER TABLE "{t}" RESET (autovacuum_enabled)')
            cur.execute(f'ANALYZE "{t}"')


SUBSET = ["q07", "q21", "q22", "q23", "q39", "q54"]


def _subset_sqls():
    sqls = {}
    for name in SUBSET:
        raw = _read_query(f"queries/{name}.sql")
        sqls[name] = _qualify(raw, PLACEMENTS["pg-dims"], FEDQ_SOURCES, "postgres")
    return sqls


def main():
    label = sys.argv[1]  # "off" or "warm"
    full = len(sys.argv) > 2 and sys.argv[2] == "full"  # warm the whole subset
    sqls = _subset_sqls()
    sql = sqls["q39"]
    conn = psycopg2.connect(host="localhost", port=5432, user="postgres",
                            password="postgres", dbname=pg_database_name(SCALE))
    try:
        _set_stats(conn, cold=True)
        if label == "warm":
            os.environ["FEDQ_STATS_CATALOG"] = "/tmp/diag_prof.sqlite"
            if os.path.exists("/tmp/diag_prof.sqlite"):
                os.remove("/tmp/diag_prof.sqlite")
            rt = build_fedq(_db_path(DEFAULT_DATA_DIR, SCALE), _opts())
            for _ in range(2):
                for name in (SUBSET if full else ["q39"]):
                    rt.execute(sqls[name])
        else:
            os.environ.pop("FEDQ_STATS_CATALOG", None)
            rt = build_fedq(_db_path(DEFAULT_DATA_DIR, SCALE), _opts())
        sys.stderr.write(f"=== TIMED {label} ===\n")
        rt.execute(sql)
        sys.stderr.write(f"=== END {label} ===\n")
    finally:
        _set_stats(conn, cold=False)
        conn.close()
        if os.path.exists("/tmp/diag_prof.sqlite"):
            os.remove("/tmp/diag_prof.sqlite")


if __name__ == "__main__":
    main()
