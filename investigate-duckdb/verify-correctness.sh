#!/bin/bash
# Verify fedq and DuckDB return identical data on Q4/Q7/Q1 (full, un-limited
# aggregations so LIMIT ties can't cause spurious diffs). Backs the "Correctness"
# section of duckdb-pushdowns.md. PG must be running on :5432 with duckpoc.
set -uo pipefail
cd "$(dirname "$0")"
ATTACH="ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);"

duck_csv() { ./bin/duckdb-stable analytics-poc.duckdb 2>/dev/null <<SQL
.mode csv
.headers off
LOAD postgres;
$ATTACH
$1
SQL
}
# parse fedq pretty-table rows whose last cell is an integer -> CSV of inner cells
fedq_csv() { { echo "$1"; printf '\\q\n'; } | /workspace/venv-fedq/bin/fedq -c fedq-poc-config.yaml 2>&1 \
  | awk -F'|' 'NF>2 { v=$(NF-1); gsub(/ /,"",v); if (v ~ /^[0-9]+$/) {
        out=""; for(i=2;i<NF;i++){c=$i; gsub(/^ +| +$/,"",c); out=(i==2?c:out","c)} print out } }'; }
norm() { tr -d ' \r'; }

check() { # $1=label $2=duck-sql $3=fedq-sql
  duck_csv "$2" | norm | sort > /tmp/v_duck.txt
  fedq_csv "$3" | norm | sort > /tmp/v_fedq.txt
  d=$(wc -l < /tmp/v_duck.txt); f=$(wc -l < /tmp/v_fedq.txt)
  if diff -q /tmp/v_duck.txt /tmp/v_fedq.txt >/dev/null; then
    echo "✅ $1: IDENTICAL  (duck=$d fedq=$f rows)"
  else
    echo "❌ $1: DIFFERS    (duck=$d fedq=$f rows)"; diff /tmp/v_duck.txt /tmp/v_fedq.txt | head
  fi
}

check "Q7 selective" \
  "SELECT f.category_id, count(*) FROM access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id;" \
  "SELECT f.category_id, count(*) FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id;"
check "Q4 same-source" \
  "SELECT c.name, count(*) FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name;" \
  "SELECT c.name, count(*) FROM pg.public.files f JOIN pg.public.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name;"
check "Q1 headline" \
  "SELECT a.day, c.name, count(*) FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day, c.name;" \
  "SELECT a.day, c.name, count(*) FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day, c.name;"
