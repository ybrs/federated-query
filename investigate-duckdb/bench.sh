#!/bin/bash
# Median-of-5 warm timing for Q4/Q7/Q1 across duckdb-stable, duckdb-main, fedq.
# Reproduces the "Timing" table in duckdb-pushdowns.md (warm, localhost, single
# connection). PG must be running on :5432 with the duckpoc db.
set -uo pipefail
cd "$(dirname "$0")"
EXT="$PWD/pg-scanner-src/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
ATTACH="ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);"
med() { sort -n | awk '{v[NR]=$1} END{print (NR%2)? v[(NR+1)/2] : (v[NR/2]+v[NR/2+1])/2}'; }

# duckdb table refs (pg.<table>, local access_logs)
duck_q4="SELECT c.name, count(*) n FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
duck_q7="SELECT f.category_id, count(*) a FROM access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY a DESC LIMIT 5;"
duck_q1="SELECT a.day, c.name, count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day, c.name;"

duck_time() { # $1=binary(+flags)  $2=loadstmt  $3=query
  { echo ".timer on"; echo "$2"; echo "$ATTACH"; for i in 1 2 3 4 5; do echo "$3"; done; } \
   | $1 analytics-poc.duckdb 2>/dev/null | grep "Run Time" | tail -5 | awk '{print $5*1000}' | med; }

echo "engine        Q4    Q7    Q1   (median ms of 5 warm runs)"
printf "duckdb-stable %5s %5s %5s\n" \
  "$(duck_time "./bin/duckdb-stable" "LOAD postgres;" "$duck_q4")" \
  "$(duck_time "./bin/duckdb-stable" "LOAD postgres;" "$duck_q7")" \
  "$(duck_time "./bin/duckdb-stable" "LOAD postgres;" "$duck_q1")"
printf "duckdb-main   %5s %5s %5s\n" \
  "$(duck_time "pg-scanner-src/build/release/duckdb -unsigned" "LOAD '$EXT';" "$duck_q4")" \
  "$(duck_time "pg-scanner-src/build/release/duckdb -unsigned" "LOAD '$EXT';" "$duck_q7")" \
  "$(duck_time "pg-scanner-src/build/release/duckdb -unsigned" "LOAD '$EXT';" "$duck_q1")"

# fedq table refs (3-part names)
fedq_q4="SELECT c.name, count(*) n FROM pg.public.files f JOIN pg.public.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
fedq_q7="SELECT f.category_id, count(*) a FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY a DESC LIMIT 5;"
fedq_q1="SELECT a.day, c.name, count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day, c.name;"
fedq_time() { { for i in 1 2 3 4 5; do echo "$1"; done; printf '\\q\n'; } \
  | /workspace/venv-fedq/bin/fedq -c fedq-poc-config.yaml 2>&1 | grep "rows in" | awk '{print $(NF-1)}' | med; }
printf "fedq          %5s %5s %5s\n" "$(fedq_time "$fedq_q4")" "$(fedq_time "$fedq_q7")" "$(fedq_time "$fedq_q1")"
