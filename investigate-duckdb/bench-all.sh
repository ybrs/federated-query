#!/bin/bash
# Full timing sweep: every probe query on duckdb-stable, duckdb-main, and fedq.
# Median of 5 warm runs (ms). PG must be running on :5432 with the duckpoc db.
set -uo pipefail
cd "$(dirname "$0")"
EXT="$PWD/pg-scanner-src/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
ATTACH="ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);"
med() { sort -n | awk '{v[NR]=$1} END{print (NR%2)? v[(NR+1)/2] : (v[NR/2]+v[NR/2+1])/2}'; }

duck_time() { # $1=binary(+flags)  $2=loadstmt  $3=query
  { echo ".timer on"; echo "$2"; echo "$ATTACH"; for i in 1 2 3 4 5; do echo "$3"; done; } \
   | $1 analytics-poc.duckdb 2>/dev/null | grep "Run Time" | tail -5 | awk '{print $5*1000}' | med; }
fedq_time() { # $1=query $2=config ; prints "<ms> <rows>"
  out=$({ for i in 1 2 3 4 5; do echo "$1"; done; printf '\\q\n'; } \
        | /workspace/venv-fedq/bin/fedq -c "$2" 2>&1 | grep "rows in")
  ms=$(echo "$out" | awk '{print $(NF-1)}' | med)
  rows=$(echo "$out" | head -1 | awk '{print $1}')
  echo "$ms $rows"; }

H="%-4s %-32s %7s %7s %7s %10s %9s\n"
printf "$H" "Q" "what it tests" "rows" "stable" "main" "fedq-psyco" "fedq-adbc"
printf "$H" "--" "------------------------------" "-----" "------" "------" "--------" "--------"

row() { # $1=id $2=desc $3=duck_sql $4=fedq_sql
  s=$(duck_time "./bin/duckdb-stable" "LOAD postgres;" "$3")
  m=$(duck_time "pg-scanner-src/build/release/duckdb -unsigned" "LOAD '$EXT';" "$3")
  read pms prows < <(fedq_time "$4" fedq-poc-config.yaml)       # psycopg2 (plain SELECT)
  read ams arows < <(fedq_time "$4" fedq-poc-config-adbc.yaml)  # ADBC (binary COPY)
  printf "$H" "$1" "$2" "${prows:-?}" "$s" "$m" "$pms" "$ams"
}

row Q0 "count(*) on remote"               "SELECT count(*) FROM pg.files;" \
    "SELECT count(*) FROM pg.public.files;"
row Q1 "headline: 3-table cross GROUP BY" "SELECT a.day,c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;" \
    "SELECT a.day,c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;"
row Q2 "filter + projection (1 table)"    "SELECT id,category_id,size_bytes FROM pg.files WHERE category_id=42 AND size_bytes>5000000;" \
    "SELECT id,category_id,size_bytes FROM pg.public.files WHERE category_id=42 AND size_bytes>5000000;"
row Q3 "projection + filter id<100"       "SELECT category_id FROM pg.files WHERE id<100;" \
    "SELECT category_id FROM pg.public.files WHERE id<100;"
row Q4 "same-source join + aggregate"     "SELECT c.name,count(*) n FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
    "SELECT c.name,count(*) n FROM pg.public.files f JOIN pg.public.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
row Q5 "remote aggregate GROUP BY"        "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.files GROUP BY category_id ORDER BY n DESC LIMIT 5;" \
    "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.public.files GROUP BY category_id ORDER BY n DESC LIMIT 5;"
row Q6 "cross-source, non-selective"      "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
    "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;"
row Q7 "cross-source, selective (10 keys)" "SELECT f.category_id,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;" \
    "SELECT f.category_id,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;"
row Q8 "comparison filters inside join"   "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
    "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
row Q9 "LIMIT (no order)"                 "SELECT id,filename FROM pg.files LIMIT 10;" \
    "SELECT id,filename FROM pg.public.files LIMIT 10;"
row Q11 "ORDER BY + LIMIT"                "SELECT id,size_bytes FROM pg.files ORDER BY size_bytes DESC LIMIT 10;" \
    "SELECT id,size_bytes FROM pg.public.files ORDER BY size_bytes DESC LIMIT 10;"
row Q12 "literal IN-list"                 "SELECT id,category_id FROM pg.files WHERE id IN (10,250,999,40000,123456);" \
    "SELECT id,category_id FROM pg.public.files WHERE id IN (10,250,999,40000,123456);"
echo "(Q10 = EXPLAIN, not timed. median of 5 warm runs, ms.)"
