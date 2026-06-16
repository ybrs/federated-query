#!/bin/bash
# Dump the FULL verbatim SQL each engine sends to PostgreSQL, per query.
# Output is markdown-ready. PG must be running on :5432 with duckpoc.
set -uo pipefail
cd "$(dirname "$0")"
PGLOG="/workspace/federated-query/postgres-server.log"
EXT="$PWD/pg-scanner-src/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
ATTACH_D="ATTACH 'dbname=duckpoc host=localhost port=5432 user=postgres' AS pg (TYPE postgres, READ_ONLY);"

# meaningful statements only: touch files/categories, drop schema-probe + tx noise
clean() { grep -E "files|categories" | grep -viE "information_schema|pg_catalog|LIMIT 0\)? AS|\) AS q LIMIT 0|^BEGIN|^ROLLBACK|^COMMIT|DISCARD|SET TRANSACTION|pg_export|to_regclass"; }

dump_fedq() { # $1=sql
  s=$(wc -l < "$PGLOG")
  { echo "$1"; printf '\\q\n'; } | /workspace/venv-fedq/bin/fedq -c fedq-poc-config.yaml >/dev/null 2>&1
  tail -n +"$((s+1))" "$PGLOG" | grep -E "LOG:  statement:" | sed -E 's/^.*statement: //' | clean
}
dump_duck() { # $1=binary(+flags) $2=load $3=sql
  s=$(wc -l < "$PGLOG")
  { echo "$2"; echo "$ATTACH_D"; echo "$3"; } | $1 analytics-poc.duckdb >/dev/null 2>&1
  tail -n +"$((s+1))" "$PGLOG" | grep -E "LOG:  (statement|execute)" | sed -E 's/^.*LOG:  (statement|execute [^:]*): //' | clean
}

q() { # $1=id $2=desc $3=input_sql_display $4=duck_sql $5=fedq_sql
  echo "### $1 — $2"
  echo
  echo "**Query:**"
  echo '```sql'
  echo "$3"
  echo '```'
  echo "**fedq → PostgreSQL:**"
  echo '```sql'
  dump_fedq "$5"
  echo '```'
  echo "**DuckDB (stable) → PostgreSQL:**"
  echo '```sql'
  dump_duck "./bin/duckdb-stable" "LOAD postgres;" "$4"
  echo '```'
  if [ "${6:-}" = "main" ]; then
    echo "**DuckDB (main) → PostgreSQL:**"
    echo '```sql'
    dump_duck "pg-scanner-src/build/release/duckdb -unsigned" "LOAD '$EXT';" "$4"
    echo '```'
  fi
  echo
}

q Q0 "count(*) on remote" \
  "SELECT count(*) FROM files;" \
  "SELECT count(*) FROM pg.files;" \
  "SELECT count(*) FROM pg.public.files;"
q Q1 "headline: 3-table cross-source GROUP BY" \
  "SELECT a.day, c.name, count(*) AS n
FROM access_logs a
JOIN files f ON f.id = a.file_id
JOIN categories c ON c.id = f.category_id
WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07'
GROUP BY a.day, c.name;" \
  "SELECT a.day,c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;" \
  "SELECT a.day,c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.day BETWEEN DATE '2026-02-01' AND DATE '2026-02-07' GROUP BY a.day,c.name;"
q Q2 "filter + projection (1 table)" \
  "SELECT id, category_id, size_bytes FROM files
WHERE category_id = 42 AND size_bytes > 5000000;" \
  "SELECT id,category_id,size_bytes FROM pg.files WHERE category_id=42 AND size_bytes>5000000;" \
  "SELECT id,category_id,size_bytes FROM pg.public.files WHERE category_id=42 AND size_bytes>5000000;"
q Q3 "projection + filter id<100" \
  "SELECT category_id FROM files WHERE id < 100;" \
  "SELECT category_id FROM pg.files WHERE id<100;" \
  "SELECT category_id FROM pg.public.files WHERE id<100;"
q Q4 "same-source join + aggregate" \
  "SELECT c.name, count(*) AS n
FROM files f JOIN categories c ON c.id = f.category_id
WHERE c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM pg.files f JOIN pg.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM pg.public.files f JOIN pg.public.categories c ON c.id=f.category_id WHERE c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
q Q5 "remote aggregate GROUP BY" \
  "SELECT category_id, count(*) AS n, sum(size_bytes) AS t
FROM files GROUP BY category_id ORDER BY n DESC LIMIT 5;" \
  "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.files GROUP BY category_id ORDER BY n DESC LIMIT 5;" \
  "SELECT category_id,count(*) n,sum(size_bytes) t FROM pg.public.files GROUP BY category_id ORDER BY n DESC LIMIT 5;"
q Q6 "cross-source, non-selective" \
  "SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id
WHERE a.action = 'download'
GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE a.action='download' GROUP BY c.name ORDER BY n DESC LIMIT 5;"
q Q7 "cross-source, selective (10 keys)" \
  "SELECT f.category_id, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id
WHERE a.day = DATE '2026-02-01' AND a.user_id = 7
GROUP BY f.category_id ORDER BY n DESC LIMIT 5;" \
  "SELECT f.category_id,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;" \
  "SELECT f.category_id,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id WHERE a.day=DATE '2026-02-01' AND a.user_id=7 GROUP BY f.category_id ORDER BY n DESC LIMIT 5;"
q Q8 "comparison filters inside cross-source join" \
  "SELECT c.name, count(*) AS n
FROM access_logs a JOIN files f ON f.id = a.file_id JOIN categories c ON c.id = f.category_id
WHERE f.size_bytes > 9000000 AND c.is_active = true
GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM access_logs a JOIN pg.files f ON f.id=a.file_id JOIN pg.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;" \
  "SELECT c.name,count(*) n FROM analytics.main.access_logs a JOIN pg.public.files f ON f.id=a.file_id JOIN pg.public.categories c ON c.id=f.category_id WHERE f.size_bytes>9000000 AND c.is_active=true GROUP BY c.name ORDER BY n DESC LIMIT 5;"
q Q9 "LIMIT (no order)" \
  "SELECT id, filename FROM files LIMIT 10;" \
  "SELECT id,filename FROM pg.files LIMIT 10;" \
  "SELECT id,filename FROM pg.public.files LIMIT 10;"
q Q11 "ORDER BY + LIMIT" \
  "SELECT id, size_bytes FROM files ORDER BY size_bytes DESC LIMIT 10;" \
  "SELECT id,size_bytes FROM pg.files ORDER BY size_bytes DESC LIMIT 10;" \
  "SELECT id,size_bytes FROM pg.public.files ORDER BY size_bytes DESC LIMIT 10;" main
q Q12 "literal IN-list" \
  "SELECT id, category_id FROM files WHERE id IN (10, 250, 999, 40000, 123456);" \
  "SELECT id,category_id FROM pg.files WHERE id IN (10,250,999,40000,123456);" \
  "SELECT id,category_id FROM pg.public.files WHERE id IN (10,250,999,40000,123456);"
