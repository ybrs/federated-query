#!/bin/bash
# capture-main.sh <sql-file> [label]
# Same as capture.sh but for the locally-built duckdb-postgres (main) binary:
# the postgres scanner is an unsigned loadable extension, so we load it by path
# and run with -unsigned. Rewrites the script's `LOAD postgres;` accordingly.
set -euo pipefail
ROOT=/workspace/federated-query/investigate-duckdb
SQLFILE="$1"; LABEL="${2:-query}"
BIN="$ROOT/pg-scanner-src/build/release/duckdb"
EXT="$ROOT/pg-scanner-src/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension"
PGLOG="/workspace/federated-query/postgres-server.log"

tmp=$(mktemp)
sed -E "s#^LOAD postgres;#LOAD '$EXT';#" "$SQLFILE" > "$tmp"

start=$(wc -l < "$PGLOG")
echo "######## DuckDB result ($LABEL) ########"
"$BIN" -unsigned "$ROOT/analytics-poc.duckdb" < "$tmp"
echo
echo "######## Statements PostgreSQL received ($LABEL) ########"
tail -n +"$((start + 1))" "$PGLOG" | grep -E "LOG:  (statement|execute)" \
  | sed -E 's/^.*LOG:  (statement|execute [^:]*): //'
echo "######## end ($LABEL) ########"
rm -f "$tmp"