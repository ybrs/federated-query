#!/bin/bash
# capture.sh <duckdb-binary> <sql-file> [label]
#
# Runs a SQL script through the given DuckDB binary, then extracts exactly the
# statements PostgreSQL received while that script ran (by bracketing the shared
# postgres-server.log, which is started with log_statement=all).
#
# Output: the DuckDB result, followed by the PG-side statements (deduped of the
# catalog-introspection noise unless VERBOSE=1).
set -euo pipefail

DUCKDB="$1"
SQLFILE="$2"
LABEL="${3:-query}"
PGLOG="/workspace/federated-query/postgres-server.log"

start=$(wc -l < "$PGLOG")

echo "######## DuckDB result ($LABEL) ########"
"$DUCKDB" /workspace/federated-query/investigate-duckdb/analytics-poc.duckdb < "$SQLFILE"

echo
echo "######## Statements PostgreSQL received ($LABEL) ########"
# New log lines only; keep statement/execute lines and their continuations.
tail -n +"$((start + 1))" "$PGLOG" \
  | grep -E "LOG:  (statement|execute)" \
  | sed -E 's/^.*LOG:  (statement|execute [^:]*): //' \
  > /tmp/_pgstmts.txt

if [ "${VERBOSE:-0}" = "1" ]; then
  cat /tmp/_pgstmts.txt
else
  # Drop the catalog-introspection chatter; keep queries that touch our tables.
  grep -viE "pg_catalog|information_schema|pg_namespace|pg_class|pg_attribute|pg_type|pg_proc|SET |BEGIN|COMMIT|ROLLBACK|^SELECT 1$|current_schema|pg_settings|version\(\)|pg_database" /tmp/_pgstmts.txt \
    || echo "(no table-level statements captured — run with VERBOSE=1 to see all)"
fi
echo "######## end ($LABEL) ########"
