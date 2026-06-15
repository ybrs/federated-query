#!/bin/bash
# capture-fedq.sh <sql-file> [label]
# Runs a SQL script through the fedq REPL (POC config) and prints the statements
# PostgreSQL received while it ran, by bracketing the shared postgres log.
set -euo pipefail
ROOT=/workspace/federated-query/investigate-duckdb
SQLFILE="$1"; LABEL="${2:-query}"
PGLOG="/workspace/federated-query/postgres-server.log"

start=$(wc -l < "$PGLOG")
echo "######## fedq result ($LABEL) ########"
{ cat "$SQLFILE"; printf '\n\\q\n'; } | /workspace/venv-fedq/bin/fedq -c "$ROOT/fedq-poc-config.yaml" 2>&1 \
  | grep -vE "not a terminal|Type SQL|Use .catalog|^fedq>\s*$"
echo
echo "######## Statements PostgreSQL received ($LABEL) ########"
tail -n +"$((start + 1))" "$PGLOG" | grep -E "LOG:  (statement|execute)" \
  | sed -E 's/^.*LOG:  (statement|execute [^:]*): //'
echo "######## end ($LABEL) ########"