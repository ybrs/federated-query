#!/bin/bash
# Stops the bundled PostgreSQL instance started by run-postgres.sh.
# The data directory is left intact so a later start reuses the same cluster.

set -euo pipefail

PG_DIR="./postgres-17"
DATA_DIR="${PGDATA:-./postgres-data}"

"${PG_DIR}/bin/pg_ctl" -D "${DATA_DIR}" -m fast stop
echo "PostgreSQL stopped."
