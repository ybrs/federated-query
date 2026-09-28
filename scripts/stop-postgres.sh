#!/bin/bash
# Stops the bundled PostgreSQL instance started by run-postgres.sh.
# The data directory is left intact so a later start reuses the same cluster.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
PG_DIR="${ROOT}/postgres-17"
DATA_DIR="${PGDATA:-${ROOT}/postgres-data}"

"${PG_DIR}/bin/pg_ctl" -D "${DATA_DIR}" -m fast stop
rm -f "${ROOT}/.fedq-test-postgres-port"
echo "PostgreSQL stopped."
