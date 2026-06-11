#!/bin/bash
# Launches the bundled PostgreSQL in the background for the test harness.
#
# - Initializes a data directory on first run (superuser "postgres", trust auth
#   so local tests need no password handling).
# - Starts the server detached via pg_ctl and waits until it accepts connections.
# - Creates the test database if it does not yet exist.
#
# Connection defaults match the CI workflow and the pytest fixtures, and every
# value is overridable through the standard libpq environment variables.

set -euo pipefail

PG_DIR="./postgres-17"
BIN="${PG_DIR}/bin"
DATA_DIR="${PGDATA:-./postgres-data}"
PORT="${PGPORT:-5432}"
SUPERUSER="${PGUSER:-postgres}"
DB_NAME="${PGDATABASE:-test_db}"
LOG_FILE="${PG_LOG_FILE:-./postgres-server.log}"

if [ ! -x "${BIN}/postgres" ]; then
  echo "PostgreSQL binaries not found in ${PG_DIR}." >&2
  echo "Run scripts/download_postgresql.sh first." >&2
  exit 1
fi

if [ ! -d "${DATA_DIR}" ]; then
  echo "Initializing new database cluster at ${DATA_DIR} (superuser: ${SUPERUSER})"
  "${BIN}/initdb" -D "${DATA_DIR}" -U "${SUPERUSER}" --auth=trust >/dev/null
fi

echo "Starting PostgreSQL on port ${PORT} (background, log: ${LOG_FILE})"
"${BIN}/pg_ctl" -D "${DATA_DIR}" -l "${LOG_FILE}" \
  -o "-p ${PORT} -c log_statement=all -c log_min_duration_statement=0" \
  -w start

exists="$("${BIN}/psql" -h localhost -p "${PORT}" -U "${SUPERUSER}" -d postgres \
  -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'")"
if [ "${exists}" != "1" ]; then
  echo "Creating database ${DB_NAME}"
  "${BIN}/createdb" -h localhost -p "${PORT}" -U "${SUPERUSER}" "${DB_NAME}"
fi

echo "PostgreSQL ready: postgresql://${SUPERUSER}@localhost:${PORT}/${DB_NAME}"
echo "Stop it with scripts/stop-postgres.sh"
