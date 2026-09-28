#!/bin/bash
# Launches the bundled PostgreSQL in the background for the test harness.
#
# - Initializes a data directory on first run (superuser "postgres", trust auth
#   so local tests need no password handling).
# - Starts the server detached via pg_ctl and waits until it accepts connections.
# - Creates the test database if it does not yet exist.
#
# The test server uses an available port and stores it for the test runners.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"
PG_DIR="${ROOT}/postgres-17"
BIN="${PG_DIR}/bin"
DATA_DIR="${PGDATA:-${ROOT}/postgres-data}"
PORT_FILE="${ROOT}/.fedq-test-postgres-port"
PYTHON="${VIRTUAL_ENV:-}/bin/python3"
SUPERUSER="${PGUSER:-postgres}"
DB_NAME="${PGDATABASE:-test_db}"
LOG_FILE="${PG_LOG_FILE:-${ROOT}/postgres-server.log}"

# Return whether a local TCP port can be bound.
port_is_free() {
  "${PYTHON}" -c 'import socket, sys; sock = socket.socket(); sock.bind(("127.0.0.1", int(sys.argv[1]))); sock.close()' "$1" >/dev/null 2>&1
}

# Ask the operating system for an available local TCP port.
free_port() {
  "${PYTHON}" -c 'import socket; sock = socket.socket(); sock.bind(("127.0.0.1", 0)); print(sock.getsockname()[1]); sock.close()'
}

# Start the bundled server on the selected port and wait for connections.
start_postgres() {
  local port="$1"
  echo "Starting the test PostgreSQL server on port ${port}."
  "${BIN}/pg_ctl" -D "${DATA_DIR}" -l "${LOG_FILE}" \
    -o "-p ${port} -c max_connections=2000 -c log_statement=all -c log_min_duration_statement=0" \
    -w start
}

if [ -z "${VIRTUAL_ENV:-}" ] || [ ! -x "${PYTHON}" ]; then
  echo "Activate the Python virtual environment before starting test PostgreSQL." >&2
  exit 1
fi
if [ ! -x "${BIN}/postgres" ]; then
  echo "PostgreSQL binaries are missing from ${PG_DIR}. Run make download_postgres." >&2
  exit 1
fi
if [ ! -d "${DATA_DIR}" ]; then
  echo "Initializing the test database cluster at ${DATA_DIR}."
  "${BIN}/initdb" -D "${DATA_DIR}" -U "${SUPERUSER}" --auth=trust >/dev/null
fi

if "${BIN}/pg_ctl" -D "${DATA_DIR}" status >/dev/null 2>&1; then
  if [ -s "${PORT_FILE}" ]; then
    PORT="$(cat "${PORT_FILE}")"
  elif [ -n "${PGPORT:-}" ]; then
    PORT="${PGPORT}"
  else
    PORT="$("${PYTHON}" -c 'import shlex, sys; args = shlex.split(open(sys.argv[1]).read()); print(args[args.index("-p") + 1] if "-p" in args else "")' "${DATA_DIR}/postmaster.opts")"
    if [ -z "${PORT}" ]; then
      PORT="$("${BIN}/postgres" -D "${DATA_DIR}" -C port)"
    fi
  fi
else
  PORT="${PGPORT:-${POSTGRES_PORT:-}}"
  if [ -z "${PORT}" ]; then
    PORT="$(free_port)"
  elif ! port_is_free "${PORT}"; then
    PORT="$(free_port)"
  fi
  start_postgres "${PORT}"
fi

printf '%s\n' "${PORT}" > "${PORT_FILE}"
exists="$("${BIN}/psql" -h localhost -p "${PORT}" -U "${SUPERUSER}" -d postgres \
  -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'")"
if [ "${exists}" != "1" ]; then
  echo "Creating the test database ${DB_NAME}."
  "${BIN}/createdb" -h localhost -p "${PORT}" -U "${SUPERUSER}" "${DB_NAME}"
fi

echo "Test PostgreSQL is ready on port ${PORT}."
