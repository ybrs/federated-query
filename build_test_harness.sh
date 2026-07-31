#!/bin/bash
# Build every datasource the test suite reads. THE ONLY THING THAT CREATES TEST DATA.
#
# The tests do not seed. They open what this script built, and if something is
# missing they raise naming it. That split is the point: what the tests run
# against is decided here, once, visibly, instead of being conjured per test by
# code nobody reads and thrown away again before anyone can look at it.
#
# Destructive and complete: it removes .test-harness/ and drops every fed_%
# schema in the test database, then creates every datasource the corpus needs.
# Re-run it whenever the corpus or the table library changes.
#
# Prerequisites: ./venv (make venv) and a running PostgreSQL (make pg-start).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

PYTHON="${ROOT}/venv/bin/python3"
PG_ISREADY="${ROOT}/postgres-17/bin/pg_isready"
PGPORT="${PGPORT:-5432}"

if [ ! -x "${PYTHON}" ]; then
    echo "ERROR: no virtualenv at ${ROOT}/venv" >&2
    echo "  fix: make venv" >&2
    exit 1
fi

if [ ! -x "${PG_ISREADY}" ]; then
    echo "ERROR: PostgreSQL binaries not found in ${ROOT}/postgres-17" >&2
    echo "  fix: make download_postgres" >&2
    exit 1
fi

if ! "${PG_ISREADY}" -h localhost -p "${PGPORT}" >/dev/null 2>&1; then
    echo "PostgreSQL is not running; starting it"
    make pg-start
fi

"${PYTHON}" -m tests.e2e_federated.build_harness

echo
echo "verifying what was just built"
"${PYTHON}" -m tests.e2e_federated.verify_harness
