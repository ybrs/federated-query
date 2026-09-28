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
# Prerequisite: an active Python virtualenv. The script starts test PostgreSQL.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

VENV="${VIRTUAL_ENV:-}"
PYTHON="${VENV}/bin/python3"
if [ -z "${VENV}" ]; then
    echo "ERROR: no active Python virtualenv" >&2
    echo "  fix: activate a virtualenv before running ./build_test_harness.sh" >&2
    exit 1
fi
if [ ! -x "${PYTHON}" ]; then
    echo "ERROR: no interpreter at ${PYTHON}" >&2
    exit 1
fi

source "${ROOT}/scripts/test-postgres-env.sh"
ensure_test_postgres "${ROOT}"

"${PYTHON}" -m tests.e2e_federated.build_harness

echo
echo "verifying what was just built"
"${PYTHON}" -m tests.e2e_federated.verify_harness
