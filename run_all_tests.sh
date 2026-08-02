#!/bin/bash
# Run EVERY check in this repository, in one command, with no way to run less.
#
# There is one suite. There is no dev suite and no production suite, no quick
# mode, no smoke mode, and no flag that narrows what runs: a run that covers less
# than everything reports the same "passed" as a run that covered it all, which
# is how a regression ships. Every stage below is mandatory and every stage is
# loud - the first failure stops the run and names itself.
#
# Prerequisites, each checked and reported rather than assumed:
#   - ./venv           the one virtualenv (make venv)
#   - .duckdb-lib      the prebuilt libduckdb the workspace links (make duckdb-lib)
#   - PostgreSQL       started here if it is not already accepting connections
#   - .test-harness    the datasources the tests read (./build_test_harness.sh)
#
# This script NEVER creates test data. It verifies the data is there and dies
# naming what is missing; building it is build_test_harness.sh's single job.
#
# Two things are deliberately outside this script, and neither is a check that
# gets skipped:
#   - Benchmarks. Perf numbers come from benchmarks/perf_compare only, on a
#     machine that is not simultaneously running a test suite.
#   - The semantic comment gate. It judges the comments in a COMMIT, and the
#     pre-commit hook runs it on every commit; it reads staged files, so calling
#     it here with nothing staged would judge nothing and print a pass.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

# EVERY line this run produces goes to run_all_tests.log, in full.
#
# READ THE LOG. Do not pipe this script through `tail`, `head` or a grep that
# keeps only the summary. A run takes about twenty minutes, and truncating its
# output means the failure detail is thrown away at the exact moment it is
# produced - so the only way to see what broke is to run it again, twenty
# minutes later, and hope to keep the right lines that time. That has cost this
# project multiple hours. The log is written for you; open it.
#
# The log is overwritten each run: it describes the run that just happened, not
# a history. Copy it elsewhere if you need to compare two runs.
RUN_LOG="${ROOT}/run_all_tests.log"
exec > >(tee "${RUN_LOG}") 2>&1
echo "full output of this run: ${RUN_LOG}"

VENV="${ROOT}/venv"
PYTHON="${VENV}/bin/python3"
PG_ISREADY="${ROOT}/postgres-17/bin/pg_isready"
PGPORT="${PGPORT:-5432}"

STAGE_NUMBER=0

# Announce a stage on its own line so the failing one is obvious in the output.
announce() {
    STAGE_NUMBER=$((STAGE_NUMBER + 1))
    printf '\n===== [%d] %s =====\n' "${STAGE_NUMBER}" "$1"
}

# Report which stage failed, then exit non-zero. Wired to ERR so no failure can
# scroll past as a wall of unrelated output.
report_failure() {
    printf '\n***** FAILED at stage [%d] *****\n' "${STAGE_NUMBER}" >&2
    exit 1
}
trap report_failure ERR

# Fail with the fix command when the one virtualenv is missing or incomplete.
require_venv() {
    if [ ! -x "${PYTHON}" ]; then
        echo "ERROR: no virtualenv at ${VENV}" >&2
        echo "  fix: make venv" >&2
        exit 1
    fi
    if ! "${PYTHON}" -c "import pytest, pyarrow, sqlglot, flake8" 2>/dev/null; then
        echo "ERROR: ${VENV} is missing test dependencies" >&2
        echo "  fix: make install" >&2
        exit 1
    fi
}

# Start PostgreSQL unless it is already accepting connections. The suite requires
# it: there is no toggle that runs the tests without it.
require_postgres() {
    if [ ! -x "${PG_ISREADY}" ]; then
        echo "ERROR: PostgreSQL binaries not found in ${ROOT}/postgres-17" >&2
        echo "  fix: make download_postgres" >&2
        exit 1
    fi
    if "${PG_ISREADY}" -h localhost -p "${PGPORT}" >/dev/null 2>&1; then
        echo "PostgreSQL already accepting connections on port ${PGPORT}"
        return
    fi
    echo "PostgreSQL is not running; starting it"
    make pg-start
}

announce "Preflight: virtualenv, libduckdb, PostgreSQL"
require_venv
make duckdb-lib
require_postgres
echo "python: $("${PYTHON}" -V)"

# Release, not debug: the ACL e2e tests drive the release fedq-server binary and
# the Python suite loads the release extension through tests/fedq.so.
announce "Build (release, whole workspace)"
make build

announce "Rust format"
cargo fmt --all --check

announce "Rust clippy (all targets, warnings denied)"
cargo clippy --workspace --all-targets

announce "Linters (codepoints, flake8, fq-lint)"
make lint

announce "Rust tests (whole workspace)"
# FOUR test threads, not one per core. The planning budget is wall clock, so a
# machine saturated by its own test runner measures the runner, not the planner.
# Four leaves headroom on a 12-core box; a budget kill under this load is a
# statement about the planner and must be fixed there.
cargo test --workspace -- --test-threads=4

# The suite READS its datasources; it never creates them. If the data the tests
# are about to open is missing or short, this dies naming every source rather
# than letting a test seed its own and assert against data nobody can audit.
announce "Verify the test harness data is present"
"${PYTHON}" -m tests.e2e_federated.verify_harness

# The whole repository, collected from the root: pytest.ini's norecursedirs is
# the only boundary and it excludes build output and vendored sibling repos, not
# code of ours.
announce "Python tests (whole repository, e2e included)"
"${PYTHON}" -m pytest -q

printf '\n===== ALL %d STAGES PASSED =====\n' "${STAGE_NUMBER}"
