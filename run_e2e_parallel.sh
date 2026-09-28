#!/bin/bash
# Run the Python suite as FIFTEEN processes, at most EIGHT at once, each on its
# own harness clone.
#
# The suite is one process today and takes most of the gate's wall clock. It does
# not have to be: the work divides cleanly along lines that already exist - the
# corpus, and one full corpus walk per plan variant - and those parts share
# nothing but read-only data.
#
# What each process gets of its own: a copy-on-write clone of the DuckDB files and
# the Parquet directories, a config beside them, and therefore its own engine
# state (the learned-statistics catalog and materialized-view store live beside
# the config). Cloning costs about 40ms and almost no disk because it is
# reflinked. PostgreSQL schemas are READ-ONLY here and stay shared - nothing
# writes them.
#
# THE CONCURRENCY BOUND EXISTS FOR THE PLANNING BUDGET, not for throughput. The
# budget is wall clock, and planning talks to the sources (statistics, planner
# EXPLAIN probes), so a machine saturated by its own test runner measures the
# runner: a query whose planning takes 2ms on an idle box was killed at 113ms
# under a full-width run of fifteen processes on these 12 cores. Eight leaves
# the sources and the coordinator headroom - the same reasoning as the Rust
# suite's --test-threads=4. Raising the width back is only safe if a full gate
# stays green repeatedly under the wider load.
#
# Every process writes its whole output to its own log under run-e2e-logs/, and
# reports where it has got to in <name>.progress. Read those to see a run in
# flight: the corpus shards print a character per test, but a plan variant is ONE
# test that walks the whole corpus, so without the progress file a running
# variant and a hung one look identical for twenty minutes.
#
#   ./progress.sh            what every process is doing right now
#
#   ./run_e2e_parallel.sh

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

VENV="${VIRTUAL_ENV:-}"
PYTHON="${VENV}/bin/python3"
HARNESS="${ROOT}/.test-harness"
CLONES="${ROOT}/.test-harness-parallel"
LOGS="${ROOT}/run-e2e-logs"

if [ -z "${VENV}" ]; then
    echo "The end-to-end tests need an active Python virtual environment." >&2
    echo "Activate it before committing." >&2
    exit 1
fi
if [ ! -x "${PYTHON}" ]; then
    echo "The active Python virtual environment has no interpreter at ${PYTHON}." >&2
    exit 1
fi
source "${ROOT}/scripts/test-postgres-env.sh"
ensure_test_postgres "${ROOT}"
if [ ! -f "${HARNESS}/config.yaml" ]; then
    echo "The test harness is missing at ${HARNESS}. Run ./build_test_harness.sh." >&2
    exit 1
fi

rm -rf "${CLONES}" "${LOGS}"
mkdir -p "${CLONES}" "${LOGS}"

# One clone per process. `cp --reflink=auto` is copy-on-write where the
# filesystem supports it and a plain copy where it does not, so this is correct
# either way and fast where it matters.
clone_harness() {
    local name="$1"
    local root="${CLONES}/${name}"
    printf '[e2e] preparing harness clone: %s\n' "${name}" >&2
    mkdir -p "${root}"
    cp --reflink=auto -r "${HARNESS}/duck" "${root}/duck"
    cp --reflink=auto -r "${HARNESS}/parquet" "${root}/parquet"
    sed -e "s#${HARNESS}/duck#${root}/duck#g" \
        -e "s#${HARNESS}/parquet#${root}/parquet#g" \
        "${HARNESS}/config.yaml" > "${root}/config.yaml"
    printf '[e2e] harness clone ready: %s\n' "${name}" >&2
    echo "${root}"
}

# The fifteen splits. Each is a name, a pytest target, a -k expression and, for the
# corpus, which shard of the matrix it owns. Together they partition the suite:
# every test runs in exactly one process, exactly once.
run_split() {
    local name="$1" target="$2" filter="$3" shard="${4:-}" shards="${5:-}"
    local root
    root="$(clone_harness "${name}")"
    local -a args=("${target}" -q --tb=short -x)
    [ -n "${filter}" ] && args+=(-k "${filter}")
    printf '[e2e] starting %s: %s\n' "${name}" "${target}"
    FEDQ_HARNESS_ROOT="${root}" FEDQ_SHARD="${shard}" FEDQ_SHARDS="${shards}" \
        FEDQ_PROGRESS_LOG="${LOGS}/${name}.progress" \
        "${PYTHON}" -m pytest "${args[@]}" > "${LOGS}/${name}.log" 2>&1
    local exit_code="$?"
    echo "${exit_code}" > "${LOGS}/${name}.exit"
    printf '[e2e] finished %s with exit code %s\n' "${name}" "${exit_code}"
}

# Print each worker's latest progress when its progress file changes.
report_progress() {
    local progress_file="$1"
    local worker_name="$2"
    local progress
    progress="$(cat "${progress_file}")"
    printf '[e2e] %s: %s\n' "${worker_name}" "${progress}"
}

# Show the end of a failed process log in the commit output.
show_failure_log() {
    local name="$1"
    local log_file="${LOGS}/${name}.log"
    if [ -f "${log_file}" ]; then
        printf 'Output from test process %s (final 80 lines):\n\n' "${name}"
        tail -n 80 "${log_file}"
    else
        printf 'Test process %s did not create a log file: %s\n' "${name}" "${log_file}"
    fi
}

STARTED=$(date +%s)

# At most this many suite processes run at once; the header explains why.
MAX_CONCURRENT=8

# Set once a split has been seen to fail, so no further split is launched.
ABORT=""

# How many splits the suite is divided into. The tally below refuses a run that
# launched fewer, so a split dropped from the list cannot pass as a whole run.
EXPECTED_SPLITS=15
echo "[e2e] preparing ${EXPECTED_SPLITS} full-suite workers; logs: ${LOGS}"

# The names of the splits this run actually started, in launch order. The tally
# is checked against this list rather than against the exit files that happen to
# exist: a split whose subshell dies without writing one leaves no other trace.
LAUNCHED=()

# Note any failed split in ABORT, so launching and monitoring both stop.
note_failures() {
    for exit_file in "${LOGS}"/*.exit; do
        [ -e "${exit_file}" ] || continue
        if [ "$(cat "${exit_file}")" != "0" ]; then
            ABORT="$(basename "${exit_file}" .exit)"
            return
        fi
    done
}

# Start one split once a slot is free, unless a split already failed - more
# work after a failure only delays reading it.
launch() {
    [ -n "${ABORT}" ] && return
    while [ "$(jobs -rp | wc -l)" -ge "${MAX_CONCURRENT}" ]; do
        note_failures
        [ -n "${ABORT}" ] && return
        sleep 1
    done
    LAUNCHED+=("$1")
    run_split "$@" &
}

# FIFTEEN processes, longest splits first so the pool packs tight. The engine
# opens PostgreSQL connections per query across six datasources, so this needs
# max_connections well above the default - the fixture sets 2000 in
# scripts/run-postgres.sh. At the default the run dies with "sorry, too many
# clients already", which reads like an engine failure and is not one.
#
# The corpus is split six ways and the widest variant (projection pushdown
# changes 95% of plans) three, because a variant walks the whole corpus itself
# and was the bottleneck until it could be divided. Every (case, distribution)
# belongs to exactly one process by position, in the corpus and in each variant
# alike, so the set partitions rather than samples.
launch proj_0 tests/e2e_federated/test_plan_variants.py "projection_pushdown_off" 0 3
launch proj_1 tests/e2e_federated/test_plan_variants.py "projection_pushdown_off" 1 3
launch proj_2 tests/e2e_federated/test_plan_variants.py "projection_pushdown_off" 2 3
launch learned tests/e2e_federated/test_plan_variants.py \
    "learned_statistics_on or accelerator_off" 0 1
launch reorder_0 tests/e2e_federated/test_plan_variants.py "join_reordering_off" 0 2
launch reorder_1 tests/e2e_federated/test_plan_variants.py "join_reordering_off" 1 2
launch greedy tests/e2e_federated/test_plan_variants.py "join_order_greedy" 0 1
launch corpus_0 tests/e2e_federated/test_corpus.py "" 0 6
launch corpus_1 tests/e2e_federated/test_corpus.py "" 1 6
launch corpus_2 tests/e2e_federated/test_corpus.py "" 2 6
launch corpus_3 tests/e2e_federated/test_corpus.py "" 3 6
launch corpus_4 tests/e2e_federated/test_corpus.py "" 4 6
launch corpus_5 tests/e2e_federated/test_corpus.py "" 5 6
launch pred tests/e2e_federated/test_plan_variants.py "predicate_pushdown_off" 0 1
launch rest tests/ \
    "not test_corpus and not test_every_plan_this_variant_reaches"

# STOP AT THE FIRST FAILURE. A failing suite has said what it needed to say; the
# remaining twenty minutes only delay reading it. Each process runs pytest with
# -x so it stops at its own first failure, and the moment any process exits
# non-zero the rest are killed. A green run is unaffected - every test still runs.
FAILED_FIRST="${ABORT}"
declare -A REPORTED_PROGRESS=()
LAST_HEARTBEAT="${STARTED}"
while :; do
    for progress_file in "${LOGS}"/*.progress; do
        [ -f "${progress_file}" ] || continue
        worker_name="$(basename "${progress_file}" .progress)"
        progress="$(cat "${progress_file}")"
        if [ "${REPORTED_PROGRESS[${worker_name}]-}" != "${progress}" ]; then
            REPORTED_PROGRESS["${worker_name}"]="${progress}"
            report_progress "${progress_file}" "${worker_name}"
        fi
    done
    for exit_file in "${LOGS}"/*.exit; do
        [ -e "${exit_file}" ] || continue
        if [ "$(cat "${exit_file}")" != "0" ]; then
            FAILED_FIRST="$(basename "${exit_file}" .exit)"
            break
        fi
    done
    [ -n "${FAILED_FIRST}" ] && break
    jobs %% >/dev/null 2>&1 || break
    pgrep -P $$ >/dev/null 2>&1 || break
    NOW="$(date +%s)"
    if [ $((NOW - LAST_HEARTBEAT)) -ge 30 ]; then
        RUNNING=0
        for name in "${LAUNCHED[@]}"; do
            [ -f "${LOGS}/${name}.exit" ] || RUNNING=$((RUNNING + 1))
        done
        printf '[e2e] still running: %s/%s workers; elapsed %ss\n' \
            "${RUNNING}" "${EXPECTED_SPLITS}" "$((NOW - STARTED))"
        LAST_HEARTBEAT="${NOW}"
    fi
    sleep 2
done

if [ -n "${FAILED_FIRST}" ]; then
    echo "A test process failed. Stopping the remaining processes."
    pkill -P $$ >/dev/null 2>&1
    pkill -9 -f "FEDQ_HARNESS_ROOT=${CLONES}" >/dev/null 2>&1
fi
wait 2>/dev/null

if [ -z "${FAILED_FIRST}" ]; then
    for exit_file in "${LOGS}"/*.exit; do
        [ -e "${exit_file}" ] || continue
        if [ "$(cat "${exit_file}")" != "0" ]; then
            FAILED_FIRST="$(basename "${exit_file}" .exit)"
            break
        fi
    done
fi

if [ -n "${FAILED_FIRST}" ]; then
    printf '\nThe first failing test process was %s.\n' "${FAILED_FIRST}"
    show_failure_log "${FAILED_FIRST}"
    printf '\nThe run stopped after this failure. Full logs are in %s.\n' "${LOGS}"
    exit 1
fi

ELAPSED=$(( $(date +%s) - STARTED ))
echo "End-to-end tests completed in ${ELAPSED} seconds across $(ls "${LOGS}"/*.exit | wc -l) processes."

# EVERY LAUNCHED SPLIT MUST HAVE REPORTED. run_split writes its exit file only
# when pytest RETURNS, so a subshell killed outright - the OOM killer taking
# bash instead of pytest, a shell crash, an external kill - leaves none at all.
# Tallying the exit files that exist would then read the survivors' zeros and
# exit 0 with a fifteenth of the suite never run, which is exactly a narrowed
# run printing the same "passed" as a whole one.
#
# On the fail-fast path the surviving splits were killed on purpose, so their
# missing exit files are expected and the run fails on the failure itself.
if [ -z "${FAILED_FIRST}" ]; then
    if [ "${#LAUNCHED[@]}" -ne "${EXPECTED_SPLITS}" ]; then
        echo "The end-to-end suite did not start all ${EXPECTED_SPLITS} test processes." >&2
        exit 1
    fi
    UNREPORTED=""
    for name in "${LAUNCHED[@]}"; do
        [ -f "${LOGS}/${name}.exit" ] || UNREPORTED="${UNREPORTED} ${name}"
    done
    if [ -n "${UNREPORTED}" ]; then
        echo "These test processes ended without reporting a result:${UNREPORTED}." >&2
        echo "The end-to-end suite did not finish." >&2
        first_unreported="${UNREPORTED# }"
        show_failure_log "${first_unreported%% *}"
        exit 1
    fi
fi

for exit_file in "${LOGS}"/*.exit; do
    name="$(basename "${exit_file}" .exit)"
    code="$(cat "${exit_file}")"
    tail_line="$(grep -aE '[0-9]+ (passed|failed)' "${LOGS}/${name}.log" | tail -1)"
    if [ "${code}" = "0" ]; then
        printf '%s: passed (%s)\n' "${name}" "${tail_line}"
    fi
done
