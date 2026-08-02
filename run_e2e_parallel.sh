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

PYTHON="${ROOT}/venv/bin/python3"
HARNESS="${ROOT}/.test-harness"
CLONES="${ROOT}/.test-harness-parallel"
LOGS="${ROOT}/run-e2e-logs"

if [ ! -x "${PYTHON}" ]; then
    echo "ERROR: no virtualenv at ${ROOT}/venv (fix: make venv)" >&2
    exit 1
fi
if [ ! -f "${HARNESS}/config.yaml" ]; then
    echo "ERROR: no harness at ${HARNESS} (fix: ./build_test_harness.sh)" >&2
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
    mkdir -p "${root}"
    cp --reflink=auto -r "${HARNESS}/duck" "${root}/duck"
    cp --reflink=auto -r "${HARNESS}/parquet" "${root}/parquet"
    sed -e "s#${HARNESS}/duck#${root}/duck#g" \
        -e "s#${HARNESS}/parquet#${root}/parquet#g" \
        "${HARNESS}/config.yaml" > "${root}/config.yaml"
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
    FEDQ_HARNESS_ROOT="${root}" FEDQ_SHARD="${shard}" FEDQ_SHARDS="${shards}" \
        FEDQ_PROGRESS_LOG="${LOGS}/${name}.progress" \
        "${PYTHON}" -m pytest "${args[@]}" > "${LOGS}/${name}.log" 2>&1
    echo "$?" > "${LOGS}/${name}.exit"
}

STARTED=$(date +%s)

# At most this many suite processes run at once; the header explains why.
MAX_CONCURRENT=8

# Set once a split has been seen to fail, so no further split is launched.
ABORT=""

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
while :; do
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
    sleep 2
done

if [ -n "${FAILED_FIRST}" ]; then
    echo "FAILED FAST: ${FAILED_FIRST} failed; stopping the other processes"
    pkill -P $$ >/dev/null 2>&1
    pkill -9 -f "FEDQ_HARNESS_ROOT=${CLONES}" >/dev/null 2>&1
fi
wait 2>/dev/null

ELAPSED=$(( $(date +%s) - STARTED ))
echo "e2e (parallel): ${ELAPSED}s across $(ls "${LOGS}"/*.exit | wc -l) processes"
echo "logs: ${LOGS}/"

FAILED=0
for exit_file in "${LOGS}"/*.exit; do
    name="$(basename "${exit_file}" .exit)"
    code="$(cat "${exit_file}")"
    tail_line="$(grep -aE '[0-9]+ (passed|failed)' "${LOGS}/${name}.log" | tail -1)"
    echo "  ${name}: exit ${code}  ${tail_line}"
    [ "${code}" != "0" ] && FAILED=1
done

if [ "${FAILED}" != "0" ]; then
    echo "FAILED - read the logs in ${LOGS}/" >&2
    exit 1
fi
