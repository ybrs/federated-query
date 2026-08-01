#!/bin/bash
# Run every corpus variation in its own process against its own fresh engine.
#
# The pytest suite runs all 86,520 variations in one process, so every result is
# a result GIVEN everything that ran before it. This runner answers the other
# question - what does the engine do with this query on its own - by giving each
# variation a process and an engine that has executed nothing else.
#
# Every outcome is pass or fail. A query the planning budget kills is a failure
# like any other: the budget is wall clock and the engine is expected to plan
# inside it from cold.
#
# Results are appended to the output file as they happen, one JSON object per
# line, and the run resumes from that file, so stopping it costs nothing.
#
#   ./run_isolated_tests.sh [output.jsonl] [workers]
#
# It takes hours. That is the price of a number that means something. Workers
# default to 1 because parallel workers oversubscribe the machine - the engine
# builds a tokio runtime per query with one thread per core - and the planning
# budget is wall clock, so a parallel run can kill plans a sequential one would
# not. Those kills are recorded as the failures they are, with their error text,
# so a run that produces them says so instead of hiding it.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"

PYTHON="${ROOT}/venv/bin/python3"
OUTPUT="${1:-${ROOT}/isolated-results.jsonl}"
WORKERS="${2:-1}"

if [ ! -x "${PYTHON}" ]; then
    echo "ERROR: no virtualenv at ${ROOT}/venv" >&2
    echo "  fix: make venv" >&2
    exit 1
fi

"${PYTHON}" -m tests.e2e_federated.verify_harness

exec "${PYTHON}" -m tests.e2e_federated.run_isolated "${OUTPUT}" "${WORKERS}"
