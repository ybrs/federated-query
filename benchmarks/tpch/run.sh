#!/usr/bin/env bash
#
# End-to-end TPC-H benchmark: generate the dataset and queries if missing, then
# run every query through the federated engine and compare against DuckDB.
#
# Usage:
#   ./run.sh                 # scale factor 0.01 (default)
#   ./run.sh 0.1             # a different scale factor
#   ./run.sh 0.01 --verbose  # forward extra flags to run.py (e.g. --only 1,6)
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/workspace/venv-fedq/bin/python}"
SCALE_FACTOR="${1:-0.01}"
DB="$HERE/data/tpch_sf${SCALE_FACTOR}.duckdb"

if [ ! -f "$DB" ]; then
    echo "Generating TPC-H data at scale factor ${SCALE_FACTOR} ..."
    "$PYTHON" "$HERE/generate.py" --scale-factor "$SCALE_FACTOR"
fi

echo "Running TPC-H queries through the engine ..."
"$PYTHON" "$HERE/run.py" --scale-factor "$SCALE_FACTOR" "${@:2}"
