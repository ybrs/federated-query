#!/usr/bin/env bash
#
# End-to-end TPC-DS benchmark: generate the dataset and queries if missing, then
# run every query through the federated engine and compare against DuckDB.
#
# Usage:
#   ./run.sh                 # scale factor 1 (default)
#   ./run.sh 0.1             # a different scale factor
#   ./run.sh 1 --only 1,6    # forward extra flags to run.py
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-/workspace/venv-fedq/bin/python}"
SCALE_FACTOR="${1:-1}"
DB="$HERE/data/tpcds_sf${SCALE_FACTOR}.duckdb"

if [ ! -f "$DB" ]; then
    echo "Generating TPC-DS data at scale factor ${SCALE_FACTOR} ..."
    "$PYTHON" "$HERE/generate.py" --scale-factor "$SCALE_FACTOR"
fi

echo "Running TPC-DS queries through the engine ..."
"$PYTHON" "$HERE/run.py" --scale-factor "$SCALE_FACTOR" "${@:2}"
