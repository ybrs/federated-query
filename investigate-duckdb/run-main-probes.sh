#!/bin/bash
# Wait for the duckdb-postgres (main) build to finish, verify the statically
# linked postgres scanner loads, then run every probe through that binary.
set -uo pipefail
cd /workspace/federated-query/investigate-duckdb
BIN=pg-scanner-src/build/release/duckdb

while pgrep -f "ninja -j" >/dev/null || pgrep -f "make release" >/dev/null; do sleep 10; done
if ! [ -x "$BIN" ]; then echo "BUILD FAILED — no binary at $BIN"; tail -20 build-pgscanner.log; exit 1; fi

echo "=== main (duckdb-postgres) binary ready ==="; "$BIN" --version
echo "=== postgres scanner load check ==="
"$BIN" -c "LOAD postgres; SELECT 'pg scanner (main) linked' AS ok;" 2>&1 | tail -3

mkdir -p results
for q in q0_count q1_headline q2_filter q3_projection q4_pgjoin q5_agg \
         q6_xsource_full q7_xsource_selective q8_join_cmpfilter q9_limit; do
  VERBOSE=1 ./capture.sh "$BIN" queries/$q.sql "${q}-main" > results/${q}-main.txt 2>&1
  echo "ran $q on main"
done
"$BIN" analytics-poc.duckdb < queries/q10_explain.sql > results/q10_explain-main.txt 2>&1
echo "=== ALL MAIN PROBES DONE ==="