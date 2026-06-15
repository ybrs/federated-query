#!/bin/bash
# Rebuild duckdb-postgres (main) after the oauth stub, then run every probe.
set -uo pipefail
cd /workspace/federated-query/investigate-duckdb/pg-scanner-src
export PATH="/workspace/venv-fedq/bin:$PATH" GEN=ninja
make release -j12 > /workspace/federated-query/investigate-duckdb/build-pgscanner.log 2>&1
cd /workspace/federated-query/investigate-duckdb
BIN=pg-scanner-src/build/release/duckdb
if ! [ -x "$BIN" ]; then echo "BUILD FAILED"; grep -E "error:|FAILED:" build-pgscanner.log | grep -v "git is available" | head; exit 1; fi

echo "=== main (duckdb-postgres) binary ==="; "$BIN" --version
"$BIN" -c "LOAD postgres; SELECT 'main pg scanner OK' AS ok;" 2>&1 | tail -2

mkdir -p results
# q11 is main-only (ORDER BY pushdown); run it for stable too for contrast.
VERBOSE=1 ./capture.sh ./bin/duckdb-stable queries/q11_order_limit.sql "q11_order_limit-stable" > results/q11_order_limit-stable.txt 2>&1
for q in q0_count q1_headline q2_filter q3_projection q4_pgjoin q5_agg \
         q6_xsource_full q7_xsource_selective q8_join_cmpfilter q9_limit q11_order_limit; do
  VERBOSE=1 ./capture.sh "$BIN" queries/$q.sql "${q}-main" > results/${q}-main.txt 2>&1
  echo "ran $q on main"
done
"$BIN" analytics-poc.duckdb < queries/q10_explain.sql > results/q10_explain-main.txt 2>&1
echo "=== ALL MAIN PROBES DONE ==="