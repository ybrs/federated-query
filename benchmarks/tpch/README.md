# TPC-H benchmark

This benchmark checks how many of the 22 standard TPC-H queries the federated
query engine supports and whether it returns correct results. Performance is not
measured here; the goal is query coverage and correctness.

## What it does

1. `generate.py` uses the DuckDB `tpch` extension to build a native DuckDB
   database file (`data/tpch_sf<SF>.duckdb`) at a chosen scale factor and dumps
   the 22 TPC-H queries (as DuckDB emits them) to `queries/q01.sql` .. `q22.sql`.
2. `run.py` registers that database file as a single DuckDB source named `tpch`,
   then for each query:
   - qualifies the eight TPC-H base tables to `tpch.main.<table>` and transpiles
     the query to the engine's dialect, then runs it through the full engine
     pipeline;
   - runs the original query directly in DuckDB as the correctness oracle;
   - compares the two result sets.

The single source is the starting point. The same query files can later be run
across multiple sources (e.g. splitting tables between DuckDB and PostgreSQL) to
exercise federation.

## Running

```bash
cd benchmarks/tpch
./run.sh                     # scale factor 0.01 (fast, ~60k lineitem rows)
./run.sh 1                   # standard TPC-H scale factor 1 (~6M lineitem rows)
./run.sh 0.01 --only 1,6     # run a subset of queries
./run.sh 0.01 --timeout 120  # raise the per-query wall-clock limit
```

`run.sh` defaults to the `venv-fedq` interpreter; override with
`PYTHON=/path/to/python ./run.sh`.

Each engine run happens in an isolated child process bounded by `--timeout`
seconds (default 60) and `--memory-limit` MB of address space (default 12288).
A query that exceeds either becomes an `ERROR` row (`Timeout` / `Killed`)
instead of stalling or OOM-ing the whole run. Raise the limits for large scale
factors.

## Output

One line per query plus a summary:

- `PASS` - engine ran the query and the result set matches DuckDB.
- `MISMATCH` - engine ran the query but returned different rows (the reason
  names the row-count or an example differing row).
- `ERROR` - engine could not run the query (the reason is the exception).

## Correctness comparison

Results are compared by column position (the engines may name columns
differently), as a multiset of rows. Numbers are rounded to `--decimals`
(default 2, matching TPC-H monetary values) and `CHAR` padding is stripped. Row
order is not compared: TPC-H ties are order-ambiguous, so a multiset match is
the correctness signal. Adjust rounding with `run.py --decimals N`.

## Files

- `generate.py` - build the DuckDB database and query files.
- `qualify.py` - rewrite bare TPC-H table names to `source.schema.table`.
- `compare.py` - multiset result comparison with numeric tolerance.
- `run.py` - run queries through the engine and the DuckDB oracle, report.
- `run.sh` - end-to-end orchestration (generate if needed, then run).
- `data/` - generated database file (git-ignored).
- `queries/` - generated TPC-H query files.
