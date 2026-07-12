# TPC-DS benchmark

ONE benchmark entry point. Do not create another runner script here - extend
`run_federated_rust.py`. Every number this suite produces comes from it.

## Run the benchmark (the only command you normally need)

```
/workspace/venv-fedq/bin/python run_federated_rust.py --scale-factor 1 --pg-database tpcds_sf1 --warm-runs 3
```

- Drives all 99 queries through the RUST engine over the pg-dims federated
  split, verifies every result against the cached DuckDB truth, and reports
  cold/warm ms per query next to the CACHED oracle baseline.
- The DuckDB baseline is NEVER re-measured here: `oracle_timings` inside
  `data/references_sf<sf>.duckdb` was measured once per dataset by the
  reference build below. Correctness truth comes from the same file.
- Flags: `--only 1,5,70`, `--warm-runs N` (cold column = first run, warm =
  median of N), `--cold-process` (fresh process per query - the strict cold
  definition of benchmarks/perf_compare; slower), `--pg-database` per scale
  (tpcds_sf01 / tpcds_sf1 / tpcds_sf10).
- HARD WALL BUDGETS (deterministic, not overridable): sf0.1 and sf1 runs are
  KILLED at 60s, sf10 at 300s (a watchdog thread os._exits the process, exit
  code 124 - a hung native call cannot outlive it). A run past its budget is a
  regression to fix, never a budget to raise.
- The oracle is NEVER re-run while its results exist: `--mode save-refs`
  refuses when references_sf<sf>.duckdb already holds oracle timings;
  rebuilding requires deleting the file first (only when the DATA changed).
- Report: `reports/rust-fed-sf<sf>.md`.

## One-time data setup (per scale factor)

```
/workspace/venv-fedq/bin/python generate.py --scale-factor 1
/workspace/venv-fedq/bin/python load_postgres.py --scale-factor 1 --pg-database tpcds_sf1
/workspace/venv-fedq/bin/python run_federated.py --scale-factor 1 --pg-database tpcds_sf1 --mode save-refs
```

`save-refs` runs the truth + federated oracle ONCE and stores per-query result
tables and the `oracle_timings` table into `data/references_sf<sf>.duckdb`.
Re-run it only when the DATA changes. sf0.1, sf1, and sf10 are already built
(duck files + pg databases + references).

## Files

- `run_federated_rust.py` - THE benchmark runner (Rust engine).
- `run_federated.py` - the Python-engine staged tally + `save-refs` reference
  builder (reference implementation; scheduled for deletion with the Python
  engine at the rewrite's final gate).
- `run.py` - Python-engine single-source runner; also the helper library
  (`arrow_to_rows`, query loading) the other runners import.
- `qualify.py`, `compare.py` - shared qualification and result-comparison
  libraries.
- `generate.py`, `load_postgres.py` - data setup.
- `queries/` - the 99 query texts; `data/` - duck files, pg load state,
  references; `reports/` - runner output.
