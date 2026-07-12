# TPC-DS benchmark

ONE benchmark entry point: `run_federated_rust.py`. Do not create another runner
script here - extend it. Every number this suite produces comes from it. It has
four subcommands; `run` is the default when none is named, so the historical
flags-only invocation keeps working.

## Run the benchmark (the only command you normally need)

```
/workspace/venv-fedq/bin/python run_federated_rust.py run --scale-factor 1 --pg-database tpcds_sf1 --warm-runs 3
```

(equivalently, with no subcommand: `run_federated_rust.py --scale-factor 1 ...`)

- Drives all 99 queries through the RUST engine over the pg-dims federated
  split, verifies every result against the cached DuckDB truth, and reports
  cold/warm ms per query next to the CACHED oracle baseline.
- The DuckDB baseline is NEVER re-measured here: `oracle_timings` inside
  `data/references_sf<sf>.duckdb` was measured once per dataset by `save-refs`
  below. Correctness truth comes from the same file.
- Flags: `--only 1,5,70`, `--warm-runs N` (cold column = first run, warm =
  median of N), `--cold-process` (fresh process per query - the strict cold
  definition of benchmarks/perf_compare; slower), `--pg-database` per scale
  (tpcds_sf01 / tpcds_sf1 / tpcds_sf10).
- HARD WALL BUDGETS (deterministic, not overridable): sf0.1 and sf1 runs are
  KILLED at 60s, sf10 at 300s (a watchdog thread os._exits the process, exit
  code 124 - a hung native call cannot outlive it). A run past its budget is a
  regression to fix, never a budget to raise.
- Report: `reports/rust-fed-sf<sf>.md`.

## Build the cached references (per scale factor)

```
/workspace/venv-fedq/bin/python run_federated_rust.py save-refs --scale-factor 1 --pg-database tpcds_sf1
```

- Runs the pure-DuckDB truth + the federated DuckDB oracle ONCE and stores
  per-query result tables (with a `__ord` order column) and the `oracle_timings`
  table into `data/references_sf<sf>.duckdb`. Does NOT drive the Python or Rust
  engine.
- The oracle is NEVER re-run while its results exist: `save-refs` REFUSES when
  `references_sf<sf>.duckdb` already holds oracle timings. Rebuilding requires
  deleting the file first (only when the DATA changed).

## One-time data setup (per scale factor)

```
/workspace/venv-fedq/bin/python run_federated_rust.py generate --scale-factor 1
/workspace/venv-fedq/bin/python run_federated_rust.py load-pg --scale-factor 1 --pg-database tpcds_sf1
/workspace/venv-fedq/bin/python run_federated_rust.py save-refs --scale-factor 1 --pg-database tpcds_sf1
```

- `generate` writes the DuckDB dataset file and dumps the 99 query texts.
- `load-pg` loads every base table from the DuckDB file into a PostgreSQL
  database (both sources then hold identical rows; a placement only decides
  which source each table is READ from).

sf0.1, sf1, and sf10 are already built (duck files + pg databases + references).

## Files

- `run_federated_rust.py` - THE benchmark runner and the only .py file here. It
  inlines the former qualify / compare / generate / load_postgres libraries as
  sections.
- `queries/` - the 99 query texts; `data/` - duck files, pg load state,
  references; `reports/` - runner output.
