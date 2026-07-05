# TPC-H benchmark

This benchmark measures the engine against DuckDB on the 22 standard TPC-H
queries: both **timing** (median of three warm runs, ours vs DuckDB) and
**correctness** (a row-by-row differential check against DuckDB on the same
data). It runs three methodologies at two scale factors and writes a single
markdown report named after the git commit it was run against, so every result
is tied to a known code version.

The one command that produces a report is `bench.py`. Everything else here is
data preparation or the lower-level runners `bench.py` reuses.

## Methodologies (the three cells)

| Cell         | What it measures                                | Storage           | Is DuckDB fair? |
| ------------ | ----------------------------------------------- | ----------------- | --------------- |
| `single`     | Pure engine: all 8 tables in ONE Parquet source | Parquet           | yes             |
| `fedparquet` | Federation over 2 Parquet sources (facts/dims)  | Parquet x2        | no (see below)  |
| `fedpgduck`  | Federation over PostgreSQL + DuckDB             | PostgreSQL + DuckDB | yes           |

- **single** pushes the whole query to the Rust engine as one plan. DuckDB reads
  the same Parquet directly. This is the engine, with no federation.
- **fedparquet** splits the tables across two Parquet sources so most queries
  join across sources. It exercises our federation layer, but **the DuckDB
  reference reads all eight files monolithically and does not federate**, so the
  gap here overstates our federation cost. Kept for reference only.
- **fedpgduck** places the fact tables on DuckDB and the dimensions on
  PostgreSQL. The DuckDB reference federates the **same** split by attaching
  PostgreSQL through its `postgres` connector, so both engines cross the source
  boundary. **This is the honest federated comparison.**

Scale factors: `0.1` (600K lineitem rows) and `1` (6M lineitem rows).

## One-time data preparation

Each scale factor needs three inputs: a DuckDB database file, a Parquet
directory, and a PostgreSQL database. All are produced by committed scripts.

```bash
cd benchmarks/tpch
source /workspace/venv-fedq/bin/activate
export PYTHONPATH=/workspace/federated-query:/workspace/federated-query/benchmarks/tpch

# --- scale factor 0.1 ---
python generate.py       --scale-factor 0.1                                   # data/tpch_sf0.1.duckdb + queries/
python export_parquet.py --scale-factor 0.1 --target-dir /workspace/tpch_parquet
createdb -h localhost -U postgres duckpoc                                      # skip if it exists
python load_postgres.py  --scale-factor 0.1 --pg-database duckpoc

# --- scale factor 1 ---
python generate.py       --scale-factor 1                                     # data/tpch_sf1.duckdb
python export_parquet.py --scale-factor 1 --target-dir /workspace/tpch_parquet_sf1
createdb -h localhost -U postgres duckpoc_sf1                                 # skip if it exists
python load_postgres.py  --scale-factor 1 --pg-database duckpoc_sf1
```

PostgreSQL must be running (`make pg-start` from the repo root). The `createdb`,
`psql`, and `pg_ctl` binaries are under `postgres-17/bin`; add it to `PATH` if
they are not found. The DuckDB `.duckdb` files are git-ignored; the Parquet
directories live outside the repo. `bench.py` maps each scale factor to its
inputs (see the tables at the top of `bench.py`):

| SF  | Parquet dir                    | PostgreSQL database | DuckDB file            |
| --- | ------------------------------ | ------------------- | ---------------------- |
| 0.1 | `/workspace/tpch_parquet`      | `duckpoc`           | `data/tpch_sf0.1.duckdb` |
| 1   | `/workspace/tpch_parquet_sf1`  | `duckpoc_sf1`       | `data/tpch_sf1.duckdb`   |

## Running the benchmark

From the repo root, with the venv active and PostgreSQL up:

```bash
python benchmarks/tpch/bench.py                      # all 3 cells, SF0.1 + SF1
python benchmarks/tpch/bench.py --scales 0.1         # one scale factor
python benchmarks/tpch/bench.py --cells single,fedpgduck   # a subset of cells
python benchmarks/tpch/bench.py --memlimit-gb 12 --timeout 600   # caps (defaults shown)
```

Each query runs in its own subprocess bounded by `--timeout` seconds and an RSS
cap of `--memlimit-gb`. A query that hangs becomes a `TIMEOUT` row; one that
crosses the memory cap becomes a `KILLED` row; a query the engine cannot run
becomes an `ERROR` row. None of these stall or abort the rest of the matrix.

The report is written to `reports/report-result-<commit>.md`, where `<commit>`
is the current short git hash. Commit the harness before running so the report
is tied to a clean commit; a dirty working tree is flagged in the report header.

## The report

`report-result-<commit>.md` contains:

- **header** - commit, host CPU, engine (Rust/DataFusion), DuckDB oracle
  version, scale factors, and the methodology description;
- **summary matrix** - one row per (cell, scale): queries correct out of 22,
  total ours ms, total DuckDB ms, and the ours/DuckDB ratio;
- **issues** - every query that did not match, with its status
  (`MISMATCH` / `ERROR` / `TIMEOUT` / `KILLED`) and detail;
- **per-query detail** - one table per (cell, scale) with each query's ours ms,
  DuckDB ms, ratio, correctness, and engine/oracle row counts.

Ratio is ours / DuckDB, so higher means we are slower. Time totals cover only
queries that produced a measurement; killed/timed-out/errored queries are
excluded from the ms totals but counted as not-correct.

## Correctness comparison

Results are compared against DuckDB on the same data. Numbers are rounded to two
decimals (matching TPC-H monetary values) and `CHAR` padding is stripped, so
only genuine value differences count. A query passes only when every row and
every value matches.

## Files

- `bench.py` - the orchestrator: runs the matrix and writes the commit-named report.
- `generate.py` - build the DuckDB database file and the 22 query files.
- `export_parquet.py` - export a DuckDB database to a Parquet directory.
- `load_postgres.py` - load the eight base tables into PostgreSQL.
- `run_tpch.py` - lower-level Parquet runner (single / fedparquet); `bench.py`
  reuses its per-query measurement.
- `run_federated.py` - lower-level PG+DuckDB runner (fedpgduck); `bench.py`
  reuses its per-query evaluation.
- `run.py` - standalone single-source coverage/correctness runner (no timing).
- `qualify.py` / `compare.py` - table qualification and result comparison helpers.
- `data/` - generated DuckDB database files (git-ignored).
- `queries/` - generated TPC-H query files.
- `reports/` - generated `report-result-<commit>.md` reports.
