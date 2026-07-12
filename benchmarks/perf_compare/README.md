# perf_compare: the ONLY sanctioned engine perf comparison

Reproducible cold + warm comparison of the OLD (Python) and NEW (Rust) engines
per source. No ad-hoc timing runs: if a perf number is not produced by this
harness, it does not go in a report, a commit message, or a decision.

## Definitions (fixed - do not reinterpret per run)

- COLD: a fresh OS process AND a fresh runtime per single measurement. A cached
  statistic, plan, or connection cannot leak in. One number per process.
- WARM: one live runtime per (engine, source); per query, `--warmups` untimed
  executes, then the MEDIAN of `--runs` timed plans and `--runs` timed executes.
- plan: planning only. OLD = the internal stage pipeline; NEW = EXPLAIN (plans
  fully, never executes).
- total: `execute()` wall clock including the engine's own planning.
- fetch: source_scan time inside the last warm execute (FEDQRS_PROFILE trace).
- Engines never share a process (each links its own DuckDB).
- The NEW engine's planning budget (default 100ms) applies: a blown budget is a
  KILLED row carrying the engine's own stage report. A source an engine cannot
  build (NEW has no parquet connector yet) is UNSUPPORTED. Errors are ERROR with
  the engine's message. Nothing is silently skipped.
- Release build only; compare.py builds `--release` itself (use --skip-build to
  reuse the existing release build). DuckDB is prebuilt (scripts/setup-duckdb-lib.sh).

## Run

```
cd benchmarks/perf_compare
/workspace/venv-fedq/bin/python compare.py --scale-factor 1 --label sf1
```

Options: `--sources duck,parquet[,pgduck]` (pgduck needs the pg database loaded,
`--pg-database duckpoc_sf1`), `--queries q01,q06`, `--planning-budget-ms 100000`
to lift the kill when you need the underlying number, `--warmups/--runs`.

Data prerequisites (fail loudly if missing): `benchmarks/tpch/data/tpch_sf<sf>.duckdb`
(generate.py), `benchmarks/tpch/data/parquet_sf<sf>/` (export_parquet.py), and for
pgduck the pg database (load_postgres.py).

## Output

`results/<label>/results.json` (raw records: every measurement, status, message)
and `results/<label>/REPORT.md` (per-source tables: plan/total x cold/warm x
engine, one row per query, plus totals over the queries both engines completed
and the new-engine status counts). Values are unrounded milliseconds.

Re-running with the same label overwrites - use one label per (machine, scale,
commit) intent, e.g. `sf1`, `sf10-post-stats-fix`.
