# perf_compare: the ONLY sanctioned engine perf comparison

Reproducible cold + warm comparison of the OLD (Python) and NEW (Rust) engines
per source. No ad-hoc timing runs: if a perf number is not produced by this
harness, it does not go in a report, a commit message, or a decision.

## Game-studio accelerator over the real server

The same runner also measures the complete materialized-view lifecycle over
the PostgreSQL wire protocol and the game-studio Docker Compose sources:

```bash
"$VIRTUAL_ENV/bin/python3" benchmarks/perf_compare/compare.py \
  studio-accelerator --label my-studio-accelerator-run
```

Run from the repository root after generating/loading
`tutorials/game-studio` and starting its Compose databases. The runner builds
the release workspace, starts its own FedQ servers, and copies the source
config into a private report catalog. Set `GAME_STUDIO_DOCKER='sudo -n docker'`
on hosts where Docker requires it. Labels are unique and never overwritten.

Every invocation runs all seven phases: live join, full CREATE, substituted
read, source inserts with a stale read, live bypass, whole REFRESH, and a
refreshed substituted read. Default work is 175 FedQ client statements:
56 timed targets, 14 untimed warmups, and 105 setup/inspection/cleanup calls,
plus two real MySQL mutation statements. Allow a few minutes for this
22-server-start lifecycle with an up-to-date release build. Required release
compilation happens first and is outside the per-statement measurements.

Cold samples each use a fresh Python worker, release-server process, and
runtime; warm medians use one live server connection across the experiment.
Defaults are `--cold-runs 3 --runs 5 --warmups 2`. Cold and warm are always
measured. Learned-statistics reads are disabled to prevent persisted estimates
leaking between cold samples; the default planning budget is unchanged.
SQL tracing and fragment recording stay enabled throughout. Timings cover
statement issue through full result decoding, including planning, execution,
and wire transport. CREATE includes its complete build; REFRESH includes its
complete refresh. The DROP preparing each CREATE is outside that CREATE.

Both data states have independent DuckDB oracle answers, computed once before
measurement and checked against every sampled result. Cache claims require
`MaterializedScan` plus a sole materialized fragment. The runner checks chunk
content hashes, whole-refresh statuses, and successful restoration of the
original live result. Existing reserved ticket IDs raise before mutation.
SQL errors/budget kills are ERROR/KILLED; other failures raise and keep an
incomplete report. Source rows and the experiment's view are cleaned up.

`results/<label>/results.json` retains every raw sample and diagnostic;
`REPORT.md` shows cold and warm medians. Full server/worker logs and copied
Arrow chunks accompany them. CREATE/REFRESH do not replace the SELECT fragment
journal, so their actual fetch SQL is in `live.server.log`. Engine, harness,
SQL, and fixture hashes identify the measured implementation and inputs.
These helpers are orchestrated by `compare.py`; there is no second runner.

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
  build is UNSUPPORTED. Errors are ERROR with the engine's message. Nothing is
  silently skipped.
- Release build only; compare.py builds `--release` itself (use --skip-build to
  reuse the existing release build). DuckDB is prebuilt (scripts/setup-duckdb-lib.sh).

## Run

```
cd benchmarks/perf_compare
python compare.py --scale-factor 1 --label sf1
```

Options: `--sources duck,parquet[,pgduck]` (pgduck needs the pg database loaded,
`--pg-database duckpoc_sf1`), `--queries q01,q06`, `--planning-budget-ms 100000`
to lift the kill when you need the underlying number, `--warmups/--runs`.

### Measuring the OLD engine

The OLD engine lives in the fedqrs repository and is installed in its OWN
virtualenv - it links its own DuckDB and pins its own dependencies, so it cannot
share an interpreter with this one. It is reached as a COMMAND that emits the
same JSON lines, never imported, and this harness records nothing about where it
is installed:

```
python3 -m venv <fedqrs-venv> && source <fedqrs-venv>/bin/activate
pip install maturin && (cd <fedqrs-checkout> && maturin develop --release)

python compare.py --scale-factor 1 --label sf1 \
    --old-engine-cmd <fedqrs-venv>/bin/fedqrs-bench
```

`FEDQ_OLD_ENGINE_CMD` works instead of the flag. Asking for `--engines old`
without either exits naming what is missing rather than reporting a half table.
`--engines new` needs none of this.

The TPC-H queries and data stay in this repository either way; compare.py passes
their location to that command with `--tpch-dir`.

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
