# HANDOFF - Python to Rust rewrite

This document is the working state of the Python->Rust rewrite of the engine.
The Python engine's own development history is preserved in `oldhandoff.md`;
the rewrite's earlier per-crate build log lives in git history of this file.

## What this is

We are rewriting the entire engine (30,183 lines of Python under
`federated_query/`) into Rust, as a single Cargo workspace under
`federated-query/`. The governing technical plan is
`rewrite-python-to-rust-plan.md`.

Strategy (decided with the user):

- BIG-BANG, not incremental. The Rust engine is a parallel implementation.
  The Python engine stays the working product and the behavioral reference,
  untouched, until the Rust engine passes the full gate (all translated tests
  + the SQL-level corpus + the TPC-DS/TPC-H tallies vs pure-DuckDB truth),
  after which the Python package is deleted.
- TEST-FIRST per crate; every retirement of a Python-mechanics test is stated
  in the crate's port notes, never silent.
- CLEAN RUST over Python parity: no byte-compatibility, no reading Python
  on-disk artifacts (learned-stats SQLite rebuilds from scratch); port
  BEHAVIOR, express it the natural Rust way.
- Python is FROZEN during the rewrite (stable reference; no new features).

## Where we stand (2026-07-13)

ALL 13 CRATES ARE BUILT AND THE ENGINE IS COMPLETE END TO END: SQL -> parse ->
bind -> decorrelate -> optimize -> PhysicalPlan -> Steps -> DataFusion
execution -> Arrow -> Python (pyo3 `fedq.Runtime`). Cross-source federation
(DuckDB x Postgres) works. 621 workspace tests green. Build-order steps 1-7
of the plan are done; what remains is Milestone D (the gate) - see NEXT.

Correctness and perf, all measured by the suite runners (never ad hoc):

- TPC-H: 22/22 correct, single-source AND federated pg-dims (sf0.01); SF1
  22/22 via perf_compare, zero planning-budget kills, warm plan 1-11ms.
- TPC-DS federated pg-dims (`benchmarks/tpcds/run_federated_rust.py`):
  98 ok | 0 wrong | 1 error at ALL of sf0.1 / sf1 / sf10. The one error is
  q14, an upstream polyglot-sql parser bug (trailing UNION ALL attached to a
  scalar-subquery operand), surfaced loudly.
- TPC-DS timings vs the cached pure-DuckDB baseline (warm, --warm-runs 1):
  sf0.1 total 1.28x geomean 1.26x; sf1 total 1.02x geomean 0.97x; sf10
  total 0.89x geomean 1.21x (ours 60.6s vs DuckDB 68.3s). The Rust engine
  BEATS local DuckDB on sf10 totals. (The Python engine's final sf10 board
  was 1.00x total / 1.61x geomean.)

## Branch, build, gates

- Branch: `rewrite-python-to-rust` (in this repo; `fedqrs` has a matching
  branch but has not been merged in yet).
- Toolchain: cargo 1.96.1 at `$HOME/.cargo/bin` (HOME=/tmp). Build env:
  `export CARGO_TARGET_DIR=/workspace/federated-query/target`.
- Before every commit: `cargo fmt --all && cargo clippy --workspace
  --all-targets && cargo test --workspace`. The full gate after a one-line
  bottom-crate change is ~7 SECONDS wall (was 30+ min): the dev profile
  strips dependency debuginfo (`debug = "line-tables-only"` for workspace
  code, `debug = false` for deps - linking, not compiling, was the cost) and
  each crate links ONE integration-test binary (`tests/it/main.rs`, suites
  as modules). Keep it that way: a new test file goes into the crate's
  `tests/it/`, never as a new top-level `tests/*.rs` target.
- Lint: `warnings = "deny"` escalates clippy pedantic; curated allows in
  `Cargo.toml [workspace.lints.clippy]`. `make fq-lint` runs the house rules
  (FQ-BUNDLED, FQ-PMCOMMENT, ...). The semantic comment gate
  (scripts/comment-gate) blocks commits whose comments describe project
  history instead of the code.
- DuckDB is NEVER compiled in cargo: `make duckdb-lib` fetches the official
  prebuilt library pinned by Cargo.lock into `.duckdb-lib/current`;
  `.cargo/config.toml` wires lib dir + rpath.
- Planning is O(metadata), enforced: `optimizer.planning_budget_ms` (default
  100) is a hard kill with a per-stage report. No off switch.
- Perf numbers come ONLY from the suite runners: `benchmarks/perf_compare/`
  (cross-engine cold+warm) and each suite's single runner.
- Test env: Postgres 17.4 on :5432 (binaries `postgres-17`, trust auth,
  started via `scripts/run-postgres.sh`). Python: `/workspace/venv-fedq`.

## PERF ROUND 2026-07-13: outlier hunt on the TPC-DS boards

Method: rank warm ratio AND absolute excess vs the cached DuckDB baseline at
SF10 (the target scale), profile with `FEDQRS_PROFILE=1` through the suite
runner (`--only N,M`), read plans via `Runtime::execute("EXPLAIN <sql>")`.

Landed fixes (each with unit tests pinning the behavior):

1. CTE PRODUCER SHARING (fq-plan + fq-physical). `PhysicalCteScan.producer`
   was a `Box`; `plan_cte_ref` deep-copied the lowered producer into every
   reference, so step-building's pointer-keyed cte_bindings cache never hit
   and the CTE body re-executed once per consumer (q04 ran its 3-channel
   union aggregate SIX times). Now the planner registry holds one
   `Arc<PhysicalPlan>` per CTE and every CteScan clones the pointer: the
   body's fragments are built and executed once. q04 15227ms -> 3802ms,
   q23 7589 -> 4420, q11 3132 -> 2014, q47 2549 -> 1085, q57/q59/q74/q75/q02
   all dropped proportionally.
2. FLAG-UNION PASSTHROUGH NARROWING (fq-optimize ProjectionPushdown).
   Decorrelation's boolean-flag unions project EVERY input column as explicit
   qualified passthroughs (the no-star rule); the pruner's GLOBAL
   required-column collection treated each entry as a requirement, so one
   flag union pinned every column of every relation and NO scan pruned
   (q45 pulled all 32 columns of customer x customer_address, twice; Python
   never hit this because its passthrough was a literal `*`, invisible to
   collection). Narrowable unions (non-distinct, branches are wrapper chains
   over plain projections, no positional consumer above) now resolve
   passthroughs at the union: collection skips plain-column entries, pruning
   keeps a position only when computed or name-referenced above, kept
   columns re-enter the required set for the branch subtrees. Positional
   consumers (set ops, column_names renames, outer unions) and DISTINCT
   shapes keep every entry. q45 2000ms -> 482ms. NOTE: predicate pushdown
   runs before projection pushdown and distributes filters INTO union
   branches, so the branch shape is Filter-over-Projection - the narrowing
   sees through row-preserving wrappers (Filter/Sort/Limit/GroupedLimit/
   SingleRowGuard).

Remaining SF10 tail (ranked by warm excess vs DuckDB; diagnoses measured,
fixes NOT started):

- q04 +2.6s (2.99x): the year_total branch join+aggregate still runs on the
  coordinator (customer 500k rows exceeds the 200k dim-ship budget; CTE-root
  shape misses the shippable gate). Candidate: aggregate-collapse-aware ship
  budget or eager aggregation below the customer join. Same class: q11, q23.
- q78 +2.2s (2.78x): three channel CTEs (sales-minus-returns joins) pulled
  and joined on the coordinator.
- q95 +1.7s (2.75x): the ws_wh self-join CTE materializes 74.8M rows and
  PULLS them to the coordinator; the EXISTS consumers should keep it inside
  duck (q16 proves the pushed EXISTS shape works).
- q39 +1.3s (4.14x): unchanged by producer sharing - the shipped duck island
  re-runs per consumer (4 identical 381k-row island scans). The binding is
  shared at step level; investigate why the island SourceScan re-executes.
- q06 +1.2s (6.65x): the date filter arrives via a scalar subquery
  (d_month_seq = (SELECT ...)), semi-join reduction cannot derive keys from
  it, so 28.8M store_sales rows cross unfiltered. Needs scalar-first
  evaluation or a two-phase reduction.
- q59 (3.4x): the consumers' week-range filter reaches the CTE body only as
  a join, so the body aggregates ALL weeks of store_sales; CTEUnionFilter
  pushdown translates literal filters only.
- q02 (2.6x): web_sales+catalog_sales pulled whole for the same reason
  (d_week_seq filter via join).
- q15 (3.4x): customer x customer_address remote join is narrow (5 cols) but
  the zip/state/price OR filter stays on the coordinator instead of inside
  the pg query.

## How to run everything

Environment for every command:
```
export PATH=$HOME/.cargo/bin:/tmp/.cargo/bin:$PATH
export HOME=/tmp
export CARGO_TARGET_DIR=/workspace/federated-query/target
```
Python interpreter: `/workspace/venv-fedq/bin/python`.

Build the engine for the benchmark harnesses (release; the benchmark symlinks
`benchmarks/{tpch,tpcds}/fedq.so` point at `target/release/libfedq_py.so`):
```
cargo build --release -p fedq-py
```

TPC-DS (ONE runner; see benchmarks/tpcds/README.md for save-refs/generate):
```
cd benchmarks/tpcds
python run_federated_rust.py run --scale-factor 10 --pg-database tpcds_sf10 --warm-runs 1
```
Flags: `--only 4,45`, `--cold-process`. Hard wall budgets kill a run past
60s (sf0.1/sf1) / 300s (sf10). Reports: `reports/rust-fed-sf<sf>.md`.
Profiling: prefix `FEDQRS_PROFILE=1` (per-step ms on stderr); the engine also
honors `FEDQRS_TRACE_SQL=1` (every pushed SQL). Plans:
`Runtime::execute("EXPLAIN <sql>")` (qualify table names first - see the
runner's `_qualify`).

TPC-H: `benchmarks/tpch/run_federated_rust.py` (same shape); data state:
pg `duckpoc` (sf0.01) / `duckpoc_sf1`, duck files
`benchmarks/tpch/data/tpch_sf{0.01,1}.duckdb`, parquet
`benchmarks/tpch/data/parquet_sf1/`.
TPC-DS data: duck `benchmarks/tpcds/data/tpcds_sf{0.1,1,10}.duckdb`; pg
databases `tpcds_sf01` / `tpcds_sf1` / `tpcds_sf10`; cached truth+baseline in
`data/references_sf<sf>.duckdb` (NEVER re-measured by a run).

Cross-engine perf: `benchmarks/perf_compare/compare.py` (the only sanctioned
cold/warm cross-engine numbers; parquet on the NEW engine reports
UNSUPPORTED until the parquet connector exists).

## Crate map (all built; per-crate test counts in `cargo test`)

```
fq-parse -> fq-plan -> fq-common          fq-emit -> fq-plan
fq-bind -> fq-plan, fq-catalog            fq-exec -> fq-plan, fq-connectors
fq-decorrelate -> fq-plan                 fq-runtime -> everything above
fq-optimize -> fq-plan, fq-catalog, fq-connectors
fq-physical -> fq-plan, fq-optimize, fq-emit
fedq-py -> fq-runtime      (fedq CLI: NOT built yet)
```

- fq-common: config (serde-YAML, deny_unknown_fields), errors, tracing,
  DataType.
- fq-catalog: Schema/Table/Column, DataSource trait (catalog/statistics tier
  ONLY - no drivers), StatsCatalog on rusqlite.
- fq-connectors: DuckDB (metadata-only stats; EXPLAIN "~N rows" estimates)
  + Postgres (information_schema, pg_stats decode, statless probe, EXPLAIN
  JSON estimates). NO parquet/clickhouse catalog tier yet.
- fq-plan: Expr/LogicalPlan/PhysicalPlan enums, exhaustive walkers (no `_`
  arms), typed `schema()`. CTE producers are `Arc`-shared (one allocation
  per CTE, every CteScan points at it).
- fq-parse: polyglot-sql 0.5.15 -> LogicalPlan; allowlist conversion, star
  expansion, window functions, comma joins, LIKE. Still raises: WITH
  RECURSIVE, positioned TRIM, APPLY/ASOF, BETWEEN SYMMETRIC.
- fq-bind: scope chain + ONE resolver (invalid query MUST raise BindError),
  aggregate hoists for HAVING/ORDER BY, star read-set expansion.
- fq-decorrelate: the always-decorrelate pass (EXISTS/IN/scalar/quantified,
  NULL-aware, disjunctive domain-union + flag-union, Neumann-Kemper
  dependent join, lateral fallback); synthetic columns carry qualifier AND
  DataType; post-pass asserts no subquery survives + validate_scope.
- fq-optimize: CostModel (honest unknowns, learned overlay, source
  estimates), fixpoint driver, rules: CTEUnionFilter, Predicate, SemiJoin,
  EagerAgg, JoinOrdering (Selinger DP + GOO + locality), Projection
  (incl. flag-union narrowing), Aggregate, OrderBy, Limit.
- fq-emit: canonical Postgres-form SQL text + polyglot transpile boundary
  (`to_source_sql`).
- fq-physical: PhysicalPlanner (lowering, join algorithm/orientation,
  dynamic-filter marking), SingleSourcePushdown (subtree -> one
  RemoteQuery), DimShipping (9 gates), step building (PhysicalPlan ->
  Vec<Step> + fragments, CSE, reduction orientation, observations).
- fq-exec: the imported fedqrs DataFusion engine, de-pyo3'd (fragment
  fusion, spill, reductions, ship, prefetch pools); bridge keeps core::ir
  in-process (no JSON).
- fq-runtime: config-driven Runtime, full pipeline, textual EXPLAIN,
  session-shared StatisticsCollector, learned-stats persistence
  (`<config>.stats.sqlite`), StageLog + planning-budget kill.
- fedq-py: pyo3 cdylib; `fedq.Runtime(config_path).execute(sql)` -> Arrow
  C stream (GIL released).

## NEXT: Milestone D (the gate), in rough order

1. E2E suites -> Rust engine: STARTED. tests/rust_runtime.py drives fedq
   (temp YAML per env, pa.Table results, pushed SQL re-parsed from textual
   EXPLAIN); e2e_pushdown conftest/helpers rewired; row-based suites green
   (44 tests). BLOCKER for the ~31 EXPLAIN-shape suites: the textual EXPLAIN
   renders Scan-node pushdowns as tags (+filter/+agg) with NO rendered SQL -
   fq-runtime explain.rs must emit the effective pushed SQL for Scans.
   Engine gaps the run surfaced (each errors loudly): parser lacks named
   WINDOW, star EXCEPT/REPLACE/RENAME, WITHIN GROUP + MEDIAN + MODE, PIVOT,
   FETCH FIRST, QUALIFY; star-over-VALUES is VALID on the Rust engine
   (test_star_over_values_fails_fast pins a Python-only limitation -
   product decision pending). FIXED since: user LATERAL joins (parse ->
   LateralJoin, bind with left scope, decorrelation flattens via the
   dependent shapes; PhysicalLateralJoin has NO exec path, so a
   non-flattenable lateral raises) and the flag-union duplicate-alias
   schema error (passthrough aliases uniquify). Python-internal suites
   (e2e_decorrelation, plan-object assertions) retire with the Python
   package, not converted.
2. Adversarial placement: the tpcds runner only implements pg-dims; the gate
   needs BOTH placements at sf0.1/sf1/sf10. Extend the ONE runner.
3. q14: upstream polyglot-sql fix or pre-parse normalization -> 99|0|0.
4. Parquet connector (catalog/stats tier in fq-connectors; exec plane
   already reads parquet). Unblocks perf_compare parquet columns.
5. EXPLAIN document builder (costed, JSON shape the e2e suites assert on).
6. fedq CLI binary (plan 3.12).
7. Perf tail (see PERF ROUND above) - not gate-blocking; totals already beat
   DuckDB at sf10.
8. Teardown (only after 1-7): delete `federated_query/` and
   `/workspace/fedqrs` remnants, move `lint/` rules into fq-lint, rewrite
   HANDOFF/architecture docs against the crates.

FINAL GATE (unchanged): TPC-DS 99|0|0 at SF0.1/SF1/SF10 (pg-dims AND
adversarial) + TPC-H 22/22 + cold/warm columns, vs pure-DuckDB truth, then
DELETE `federated_query/`.

## Commit log (rewrite, newest first; earlier log in git history of this file)

```
4c19228 TPC-DS boards after union narrowing: sf10 total 0.89x, sf1 geomean 0.97x
dcdf73e ProjectionPushdown: narrow flag-union passthrough projections
4ec6942 TPC-DS boards after CTE producer sharing: sf10 total 0.91x (beats DuckDB)
1816716 CTE producer sharing: one Arc'd body per CTE, executed once per query
5d1720e Build: strip dependency debuginfo + one integration-test binary per crate
7017f51 TPC-DS boards refreshed: sf1 98|0|1 1.21x, sf10 98|0|1 1.22x geomean 1.31x
27f8f39 TPC-DS: one self-contained runner, everything else deleted
57700b4 TPC-DS: deterministic run guards - wall-budget kill + oracle-rerun refusal
d094dd6 TPC-DS suite: one runner, cached oracle baseline, honest ratios
cd35ec8 Rust rewrite: TPC-DS federated 98|0|1 at sf0.1 - ROLLUP, GROUPING-window split
f35910f Rust rewrite: TPC-DS federated 66|0|33 -> 92|0|7 at sf0.1 (zero wrong answers)
9483474 fq-parse comma joins + LIKE - TPC-H 3/22 -> 18/22 correct
5cd291a fedq-py MC3b - pyo3 bindings; Python drives the Rust engine
14cf5be fq-runtime MC3a - config-driven execute(sql), cross-source federation works
ea297bb fq-exec MC2 - Step bridge + first end-to-end Rust query execution
9bdba53 fq-exec MC1 - import the fedqrs execution engine (de-pyo3'd)
(fq-physical M0-M4d, fq-emit, fq-optimize M1-M4, fq-decorrelate, fq-bind,
 fq-parse, connectors, scaffold: see git log)
```

UPSTREAM WATCH: the q14 set-op repair in fq-parse convert.rs (SwallowedSetOp)
works around a polyglot-sql 0.5.15 mis-nesting; when a polyglot release nests
trailing set operations correctly, delete the repair block and its tests.
