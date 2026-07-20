# HANDOFF - Python to Rust rewrite

This document is the working state of the Python->Rust rewrite of the engine.
The Python engine's own development history is preserved in `oldhandoff.md`;
the rewrite's earlier per-crate build log lives in git history of this file.
Superseded plan docs were removed 2026-07-19 after digesting their decisions
into `historical-docs/design-history.md` (full text in git history).

## What this is

We rewrote the entire engine (originally 30,183 lines of Python under
`federated_query/`) into Rust, as a single Cargo workspace under
`federated-query/crates/`. The governing technical plan is
`rewrite-python-to-rust-plan.md`.

Strategy (decided with the user, unchanged):

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

## Where we stand (2026-07-20)

ALL 17 CRATES ARE BUILT AND THE ENGINE IS COMPLETE END TO END: SQL -> parse ->
bind -> decorrelate -> optimize -> PhysicalPlan -> Steps -> DataFusion
execution -> Arrow. Cross-source federation (DuckDB x Postgres, plus
ClickHouse/MySQL/Parquet connectors) works. There are three front doors over
one `fq_runtime::Runtime`: the Python extension (`fedq-py`), the PostgreSQL
wire server (`fedq-server`), and the `fedq` CLI. Around 979 workspace tests
green.

Correctness and perf, all measured by the suite runners (never ad hoc):

- TPC-DS federated, `benchmarks/tpcds/run_federated_rust.py`, BOTH placements
  at ALL scales:
  - pg-dims (dimensions on Postgres, facts on DuckDB): 99 ok | 0 wrong |
    0 error at sf0.1 / sf1 / sf10.
  - adversarial (sales facts split from their matching returns facts,
    dimensions alternated): 99 ok | 0 wrong | 0 error at sf0.1 / sf1 / sf10.
- TPC-DS timings vs the cached pure-DuckDB baseline (warm, `--warm-runs 1`):
  - pg-dims: sf0.1 total 1.42x geomean 1.35x; sf1 total 1.04x geomean 1.00x;
    sf10 total 0.91x geomean 1.25x (ours 65.1s vs DuckDB 71.4s). The Rust
    engine BEATS local DuckDB on sf10 totals.
  - adversarial: sf0.1 total 1.65x geomean 1.59x; sf1 total 2.61x geomean
    1.79x; sf10 total 3.34x geomean 2.90x (ours 238.6s vs DuckDB 71.4s). The
    adversarial split is the harder, still-open perf tail (see PERF ROUND).
- TPC-H: 22/22 correct, single-source AND federated pg-dims; via the runner /
  `perf_compare`, zero planning-budget kills, warm plan single-digit ms.
- E2E SQL corpus: `tests/e2e_pushdown` runs against the Rust engine (temp YAML
  per env, `pa.Table` results, pushed SQL re-parsed from the textual EXPLAIN);
  416 pass, 0 fail.
- FEDERATED PLACEMENT CORPUS: `tests/e2e_federated` - ~355 data-driven cases,
  each run under 7 table placements (single-DuckDB oracle, duck|duck, pg|duck,
  duck|pg, all-pg, parquet|duck, parquet|pg) and diffed value-by-value against
  the single-DuckDB oracle; 2475 passing checks in ~130s, one process (the
  suite README documents the harness, FEDQ_E2E_CORPUS module selection, and
  the bounded env LRU). This is the placement-differential gate the TPC
  tallies cannot provide at small scale.
- CORRECTNESS ROUND (2026-07-19/20): the corpus found 8 engine bugs on its
  first sweep; ALL are fixed, each with the corpus repro re-enabled as the
  regression guard (commits ece36bc, cb721cf, b5055ed, 6615543, 8b407ef):
  INTERSECT/EXCEPT ALL cross-source multisets (DataFusion is_all lowering is
  semi/anti; coordinator rewrites with row_number tags), parquet NULLS
  FIRST/LAST (parquet scans now render in DataFusion dialect), the pg stats
  probe on unorderable column types (fail-closed orderability allowlist +
  real pg error text surfaced), coordinator AVG over DECIMAL truncation
  (double-cast at the three coordinator render paths; TPC-DS sf0.1 99|0|0
  and TPC-H 22/22 re-verified), LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTILE,
  the || execution IR form (StringConcat, null-propagating), IS DISTINCT
  FROM as a general predicate, LIKE ... ESCAPE (first-class Expr::Like),
  and the SESSION-SCOPED DATA PLANE: every fq-exec registry/cache/pool
  connection keys on the runtime's SessionId and Runtime::drop releases
  them - no cross-runtime table bleed, bounded pg connections, and the
  full corpus runs in one process.

Feature surface built on top of the core engine:

- MATERIALIZED VIEWS (fq-accel + fq-runtime): `CREATE / REFRESH / DROP
  MATERIALIZED VIEW`, `SHOW MATERIALIZED VIEWS`. A view is a user-managed
  cached result persisted as framed Arrow IPC chunks under `<config>.mv/`
  through the `object_store` abstraction; the `materialized_views` row (in the
  stats SQLite) is the source of truth for the chunk list. Freshness is
  user-controlled: nothing on the query path checks the sources. REFRESH has
  three mechanics: WHOLE re-pull, DELTA APPEND past a monotonic change key's
  watermark (`change_keys` config), and PRIMARY-KEY MERGE (diff a fresh whole
  pull by key, rewrite only changed/deleted chunks). A per-base-table source
  token lets REFRESH skip the pull entirely when nothing changed (no-op skip).
  AUTOMATIC SUBSTITUTION (`fq-runtime` substitute.rs) rewrites a query plan to
  read from a view when it matches structurally, clears a name guard, and
  clears a cost gate (estimated recompute cost must strictly exceed the read
  cost); it is off-switchable per session (`accelerator.enable_substitution`,
  read fresh per query) and tracks per-view benefit counters surfaced by SHOW
  MATERIALIZED VIEWS. Design: `accelerator-plan.md`, `crates/fq-accel/README.md`.
- SETTINGS (fq-runtime settings.rs): one registry of 33 tunables (optimizer,
  cost, exec, executor, env toggles, accelerator, server) exposed as `SHOW
  SETTINGS`, `SHOW SETTING <name>`, `SET <name> = <value>`, `RESET <name>`,
  `RESET ALL`. SET/RESET mutate the live session; values are type-checked and a
  bad value or unknown name raises (with a nearest-name suggestion).
- EVENT ANALYTICS (fq-events + fq-runtime): an EVENT VIEW is a materialized
  view whose columns are mapped onto event roles - entity id, timestamp, event
  name, optional TIEBREAK; every other column is a property - and whose chunks
  are globally sorted by (entity, timestamp[, tiebreak]) with no NULL role.
  `CREATE / REFRESH / DROP EVENT VIEW`, then the analysis statements FUNNEL
  (ordered step conversion within a window), SEGMENT (per-bucket event and
  distinct-entity measures), and PATHS (top event sequences, optional STARTING
  AT anchor). Two DERIVED sidecars rebuild with each generation over one scan:
  a SEGMENT pre-aggregate (per-bucket counts at day/week/month grain) that
  answers SEGMENT BY DAY/WEEK/MONTH from disk - roughly 1000x on the measured
  suite (SEGMENT ~2000ms of DuckDB collapses to sub-millisecond) - and gated
  per-event-name entity ROARING BITMAPS that give FUNNEL its exact step-1 count
  and shrink the step-1-and-step-2 scan set. Both are non-authoritative: a
  missing/mismatched sidecar falls back to the identical scan answer. Suite:
  `benchmarks/events/run_events.py` (small ~1M events, medium ~100M).
  Design: `events-plan.md`, `crates/fq-events/README.md`.
- fedq-server (`crates/fedq-server`): a PostgreSQL wire-protocol server
  (pgwire) over the runtime - simple and EXTENDED query protocol
  (Parse/Bind/Describe/Execute), SCRAM auth, query cancellation. One `Runtime`
  per connection on its own OS thread (the fq-exec data plane uses thread-local
  caches, so a query must run single-threaded); sources open READ-ONLY.
- fedq CLI (`crates/fedq`): clap-based. `fedq --config <yaml> --command
  "<sql>"` (or `--file`) runs one statement and prints table/csv/json; with
  neither, it opens an interactive REPL.
- CONNECTORS (fq-connectors catalog/stats tier + fq-exec data plane): DuckDB,
  Postgres, ClickHouse live-tested; MySQL full-stack but not live-verified;
  Parquet is footer-only (schema, exact row counts, row-group stats), a
  read-only file source. Iceberg is ABSENT: it needs a DataFusion-54 surface
  the pinned crate does not expose (upstream gap). Any other source kind is
  rejected loudly at registration.

## Branch, build, gates

- Branch: `rewrite-python-to-rust` (this repo). Local `main` is fast-forwarded
  to this branch (identical, 0 commits apart); nothing is pushed to `origin`
  yet. The imported fedqrs engine now lives in-tree as `fq-exec`; the standalone
  `/workspace/fedqrs` remnants are a teardown item.
- Toolchain: cargo 1.96.1 at `$HOME/.cargo/bin` (HOME=/tmp). Build env:
  `export CARGO_TARGET_DIR=/workspace/federated-query/target`.
- Before every commit: `cargo fmt --all && cargo clippy --workspace
  --all-targets && cargo test --workspace`. The full gate after a one-line
  bottom-crate change is ~7 SECONDS wall: the dev profile strips dependency
  debuginfo (`debug = "line-tables-only"` for workspace code, `debug = false`
  for deps - linking, not compiling, was the cost) and each crate links AT MOST
  ONE integration-test binary (`tests/it/main.rs` with suites as modules, or a
  single flat `tests/<name>.rs`; fq-emit and fq-events are unit-test-only in
  `src/`). Keep it that way: a new test file goes into the crate's existing
  test binary, never as a new top-level `tests/*.rs` target. ONE sanctioned
  exception: `fq-physical/tests/dim_shipping_kill_switch.rs` is its own binary
  because it mutates the process-global `FEDQ_DIM_SHIPPING` env var, which cannot
  share a binary with the parallel dim-ship tests that read it at plan time.
- Lint: `warnings = "deny"` escalates clippy pedantic; curated allows in
  `Cargo.toml [workspace.lints.clippy]`. `make fq-lint` runs the house rules
  (FQ-BUNDLED, FQ-PMCOMMENT, ...). The semantic comment gate
  (`scripts/comment-gate`) blocks commits whose comments describe project
  history instead of the code (a haiku judge over staged comment blocks); it
  installs itself on session start.
- DuckDB is NEVER compiled in cargo: `make duckdb-lib` fetches the official
  prebuilt library pinned by Cargo.lock into `.duckdb-lib/current`;
  `.cargo/config.toml` wires lib dir + rpath. fq-lint FQ-BUNDLED fails on any
  reintroduction of the `bundled` feature.
- Planning is O(metadata), enforced: `optimizer.planning_budget_ms` (default
  100) is a hard kill with a per-stage report. No off switch.
- Perf numbers come ONLY from the suite runners: `benchmarks/perf_compare/`
  (cross-engine cold+warm) and each suite's single runner.
- Test env: Postgres 17.4 on :5432 (binaries `postgres-17`, trust auth,
  started via `scripts/run-postgres.sh`). Python: `/workspace/venv-fedq`.

## PERF ROUND (measured on the TPC-DS boards)

Method: rank warm ratio AND absolute excess vs the cached DuckDB baseline at
SF10 (the target scale), profile with `FEDQRS_PROFILE=1` through the suite
runner (`--only N,M`), read plans via `Runtime::execute("EXPLAIN <sql>")`.

Landed fixes (each with unit tests pinning the behavior):

1. CTE PRODUCER SHARING (fq-plan + fq-physical). A CTE producer is now one
   `Arc<PhysicalPlan>` in the planner registry and every `PhysicalCteScan`
   clones the pointer, so step-building's pointer-keyed cte_bindings cache hits
   and the CTE body's fragments are built and executed ONCE (previously the
   producer was deep-copied into each reference, re-executing the body per
   consumer; q04's 3-channel union aggregate ran six times). q04 15227ms ->
   3802ms, q23 7589 -> 4420, q11 3132 -> 2014, q47 2549 -> 1085, and the other
   multi-consumer CTE queries dropped proportionally.
2. FLAG-UNION PASSTHROUGH NARROWING (fq-optimize ProjectionPushdown).
   Decorrelation's boolean-flag unions project every input column as explicit
   qualified passthroughs (the no-star rule); the pruner's global
   required-column collection had treated each entry as a hard requirement, so
   one flag union pinned every column of every relation and no scan pruned.
   Narrowable unions (non-distinct, branches are wrapper chains over plain
   projections, no positional consumer above) now resolve passthroughs at the
   union: collection skips plain-column entries, pruning keeps a position only
   when computed or name-referenced above. Positional consumers (set ops,
   column_names renames, outer unions) and DISTINCT shapes keep every entry.
   q45 2000ms -> 482ms. (Predicate pushdown runs first and distributes filters
   into union branches, so the narrowing sees through row-preserving wrappers:
   Filter/Sort/Limit/GroupedLimit/SingleRowGuard.)

Remaining SF10 pg-dims tail (geomean 1.25x; every item root-caused to the exact
blocking gate - these are MEASURED diagnoses, not guesses; a fix attempt for
q06's estimate REGRESSED and was reverted):

- q06-class (q06 6.9x): the date keys ARE derived (the guard-driven
  d_month_seq injection reduces date_dim to ~31 rows), but store_sales still
  reads full 28.8M because `steps/reduction.rs` reduction_filters gates
  (build_donates_whole_domain / useless_key_reduction) price date_dim at its
  STATIC 73k rows - the dynamic injection is not credited. Crediting
  post-injection cardinality (chained-NDV reduction) would cut store_sales to
  ~480k; it is a structural change to the injection-winner pre-pass with
  cross-query regression risk.
- q95: ws_wh is a duck-only CTE referenced twice, but
  `SingleSourcePushdown::try_build` has no CTE producer scope, so
  absorb_cte_ref_base declines and the 74.8M-row self-join is pulled. Fix =
  thread visible CTE definitions into pushdown and inline, so the duck-only
  subtree pushes as q16's proven inline-EXISTS shape; semi-join/DISTINCT
  semantics need care.
- q59/q02: the week-range filter arrives via a JOIN on d_week_seq;
  `CTEUnionFilterPushdown::record_consumer` only accepts a Filter parent, so
  the shared CTE body aggregates all weeks. Deriving the week set needs a
  semi-join reduction across the materialized CTE boundary.
- q04/q11/q23 residual: coordinator aggregate; the dim-ship collapse gate
  cannot yet be extended soundly (NDV-independence mis-estimates, see
  `dim-shipping-open-problems.md`).
- q39 (4.1x) is NOT a sharing bug: the shipped island executes exactly once
  (profiled); the cost is inherent (ship 3 dims + aggregate 26.5M inventory +
  self-join). q15 is NOT a bug: the (zip OR state OR cs_sales_price)
  disjunction spans BOTH sources, so the coordinator filter is CORRECT and
  there is nothing to push. Do not chase these two.

Events findings (from `benchmarks/events`):

- SEGMENT is a decisive win (the pre-aggregate sidecar): sub-millisecond vs
  DuckDB's seconds at 100M events.
- FUNNEL and PATHS beat DuckDB when the funnel/path is common (funnel common
  0.18x, paths 0.30-0.35x at 100M). The SELECTIVE-FUNNEL ROW-INDEX round has
  LANDED (per-event-name row index + funnel column projection): a selective
  funnel materializes only the step-event rows of the single sorted chunk and
  reads just the three consulted columns, cutting the selective funnel from
  3.55x to 2.48x at 100M.
- MEMORY, apportioned by measurement (fresh-process peak `ru_maxrss`, one phase
  each, over the built 100M view): every query kernel peaks at ~5.0-5.1 GB - the
  size of the one materialized chunk, no blow-up (funnel common 5061 MB, funnel
  selective 5060 MB, paths 5126 MB, segment 5059 MB). The whole-run peak was
  ENTIRELY the CREATE EVENT VIEW step, NOT a query-path cost. So streaming the
  query kernels was NOT the fix (the earlier HANDOFF item was a misdiagnosis);
  the lever is CREATE.

Events SOURCE is now Parquet (the runner exports the generated table to
`data/events_<scale>_parquet/events.parquet` and the config is a `type: parquet`
source; the DuckDB source files were removed - `references_<scale>.duckdb`
baselines stay). CREATE reads the Parquet through the read-only connector.

CREATE EVENT VIEW - the sort moved to the executor (LANDED). The in-memory
Arrow lexsort + `take_record_batch` (three full copies + a 100M-row string
gather) is gone: the materialization now runs the defining SELECT wrapped in an
ORDER BY on the contract keys, so DataFusion sorts, and `build_event_view` only
validates the roles (no column clone) and concatenates the ordered result into
the single stored chunk. Result at 100M (runner, warm 1): 6 agree | 0 differ;
CREATE 208s -> 68s (3x); peak RSS 29.3 GB -> 26.8 GB (barely moved). 986
workspace tests green.

CREATE memory + the last ~2x of time are STILL OPEN and PARKED pending a
direction decision. Apportioned at 100M: DuckDB sorts+writes the same Parquet in
12.8s; our engine's scan+sort alone is 24.6s / 13.4 GB (the ORDER BY is pushed
into the one Parquet scan step - DataFusion is ~2x DuckDB at bulk parquet+sort,
12 cores); the remaining ~43s and the memory doubling to 26.8 GB is CREATE's own
post-sort work - the `concat_batches` copy, writing a 5 GB UNCOMPRESSED IPC
chunk, and `persist_sidecars` re-normalizing all 100M rows a second time
(`build_sidecars` opens a fresh `EventStream`, re-casting the string columns).
The three candidate directions (not yet chosen): (a) cut the post-sort waste +
bound the sort so it spills (keep the engine sort; est. ~30-35s / ~8 GB, no
architectural risk); (b) a streaming materialization that pipes sorted batches
once into chunk-write + sidecar-build in one bounded pass (touches the proven
single-batch kernel/sidecar path); (c) let DuckDB do the build sort (the only
path to ~13s, since our sort floor is ~2x). Note `MEMORY_LIMIT_BYTES` is a
shared 32 GB `FairSpillPool`, so the 100M sort never spills - a per-build bounded
budget would be needed for (a)/(b).

## How to run everything

Environment for every command:
```
export PATH=$HOME/.cargo/bin:/tmp/.cargo/bin:$PATH
export HOME=/tmp
export CARGO_TARGET_DIR=/workspace/federated-query/target
```
Python interpreter: `/workspace/venv-fedq/bin/python`.

Build the engine for the benchmark harnesses (release; the benchmark symlinks
`benchmarks/{tpch,tpcds,events}/fedq.so` point at
`target/release/libfedq_py.so`):
```
cargo build --release -p fedq-py
```

TPC-DS (ONE runner; see `benchmarks/tpcds/README.md` for save-refs/generate):
```
cd benchmarks/tpcds
python run_federated_rust.py run --scale-factor 10 --pg-database tpcds_sf10 --warm-runs 1
```
Flags: `--only 4,45`, `--cold-process`, and the placement selector for the
pg-dims vs adversarial split. Hard wall budgets kill a run past 60s
(sf0.1/sf1) / 300s (sf10). Reports: `reports/rust-fed-sf<sf>.md` and
`reports/rust-fed-adversarial-sf<sf>.md`. Profiling: prefix `FEDQRS_PROFILE=1`
(per-step ms on stderr); `FEDQRS_TRACE_SQL=1` prints every pushed SQL. Plans:
`Runtime::execute("EXPLAIN <sql>")` (qualify table names first - see the
runner's `_qualify`).

TPC-H: `benchmarks/tpch/run_federated_rust.py` (same shape). Events:
`benchmarks/events/run_events.py` (build the view, then run the analyses vs the
cached DuckDB baseline). Data locations live in each suite's README.

Cross-engine perf: `benchmarks/perf_compare/compare.py` - the only sanctioned
cold/warm cross-engine numbers.

## Crate map (all built; per-crate test counts in `cargo test`)

```
fq-common (leaf)
fq-plan       -> fq-common
fq-catalog    -> fq-common
fq-connectors -> fq-catalog
fq-parse      -> fq-plan, fq-catalog       fq-emit    -> fq-plan
fq-bind       -> fq-plan, fq-catalog       fq-accel   -> storage half of MV
fq-decorrelate-> fq-plan                   fq-events  -> fq-accel
fq-optimize   -> fq-plan, fq-catalog, fq-connectors
fq-physical   -> fq-plan, fq-optimize, fq-emit
fq-exec       -> fq-plan, fq-connectors
fq-runtime    -> everything above
fedq-py / fedq-server / fedq  -> fq-runtime
```

- fq-common: config (serde-YAML, `deny_unknown_fields`), errors, tracing,
  DataType, PlanBudget, the event-statement value types.
- fq-catalog: Schema/Table/Column, the `DataSource` trait (catalog/statistics
  tier ONLY - no drivers), StatsCatalog on rusqlite.
- fq-connectors: DuckDB, Postgres, ClickHouse, MySQL, Parquet catalog/stats
  connectors implementing `DataSource`. The DATA-PLANE fetch tier lives in
  fq-exec.
- fq-plan: Expr/LogicalPlan/PhysicalPlan enums, exhaustive walkers (no `_`
  arms), typed `schema()`. CTE producers are `Arc`-shared.
- fq-parse: polyglot-sql 0.5.15 -> LogicalPlan; allowlist conversion, star
  expansion, window functions, comma joins, LIKE, WITH RECURSIVE, PIVOT. Also
  classifies the non-query statement forms (MV DDL, settings, event DDL +
  analyses) before the query pipeline.
- fq-bind: scope chain + ONE resolver (invalid query MUST raise BindError),
  aggregate hoists for HAVING/ORDER BY, star read-set expansion.
- fq-decorrelate: the always-decorrelate pass (EXISTS/IN/scalar/quantified,
  NULL-aware, disjunctive domain-union + flag-union, Neumann-Kemper dependent
  join, lateral fallback); synthetic columns carry qualifier AND DataType;
  post-pass asserts no subquery survives + validate_scope.
- fq-optimize: CostModel (honest unknowns, learned overlay, source estimates),
  StatisticsCollector, fixpoint driver, rules: CTEUnionFilter, Predicate,
  SemiJoin, EagerAgg, JoinOrdering (Selinger DP + GOO + locality), Projection
  (incl. flag-union narrowing), Aggregate, OrderBy, Limit.
- fq-emit: canonical Postgres-form SQL text + polyglot transpile boundary
  (`to_source_sql`).
- fq-physical: PhysicalPlanner (lowering, join algorithm/orientation,
  dynamic-filter marking), SingleSourcePushdown (subtree -> one RemoteQuery),
  DimShipping (9 gates), step building (PhysicalPlan -> Vec<Step> + fragments,
  CSE, reduction orientation, observations).
- fq-exec: the imported fedqrs DataFusion engine, de-pyo3'd (fragment fusion,
  spill, reductions, ship, prefetch pools, ctid-parallel reads); the step
  bridge runs the whole pipeline end to end in-process (no JSON).
- fq-runtime: config-driven Runtime, full pipeline, textual + document EXPLAIN,
  session-shared StatisticsCollector, learned-stats persistence
  (`<config>.stats.sqlite`), StageLog + planning-budget kill, and the
  statement dispatch for MV DDL / settings / event forms / substitution.
- fq-accel: the materialized-view store (ChunkStore over object_store,
  ViewCatalog, the create/refresh/drop lifecycle and the three refresh
  mechanics).
- fq-events: event-view contract, EventStream cursor, the FUNNEL/SEGMENT/PATHS
  kernels, and the derived sidecars.
- fedq-py: pyo3 cdylib; `fedq.Runtime(config_path).execute(sql)` -> Arrow C
  stream (GIL released).
- fedq-server: pgwire server (per-connection Runtime, extended protocol, SCRAM,
  cancellation).
- fedq: clap CLI (one-shot + REPL, table/csv/json output).

## NEXT

Core engine and the feature surface are complete and gated; what remains is a
short punch list plus the teardown, in rough order:

1. CREATE EVENT VIEW at 100M - time PARTLY fixed, memory PARKED (see the events
   findings above for the full apportionment). The sort now runs in the executor
   (ORDER BY on the contract keys) instead of an in-memory Arrow lexsort+gather:
   CREATE 208s -> 68s, correctness held (6 agree). STILL OPEN and awaiting a
   direction decision: peak RSS is still 26.8 GB and the last ~2x of time. The
   post-sort work (concat copy + 5 GB uncompressed IPC write + a redundant 100M-
   row sidecar re-normalize) is the ~43s / +14 GB to cut; the sort itself is
   ~2x DuckDB (24.6s vs 12.8s) and never spills (shared 32 GB pool). Three
   candidate directions in the events findings: (a) cut post-sort waste + bound
   the sort, (b) streaming materialization, (c) DuckDB build sort. Streaming the
   query kernels (~5 GB -> ~1-2 GB) is a separate, lower-value follow-up.
2. Accelerator Phase D: S3 via `object_store` config + `max_bytes`
   LRU-by-benefit eviction + tombstone sweeper (see `accelerator-plan.md`
   section 7).
3. A formal `perf_compare` gate run across all sources (parquet columns now
   have a connector, so they no longer report UNSUPPORTED).
4. MySQL LIVE verification (the connector is full-stack but unverified against
   a running server).
5. Adversarial SF10 perf tail (geomean 2.90x) - the harder split; not
   gate-blocking, pg-dims totals already beat DuckDB.
6. q14 (see UPSTREAM WATCH) is currently repaired in fq-parse; when a polyglot
   release nests trailing set operations correctly, delete the repair.
7. Python teardown: delete `federated_query/` and the `/workspace/fedqrs`
   remnants, move `lint/` rules fully into fq-lint. This awaits the
   maintainer's explicit call (no delete without approval).

## UPSTREAM WATCH

The q14 set-op repair in fq-parse `convert.rs` (SwallowedSetOp) works around a
polyglot-sql 0.5.15 mis-nesting (a trailing UNION ALL attached to a
scalar-subquery operand). When a polyglot release nests trailing set operations
correctly, delete the repair block and its tests. See commit `26d3d9f`.

DataFusion 54 mis-executes `Filter` over an outer join in a fused plan when an
intervening projection blocks its own outer-join elimination (a null-extended
row that the filter must remove survives). Our optimizer now simplifies
null-rejecting-filtered outer joins to inner joins before lowering
(fq-optimize predicate.rs), so DataFusion never receives the fragile shape;
if a future DataFusion release fixes the execution, the simplification stays
(it is a standard optimization), but the fuzzer's RIGHT-join findings are the
canary if any unsimplifiable shape resurfaces.

## Commit log (rewrite, newest first; earlier log in git history of this file)

```
8b407ef Session-scoped data plane: no cross-runtime bleed, connections release
6615543 LIKE gains a first-class plan variant carrying ESCAPE
b5055ed Coordinator AVG over DECIMAL computes in double precision
cb721cf Four engine fixes: || execution, IS DISTINCT FROM, window tail, pg probe
ece36bc Fix INTERSECT ALL / EXCEPT ALL multisets and parquet null ordering
2d722d3 Federated e2e correctness corpus: placement-matrix suite
88188b2 Sweep stale docs into a design-history digest; fix stale deferral comments
b073e92 Event view CREATE: sort in the executor; Parquet source for the events suite
ac54283 Per-event-name row index + funnel column projection for selective funnels
96eea41 HANDOFF + architecture docs rewritten against the current workspace
df93802 Event sidecars: segment pre-aggregates (decisive win) + gated entity bitmaps
38fe691 Substitution order guard walks the whole matched subtree; accelerator tutorial
3baa0bf Accelerator Phase C: automatic substitution, cost gate, benefit tracking
f2e1ce4 Events benchmark suite + user tutorial
4f69e1a PATHS analysis + TIEBREAK role: deterministic event sequences
42cba35 Event analytics extension: EVENT VIEWs, FUNNEL and SEGMENT statements (fq-events)
7e8bb21 Settings surface: one registry, SHOW SETTINGS, SET/RESET on live sessions
cac1fad Accelerator Phase B: change-key delta refresh, PK merge, source-token no-op skips
e66df52 HANDOFF: perf-tail diagnoses corrected from the measured round
26d3d9f Document the polyglot 0.5.15 set-op mis-nesting the q14 repair undoes
ee2d8e9 Materialized views (accelerator Phase A) + native parquet connector, integrated
617e90a ClickHouse + MySQL connectors: full stack (config, catalog/stats, dialect, Arrow data plane)
4be8e81 JoinOrdering skips edgeless regions; scan probes projection-independent: full sweep 99|0|0
e5e9808 fedq CLI: clap-based one-shot + REPL over the Rust runtime
c908705 Canonical float cast renders DOUBLE PRECISION: adversarial TPC-DS 99|0|0 at all scales
14a5165 accelerator-plan.md rewritten: user-controlled freshness, Arrow IPC chunks, no LSM engine
d557a1d fedq-server README: run, options, auth, behavior limits
6e29ae3 fedq-server: SCRAM auth + cancellation; test fixtures stop sharing one duckdb file
c81b768 q14 repaired + adversarial placement runner: TPC-DS pg-dims 99|0|0
78f7857 fedq-server: extended query protocol (Parse/Bind/Describe/Execute)
9e05a3e fedq-server: PostgreSQL wire-protocol server over the engine (pgwire)
6437996 WITH RECURSIVE + PIVOT: the last e2e parser gaps close, e2e_pushdown 0 failed
0e32510 accelerator-plan.md: fragment-cache design (keying, store, invalidation, phases)
252b159 Dim-ship gates in optimizer config; cross-source CTE tests as Rust e2e; orientation re-pinned
c8ae969 E2E suites: SEMI/ANTI marker classification, EXPLAIN-based dynamic-filter pins, fixture fixes
```

Older commits (fq-physical M0-M4d, fq-emit, fq-optimize M1-M4, fq-decorrelate,
fq-bind, fq-parse, connectors, the TPC-DS climb 66|0|33 -> 99|0|0, the build/
test-binary speedups, and the initial scaffold) are in this file's git history
and the full `git log`.
