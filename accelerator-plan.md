# Query accelerator plan - materialized plan fragments

Cache the RESULT of expensive plan subtrees to disk as Parquet, register those
files as a local source, and rewrite future physical plans to SCAN the cached
result instead of refetching from the remote source or recomputing the join.
This is cross-query CSE persisted to disk: the engine already shares identical
steps WITHIN one query (`fq-physical/src/steps/cse.rs`) and already ships small
dimensions INTO a fact source (dim-shipping); the accelerator does the same
sharing ACROSS queries and across process restarts.

## Sequencing prerequisite

The accelerator WAITS for R1 (physical planner + SQL emission + IR construction
in Rust), per `rust-rewrite-plan.md`. Its machinery concentrates at exactly the
layer R1 moves: fragments are WRITTEN by the engine (Rust holds every
materialized binding in `fq-exec/src/engine.rs`), READ as local Parquet scans
(the engine already reads Parquet via `connectors::fetch_parquet`), and matched
at plan time by subplan signature then substituted into the physical plan. Built
in Python first it is built twice. This doc is the design to execute once R1
lands; the `materialized_fragments` SQLite table (`fq-catalog/src/stats_catalog.rs`)
already exists as the registry slot.

## 1. What a cacheable fragment IS

A fragment is a relation that is a PURE, DETERMINISTIC function of source data:
the same source bytes always produce the same rows. Candidate physical subtrees,
in admission-priority order:

- SOURCE SCANS with folded filters/aggregates (Phase A). A `PhysicalScan` /
  `PhysicalRemoteQuery` leaf the planner pushed to one source: `SELECT ... FROM t
  WHERE <folded filter> [GROUP BY ...]`. Highest reuse (base tables plus common
  predicates recur across a workload), cheapest to key, and it is the exact work
  that dominates cold latency (the HANDOFF cold delta is source fetches).
- SHIPPED ISLANDS (Phase B). A whole pushed remote subtree rendered as one SQL
  round trip (`PhysicalRemoteQuery`, `PhysicalRemoteSetOp`) - a join+aggregate
  that already collapses to one source query. Its result relation is cacheable
  as-is.
- CTE BODIES (Phase B). A `PhysicalPlan::Cte` producer is already materialized
  ONCE and shared within a query (`fq-runtime/src/explain.rs` prints the shared
  `#id`). Persisting the body lets a later query that references the same CTE
  shape read it back.

EXCLUDED from caching (see non-goals): SEMI-JOIN-REDUCED PULLS (`InjectedScan`).
Their content depends on the DYNAMIC build-key set from the other join side, so
the result is query-specific, not a function of source data alone; v1 caches the
UNREDUCED base+filter scan instead and lets the coordinator re-reduce. Also
excluded: any subplan carrying a volatile function (`now()`, `random()`) or a
bare `LIMIT` without a total `ORDER BY` (arbitrary-row result is not a stable
function of the data).

### Keyed HOW

`subplan_signature` (`fq-optimize/src/subplan_signature.rs`) is a sha1 over
`{filters, joins, tables}` that is deliberately ALIAS- and CONSTANT-NEUTRAL. That
is correct for STATS keying (a filter's SHAPE predicts selectivity) but it is NOT
a result identity: two queries with `d_year = 1998` and `d_year = 2000` share one
signature and have DIFFERENT rows. A materialized fragment holds the rows for
SPECIFIC constants, so the fragment key must ADD, beyond the signature:

- CONSTANT VALUES. The canonical, ordered vector of literals the signature
  dropped (the same values `predicate_stats.param_bucket` would carry). Two plans
  substitute the same fragment only when their constants match exactly.
- FOLDED OUTPUT SHAPE. The signature omits projection, aggregates, group-by,
  DISTINCT, ORDER BY, and LIMIT. A base-scan fragment folds these, so the key
  captures the projected column set and the folded agg/group/distinct/limit/sort.
  (Alternative: store all base columns and permit a reuse whose requested columns
  are a SUBSET; recommended refinement for Phase B, exact-match in Phase A.)
- SOURCE DATA VERSION. A version token per base table touched (section 3). A
  fragment matches only when every token equals the token recorded at
  materialization. This is the freshness half of the key.
- CONFIG / PLACEMENT. The signature already embeds `datasource.schema.table`, so
  a datasource name repointed at a different database is caught by the
  per-datasource `source_fingerprint` (section 3) folded into the source token.
  Placement decisions (dim-shipping, join order) affect HOW rows are computed, not
  WHICH rows, so they are NOT in the key.
- DIALECT / ENGINE-SEMANTICS VERSION. The result depends on the executing
  engine's type coercion, collation, and aggregate semantics, and on the source
  dialect that evaluated a pushed filter. A single `engine_semantics_version`
  constant (bumped on any semantics-affecting change) plus the source `DsKind`
  guard against reusing a fragment a different engine version would compute
  differently.

The stored key is `fragment_key = sha1(subplan_signature || constants ||
output_shape || engine_semantics_version)`; the SOURCE TOKENS are stored as a
separate column set and checked at match time (they change without changing what
the query asked for).

## 2. Store design

- ON-DISK LAYOUT. Parquet, one file (or one directory of row-group files for a
  large fragment) per fragment, under a store dir that defaults NEXT TO the config
  and the learned catalog: `<config-stem>.fragments/` beside
  `<config-stem>.stats.sqlite` (`open_stats_catalog` already derives that stem).
  File name is `<fragment_key>.parquet`. Parquet is chosen over Arrow IPC because
  it is what the engine already reads back as a source and it self-describes its
  schema; the spill path (`SpilledBinding`, Arrow IPC) stays the in-query
  mechanism.
- CATALOG TABLE. Widen the existing `materialized_fragments` (its current PK is
  `subplan_signature` alone, insufficient per section 1) to:

  ```sql
  materialized_fragments (
    fragment_key      TEXT PRIMARY KEY,   -- section 1 composite hash
    subplan_signature TEXT NOT NULL,      -- kept, indexed, for shape lookup
    location          TEXT NOT NULL,      -- relative path under the store dir
    output_schema     TEXT NOT NULL,      -- JSON: column names + types
    source_tokens     TEXT NOT NULL,      -- JSON: {ds.schema.table: version_token}
    measured_rows     INTEGER,
    byte_size         INTEGER,            -- eviction accounting
    materialized_at   TEXT,
    last_used_at      TEXT,               -- eviction (recency)
    use_count         INTEGER DEFAULT 0,  -- benefit accounting
    cost_saved_ms     REAL DEFAULT 0      -- eviction benefit (section 6, Phase C)
  )
  ```

  It lives in the SAME SQLite file as the learned stats (one store per config),
  reusing the WAL + `synchronous=OFF` tuning already set in `StatsCatalog::open`.
- SIZE BUDGET + EVICTION. A configurable byte budget (`accelerator.max_bytes`,
  default e.g. 8 GiB). Eviction is LRU-by-BENEFIT, not plain LRU: rank live
  fragments by `cost_saved_ms / byte_size` (measured cost saved per byte held) and
  evict the lowest first until under budget. `cost_saved_ms` accrues on each reuse
  as (measured recompute cost of the shape, from the learned catalog / cost model)
  minus (measured Parquet read cost); a fragment that never earns its bytes is
  evicted first.
- WRITE PATH. Piggyback on execution. The engine ALREADY materializes every
  binding and already spills large ones to a `RefCountedTempFile`
  (`fq-exec/src/engine.rs`); a binding that the planner tagged as a fragment
  candidate is, AFTER the `Return` step and after the result is handed back,
  written to Parquet and registered. AFTER-RESULT (not write-through) is the v1
  default: writing must NEVER delay the answer, matching how `persist_observations`
  already runs off the critical path. Write-through (materialize straight to the
  fragment file during execution, skipping the temp spill) is a Phase C option for
  fragments the admission policy already decided to keep, saving one copy.

## 3. Invalidation - the hard part

The correctness rule is absolute: a fragment may substitute ONLY when it is
PROVABLY FRESH - every base table's current version token equals the token stored
at materialization. Any uncertainty (a token cannot be read, a required GUC is
off, a counter looks reset) is treated as STALE, and the plan recomputes from
source. We NEVER serve a fragment we cannot prove current. Staleness costs a slow
query; serving stale costs a wrong answer, and a wrong answer is the worst failure
mode in this engine (CLAUDE.md).

Per-connector version tokens, stating honestly what each source CAN give:

- PARQUET. Exact and cheap. Token = a hash of the listing set: per-file
  `(path, size, mtime_ns)` for every file backing the table, sorted. Parquet
  files are immutable (append/replace), so any change alters the listing and the
  token. This is the strongest case.
- DUCKDB. A single file, no exposed per-table transaction id. Token = the
  database file's `(size, mtime_ns)`. Granularity is WHOLE-FILE: any write to the
  DuckDB database invalidates every fragment derived from it. Acceptable because a
  DuckDB analytic file is typically bulk-refreshed, not trickle-updated;
  conservative-invalidate is correct, just coarse.
- POSTGRES. No cheap EXACT table-version exists; be explicit about the ladder:
  - relfilenode (`pg_class.relfilenode`): changes on TRUNCATE / VACUUM FULL /
    CLUSTER / rewriting ALTER. Catches rewrites, MISSES ordinary INSERT/UPDATE/
    DELETE. Necessary but not sufficient alone.
  - tuple counters (`pg_stat_all_tables.n_tup_ins + n_tup_upd + n_tup_del +
    n_dead_tup`): catch row-level DML, but are approximate and RESET on a stats
    reset. A reset makes the counter DECREASE; the freshness check treats any
    mismatch (not only an increase) as stale, so a reset invalidates (the safe
    direction).
  - commit timestamps (`track_commit_timestamp` GUC -> `pg_last_committed_xact`):
    EXACT when the GUC is on. Opt-in, off by default.
  v1 token = `hash(relfilenode, tuple_counter_sum)` combined with the datasource
  `source_fingerprint`. It catches rewrites and row DML in the common case; when
  `track_commit_timestamp` is available the token uses it and is exact. Document
  that strict PG freshness wants the GUC.
- `source_fingerprint` (already a column in `source_identity` and
  `materialized_fragments`) is a hash of the connection target, folded into every
  token so a datasource repointed at a different database never matches an old
  fragment.

STALENESS POLICY. The version token is the PRIMARY gate. A TTL
(`accelerator.max_age_seconds`) is a BACKSTOP only: even a token that still
matches is distrusted past the TTL (bounding the "source changed but our token
scheme could not see it" window, e.g. a DuckDB file touched without a content
change, or a PG counter-reset edge). Reading the token is itself O(metadata) and
runs inside the planning budget (CLAUDE.md rule 1); a token read that overruns
kills the plan rather than silently skipping the freshness check.

## 4. Plan integration

- WHERE. Substitution is a physical-planning rewrite that runs AFTER the physical
  planner produces the `PhysicalPlan` tree and BEFORE `build_steps`
  (`fq-physical/src/steps/mod.rs`) lowers it to steps. It walks the physical tree
  bottom-up; at each cacheable subtree it computes the `fragment_key`, looks it up
  in `materialized_fragments`, checks freshness (section 3) and the cost gate, and
  on a hit REPLACES the subtree with a `PhysicalScan` (or a new
  `PhysicalPlan::FragmentScan`) against the fragment store's Parquet source.
  Step-building then emits an ordinary `SourceScan` and the interpreter reads it
  through the existing `DsKind::Parquet` path with zero new execution machinery.
- RELATION TO CSE AND DIM-SHIPPING. In-query CSE (`steps/cse.rs`) shares identical
  steps in ONE plan; the accelerator is its persistent cross-query analogue and
  runs FIRST (a substituted subtree is smaller, so in-query CSE and dim-shipping
  then operate on the reduced tree). A substituted fragment is never itself a
  dim-shipping source (it is already local); the ship-thresholds pass sees a local
  Parquet leaf and leaves it alone.
- COST GATE. Substitute only when the learned catalog says RECOMPUTE cost > READ
  cost. Recompute cost comes from the cost model over `subplan_stats` /
  `predicate_stats` measured cardinalities (already read at this layer); read cost
  is a Parquet scan of `measured_rows`. When recompute is not measurably more
  expensive than the read, DO NOT substitute (a fresh source scan may beat a cold
  Parquet open for a tiny relation). The gate reuses the CostModel the planner
  already carries (`Runtime::cost_model`).
- KILL SWITCH. `FEDQ_ACCELERATOR=0` (env) and `accelerator.enabled: false`
  (config) both fully disable lookup, substitution, and writes, restoring the
  exact pre-accelerator plan. Default is OFF until Phase A's correctness gate
  passes, then ON.
- EXPLAIN VISIBILITY. A substituted scan MUST be labeled. `node_label`
  (`fq-runtime/src/explain.rs`) is exhaustive (no `_` arm), so a `FragmentScan`
  node forces a label; it renders as `FragmentScan [<fragment_key prefix>] rows=N
  from <ds.schema.table...>` so a plan dump shows exactly which source work was
  elided and from which cached file. An accelerated plan is never
  indistinguishable from a cold one.

## 5. Concurrency and the pgwire server

Multiple runtimes (the pgwire server being built in parallel spawns one per
session, plus batch processes) share ONE fragment store per config. Two hazards:
catalog races and file races.

- CATALOG LOCKING. SQLite WAL already gives many concurrent readers plus one
  writer. A fragment REGISTRATION is one short transaction: `INSERT OR IGNORE` on
  `fragment_key` (first writer wins; a loser that materialized the same fragment
  concurrently discards its own file). A reuse bumps `use_count` / `last_used_at`
  / `cost_saved_ms` in a separate short transaction. Eviction runs under a
  transaction that marks a `deleted_at` TOMBSTONE rather than deleting the row
  outright.
- ATOMIC PARQUET PUBLICATION. Write the fragment to a temp name in the store dir,
  fsync, then POSIX-`rename` it into `<fragment_key>.parquet` (atomic on the same
  filesystem), THEN insert the catalog row. A reader therefore never observes a
  partially written file: the catalog row exists only after the file is complete.
- EVICTION UNDER CONCURRENCY. Never `unlink` a file a peer may be mid-scan on. Two
  layers: (a) the tombstone plus a grace period - a tombstoned fragment stops
  being handed to NEW plans immediately but its file is unlinked only after the
  grace window; (b) POSIX semantics - a reader that already opened the file keeps
  reading after unlink (the inode survives until the last fd closes), so even a
  racing unlink cannot corrupt an in-flight scan. A single background sweeper
  (whichever runtime wins an advisory lock) performs unlinks; other runtimes only
  tombstone.

## 6. Phased plan with gates

Each phase stays behind the existing 98|0|1 TPC-DS + 22/22 TPC-H correctness
harness AND the perf_compare suite (CLAUDE.md rule 3: numbers only from
`benchmarks/perf_compare`). No phase advances without its gate.

- PHASE A - exact source-scan reuse behind the kill switch.
  Scope: cache SOURCE SCANS with folded filters/aggregates only (section 1 first
  bullet), exact key match (constants + output shape + fresh tokens). Write
  after-result; substitute at physical planning.
  Test strategy: a new harness mode runs each TPC-DS/TPC-H query TWICE in one
  process - first COLD (empty store, populates it), then ACCELERATED (store warm) -
  and asserts the two answers are BYTE-IDENTICAL (same rows, same order under the
  query's ORDER BY). A signature-stability / key-collision check (distinct
  constants must not collide). A freshness test that MUTATES a source between runs
  and asserts the mutated query recomputes (no stale hit).
  Gate to advance: 98|0|1 unchanged with the accelerator ON; accelerated ==
  cold for every query; a measurable reuse HIT RATE on the second run; perf_compare
  warm totals no worse than baseline (cold Parquet opens must not regress the warm
  board).

- PHASE B - aggregate / island / CTE-body fragments.
  Scope: extend candidates to `RemoteQuery`/`RemoteSetOp` islands and `Cte`
  bodies; add subset-column matching (a reuse needs a SUBSET of stored columns).
  Test strategy: the Phase A twice-in-one-process identity harness, now exercising
  island and CTE shapes; the q23/q70 shared-subplan family (known CSE-sensitive)
  checked for reuse.
  Gate: correctness parity; NET-POSITIVE perf on TPC-DS totals at SF1 and SF10 via
  perf_compare (the accelerator must beat the already-strong cold board, not just
  break even).

- PHASE C - eviction + benefit tracking.
  Scope: enforce `max_bytes`, populate `byte_size` / `cost_saved_ms` / `use_count`,
  implement LRU-by-benefit eviction and the tombstone sweeper; optional
  write-through for admitted fragments.
  Test strategy: a store-pressure test that materializes past the budget and
  asserts the store stays under budget, that the lowest-benefit fragments are
  evicted first, and that no in-flight scan is corrupted by a concurrent evict
  (spawn a reader across an eviction).
  Gate: bounded store size under sustained load; no eviction thrash (a reused
  fragment is not evicted then re-materialized in a loop); benefit accounting
  matches measured savings.

- PHASE D - cross-query admission policy.
  Scope: do NOT materialize every eligible subtree - materialize only a shape SEEN
  BEFORE (the learned catalog already records `observation_count` per signature)
  and only when the cost gate predicts the fragment will earn its bytes. This is
  the muscle that keeps the store from filling with one-shot fragments.
  Test strategy: replay a realistic mixed workload; measure admission PRECISION
  (fraction of materialized fragments later reused) and store bloat.
  Gate: high admission precision (materialized fragments are actually reused), no
  unbounded growth of one-shot fragments, and overall workload latency improved
  over Phase C's always-admit behavior.

## 7. Non-goals for v1

- No SUBSUMPTION or partial/range matching: only exact `fragment_key` hits. A
  fragment for `d_year = 1998` never serves `d_year IN (1998, 1999)` and never
  serves a superset predicate. (Subsumption is a later loosening, gated on
  measured demand, exactly as the signature scheme loosens.)
- No INCREMENTAL / delta maintenance: an invalidated fragment is dropped and
  recomputed from source, never refreshed by applying source deltas.
- No caching of SEMI-JOIN-REDUCED pulls (`InjectedScan`): their content depends on
  the dynamic build-key set, so they are not a function of source data alone.
- No caching of subplans with VOLATILE functions or unordered `LIMIT`
  (non-deterministic results).
- No CROSS-CONFIG sharing: one store per config, keyed by that config's datasource
  identities. No global/shared cache across configs.
- No MID-QUERY re-optimization or adaptive re-materialization: substitution is a
  static plan-time decision; the runtime does not swap in a fragment mid-execution.
- No REMOTE / distributed fragment store: local filesystem only. A network object
  store is a later concern for multi-node deployments.
- No caching of the FINAL result set (that is plan/result caching, a separate
  latency-floor feature keyed on normalized SQL + catalog version, per the
  adaptive-catalog plan); the accelerator caches INTERNAL subtrees only.
