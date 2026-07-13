# Query accelerator plan - cached scan fragments with user-controlled refresh

Cache the RESULT of a scan to local disk as Arrow IPC chunks, register those
chunks as a source, and rewrite future physical plans to read the cached result
instead of refetching from the remote source. This is cross-query CSE persisted
to disk: the engine already shares identical steps WITHIN one query
(`fq-physical/src/steps/cse.rs`) and already ships small dimensions INTO a fact
source (dim-shipping); the accelerator does the same sharing ACROSS queries and
across process restarts.

Freshness is NOT automatic. Serving trusts the last pull. Bringing a fragment up
to date is an EXPLICIT user REFRESH operation, never a check on the query path.
This replaces the earlier automatic-invalidation design (per-query token gating,
an LSM storage engine); that design is deleted, its rejected pieces in section 5.

## Sequencing prerequisite

The accelerator WAITS for R1 (physical planner + SQL emission + IR construction
in Rust), per `rust-rewrite-plan.md`. Its machinery concentrates at exactly the
layer R1 moves: fragments are WRITTEN by the engine (Rust holds every
materialized binding in `fq-exec/src/engine.rs`), READ as local Arrow IPC scans,
and matched at plan time by scan identity then substituted into the physical
plan. Built in Python first it is built twice. The `materialized_fragments`
SQLite table (`fq-catalog/src/stats_catalog.rs`) already exists as the slot.

## 1. What a fragment IS (cacheable)

A fragment is the cached result of a SCAN that is a PURE, DETERMINISTIC function
of source data: the same source bytes always produce the same rows. At plan time
the engine already knows every scan in the tree, so every fragment corresponds to
a scan the planner emits. Candidate physical subtrees, in admission-priority
order:

- SOURCE SCANS with folded filters/aggregates (Phase A). A `PhysicalScan` /
  `PhysicalRemoteQuery` leaf the planner pushed to one source: `SELECT ... FROM t
  WHERE <folded filter> [GROUP BY ...]`. Highest reuse (base tables plus common
  predicates recur across a workload), cheapest to key, and it is the exact work
  that dominates cold latency (the cold delta is source fetches).
- SHIPPED ISLANDS (Phase B). A whole pushed remote subtree rendered as one SQL
  round trip (`PhysicalRemoteQuery`, `PhysicalRemoteSetOp`) - a join+aggregate
  that already collapses to one source query. Its result relation is cacheable
  as-is.
- CTE BODIES (Phase B). A `PhysicalPlan::Cte` producer is already materialized
  ONCE and shared within a query. Persisting the body lets a later query that
  references the same CTE shape read it back.

NOT cacheable (see non-goals):

- SEMI-JOIN-REDUCED PULLS (`InjectedScan`). Their content depends on the DYNAMIC
  build-key set from the other join side, so the result is query-specific, not a
  function of source data alone; the accelerator caches the UNREDUCED base+filter
  scan instead and lets the coordinator re-reduce.
- Any subplan carrying a VOLATILE function (`now()`, `random()`), or a bare
  `LIMIT` without a total `ORDER BY` (an arbitrary-row result is not a stable
  function of the data).

### Keyed HOW

`subplan_signature` (`fq-optimize/src/subplan_signature.rs`) is a sha1 over
`{filters, joins, tables}` that is deliberately ALIAS- and CONSTANT-NEUTRAL. That
is correct for STATS keying (a filter's SHAPE predicts selectivity) but it is NOT
a result identity: two queries with `d_year = 1998` and `d_year = 2000` share one
signature and have DIFFERENT rows. A fragment holds the rows for SPECIFIC
constants, so the fragment key ADDS, beyond the signature:

- CONSTANT VALUES. The canonical, ordered vector of literals the signature
  dropped (the same values `predicate_stats.param_bucket` would carry). Two plans
  substitute the same fragment only when their constants match exactly.
- FOLDED OUTPUT SHAPE. The signature omits projection, aggregates, group-by,
  DISTINCT, ORDER BY, and LIMIT. A base-scan fragment folds these, so the key
  captures the projected column set and the folded agg/group/distinct/limit/sort.
  (Phase A is exact-match on the column set; subset matching - a reuse whose
  requested columns are a SUBSET of the stored set - is a Phase B refinement.)
- ENGINE-SEMANTICS VERSION. The result depends on the executing engine's type
  coercion, collation, and aggregate semantics, and on the source dialect that
  evaluated a pushed filter. A single `engine_semantics_version` constant (bumped
  on any semantics-affecting change) plus the source `DsKind` guard against
  reusing a fragment a different engine version would compute differently.
- CONFIG / PLACEMENT. The signature already embeds `datasource.schema.table`, so a
  datasource repointed at a different database is caught by the per-datasource
  `source_fingerprint` folded into the key. Placement decisions (dim-shipping,
  join order) affect HOW rows are computed, not WHICH rows, so they are NOT in the
  key.

The stored key is `fragment_key = sha1(subplan_signature || constants ||
output_shape || engine_semantics_version || source_fingerprint)`. Note what the
key does NOT contain: a source DATA version. Data version is a freshness concern,
and freshness is user-controlled (section 2), so it never gates a match.

## 2. Freshness - user-controlled REFRESH

The correctness model is deliberately simple: SERVING TRUSTS THE LAST PULL. A
fragment that matches by key is substituted with no data-version check on the
query path. Bringing a fragment up to date is an explicit user
action. This trades automatic freshness for two things the engine values more: a
query path that reads catalogs only (CLAUDE.md rule 1, O(metadata) planning) and
a freshness contract the operator controls and can reason about.

- REFRESH OPERATION. A `REFRESH` verb exposed through the pgwire server and the
  CLI: `REFRESH FRAGMENTS` (all), `REFRESH FRAGMENTS FOR <datasource>[.<table>]`
  (scoped). It pulls changed data per fragment and republishes chunks - the ONLY
  thing that moves a fragment's contents forward; nothing on the query path does.
- EXPLAIN ALWAYS LABELS. Because serving trusts the last pull, the operator must
  be able to SEE that a scan came from cache. A substituted scan always renders as
  a `FragmentScan` node (section 4), so a plan dump shows exactly which source
  work was elided and from which cached fragment - an accelerated plan is never
  indistinguishable from a cold one.

### Version tokens - stored at pull, read only at refresh

Per-connector version tokens still exist, but their role is narrow: they let
REFRESH SKIP a no-op pull. When a fragment is materialized or refreshed, the
current token of each base table is recorded in the catalog. At the NEXT refresh,
the connector reads the current token; if it equals the stored token, the source
has not changed and REFRESH skips the pull entirely. Tokens are NEVER read on the
query path.

- PARQUET. Exact and cheap. Token = a hash of the listing set: per-file
  `(path, size, mtime_ns)` for every file backing the table, sorted. Parquet
  files are immutable, so any change alters the listing and the token.
- DUCKDB. Token = the database file's `(size, mtime_ns)`. Whole-file granularity:
  any write to the DuckDB database moves the token, so any fragment over it is
  re-pulled on the next refresh. Acceptable because a DuckDB analytic file is
  bulk-refreshed, not trickle-updated.
- POSTGRES. Token = `hash(relfilenode, tuple_counter_sum)` where the counters are
  `pg_stat_all_tables.n_tup_ins + n_tup_upd + n_tup_del + n_dead_tup`; when
  `track_commit_timestamp` is on the token uses `pg_last_committed_xact` and is
  exact. relfilenode alone catches rewrites (TRUNCATE / VACUUM FULL / CLUSTER) but
  MISSES ordinary DML, so the counter sum is required. Any mismatch (a counter
  reset DECREASES the sum) is treated as changed, so REFRESH re-pulls - the safe
  direction for a skip decision.

A token that cannot be read simply means REFRESH does not skip: it pulls. The
token is a refresh optimization, never a correctness gate, so an unreadable token
costs work, never a wrong answer.

### REFRESH mechanics - delta vs whole

How REFRESH pulls depends on whether the datasource config declares a CHANGE KEY
for the table:

- WITH A CHANGE KEY. The config names either a monotonic column (an
  `updated_at`-style timestamp or a monotonic id) or the table's PRIMARY KEY.
  - Monotonic column: REFRESH remembers the max value it last pulled and pulls
    only rows above it (`WHERE updated_at > <watermark>`), then APPENDS them as new
    chunks. Cheapest path; correct only for append/monotonic-update tables, which
    is exactly what the declaration asserts.
  - Primary key: REFRESH pulls changed rows and MERGES them by key - a changed row
    replaces the old row in whichever chunk holds it, deletes are applied by key.
    Merge rewrites only the affected chunks (section 4).
- WITHOUT A CHANGE KEY. REFRESH re-pulls the fragment WHOLE and republishes all
  chunks. Always correct, never assumes anything about the source; the fallback
  when the operator has not declared how the table changes.

The change-key declaration lives in the datasource config next to the connection
details, e.g.:

```yaml
datasources:
  warehouse:
    kind: postgres
    change_keys:
      public.sales:  { column: updated_at }   # monotonic -> delta append
      public.customer: { primary_key: c_custkey }  # PK -> merge
      # tables not listed re-pull whole on refresh
```

## 3. Store design

- ONE FRAGMENT = ONE DIRECTORY of Arrow IPC chunk files. Each chunk is a framed
  Arrow IPC file holding a bounded number of record batches. Writing a chunk is
  Arrow framing plus a buffer copy; reading is mmap/zero-copy straight into
  DataFusion arrays, so there is no decode step between the cache and execution.
  The fragment directory defaults NEXT TO the config and the learned catalog:
  `<config-stem>.fragments/<fragment_key>/` beside `<config-stem>.stats.sqlite`
  (`open_stats_catalog` already derives that stem). Chunk files are
  `chunk-<n>.arrow`.
- MERGE-ON-REFRESH REWRITES ONLY AFFECTED CHUNKS. An append adds new
  `chunk-<n>.arrow` files without touching existing ones. A PK merge rewrites only
  the chunks that hold a changed or deleted key; untouched chunks stay byte-for-
  byte as they were. The chunk list in the catalog is the source of truth for
  which files compose a fragment.
- ALL FILE IO BEHIND `object_store`. Every read, write, rename, and delete goes
  through the `object_store` abstraction. Local filesystem is the only backend for
  now; S3 (on the roadmap) becomes a config change - a bucket URL and credentials
  - with no change to the chunk model. The same chunk abstraction admits a later
  per-fragment format flip to Parquet where S3 STORAGE COST matters (Parquet's
  compression is worth the decode cost when bytes are billed); Arrow IPC stays the
  default for the mmap/zero-copy local read path.
- SQLITE HOLDS FRAGMENT METADATA. The existing learned-stats catalog
  (`fq-catalog/src/stats_catalog.rs`, WAL + `synchronous=OFF` already tuned in
  `StatsCatalog::open`) carries the fragment registry in the SAME file, one store
  per config. Widen `materialized_fragments` (its current PK `subplan_signature`
  alone is insufficient per section 1) to:

  ```sql
  materialized_fragments (
    fragment_key       TEXT PRIMARY KEY,   -- section 1 composite hash
    subplan_signature  TEXT NOT NULL,      -- kept, indexed, for shape lookup
    location           TEXT NOT NULL,      -- fragment dir, relative to store dir
    chunk_list         TEXT NOT NULL,      -- JSON: ordered chunk file names
    output_schema      TEXT NOT NULL,      -- JSON: column names + types
    source_tokens      TEXT NOT NULL,      -- JSON: {ds.schema.table: token} at pull
    change_key         TEXT,               -- JSON: declared change key + watermark
    measured_rows      INTEGER,
    byte_size          INTEGER,            -- eviction accounting
    materialized_at    TEXT,
    refreshed_at       TEXT,               -- last successful REFRESH
    last_used_at       TEXT,               -- eviction (recency)
    use_count          INTEGER DEFAULT 0,  -- benefit accounting
    cost_saved_ms      REAL DEFAULT 0      -- eviction benefit (section 6, Phase C)
  )
  ```

- SIZE BUDGET + EVICTION (Phase D). A configurable byte budget
  (`accelerator.max_bytes`, default e.g. 8 GiB). Eviction is LRU-by-BENEFIT: rank
  live fragments by `cost_saved_ms / byte_size` and evict the lowest first until
  under budget. `cost_saved_ms` accrues on each reuse as measured recompute cost
  minus measured cached-read cost; a fragment that never earns its bytes goes first.
- WRITE PATH. Piggyback on execution, AFTER-RESULT. The engine already
  materializes every binding and spills large ones to Arrow IPC
  (`SpilledBinding`, `fq-exec/src/engine.rs`). A binding the planner tagged as a
  fragment candidate is, AFTER the `Return` step and after the result is handed
  back, written as chunks and registered. Writing must NEVER delay the answer,
  matching how `persist_observations` runs off the critical path. Write-through
  (frame chunks straight to the fragment dir during execution, skipping the temp
  spill) is a later option for candidates the admission policy keeps, saving a copy.

## 4. Plan integration

Unchanged in spirit from the pre-existing design: substitution sits between
physical planning and step-building, as a labeled node, cost-gated, behind a kill
switch.

- WHERE. Substitution is a physical-planning rewrite that runs AFTER the physical
  planner produces the `PhysicalPlan` tree and BEFORE `build_steps`
  (`fq-physical/src/steps/mod.rs`). It walks the physical tree bottom-up; at each
  cacheable scan it computes the `fragment_key`, looks it up in
  `materialized_fragments`, applies the cost gate, and on a hit REPLACES the
  subtree with a `PhysicalPlan::FragmentScan` over the fragment's chunk directory.
  Step-building then emits a `SourceScan` that the interpreter reads through the
  existing Arrow IPC path with zero new execution machinery.
- COST GATE. Substitute only when the learned catalog says RECOMPUTE cost > cached
  READ cost. Recompute cost comes from the cost model over `subplan_stats` /
  `predicate_stats` measured cardinalities (already read at this layer); read cost
  is a scan of `measured_rows` over the chunk directory. When recompute is not
  measurably more expensive than the read, DO NOT substitute (a fresh source scan
  can beat a cold cache open for a tiny relation). The gate reuses the CostModel
  the planner already carries (`Runtime::cost_model`).
- RELATION TO CSE AND DIM-SHIPPING. In-query CSE (`steps/cse.rs`) shares identical
  steps within ONE plan; the accelerator is its persistent cross-query analogue
  and runs FIRST (a substituted subtree is smaller, so in-query CSE and
  dim-shipping then operate on the reduced tree). A substituted fragment is never
  itself a dim-shipping source (it is already local); the ship-thresholds pass
  sees a local leaf and leaves it alone.
- KILL SWITCH. `FEDQ_ACCELERATOR=0` (env) and `accelerator.enabled: false`
  (config) both fully disable lookup, substitution, and writes, restoring the
  exact pre-accelerator plan. Default is OFF until Phase A's correctness gate
  passes, then ON.
- EXPLAIN VISIBILITY. `node_label` (`fq-runtime/src/explain.rs`) is exhaustive (no
  `_` arm), so a `FragmentScan` node forces a label; it renders as `FragmentScan
  [<fragment_key prefix>] rows=N refreshed=<ts> from <ds.schema.table...>`. The
  `refreshed=` field surfaces the freshness the operator is trusting (section 2).

## 5. Store engine - decision and rejected alternatives

The store is Arrow IPC chunks under `object_store`, sqlite for metadata. Rejected:

- fjall / SlateDB / any embedded LSM key-value engine. An LSM tree is built for
  many small keyed point writes with background compaction. A fragment is the
  opposite access pattern: a bounded number of LARGE columnar blobs read by full
  scan. Storing Arrow batches as KV values imposes a row/KV layout and a
  serialize/deserialize hop between the store and DataFusion's columnar arrays,
  defeating the mmap/zero-copy read that is the whole point. The LSM structure's
  real benefits - fast overwrite, tombstones, compaction of superseded versions -
  were needed by the DELETED automatic-invalidation design, where fragments were
  overwritten constantly as sources drifted. Under user-controlled REFRESH,
  fragments change only on an explicit operator action, so that machinery has no
  job. A directory of immutable chunk files plus a sqlite metadata row gives
  atomic publication (section 6) without an embedded engine.
- Parquet as the default fragment format. Parquet self-describes and compresses,
  but its read requires a decode step; Arrow IPC memory-maps straight into the
  arrays the executor already uses. Parquet is retained only as a LATER per-
  fragment flip for S3, where storage cost outranks local read latency (section
  3). The chunk abstraction is format-agnostic, so the flip is contained.

## 6. Concurrency and the pgwire server

Multiple runtimes (the pgwire server spawns one per session, plus batch
processes) share ONE fragment store per config. Two hazards: catalog races and
file races.

- CATALOG LOCKING. SQLite WAL already gives many concurrent readers plus one
  writer. A fragment REGISTRATION is one short transaction: `INSERT OR IGNORE` on
  `fragment_key` (first writer wins; a loser that materialized the same fragment
  concurrently discards its own chunks). A reuse bumps `use_count` /
  `last_used_at` / `cost_saved_ms` in a separate short transaction. A REFRESH
  updates `chunk_list`, `source_tokens`, `change_key` watermark, and
  `refreshed_at` in one transaction AFTER the new chunks are published.
- ATOMIC CHUNK PUBLICATION. Write each new chunk to a temp name in the fragment
  dir, fsync, then `rename` it into `chunk-<n>.arrow` (atomic on the same
  filesystem via `object_store`). The catalog `chunk_list` is updated to point at
  the new set only AFTER every chunk is renamed into place, so a reader that reads
  the catalog row then opens the listed chunks never observes a partial write. A
  REFRESH that appends adds chunks then extends the list; a merge writes the
  rewritten chunks under temp names, renames them, then swaps the list in one
  transaction - superseded chunk files are unlinked only after the swap.
- EVICTION / STALE-CHUNK CLEANUP UNDER CONCURRENCY. Never `unlink` a chunk a peer
  may be mid-scan on. Two layers: (a) a `deleted_at` tombstone plus a grace period
  - a tombstoned fragment (or a superseded chunk set) stops being handed to NEW
  plans immediately, but its files are unlinked only after the grace window; (b)
  POSIX semantics - a reader that already opened a file keeps reading after unlink
  (the inode survives until the last fd closes), so even a racing unlink cannot
  corrupt an in-flight scan. A single background sweeper (whichever runtime wins an
  advisory lock) performs unlinks; other runtimes only tombstone.

## 7. Phased plan with gates

Each phase stays behind the existing 98|0|1 TPC-DS + 22/22 TPC-H correctness
harness AND the perf_compare suite (CLAUDE.md rule 3: numbers only from
`benchmarks/perf_compare`). No phase advances without its gate. Quality over
speed - no phase is rushed to hit the next.

- PHASE A - store + catalog + manual save/refresh of whole-scan fragments.
  Scope: the store (Arrow IPC chunk dirs under `object_store`, local backend), the
  widened `materialized_fragments` catalog, after-result writes for SOURCE SCANS
  with folded filters/aggregates (section 1 first bullet, exact key match), and a
  `REFRESH FRAGMENTS` that re-pulls WHOLE (no change key yet). Substitution at
  physical planning behind the kill switch.
  Test strategy: a harness mode runs each TPC-DS/TPC-H query TWICE in one process
  - first COLD (empty store, populates it), then ACCELERATED (store warm) - and
  asserts the two answers are BYTE-IDENTICAL (same rows, same order under the
  query's ORDER BY). A key-collision check (distinct constants must not collide). A
  freshness test that MUTATES a source, runs the query (asserts it still serves the
  OLD cached rows - serving trusts the last pull), then `REFRESH`es and asserts the
  new rows appear.
  Gate: 98|0|1 unchanged with the accelerator ON; accelerated == cold for every
  query; a measurable reuse HIT RATE on the second run; perf_compare warm totals no
  worse than baseline (cold cache opens must not regress the warm board).

- PHASE B - change-key delta refresh + merge.
  Scope: read the datasource `change_keys` config; monotonic-column delta append
  and primary-key merge (section 2), rewriting only affected chunks; extend
  candidates to `RemoteQuery`/`RemoteSetOp` islands and `Cte` bodies with
  subset-column matching.
  Test strategy: the Phase A identity harness; plus a delta harness that inserts,
  updates, and deletes rows in a source, runs `REFRESH`, and asserts the refreshed
  fragment equals a full re-pull (delta correctness) while touching fewer chunks
  than a whole re-pull (delta efficiency); the q23/q70 shared-subplan family
  checked for reuse.
  Gate: correctness parity (delta refresh == whole re-pull, byte-identical); a
  refresh that changes few rows rewrites few chunks; NET-POSITIVE perf on TPC-DS
  totals at SF1 and SF10 via perf_compare.

- PHASE C - substitution cost gate + benefit tracking.
  Scope: the cost gate (section 4) fully enforced - substitute only when recompute
  > cached read; populate and use `use_count` / `cost_saved_ms` per fragment;
  admission policy that materializes a shape only when the learned catalog has SEEN
  it before (`observation_count`) and the gate predicts it earns its bytes.
  Test strategy: replay a mixed workload; measure admission PRECISION (fraction of
  materialized fragments later reused) and that the gate declines fragments cheaper
  to recompute than to read.
  Gate: high admission precision; the gate never substitutes a fragment slower than
  its source scan; overall workload latency improved over always-admit.

- PHASE D - S3 via object_store config + eviction.
  Scope: an S3 backend selected purely by `object_store` config (bucket + creds),
  no chunk-model change; enforce `max_bytes` with LRU-by-benefit eviction and the
  tombstone sweeper (section 6); optional per-fragment Parquet flip for S3 storage
  cost.
  Test strategy: a store-pressure test (materialize past budget, assert bounded
  size, lowest-benefit evicted first, no in-flight scan corrupted by a concurrent
  evict); an S3 round-trip test against a local S3-compatible endpoint asserting
  byte-identical results to the local backend.
  Gate: bounded store size under sustained load; no eviction thrash; S3 results
  byte-identical to local; benefit accounting matches measured savings.

## 8. Non-goals for v1

- No AUTOMATIC freshness. There is no query-path token check and no background
  invalidation. Fragments advance only on explicit `REFRESH`; serving trusts the
  last pull. A deliberate contract, not a gap.
- No SUBSUMPTION or partial/range matching: only exact `fragment_key` hits. A
  fragment for `d_year = 1998` never serves `d_year IN (1998, 1999)` and never
  serves a superset predicate. (Subset-COLUMN matching lands in Phase B; predicate
  subsumption is a later loosening, gated on measured demand.)
- No caching of SEMI-JOIN-REDUCED pulls (`InjectedScan`): their content depends on
  the dynamic build-key set, so they are not a function of source data alone.
- No caching of subplans with VOLATILE functions or unordered `LIMIT`
  (non-deterministic results).
- No embedded LSM / KV store engine (section 5) and no general delta maintenance
  beyond the declared change-key paths: a fragment with no change key re-pulls
  whole on refresh.
- No CROSS-CONFIG sharing: one store per config, keyed by that config's datasource
  identities. No global cache across configs.
- No MID-QUERY re-optimization or adaptive re-materialization: substitution is a
  static plan-time decision; the runtime does not swap in a fragment mid-execution.
- No caching of the FINAL result set (that is plan/result caching, a separate
  feature keyed on normalized SQL, per the adaptive-catalog plan); the accelerator
  caches SCAN results only.
