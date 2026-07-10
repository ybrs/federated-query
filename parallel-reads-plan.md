# Parallel reads - plan

Overlap INDEPENDENT remote fetches across IR steps. The engine's step loop
(`fedqrs engine.rs::execute`) is strictly sequential today: `for step in
&ir.steps` blocks on each fetch, so three independent fact reads serialize
even though nothing orders them. This plan adds a dependency-driven scheduler
that runs ready READ steps concurrently while preserving every real ordering
(dynamic-filter keys before the injected scan, ships before the island).

## What parallelism exists TODAY (verified in code)

- WITHIN one scan: a big plain Postgres table read is ctid-partitioned across
  a persistent 8-worker thread pool (`connectors::fetch_parallel`,
  `PARALLEL_WORKERS`), each worker holding its own pooled connections (ADBC
  handles are not Send, so connections live inside workers - the Job/mpsc
  pattern this plan reuses).
- WITHIN a merge region: lazy fusion composes fragments into one DataFusion
  execution, which is internally multi-threaded (tokio).
- ACROSS steps: NOTHING. The step loop is serial; "serial steps" has been an
  open item since the fedpgduck gap diagnosis. No prior cross-step work
  exists - the remembered parallelization was the within-scan ctid pool.

## Measured motivation (SF10, analyzed, FEDQRS_PROFILE)

- q78: ss/ws/cs channel injections are mutually independent and serialize at
  423 + 113 + 180 = 716ms; parallel = max = ~423ms.
- q33: THREE whole chains (channel-fact injected scan -> collect item keys ->
  item injected scan) are mutually independent, plus two independent leading
  dim reads; ~390ms of a 484ms total is serialized reads; parallel = the
  slowest chain (~150ms). The q16/q54/q94/q33/q56/q60 sub-second family -
  where the geomean tail lives - has this shape.
- q39 (ships): the scan+ship pairs per dimension are independent and cheap
  (ms); the win is small but the ordering constraint (ships before island)
  is the correctness-critical case the design must model.

## Dependencies: what actually orders steps

Explicit (already in the IR as binding references):
- CollectDistinct.input, InjectedScan.keys_from + extra_injections[].keys_from,
  Merge.inputs, Return.input, Ship.input. A reduction chain
  (dim scan -> collect keys -> injected fact scan) is ordered by these edges
  naturally - the dynamic-filter sequencing costs nothing to preserve.

Implicit (NOT visible in bindings; must become explicit or conservative):
1. SHIP VISIBILITY: a Ship creates a temp table on a PINNED connection
   (`PINNED_DUCK` / the pooled pg connection); a later scan on that
   datasource that references the shipped table must run AFTER the ship AND
   on the SAME connection. Conservative rule for Phase A: any scan on a
   datasource that is the target of any ship in this plan is NOT
   parallelized (runs on the driving thread, in step order). Exact edges
   (which scan reads which temp tables) can come later via an IR field.
2. CONNECTION AFFINITY: `PG_CACHE` is thread-local by design; pg-shipped
   temp tables live on the driving thread's connection. Parallel workers use
   their OWN connections (the ctid pool already does), which is correct for
   plain reads and forbidden for shipped-table reads - covered by rule 1.
3. CSE-shared steps (`_emit_step_once` dedup) are one step with many
   consumers - binding availability covers them.

## Design

A READY-SET SCHEDULER in `engine.rs::execute`, replacing the plain loop:

- Build the step DAG once from the explicit binding edges + the conservative
  ship rule. Steps classify as READ (SourceScan, InjectedScan - remote I/O,
  parallelizable) or LOCAL (CollectDistinct, Merge, Ship, Return - they need
  `&mut bindings`/tokio/pinned connections and stay on the driving thread).
- The driving thread dispatches every READY read to a step-worker pool
  (extend the existing persistent Job/mpsc pool - workers already keep
  per-worker pg connections; duck workers fetch through `duck_cursor`'s
  `try_clone`, which is concurrent-reader safe; parquet through the shared
  DataFusion context). Results return as `Batches` over channels; the
  driving thread stores them into `bindings`, records observations, logs the
  profile line, and advances the ready set. LOCAL steps run inline when
  ready, in IR order among themselves.
- BINDINGS STAY SINGLE-THREADED: only the driving thread touches the map,
  the use-count lifecycle, the resident-memory budget, and the spill
  machinery - none of that becomes concurrent. The only new memory is the
  bounded set of in-flight results.
- CONCURRENCY CAPS, per datasource kind: pg reads cap at the connection
  budget (shared with the ctid pool - a parallel ctid scan already fans to 8
  connections, so the cap is on total outstanding pg work, not steps);
  duck at a small clone count; parquet at DataFusion's discretion. An
  in-flight cap bounds peak RSS.
- DETERMINISM: batches land under their own binding regardless of completion
  order; nothing downstream observes cross-binding timing. Profile output
  keeps step lines (now with queue/run split).
- Kill switch: FEDQRS_PARALLEL_STEPS=0 restores the sequential loop.

## Phases

- A. Scheduler + parallel plain SourceScans only. InjectedScan, Ship and
  every scan on a ship-target datasource stay on the driving thread.
  DONE 2026-07-10 (fedqrs 2624dea). Shape as landed: no general DAG yet -
  plain SourceScans have no dependencies, so ALL prefetchable scans dispatch
  to a persistent 6-worker pool (STEP_WORKERS) before the loop, which
  consumes each receiver at its step index; workers go through the same
  connectors::fetch as the sequential path (thread-correct by construction,
  per-worker connections pool across queries). Errors defer to the
  sequential arm (identical loud failure); kill switch
  FEDQRS_PARALLEL_STEPS=0. MEASURED at SF10: q23 5704 -> 4991ms (its 1s
  whole-fact duck read overlaps the 688ms injected scan), q02 -99ms,
  q88 -75ms, tally totals 63.7s (0.89x, geomean 1.34x from 1.38x); q33/q78
  are UNCHANGED as predicted - their serialized reads are injection chains
  (Phase B). One measured caution for B: two CPU-heavy pushed-aggregate
  DuckDB scans running concurrently contend (duck already parallelizes each
  internally) - a per-source concurrency policy may be needed when
  injected scans join the pool.
  Gates PASSED: suite 1313; 99|0|0 at SF0.1/SF1/SF10 pg-dims AND
  adversarial; TPC-H 22/22 at 1.55x. (The adversarial run also surfaced a
  PRE-EXISTING harness hole - the timing oracle ground past the child
  timeout on q85 - fixed by an oracle interrupt budget, federated-query
  023cb5d.)
- B. Parallel InjectedScans. DONE 2026-07-10 (fedqrs 8a0d41b), POSTGRES-ONLY
  after measurement. Delivery-path audit result: inline-IN is
  connection-free; temp-table delivery is per-CONNECTION state, so each
  worker's own connection ISOLATES the fixed fedq_dyn_keys name (pg session
  temp tables, duck clone-scoped temp tables) - no pinning needed; the
  unselective ctid fallback crosses to a different pool one-way (no cycle).
  THE MEASURED SURPRISE: the predicted q78/q33 wins did NOT materialize on
  this box - a DuckDB injected scan is in-process and already saturates
  every core, so concurrent duck semi-joins pay contention for nothing
  (q46 +82ms, q78 +60ms) and duck injections now stay sequential; pg
  injections overlap cleanly but measure neutral against a LOOPBACK pg
  (zero network latency is exactly what overlap hides). The mechanism's
  value scales with real source latency; local gates are no-regression:
  SF10 63.2s/0.89x geomean 1.32x, 99|0|0 all scales + adversarial, TPC-H
  22/22 at 1.56x, suite 1313. Keys are Arc-shallow cloned into jobs; the
  binding lifecycle stays single-threaded.
- C. Parallel Ship uploads + exact ship-visibility edges in the IR (each
  island scan lists the ship tables it reads), releasing the conservative
  rule. ASSESSED 2026-07-10, DEFERRED to the multi-server phase - quantified:
  ship uploads cost 3.8-27.3ms in queries of 178-1582ms at SF10 (q17 24ms/5
  ships, q25 27ms/5, q39 3.8ms/3, q66 5.9ms/8), so parallel uploads save at
  most ~20ms per shipping query locally; the island read that DOMINATES
  those queries (q39: 1551 of 1582ms) depends on ALL ships and can never
  overlap them; and the conservative duck-scan exclusion costs nothing
  because duck reads only overlap profitably when I/O-bound, which shipping
  queries' remaining reads are not. The value appears only with real
  network RTTs to the ship target (5 serialized COPYs at 10ms RTT = ~40ms
  recoverable) - exactly what the planned multi-server rig measures. Do it
  then, if the RTT term shows up.

## Outlier sweep (SF10, full 99-query tally, par vs FEDQRS_PARALLEL_STEPS=0)

One back-to-back full-tally diff after Phase B: the ONLY per-query delta
beyond +-150ms in EITHER direction is q23 at -930ms (the win). Totals 64.3s
parallel vs 66.0s sequential. The two 50-150ms positives the diff flagged
(q67 +81, q51 +77) dissolved under repeated dedicated A/B (q67's sequential
runs alone spanned 3821-4420ms). On a single box competing for one disk,
parallel fetch is net -1.7s with no reproducible per-query backfire.

## Multi-source parallelism (verified in code)

The pool is SOURCE-AGNOSTIC by construction: a job carries (datasource name,
work), workers resolve the connector per job through connectors::fetch, and
per-worker connection caches are per-NAME maps - scans from any number of
DISTINCT sources round-robin onto the same workers and overlap freely (the
q23 win IS cross-source: a duck whole-fact read overlapping pg reads and a
duck injected scan). Two caveats:
- The Rust engine supports exactly THREE source kinds today (DsKind:
  Postgres, DuckDb, Parquet). ClickHouse exists only as a python connector -
  register_datasources RAISES UnsupportedIR for it, so a clickhouse source
  cannot run through the engine at all yet; mysql does not exist anywhere.
  When a new kind gets its connectors::fetch arm, the parallel path inherits
  it with zero extra work.
- Per-KIND concurrency policy is the knob to revisit as sources multiply:
  an in-process/embedded kind wants the duck-style injection exclusion
  (already saturates local cores), a networked kind wants full overlap;
  there is no per-source in-flight cap yet (6 workers could all hit one
  source - fine for pg today, worth a cap when sources are many or small).

## Non-goals

- No mid-query adaptivity, no reordering of LOCAL steps, no cross-QUERY
  concurrency changes.
- No parallel Python: the IR is already built whole before execute; the
  Python side is untouched.
- The engine's own binding accumulation stays single-threaded; this plan
  does not touch the memory pool or spill paths.
