# Dim shipping - ship-size tail-risk hardening (plan)

Branch: `feature/dim-shipping-ship-cap` (both federated-query and fedqrs).
Source of the gap: `dim-shipping-open-problems.md` section 4 ("Tail risk / is
it a bomb?"). This plan takes the two hardening items that section lists as
"proposed but not yet implemented", plus one guard test. It does NOT touch the
estimator work (sections 1-3), which the doc marks as large and not required
for safety.

STATUS: LANDED 2026-07-09 (fedqrs `connectors.rs`). Both items implemented:
(1) a hard row cap `SHIP_MAX_ROWS = 50_000_000` in `ship_table` that raises
loudly before ingesting an over-cap relation; (2) `ANALYZE` on the shipped
Postgres temp table after ingest. The cap value differs from the 2M this plan
first proposed: a safety valve has no query-derived "right" number, so it is set
to a deliberately large fixed constant (per the user's call) and will move to
user-overridable settings later; the planner's own SHIP_ROW_BUDGET (~200k
estimated) is what keeps legitimate ships small, so the cap only ever fires on a
stale-stats blowup. Verified: TPC-DS SF10 adversarial (ships into pg) stays
green with ANALYZE in the path; no TPC-DS ship approaches the cap.

Scope note: section 5 (the psycopg2 client_encoding=UTF8 latent bug) is ALREADY
FIXED on `feature/cost-based-optimizer` (commit ca13e1a) - nothing to do here.

## The gap, restated

Dim shipping picks its target by cost using ESTIMATED dim rows (ship-size gate
declines above ~200k estimated, declines on defaulted stats). Every other part
of the tail is bounded: the fact never moves, the island runs on the fact's own
source, and TRANSFER is at most a constant factor worse than no-ship (measured
1.15x on q23, plausibly 2-3x pathological). Compute and transfer cannot run away.

THE ONE UNBOUNDED EDGE is STALE STATS on the shipped dimension. If a dim's
ANALYZE is stale - estimated 100k rows, actually 100M - the optimizer ships a
huge relation. Into Postgres via ADBC binary-COPY that is the "hammer pg /
saturate bandwidth" case: a multi-minute-to-hours ingest presented as a normal
plan. Narrow (needs a stale ANALYZE) but real, and the only edge that is not a
bounded constant factor.

A second, smaller problem: the shipped Postgres temp table is created by ADBC
ingest and never ANALYZEd, so Postgres plans the island join over an unstatted
temp table (default row-count guesses), which can pick a bad island plan even
when the ship decision itself was right.

## Where the fix lives

Both fixes are in fedqrs (Rust), in `src/connectors.rs`, at the `ship_table`
boundary. This is the right place because:

- At ship time the binding is ALREADY materialized in memory as
  `Vec<RecordBatch>` (see `engine.rs:412` Step::Ship -> `connectors::ship_table`
  at `connectors.rs:88`). The ACTUAL row count is free to compute - no estimate,
  no probe - by summing `batch.num_rows()`. This is the honest number the
  planner's estimate could be wrong about.
- The cap must fire BEFORE the ingest, so it converts a doomed multi-hour COPY
  into an immediate, honest crash. That matches the project doctrine: "a crash
  never ships a lie". Failing loud here is strictly better than a silent runaway.
- It bounds the tail INDEPENDENTLY of ever fixing the estimator. The estimator
  can stay wrong; the runtime cap catches the one case where being wrong is
  catastrophic.

## Item 1: hard runtime cap on the shipped relation (highest priority)

In `ship_table` (`connectors.rs:88`), before dispatching to the DuckDB or
Postgres path, sum the actual rows across `batches`. If the total exceeds a hard
cap, RAISE (PyRuntimeError) with a message naming the table, the actual count,
and the cap. Do not ingest.

- Cap value: 2_000_000 rows. Rationale from the open-problems doc: the ship-size
  gate's plan budget is <=200k estimated dim rows; 2M is 10x that budget, so a
  correct decision (even one that lands somewhat above estimate) never trips it,
  while a stale-stats mis-ship (100k estimated / 100M actual) always does. The
  cap is a safety backstop, not a second cost gate - it must sit well above any
  legitimate ship so it never changes a good plan's behavior.
- Applies to BOTH targets (DuckDB pinned temp table and Postgres ADBC COPY).
  The DuckDB path is in-process and less dangerous, but the row count is equally
  free there and a 100M-row in-process temp table is its own memory hazard, so
  the guard belongs before the `match s.kind` dispatch, covering both.
- Message shape (loud, actionable, ASCII only), e.g.:
  `dim shipping refused: table "<t>" has <n> rows, over the hard cap of
  <cap> - stale stats likely mis-planned this ship; not ingesting`.
- The raise unwinds the whole query (there is no fallback path at execute time -
  the plan committed to shipping). That is the intended behavior: fail fast and
  honest rather than silently ingest a giant relation. A future refinement could
  re-plan without shipping, but that is out of scope here and NOT needed for the
  safety guarantee.

Kill switch / configurability: the existing `FEDQ_DIM_SHIPPING=0` already
disables shipping entirely. The cap itself is a fixed constant; if we later want
it tunable, read an env var (`FEDQ_SHIP_MAX_ROWS`) with the 2M default. Decide
during implementation - default to the fixed constant unless there is a concrete
need, to avoid a knob nobody sets.

## Item 2: ANALYZE the shipped Postgres temp table after ingest

In `ship_table_postgres` (`connectors.rs:126`), after `ingest_temp` succeeds,
run `ANALYZE "<table>"` on the same pooled connection (via the existing
`exec_update`). This gives Postgres real row counts / NDVs for the temp table so
the island join is planned against true cardinality instead of the default
guess for an unstatted relation.

- Only the Postgres path needs this. The DuckDB ship path builds a TEMP TABLE
  and appends batches (`create_shipped_table`, `connectors.rs:169`); DuckDB
  gathers its own lightweight stats and there is no ANALYZE-shaped concern
  called out in the open-problems doc. Leave the DuckDB path unchanged.
- Ordering: ANALYZE runs AFTER a successful ingest and BEFORE the island read
  fires. Since the island read is a later step in the same query on the same
  pinned/pooled connection, doing it at the tail of `ship_table_postgres` is
  correct.
- Failure handling: an ANALYZE failure is a real error (the ingest succeeded, so
  the connection is live). Do NOT swallow it - propagate via `map_err` like the
  surrounding ingest calls. If ANALYZE genuinely cannot run we want to know, not
  silently plan on empty stats. (Contrast the DROP-at-query-end cleanup, which
  is deliberately best-effort - that is teardown, not a correctness step.)

## Tests

Rust-side (fedqrs), matching how the ship paths are exercised today:

1. Cap guard fires: build a batch set whose summed `num_rows()` exceeds the cap
   and assert `ship_table` returns an Err whose message names the table and the
   count. Keep the batch small in bytes (many tiny rows or a lowered test-only
   cap) so the test is cheap - prefer making the cap a named constant the test
   can reference rather than allocating 2M real rows.
2. Cap guard does NOT fire below the cap: a normal small ship still succeeds
   (this is already covered by the existing dim-shipping ship tests; confirm one
   asserts a below-cap ship ingests and the island reads it).
3. ANALYZE runs on the pg path: harder to unit-test in isolation without a live
   pg; the correctness signal is the full adversarial TPC-DS run that ships into
   pg (99|0|0) staying green, plus asserting the ANALYZE statement is issued.
   Decide during implementation whether an integration assertion is worth it or
   the suite tally is sufficient.

## Verification gates (must hold before merge)

- fedqrs builds release-clean: `maturin develop --release`.
- Full fedq suite green: `POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python
  -m pytest -q` (baseline 1287 passed / 3 skipped / 25 xfailed).
- Correctness unchanged at every scale (the cap must not alter any legitimate
  plan): TPC-DS 99|0|0 at SF0.1/SF1/SF10 pg-dims + 99|0|0 adversarial (ships
  into pg) + TPC-H 22/22. Use the staged tally (`--mode compare` against
  `references_sf<sf>.duckdb`, ~1s) so this is cheap to re-check.
- Perf neutral: the cap sits above every real ship, so SF10 dim-shipping wins
  (q39 etc.) must be unchanged. Spot-check q39 SF10 stays ~1.6s. ANALYZE adds a
  small one-time cost on the shipped temp table; confirm it does not regress the
  adversarial pg-ship queries (it should help or wash).

## Order of work

1. Item 1 (cap) - the actual safety win. Land + test first.
2. Item 2 (ANALYZE) - island-plan quality. Land + test second.
3. Re-run the staged tallies + TPC-H regression check; update HANDOFF.md and
   `dim-shipping-open-problems.md` (move section 4's two items from "proposed"
   to "landed", record commit ids).

## Explicitly out of scope

- The estimator / NDV-propagation work (open-problems sections 1-3): gating the
  q23-shaped plain-aggregate-that-does-not-collapse regression. Large, a
  cost-model project (NDV through joins / SubqueryScan / derived tables), and not
  required for the safety guarantee this branch delivers.
- Re-planning without shipping on a cap trip. The raise is the contract here;
  automatic fallback is a possible later refinement, not part of the safety fix.
