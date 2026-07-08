# Dim shipping - gate the non-collapsing ship (q23) by dimension width

Branch: `feature/dim-shipping-ship-cap` (repurposed to the estimator/gating work
at the user's direction). Supersedes the "collapse-ratio estimation is
intractable" conclusion in `dim-shipping-open-problems.md` sections 1-3 with a
measured, tractable gate.

STATUS: LANDED 2026-07-09. All three pieces below are implemented
(dimension gate in `optimizer/dim_shipping.py`, row-count-bounded NDV in
`optimizer/cost.py`, ANALYZE-after-load in `benchmarks/tpcds/load_postgres.py`);
piece (C) deferred as planned. Verified: full pytest 1300 passed / 3 skipped /
25 xfailed; TPC-DS 99|0|0 at SF0.1/SF1/SF10 pg-dims; TPC-DS SF10 adversarial
98|0|1 (the 1 is a flaky in-batch memory kill on q67, which passes when run
alone and is not a ship candidate); TPC-H SF1 22/22. Perf at SF10: q23 ship
declined (6058 -> 5223 ms, PASS 100/100), every shipping winner preserved
(q39/q17/q25/q29/q37/q66/q82 still ship and PASS); pg-dims totals 71.4s -> 65.0s
(0.91x vs DuckDB, was 1.00x), geomean 1.61 -> 1.38x. New unit tests:
`tests/test_dim_shipping_gate.py` (5). NDV/owner tracer kept for future
debugging: `benchmarks/tpcds/diag_ndv.py`.

NOTE ON ROOT CAUSE (to avoid the doc's earlier mis-framing): the q23 regression
was NOT caused by missing ANALYZE - it was caused by having no collapse-aware
gate at all, so a non-collapsing aggregate shipped. Missing ANALYZE was a
SEPARATE fault that made the GATE look unusable (warehouse defaulted to
high-card, so any width-style guard wrongly declined the q39 WIN). Both are
fixed here: the gate declines q23, and reliable stats keep q39 shipping.

## The problem (restated)

Dim shipping regresses one query, q23, by ~13% / ~0.8s at SF10: a PLAIN
aggregate that does NOT collapse (store_sales grouped by item x date, 28M ->
13.8M rows) slips past the structural gate and ships, materializing 13.8M rows
as a boundary where no-ship would pipeline them. Every other shipping query is a
win. The gate needs to decline q23 WITHOUT declining any winner.

The open-problems doc concluded this needs multi-column-distinct estimation (a
large cost-model project) and that the group-width guard (Option 2) does not
work. Both conclusions were based on a FLAKY STATS ENVIRONMENT. Re-measured
below, the gate is tractable.

## What the measurement actually shows (SF10, pg-dims)

Ground-truth engine wall time, shipping ON vs OFF (FEDQ_DIM_SHIPPING=0), for
every query the naive key-count guard would flip:

| query | ship ON | ship OFF | shipping is | correct verdict |
|-------|---------|----------|-------------|-----------------|
| q17   | 227 ms  | 406 ms   | FASTER      | keep (ship)     |
| q23   | 6058 ms | 5394 ms  | SLOWER      | DECLINE         |
| q25   | 180 ms  | 215 ms   | faster      | keep            |
| q29   | 208 ms  | 579 ms   | MUCH faster | keep            |
| q37   | 66 ms   | 71 ms    | faster      | keep            |
| q66   | 334 ms  | 360 ms   | faster      | keep            |
| q82   | 85 ms   | 81 ms    | neutral     | keep            |

q23 is the ONLY genuine regression. A guard must decline exactly q23.

Why the naive "decline when >= 2 high-cardinality group keys" guard is WRONG:
q17/q25/q29/q37/q82 all group by `(i_item_id, i_item_desc, ...)`. Both
`i_item_id` (NDV 45,692) and `i_item_desc` (NDV 65,602) are high-cardinality, so
the naive count is 2 and it would decline them - but they are CORRELATED (the
description is functionally determined by the id; both live in `item`), so their
combination does NOT explode and the fact still collapses. Shipping wins. The
naive guard is fooled by two correlated keys from ONE dimension - the same
NDV-independence blind spot the doc flagged for the ratio gate, now biting the
width gate.

q23 is different: it groups by `item` (high) x `date_dim.d_date` (NDV 73,049,
high) - TWO INDEPENDENT high-cardinality dimensions. Every item sold on many
dates, so (item x date) has 13.8M real combinations. No collapse.

THE DISTINGUISHING SIGNAL is the number of distinct high-cardinality SOURCE
DIMENSIONS the group-by spans, not the number of high-cardinality keys.

## The gate: distinct high-card dimension count

In `optimizer/dim_shipping.py`, after `_collapses_via_aggregate` finds the plain
aggregate root, resolve each of its group keys to (owner relation, NDV) via the
cost model and count the DISTINCT owner relations that contribute a
high-cardinality key. Decline shipping when that count is >= 2.

- High-cardinality = resolved NDV >= `HIGH_CARD_NDV` (10,000), OR NDV unknown.
  10k sits with wide margin between every TPC-DS dimension that ships: low side
  tops out at `i_current_price` = 3,656 (stores 51-102, brands ~950, months 12,
  states 3-30, warehouses 10); high side starts at `i_item_id` = 45,692
  (i_item_desc 65k, d_date 73k, customer 500k).
- Owner = the base relation a group key's qualifier resolves to in the
  aggregate's input subtree (the cost model's relation resolver). Two keys from
  the same dimension (i_item_id + i_item_desc, both `item`) share an owner and
  count ONCE. Keys from one derived table (q66's 8 warehouse-attribute keys,
  exposed by one SubqueryScan) share that SubqueryScan owner and count ONCE.
- Unknown-NDV or ownerless keys count as high-cardinality / their-own-dimension
  (conservative: prefer to decline when we cannot prove a key is low-card or
  shares a dimension). Declining is always SAFE - it falls back to the proven
  no-ship plan; it never changes a RESULT, only a runtime.

Verified against the table above: this rule declines q23 (owners {item-derived
`sq1`, `date_dim`} = 2) and keeps every winner (q17/q25/q29/q37/q82 = {item} = 1;
q66 = {one warehouse-derived SubqueryScan} = 1; q39 = {item} = 1, since
`warehouse` NDV 10 and `d_moy` NDV 12 are low). Blast radius among all 99
queries at SF10 = q23 only.

## The reliability foundation (why Option 2 looked broken before)

The gate classifies a dimension as high/low by its column NDV. That resolution
must be RELIABLE or the gate misfires. The doc's Option-2 revert was caused by
two now-understood environmental faults, not by the heuristic:

1. STALE / MISSING BASE STATS (the real q39 breaker). Measured: the SF10
   `warehouse` table (and `call_center`, `income_band`, `reason`, `ship_mode`,
   `web_site`) had NO pg statistics at all - `reltuples = -1`, zero `pg_stats`
   rows - because nothing ANALYZEd them (autoanalyze fires nondeterministically
   and had not covered the small tables). With warehouse unstatted, its columns
   default to unknown -> high-card -> q39 counts {warehouse, item} = 2 -> wrongly
   declined. After `ANALYZE`, warehouse resolves to NDV 10 (low) and q39 counts
   {item} = 1 -> ships. This is exactly the "NDV defaulting" the doc named, but
   the cause is missing stats, not an unfixable estimator.

2. The psycopg2 non-ASCII crash the Option-2 probe surfaced (open-problems
   section 5) is ALREADY FIXED (commit ca13e1a, client_encoding=UTF8).

So the gate rests on three reliability pieces, in priority order:

- (A) REQUIRED - benchmark hygiene: `benchmarks/tpcds/load_postgres.py` must run
  `ANALYZE` on every loaded table (all scales) so stats are present and
  deterministic. Without it the gate's dimension classification depends on
  autoanalyze timing (as it did when Option 2 was measured). This is a
  data-loading fix, not an engine change, but it is the foundation.
- (B) ENGINE robustness - NDV bounded by row count: when a column has no
  histogram but its table's row count is known, bound its NDV by that row count
  (a column cannot have more distinct values than the table has rows). A small
  dimension then classifies low-card even without per-column stats. Complements
  (A) for any table with a row count but a missing column histogram.
- (C) ENGINE hardening (optional, no TPC-DS query needs it today) - SubqueryScan
  NDV attribution: resolve a derived-table group key to its underlying base
  relation(s), so the distinct-dimension count stays exact even if two
  INDEPENDENT high-card dimensions live inside one derived table. Today the
  shared-owner rule (a SubqueryScan is one owner) handles every measured case
  because each ship candidate's derived table draws from a single base
  dimension; (C) removes the residual blind spot on principle. A missed decline
  here is a perf regression, never a wrong answer.

## Implementation sketch

1. Cost model (`optimizer/cost.py`): expose a clean API the gate can call
   without reaching into privates. The gate needs, per group key over an input
   subtree: the owning relation's identity and the key's NDV (None when
   unknown). Add a public `group_key_dimension(input_node, key) -> (owner, ndv)`
   (or reuse the existing public `column_ndv` plus a public owner resolver).
   Fold (B) - row-count-bounded NDV - into `_owner_column_ndv` so every NDV
   consumer benefits (join ordering already uses these).
2. Dim shipping (`optimizer/dim_shipping.py`): add `_dimension_explosion(agg)`
   that counts distinct high-card owners over `agg.group_by` and returns True at
   >= 2; call it in `_analyze` right after `_collapses_via_aggregate`, declining
   (return None) when it fires. Add `HIGH_CARD_NDV = 10_000` next to the other
   ship constants with a comment citing the low/high margin.
3. Benchmark (`benchmarks/tpcds/load_postgres.py`): `ANALYZE` after load.
4. (C), if included: SubqueryScan attribution in the cost model's relation
   resolver - map an exposed derived-table column to its inner output expression
   and recurse to the base relation.

## Tests

- Unit (`tests/`): a `DimShipping._dimension_explosion` test over synthetic
  aggregates - one high-card dimension with several correlated keys => ship; two
  independent high-card dimensions => decline; unknown-NDV key => treated
  high/own-dimension. No live source needed (feed a stubbed cost model).
- Cost-model unit for (B): a column with no histogram but a known small row
  count resolves NDV <= row_count, not the default.
- Guard against regressing the winners: the SF10 staged tally (below) is the
  real signal; assert q23 no longer ships and q17/q25/q29/q37/q66/q82/q39 still
  do (a plan-shape assertion via the dump-plan path, or the timing tally).

## Verification gates (must hold before merge)

- Full suite green: `POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m
  pytest -q` (baseline 1287 passed / 3 skipped / 25 xfailed).
- Correctness unchanged: TPC-DS 99|0|0 at SF0.1/SF1/SF10 pg-dims + 99|0|0
  adversarial + TPC-H 22/22 (declining a ship cannot change a result; this
  confirms it). Use the staged tally (`--mode compare`, ~1s).
- Perf: q23 SF10 ship-declined recovers ~0.6-0.8s (6.06s -> ~5.4s); q39 and the
  other shipping winners unchanged (still ship). Net SF10 totals improve or hold.
  Re-measure the ON/OFF table above with the gate in place - every "keep" row
  must still ship, q23 must not.

## Measured facts to preserve (diagnostics)

`benchmarks/tpcds/diag_ndv.py` (per-aggregate group-key NDV + owner trace) and
`benchmarks/tpcds/diag_shipscan.py` (all-query ship + distinct-high-owner
verdict) are the tools that produced the tables above; keep them until the gate
lands and is verified, then decide whether they earn a place in the tree.

NOTE: this analysis ran `ANALYZE` on the live `tpcds_sf10` pg database to reach
a clean stats state. Piece (A) makes that ANALYZE part of the load so the state
is reproducible.

## Open decisions for the user

- Include piece (C) (SubqueryScan attribution) now, or defer it as documented
  hardening (no measured query needs it)? Recommendation: DEFER - land the gate
  + (A) + (B), which fix q23 and make q39 reliable, and keep (C) as a noted
  follow-up so we do not add propagation code no benchmark exercises.
- HIGH_CARD_NDV threshold: 10,000 (recommended, wide margin). Fixed constant vs
  env-tunable: recommend fixed (no knob nobody sets).
