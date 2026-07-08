# Dim shipping - open problems and safety (revisit)

Status 2026-07-08: dim shipping (Phase A+B) is landed, correct at every scale
(99|0|0 SF0.1/SF1/SF10 pg-dims, 99|0|0 adversarial, TPC-H 22/22), and net
perf-positive at SF10 (70.6s vs 73.1s no-ship). This doc records the KNOWN
open problems to revisit - the gate is a heuristic, not a cost decision, and
there is one real tail-risk edge to harden.

See `dim-shipping-plan.md` for the design and the gates as implemented.

RESOLVED 2026-07-08 (sections 1-3, the q23 gating problem): a
dimension-width gate now declines a plain aggregate whose GROUP BY spans two or
more INDEPENDENT high-cardinality dimensions. Full design, measured evidence,
and the correction to this doc's "intractable" framing are in
`dim-shipping-collapse-gate-plan.md`. Key facts: the distinguishing signal is
the number of distinct high-card SOURCE DIMENSIONS (owner relations), NOT the
number of high-card keys - correlated keys from one dimension (i_item_id +
i_item_desc) still collapse. Option 2 (group-width) looked broken only because
`warehouse` and five other small tables had NO pg stats (nondeterministic
autoanalyze), now fixed by ANALYZE-after-load; the psycopg2 crash it also hit is
already fixed (ca13e1a). Measured at SF10: q23 ship declined (6.06s -> 5.22s,
99|0|0), every shipping winner preserved (q39/q17/q25/q29/q37/q66/q82 still
ship), SF10 pg-dims totals 71.4s -> 65.0s (0.91x vs DuckDB), geomean 1.61 ->
1.38x. The tail-risk hardening (section 4, ship-size cap) is still OPEN.

## 1. The core problem: we cannot tell "will this aggregate collapse?"

Dim shipping trades one execution shape for another:

- No-ship: semi-join REDUCE the fact by the dim keys, move the reduced fact to
  the coordinator, aggregate there (pipelined / spillable via fragment fusion).
- Ship: keep the fact on its source, ship the small dims in, run the whole
  join+aggregate as ONE island there, and move only the aggregate OUTPUT.

Shipping wins iff the aggregate OUTPUT is much smaller than the reduced fact
that would otherwise move - i.e. iff the GROUP BY collapses a lot. Measured at
SF10 (FEDQRS_PROFILE):

- q39 (wins): `inventory` grouped by (warehouse, item, d_moy). 26.5M -> 382k
  rows = 69x collapse. Ship moves 382k; no-ship moves the 26.5M reduced fact.
  Ship 2236ms vs no-ship 3362ms.
- q23 (loses): `store_sales` grouped by (item_desc, item, d_date) in the
  `frequent_ss_items` CTE. ~16.5M -> 13.8M rows = 1.2x, NO collapse. Ship runs
  one duck island that materializes all 13.8M grouped rows and transfers them
  (2505ms); no-ship reduces the fact and PIPELINES the aggregate into the
  downstream join, never materializing 13.8M as a boundary (~900ms). Net q23
  ship 6096ms vs no-ship 5278ms (+818ms, ~13%).

The distinguishing quantity is the number of GROUP-BY output rows (382k vs
13.8M). We cannot estimate it:

- The cost model DOES have NDV-based group-count estimation
  (`cost.py _estimate_aggregate_tracked`: group count = product of the group
  keys' NDVs, clamped to input rows). But:
  - It assumes NDV INDEPENDENCE. For q39, warehouse x item x moy ~= 490M
    possible combinations, but only 382k actually occur in `inventory` (highly
    correlated). Even with perfect single-column NDVs it over-estimates q39's
    real group count by ~1000x -> reports "no collapse", wrong.
  - Single-column NDVs also DEFAULT unpredictably: q39's `warehouse` NDVs
    default (the estimator cannot resolve them through the join -> falls back to
    input*fraction = 133M); q23's `i_item_sk`/`item_desc` default (the group key
    comes through a derived table `(SELECT SUBSTRING(...), * FROM item) sq1`,
    and NDV does not propagate through a SubqueryScan).

Net: the estimate reports "no collapse" for BOTH q39 (wrong) and q23 (right),
so any collapse-ratio gate declines both. This is the classic
multi-column-distinct / join-cardinality estimation problem.

## 2. Why the current gate is structural (and its blind spot)

Because collapse is not estimable, the gate uses reliable STRUCTURAL signals
(`optimizer/dim_shipping.py`): ship only a plain (non-rollup) aggregate root,
INNER joins only, writable target, fact large + foreign small + ratio, no
window, island outputs match the pure plan. These catch the big
non-collapsing shapes (rollup/cube, bare joins, set-op DISTINCT branches, large
dims).

BLIND SPOT: a PLAIN aggregate that happens not to collapse (q23) slips through.
It is the one case stats cannot see. Result: q23 regresses ~13%. Across the
suite the wins dominate (net -2.4s at SF10), but individual queries of this
shape can each give back ~10-15%.

## 3. Option 2 (group-width guard) - investigated, does NOT work

Idea: decline when the aggregate has >=2 high-cardinality group keys (NDV>=10k;
unknown NDV counts as high-card), since two wide keys multiply into a
non-collapsing product.

Measured (SF10): it declines q23 (good, recovers ~700ms) but ALSO declines q39
(bad, loses ~830ms) because q39's `warehouse` NDVs DEFAULT to high-cardinality
(section 1). It also destabilized q04/q11 (the plan change surfaced the
psycopg2 encoding bug, section 5). Same NDV-defaulting root cause; not a clean
win. Reverted.

A group-width guard could only work if the NDV defaulting were fixed - i.e. if
NDV propagated through joins and derived tables so `warehouse` (q39) resolves
to ~20 and `i_item_sk` (q23) resolves to ~100k. That is a cost-model project
(NDV propagation through SubqueryScan/Projection/Join), not a small patch.

## 4. Tail risk / "is it a bomb?" - mostly bounded, one real edge

RESOLVED 2026-07-09 (fedqrs `connectors.rs`; see
`dim-shipping-ship-cap-plan.md`). The proposed hardening below is now
implemented: a hard row cap `SHIP_MAX_ROWS = 50_000_000` in `ship_table` raises
loudly BEFORE ingesting an over-cap relation (converting a stale-stats runaway
into an immediate honest crash), and the shipped Postgres temp table is ANALYZEd
after ingest so the island join is planned over real stats. The cap is a fixed
constant for now (a safety valve has no query-derived right value; the planner's
SHIP_ROW_BUDGET keeps legitimate ships small) and will move to user-overridable
settings later.

Concern: on a very large table, could a wrong ship decision be catastrophic
(saturate the network, hammer Postgres, run for hours)?

- TRANSFER is bounded, not unbounded. Ship moves the aggregate output
  (<= joined-fact size); no-ship moves the reduced fact (<= fact size). When
  dims are selective both are small; when not, both are ~fact-size. Shipping
  never moves unboundedly more than no-ship - worst case is a CONSTANT factor
  from losing pipelining/spilling (measured 1.15x on q23; plausibly 2-3x on a
  pathological large aggregate), not minutes-to-hours.
- The island runs on the fact's OWN source (the fact is never moved), so it is
  the same engine doing similar work.

THE ONE REAL EDGE: STALE STATS on the shipped dimension. The ship-size gate
uses ESTIMATED dim rows (<=200k, declines on defaulted stats). If a dim's
stats are stale - estimated 100k, actually 100M - we would ship a huge
relation. Into Postgres via ADBC COPY that is exactly the "hammer pg / saturate
bandwidth" case. Narrow (stale ANALYZE) but real, and unbounded.

### Proposed hardening (not yet implemented)

- HARD RUNTIME CAP on the shipped relation. At the `ship` step the binding is
  already materialized in memory; check its ACTUAL row count and RAISE loudly
  if it exceeds a hard cap (e.g. 2M rows, 10x the plan budget) instead of
  ingesting. Converts a multi-hour ingest into an immediate, honest failure
  (matches "a crash never ships a lie"). ~10 lines in `connectors::ship_table`.
  This bounds the tail INDEPENDENTLY of fixing the estimator.
- ANALYZE the pg temp table after ingest (or set a stats hint), so Postgres
  does not mis-plan the island join over an unstatted temp table. Small.

## 5. Latent bug found (independent of dim shipping): psycopg2 encoding

`PostgreSQLDataSource.connect()` builds the psycopg2 pool WITHOUT
`client_encoding='UTF8'`. Reading any non-ASCII text back through psycopg2 -
here `pg_stats` histogram_bounds for a TEXT column (an accented name / country /
email; 0xc3 is a UTF-8 lead byte) - crashes `cursor.fetchall()` with
`UnicodeDecodeError: 'ascii' codec can't decode byte 0xc3`.

- Path: `_pg_stats_columns` -> `cursor.fetchall()` (postgresql.py:271).
- LATENT because psycopg2 is only the Python-side metadata/stats path (data
  goes through Rust/ADBC), and text-column stats are only fetched when the
  planner needs NDV/selectivity for that column. Normal q11 never requests
  stats for customer NAME columns; the option-2 guard did -> triggered it.
- Independent of dim shipping: ANY plan whose costing needs stats for a
  non-ASCII text column (a filter/group/join on such a column) hits it.
- FIX: one line - pass `client_encoding="UTF8"` to the ThreadedConnectionPool
  (or `set_client_encoding('UTF8')` per connection).

## What to do next (when we revisit)

1. Land the runtime ship-size cap (section 4) - the real production-safety win,
   independent of estimation. Highest priority.
2. Fix the psycopg2 UTF-8 encoding bug (section 5) - one line, real correctness.
3. Only then consider improving the estimator (NDV propagation through
   joins/derived tables, or a multi-column-distinct signal) to gate q23-shaped
   queries out; large, and not required for safety.
