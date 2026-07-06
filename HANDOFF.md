# Status: CBO complete at the decision layer; fair federated gap 38.6x -> 1.98x

State of the work. Facts only: what exists, what passes, what is measured,
what is not done. Previous phase (Rust cutover, N-K decorrelation, merge-engine
removal) is in git history before commit `4018914`.

## Branches

```
merge-engine-datafusion
  `- nk-decorrelation
       `- remove-duckdb-merge          Rust-only execution, N-K decorrelation
            `- feature/cost-based-optimizer   <-- HEAD (this document)
```

Test suite: **1168 passed, 3 skipped, 39 xfailed, 0 failed**
(`POSTGRES_DB=duckpoc python -m pytest -q`). `make lint` green.

## What was built on this branch

### 1. Reproducible benchmark harness (committed before the optimizer)
- `benchmarks/tpch/bench.py`: one entry point, three methodologies
  (`single` Parquet / `fedparquet` 2-Parquet / `fedpgduck` fair PG+DuckDB)
  x SF0.1 + SF1, per-query subprocess with RSS cap + timeout, differential
  correctness vs DuckDB, report written to
  `benchmarks/tpch/reports/report-result-<commit>.md`.
- Data prep is fully scripted (`generate.py`, `export_parquet.py`,
  `load_postgres.py` - now runs ANALYZE). See `benchmarks/tpch/README.md`.
- Baseline (pre-optimizer) report: `report-result-3872243.md`.

### 2. Cost-based join-ordering optimizer (M0-M7, ON by default)
- **Statistics** (`optimizer/statistics.py`, datasources): fetched from the
  SOURCES' CATALOGS at optimize time, lazily per column (join keys + filter
  columns only), session-cached, absence cached. PG: schema-qualified
  `pg_class.reltuples` (+`pg_namespace`; -1 = honest None) + `pg_stats`
  (n_distinct decode, null_frac, avg_width, histogram min/max). DuckDB /
  Parquet: `duckdb_tables()` row count + ONE `approx_count_distinct/min/max/
  count` scan for requested columns. Sources never fabricate a statistic.
- **Estimates** (`optimizer/cost.py`, `estimate_defaults.py`):
  `CostModel.estimate(plan) -> CardinalityEstimate` covers every logical node
  type; joins use `card(L) x card(R) / prod max(ndv(l_key), ndv(r_key))` with
  NDVs clamped per side. Every missing statistic falls back to a NAMED default
  and is recorded in `defaults_used` provenance. `join_tree_cost` (C_out) is
  the documented seam for a future locality/network cost term.
- **Ordering** (`optimizer/join_graph.py`, `join_ordering.py`): region
  extraction (INNER/CROSS joins + absorbed Filters; boundaries: outer/semi/
  anti/NATURAL/USING joins, laterals, guards, derived tables - recursed into
  separately), left-deep Selinger DP over connected subsets per component
  (<= `max_join_reorder_size`, default 10; GOO greedy above), connected
  extensions only = intra-component cross products structurally impossible,
  components CROSSed smallest-first. Deterministic, idempotent.
- **The rule** (`JoinOrderingRule`): re-emits in PredicatePushdown's normal
  form; a predicate-conservation guard RAISES if any conjunct is not placed
  exactly once. Runs right after PredicatePushdown via `optimizer/factory.py`
  (`build_optimizer` - the single assembly point; `enable_join_reordering`,
  `enable_predicate_pushdown`, `enable_projection_pushdown` are real flags
  now). `FedQRuntime` takes the whole `Config`.
- **EXPLAIN**: joins print `rows=<estimate>` and `stats=defaulted[...]`
  naming every estimate built on a default instead of a real statistic.
- Tests: unit (stats layer, NDV formula, extraction, enumerator with stub
  stats incl. the q05 cyclic trap, rule shape/idempotence/guard), e2e
  differential (reordering on vs off vs DuckDB oracle,
  `tests/e2e_pushdown/test_join_reordering.py`), plus the committed
  experiment gate.

## Measured (committed reports)

`benchmarks/tpch/reports/report-result-8b54580.md` vs baseline
`report-result-3872243.md` (ratio = ours/DuckDB, lower is better):

| Cell       | SF  | Before                | After           |
| ---------- | --- | --------------------- | --------------- |
| single     | 0.1 | 4.31x                 | **2.47x**       |
| single     | 1   | 13.76x, q09 OOM-KILLED | **5.62x, 22/22** |
| fedparquet | 0.1 | 6.47x                 | 5.06x           |
| fedparquet | 1   | 18.59x, q09 OOM-KILLED | **9.93x, 22/22** |
| fedpgduck  | 0.1 | 12.01x                | 11.29x          |
| fedpgduck  | 1   | 38.55x                | **23.24x**      |

- **All 132 cells correct; zero KILLED/ERROR anywhere.** q09 went from
  OOM (>12GB) to 448ms (single SF1).
- Join-order pathologies eliminated: single SF1 q02 3857 -> 37ms,
  q08 6253 -> 92ms, q09 KILLED -> 448ms.
- Order-independence confirmed (`experiments/reports/joinorder-12609d3-sf1.md`):
  original vs hand-reordered CONVERGE; q07 fedpgduck the DP beat the hand
  order (1247ms vs 2635ms).

## Projection pushdown: FIXED (PP-M1..M6)

Column pruning was a NO-OP on every TPC-H query: the gate
(_has_explicit_projection) did not see through Sort, and every query has
ORDER BY at the root. Fixed end to end:
- gate sees through Sort/GroupedLimit/SingleRowGuard, CTE.child, set-op
  branches;
- collection is qualified (table.col) and annotation-driven
  (direct_expressions - total by construction, unclassifiable fields raise),
  covering Scan.order_by_keys (fixpoint iteration 2), subquery-hidden probe
  values, and correlated refs across scopes;
- scans match by alias AND physical name; pushed-aggregate/DISTINCT scans and
  star refs are never pruned; unreferenced relations keep everything;
- the prune walker is total and fail-loud; derived tables / CTE plans /
  set-op branches prune inside only when their root pins an explicit schema.
Measured widths (fedpgduck q03): customer 8 -> 2, orders 9 -> 4, lineitem
16 -> 4 columns; q15's CTE interior narrows too. Zero e2e fallout; the q07
history (equi-only connectivity fix and its data-movement trade-off) is in
the git log at 063aada.

## CTE pushdown: FIXED (the q15 outlier, commit a044026)

q15 ran at 169x DuckDB for three stacked reasons; two fixed:
- All four pushdown rules (Predicate/Aggregate/OrderBy/Limit) were CTE-blind:
  the WITH body's filter and GROUP BY never reached the lineitem scan, so
  the entire 6M-row table crossed the wire TWICE, unfiltered. All four
  walkers now descend CTE (cte_plan + child); the body collapses to a
  remote GROUP BY returning ~10k rows.
- available_columns had no CTERef/SubqueryScan/Sort arms (unknown = empty =
  decline to push): the s_suppkey = supplier_no equality could not be
  classified past the decorrelated LEFT join, leaving supplier x revenue a
  conditionless nested-loop CROSS. CTE references now expose their outputs.
- Consequence fix: the dynamic-filter mark requires an executable build side
  (PhysicalScan/PhysicalRemoteQuery) - a CTE build side crashed EXPLAIN's
  value prefetch.
Deferred: the CTE body still evaluates once per reference (each copy is now
a cheap remote aggregate; materialize-once is an engine feature).

## COUNT(*) pruning fix: the q21 outlier (commit 6dcc2ca)

count(*) contributes a ColumnRef('*') to the required-column set, and the
star guard then kept EVERY scan in the query at full width - q21's two
EXISTS bodies each shipped all 16 lineitem columns x 6M rows. COUNT(*)
counts rows and needs no columns: the collector skips count-star calls (a
genuine star PROJECTION still blocks pruning). The suspected nested-loop
SEMI/ANTI joins were a non-issue (the engine rewrites them to hash joins
with filters). q21 3297 -> 365ms; every count(*) query benefits (q13 774
-> 296ms, q22 431 -> 120ms).

## Measured across all phases (report-result-6dcc2ca.md)

| Cell       | SF  | Pre-opt | Join order | +Projection | +CTE  | +count(*) |
| ---------- | --- | ------- | ---------- | ----------- | ----- | --------- |
| single     | 0.1 | 4.31x   | 2.47x      | 1.90x       | 1.87x | **1.83x** |
| single     | 1   | 13.8x*  | 5.62x      | 3.97x       | 2.13x | **2.11x** |
| fedparquet | 0.1 | 6.47x   | 5.06x      | 2.71x       | 2.62x | **2.39x** |
| fedparquet | 1   | 18.6x*  | 9.93x      | 5.43x       | 3.49x | **2.82x** |
| fedpgduck  | 0.1 | 12.01x  | 11.29x     | 6.31x       | 5.84x | **4.74x** |
| fedpgduck  | 1   | 38.55x  | 23.24x     | 11.36x      | 8.60x | **5.10x** |

(* = q09 OOM-KILLED in those runs.) All 132 cells correct in every phase.
No query in the fair federated cell is above ~11x anymore; the worst are
q07 479ms/11.1x, q09 950ms/10.2x, q18 783ms/8.6x.

## Locality term: LANDED (phase A, commits 1efefc7 + 39a1c8f)

The DP charges rows crossing a source boundary (TRANSFER_WEIGHT x transfer)
on top of C_out; only the leading same-source run of a left-deep component
collapses into one remote query, and candidates track their island state
(subset-determined, so the DP key stays the plain subset - proof in the
_Candidate docstring). Gate (report-result-39a1c8f.md): fedpgduck SF1
5594 -> 5209ms (5.01x), all 132 cells correct. q05 439 -> 195ms (its duck
island lineitem JOIN orders now ships 912k rows instead of 6.2M), q03 -123ms,
q10 -108ms, q07 -63ms. KNOWN trade-off, deliberate: q08 +134ms (both
federated cells) and q09 fedparquet +299ms - the ultra-selective part filter
used to cut intermediates before the fact join; the island defers it. Phase B
(generalized key injection) targets exactly that shape; if q08 is still
regressed after B, recalibrate TRANSFER_WEIGHT.

## Generalized key injection: LANDED (phase B, commits 0d397c8..62bba5f
## + fedqrs db514e4/5313c18/d839050)

The EXISTING adaptive strategy (IN list < 2000 keys; temp-table semi-join;
selectivity guard) now actually fires on federated shapes - nothing about it
was rewritten:
- B1: the gate accepts a COMPUTED build side (it is materialized for the
  join anyway; every step output is a re-readable binding).
- B2: the missing DuckDB ingestion arm - Arrow keys appended into a
  connection-scoped temp table, same temp_join_sql, same 40% guard fed by a
  duckdb_tables() catalog estimate, plus DUCK_TEMP_CAP=50k (the keys/rows
  estimate underestimates selectivity for near-superset key sets - a
  measured q18 regression, the classic no-index backfire; PG keeps its
  pg_stats guard uncapped).
- B3: pushed remote queries are injection targets - the island's SQL wraps
  as a derived table and the key filter applies to its output columns
  (SELECT * FROM (<island>) AS fedq_probe WHERE col IN ...). This is the
  locality + reduction composition.

Gate (report-result-62bba5f.md): fedpgduck SF1 5209 -> 4786ms (4.43x), all
132 cells correct. q09 1031 -> 479ms (the 10.6k %green% part keys semi-join
inside DuckDB), q08 294 -> 167ms (phase A's deferred-filter regression
erased), q05/q18 held.

## Measured across all phases (fedpgduck SF1, ours/DuckDB)

pre-opt 38.55x -> join order 23.24x -> +projection 11.36x -> +CTE 8.60x ->
+count(*) 5.10x -> +locality 5.01x -> +key injection **4.43x**.
Single-source (pure engine) SF1: 2.06x. All 132 cells correct in every phase.

## Shape fixes: q18 + q07 (commits 12f59d2, 37c413d)

- **q18 (IN group-by-having subquery)**: AggregatePushdown stopped at an
  aggregate it could not fold and never visited its input - the HAVING
  subquery aggregated 6M rows on the coordinator (the FIFTH instance of the
  walker-descent disease). Fixed with a transform_children arm; the subquery
  now collapses to a remote GROUP BY..HAVING (~60 rows). SEMI joins also
  joined the key reduction (the injected IN IS the semi condition; the
  coordinator semi stays; ANTI never injects). q18 840 -> 653ms.
- **q07 (OR spanning two relations)**: the rule derives the IMPLIED
  single-relation predicates from a multi-relation disjunction
  ((n1=F or n1=G), (n2=G or n2=F)) and pushes them to the scans; the OR
  stays for exactness. Companion fix: _push_filter_to_scan merges conjuncts
  UNIQUELY (re-derivation would otherwise grow scan filters every fixpoint
  pass). q07 416 -> 280ms; q19 held.

Gate (report-result-37c413d.md): fedpgduck SF1 4786 -> 4147ms (4.02x), all
132 cells correct. Full run: 38.55x -> 23.24 -> 11.36 -> 8.60 -> 5.10 ->
5.01 -> 4.43 -> **4.02x**. Single-source SF1: 2.05x.

## Semi-join pushdown (q18, commit 1b5c9da)

SemiJoinPushdownRule commutes SEMI/ANTI joins below the INNER join they sit
on when the semi condition references only ONE inner side
(SEMI(A JOIN B, p) == SEMI(A, p) JOIN B for p over A) - so a selective
existential (q18: an orders-only HAVING subquery keeping 57 of 1.5M orders)
runs BEFORE the fact join, and the reduced orders feed the existing
dynamic-filter reduction of lineitem. Fires only for the provable shape
(plain INNER host, one-sided condition; every gate unit-tested). Surfaced +
fixed a latent cost-model bug: _scan_needed_columns requested source stats
for a pushed-aggregate scan's HAVING columns (aggregate output aliases, not
stored columns) - now restricted to real base columns.

Gate (report-result-1b5c9da.md): fedpgduck SF1 4147 -> 3820ms (3.61x), all
132 cells correct. q18 653 -> 199ms (7.3x -> 2.3x); nothing else moved.
Nine-phase run: 38.55x -> 3.61x; single-source SF1 2.15x. 18/22 fair queries
under 5x, 8/22 under 3x.

## SEMI/ANTI cardinality estimate (q21, commit f1e4921)

Diagnosis pivoted after instrumenting the real DP: q21 was slow NOT because the
transfer model is blind to reduction, but because the SEMI/ANTI fact atom
(l1 with EXISTS/NOT-EXISTS lineitem self-joins) estimated to 0 rows, so the DP
led with it (free) and applied the selective nation filter last, shipping
98,833 rows. Root cause (cost.py _clamp_other_join_rows): matched =
min(left, inner) counts match MULTIPLICITY off the many-to-many inner-join size
and saturates to left_rows, forcing ANTI = 0. Fix: the occupancy estimate
matched = left * (1 - e^-(inner/left)) - never saturates, so an ANTI is never
spuriously empty; unchanged (~inner) for a selective semi. The DP then leads
with the pg dims on its own; NO transfer/enumerator change was needed (the
originally-planned reduction-aware ordering was abandoned - it would not have
fixed a 0-estimate).

Gate (report-result-f1e4921.md): fedpgduck SF1 3820 -> 3432ms (3.26x), all 132
cells correct. q21 everywhere: fedpgduck SF1 591 -> 172ms (4.9x -> 1.4x),
single SF1 390 -> 152ms, fedparquet SF1 305 -> 142ms. The ONLY other >40ms
delta (q16 single 160 -> 214) is subprocess noise - its plan is byte-identical
old vs new clamp. Ten-phase run: 38.55x -> 3.26x; single-source SF1 1.99x.

## LEFT-join aggregate-subquery reduction (q17, commit c0e8d43)

A scalar-aggregate subquery (l_quantity < (SELECT avg ... WHERE l_partkey =
p_partkey)) decorrelates to a LEFT join onto a GROUP-BY subquery. Reduction
used to skip it (INNER/SEMI only) so the subquery averaged ALL 200k partkeys
and shipped them, when only the 204 filtered parts were needed. Two additions:
- LEFT-join reduction: reduce only the NULLABLE right by the PRESERVED left's
  keys (identical result; fixed orientation, never reduce the preserved side).
- Composite probe: descend through pure alias/projection wrappers to the
  injectable base IF the inject column survives unchanged (else refuse); the
  aggregate base wraps as a derived table filtered on its group key, its
  binding cached in _Ctx.injected so the wrappers emit as normal coordinator
  fragments reading the reduced base. No per-emitter refactor.

Gate (report-result-c0e8d43.md): fedpgduck SF1 3432 -> 3392ms (3.16x), all 132
correct. q17 174 -> 73ms (6.6x -> 2.8x); fedparquet q17 272 -> 81ms; NO >40ms
regression anywhere (INNER/SEMI paths untouched). Eleven-phase run 38.55x ->
3.16x; single-source SF1 1.97x.

## Reduction orientation by cost cardinality (q11, commit 64b7feb)

The cost model was used ONLY for join ordering; the dynamic-filter reduction
re-decided build/probe at IR-build with a STRUCTURAL heuristic
(_probe_preference: remote>scan), which mis-sized a small dim collapsed into a
remote query - q11 shipped the 800k partsupp WHOLE and injected into the
400-row German-supplier dim (9.2x, worst ratio). Fix: carry the estimate the
optimizer already computed to the orientation.
- estimated_rows on logical Scan / PhysicalScan / PhysicalRemoteQuery, threaded
  from join ordering's atom estimate (_emit_atom annotates a Scan leaf) and the
  physical planner / single_source_pushdown.
- _orient_join (INNER) reduces the LARGER estimated side; _cardinality_probe
  declines (falls back to the old heuristic) on missing/tied estimate or a
  non-injectable larger side, so the reduction SET is unchanged. Shared
  larger_estimated_side helper also drives _choose_build_side (build smaller) so
  EXPLAIN marks the truly-reduced side. SEMI/LEFT untouched. Orientation is
  performance-only (bindings map by identity, the coordinator re-checks the key).
- A remote's orientation size is its LARGEST BASE SCAN, not the join OUTPUT: a
  multi-join output under-counts via composite-key correlation (fact island
  lineitem+partsupp+orders estimated 3522), which had flipped q09 to 1207ms -
  max base keeps a fact-containing remote from ever looking small.

Gate (report-result-64b7feb.md): fedpgduck SF1 3392 -> 3280ms (3.08x), all 132
correct. q11 142 -> 75ms (9.2x -> 4.0x), q09 467 -> 424ms, q02 down; NO >40ms
regression anywhere. Twelve-phase run 38.55x -> 3.08x; single-source SF1 1.96x.

## Composite-key NDV cap (q09, commit 05d085a)

A join's equi-key denominator is the PRODUCT of per-column NDVs, which assumes
the columns are INDEPENDENT. For a multi-column FK key the product wildly
over-counts distinct combinations: lineitem<->partsupp on (partkey, suppkey)
got 200k*10k = 2e9, collapsing a 6M-row join estimate to ~3522 (~1700x low).
Distinct key COMBINATIONS can never exceed the smaller side's rows, so the
product is capped there - ONLY for composite keys (>=2 equi conjuncts); a
single key's max(ndv) can legitimately exceed min(rows) (small table -> big
unique key) and is left alone (capping it over-estimated a 50-row FK join to
1000 - pinned by test). Shared apply_conjunct_term / cap_composite_denom in
estimate_defaults, used by both cost.py and the join-order estimator.

Gate (report-result-05d085a.md): all 132 correct, NO regression. q09 improved
in EVERY cell (a wrong estimate was costing better join orders everywhere):
single SF1 442->152ms, fedparquet SF1 1006->361ms, fedpgduck SF1 424->309ms;
q05 single 149->100ms. Totals: single SF1 1.70x, fedparquet SF1 1.99x,
fedpgduck SF1 2.98x. Thirteen-phase run 38.55x -> 2.98x fair.

NOTE: the reduction's max-base remote sizing (commit 64b7feb) is KEPT - the cap
fixes the composite-key under-count specifically, max-base still guards any
other remote-output under-count (multi-join selectivity compounding).

## DuckDB connection reuse: the per-fetch floor eliminated (fedqrs, working tree)

The "per-fetch overhead" was PROFILED, not assumed, and the earlier attribution
was wrong on every count. Measured (SF1 duck file, min of 60 runs):
- tokio `Runtime::new()`: **0.18ms**, once per query (standalone Rust bench).
  Not milliseconds; the "tokio runtime per execute" headline was noise.
- whole DataFusion coordinator path (Runtime + SessionContext + trivial plan +
  collect): **0.6ms**. The "~6ms coordinator setup" number was wrong.
- per merge fragment (its own `SessionContext`): **0.40ms**. Marginal.
- DuckDB `Connection::open`: **7-10ms PER FETCH**, and INDEPENDENT of file size
  (7.2ms on the 27MB SF0.1 file, 7.7ms on the 264MB SF1 file). It is DuckDB
  database-INSTANCE creation (built-in function/type registration), not I/O, WAL,
  or a catalog snapshot: open `:memory:` costs the same ~8ms. This was ~99% of
  the overhead, paid once per `source_scan`/`injected_scan` because
  `open_duckdb` ran per fetch.

Fix (`fedqrs/src/connectors.rs`, `duck_cursor`): a process-global cache of one
open DuckDB instance per file path, handing out cheap per-fetch cursors via
`Connection::try_clone` (a `duckdb_connect` on the already-open Arc-shared
`DatabaseHandle`, microseconds). Mirrors the PG thread-local pool and the Parquet
`SessionContext` cache. Each fetch gets its own cursor so the temp-join arm's
connection-scoped temp tables stay isolated (dropped with the cursor). Same
read-write open mode as before, so it shares configuration with any other DuckDB
handle on the file in-process.

Effect: duck fetch 7.7 -> 0.44ms; per-duck-fetch slope 7.25 -> 0.21ms/fetch.
Same-machine A/B (git-stash the one-file change, rebuild, same bench), 22/22
correct on each side, DuckDB oracle stable across both (~1090ms SF1, ~290ms
SF0.1) so the gain is ours-side:
  fedpgduck SF1    2.97x -> **2.26x**  (ours 3245 -> 2478ms, -24%)
  fedpgduck SF0.1  4.41x -> **3.27x**  (ours 1271 ->  944ms, -25%)
single / fedparquet cells are Parquet-based (cached ctx) and unaffected. The
full pytest suite is green with the fix (1169 passed). Change is UNCOMMITTED in
the fedqrs working tree; the regenerated report is report-result-e10d67b.md.

## The CBO completion round (commits 618e58d..75d1e87 + fedqrs main)

Every size-sensitive DECISION now consumes cost-model numbers; the full
decision inventory with statuses, all measurements, one negative result, and
one caught correctness regression live in perf-explore.md. The steps, each
gated on the suite + fedpgduck both scales, closing matrix all 132 correct:
- CBO-1: temporal range interpolation on a day scale, per-column INTERVAL
  pairing in conjunctions (F(upper)-F(lower), not marginal products),
  BETWEEN included. TPC-H estimates snapped to reality (q07 lineitem within
  5% of actual).
- CBO-2: PhysicalRemoteQuery carries output_estimated_rows (the island
  root's real estimate) separate from the max-base orientation floor;
  orientation, hash-build choice, and the usefulness gate consume it (q10
  runs the REVERSE reduction). fedqrs: pg full-scan fraction 0.15 (its
  fallback is the 8-way parallel read; measured break-even).
- CBO-3: single-sided condition conjuncts of LEFT/SEMI/ANTI joins move into
  the non-preserved input (q13's NOT LIKE runs in duck; pure-equi hash join).
- CBO-4: island key filters prerender INSIDE the island SQL on the owning
  relation (sources do not push semi-joins through the wrapper; q03 3x).
- CBO-5: ONE shared cost model session-wide; the planner annotates
  LEFT/SEMI/ANTI join scans regions never visit; usefulness = expected_keys
  / max(build NDV, probe NDV); injected_scan carries probe NDV so the
  runtime delivery guard prices keys/NDV (q14 -35ms).
- Parallel pg source scans (round 2): big plain pg reads go through the
  ctid-parallel path (customer wide read 136 -> 37.5ms).
- CORRECTNESS CATCH: the closing matrix (not the suite) caught q13 wrong
  rows in the single-parquet cell - the collapsed SQL hoisted the nullable
  side's filter into WHERE, dropping null-extended rows. Fixed (75d1e87):
  the filter rides the JOIN ON; FULL/NATURAL/USING decline; pinned by
  differential test. RULE: plan-shape changes gate on the FULL matrix.
- NEGATIVE RESULT: pricing the runtime reduction inside the DP transfer
  term made totals worse (q03 74 -> 180ms) - coordinator join-INPUT work
  has no cost term, so cheap transfer mis-prices island breaking. Reverted,
  documented at the call site; needs a paired input term + TRANSFER_WEIGHT
  recalibration as its own gated experiment.

Gate (report-result-75d1e87.md): fedpgduck SF1 38.55x -> **1.98x**
(2133/1079ms), SF0.1 3.16x; all 132 cells correct.

## Known gaps / next work (in priority order from the data)

Fair-federated SF1 1.98x, SF0.1 3.16x; all correct.
1. **q21/q08 in the PARQUET cells regressed** across the CBO round (q21 147
   -> ~390ms both parquet cells; fedpgduck q21 unaffected): a fully-collapsed
   single-source query is executed by DataFusion's own optimizer, which our
   rendered join order still steers - an execution model the transfer-based
   cost does not price. Reordering OFF is worse (472ms), so it is order
   choice, not reordering. See perf-explore.md item 3.
2. **Coordinator-input cost term + TRANSFER_WEIGHT recalibration** (unlocks
   reduction-aware ordering; see the negative result).
3. **Bushy same-source runs**: q02's mid-chain supplier x nation x region (3
   pg trips -> 1) and q09's orders+partsupp need an emitter that can build a
   bushy right subtree; cost side is ready.
4. **Scan SELECT lists include filter-only columns** (q13 ships o_comment
   although its filter runs in duck) - projection-pushdown gap, ~30-40ms.
5. **Per-fetch engine overhead is DONE** (duck instance reuse; residuals
   0.2-0.5ms/step are not a lever). fedqrs missing operators (xfails);
   `enable_decorrelation` unwired.
2. **q07 (6.4x), q13 (1.5M-order LEFT-count, inherent), q10**: the plan tail.
3. **Composite-key correlation is only PARTIALLY modelled**: the cap fixes the
   worst 2-column-FK case; a full multi-column correlation NDV (or a lazy
   composite approx_count_distinct) would be the complete fix.
4. **TRANSFER_WEIGHT calibration**, **CTE materialize-once**.
5. **fedqrs missing operators** (18 xfails); `enable_decorrelation` unwired.

## How to run

```
# suite + lint
POSTGRES_DB=duckpoc python -m pytest -q
make lint

# full benchmark matrix -> reports/report-result-<commit>.md
python benchmarks/tpch/bench.py

# join-order experiment (original vs hand-reordered through the same engine)
python benchmarks/tpch/experiments/joinorder_experiment.py --scale 1
```

Data prep for both scale factors: `benchmarks/tpch/README.md`. PostgreSQL:
`make pg-start`; SF0.1 lives in `duckpoc`, SF1 in `duckpoc_sf1` (ANALYZEd).
