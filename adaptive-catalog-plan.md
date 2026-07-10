# Adaptive cardinality catalog - plan

The engine currently GUESSES cardinalities (estimates from source stats, with
NDV-independence and defaulting that this project has repeatedly been bitten by:
dim-shipping collapse, q23/q39 gating, q54 reduction orientation). A federated
engine is uniquely positioned to MEASURE instead - it materializes every
cross-source intermediate anyway, so the exact size and key-distribution of each
is free at runtime. This plan makes those measurements FIRST-CLASS and PERSISTS
them, so planning starts warm and the system learns its workload.

Target workload (confirmed): bimodal. Agents (and analysts) fire a burst of
small exploratory/sampling queries to learn a dataset's shape, then run heavier
queries. The two phases are NOT independent - the cheap exploration touches the
same tables/columns/filters/join-shapes the heavy queries will, so it TRAINS the
optimizer for free. Exploration warms the engine for exploitation.

---

# COURSE CORRECTION 2026-07-09 - measurements or null, never fabricated

This section is the AUTHORITATIVE plan forward. It supersedes the handoff's
"WHAT IS MISSING" list (systematic passive NDV capture; the atomic-triple
consistency guard) after the warm-regression diagnosis was verified in code and
by probe. Phases A/B/v1.5 below remain accurate as the record of what landed.

## The verified failure, restated precisely

A probe at SF1 (2 full warmups of the cold_sources subset, catalog kept and
dumped) measured what learning actually captures: 6 row counts, 0 column NDVs,
0 predicate selectivities. Structural, not incidental: `_record_scan_observation`
and `_ndv_base_scan` only fire on UNFILTERED, UNREDUCED scans, and a good plan
never reads a fact unfiltered - so a fact can NEVER be learned passively.
`inventory` therefore stays at the fabricated DEFAULT_ROW_COUNT=1000 while its
dims carry measured real counts (date_dim 73049, item 18000). The planner
compares the fabricated 1000 against the measured 73049 as if both were facts
and every larger-side decision (reduction orientation, build side, join order,
the dim-shipping fact-large gate) INVERTS. Cold-off fabricates 1000 uniformly,
which at least preserves the (unknown) relative order. That is the whole
warm-worse-than-cold mystery.

Second flavor (q23): store_sales DOES learn real rows and pairs them with a
fabricated NDV (rows * DEFAULT_NDV_FRACTION) inside max(ndv_l, ndv_r).

Both flavors are ONE disease: a fabricated constant meeting a measurement in
the same arithmetic as an equal citizen.

## The principle (this project's own rule, applied to statistics)

A statistic is either a MEASUREMENT with provenance - source catalog (ANALYZE),
learned (exact runtime observation), probed (sampled, see Phase PROBE) - or it
is NULL. Never an arbitrary constant dressed up as a number. A fabricated
default is the stats-level equivalent of a silent `except: pass`: it hides
"I do not know" behind a confident wrong answer. Unknown PROPAGATES through
estimate arithmetic (any formula over an unknown input is unknown), and every
DECISION POINT declares an explicit, documented policy for unknown inputs -
conservative, meaning "decline the optimization / keep the safe orientation",
which is exactly the cold-off behavior measured as acceptable. Model
assumptions on how to COMBINE measurements (NDV independence, containment)
remain - those are documented estimator formulas, not fabricated inputs.

Precedents already in the codebase (this is not a new invention here):
- `larger_estimated_side` returns None when either side's estimate is missing;
  callers keep their default orientation.
- `useless_key_reduction` abstains (reduce-by-default) on an unknown build NDV.
- The fact-injection round's orientation urgency treats a statless remote as
  INFINITE size - unknown = assume large - and that heuristic measurably won.

## Phase HONEST-UNKNOWNS (first; fixes the regression by construction)

Remove every fabricated stat constant. `estimate_defaults.py` keeps ONLY
TRANSFER_WEIGHT (a cost-model weight, not a statistic) and the estimator
combination helpers. Deleted, with their substitution sites:
- DEFAULT_ROW_COUNT (cost.py `_tracked_base_rows`)
- DEFAULT_ATOM_ROWS (cost.py `_estimate_rare_tracked`, CTE atoms) - a CTE atom
  is not unknowable: estimate its BODY through the estimator (the plan is right
  there), and fill from measured `subplan_stats` when the signature matches;
  only a genuinely unknown body propagates unknown.
- DEFAULT_NDV_FRACTION (cost.py x3, join_ordering.py x1)
- DEFAULT_EQ_SELECTIVITY / DEFAULT_RANGE_SELECTIVITY / DEFAULT_LIKE_SELECTIVITY
  / DEFAULT_NULL_FRACTION - selectivity is 1/NDV or min-max interpolation over
  MEASURED stats, else unknown.

`CardinalityEstimate.rows` becomes Optional[int] (None = unknown); the
`defaults_used` provenance list stays for EXPLAIN but now records which objects
made an estimate UNKNOWN rather than which default was substituted. Estimator
arithmetic null-propagates.

Decision-point unknown policies (each documented at the decision site):
- Reduction orientation: an unknown side is treated as INFINITE (reduce it,
  never reduce INTO a measured side using an unknown side's keys blindly) -
  the existing urgency precedent, now uniform.
- Hash build side: build on the known-smaller side; both unknown = keep the
  planner's syntactic choice.
- Join-order DP (`choose_order`): a region containing ANY unknown atom is NOT
  enumerated - keep syntactic order with island adjacency (the cold-off
  behavior). Rationale: a DP over mixed known/fabricated costs is confidently
  wrong (q39); a DP over all-unknown is noise. Phase PROBE makes this branch
  rare rather than making the DP unknown-tolerant.
- Dim-shipping gate: any stat the gate needs unknown = decline the ship
  (already mostly true; make it total).
- useless_key_reduction: unchanged (already abstains correctly).

Gates: full pytest; 99|0|0 at SF0.1/SF1/SF10 pg-dims + TPC-H 22/22 with NO
tally regression (the analyzed path never sees an unknown, so nothing may
move); cold_sources.py shows WARM >= COLD on every subset query (the
inversions are gone by construction because a fabricated number no longer
exists to invert against).

STATUS 2026-07-10: IMPLEMENTED (all seven constants deleted; rows Optional;
per-decision policies as above; CTE bodies estimated via a registry the
JoinOrderingRule fills during descent, reset per plan walk). Suite 1334 pass.
TWO MEASURED FINDINGS from the SF10 cold benchmark run between this phase and
PROBE:
1. The inversion class is dead: q21 warm converged EXACTLY to analyzed
   (89ms vs 88ms), q23 warm beat cold by 635ms.
2. Honest unknowns ALONE are not sufficient: q39 warm still lost to cold
   (+3231ms) because learned ROW COUNTS with no NDVs/selectivities let the
   DP enumerate on very loose bounds (date_dim priced 73049 when the true
   filtered size is 365 - its d_year selectivity was unknown), and the
   bound-ranked order lost to the written order in DataFusion. A stricter
   "decline enumeration on ANY gap" gate was probed and REJECTED: on the
   ANALYZED path 218 of 372 regions carry at least one gapped atom (CTE-body
   and expression-shape gaps), so the gate would de-enumerate 59 percent of
   the healthy path. The correct fix is better INPUTS, not a blunter gate -
   which is exactly Phase PROBE.

BOUNDS SEMANTICS (third measured finding, 2026-07-10): with the fabricated
constants gone, a gap-fed estimate is an UPPER BOUND BY CONSTRUCTION (an
unknown selectivity ceilings at 1.0, an unknown NDV floors the join
denominator at 1, an unknown group NDV clamps to input rows) - so
CardinalityEstimate.rows now means: None = unknown; gap-free value = point
estimate; gap-carrying value = upper bound. Decision points may USE bounds
for two-way comparisons: a side whose upper bound is under the other side's
measured size is PROVABLY smaller. The first implementation refused gap-fed
sizes at _scan_estimated_rows and _annotated_scan, and that regressed
q65 945->2047ms, q14 2479->3356, q51 2052->2628 on the ANALYZED path:
q65's date_dim filter (d_month_seq BETWEEN 1176 AND 1176+11, an expression
bound the interval pricer cannot place) made the estimate gap-carrying, the
orientation abstained, and the un-reduced fact shipped whole (28.8M rows vs
5.4M date-key-reduced). Fixed by stamping bounds: only truly UNKNOWN (None)
abstains. Caveat recorded: a bound passing through a COMPLEMENT (ANTI join's
left - matched) can undershoot - bounds are approximate there, matching the
epistemic status the old priors had, minus fabrication. The dim-shipping
gate KEEPS its stricter no-gap policy (pre-existing, tuned; shipping is a
bigger commitment than orientation).

## Phase PROBE (second; the only way unknown becomes a number)

IMPLEMENTED 2026-07-10 in the pg CONNECTOR, not the catalog layer:
`PostgreSQLDataSource.get_table_statistics` probes a never-ANALYZEd table
(reltuples=-1) instead of reporting unknown. This mirrors what the DuckDB
connector has ALWAYS done (its stats path runs one approx aggregate scan);
pg merely reaches parity. The StatisticsCollector's session cache bounds the
cost to one probe per table per session.

Shape as landed:
- Small table (pg_relation_size <= PROBE_EXACT_BYTES, 256MB - every SF10
  TPC-DS dimension fits, every fact exceeds it): ONE exact aggregate scan
  measuring count(*) plus per-requested-column count(DISTINCT), null count,
  min, max. Analyzed-grade inputs: exact NDVs and range bounds.
- Large table: `TABLESAMPLE SYSTEM (p)` with p sized to a
  PROBE_SAMPLE_TARGET_BYTES (32MB) block sample; the scaled count(*) is the
  row estimate. NO column stats from a sample - a block-sampled distinct
  count is biased low, and reporting it would fabricate a statistic.
- Reads the actual file size, so it works even when relpages=0 (the
  cold_sources simulation included). Kill switch: FEDQ_STATS_PROBE=0.
- Tests: tests/test_statistics_layer.py (probe measures, kill switch reports
  honest unknown, exact column stats, sampled path drops column stats).

ASK-THE-SOURCE (second probe mechanism, landed 2026-07-10): when the engine's
own predicate pricing leaves gaps on a PLAIN filtered scan (LIKE patterns,
column-vs-column ranges, expression bounds), the cost model asks the SOURCE
PLANNER for the rendered scan's row estimate - `DataSource.estimate_scan_rows`
via `EXPLAIN (FORMAT JSON)` on pg / EXPLAIN plan-text parse on DuckDB. The
source prices the predicate from ITS statistics: informed, with provenance,
never our fabricated prior. Guards, each measured-in:
- pg answers ONLY for an ANALYZEd table - a statless table would relay pg's
  own default selectivities, i.e. smuggle the fabricated priors back in; the
  cold path stays on probe + honest bounds (validated converged).
- Only PLAIN scans ask: a pushed-aggregate scan's filter is a HAVING over
  aggregate OUTPUT names - invalid as a WHERE at the source.
- The EXPLAIN is a CAPABILITY PROBE: a predicate shape the rendering cannot
  express for that source (engine-evaluated functions like age()) abstains
  (pg rolls back to unpoison the pooled connection) - the query itself still
  executes via its normal path.
- Cached per (datasource, rendered SQL) on the StatisticsCollector - one
  round trip per distinct scan per session, however often the DP re-estimates.
WHY IT EXISTS: removing the priors regressed TPC-H q09 136->896ms (the LIKE
prior 0.1 was load-bearing: part priced at its 200k bound stopped the
dim-ship, breaking the one-island collapse) and q21 144->539ms (the
column-vs-column range prior shaped the DP order that kept nation+supplier
one pg island). With the source's estimate both restored to baseline
(q09 136ms, q21 152ms). Tests: test_statistics_layer.py (pg analyzed answers
/ statless refuses / bad SQL abstains + pool survives; duck parse incl.
singular "~1 row").

DEFERRED from the original sketch (add only if measurement demands):
- UNIQUE/PK-metadata NDVs (dims are small enough for the exact path; the
  unique-column shortcut only matters for facts, whose keys are not unique).
- Catalog persistence of probed values under a 'provenance' column (the
  session cache already amortizes; cross-session persistence is a latency
  refinement, and learned exact observations land in the catalog anyway).

Gates: cold_sources.py gained the ANALYZED(ms) reference column; the
scoreboard criterion is WARM (and now OFF too - the probe needs no catalog)
CONVERGING to ANALYZED per query, not merely beating COLD. Plus the standing
correctness harness.

## Phase REFINE (third; existing passive machinery, now on honest ground)

ALL THREE LANDED 2026-07-10:
- predicate_stats feed - DONE. A filtered, UNREDUCED plain scan records its
  measured output rows keyed by the filter's CONSTANT-NEUTRAL template
  (`scan_predicate_template`: sorted conjunct shapes, alias-neutral, constants
  dropped - reusing the subplan-signature machinery). The persist step joins
  the catalog's own base count so the stored selectivity ratio is
  measurement-over-measurement; an unknown base stores the output alone.
  READ: `_scan_filter_rows` consults the learned output BEFORE the source
  planner (a measurement of this very predicate beats the source's guess),
  and only when the engine's own pricing left gaps - fill, never override.
  A reduced (injection-shrunk) scan's output is excluded: recording it would
  learn a lie. Verified live: a warm SF1 session records d_year eq at
  0.49966 percent, d_date BETWEEN at 0.0835 percent, plus templates whose
  base was unknown (output rows only).
- Dim-shipped island group counts - DONE. dim_shipping stamps
  `group_observation` {pre-ship subject, group columns} on the island
  RemoteQuery - the SAME key the cost model and the gate read - but ONLY when
  the wrappers above the aggregate are row-count-preserving
  (Projection/Sort/SubqueryScan; a Filter or Limit above would make the
  island's row count NOT the group count). rust_ir records the island's
  materialized rows as the measured group count.
- Gate consumption - DONE. `_dimension_explosion` prefers the MEASURED group
  count when a previous run (island- or coordinator-side) recorded this
  aggregate's shape: explosion = measured output above
  SHIP_COLLAPSE_MAX_FRACTION (0.1) of the aggregate's estimated input rows.
  Self-correcting BOTH ways: a bad ship records its big island output and
  declines next run; a declined ship records its coordinator group count and
  may unlock next run. The width heuristic remains the unmeasured fallback.
  Tests: tests/test_dim_shipping_gate.py (measured overrides in both
  directions; the island stamp keys identically to the gate's read).

---

## The safety property (why this whole layer is low-risk)

A learned observation can only change WHICH PLAN the optimizer picks - never the
answer. Everything it feeds (reduction orientation, the dim-shipping gate, join
order, build-side choice, injection delivery) is correctness-neutral: the join /
aggregate is always computed correctly regardless of the estimate. Worst case
from a stale or wrong observation is a SLOW query, never a wrong one. So we learn
aggressively and invalidate lazily.

## Two stores, two access patterns

- CATALOG (this doc) - the brain. Stats + a registry of what the accelerator has
  materialized. Access is OLTP: frequent tiny upserts (during execution) and
  point lookups (during planning), single-process. Store: SQLite (Python via the
  stdlib `sqlite3` - ZERO new dependency; Rust via `rusqlite`, Phase C only).
  Future: PostgreSQL for concurrency / server mode - deferred, not a dependency
  now.
- ACCELERATOR (separate, later) - the muscle. Materialized remote data (large
  scans). Store: DuckDB / Parquet / Arrow (OLAP). The catalog TRACKS accelerator
  fragments (signature, freshness, file location); the DATA lives in the fast
  tier, never in SQLite.

## Scope and identity

One catalog file per fedq CONFIGURATION (the config that defines which sources
connect under which datasource names). Stats key on `(datasource, schema, table,
...)`; the config fixes what each datasource name points at, so the keys are
stable. A different config uses a different catalog file - no cross-contamination.
Default the catalog path to a state dir derived from the config; overridable.

Schema carries a `source_fingerprint` per datasource (a hash of the connection
target) so a datasource repointed at a different database under the same config
can be DETECTED and its stale stats distrusted. v1 trusts config stability; the
column exists so no migration is needed to enforce it later.

## Freshness: self-heal on write + TTL on read

There is NO separate drift-detection pass. Every execution measures the truth and
UPSERTS it, so a stat is never staler than "the last time this shape ran" - the
catalog self-heals on use. TTL only guards stats that go UNUSED for a long time:
a read ignores any observation older than TTL and falls back to source stats,
then named defaults. The only lag window is "the source changed AND no query
re-measured that shape"; TTL bounds it, and it is correctness-safe regardless.
`observation_count` / `last_seen` give a confidence signal for tie-breaks.

## Schema (SQLite)

All keys use the datasource NAME from config. Value/JSON columns use `json1`.
`min_val` / `max_val` store the typed value serialized as text.

```sql
-- Per-datasource identity, for the repoint-detection slot.
CREATE TABLE source_identity (
  datasource        TEXT PRIMARY KEY,
  source_fingerprint TEXT,          -- hash of connection target; NULL in v1
  first_seen        TEXT,
  last_seen         TEXT
);

-- 1. Base per-table / per-column stats (the learned pg_statistic analogue).
CREATE TABLE table_stats (
  datasource   TEXT, schema_name TEXT, table_name TEXT,
  column_name  TEXT,                -- NULL => table-level row count
  measured_rows     INTEGER,        -- table-level, base (no filter)
  measured_ndv      INTEGER,        -- column-level (collect_distinct is exact)
  null_fraction     REAL,
  min_val TEXT, max_val TEXT,
  observed_at  TEXT, observation_count INTEGER DEFAULT 1,
  PRIMARY KEY (datasource, schema_name, table_name, column_name)
);

-- 2. Filter selectivity, conditioned on a predicate TEMPLATE.
CREATE TABLE predicate_stats (
  datasource TEXT, schema_name TEXT, table_name TEXT,
  predicate_template TEXT,          -- constants -> $1,$2: "d_month_seq BETWEEN $1 AND $2"
  param_bucket TEXT,                -- optional value-sensitive bucket; NULL = template-wide
  measured_input_rows INTEGER, measured_output_rows INTEGER, selectivity REAL,
  observed_at TEXT, observation_count INTEGER DEFAULT 1,
  PRIMARY KEY (datasource, schema_name, table_name, predicate_template, param_bucket)
);

-- 3. Group-by output cardinality - the correlation killer (cannot be estimated,
--    trivially measured). Retires the dim-shipping collapse heuristic.
CREATE TABLE group_stats (
  subject TEXT,                     -- table name OR subplan_signature
  group_key_set TEXT,               -- sorted JSON array of columns/exprs
  measured_group_count INTEGER, measured_input_rows INTEGER,
  observed_at TEXT, observation_count INTEGER DEFAULT 1,
  PRIMARY KEY (subject, group_key_set)
);

-- 4. Join / subtree output cardinality + output key NDVs, by canonical signature.
CREATE TABLE subplan_stats (
  subplan_signature TEXT PRIMARY KEY,
  measured_output_rows INTEGER,
  output_key_ndv TEXT,              -- JSON map column -> observed NDV
  observed_at TEXT, observation_count INTEGER DEFAULT 1
);

-- 5. Registry of accelerator-materialized fragments (populated much later).
CREATE TABLE materialized_fragments (
  subplan_signature TEXT PRIMARY KEY,
  location TEXT,                    -- path to the DuckDB/Parquet fragment
  measured_rows INTEGER, materialized_at TEXT, source_fingerprint TEXT
);
```

## Signatures (start conservative, loosen later)

- `predicate_template`: render the predicate with constants replaced by ordered
  placeholders ($1, $2, ...). `param_bucket` stays NULL unless a filter's
  selectivity is strongly value-dependent (dates, ranges) - add bucketing only
  where measurement shows it matters.
- `subplan_signature`: a canonical hash over the normalized subplan - sorted base
  tables, the join graph (conditions as templates), and filters as templates -
  MODULO alias names and constant values. Two structurally-equivalent subplans
  hash equal. Start with exact-structural matching; loosen (e.g. subsumption)
  only when measured reuse demands it. Reuse the existing alias-neutral scan
  identity work (CSE `_scan_share_key`) as the starting point.

## Write path (phased to avoid early coupling)

- Phase A/B: `fedqrs` ALREADY computes per-step rows / distinct-key counts /
  group counts (that is what `FEDQRS_PROFILE` prints to stderr). Formalize those
  into a structured OBSERVATIONS payload returned alongside the Arrow result
  (per step: kind, output binding, measured rows; for `collect_distinct` the
  key NDV; for `aggregate` the group count). Python persists them to SQLite.
  No `fedqrs` -> SQLite coupling yet.
- Phase C: `fedqrs` gains READ access to the catalog (`rusqlite`) for runtime
  decisions, and later write access as the decision layer migrates to Rust.

## Read path

`CostModel.estimate` and the gates consult the catalog FIRST, then source stats,
then named defaults - provenance recorded so EXPLAIN can surface
`stat=learned[...]` vs `stat=source[...]` vs `stat=defaulted[...]`. Consumers:

- base rows / NDV -> `table_stats`
- filter selectivity -> `predicate_stats`
- collapse gate (dim shipping / eager aggregation) -> `group_stats`
- reduction orientation / join order / build-side -> `subplan_stats`

## Phases

- A - RECORD (get the schema right). DONE 2026-07-09 (branch
  feature/adaptive-catalog). SQLite catalog (`catalog/stats_catalog.py`, stdlib
  sqlite3, self-heal + TTL, 10 tests); engine returns (stream, observations)
  = (binding, rows) per materialized step (fedqrs `engine::execute` /
  `execute_ir`); `build_ir_with_observations` records the binding->provenance
  map; `execute_via_rust`/`Executor` persist off the critical path
  (`_persist_observations`), None by default = no behavior change (1310 pass).
  Validated end to end at SF10: q54/q39/q70/q03/q07 populate correct base row
  counts - item=102000, store=102, and warehouse=10 (the exact stat pg lacked
  that broke the dim-shipping gate), with observation_count self-healing.

  COVERAGE FINDING (drives v1.5 priority): v1 source-side observations are
  SPARSE. `table_rows` fires only for an UNFILTERED base scan (mostly small
  dims); `column_ndv` rarely fires because a collect_distinct almost always runs
  over a REDUCED (injected) or FILTERED scan, whose distinct count is NOT the
  base column's domain and is correctly refused (q54 was briefly mis-recording
  store_sales.ss_sold_date_sk=48 vs the real 1823 - fixed). So v1 mainly learns
  DIMENSION base row counts. The real coverage - and the estimator wins - live
  in v1.5: `predicate_stats` for filtered scans (very common) and merge-side
  `group_stats` / `subplan_stats`. Prioritize v1.5 (DataFusion output-row metric
  harvest) right after Phase B, or interleave it.
- B - CONSULT (warm planning). DONE 2026-07-09. `StatisticsCollector` overlays
  learned row counts / NDVs (`_overlay_learned`); `FedQRuntime` opens ONE
  `StatsCatalog` when `FEDQ_STATS_CATALOG` names a path and shares it across the
  read path (cost model) and write path (executor), None = off. Validated: a
  cold session populates, a warm session reads back warehouse=10 / item=102000.
  99|0|0 at SF10 with learning on. TWO findings from measuring the read path on:
  (1) FILL-ONLY, not prefer-learned - overriding present source estimates
  destabilizes the reduction/orientation decisions tuned against them (+5s at
  SF10); fill only the gaps the source left. (2) BATCH the writes - a per-upsert
  commit fsynced on the timed execute path (+50ms/query); one commit per query +
  WAL/synchronous=NORMAL cut it to ~+4ms/query. The dim-shipping-gate improvement
  from measured group_stats waits on v1.5 (this phase reads only v1's table_rows
  / NDVs).
- C - ADAPT (runtime decision points). Move reduction orientation / usefulness /
  build-side to decisions made AT execution time from observed cardinalities of
  already-executed inputs - the real adaptive layer, and the first slice of
  decision logic to migrate into Rust (`rust_ir.py`'s orientation logic is the
  natural target). Static plan + runtime decision points; NOT mid-query
  re-optimization.
- D - STALENESS hardening. TTL tuning, `source_fingerprint` population and
  repoint detection.
- Later - ACCELERATOR (materialized fragments registered in table 5) and PLAN
  CACHING (keyed on normalized SQL + catalog version) for the many-small-queries
  latency floor.

## Verification

Every phase stays behind the 99|0|0 correctness harness (SF0.1/SF1/SF10 pg-dims +
adversarial, TPC-H). Phase A: schema populated by the suite + a key-collision /
signature-stability check. Phase B: an estimate-quality metric (predicted vs
measured rows, logged), the q23-family gate check, and 99|0|0. Phase C:
per-decision correctness + the perf tally.

## Observations payload (write-path interface) - DESIGNED

Core principle: the engine reports ONE number per step - its output row count -
and Python decides what each number MEANS. The engine stays dumb (no knowledge
of catalog keys, tables, or templates); all semantics live in a Python-side
provenance map. This keeps the Rust<->Python surface tiny and stable.

Interface:

```
fedqrs.execute_ir(ir_json)  ->  (arrow_stream, observations)
# observations: [(binding_name, output_rows), ...]   -- dozens of entries, small
```

Always on (counting already-materialized batches is negligible), no flag;
`execute_via_rust` is the only caller, so it unpacks a tuple.

Correlation - `build_ir` records `binding -> provenance` as it emits each
observable step; provenance is the catalog target for that binding's row count:

```
source_scan(base, no filter)        -> table_stats(ds, schema, table).measured_rows
source_scan/injected(base + filter) -> predicate_stats(..., predicate_template)
collect_distinct(key -> base col C)  -> table_stats(...C).measured_ndv  # rows == NDV
```

The key `collect_distinct` traces to its base column via the SAME injection-base
tracing the reduction already uses (`_probe_injection_bases`). After execute, a
`CatalogWriter` joins `observations` with the provenance map and upserts - OFF
the critical path (after the result is returned), so it adds no latency.

THE LAZY-FUSION WRINKLE (phasing): merge fragments report rows=0 today (fusion
streams rows past the fragment boundary without counting). So:
- v1: source-side only - `source_scan` / `injected_scan` rows and
  `collect_distinct` NDVs. These are explicitly materialized and counted, and
  they are exactly what hurt us: EXACT NDVs replace the missing/wrong pg_stats
  NDVs that broke the dim-shipping gate, and base row counts fix the orientation
  family. No engine execution change needed.
- v1.5: merge-side (`group_stats`, `subplan_stats`) by harvesting DataFusion's
  built-in per-operator `output_rows` metric (`MetricsSet`) after a region runs
  and mapping operators back to logical fragments. Real but contained Rust work.

FORWARD-COMPAT (Phase C): when Rust must READ the catalog at execution time to
make runtime decisions, promote the provenance into the IR - each observable
step carries an `"observe": {...}` tag the engine echoes back - so Rust keys the
catalog inline. v1 keeps provenance Python-side; design the provenance struct now
to be IR-serializable (plain fields: target_kind, datasource, schema, table,
column, predicate_template) so there is no interface churn later.

## Open items to close before Phase A

- `source_fingerprint` definition per connector (connection-target hash).
- Catalog path default location (a state dir derived from the config).
- v1.5 metric-harvest mechanism: confirm DataFusion `MetricsSet` exposes
  per-fragment `output_rows` under fusion, or add lightweight CountExec wrappers
  at logical fragment boundaries.
