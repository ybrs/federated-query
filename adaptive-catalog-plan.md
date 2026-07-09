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

- A - RECORD (get the schema right). Build the SQLite catalog + the observations
  payload from `fedqrs` + the Python persist path + read/write plumbing. NO
  behavior change. Populate by running the benchmark suite; VALIDATE the schema:
  do near-identical queries hit the same keys, does the signature collapse
  alias/constant variants, are the four tables populated with sane values. This
  phase locks the schema before anything depends on it.
- B - CONSULT (warm planning). `CostModel` + gates READ observations; cold ->
  warm within a session, then across sessions (persistence). Measure estimate-
  quality lift across the suite and the dim-shipping gate improvement (q23-shape
  now answered by measured `group_stats`). Verify 99|0|0 - trivially safe
  (perf-only).
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
