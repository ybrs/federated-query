# fq-events: event views and FUNNEL / SEGMENT / PATHS

This crate adds event analytics to the engine: over an event-shaped table (an
entity id, a timestamp, an event name, and arbitrary property columns) you can
run FUNNELS (ordered step conversion in a time window), SEGMENTATION (a measure
bucketed by time), and PATHS (the most common event sequences).

The feature is built ON the materialized-view machinery. An EVENT VIEW *is* a
materialized view with a sort/partition contract: its rows are pulled once by
the defining SELECT, validated, sorted globally by (entity, timestamp, and an
optional tiebreak), and persisted as Arrow IPC chunks. The three analyses are
single-pass kernels over that sorted stream. Staleness is the materialized-view
refresh contract: serving always trusts the last pull; only REFRESH moves the
data forward, never a background process.

This document is the user flow end to end. Every statement here runs through
`Runtime::execute` (Rust) or `fedq.Runtime.execute` (Python); each returns an
Arrow relation.

Why this file lives here: fq-events owns the registry, the materialization
contract, and the three kernels, so the crate directory is where a user of the
feature looks first. The parser forms, the runtime dispatch, and the spec types
live in fq-parse / fq-runtime / fq-common, but the semantics documented below
are this crate's, so the tutorial is co-located with them.

--------------------------------------------------------------------------------

## 1. Configuration

A runtime is built from one YAML config. For event views you need at least one
datasource (the source the defining SELECT reads) and a config file PATH: the
view's chunk store and registry hang off the config file's stem, as
`<stem>.mv/` (chunks) and `<stem>.stats.sqlite` (the materialized-view and
event-view registries). A runtime built from an in-memory config with no file
path has nowhere to keep a store, so event DDL against it raises.

```yaml
datasources:
  warehouse:
    type: duckdb
    path: /data/events.duckdb
    read_only: true
    # change_keys declares, per schema.table, how a table changes so a plain
    # MATERIALIZED VIEW can refresh incrementally. It does NOT apply to event
    # views (they always re-pull whole; see section 3), but it is part of the
    # same DDL surface, so it is documented here.
    change_keys:
      public.web_events:
        column: event_id          # monotonic, append-only: delta append
      public.accounts:
        primary_key: account_id   # unique key: merge by key on refresh
```

`change_keys` is validated at config load. Exactly one form per table:

- `{ column: <name> }` asserts the column is monotonic and non-null (an
  `updated_at`, an append-only id); rows are only ever ADDED above every value
  seen so far.
- `{ primary_key: <name> }` asserts the column uniquely keys every row; rows may
  be inserted, updated, or deleted arbitrarily.

Declaring both for one table raises at load: a watermarked append cannot see
deletes, so the combination would ship a deleted row as current.

--------------------------------------------------------------------------------

## 2. Materialized views: CREATE / REFRESH / DROP and the delta model

An event view is a materialized view underneath, so the plain-MV DDL is the
substrate. A plain materialized view caches a SELECT's rows as chunks:

```
CREATE MATERIALIZED VIEW recent_orders AS
  SELECT account_id, total FROM warehouse.public.orders WHERE total > 0

REFRESH MATERIALIZED VIEW recent_orders
DROP MATERIALIZED VIEW recent_orders
```

A registered materialized view also serves matching queries you did NOT rewrite:
the accelerator reads its stored chunks in place of recomputing a matching
subtree. See `crates/fq-accel/README.md` for that substitution flow end to end.

REFRESH decides how to bring the view forward, in this order, and NAMES the
choice in its status row:

1. NO-OP when every base table's version token still equals the token captured
   at the last pull. Nothing is pulled and nothing is written. (Version tokens
   are read from the source before each pull, so they can only understate
   freshness, never overstate it.)
2. DELTA APPEND when the single base table declares a monotonic `column` change
   key: pull only rows past the stored high-water mark and append them as new
   chunks, never rewriting existing files.
3. MERGE when the base table declares a `primary_key`: re-pull whole and rewrite
   only the chunks whose rows changed.
4. WHOLE re-pull otherwise; the status row states the reason.

The tokens are what make the no-op cheap: over an unchanged read-only source,
REFRESH returns `REFRESH MATERIALIZED VIEW (no-op: source tokens unchanged)`
without touching the data. This is exactly why the benchmark's REFRESH step
below measures near zero milliseconds on an unchanged file.

--------------------------------------------------------------------------------

## 3. CREATE EVENT VIEW: the four roles

An event view maps the OUTPUT COLUMNS of a defining SELECT onto event roles:

```
CREATE EVENT VIEW page_events
  ENTITY user_id
  TIMESTAMP occurred_at
  EVENT event_name
  TIEBREAK sequence_no          -- optional
  AS SELECT user_id, occurred_at, event_name, sequence_no, device, country
     FROM warehouse.public.web_events

REFRESH EVENT VIEW page_events
DROP EVENT VIEW page_events
```

The roles appear in the FIXED order ENTITY, TIMESTAMP, EVENT, then the optional
TIEBREAK. A missing or misordered clause raises, naming what was expected.

- ENTITY: the actor whose event stream is analysed. Type INTEGER / BIGINT /
  TEXT-like.
- TIMESTAMP: the event time. Type TIMESTAMP (any unit) or DATE.
- EVENT: the event name matched by FUNNEL steps, SEGMENT filters, and PATHS.
  Type TEXT-like.
- TIEBREAK (optional): a column that orders events sharing an
  (entity, timestamp) pair - a source sequence number, a log offset, an
  ingestion id. Type must be an orderable EXACT type: INTEGER / BIGINT /
  TEXT-like / DATE / TIMESTAMP. No float, no bool (neither has defined
  ordering/equality semantics for sequence analysis).

The declared roles must name DISTINCT existing output columns. A role naming a
column the SELECT does not produce raises (naming the column); a tiebreak that
repeats a role column raises (it adds no ordering information); a role of the
wrong type raises (naming the expected type). Every output column that is NOT a
role column is a PROPERTY, carried along implicitly - there is no PROPERTIES
clause, because the SELECT list already declares what the view carries.

NULL in ANY role column (the tiebreak included) fails the build, naming the
view, column, and row: an event with no entity, time, or name is not an event,
and a NULL tiebreak cannot break a tie. When the build fails, NOTHING is
persisted; the view name does not come into existence.

The source can be ANY SELECT: a single table, a filtered scan, a join, even a
cross-source join. Everything inside the SELECT plans and pushes exactly like a
normal query. To exclude bad rows, filter them in the SELECT
(`... WHERE event_name IS NOT NULL`).

### The tie rule

Rows equal in every sort key are stored in UNSPECIFIED relative order, so no
analysis consults that residual order. Where an analysis needs an order among
equal-timestamp DISTINCT events (path extraction), the pinned rule is:

- the declared TIEBREAK column when the view has one, and
- EVENT NAME ascending otherwise (event name also breaks ties WITHIN one
  tiebreak value).

This is a deterministic, documented order - not a claim about the true order -
so identical data always yields identical results across rebuilds. Analyses that
can be tie-independent stay so (the funnel's strict-increase rule; the collapse
of equal-timestamp duplicates in paths).

### Cross-form guards

`REFRESH/DROP MATERIALIZED VIEW` on an event view raises and names the EVENT
VIEW form (a plain-MV refresh would skip the sort contract). `REFRESH/DROP EVENT
VIEW` on a plain materialized view raises symmetrically. `CREATE EVENT VIEW`
under a name that already resolves anywhere raises instead of shadowing.

--------------------------------------------------------------------------------

## 4. The sort contract and the staleness model

### Sort contract

An event view's chunks are GLOBALLY SORTED by (entity ASC, timestamp ASC),
extended to (entity ASC, timestamp ASC, tiebreak ASC) when a TIEBREAK is
declared. Sorting by entity first makes the stream entity-partitioned: all
events of one entity are contiguous, so every kernel is a single linear scan
holding one entity's events at a time - no hash of entity to event list, no
shuffle, memory bounded by the largest single entity. Sorting by timestamp
within the entity gives the kernels time order for free.

The contract is enforced TWICE. At BUILD, fq-events validates the roles, rejects
NULLs, and sorts (an arrow lexsort over the executed result) before the chunks
are published. At SCAN, every kernel streams through a cursor that re-verifies
monotonicity and role non-nullness as it reads and raises on the first
regression, so a kernel never returns numbers over a stream whose ordering
premise is broken (this defends against out-of-band chunk edits).

The SELECT results are re-sorted in the engine at build time, not pushed as an
ORDER BY: the engine guarantees the contract rather than trusting a remote's
collation, and the build path is off the query path, so the sort cost is paid
once per REFRESH.

### Staleness model

Serving trusts the last pull. FUNNEL / SEGMENT / PATHS and plain reads of the
view (`SELECT ... FROM page_events`) NEVER check the source. Only
`REFRESH EVENT VIEW` moves the data forward, and it always re-pulls WHOLE and
re-sorts: the global (entity, timestamp) order spans every row, so a delta
append would break the order the kernels require. An event view stores no
change-key state; only source version tokens are recorded, which drive the
no-op skip when the source is unchanged. Freshness is never silent - the
registry records `refreshed_at`, and the analyses run over exactly what a plain
SELECT of the view shows.

--------------------------------------------------------------------------------

## 5. FUNNEL

```
FUNNEL OVER <event_view>
  STEPS ('signup', 'activate', 'purchase')   -- 2..16 string event names
  WITHIN 2 HOURS                             -- SECONDS | MINUTES | HOURS | DAYS
```

Semantics (pinned, not configurable):

- An ATTEMPT starts at every event whose name equals step 1.
- Within an attempt anchored at time t0, step k matches the EARLIEST event named
  steps[k] with timestamp STRICTLY GREATER than the step k-1 match and timestamp
  <= t0 + window (window inclusive at the boundary, anchored at the step-1 event
  - the Amplitude model, not rolling).
- Ordering is NON-STRICT: events between matched steps are ignored.
- TIES NEVER ADVANCE: equal timestamps cannot satisfy "strictly greater".
- RE-ENTRY: an entity's depth is the MAXIMUM over all its attempts; each entity
  is counted once.
- `entities` at step k = distinct entities with depth >= k (non-increasing down
  the steps). `conversion_from_start` = entities[k]/entities[1];
  `conversion_from_previous` = entities[k]/entities[k-1]; a zero denominator
  yields NULL, and step 1's `conversion_from_previous` is NULL.
- A non-positive WITHIN raises. A sub-day WITHIN over a DATE timestamp raises.

Result: `step BIGINT, event TEXT, entities BIGINT, conversion_from_start DOUBLE,
conversion_from_previous DOUBLE`.

### Worked example

Input event view `ev` (ENTITY user_id, TIMESTAMP ts, EVENT name), all on
2026-01-01 except the last row:

```
user_id  ts        name
1        10:00     signup
1        10:30     activate
1        11:00     purchase
2        10:00     signup
2        13:00     activate      (too late for a 2 HOURS window from 10:00)
3        10:00     signup
3        10:20     purchase      (purchase before activate: cannot follow)
3        10:40     activate
4        10:00     activate      (never signs up)
4        10:30     purchase
5        10:00     signup
5        12:30     signup        (re-entry: a second attempt)
5        13:00     activate
5        13:30     purchase
6        10:00     signup
6        10:00     activate      (same instant: a tie, never advances)
1 (2026-01-02) 09:00 view
```

```
FUNNEL OVER ev STEPS ('signup', 'activate', 'purchase') WITHIN 2 HOURS
```

Expected output:

```
step  event     entities  conversion_from_start  conversion_from_previous
1     signup    5         1.0                    NULL
2     activate  3         0.6                    0.6
3     purchase  2         0.4                    0.6666666666666666
```

Step 1 = {u1,u2,u3,u5,u6} = 5 (u4 never signs up). Step 2 = {u1,u3,u5} = 3 (u2's
activate is past the 2h window; u6's activate ties its signup and cannot
advance). Step 3 = {u1,u5} = 2 (u3's only purchase precedes its activate).

--------------------------------------------------------------------------------

## 6. SEGMENT

```
SEGMENT OVER <event_view>
  MEASURE EVENTS | ENTITIES            -- event count vs distinct entities
  [EVENT 'signup']                     -- optional single-event name filter
  BY HOUR | DAY | WEEK | MONTH         -- UTC calendar time bucket
```

Semantics:

- MEASURE EVENTS counts events per bucket; MEASURE ENTITIES counts distinct
  entities per bucket (O(1) memory per bucket: within a bucket's substream
  entities arrive sorted, so "distinct" is "changed since this bucket's last
  contributor").
- Buckets are UTC calendar truncations (HOUR; DAY; WEEK = ISO Monday 00:00;
  MONTH = first of month). Output is ordered by bucket. Buckets with no events
  are NOT emitted (gap filling is presentation, not measurement).
- `EVENT 'name'` filters by exact event-name equality before measuring.

Result: `bucket TIMESTAMP, value BIGINT`.

### Worked example

Over the same `ev` (16 events on 2026-01-01, one `view` on 2026-01-02):

```
SEGMENT OVER ev MEASURE EVENTS BY DAY
bucket               value
2026-01-01 00:00:00  16
2026-01-02 00:00:00  1

SEGMENT OVER ev MEASURE ENTITIES BY DAY
bucket               value
2026-01-01 00:00:00  6      (u1..u6 all act on day one)
2026-01-02 00:00:00  1      (only u1 on day two)

SEGMENT OVER ev MEASURE EVENTS EVENT 'signup' BY DAY
bucket               value
2026-01-01 00:00:00  6      (u5 signs up twice)

SEGMENT OVER ev MEASURE ENTITIES EVENT 'signup' BY DAY
bucket               value
2026-01-01 00:00:00  5      (five distinct signup entities)
```

--------------------------------------------------------------------------------

## 7. PATHS

```
PATHS OVER <event_view>
  [STARTING AT 'signup']       -- optional anchor event
  MAX DEPTH <n>                -- positive collapsed-step bound
  TOP <k>                      -- positive result cut
```

Semantics:

- The path is the entity's event-name sequence starting at the anchor: the first
  event named by STARTING AT (an entity with no such event contributes no path),
  or the entity's first event when no anchor is given.
- CONSECUTIVE DUPLICATES COLLAPSE into one step (a reload storm is one step),
  which also makes equal-timestamp reorderings of the SAME event invisible.
- Equal-timestamp DISTINCT events are ordered by the tie rule (section 3): the
  declared TIEBREAK column, else event name ascending.
- The path keeps at most MAX DEPTH collapsed steps; later events are dropped.
- Identical paths count across entities (each entity contributes exactly one
  path). The TOP k most common return, ordered by entities DESC then path ASC.
- A non-positive MAX DEPTH or TOP raises.

Result: `path TEXT (names joined with " -> "), entities BIGINT, depth BIGINT`.

### Worked example (event-name tie fallback)

Over `ev` (no TIEBREAK declared, so equal timestamps order by event name):

```
PATHS OVER ev MAX DEPTH 2 TOP 10
path                  entities  depth
signup -> activate    3         2
activate -> purchase  1         2
activate -> signup    1         2
signup -> purchase    1         2
```

u1/u3/u5 all begin signup then activate (u5's double signup collapses); u4 begins
activate then purchase; u6's same-instant pair has no tiebreak, so event name
orders it activate before signup; u2 begins signup then activate too. With
STARTING AT 'signup', u4 (no signup) drops out and u6 anchors at its signup.

### Worked example (declared TIEBREAK)

When the true order of same-instant events matters, declare a TIEBREAK. Given a
`seq` column recording real order (u1: browse before signup; u2: signup before
browse), both at the same instant:

```
CREATE EVENT VIEW seq_ev ENTITY user_id TIMESTAMP ts EVENT name TIEBREAK seq
  AS SELECT user_id, ts, name, seq FROM warehouse.public.seq_events

PATHS OVER seq_ev MAX DEPTH 5 TOP 10
path                          entities  depth
browse -> signup -> purchase  1         3
signup -> browse -> purchase  1         3
```

The event-name fallback could not tell these two orders apart; the tiebreak
does. Permuting the equal-timestamp input rows yields byte-identical results.

--------------------------------------------------------------------------------

## 8. SHOW SETTINGS / SET

Every scalar tunable is inventoried; `SHOW SETTINGS` returns one row per setting:

```
SHOW SETTINGS
name                              value  default  source   mutable  description
optimizer.planning_budget_ms      100    100      default  session  Hard wall-clock budget ...
optimizer.enable_predicate_...    true   true     default  session  Push WHERE predicates ...
...
```

Columns: `name, value, default, source, mutable, description`.

- `SHOW SETTING <name>` shows one setting (an unknown name raises with the
  nearest match).
- `SET <name> = <value>` changes a session-mutable setting on the live runtime;
  it is type-checked (a bad value raises) and takes effect on the next query.
  `SET` on a static setting raises, naming why.
- `RESET <name>` restores one setting to its default; `RESET ALL` restores every
  session override.

A setting is either SESSION-MUTABLE (lives in this runtime's config, re-read per
query) or STATIC (a compile-time exec constant, a process-global env flag, or a
startup credential). The `source` column reports where the current value came
from: `default`, `config`, `env`, or `set`. There is no per-query semantic knob
for the analyses: the FUNNEL / SEGMENT / PATHS semantics above are pinned, not
tunable.

--------------------------------------------------------------------------------

## 9. Measured performance

The numbers below come from the synthetic event-analytics benchmark
(`benchmarks/events/run_events.py`; see `benchmarks/events/README.md`). The
engine runs each analysis as a post-scan kernel over the sorted materialization;
the DuckDB baseline is the honest single-source SQL a user would otherwise write
over the same file (a self-join + greedy-earliest aggregation for the funnel, a
plain GROUP BY for segmentation, a window-function reconstruction for paths).
Every engine result AGREES with its baseline signature (identical counts), so
these are like-for-like timings. Ratio is engine (warm) / DuckDB baseline; below
1.0x the engine is faster.

SMALL (1,000,000 events over 50,000 entities, 20 event types, 30 days).
Build: CREATE ~810 ms (scan + sort + chunk write + derived sidecars), REFRESH
~0 ms (no-op). Full table in `benchmarks/events/reports/events-small.md`.

| Analysis                          | Engine warm ms | DuckDB ms | Ratio |
| --------------------------------- | -------------- | --------- | ----- |
| FUNNEL common (3 frequent events) | 69.5           | 39.8      | 1.75x |
| FUNNEL selective (3 rarer events) | 52.8           | 13.2      | 4.01x |
| SEGMENT MEASURE EVENTS BY DAY     | 0.2            | 8.2       | 0.02x |
| SEGMENT MEASURE ENTITIES BY DAY   | 0.2            | 23.0      | 0.01x |
| PATHS MAX DEPTH 5 TOP 20          | 103.3          | 224.4     | 0.46x |
| PATHS STARTING AT ... DEPTH 5     | 91.8           | 240.3     | 0.38x |

MEDIUM (100,000,000 events over 500,000 entities, 20 event types, 30 days).
Build: CREATE ~222 s (scan 100M + global sort + chunk write + derived
sidecars; ~21 s of that is the sidecar pass), REFRESH ~0 ms (no-op). Full
table in `benchmarks/events/reports/events-medium.md`.

| Analysis                          | Engine warm ms | DuckDB ms | Ratio |
| --------------------------------- | -------------- | --------- | ------ |
| FUNNEL common (3 frequent events) | 6801           | 38805     | 0.18x  |
| FUNNEL selective (3 rarer events) | 6243           | 1760      | 3.55x  |
| SEGMENT MEASURE EVENTS BY DAY     | 0.3            | 342       | 0.00x  |
| SEGMENT MEASURE ENTITIES BY DAY   | 0.2            | 2003      | 0.00x  |
| PATHS MAX DEPTH 5 TOP 20          | 6949           | 20061     | 0.35x  |
| PATHS STARTING AT ... DEPTH 5     | 6620           | 21778     | 0.30x  |

At 100M events SEGMENT is answered from the pre-aggregate sidecar in
sub-millisecond (section 10), turning its 4.4x-26x deficit into a >1000x win.
The funnel and paths kernels still scan the whole sorted stream (the entity
bitmaps do not prune this event-dense view), so they beat DuckDB where DuckDB
does expensive work (the funnel self-join at 38.8 s, the paths window sorts at
~20 s) and lose on the selective funnel where DuckDB filters to the rare anchor
rows first.

Where the flat sorted-scan design is fast and where it is not:

- PATHS beats the baseline (0.30x-0.47x): the kernel is a single linear pass
  over an already entity-partitioned, time-ordered stream, while the baseline
  pays several window sorts.
- FUNNEL is competitive on frequent steps (0.18x at 100M, where DuckDB's
  self-join is expensive) but loses on the selective funnel (3.5x): the kernel
  scans the WHOLE stream, whereas the baseline filters to the anchor rows first.

## 10. Derived structures beside the chunks

A refresh builds two derived sidecars next to the chunks, versioned with the
chunk generation and recorded in the `event_views` registry; a missing or
stale-generation sidecar makes the kernel fall back to the plain scan, which
returns the identical answer (`sidecar.rs`).

- SEGMENT TIME-BUCKETED PRE-AGGREGATE (`segagg-<generation>.arrow`): per
  (calendar bucket, event name) event counts at day grain and distinct-entity
  counts at day / week / month grain, all built in the refresh's sort pass.
  SEGMENT BY DAY / WEEK / MONTH answers from it without touching the chunks;
  BY HOUR falls back to the scan. Distinct counts are stored per grain, not
  summed from days, because a distinct count does not roll up additively.
  DECISIVE: at 100M events SEGMENT drops from 4.4x-26x behind DuckDB to
  answering in sub-millisecond (the whole aggregate is a few thousand cells,
  ~47 KB), a >1000x win over both the old kernel and the DuckDB GROUP BY.

- PER-EVENT-NAME ENTITY BITMAPS (`bitmaps-<generation>.fqb`): one roaring
  bitmap of entity ordinals per event name over a dictionary of the view's
  distinct entities. A funnel reads step 1's bitmap popcount as its exact
  step-1 count and scans only the entities in `step1 & step2`; anchored PATHS
  scans only the anchor event's entities. This helps ONLY when the step events
  are ENTITY-selective (few entities ever emit them). On the synthetic
  benchmark's 100M view each entity emits ~200 events, so nearly every entity
  emits every mid-frequency event and the candidate set is ~all entities; a
  selectivity gate (`worth_pruning`) then keeps the plain scan, so the bitmap
  neither helps nor regresses the 100M funnel. At the 1M scale (~20 events per
  entity) the same funnel IS entity-selective and pruning engages. The
  granularity that would cut the dense-entity selective funnel is a per-event
  ROW index (skip to the ~8% step-event rows), not an entity index.
