# PRD - Materialized Event Analytics for fedq (Mixpanel/Amplitude-class)

## 1. What this is

A product-analytics capability built into the fedq engine that ingests raw
event streams once into a materialized, indexed, columnar store and serves
Amplitude/Mixpanel-class analyses - funnels, retention/cohorts, segmentation,
and flows/paths - at interactive speed, without re-querying the origin
(ClickHouse, Postgres, parquet, etc.) on every question.

It is NOT a benchmark demo. It must run on real production event data: arbitrary
actor ids, unbounded event-type cardinality, billions of rows, sub-second
timestamps. Speed comes from the storage layout, indexes, and algorithms - never
from assuming the data is small, dense, or simple.

## 2. Why (the problem)

Analysts ask exploratory event questions repeatedly and interactively: "what is
the signup -> activate -> purchase funnel this month, broken down by device?",
"what is week-4 retention for the March cohort?", "how many unique users did
add_to_cart per day in Germany?", "what paths do users take after search?".

Answering these directly against ClickHouse/Postgres/parquet on every click is
slow and hammers the source. The origin is columnar-general, not tuned for
per-actor sequence scans, funnel window logic, or cohort grids. We materialize
once into a store shaped for exactly these access patterns, and serve the
questions from it.

## 3. Users and primary use cases

- Product/growth analyst running interactive funnel/retention/segmentation
  charts and iterating (change window, add a breakdown, filter a property).
- Data engineer wiring the event source once and refreshing it on a schedule.
- The three fedq front doors (Python, PG-wire server, CLI) all reach it.

## 4. Data model

An event is `(actor, time, name, properties)`:

- `actor` (aka entity/user id): the analysis subject. ANY type - int64 (dense OR
  sparse), UUID, arbitrary string. Never assume a dense small integer range.
- `time`: event timestamp at full source precision (millisecond or microsecond).
  Ordering within the same instant is broken by a stable, configurable tiebreak
  (e.g. a sequence column), so funnel/path ordering is deterministic and correct.
- `name`: the event type, an arbitrary string. UNBOUNDED cardinality - a real
  product has hundreds to thousands of event types. No fixed cap.
- `properties`: typed key/values carried on the event (device, country, plus
  arbitrary user/event props). Analyses filter and break down by these.

The schema is declared once (which columns are actor / time / name / tiebreak;
everything else is a property), not hard-coded to a benchmark's column names.

Scale target: a single node must handle billions of events with bounded memory
(the working set spills; RAM does not grow with total row count).

## 5. Capabilities (the analytics surface)

Each analysis returns exact results and must be cross-checkable against a plain
SQL computation of the same definition (the correctness oracle).

### 5.1 Funnels
- Ordered steps (2..N event names) with a conversion WINDOW (e.g. within 7 days
  of entering the funnel).
- Per-step: entities that reached the step, conversion-from-previous,
  conversion-from-start; overall conversion; median/percentile time-to-convert
  between steps.
- Ordering modes: "this order" (strict sequence) and "any order" (Amplitude's
  unordered) - at least strict in phase 1, unordered flagged.
- Window anchoring and re-entry rules stated precisely (a later first-step event
  can start a fresh attempt); one entity counted once per step at max depth.
- FILTER (only events/actors matching a property predicate) and BREAKDOWN
  (split the funnel by a property, e.g. device) - a funnel per property value.

### 5.2 Retention and cohorts
- Cohort = actors whose BIRTH event (first-ever, or first occurrence of a named
  event) falls in a calendar period (day/week/month).
- Retention = distinct actors in a cohort who performed a RETURN event (any
  event, or a named one) in a later period, as a rate over cohort size.
- Both the full cohort x period-offset GRID and a single N-period retention
  number. BOUNDED (exactly period k) and UNBOUNDED/rolling (period k or later).
- Optional breakdown by a property.

### 5.3 Segmentation (event volume over time)
- A measure over time buckets (hour/day/week/month): event totals, unique
  actors, events-per-actor, and similar.
- Optional event filter (a specific event name) and property predicates.
- GROUP BY / breakdown by a property (device, country, ...) - the Amplitude
  Segmentation chart.

### 5.4 Flows / paths
- Top-N most common event sequences actors take (depth-limited, consecutive
  duplicates collapsed).
- "Starting at <event>" (forward flow) and "ending at <event>" (backward flow)
  variants; a Sankey-style top-flows shape.
- Optional property filter.

## 6. Materialization and indexing (the core of "fast as fuck")

- BUILD ONCE from the source (a connector or a defining SELECT) into a persistent
  store; queries read the store, never the origin.
- The store is columnar, compressed, and carries whatever indexes make the four
  analyses fast. Total implementation freedom on structure: per-actor ordered
  event sequences (CSR/segment layout), per-event-name inverted indexes / roaring
  bitmaps, per-time-bucket pre-aggregates, per-property dictionaries and indexes,
  zone maps, etc. Use any crate/library/algorithm.
- Actor ids are hashed/dictionary-encoded to dense internal ids (handles sparse/
  UUID/string ids). Event names and property values are dictionary-encoded
  (handles unbounded cardinality). Timestamps keep full precision with a tiebreak.
- REFRESH: incremental append of new events (the common case is append-only
  streams); a full rebuild path exists. Freshness is explicit/user-controlled.
- Memory-bounded build that spills to disk; the build does not require the whole
  dataset resident in RAM.

## 7. Performance requirements (targets, to be validated by measurement)

Measured on the reference 100M-event dataset, single node, and reasoned about for
billions:

- BUILD/ingest: fast enough to materialize 100M events in seconds (single-digit
  to low-tens), bounded RAM (a few GB, not tens); ingest throughput reported in
  events/sec so billions is a stated multiple, not a guess.
- QUERY latency (interactive): each analysis in roughly <= 1-2s on 100M for a
  typical query; pre-aggregated segmentation sub-second. Report cold and warm.
- Peak RSS bounded and reported for both build and query. No blow-up
  proportional to raw data size.
- Every number comes from a measured run, never a claim. Cold and warm both.

## 8. Non-negotiables (what makes it not a toy)

1. ANY actor id type and distribution (sparse int, UUID, string) - via hashing,
   never a `id - min` array.
2. UNBOUNDED event-type and property cardinality - via dictionaries, no bit-width
   cap (no "32 events" limits).
3. Scale to billions of rows - bounded-memory, spilling build; not "everything
   packed in a RAM index."
4. Full timestamp precision + deterministic tiebreak - correct ordering, or the
   funnel/path answers are wrong.
5. EXACT correctness - every analysis matches a plain-SQL oracle of the same
   definition on the same data. A fast wrong answer is the worst outcome.
6. Loud failure - unsupported input or a violated contract raises a clear typed
   error; never a silent wrong result.

## 9. Integration (plugged into fedq)

- A first-class fedq extension, driven through the runtime, reachable from all
  three front doors (Python, PG-wire server, CLI).
- A SQL/statement surface to define the store and run the analyses (e.g. a
  materialized event dataset plus FUNNEL / RETENTION / SEGMENT / PATHS
  statements, or table-function equivalents) - final shape decided in the tech
  design. It must be usable as SQL, not only a Rust API.
- Reuses the engine's connectors for ingestion and its config/session model.

## 10. Correctness and testing

- An oracle harness: each analysis computed by plain SQL (DuckDB) over the same
  events is the ground truth; the materialized engine must match it exactly on a
  battery of queries (varied windows, breakdowns, filters, edge cases: empty
  cohorts, single-event actors, same-instant events, null properties).
- Adversarial / property-based tests on ordering, dedup, window boundaries.
- Generality note: the reference dataset is dense-id / 20-event / 100M, so it
  proves SPEED but not the general limits. The mechanisms that provide generality
  (id hashing, name/property dictionaries, spilling, sub-second tiebreak) are
  validated directly with targeted tests / small crafted inputs; the design
  states plainly what the benchmark proves vs. what is validated by mechanism.

## 11. Phasing

- Phase 1: the materialized indexed store (general, no caps) + Funnels +
  Segmentation + Retention/cohorts + Flows, single node, over the parquet source,
  proven fast AND correct AND general (mechanisms), driven as SQL through fedq.
- Phase 2: incremental/real-time refresh, more chart types (property funnels
  unordered mode, lifecycle, stickiness), richer property predicates.
- Phase 3: distribution/sharding across nodes; S3-backed store.

## 12. Out of scope (phase 1)

Distributed execution; a UI; streaming/real-time sub-second freshness; ML/predictive
analyses. The store and the four analyses, correct/fast/general on a single node,
come first.
