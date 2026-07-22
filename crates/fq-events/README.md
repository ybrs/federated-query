# fq-events - Event analytics

Mixpanel/Amplitude-class product analytics over an event log, materialized once
into an indexed columnar store and queried as SQL through the fedq runtime. The
same statements work from all three front doors: the Python extension, the
PostgreSQL wire server, and the CLI.

## The model: ingest once, query forever

You build the dataset ONCE from an event source (a connector table or a defining
SELECT). That single build reads the events, hashes actors, dictionary-encodes
names and properties, and writes the sorted per-actor store plus its indexes.
Every analysis after that reads the store in milliseconds - you never rebuild to
run a different funnel, path, or breakdown. New events are folded in with
`REFRESH` (an incremental append), not a full rebuild.

An event is `(actor, time, name, properties)`:
- `actor` is the analysis subject (user/entity). Any id type: sparse int64,
  UUID, arbitrary string - hashed internally, no dense-range assumption.
- `time` is the event timestamp at full precision; ordering within one instant
  is broken by the `TIEBREAK` column, so results are deterministic.
- `name` is the event type, an arbitrary string, unbounded cardinality.
- `PROPERTIES` are columns you can filter and break down by (device, country,
  ...).

## Define the dataset

```
CREATE EVENT DATASET web FROM ev.main.events
  ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq
  PROPERTIES (device, country)
  WITH (refresh_key = 'seq')
```

- `FROM` is a connector table (or a defining SELECT) that yields the events.
- `ACTOR / TIME / EVENT / TIEBREAK` map columns onto the event roles; everything
  named in `PROPERTIES` is kept for filtering and breakdowns.
- `refresh_key` is a monotonic column used to append only new events on REFRESH.

Lifecycle:
```
REFRESH EVENT DATASET web      -- append events past the last refresh_key watermark
REBUILD EVENT DATASET web      -- full rebuild from the source
DROP EVENT DATASET web         -- remove the dataset and its store
SHOW EVENT DATASETS            -- list datasets, row counts, build info, watermark
```

## Funnels

Ordered step conversion within a time window, first-touch per actor.

```
FUNNEL ('page_view', 'view_item', 'add_to_cart') ON web WINDOW 7 DAY
FUNNEL ('signup', 'begin_checkout', 'purchase') ON web WINDOW 3 DAY
  BREAKDOWN BY device
FUNNEL ('signup', 'purchase') ON web WINDOW 3 DAY WHERE country = 'DE'
```

Result: one row per step - the step index and name, entities that reached it,
conversion-from-previous, conversion-from-start, and median/p90 time-to-convert.
A `BREAKDOWN BY <property>` returns those rows per property value. `WINDOW` bounds
the time from the first step to each later step. (Strict order is the default;
`MODE ANY_ORDER` is reserved and currently raises.)

## Retention and cohorts

Cohorts by a birth event, retention as the rate of a return event in later
periods.

```
RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD DAY
RETENTION ON web BIRTH ANY EVENT RETURN 'purchase' PERIOD WEEK
```

- `BIRTH <name>` (first occurrence) or `BIRTH ANY EVENT` (first-ever event) puts
  each actor in exactly one cohort by the period of its birth.
- `RETURN ANY EVENT` or `RETURN <name>` defines "active".
- `PERIOD DAY|WEEK|MONTH`.

Result: the cohort x period-offset grid - cohort bucket, cohort size, period
index, retained actors, retention rate.

## Segmentation (event volume over time)

A measure over time buckets, optionally filtered and broken down.

```
SEGMENT COUNT ON web BUCKET DAY
SEGMENT COUNT ON web EVENT 'purchase' BUCKET WEEK
SEGMENT UNIQUES ON web EVENT 'signup' BUCKET DAY WHERE country = 'US'
SEGMENT UNIQUES ON web BUCKET DAY BREAKDOWN BY device
```

- `COUNT` (events) or `UNIQUES` (distinct actors); `COUNT_PER_UNIQUE` for the
  average.
- optional `EVENT '<name>'` filter, a `WHERE` property predicate, and
  `BREAKDOWN BY <property>`.
- `BUCKET HOUR|DAY|WEEK|MONTH`. `COUNT` at day/week/month is served from a
  pre-aggregate and returns in well under a millisecond.

Result: one row per bucket (per breakdown value) with the measure.

## Flows / paths

Top-N most common event sequences, consecutive duplicates collapsed.

```
PATHS ON web DEPTH 3 TOP 20
PATHS ON web STARTING AT 'search' DEPTH 5 TOP 20
PATHS ON web ENDING AT 'purchase' DEPTH 5 TOP 20
```

- `STARTING AT` (forward flow) or `ENDING AT` (backward flow); omit both for
  unanchored top sequences.
- `DEPTH` bounds the sequence length; `TOP` bounds the returned rows.

Result: one row per sequence with its actor count, ordered by count.

## The WHERE predicate language

`WHERE` on any analysis is a closed predicate grammar over the dataset's declared
`PROPERTIES`: equality/inequality and `AND`/`OR`/`NOT` over property = 'value'
comparisons. An unknown property or event name raises a typed error naming the
nearest known names.

## Performance characteristics

Measured on a 100M-event dataset (single node, 12 cores):
- Build (once): ~24s, peak RSS ~4.3 GiB, store ~850 MB (bounded-memory,
  spilling; scales to billions).
- Queries (warm), all against the one built store:
  funnels ~0.2-0.6s, retention ~0.2-0.3s, segmentation sub-millisecond
  (pre-aggregate) to ~0.4s, paths ~0.3-0.7s.

Every analysis result is exact - the suite in `benchmarks/events` cross-checks
each one cell-for-cell against a DuckDB SQL oracle, including on datasets with
sparse ids, UUID string ids, and sub-second timestamp ties.

## Failure behavior

Unsupported input or a violated contract raises a clear typed error, never a
silent wrong answer: an unknown dataset/property/event name (with nearest-name
suggestions), a null in a role column, an out-of-range `DEPTH`, the reserved
`MODE ANY_ORDER`, and a per-query memory ceiling exceeded on an extreme breakdown
cardinality.
