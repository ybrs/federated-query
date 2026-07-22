# Tutorial: event analytics over a parquet file, via the server

You have a parquet file of events. This walks through running the fedq
PostgreSQL-wire server, registering the parquet with the dynamic catalog,
materializing an event dataset once, and running funnel / retention /
segmentation / path queries from any PostgreSQL client (psql shown here).

Every command below is a real, working command - copy them and substitute your
own paths.

## 0. Your data

The parquet must have, per event row: an actor/user id column (any type - int,
uuid, string), a timestamp column, an event-name column, and any property
columns you want to filter or break down by (device, country, ...). An optional
sequence column gives a deterministic tiebreak for events at the same instant.

The example below uses the bundled 1M-event file. Its columns are
`entity_id, ts, event_name, seq, device, country`.

The parquet connector reads a DIRECTORY of parquet files and exposes each file
`<name>.parquet` as the table `main.<name>`. So a directory holding
`events.parquet` becomes the table `main.events`.

## 1. Build the server

```
export CARGO_TARGET_DIR=/workspace/federated-query/target
cargo build --release -p fedq-server
```

The binary is `target/release/fedq-server`.

## 2. Write a bootstrap config

A minimal config declares no static sources - you add them at runtime with the
dynamic catalog. The engine's state (the datasource catalog, the materialized
event store, learned stats) hangs off the config file's path stem, so put the
config where you want that state to live.

```
mkdir -p ~/events && cd ~/events
printf 'datasources: {}\n' > fedq.yaml
```

## 3. Start the server

```
fedq-server --config ~/events/fedq.yaml --listen 127.0.0.1:5433
```

It prints `fedq-server listening on 127.0.0.1:5433`. With no users configured it
runs in trust mode (any username connects, no password). To require
authentication, create users - see section 9.

## 4. Connect with psql (or any PostgreSQL client)

In another terminal:

```
psql "host=127.0.0.1 port=5433 user=analyst dbname=fedq"
```

Any driver works (psycopg, JDBC, Go pgx, ...) - it is the PostgreSQL wire
protocol.

## 5. Register the parquet with the dynamic catalog

```sql
CREATE DATASOURCE ev TYPE parquet
  WITH (dir = '/workspace/federated-query/benchmarks/events/data/events_small_parquet');

SHOW DATASOURCES;
```

`SHOW DATASOURCES` lists it:

```
 name |  type   | origin  |                 summary                 |        created
------+---------+---------+-----------------------------------------+-----------------------
 ev   | parquet | dynamic | dir=/.../events_small_parquet           | 2026-...
```

The source is persisted, so a later server restart over the same config still
sees it. `DROP DATASOURCE ev` removes it.

## 6. Materialize the event dataset (build once)

This reads the events once and builds the indexed store. Every analysis
afterwards reads that store - you never rebuild to run a different query.

```sql
CREATE EVENT DATASET web FROM ev.main.events
  ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq
  PROPERTIES (device, country)
  WITH (refresh_key = 'seq');
```

It reports the build:

```
CREATE EVENT DATASET: 1000000 events, 50000 actors, 64 shards, ...,
  build 0.187s (partition 0.086s, finalize 0.097s), ...
```

At 1M this is ~0.2s; at 100M it is ~28s end-to-end, including the paths
index (a one-time cost). `ACTOR/TIME/EVENT` name the role columns, `TIEBREAK`
orders same-instant events, `PROPERTIES` lists the columns you can filter or
break down by, and `refresh_key` is the monotonic column used to append new
events later. Declare ONLY the properties you need: every listed column is
ingested and stored, so a wide string column you never filter on just makes
the build slower.

## 7. Run the analytics

Four statements, four questions. All of them run over the same materialized
dataset; none of them re-reads the parquet. To make each one concrete, take
this stream for one actor (consecutive rows of the same event collapse only
where a statement says so):

```
actor 42:  ts 09:00 page_view
           ts 09:01 view_item
           ts 09:02 view_item      (consecutive duplicate)
           ts 09:05 add_to_cart
           ts +8d   purchase
```

### FUNNEL - "of the actors who did A, how many went on to B, then C?"

Ordered conversion within a time window, counted once per actor (first
qualifying attempt wins). Use it for signup flows, checkout flows, feature
adoption: any "step 1 -> step 2 -> step 3" question.

```sql
FUNNEL ('page_view', 'view_item', 'add_to_cart') ON web WINDOW 7 DAY;
```

Actor 42 enters at `page_view` (09:00), converts to `view_item` (09:01) and
`add_to_cart` (09:05) inside the 7-day window, so it counts in all three
steps; the duration columns aggregate the per-step gaps (60s, 240s).

```
 step_index |  step_name  | entered | conversion_from_previous | conversion_from_start | median_seconds_from_previous | p90_seconds_from_previous
------------+-------------+---------+--------------------------+-----------------------+------------------------------+---------------------------
          1 | page_view   |   47913 |                      1.0 |                   1.0 |                              |
          2 | view_item   |   30583 |                   0.6383 |                0.6383 |                     182440.0 |                  512253.2
          3 | add_to_cart |    8020 |                   0.2622 |                0.1674 |                     149100.5 |                  466860.1
```

Break down or filter (the WHERE tests event properties):

```sql
FUNNEL ('page_view', 'add_to_cart') ON web WINDOW 3 DAY BREAKDOWN BY device;
FUNNEL ('signup', 'purchase') ON web WINDOW 3 DAY WHERE country = 'US';
```

### RETENTION - "do actors come back?"

Cohorts by a birth event, return by a return event, on a period grid. Use it
for stickiness: "of the users who signed up in week X, how many were active
N weeks later?"

```sql
RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD WEEK;
```

Actor 42's birth cohort is the week of its first `signup`; its `purchase` 8
days later marks it retained at period offset 1. The result is one row per
(cohort bucket, period offset):

```
 cohort_start | cohort_size | period_offset | retained | retention_rate
--------------+-------------+---------------+----------+----------------
 2025-01-06   |        1284 |             0 |     1284 |            1.0
 2025-01-06   |        1284 |             1 |      612 |         0.4766
 ...
```

### SEGMENT - "how much activity, over time?"

A measure per time bucket: event counts, unique actors, or events-per-unique.
Use it for dashboards and trend lines.

```sql
SEGMENT COUNT ON web BUCKET DAY;                              -- events/day
SEGMENT UNIQUES ON web EVENT 'purchase' BUCKET DAY WHERE country = 'DE';
SEGMENT UNIQUES ON web BUCKET DAY BREAKDOWN BY device;        -- DAU by device
```

Actor 42 contributes 4 events to its day's COUNT bucket and exactly 1 to its
UNIQUES bucket. Un-filtered `SEGMENT COUNT` at day/week/month grain answers
from a pre-aggregate in well under a millisecond at 100M events.

### PATHS - "what routes do actors actually take?"

The top event sequences, at ANY depth (window length). Per actor the stream
is ordered by time and CONSECUTIVE duplicates collapse, so actor 42's stream
becomes `page_view -> view_item -> add_to_cart -> purchase`; a DEPTH 3
unanchored statement emits its two windows
(`page_view -> view_item -> add_to_cart` and
`view_item -> add_to_cart -> purchase`), each counted once per occurrence
across all actors.

```sql
PATHS ON web DEPTH 3 TOP 20;                            -- top routes anywhere
PATHS ON web STARTING AT 'page_view' DEPTH 4 TOP 10;    -- forward from an event
PATHS ON web ENDING AT 'purchase' DEPTH 5 TOP 10;       -- what led INTO an event
PATHS ON web DEPTH 50 TOP 10;                           -- depth is unbounded
PATHS ON web DEPTH 10 TOP 10 FROM '2026-06-17' TO '2026-06-22';
PATHS ON web DEPTH 6 TOP 10 MAXGAP 30 MINUTE;           -- session-break on gaps
```

The result is one row per STEP of each ranked path, so it pivots and joins
like a normal relation; `path_id` is the rank, `occurrences` and `share`
repeat on each of a path's rows:

```
 path_id | seq |    event    | occurrences |  share
---------+-----+-------------+-------------+---------
       1 |   1 | page_view   |        7274 | 0.00904
       1 |   2 | view_item   |        7274 | 0.00904
       1 |   3 | page_view   |        7274 | 0.00904
       2 |   1 | page_view   |        4872 | 0.00605
       ...
```

Semantics worth knowing:

- FROM/TO and WHERE use ANCHOR semantics: they are tested on each window's
  anchor event (its first event, or the ENDING AT anchor), and a qualifying
  window follows the actor past the range end. Events are never dropped from
  the stream by a range.
- `STARTING AT`/`ENDING AT` windows truncate at the stream (or MAXGAP
  fragment) boundary, so they return length >= 2 up to DEPTH.
- `MAXGAP` breaks an actor's stream where consecutive events are further
  apart than the gap, BEFORE collapsing duplicates.

Any statement with no WHERE and no MAXGAP - anchored or not, ranged or not,
at any depth - answers from the paths index built at materialize time: at
100M events, depth 4 in ~90ms, depth 10 in ~170ms, depth 50 in ~1.2s warm.
WHERE and MAXGAP forms scan the dataset (a few seconds at 100M). The index
is a few gigabytes and loads into the warm cache in the background when the
process starts; a paths query fired in the first seconds of a fresh process
waits for that one load.

## 8. Keep it fresh

```sql
REFRESH EVENT DATASET web;    -- append events past the last refresh_key watermark
SHOW EVENT DATASETS;          -- rows, build info, freshness
REBUILD EVENT DATASET web;    -- full rebuild from the source
DROP EVENT DATASET web;       -- remove the dataset and its store
```

## 9. Requiring authentication (optional)

Trust mode (section 3) is fine for a local box. To require a password, mint a
SCRAM verifier and add the user to the config before starting:

```
fedq-server hash-password --superuser analyst 'your-password'
```

This prints a user entry (name + SCRAM verifier + superuser flag, never the
plaintext):

```
    - name: analyst
      verifier: SCRAM-SHA-256$4096:...$...:...
      superuser: true
```

Nest it under a `users:` key in `fedq.yaml`. With any user configured, the server
enforces authentication;
connect with `PGPASSWORD=your-password psql ...`. Grants (`GRANT SELECT ON
DATASOURCE ...`) then control what each user can read.

## 10. Large data and memory

The build is bounded-memory and spills to disk, so it scales to billions of rows
on one node. If you have the RAM and want the fastest build, raise the in-memory
budget so the build keeps its working set resident instead of spilling:

```sql
SET events.build_memory_bytes = 17179869184;   -- 16 GiB, then CREATE/REBUILD
```

Identical results either way - the budget only trades build seconds for
resident memory. The measured 100M end-to-end build (ingest, store, paths
index, write verification) is ~28s on 12 cores.

## Other front doors

The same statements work without the server:
- CLI: `fedq --config fedq.yaml --command "FUNNEL (...) ON web WINDOW 7 DAY"`.
- Python: `import fedq; rt = fedq.Runtime("fedq.yaml"); rt.execute("...")`.
