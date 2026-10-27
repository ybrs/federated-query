# Firebase Parquet exports from the CLI

A Parquet directory registers one table per file. A view combines the daily
files with UNION ALL; the files stay in place. The event dataset is built from
that view and supplies paths, funnels, and retention.

An alternative with no view: register the matching daily files as ONE table,
with `glob: '/data/firebase-export/events_intraday_*.parquet'`
or a `manifest` file listing one path per line. The engine merges the files'
schemas by column name (a column absent from a file reads as NULL) and folds
their footer row counts and statistics, so the event dataset reads one table.
The file set is fixed at registration, so re-register the source to include a
newly exported day.

Run these commands from the repository root with the intended Python virtualenv
activated. `make build` builds the CLI after installing the project dependencies.

## Register the export

```bash
mkdir -p event-analytics/firebase
cat > event-analytics/firebase/fedq.yaml <<'YAML'
datasources:
  firebase:
    type: parquet
    dir: /data/firebase-export/
YAML

target/release/fedq --config event-analytics/firebase/fedq.yaml \
  --command 'SHOW TABLES FROM firebase'
```

For example, `events_intraday_20260313.parquet` is available as
`firebase.main.events_intraday_20260313`. The manifest is not a table. List and
struct columns retain their nested Arrow schemas.

## Combine daily tables

Generate one view statement from filenames. The helper reads directory entries;
the engine validates every referenced table and column when creating the view.
Explicit columns keep the view valid when later exports add unrelated columns.

```bash
"$VIRTUAL_ENV/bin/python3" scripts/parquet_union_view.py \
  /data/firebase-export/ \
  --source firebase --view firebase_events \
  --pattern 'events_intraday_*.parquet' \
  --columns user_pseudo_id event_timestamp event_name platform geo device \
    event_params event_bundle_sequence_id batch_event_index \
  > event-analytics/firebase/create_view.sql

target/release/fedq --config event-analytics/firebase/fedq.yaml \
  --file event-analytics/firebase/create_view.sql

target/release/fedq --config event-analytics/firebase/fedq.yaml
```

In the interactive shell:

```sql
SELECT event_name, count(*) AS events
FROM firebase_events
GROUP BY event_name
ORDER BY events DESC
LIMIT 20;

SELECT user_pseudo_id, event_name, geo.country AS country,
       device.operating_system AS os
FROM firebase_events
LIMIT 10;
```

The view persists beside the config. Regenerate its definition to include newly
added files. Rebuild the event dataset after changing its input.

## Query nested parameters

FROM UNNEST returns one row per list element. WITH ORDINALITY numbers elements
from one. LEFT JOIN keeps events that have no matching parameter.

```sql
SELECT e.user_pseudo_id, e.event_name,
       p.param.key AS parameter,
       p.param.value.string_value AS text_value,
       p.param.value.int_value AS integer_value,
       p.position
FROM firebase.main.events_intraday_20260313 AS e
CROSS JOIN UNNEST(e.event_params)
  WITH ORDINALITY AS p(param, position)
LIMIT 20;

SELECT e.user_pseudo_id, e.event_name,
       p.param.value.string_value AS screen_class
FROM firebase.main.events_intraday_20260313 AS e
LEFT JOIN UNNEST(e.event_params) AS p(param)
  ON p.param.key = 'firebase_screen_class'
LIMIT 20;
```

Repeated matching keys yield repeated rows. Aggregate or otherwise resolve
parameters before promoting them into an event dataset, so parameter expansion
does not count one event multiple times.

SELECT-list UNNEST calls zip lists and pad shorter lists with NULL. Independent
FROM UNNEST calls form a Cartesian product. Empty and NULL lists produce no
rows in ordinary expansion; outer expansion preserves a row with NULL values.
Missing fields and scalar UNNEST inputs raise.

## Build one event dataset

This definition uses scalar properties from the structs and keeps one input row
per event. Firebase timestamps are integer microseconds. The sequence orders
simultaneous events by their bundle, batch position, and event name.

```sql
CREATE EVENT DATASET app_events AS (
  SELECT user_pseudo_id AS actor,
         event_timestamp AS ts,
         event_name AS event,
         CAST(ROW_NUMBER() OVER (
           PARTITION BY user_pseudo_id, event_timestamp
           ORDER BY event_bundle_sequence_id, batch_event_index, event_name
         ) AS BIGINT) AS sequence,
         platform,
         geo.country AS country,
         device.operating_system AS os
  FROM firebase_events
) ACTOR actor TIME ts EVENT event TIEBREAK sequence
  PROPERTIES (platform, country, os)
  WITH (time_unit = 'us');
```

The definition is a SELECT, so use `REBUILD EVENT DATASET app_events` to pick up
changed input. Keep the config and its event store together across CLI sessions.

## Paths, funnels, and retention

These examples use event names present in this export:

```sql
SELECT path_id, seq, event, occurrences, share
FROM paths('app_events', starting_at => 'first_open', depth => 4, top => 10)
ORDER BY path_id, seq;

SELECT step_index, step_name, entered, conversion_from_start
FROM funnel('app_events', 'first_open', 'tutorial_begin', 'tutorial_complete',
            window => '7 days')
ORDER BY step_index;

SELECT cohort_start, cohort_size, period_offset, retained, retention_rate
FROM retention('app_events', birth => 'first_open', return => 'session_start',
               period => 'day', periods => 8)
ORDER BY cohort_start, period_offset
LIMIT 32;
```

For a one-shot query, use `--command 'SQL'`; `--file` accepts one statement.
`--format json` and `--format csv` export results.
