# Firebase Parquet exports from the CLI

A Parquet directory registers one table per file. A view combines the daily
files with UNION ALL; the files stay in place and ordinary SQL queries the view.

An alternative with no view: register the matching daily files as ONE table,
with `glob: '/data/firebase-export/events_intraday_*.parquet'`
or a `manifest` file listing one path per line. The engine merges the files'
schemas by column name (a column absent from a file reads as NULL) and folds
their footer row counts and statistics, so queries read one logical table.
The file set is fixed at registration, so re-register the source to include a
newly exported day.

Run these commands from the repository root with the intended Python virtualenv
activated. `make build` builds the CLI after installing the project dependencies.

## Register the export

```bash
mkdir -p run/firebase
cat > run/firebase/fedq.yaml <<'YAML'
datasources:
  firebase:
    type: parquet
    dir: /data/firebase-export/
YAML

target/release/fedq --config run/firebase/fedq.yaml \
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
  > run/firebase/create_view.sql

target/release/fedq --config run/firebase/fedq.yaml \
  --file run/firebase/create_view.sql

target/release/fedq --config run/firebase/fedq.yaml
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
added files.

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

Repeated matching keys yield repeated rows. Aggregate matching parameters when
the query needs one row per source event.

SELECT-list UNNEST calls zip lists and pad shorter lists with NULL. Independent
FROM UNNEST calls form a Cartesian product. Empty and NULL lists produce no
rows in ordinary expansion; outer expansion preserves a row with NULL values.
Missing fields and scalar UNNEST inputs raise.

For a one-shot query, use `--command 'SQL'`; `--file` accepts one statement.
`--format json` and `--format csv` export results.
