"""The event-analytics suite runner (the ONE runner of this benchmark).

Staged modes, mirroring the tpcds harness:

- ``--mode gen``: synthesize the small/medium events datasets and write
  ``data/events_<size>_parquet/events.parquet``. Rows are emitted sorted by
  (ts, generation order) with ``seq`` renumbered 0..N-1 in that sorted order,
  matching production: a source buffers events and compacts to parquet in
  time order, so ``seq`` is never uncorrelated with ``ts`` in real data.
- ``--mode gen-crafted``: write the crafted generality datasets as parquet
  under ``data/crafted/`` (deterministic seeds). These datasets deliberately
  probe generality shapes (sparse/UUID actors, subsecond ties) and are
  untouched by ``--mode gen``.
- ``--mode save-refs``: compute every battery entry's ORACLE result via DuckDB
  SQL over the raw parquet and store results + oracle wall times in
  ``data/references_events_<size>.duckdb`` (results are a function of the
  data: computed once, read forever).
- ``--mode engine``: drive the fedq engine through ``tests/fedq.so``:
  CREATE EVENT DATASET once (recording build metrics), then run the battery
  with a cold (fresh-process; OS cache warm, so strictly "cool") and warm
  (live-runtime median) timing per entry, saving results to
  ``data/engine_results_<size>.json``.
- ``--mode compare``: cell-exact comparison of engine vs reference rows
  (float cells compared to 1e-9); any mismatch prints both sides and fails.

Before running, the runner prints how many engine executions the invocation
performs.
"""

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
PYTHON = sys.executable


def _small_medium_parquet_path(size):
    """The single Parquet file a small/medium dataset is exposed as."""
    return os.path.join(DATA_DIR, "events_%s_parquet" % size, "events.parquet")


def dataset_config(size):
    """The (config_path, parquet_path, dataset_name, create_sql) of a size."""
    if size == "100m":
        big_dir = os.environ.get("FEDQ_EVENTS_100M_DIR", "/workspace/synthetic-data")
        parquet = os.path.join(big_dir, "events_100m.parquet")
        if not os.path.exists(parquet):
            raise SystemExit(
                "100m parquet not found at %s; set FEDQ_EVENTS_100M_DIR" % parquet)
        config = os.path.join(DATA_DIR, "events_100m.config.yaml")
        with open(config, "w") as handle:
            handle.write(
                "datasources:\n  ev:\n    type: parquet\n    dir: %s\n" % big_dir)
        create = ("CREATE EVENT DATASET web FROM ev.main.events_100m "
                  "ACTOR user_id TIME created_at EVENT event "
                  "PROPERTIES (app_id)")
        return config, parquet, "web", create
    if size in ("small", "medium"):
        config = os.path.join(DATA_DIR, "events_%s.config.yaml" % size)
        parquet = _small_medium_parquet_path(size)
        create = (
            "CREATE EVENT DATASET web FROM ev.main.events "
            "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
            "PROPERTIES (device, country) WITH (refresh_key = 'seq')"
        )
        return config, parquet, "web", create
    crafted_dir = os.path.join(DATA_DIR, "crafted", size)
    config = os.path.join(crafted_dir, "config.yaml")
    parquet = os.path.join(crafted_dir, "events.parquet")
    create = CRAFTED[size]["create"]
    return config, parquet, "web", create


def write_crafted_config(size):
    """Write the YAML config of a crafted dataset's parquet directory."""
    crafted_dir = os.path.join(DATA_DIR, "crafted", size)
    config = os.path.join(crafted_dir, "config.yaml")
    with open(config, "w") as handle:
        handle.write(
            "datasources:\n  ev:\n    type: parquet\n    dir: %s\n" % crafted_dir
        )
    return config


# ---------------------------------------------------------------------------
# gen: synthesize the small/medium events datasets
# ---------------------------------------------------------------------------
#
# A gen scale fixes the event count and the distinct-entity target. Entities
# are drawn from a bounded power law, floor(E * u^ENTITY_SKEW) with
# u = hash(i)/max in [0, 1): the density goes as entity^(1/skew - 1), so a few
# head entities carry many events and the tail is long - the real shape of
# behavioural data - while the top entity stays a realistic fraction of the
# log (~0.14% at skew 2), unlike a pure Zipf(s~=1) whose single head entity
# would swallow ~5% of all events and make per-entity sequence work
# pathological.
GEN_ENTITY_SKEW = 2.0

GEN_SCALES = {
    "small": {"events": 1_000_000, "entities": 50_000},
    "medium": {"events": 100_000_000, "entities": 500_000},
}

# Twenty event types, listed most-frequent first (rank 1 is the head). Event
# rank is drawn Zipf over pow(21, u) -> 1..20, so `page_view` dominates and
# `cancel_account` is the rare tail.
GEN_EVENT_TYPES = (
    (1, "page_view"), (2, "view_item"), (3, "search"), (4, "click_ad"),
    (5, "scroll"), (6, "add_to_cart"), (7, "watch_video"), (8, "share"),
    (9, "comment"), (10, "signup"), (11, "login"), (12, "begin_checkout"),
    (13, "add_payment"), (14, "purchase"), (15, "refund"), (16, "rate_item"),
    (17, "wishlist"), (18, "apply_coupon"), (19, "contact_support"),
    (20, "cancel_account"),
)

# Property-column value pools (skewed by the same pow-draw so head values
# dominate, like real device/geo mixes).
GEN_DEVICES = ("ios", "android", "web", "desktop")
GEN_COUNTRIES = ("US", "GB", "DE", "FR", "IN", "BR", "JP", "CA")

# The event timeline spans GEN_WINDOW_DAYS; each entity's events cluster into
# sessions of at most GEN_SESSION_SECONDS, whose base instants are a hash of
# (entity, session) so all events of one session land in one burst.
GEN_WINDOW_DAYS = 30
GEN_SESSIONS_PER_ENTITY = 12
GEN_SESSION_SECONDS = 3600
GEN_START_TS = "2025-01-01"


def _gen_unit(salt):
    """A deterministic pseudo-random draw in [0, 1) from the row index `i` and
    a salt: hash is a pure function of its arguments, so the value does not
    depend on DuckDB's scan/thread order and the dataset is byte-reproducible.
    The full 64-bit hash is divided by 2^64 (not a coarse modulus) so the
    power-law tail is not aliased away. Each salt hashes a DISTINCT integer
    (i * 11 + salt), not hash(i, salt): DuckDB's multi-argument hash only
    shifts the base hash by the salt, which would tie an entity's events to
    one event name and one session. Hashing distinct inputs decorrelates the
    draws (measured correlation ~0)."""
    return "(hash(i * 11 + {0})::DOUBLE / 18446744073709551616.0)".format(salt)


def _gen_entity_expr(entities):
    """The bounded power-law entity id in [1, entities], as a BIGINT column."""
    return "least({0}, 1 + CAST(floor({0}.0 * pow({1}, {2})) AS BIGINT))".format(
        entities, _gen_unit(1), GEN_ENTITY_SKEW
    )


def _gen_event_rank_expr():
    """The Zipf event rank in [1, 20] (pow(21, u) reaches every rank)."""
    return "least(20, CAST(floor(pow(21.0, {0})) AS INT))".format(_gen_unit(2))


def _gen_timestamp_expr(entities):
    """A sessionized timestamp: the session's base instant is a hash of
    (entity, session) so all its events share one burst, plus a within-session
    second offset. Sessions scatter across the GEN_WINDOW_DAYS span."""
    session = "CAST(floor({0} * {1}) AS BIGINT)".format(
        _gen_unit(3), GEN_SESSIONS_PER_ENTITY
    )
    base = "(hash({0}, {1}, 7) % ({2} * 86400))".format(
        _gen_entity_expr(entities), session, GEN_WINDOW_DAYS
    )
    intra = "CAST(floor({0} * {1}) AS BIGINT)".format(
        _gen_unit(4), GEN_SESSION_SECONDS
    )
    return "TIMESTAMP '{0}' + to_seconds({1} + {2})".format(GEN_START_TS, base, intra)


def _gen_pool_expr(pool, salt):
    """Pick from a small value pool with a skewed pow-draw, 1-based index."""
    values = "', '".join(pool)
    index = "CAST(floor(pow({0}.0, {1})) AS INT)".format(len(pool), _gen_unit(salt))
    return "(ARRAY['{0}'])[least({1}, {2})]".format(values, len(pool), index)


def _gen_names_values():
    """The 20-row (rank, name) VALUES clause for the event-type join."""
    rows = []
    for rank, name in GEN_EVENT_TYPES:
        rows.append("({0}, '{1}')".format(rank, name))
    return ", ".join(rows)


def _gen_raw_sql(events, entities):
    """The CREATE TABLE AS SELECT that builds the raw (unsorted) events table.

    Every column is a pure function of the range index `i`, joined to the
    20-row event-name table on the drawn rank, so the result is deterministic
    regardless of DuckDB's parallel scan order. `gen_order` carries `i`
    itself: the row's place in a hypothetical single-writer arrival stream,
    used as the sort tiebreak below (production ties within one timestamp
    keep buffer-arrival order)."""
    entity = _gen_entity_expr(entities)
    timestamp = _gen_timestamp_expr(entities)
    device = _gen_pool_expr(GEN_DEVICES, 5)
    country = _gen_pool_expr(GEN_COUNTRIES, 6)
    return (
        "CREATE TABLE raw_events AS SELECT\n"
        "  {entity} AS entity_id,\n"
        "  {timestamp} AS ts,\n"
        "  names.name AS event_name,\n"
        "  CAST(i AS BIGINT) AS gen_order,\n"
        "  {device} AS device,\n"
        "  {country} AS country\n"
        "FROM range({events}) t(i)\n"
        "JOIN (SELECT * FROM (VALUES {names}) v(rank, name)) names\n"
        "  ON names.rank = {event_rank}\n"
    ).format(
        entity=entity,
        timestamp=timestamp,
        device=device,
        country=country,
        events=events,
        names=_gen_names_values(),
        event_rank=_gen_event_rank_expr(),
    )


def _gen_export_sql(parquet_path):
    """The COPY that sorts the raw table by (ts, gen_order) and renumbers
    `seq` to 0..N-1 in that order - the ts-sorted, seq-ordered shape a
    production compaction produces, matched here so seq is never uncorrelated
    with ts the way an unordered synthetic generation would leave it."""
    return (
        "COPY (SELECT entity_id, ts, event_name, "
        "CAST(row_number() OVER (ORDER BY ts, gen_order) - 1 AS BIGINT) AS seq, "
        "device, country FROM raw_events ORDER BY ts, gen_order) "
        "TO '{0}' (FORMAT PARQUET)"
    ).format(parquet_path)


def _report_gen_shape(connection):
    """Print the raw generated table's headline shape before sort+export."""
    counts = connection.execute(
        "SELECT count(*), count(DISTINCT entity_id), count(DISTINCT event_name) "
        "FROM raw_events"
    ).fetchone()
    print(
        "rows=%d  distinct entities=%d  distinct event types=%d"
        % (counts[0], counts[1], counts[2])
    )


def _human_bytes(path):
    """A human-readable file size for the generated-dataset banner."""
    size = float(os.path.getsize(path))
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024.0:
            return "%.1f%s" % (size, unit)
        size /= 1024.0
    return "%.1fTB" % size


def mode_gen(size):
    """gen subcommand: synthesize a small/medium events dataset and write its
    Parquet source file, sorted by (ts, generation order) with seq assigned
    0..N-1 in that order - the shape production data always has (a buffer
    compacts to parquet in time order), which the raw generation-order rows
    do not until this sort+renumber step is applied."""
    import duckdb

    if size not in GEN_SCALES:
        raise SystemExit("gen only supports --size small or --size medium")
    shape = GEN_SCALES[size]
    parquet_path = _small_medium_parquet_path(size)
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    print(
        "generating scale=%s: %d events over ~%d entities -> %s"
        % (size, shape["events"], shape["entities"], parquet_path)
    )
    started = time.time()
    connection = duckdb.connect()
    connection.execute("SET threads TO %d" % os.cpu_count())
    connection.execute(_gen_raw_sql(shape["events"], shape["entities"]))
    _report_gen_shape(connection)
    connection.execute(_gen_export_sql(parquet_path))
    connection.close()
    elapsed = time.time() - started
    print("generated in %.1fs (%s)" % (elapsed, _human_bytes(parquet_path)))


# ---------------------------------------------------------------------------
# Oracle SQL builders (DuckDB over the raw parquet)
# ---------------------------------------------------------------------------


DEFAULT_COLUMNS = {
    "select": "entity_id AS actor, ts, seq AS tb, event_name AS name, "
              "device, country",
    "ts": "ts",
}

COLUMNS_100M = {
    "select": "user_id AS actor, created_at AS ts, 0 AS tb, event AS name",
    "ts": "created_at",
}


def ev_cte(parquet, where=None, time_from=None, time_to=None, columns=None):
    """The filtered event-stream CTE every oracle starts from. `columns` maps
    the parquet's schema onto the actor/ts/tb/name roles (defaults to the
    small/medium schema)."""
    columns = columns or DEFAULT_COLUMNS
    clauses = []
    if where:
        clauses.append(where)
    if time_from:
        clauses.append("%s >= TIMESTAMP '%s'" % (columns["ts"], time_from))
    if time_to:
        clauses.append("%s < TIMESTAMP '%s'" % (columns["ts"], time_to))
    predicate = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return (
        "ev AS (SELECT %s FROM read_parquet('%s')%s)"
        % (columns["select"], parquet, predicate)
    )


def funnel_oracle_sql(parquet, steps, window_days, where=None, time_from=None,
                      time_to=None, breakdown=None):
    """The funnel oracle: greedy chase from EVERY anchor (LATERAL), depth =
    max over anchors, witness = earliest anchor achieving it; entered counts,
    conversions, and witness-duration quantiles per the pinned semantics."""
    k = len(steps)
    parts = ["WITH " + ev_cte(parquet, where, time_from, time_to)]
    bd = ", " + breakdown + " AS bd" if breakdown else ""
    parts.append(
        "anchors AS (SELECT actor, ts AS t1, tb AS b1%s FROM ev WHERE name = '%s')"
        % (bd, steps[0])
    )
    previous = "anchors"
    for j in range(2, k + 1):
        parts.append(
            "chase%d AS (SELECT c.*, l.t%d, l.b%d FROM %s c "
            "LEFT JOIN LATERAL (SELECT e.ts AS t%d, e.tb AS b%d FROM ev e "
            "WHERE e.actor = c.actor AND e.name = '%s' "
            "AND (e.ts, e.tb) > (c.t%d, c.b%d) "
            "AND e.ts - c.t1 <= INTERVAL %d DAY "
            "ORDER BY e.ts, e.tb LIMIT 1) l ON TRUE)"
            % (j, j, j, previous, j, j, steps[j - 1], j - 1, j - 1, window_days)
        )
        previous = "chase%d" % j
    depth_case = "CASE "
    for j in range(k, 1, -1):
        depth_case += "WHEN t%d IS NOT NULL THEN %d " % (j, j)
    depth_case += "ELSE 1 END"
    parts.append("chased AS (SELECT *, %s AS depth FROM %s)" % (depth_case, previous))
    parts.append(
        "witness AS (SELECT * FROM chased "
        "QUALIFY row_number() OVER (PARTITION BY actor "
        "ORDER BY depth DESC, t1 ASC, b1 ASC) = 1)"
    )
    duration_selects = []
    for j in range(2, k + 1):
        duration_selects.append(
            "quantile_cont(epoch(t%d) - epoch(t%d), 0.5) AS med%d, "
            "quantile_cont(epoch(t%d) - epoch(t%d), 0.9) AS p90%d"
            % (j, j - 1, j, j, j - 1, j)
        )
    group = "bd, " if breakdown else ""
    entered_selects = []
    for j in range(1, k + 1):
        entered_selects.append(
            "count(*) FILTER (WHERE depth >= %d) AS entered%d" % (j, j)
        )
    parts.append(
        "agg AS (SELECT %s%s, %s FROM witness GROUP BY ALL)"
        % (group, ", ".join(entered_selects), ", ".join(duration_selects))
    )
    order = " ORDER BY bd NULLS LAST" if breakdown else ""
    return ",\n".join(parts) + "\nSELECT * FROM agg" + order


def funnel_oracle(con, parquet, steps, window_days, where=None, time_from=None,
                  time_to=None, breakdown=None):
    """Run the funnel oracle and shape it into the engine's result rows."""
    sql = funnel_oracle_sql(parquet, steps, window_days, where, time_from,
                            time_to, breakdown)
    raw = con.execute(sql).fetchall()
    k = len(steps)
    rows = []
    for record in raw:
        offset = 1 if breakdown else 0
        bd = record[0] if breakdown else None
        entered = record[offset:offset + k]
        quantiles = record[offset + k:]
        start = entered[0]
        previous = start
        for j in range(1, k + 1):
            count = entered[j - 1]
            if j == 1:
                conv_prev, conv_start = 1.0 if count else 0.0, 1.0 if count else 0.0
                med, p90 = None, None
            else:
                conv_prev = (count / previous) if previous else 0.0
                conv_start = (count / start) if start else 0.0
                med = quantiles[(j - 2) * 2]
                p90 = quantiles[(j - 2) * 2 + 1]
            row = [j, steps[j - 1], count, conv_prev, conv_start, med, p90]
            if breakdown:
                row.insert(0, bd)
            rows.append(tuple(row))
            previous = count
    if breakdown:
        rows.sort(key=lambda r: ((r[0] is None, r[0] or ""), r[1]))
    return rows


def period_exprs(period):
    """The (trunc expr, offset expr) SQL pieces of a retention period grain."""
    if period == "week":
        trunc = "date_trunc('week', %s)"
        offset = ("CAST((epoch(date_trunc('week', e.ts)) - epoch(b.cohort)) "
                  "// (7 * 86400) AS INT)")
    elif period == "day":
        trunc = "date_trunc('day', %s)"
        offset = "date_diff('day', b.cohort, date_trunc('day', e.ts))"
    else:
        trunc = "date_trunc('month', %s)"
        offset = "date_diff('month', b.cohort, date_trunc('month', e.ts))"
    return trunc, offset


def retention_oracle(con, parquet, birth, ret, period, mode="bounded",
                     periods=None, at=None, where=None, time_from=None,
                     time_to=None):
    """The retention oracle: births, per-actor return offsets, then the dense
    grid the engine pins (every period in range, offsets 0..limit)."""
    trunc, offset_expr = period_exprs(period)
    birth_pred = "TRUE" if birth == "any" else "name = '%s'" % birth
    ret_pred = "TRUE" if ret == "any" else "name = '%s'" % ret
    base = ev_cte(parquet, where, time_from, time_to)
    births = ("births AS (SELECT actor, min_by(ts, (ts, tb)) AS birth_ts, "
              "min_by(tb, (ts, tb)) AS birth_tb, "
              + (trunc % "min_by(ts, (ts, tb))") + " AS cohort "
              "FROM ev WHERE %s GROUP BY actor)" % birth_pred)
    rets = ("rets AS (SELECT DISTINCT b.actor, b.cohort, %s AS off "
            "FROM births b JOIN ev e USING (actor) "
            "WHERE (%s) AND (e.ts, e.tb) > (b.birth_ts, b.birth_tb))"
            % (offset_expr, ret_pred))
    bounds = con.execute(
        "WITH %s SELECT %s, %s FROM ev"
        % (base, trunc % "min(ts)", trunc % "max(ts)")
    ).fetchone()
    if bounds[0] is None:
        return []
    first, last = bounds
    interval_by_period = {"day": "1 DAY", "week": "7 DAY", "month": "1 MONTH"}
    cohorts = con.execute(
        "SELECT unnest(generate_series(TIMESTAMP '%s', TIMESTAMP '%s', "
        "INTERVAL %s))" % (first, last, interval_by_period[period])
    ).fetchall()
    sizes = dict(con.execute(
        "WITH %s, %s SELECT cohort, count(*) FROM births GROUP BY 1"
        % (base, births)).fetchall())
    if mode == "bounded":
        cells = {}
        for cohort, off, count in con.execute(
                "WITH %s, %s, %s SELECT cohort, off, count(*) FROM rets "
                "GROUP BY 1, 2" % (base, births, rets)).fetchall():
            cells[(cohort, off)] = count
    else:
        cells = {}
        for cohort, max_off, count in con.execute(
                "WITH %s, %s, %s, mo AS (SELECT actor, cohort, max(off) AS m "
                "FROM rets GROUP BY 1, 2) "
                "SELECT cohort, m, count(*) FROM mo GROUP BY 1, 2"
                % (base, births, rets)).fetchall():
            cells[(cohort, max_off)] = count
    last_index = len(cohorts) - 1
    rows = []
    for index, (cohort,) in enumerate(cohorts):
        size = sizes.get(cohort, 0)
        limit = periods if periods is not None else (last_index - index)
        if mode == "unbounded":
            suffix = [0] * (limit + 1)
            for (cell_cohort, off), count in cells.items():
                if cell_cohort == cohort and off >= 0:
                    suffix[min(off, limit)] += count
            for position in range(limit - 1, -1, -1):
                suffix[position] += suffix[position + 1]
        for off in range(0, limit + 1):
            if at is not None and off != at:
                continue
            if mode == "bounded":
                retained = cells.get((cohort, off), 0)
            else:
                retained = suffix[off]
            rate = (retained / size) if size else None
            rows.append((cohort.isoformat(), size, off, retained, rate))
    return rows


def bucket_expr(grain):
    """The DuckDB bucket expression of a segmentation grain."""
    return "date_trunc('%s', ts)" % grain


def segment_oracle(con, parquet, measure, grain, event=None, where=None,
                   time_from=None, time_to=None, breakdown=None):
    """The segmentation oracle: a direct GROUP BY over the filtered stream."""
    clauses = []
    if event:
        clauses.append("name = '%s'" % event)
    filter_where = " AND ".join(clauses) if clauses else None
    combined = where
    if filter_where:
        combined = ("(%s) AND (%s)" % (where, filter_where)) if where else filter_where
    base = ev_cte(parquet, combined, time_from, time_to)
    value = {
        "count": "count(*)",
        "uniques": "count(DISTINCT actor)",
        "count_per_unique": "count(*) * 1.0 / count(DISTINCT actor)",
    }[measure]
    if breakdown:
        sql = ("WITH %s SELECT %s AS bucket, %s AS bd, %s AS value FROM ev "
               "GROUP BY 1, 2 ORDER BY (bd IS NULL), bd, bucket"
               % (base, bucket_expr(grain), breakdown, value))
    else:
        sql = ("WITH %s SELECT %s AS bucket, %s AS value FROM ev "
               "GROUP BY 1 ORDER BY 1" % (base, bucket_expr(grain), value))
    rows = []
    for record in con.execute(sql).fetchall():
        if breakdown:
            rows.append((record[0].isoformat(), record[1], record[2]))
        else:
            rows.append((record[0].isoformat(), record[1]))
    return rows


def paths_oracle(con, parquet, depth, top, anchor=None, direction="forward",
                 maxgap_minutes=None, where=None, time_from=None, time_to=None,
                 columns=None):
    """The paths oracle: gap-flag sessions BEFORE collapsing, collapse
    consecutive duplicates within a fragment, then enumerate windows per the
    anchor variant. A FROM/TO range never drops events from the stream: it
    admits a window by its ANCHOR event's timestamp (engine anchor
    semantics), tested on the collapsed anchor row below. Share divides by
    the pre-limit window total."""
    base = ev_cte(parquet, where, None, None, columns)
    if maxgap_minutes is not None:
        gap = ("gapped AS (SELECT actor, ts, tb, name, sum(brk) OVER "
               "(PARTITION BY actor ORDER BY ts, tb ROWS UNBOUNDED PRECEDING) "
               "AS ses FROM (SELECT *, CASE WHEN ts - lag(ts) OVER "
               "(PARTITION BY actor ORDER BY ts, tb) > INTERVAL %d MINUTE "
               "THEN 1 ELSE 0 END AS brk FROM ev))" % maxgap_minutes)
    else:
        gap = "gapped AS (SELECT actor, ts, tb, name, 0 AS ses FROM ev)"
    collapsed = ("collapsed AS (SELECT actor, ses, ts, tb, name FROM "
                 "(SELECT *, lag(name) OVER w AS prev_name FROM gapped "
                 "WINDOW w AS (PARTITION BY actor, ses ORDER BY ts, tb)) "
                 "WHERE prev_name IS NULL OR prev_name <> name)")
    anchor_range = []
    if time_from:
        anchor_range.append("ts >= TIMESTAMP '%s'" % time_from)
    if time_to:
        anchor_range.append("ts < TIMESTAMP '%s'" % time_to)
    if direction == "forward":
        lead_cols = []
        for i in range(1, depth):
            lead_cols.append("lead(name, %d) OVER w AS l%d" % (i, i))
        strung = ("strung AS (SELECT actor, ts, name, %s FROM collapsed "
                  "WINDOW w AS (PARTITION BY actor, ses ORDER BY ts, tb))"
                  % ", ".join(lead_cols))
        path = "name"
        for i in range(1, depth):
            path += " || coalesce(' -> ' || l%d, '')" % i
        if anchor:
            condition = "name = '%s' AND l1 IS NOT NULL" % anchor
        else:
            condition = "l%d IS NOT NULL" % (depth - 1)
    else:
        lag_cols = []
        for i in range(1, depth):
            lag_cols.append("lag(name, %d) OVER w AS g%d" % (i, i))
        strung = ("strung AS (SELECT actor, ts, name, %s FROM collapsed "
                  "WINDOW w AS (PARTITION BY actor, ses ORDER BY ts, tb))"
                  % ", ".join(lag_cols))
        path = ""
        for i in range(depth - 1, 0, -1):
            path += "coalesce(g%d || ' -> ', '') || " % i
        path += "name"
        condition = "name = '%s' AND g1 IS NOT NULL" % anchor
    condition = " AND ".join([condition] + anchor_range)
    sql = ("WITH %s, %s, %s, %s, counted AS (SELECT %s AS path, count(*) AS occ "
           "FROM strung WHERE %s GROUP BY 1) "
           "SELECT path, occ, occ * 1.0 / sum(occ) OVER () AS share "
           "FROM counted ORDER BY occ DESC, path LIMIT %d"
           % (base, gap, collapsed, strung, path, condition, top))
    rows = []
    rank = 0
    for record in con.execute(sql).fetchall():
        rank += 1
        rows.append((rank, record[0], record[1], record[2]))
    return rows


# ---------------------------------------------------------------------------
# The battery
# ---------------------------------------------------------------------------


def battery_100m(parquet):
    """The 100m battery: PATHS depth coverage on the big dataset. Depths a
    DuckDB oracle can materialize (window strings per position) are
    oracle-checked; deeper entries are cross-checked in compare mode against
    their MAXGAP twin (`same_as`), which answers from the scan instead of the
    index - two disjoint code paths that must agree exactly (an effectively
    infinite MAXGAP never breaks a stream, so the semantics are identical)."""
    entries = []

    def add(name, sql, oracle, same_as=None):
        entries.append({"name": name, "sql": sql, "oracle": oracle,
                        "same_as": same_as})

    def deep_pair(name, base_sql, depth, top):
        add(name, base_sql, None)
        add(name + "_gapcheck", base_sql + " MAXGAP 365000 DAY", None,
            same_as=name)

    add("paths_d4_unanchored",
        "PATHS ON web DEPTH 4 TOP 10",
        lambda con: paths_oracle(con, parquet, 4, 10, columns=COLUMNS_100M))
    add("paths_d5_forward",
        "PATHS ON web STARTING AT 'game_opened' DEPTH 5 TOP 10",
        lambda con: paths_oracle(con, parquet, 5, 10, anchor="game_opened",
                                 columns=COLUMNS_100M))
    add("paths_d5_backward",
        "PATHS ON web ENDING AT 'uninstalled' DEPTH 5 TOP 10",
        lambda con: paths_oracle(con, parquet, 5, 10, anchor="uninstalled",
                                 direction="backward", columns=COLUMNS_100M))
    add("paths_d8_ranged",
        "PATHS ON web DEPTH 8 TOP 10 FROM '2026-06-15' TO '2026-06-22'",
        lambda con: paths_oracle(con, parquet, 8, 10,
                                 time_from="2026-06-15", time_to="2026-06-22",
                                 columns=COLUMNS_100M))
    add("paths_d6_maxgap",
        "PATHS ON web DEPTH 6 TOP 10 MAXGAP 30 MINUTE",
        lambda con: paths_oracle(con, parquet, 6, 10, maxgap_minutes=30,
                                 columns=COLUMNS_100M))
    deep_pair("paths_d10_unanchored", "PATHS ON web DEPTH 10 TOP 10", 10, 10)
    deep_pair("paths_d20_unanchored", "PATHS ON web DEPTH 20 TOP 10 USING SCAN", 20, 10)
    # Depth 20/50 exceed the dataset's max_paths_depth: the statement must
    # opt into the exact scan, which is the semantics the oracle checks.
    deep_pair("paths_d50_unanchored", "PATHS ON web DEPTH 50 TOP 10 USING SCAN", 50, 10)
    deep_pair("paths_d50_forward",
              "PATHS ON web STARTING AT 'game_opened' DEPTH 50 TOP 10 USING SCAN",
              50, 10)
    deep_pair("paths_d20_backward",
              "PATHS ON web ENDING AT 'uninstalled' DEPTH 20 TOP 10 USING SCAN",
              20, 10)
    return entries


def battery(size, parquet):
    """The (name, engine_sql, oracle_fn) battery of a dataset. `oracle_fn`
    takes a DuckDB connection and returns the pinned engine-row shape."""
    if size == "100m":
        return battery_100m(parquet)
    entries = []

    def add(name, sql, oracle):
        entries.append({"name": name, "sql": sql, "oracle": oracle})

    add("funnel_common",
        "FUNNEL ('page_view', 'view_item', 'add_to_cart') ON web WINDOW 7 DAY",
        lambda con: funnel_oracle(con, parquet,
                                  ["page_view", "view_item", "add_to_cart"], 7))
    add("funnel_selective",
        "FUNNEL ('signup', 'begin_checkout', 'purchase') ON web WINDOW 7 DAY",
        lambda con: funnel_oracle(con, parquet,
                                  ["signup", "begin_checkout", "purchase"], 7))
    add("funnel_two_step_range",
        "FUNNEL ('signup', 'purchase') ON web WINDOW 3 DAY "
        "FROM '2025-01-05' TO '2025-01-20'",
        lambda con: funnel_oracle(con, parquet, ["signup", "purchase"], 3,
                                  time_from="2025-01-05", time_to="2025-01-20"))
    add("funnel_breakdown_filter",
        "FUNNEL ('signup', 'begin_checkout', 'purchase') ON web WINDOW 7 DAY "
        "WHERE country = 'DE' BREAKDOWN BY device",
        lambda con: funnel_oracle(con, parquet,
                                  ["signup", "begin_checkout", "purchase"], 7,
                                  where="country = 'DE'", breakdown="device"))
    add("retention_weekly",
        "RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD WEEK",
        lambda con: retention_oracle(con, parquet, "signup", "any", "week"))
    add("retention_monthly_unbounded",
        "RETENTION ON web BIRTH ANY EVENT RETURN 'purchase' PERIOD MONTH "
        "MODE UNBOUNDED",
        lambda con: retention_oracle(con, parquet, "any", "purchase", "month",
                                     mode="unbounded"))
    add("retention_at",
        "RETENTION ON web BIRTH 'signup' RETURN 'purchase' PERIOD WEEK "
        "PERIODS 4 AT 2",
        lambda con: retention_oracle(con, parquet, "signup", "purchase",
                                     "week", periods=4, at=2))
    add("segment_count_daily",
        "SEGMENT COUNT ON web BUCKET DAY",
        lambda con: segment_oracle(con, parquet, "count", "day"))
    add("segment_count_event_weekly",
        "SEGMENT COUNT ON web EVENT 'add_to_cart' BUCKET WEEK",
        lambda con: segment_oracle(con, parquet, "count", "week",
                                   event="add_to_cart"))
    add("segment_uniques_daily",
        "SEGMENT UNIQUES ON web BUCKET DAY",
        lambda con: segment_oracle(con, parquet, "uniques", "day"))
    add("segment_uniques_filter_breakdown",
        "SEGMENT UNIQUES ON web EVENT 'add_to_cart' BUCKET DAY "
        "WHERE country = 'DE' BREAKDOWN BY device",
        lambda con: segment_oracle(con, parquet, "uniques", "day",
                                   event="add_to_cart", where="country = 'DE'",
                                   breakdown="device"))
    add("segment_cpu_hourly",
        "SEGMENT COUNT_PER_UNIQUE ON web EVENT 'purchase' BUCKET HOUR "
        "FROM '2025-01-10' TO '2025-01-11'",
        lambda con: segment_oracle(con, parquet, "count_per_unique", "hour",
                                   event="purchase", time_from="2025-01-10",
                                   time_to="2025-01-11"))
    add("paths_forward",
        "PATHS ON web STARTING AT 'search' DEPTH 5 TOP 20",
        lambda con: paths_oracle(con, parquet, 5, 20, anchor="search"))
    add("paths_backward_gap",
        "PATHS ON web ENDING AT 'cancel_account' DEPTH 4 TOP 20 "
        "MAXGAP 30 MINUTE",
        lambda con: paths_oracle(con, parquet, 4, 20, anchor="cancel_account",
                                 direction="backward", maxgap_minutes=30))
    add("paths_unanchored",
        "PATHS ON web DEPTH 3 TOP 20",
        lambda con: paths_oracle(con, parquet, 3, 20))
    return entries


# ---------------------------------------------------------------------------
# Row normalization and comparison
# ---------------------------------------------------------------------------


def normalize_paths_rows(table):
    """Reassemble the engine's one-row-per-step paths shape into the oracle's
    (rank, path, occurrences, share) rows; a path's step rows arrive in seq
    order under its path_id."""
    data = table.to_pydict()
    paths = {}
    for index in range(len(data["path_id"])):
        path_id = data["path_id"][index]
        entry = paths.setdefault(
            path_id,
            {"steps": [], "occ": data["occurrences"][index],
             "share": data["share"][index]},
        )
        entry["steps"].append(data["event"][index])
    rows = []
    for path_id in sorted(paths):
        entry = paths[path_id]
        rows.append((path_id, " -> ".join(entry["steps"]), entry["occ"],
                     entry["share"]))
    return rows


def normalize_engine_table(name, table):
    """Shape an engine pyarrow result into the oracle-comparable row tuples."""
    if name.startswith("paths"):
        return normalize_paths_rows(table)
    columns = table.column_names
    data = table.to_pydict()
    rows = []
    for index in range(table.num_rows):
        row = []
        for column in columns:
            value = data[column][index]
            if hasattr(value, "isoformat"):
                value = value.isoformat()
            row.append(value)
        rows.append(tuple(row))
    return rows


def cells_equal(left, right):
    """Cell equality: exact for everything except floats (1e-9 tolerance)."""
    if isinstance(left, float) or isinstance(right, float):
        if left is None or right is None:
            return left is None and right is None
        if math.isnan(left) and math.isnan(right):
            return True
        return abs(left - right) <= 1e-9 * max(1.0, abs(left), abs(right))
    return left == right


def rows_equal(engine_rows, oracle_rows):
    """Ordered row-list equality with float tolerance."""
    if len(engine_rows) != len(oracle_rows):
        return False
    for engine_row, oracle_row in zip(engine_rows, oracle_rows):
        if len(engine_row) != len(oracle_row):
            return False
        for left, right in zip(engine_row, oracle_row):
            if not cells_equal(left, right):
                return False
    return True


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def refs_path(size):
    """The references DuckDB file of a size."""
    return os.path.join(DATA_DIR, "references_events_%s.duckdb" % size)


def engine_results_path(size):
    """The engine results JSON of a size."""
    return os.path.join(DATA_DIR, "engine_results_%s.json" % size)


def mode_save_refs(size, only):
    """Compute and store every battery oracle result + timing."""
    import duckdb

    _, parquet, _, _ = dataset_config(size)
    entries = battery(size, parquet)
    if only:
        entries = [e for e in entries if e["name"] in only]
    print("save-refs: %d oracle computations (0 engine executions)" % len(entries))
    con = duckdb.connect(refs_path(size))
    con.execute("CREATE TABLE IF NOT EXISTS results "
                "(name TEXT PRIMARY KEY, payload TEXT, oracle_ms DOUBLE)")
    work = duckdb.connect()
    work.execute("SET threads TO %d" % os.cpu_count())
    # Naive timestamp literals in oracle SQL bind in the session timezone;
    # the engine binds FROM/TO in UTC, so the oracle session pins UTC too.
    work.execute("SET timezone = 'UTC'")
    for entry in entries:
        if entry["oracle"] is None:
            print("  %-34s no oracle; cross-checked in compare mode"
                  % entry["name"])
            continue
        started = time.time()
        rows = entry["oracle"](work)
        elapsed_ms = (time.time() - started) * 1000.0
        con.execute("DELETE FROM results WHERE name = ?", [entry["name"]])
        con.execute("INSERT INTO results VALUES (?, ?, ?)",
                    [entry["name"], json.dumps(rows, default=str), elapsed_ms])
        print("  %-34s oracle %.1f ms, %d rows" %
              (entry["name"], elapsed_ms, len(rows)))
    con.close()


COLD_DRIVER = r"""
import json, sys, time
sys.path.insert(0, %(tests_dir)r)
import pyarrow as pa
import fedq
runtime = fedq.Runtime(%(config)r)
started = time.time()
stream = runtime.execute(%(sql)r)
table = pa.table(stream)
elapsed_ms = (time.time() - started) * 1000.0
print(json.dumps({"ms": elapsed_ms, "rows": table.num_rows}))
"""


def cold_run(config, sql):
    """One fresh-process execution; returns wall ms (build+plan excluded from
    nothing: the whole statement is timed, runtime construction is not)."""
    code = COLD_DRIVER % {
        "tests_dir": os.path.join(REPO_ROOT, "tests"),
        "config": config,
        "sql": sql,
    }
    output = subprocess.run([PYTHON, "-c", code], capture_output=True,
                            text=True, check=True)
    return json.loads(output.stdout.strip().splitlines()[-1])["ms"]


def mode_engine(size, only, warm_runs, skip_cold):
    """Build the dataset (if absent) and run the battery, recording results
    plus cold/warm timings."""
    sys.path.insert(0, os.path.join(REPO_ROOT, "tests"))
    import pyarrow as pa
    import fedq

    config, parquet, dataset, create_sql = dataset_config(size)
    entries = battery(size, parquet)
    if only:
        entries = [e for e in entries if e["name"] in only]
    executions = len(entries) * (warm_runs + 1 + (0 if skip_cold else 1)) + 1
    print("engine: about %d engine executions" % executions)
    runtime = fedq.Runtime(config)
    listing = pa.table(runtime.execute("SHOW EVENT DATASETS")).to_pydict()
    build_status = None
    if dataset not in listing.get("name", []):
        started = time.time()
        status = pa.table(runtime.execute(create_sql)).to_pydict()
        build_wall = time.time() - started
        build_status = {"status": status["status"][0], "wall_s": build_wall}
        print("BUILD: %.2fs wall; %s" % (build_wall, status["status"][0]))
    else:
        print("BUILD: dataset '%s' already exists; reusing" % dataset)
    results = {"size": size, "build": build_status, "queries": {}}
    with open(engine_results_path(size), "w") as handle:
        json.dump(results, handle, default=str)
    for entry in entries:
        cold_ms = None
        if not skip_cold:
            cold_ms = cold_run(config, entry["sql"])
        warm = []
        table = None
        for _ in range(warm_runs):
            started = time.time()
            table = pa.table(runtime.execute(entry["sql"]))
            warm.append((time.time() - started) * 1000.0)
        rows = normalize_engine_table(entry["name"], table)
        results["queries"][entry["name"]] = {
            "sql": entry["sql"],
            "cold_ms": cold_ms,
            "warm_ms": warm,
            "warm_median_ms": statistics.median(warm),
            "rows": rows,
        }
        print("  %-34s cold %8s ms  warm %8.1f ms  %d rows" % (
            entry["name"],
            ("%.1f" % cold_ms) if cold_ms is not None else "-",
            statistics.median(warm), len(rows)))
        # Rewritten after every entry: a crash mid-battery leaves the
        # completed entries on disk instead of a stale previous run.
        with open(engine_results_path(size), "w") as handle:
            json.dump(results, handle, default=str)
    print("engine results written to", engine_results_path(size))


def mode_compare(size, only):
    """Cell-exact engine-vs-oracle comparison; any mismatch fails the run."""
    import duckdb

    print("compare: 0 engine executions")
    with open(engine_results_path(size)) as handle:
        engine = json.load(handle)
    con = duckdb.connect(refs_path(size), read_only=True)
    failures = 0
    names = list(engine["queries"].keys())
    if only:
        names = [n for n in names if n in only]
    _, parquet, _, _ = dataset_config(size)
    same_as = {}
    for entry in battery(size, parquet):
        if entry.get("same_as"):
            same_as[entry["name"]] = entry["same_as"]
    for name in names:
        if name in same_as:
            # A gapcheck twin: its rows must equal its base entry's rows
            # exactly - the twin answers from the scan, the base from the
            # index, and an effectively infinite MAXGAP changes nothing.
            base = same_as[name]
            twin_rows = [tuple(r) for r in engine["queries"][name]["rows"]]
            base_rows = [tuple(r) for r in engine["queries"][base]["rows"]]
            if rows_equal(twin_rows, base_rows):
                print("  %-34s AGREE with %s (%d rows)"
                      % (name, base, len(twin_rows)))
            else:
                failures += 1
                print("  %-34s DIFFERS from %s" % (name, base))
                print("    scan  (%d rows): %s" % (len(twin_rows), twin_rows[:6]))
                print("    index (%d rows): %s" % (len(base_rows), base_rows[:6]))
            continue
        stored = con.execute("SELECT payload FROM results WHERE name = ?",
                             [name]).fetchone()
        if stored is None:
            base_names = set(same_as.values())
            if name in base_names:
                print("  %-34s no oracle; checked via its gapcheck twin" % name)
                continue
            print("  %-34s NO REFERENCE (run --mode save-refs)" % name)
            failures += 1
            continue
        oracle_rows = []
        for row in json.loads(stored[0]):
            oracle_rows.append(tuple(row))
        engine_rows = []
        for row in engine["queries"][name]["rows"]:
            engine_rows.append(tuple(row))
        if rows_equal(engine_rows, oracle_rows):
            print("  %-34s AGREE (%d rows)" % (name, len(engine_rows)))
        else:
            failures += 1
            print("  %-34s DIFFER" % name)
            print("    engine (%d rows): %s" % (len(engine_rows), engine_rows[:6]))
            print("    oracle (%d rows): %s" % (len(oracle_rows), oracle_rows[:6]))
    con.close()
    if failures:
        print("COMPARE FAILED: %d mismatching entries" % failures)
        sys.exit(1)
    print("COMPARE OK: every entry matches the oracle")


# ---------------------------------------------------------------------------
# Crafted generality datasets
# ---------------------------------------------------------------------------

CRAFTED = {
    "crafted_sparse": {
        "create": ("CREATE EVENT DATASET web FROM ev.main.events "
                   "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
                   "PROPERTIES (device, country)"),
    },
    "crafted_uuid": {
        "create": ("CREATE EVENT DATASET web FROM ev.main.events "
                   "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
                   "PROPERTIES (device, country)"),
    },
    "crafted_subsecond": {
        "create": ("CREATE EVENT DATASET web FROM ev.main.events "
                   "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
                   "PROPERTIES (device, country)"),
    },
}


def mode_gen_crafted():
    """Write the crafted generality datasets (deterministic seeds)."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = np.random.default_rng(20260721)
    names = ["page_view", "view_item", "search", "signup", "begin_checkout",
             "purchase", "add_to_cart", "click_ad", "scroll", "watch_video",
             "cancel_account"]
    devices = ["ios", "android", "web"]
    countries = ["DE", "US", "FR", "JP"]

    def base_columns(rows, actor_values):
        """The shared ts/name/props columns over the given actor ids."""
        ts = rng.integers(1735689600, 1738368000, rows, dtype=np.int64) * 1000000
        event = rng.choice(len(names), rows, p=None)
        device = rng.choice(len(devices), rows)
        country = rng.choice(len(countries), rows)
        chosen_names = []
        chosen_devices = []
        chosen_countries = []
        for index in range(rows):
            chosen_names.append(names[event[index]])
            chosen_devices.append(devices[device[index]])
            chosen_countries.append(countries[country[index]])
        return {
            "entity_id": actor_values,
            "ts": pa.array(ts, type=pa.timestamp("us")),
            "event_name": pa.array(chosen_names),
            "seq": pa.array(np.arange(rows, dtype=np.int64)),
            "device": pa.array(chosen_devices),
            "country": pa.array(chosen_countries),
        }

    def write(size, columns):
        """Write one crafted dataset's parquet + config."""
        crafted_dir = os.path.join(DATA_DIR, "crafted", size)
        os.makedirs(crafted_dir, exist_ok=True)
        table = pa.table(columns)
        pq.write_table(table, os.path.join(crafted_dir, "events.parquet"))
        write_crafted_config(size)
        print("  wrote", size, "(%d rows)" % table.num_rows)

    rows = 1000000
    # Sparse int64 actors drawn uniformly from the full 2^63 range: no dense-
    # range assumption can survive it.
    sparse_pool = rng.integers(-(2 ** 62), 2 ** 62, 20000, dtype=np.int64)
    sparse = sparse_pool[rng.integers(0, 20000, rows)]
    write("crafted_sparse", base_columns(rows, pa.array(sparse)))
    # UUID-string actors: identity via canonical key bytes, never arithmetic.
    uuid_pool = []
    for index in range(20000):
        raw = rng.bytes(16)
        uuid_pool.append("%s-%s-%s-%s-%s" % (
            raw[0:4].hex(), raw[4:6].hex(), raw[6:8].hex(), raw[8:10].hex(),
            raw[10:16].hex()))
    uuid_actors = []
    for index in rng.integers(0, 20000, rows):
        uuid_actors.append(uuid_pool[index])
    write("crafted_uuid", base_columns(rows, pa.array(uuid_actors)))
    # Microsecond timestamps with same-instant clusters: ordering falls to the
    # declared tiebreak; a shuffled row order must produce identical results.
    rows = 200000
    actors = rng.integers(0, 2000, rows)
    ts = (1735689600 * 1000000
          + rng.integers(0, 86400, rows, dtype=np.int64) * 1000000
          + rng.integers(0, 5, rows, dtype=np.int64))
    columns = base_columns(rows, pa.array(actors, type=pa.int64()))
    columns["ts"] = pa.array(ts, type=pa.timestamp("us"))
    write("crafted_subsecond", columns)


# ---------------------------------------------------------------------------
# Refresh scenario: the grown dataset
# ---------------------------------------------------------------------------


REFRESH_QUERIES = [
    ("paths_d4_unanchored", "PATHS ON web DEPTH 4 TOP 10"),
    ("paths_d4_forward", "PATHS ON web STARTING AT 'page_view' DEPTH 4 TOP 10"),
    ("paths_d4_backward", "PATHS ON web ENDING AT 'purchase' DEPTH 4 TOP 10"),
    ("paths_d8_unanchored", "PATHS ON web DEPTH 8 TOP 10"),
]


def stage_slice(source, staged, cut):
    """Write the rows with seq < cut of `source` to `staged`, arrival order.
    `source` is the dataset's own parquet: `--mode gen` already emits it
    sorted by (ts, tiebreak) with seq assigned 0..N-1 in that order, so a
    seq-cut is already a time-cut - no derived copy is needed."""
    import duckdb
    con = duckdb.connect()
    con.execute(
        "COPY (SELECT * FROM read_parquet(?) WHERE seq < ? ORDER BY seq) "
        "TO '%s' (FORMAT PARQUET)" % staged,
        [source, cut],
    )
    con.close()


def scenario_statement(runtime, sql):
    """Run one DDL statement, returning (wall seconds, status line)."""
    import pyarrow as pa
    started = time.time()
    status = pa.table(runtime.execute(sql)).to_pydict()
    return time.time() - started, status["status"][0]


def scenario_queries(runtime, results, stage_name):
    """Median warm PATHS timings at the current segment-pair count."""
    import pyarrow as pa
    timings = {}
    for name, sql in REFRESH_QUERIES:
        warm = []
        for _ in range(3):
            started = time.time()
            pa.table(runtime.execute(sql))
            warm.append((time.time() - started) * 1000.0)
        timings[name] = statistics.median(warm)
        print("    %-22s %8.1f ms" % (name, timings[name]))
    results["stages"][stage_name]["query_ms"] = timings


def scenario_crosscheck(runtime):
    """Index vs USING SCAN row-exact agreement on every query shape; a
    mismatch is a wrong answer and fails the run."""
    import pyarrow as pa
    for name, sql in REFRESH_QUERIES:
        indexed = pa.table(runtime.execute(sql)).to_pydict()
        scanned = pa.table(runtime.execute(sql + " USING SCAN")).to_pydict()
        if indexed != scanned:
            raise SystemExit(
                "index/scan mismatch on %s:\nindex: %s\nscan:  %s"
                % (name, indexed, scanned))
        print("    %-22s index == scan (%d rows)"
              % (name, len(indexed.get("path_id", []))))


def mode_refresh_scenario(size):
    """Grow the dataset through staged refreshes and measure every statement:
    CREATE over the first 60M events, REFRESH +20M, REFRESH to N-1000, then
    the 1,000-row delta - each wall clock with the paths-index share the
    status line now reports - plus per-stage PATHS medians as index pairs
    accumulate, and a final index-vs-scan exactness cross-check."""
    if size != "medium":
        raise SystemExit("refresh-scenario runs on --size medium (100M rows)")
    sys.path.insert(0, os.path.join(REPO_ROOT, "tests"))
    import fedq
    import pyarrow.parquet as pq

    _, source, _, _ = dataset_config(size)
    total = pq.ParquetFile(source).metadata.num_rows
    cuts = [int(total * 0.6), int(total * 0.8), total - 1000, total]
    stage_names = ["create_60m", "refresh_plus_20m", "refresh_to_n_minus_1k",
                   "refresh_1k_delta"]
    executions = 1 + len(cuts) + len(cuts) * len(REFRESH_QUERIES) * 3 \
        + len(REFRESH_QUERIES) * 2
    print("refresh-scenario: about %d engine executions, %d staged writes"
          % (executions, len(cuts)))

    stage_dir = os.path.join(DATA_DIR, "refresh_stage_%s" % size)
    os.makedirs(stage_dir, exist_ok=True)
    staged = os.path.join(stage_dir, "events.parquet")
    config = os.path.join(stage_dir, "config.yaml")
    with open(config, "w") as handle:
        handle.write(
            "datasources:\n  ev:\n    type: parquet\n    dir: %s\n" % stage_dir)
    # A fresh store every run: the sibling state dirs the engine hangs off
    # the config are cleared so the CREATE below is a real from-scratch build.
    for suffix in (".events", ".mv", ".stats.sqlite", ".stats.sqlite-shm",
                   ".stats.sqlite-wal"):
        state = config[: -len(".yaml")] + suffix
        if os.path.isdir(state):
            import shutil
            shutil.rmtree(state)
        elif os.path.exists(state):
            os.remove(state)

    results = {"size": size, "total_rows": total, "cuts": cuts, "stages": {}}
    out_path = os.path.join(DATA_DIR, "refresh_scenario_%s.json" % size)
    runtime = None
    for stage, (cut, stage_name) in enumerate(zip(cuts, stage_names)):
        print("stage %d: staging %s rows" % (stage, format(cut, ",")))
        started = time.time()
        stage_slice(source, staged, cut)
        staging_s = time.time() - started
        if runtime is None:
            runtime = fedq.Runtime(config)
            sql = ("CREATE EVENT DATASET web FROM ev.main.events "
                   "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
                   "PROPERTIES (device, country) WITH (refresh_key = 'seq')")
        else:
            sql = "REFRESH EVENT DATASET web"
        wall, status = scenario_statement(runtime, sql)
        print("  %-22s %7.2fs  (staging %.1fs)  %s"
              % (stage_name, wall, staging_s, status))
        results["stages"][stage_name] = {
            "rows": cut, "wall_s": wall, "status": status}
        scenario_queries(runtime, results, stage_name)
        with open(out_path, "w") as handle:
            json.dump(results, handle)
    print("cross-check: index vs USING SCAN")
    scenario_crosscheck(runtime)
    results["crosscheck"] = "index == scan on every query shape"
    with open(out_path, "w") as handle:
        json.dump(results, handle)
    print("refresh scenario written to", out_path)


# ---------------------------------------------------------------------------
# Scan envelope: what a deep USING SCAN costs in resources
# ---------------------------------------------------------------------------


ENVELOPE_DRIVER = r"""
import json, resource, sys, time
sys.path.insert(0, %(tests_dir)r)
import pyarrow as pa
import fedq
runtime = fedq.Runtime(%(config)r)
started = time.time()
rows = 0
if %(sql)r:
    table = pa.table(runtime.execute(%(sql)r))
    rows = table.num_rows
elapsed_ms = (time.time() - started) * 1000.0
usage = resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({
    "ms": elapsed_ms,
    "rows": rows,
    "max_rss_kb": usage.ru_maxrss,
    "user_s": usage.ru_utime,
    "sys_s": usage.ru_stime,
}))
"""


ENVELOPE_SHAPES = [
    ("d8_unanchored", "PATHS ON web DEPTH 8 TOP 10 USING SCAN"),
    ("d16_unanchored", "PATHS ON web DEPTH 16 TOP 10 USING SCAN"),
    ("d16_forward",
     "PATHS ON web STARTING AT 'page_view' DEPTH 16 TOP 10 USING SCAN"),
    ("d16_backward",
     "PATHS ON web ENDING AT 'purchase' DEPTH 16 TOP 10 USING SCAN"),
    ("d8_ranged",
     "PATHS ON web DEPTH 8 TOP 10 FROM '2025-01-05' TO '2025-01-20' USING SCAN"),
]


def envelope_run(config, sql):
    """One fresh-process execution reporting wall, rows, and rusage."""
    code = ENVELOPE_DRIVER % {
        "tests_dir": os.path.join(REPO_ROOT, "tests"),
        "config": config,
        "sql": sql,
    }
    output = subprocess.run([PYTHON, "-c", code], capture_output=True,
                            text=True, check=True)
    return json.loads(output.stdout.strip().splitlines()[-1])


def mode_scan_envelope(size):
    """Measure the exact-scan escape hatch's RESOURCE envelope: per deep
    USING SCAN shape, a fresh process's wall clock, peak RSS (against a
    runtime-only baseline), and CPU seconds - then a 4-way concurrent burst
    of the heaviest shape to see contention. Fresh processes isolate the
    scan's footprint from the parent's history."""
    config, _, _, _ = dataset_config(size)
    executions = 1 + len(ENVELOPE_SHAPES) + 4
    print("scan-envelope: about %d engine executions" % executions)
    baseline = envelope_run(config, "")
    print("baseline (runtime only): rss %.2f GiB" %
          (baseline["max_rss_kb"] / 1048576.0))
    results = {"size": size, "baseline_rss_kb": baseline["max_rss_kb"],
               "shapes": {}, "burst": []}
    for name, sql in ENVELOPE_SHAPES:
        run = envelope_run(config, sql)
        run["scan_rss_kb"] = run["max_rss_kb"] - baseline["max_rss_kb"]
        results["shapes"][name] = run
        print("  %-16s %9.1f ms  rss +%.2f GiB  cpu %.1fs  %d rows" % (
            name, run["ms"], run["scan_rss_kb"] / 1048576.0,
            run["user_s"] + run["sys_s"], run["rows"]))
    burst_sql = dict(ENVELOPE_SHAPES)["d16_unanchored"]
    code = ENVELOPE_DRIVER % {
        "tests_dir": os.path.join(REPO_ROOT, "tests"),
        "config": config,
        "sql": burst_sql,
    }
    procs = []
    for _ in range(4):
        procs.append(subprocess.Popen([PYTHON, "-c", code],
                                      stdout=subprocess.PIPE, text=True))
    for proc in procs:
        stdout, _ = proc.communicate()
        if proc.returncode != 0:
            raise SystemExit("burst runner failed with %d" % proc.returncode)
        run = json.loads(stdout.strip().splitlines()[-1])
        results["burst"].append(run)
        print("  burst d16_unanchored %9.1f ms  rss %.2f GiB" % (
            run["ms"], run["max_rss_kb"] / 1048576.0))
    out_path = os.path.join(DATA_DIR, "scan_envelope_%s.json" % size)
    with open(out_path, "w") as handle:
        json.dump(results, handle)
    print("scan envelope written to", out_path)


def main():
    """Parse arguments and dispatch the mode."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True,
                        choices=["gen", "gen-crafted", "save-refs", "engine",
                                 "compare", "refresh-scenario", "scan-envelope"])
    parser.add_argument("--size", default="medium")
    parser.add_argument("--queries", default=None,
                        help="comma-separated battery entry names")
    parser.add_argument("--warm-runs", type=int, default=3)
    parser.add_argument("--skip-cold", action="store_true")
    arguments = parser.parse_args()
    only = arguments.queries.split(",") if arguments.queries else None
    if arguments.mode == "gen":
        mode_gen(arguments.size)
    elif arguments.mode == "gen-crafted":
        mode_gen_crafted()
    elif arguments.mode == "save-refs":
        mode_save_refs(arguments.size, only)
    elif arguments.mode == "engine":
        mode_engine(arguments.size, only, arguments.warm_runs, arguments.skip_cold)
    elif arguments.mode == "refresh-scenario":
        mode_refresh_scenario(arguments.size)
    elif arguments.mode == "scan-envelope":
        mode_scan_envelope(arguments.size)
    else:
        mode_compare(arguments.size, only)


if __name__ == "__main__":
    main()
