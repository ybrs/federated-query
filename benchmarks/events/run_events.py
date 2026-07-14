"""THE synthetic event-analytics benchmark runner: the single entry point.

Subcommands (the first argument selects one; when none is given the runner
defaults to ``run`` so a flags-only invocation keeps working):

  generate   Build the DuckDB dataset file for a scale: one `events` table of
             synthetic events - power-law-skewed entities, zipf-skewed event
             types, sessionized timestamps, a couple of property columns. The
             shape is a DETERMINISTIC function of the row index (every column is
             derived from `hash(i * 11 + salt)`, never a thread-order-dependent
             RNG), so the same scale always yields the
             same bytes regardless of DuckDB's parallelism.
  save-refs  Measure the pure-DuckDB BASELINE for every analysis ONCE and store
             it (timing plus a result signature) in references_<scale>.duckdb.
             The baseline is a function of the DATA, not of the engine under
             test, so it is measured here and READ BACK by `run` - never
             re-measured per invocation. Refuses to overwrite existing refs.
  run        Drive the RUST engine (fedq.Runtime): CREATE EVENT VIEW over the
             generated table, REFRESH it, then time FUNNEL / SEGMENT / PATHS
             (cold = first run, warm = median of --warm-runs) next to the CACHED
             DuckDB baseline for each, and cross-check the engine's result
             against the baseline's stored signature. Reports to
             reports/events-<scale>.md.

The engine analyses are POST-SCAN kernels over a globally (entity, timestamp,
tiebreak)-sorted materialization; the DuckDB baselines are the honest
single-source SQL a user would otherwise write: a self-join + greedy-earliest
aggregation for the funnel (the standard anchored-funnel emulation), a plain
GROUP BY for segmentation, and a window-function sequence reconstruction for
paths. All three baselines reproduce the engine's pinned semantics exactly, so
the cross-check is a real correctness gate, not decoration.
"""

import argparse
import datetime
import json
import os
import resource
import sys
import threading
import time

import duckdb
import pyarrow as pa

HERE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE_DIR, "data")
REPORTS_DIR = os.path.join(HERE_DIR, "reports")
sys.path.insert(0, HERE_DIR)


# --- scale and dataset shape --------------------------------------------------
#
# A scale fixes the event count and the distinct-entity target. Entities are
# drawn from a bounded power law, floor(E * u^ENTITY_SKEW) with u = hash(i)/max
# in [0, 1): the density goes as entity^(1/skew - 1), so a few head entities
# carry many events and the tail is long - the real shape of behavioural data -
# while the top entity stays a realistic fraction of the log (~0.14% at
# skew 2), unlike a pure Zipf(s~=1) whose single head entity would swallow ~5%
# of all events and make per-entity sequence work pathological.
ENTITY_SKEW = 2.0

SCALES = {
    "small": {"events": 1_000_000, "entities": 50_000},
    "medium": {"events": 100_000_000, "entities": 500_000},
    "large": {"events": 1_000_000_000, "entities": 5_000_000},
}

# Twenty event types, listed most-frequent first (rank 1 is the head). Event
# rank is drawn Zipf over pow(21, u) -> 1..20, so `page_view` dominates and
# `cancel_account` is the rare tail; the funnels below are chosen to sit at
# deliberately different frequencies.
EVENT_TYPES = (
    (1, "page_view"),
    (2, "view_item"),
    (3, "search"),
    (4, "click_ad"),
    (5, "scroll"),
    (6, "add_to_cart"),
    (7, "watch_video"),
    (8, "share"),
    (9, "comment"),
    (10, "signup"),
    (11, "login"),
    (12, "begin_checkout"),
    (13, "add_payment"),
    (14, "purchase"),
    (15, "refund"),
    (16, "rate_item"),
    (17, "wishlist"),
    (18, "apply_coupon"),
    (19, "contact_support"),
    (20, "cancel_account"),
)

# Property-column value pools (skewed by the same pow-draw so head values
# dominate, like real device/geo mixes).
DEVICES = ("ios", "android", "web", "desktop")
COUNTRIES = ("US", "GB", "DE", "FR", "IN", "BR", "JP", "CA")

# The event timeline spans WINDOW_DAYS; each entity's events cluster into
# sessions of at most SESSION_SECONDS, whose base instants are a hash of
# (entity, session) so all events of one session land in one burst.
WINDOW_DAYS = 30
SESSIONS_PER_ENTITY = 12
SESSION_SECONDS = 3600
START_TS = "2025-01-01"

# The analysis parameters, fixed so every scale runs the identical statements.
FUNNEL_WINDOW_DAYS = 7
COMMON_FUNNEL = ("page_view", "view_item", "add_to_cart")
SELECTIVE_FUNNEL = ("signup", "begin_checkout", "purchase")
PATHS_MAX_DEPTH = 5
PATHS_TOP = 20
PATHS_ANCHOR = "page_view"

# The event view is materialized under this name; the raw table keeps its name.
EVENT_VIEW = "events_view"
SOURCE_NAME = "ev"
TABLE_NAME = "events"

# Role columns of the event view. `seq` (the global row index) is a genuine
# tiebreak: it gives equal-(entity, timestamp) events a deterministic order the
# event-name fallback could not, and the paths baseline sorts by the same key.
ENTITY_COL = "entity_id"
TIMESTAMP_COL = "ts"
EVENT_COL = "event_name"
TIEBREAK_COL = "seq"


# --- generate: build the synthetic DuckDB dataset -----------------------------


def _db_path(scale):
    """The DuckDB dataset file for a scale."""
    return os.path.join(DATA_DIR, "events_{0}.duckdb".format(scale))


def _config_path(scale):
    """The engine config for a scale; the event-view stores hang off its stem
    (`<stem>.mv/` and `<stem>.stats.sqlite` next to it)."""
    return os.path.join(DATA_DIR, "events_{0}.config.yaml".format(scale))


def _refs_path(scale):
    """The cached-baseline references file for a scale."""
    return os.path.join(DATA_DIR, "references_{0}.duckdb".format(scale))


def _unit(salt):
    """A deterministic pseudo-random draw in [0, 1) from the row index `i` and a
    salt: hash is a pure function of its arguments, so the value does not depend
    on DuckDB's scan/thread order and the dataset is byte-reproducible. The full
    64-bit hash is divided by 2^64 (not a coarse modulus) so the power-law tail
    is not aliased away - a modulus of 1e6 loses every entity whose rank
    interval is narrower than 1e-6, undershooting the distinct-entity target.
    Each salt hashes a DISTINCT integer (i * 11 + salt), not hash(i, salt):
    DuckDB's multi-argument hash only shifts the base hash by the salt, so
    hash(i, 1) and hash(i, 2) correlate ~0.6 - which would tie an entity's
    events to one event name and one session. Hashing distinct inputs
    decorrelates the draws (measured correlation ~0)."""
    return "(hash(i * 11 + {0})::DOUBLE / 18446744073709551616.0)".format(salt)


def _entity_expr(entities):
    """The bounded power-law entity id in [1, entities], as a BIGINT column."""
    return "least({0}, 1 + CAST(floor({0}.0 * pow({1}, {2})) AS BIGINT))".format(
        entities, _unit(1), ENTITY_SKEW
    )


def _event_rank_expr():
    """The Zipf event rank in [1, 20] (pow(21, u) reaches every rank)."""
    return "least(20, CAST(floor(pow(21.0, {0})) AS INT))".format(_unit(2))


def _timestamp_expr():
    """A sessionized timestamp: the session's base instant is a hash of
    (entity, session) so all its events share one burst, plus a within-session
    second offset. Sessions scatter across the WINDOW_DAYS span."""
    session = "CAST(floor({0} * {1}) AS BIGINT)".format(_unit(3), SESSIONS_PER_ENTITY)
    base = "(hash({0}, {1}, 7) % ({2} * 86400))".format(
        _entity_expr_alias(), session, WINDOW_DAYS
    )
    intra = "CAST(floor({0} * {1}) AS BIGINT)".format(_unit(4), SESSION_SECONDS)
    return "TIMESTAMP '{0}' + to_seconds({1} + {2})".format(START_TS, base, intra)


def _entity_expr_alias():
    """The entity id re-derived inline for the session hash (a scalar subquery
    would force a self-correlation; re-deriving the pure expression is cheaper
    and identical). ENTITIES is spliced with the entity count by the caller."""
    return "least(ENTITIES, 1 + CAST(floor(ENTITIES.0 * pow({0}, {1})) AS BIGINT))".format(
        _unit(1), ENTITY_SKEW
    )


def _pool_expr(pool, salt):
    """Pick from a small value pool with a skewed pow-draw, 1-based index."""
    values = "', '".join(pool)
    index = "CAST(floor(pow({0}.0, {1})) AS INT)".format(len(pool), _unit(salt))
    return "(ARRAY['{0}'])[least({1}, {2})]".format(values, len(pool), index)


def _names_values():
    """The 20-row (rank, name) VALUES clause for the event-type join."""
    rows = []
    for rank, name in EVENT_TYPES:
        rows.append("({0}, '{1}')".format(rank, name))
    return ", ".join(rows)


def _generate_sql(events, entities):
    """The single CREATE TABLE AS SELECT that builds the events table.

    Every column is a pure function of the range index `i`, joined to the
    20-row event-name table on the drawn rank. DuckDB streams the CTAS to the
    file; no sort and no RNG state are involved, so the result is deterministic.
    """
    entity = _entity_expr(entities)
    # The session hash needs the entity value; splice the entity count into the
    # ENTITIES placeholder so the inline re-derivation matches _entity_expr.
    timestamp = _timestamp_expr().replace("ENTITIES", str(entities))
    device = _pool_expr(DEVICES, 5)
    country = _pool_expr(COUNTRIES, 6)
    return (
        "CREATE TABLE {table} AS SELECT\n"
        "  {entity} AS {entity_col},\n"
        "  {timestamp} AS {ts_col},\n"
        "  names.name AS {event_col},\n"
        "  CAST(i AS BIGINT) AS {seq_col},\n"
        "  {device} AS device,\n"
        "  {country} AS country\n"
        "FROM range({events}) t(i)\n"
        "JOIN (SELECT * FROM (VALUES {names}) v(rank, name)) names\n"
        "  ON names.rank = {event_rank}\n"
    ).format(
        table=TABLE_NAME,
        entity=entity,
        entity_col=ENTITY_COL,
        timestamp=timestamp,
        ts_col=TIMESTAMP_COL,
        event_col=EVENT_COL,
        seq_col=TIEBREAK_COL,
        device=device,
        country=country,
        events=events,
        names=_names_values(),
        event_rank=_event_rank_expr(),
    )


def _report_dataset_shape(connection):
    """Print the built dataset's headline shape: row / entity / type counts and
    the head of the event-type frequency distribution."""
    counts = connection.execute(
        "SELECT count(*), count(DISTINCT {0}), count(DISTINCT {1}) FROM {2}".format(
            ENTITY_COL, EVENT_COL, TABLE_NAME
        )
    ).fetchone()
    print(
        "rows={0}  distinct entities={1}  distinct event types={2}".format(
            counts[0], counts[1], counts[2]
        )
    )
    head = connection.execute(
        "SELECT {0}, count(*) c FROM {1} GROUP BY 1 ORDER BY 2 DESC LIMIT 5".format(
            EVENT_COL, TABLE_NAME
        )
    ).fetchall()
    print("top event types: {0}".format(head))


def generate(options):
    """generate subcommand: build the scale's DuckDB dataset file."""
    scale = options.scale
    shape = SCALES[scale]
    events = options.events or shape["events"]
    entities = options.entities or shape["entities"]
    os.makedirs(DATA_DIR, exist_ok=True)
    db_path = _db_path(scale)
    if os.path.exists(db_path):
        os.remove(db_path)
    print(
        "generating scale={0}: {1} events over ~{2} entities -> {3}".format(
            scale, events, entities, db_path
        )
    )
    started = time.perf_counter()
    connection = duckdb.connect(db_path)
    connection.execute(_generate_sql(events, entities))
    elapsed = time.perf_counter() - started
    _report_dataset_shape(connection)
    connection.close()
    print("generated in {0:.1f}s ({1})".format(elapsed, _file_size(db_path)))


def _file_size(path):
    """A human-readable file size for the dataset banner."""
    return _human_bytes(os.path.getsize(path))


def _human_bytes(size):
    """A human-readable rendering of a raw byte count."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024.0 or unit == "GB":
            return "{0:.1f} {1}".format(size, unit)
        size = size / 1024.0
    return "{0:.1f} GB".format(size)


# --- analyses: the engine statements and their honest DuckDB baselines --------
#
# Each analysis pairs the engine statement (FUNNEL / SEGMENT / PATHS over the
# event view) with the pure-DuckDB SQL a user would otherwise write over the
# raw table. The baselines reproduce the pinned semantics of events-plan.md
# section 5 exactly, so the run-time cross-check is a correctness gate.


def _funnel_engine_sql(steps):
    """The FUNNEL statement for a 3-step funnel over the event view."""
    step_list = "', '".join(steps)
    return "FUNNEL OVER {0} STEPS ('{1}') WITHIN {2} DAYS".format(
        EVENT_VIEW, step_list, FUNNEL_WINDOW_DAYS
    )


def _funnel_baseline_sql(steps):
    """The anchored-funnel emulation: for each entity, greedy-earliest step
    matching with strict time increase and an inclusive window anchored at the
    step-1 event; distinct-entity counts per depth give re-entry (max over
    attempts) for free. This is the standard single-source funnel SQL."""
    step1, step2, step3 = steps
    window = "INTERVAL {0} DAY".format(FUNNEL_WINDOW_DAYS)
    return (
        "WITH ev AS (SELECT {entity} AS e, {ts} AS t, {event} AS n FROM {table}),\n"
        "s1 AS (SELECT e, t AS t1 FROM ev WHERE n = '{step1}'),\n"
        "s2 AS (SELECT s1.e, s1.t1, min(ev.t) AS t2 FROM s1 JOIN ev\n"
        "         ON ev.e = s1.e AND ev.n = '{step2}'\n"
        "         AND ev.t > s1.t1 AND ev.t <= s1.t1 + {window}\n"
        "       GROUP BY s1.e, s1.t1),\n"
        "s3 AS (SELECT s2.e, s2.t1, min(ev.t) AS t3 FROM s2 JOIN ev\n"
        "         ON ev.e = s2.e AND ev.n = '{step3}'\n"
        "         AND ev.t > s2.t2 AND ev.t <= s2.t1 + {window}\n"
        "       GROUP BY s2.e, s2.t1)\n"
        "SELECT 1 AS step, (SELECT count(DISTINCT e) FROM s1) AS entities\n"
        "UNION ALL SELECT 2, (SELECT count(DISTINCT e) FROM s2)\n"
        "UNION ALL SELECT 3, (SELECT count(DISTINCT e) FROM s3)\n"
        "ORDER BY step"
    ).format(
        entity=ENTITY_COL,
        ts=TIMESTAMP_COL,
        event=EVENT_COL,
        table=TABLE_NAME,
        step1=step1,
        step2=step2,
        step3=step3,
        window=window,
    )


def _segment_engine_sql(measure):
    """The SEGMENT statement for a measure, bucketed by day."""
    return "SEGMENT OVER {0} MEASURE {1} BY DAY".format(EVENT_VIEW, measure)


def _segment_baseline_sql(measure):
    """The plain GROUP BY that segmentation degenerates to: per UTC-day event
    count, or per-day distinct entities. date_trunc('day', ...) matches the
    engine's UTC calendar-day bucket."""
    aggregate = "count(*)"
    if measure == "ENTITIES":
        aggregate = "count(DISTINCT {0})".format(ENTITY_COL)
    return (
        "SELECT date_trunc('day', {ts}) AS bucket, {agg} AS value\n"
        "FROM {table} GROUP BY 1 ORDER BY 1"
    ).format(ts=TIMESTAMP_COL, agg=aggregate, table=TABLE_NAME)


def _paths_engine_sql(anchor):
    """The PATHS statement, optionally anchored by STARTING AT."""
    starting = ""
    if anchor is not None:
        starting = "STARTING AT '{0}' ".format(anchor)
    return "PATHS OVER {0} {1}MAX DEPTH {2} TOP {3}".format(
        EVENT_VIEW, starting, PATHS_MAX_DEPTH, PATHS_TOP
    )


def _paths_baseline_sql(anchor):
    """The window-function paths reconstruction: order each entity's events by
    (timestamp, tiebreak) - the declared-tiebreak order - collapse consecutive
    duplicate names, keep the first MAX DEPTH steps from the anchor (the first
    STARTING-AT event, or the entity's first event), join into a ' -> ' path,
    and rank the distinct paths by entity count then path. Reproduces the pinned
    paths semantics."""
    order = "{0}, {1}".format(TIMESTAMP_COL, TIEBREAK_COL)
    anchored = _paths_anchor_cte(anchor, order)
    return (
        "WITH ordered AS (\n"
        "  SELECT {entity} AS e, {event} AS n,\n"
        "    row_number() OVER (PARTITION BY {entity} ORDER BY {order}) AS pos\n"
        "  FROM {table}),\n"
        "{anchored}"
        "flagged AS (\n"
        "  SELECT e, n, pos,\n"
        "    lag(n) OVER (PARTITION BY e ORDER BY pos) AS prev\n"
        "  FROM anchored),\n"
        "dedup AS (SELECT e, n, pos FROM flagged WHERE prev IS NULL OR prev <> n),\n"
        "ranked AS (SELECT e, n,\n"
        "    row_number() OVER (PARTITION BY e ORDER BY pos) AS step FROM dedup),\n"
        "capped AS (SELECT e, string_agg(n, ' -> ' ORDER BY step) AS path,\n"
        "    count(*) AS depth FROM ranked WHERE step <= {depth} GROUP BY e)\n"
        "SELECT path, count(*) AS entities, any_value(depth) AS depth\n"
        "FROM capped GROUP BY path ORDER BY entities DESC, path ASC LIMIT {top}"
    ).format(
        entity=ENTITY_COL,
        event=EVENT_COL,
        order=order,
        table=TABLE_NAME,
        anchored=anchored,
        depth=PATHS_MAX_DEPTH,
        top=PATHS_TOP,
    )


def _paths_anchor_cte(anchor, order):
    """The CTE that trims each entity's stream to start at the anchor. With no
    anchor the stream is unchanged; with STARTING AT it keeps only positions at
    or after the entity's first anchor event (entities lacking it drop out)."""
    if anchor is None:
        return "anchored AS (SELECT e, n, pos FROM ordered),\n"
    return (
        "first_anchor AS (SELECT e, min(pos) AS apos FROM ordered\n"
        "  WHERE n = '{anchor}' GROUP BY e),\n"
        "anchored AS (SELECT o.e, o.n, o.pos FROM ordered o\n"
        "  JOIN first_anchor f ON o.e = f.e AND o.pos >= f.apos),\n"
    ).format(anchor=anchor)


def _analyses():
    """Every benchmarked analysis: a stable key, a human label, the engine
    statement, the baseline SQL, and the signature kind used to cross-check the
    engine result against the cached baseline."""
    analyses = []
    analyses.append(
        _analysis(
            "funnel_common",
            "FUNNEL common (page_view->view_item->add_to_cart, 7d)",
            _funnel_engine_sql(COMMON_FUNNEL),
            _funnel_baseline_sql(COMMON_FUNNEL),
            "funnel",
        )
    )
    analyses.append(
        _analysis(
            "funnel_selective",
            "FUNNEL selective (signup->begin_checkout->purchase, 7d)",
            _funnel_engine_sql(SELECTIVE_FUNNEL),
            _funnel_baseline_sql(SELECTIVE_FUNNEL),
            "funnel",
        )
    )
    analyses.append(
        _analysis(
            "segment_events",
            "SEGMENT MEASURE EVENTS BY DAY",
            _segment_engine_sql("EVENTS"),
            _segment_baseline_sql("EVENTS"),
            "segment",
        )
    )
    analyses.append(
        _analysis(
            "segment_entities",
            "SEGMENT MEASURE ENTITIES BY DAY",
            _segment_engine_sql("ENTITIES"),
            _segment_baseline_sql("ENTITIES"),
            "segment",
        )
    )
    analyses.append(
        _analysis(
            "paths_all",
            "PATHS MAX DEPTH 5 TOP 20",
            _paths_engine_sql(None),
            _paths_baseline_sql(None),
            "paths",
        )
    )
    analyses.append(
        _analysis(
            "paths_anchored",
            "PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20",
            _paths_engine_sql(PATHS_ANCHOR),
            _paths_baseline_sql(PATHS_ANCHOR),
            "paths",
        )
    )
    return analyses


def _analysis(key, label, engine_sql, baseline_sql, kind):
    """Assemble one analysis descriptor."""
    return {
        "key": key,
        "label": label,
        "engine_sql": engine_sql,
        "baseline_sql": baseline_sql,
        "kind": kind,
    }


# --- result signatures: the engine/baseline cross-check -----------------------
#
# A signature is a compact, order-stable fingerprint of a result that both the
# engine (Arrow) and the baseline (DuckDB rows) reduce to the same value when
# they agree. The baseline signature is stored in the refs file by save-refs and
# read back by run; the engine's is computed live and compared.


def _funnel_signature_rows(rows):
    """The per-step entity counts from baseline (step, entities) rows."""
    counts = []
    for row in sorted(rows):
        counts.append(int(row[1]))
    return counts


def _funnel_signature_table(table):
    """The per-step entity counts from the engine's funnel Arrow result."""
    counts = []
    for value in table.column("entities").to_pylist():
        counts.append(int(value))
    return counts


def _segment_signature_rows(rows):
    """(bucket count, total measured value) from baseline segment rows."""
    total = 0
    for row in rows:
        total += int(row[1])
    return [len(rows), total]


def _segment_signature_table(table):
    """(bucket count, total measured value) from the engine segment result."""
    total = 0
    for value in table.column("value").to_pylist():
        total += int(value)
    return [table.num_rows, total]


def _paths_signature_rows(rows):
    """The (path, entities) pairs from baseline path rows, in result order."""
    pairs = []
    for row in rows:
        pairs.append([row[0], int(row[1])])
    return pairs


def _paths_signature_table(table):
    """The (path, entities) pairs from the engine paths result, in order."""
    paths = table.column("path").to_pylist()
    entities = table.column("entities").to_pylist()
    pairs = []
    for index in range(len(paths)):
        pairs.append([paths[index], int(entities[index])])
    return pairs


def _baseline_signature(kind, rows):
    """Reduce baseline DuckDB rows to the signature for the analysis kind."""
    if kind == "funnel":
        return _funnel_signature_rows(rows)
    if kind == "segment":
        return _segment_signature_rows(rows)
    if kind == "paths":
        return _paths_signature_rows(rows)
    raise ValueError("unknown analysis kind: {0}".format(kind))


def _engine_signature(kind, table):
    """Reduce the engine's Arrow result to the signature for the analysis kind."""
    if kind == "funnel":
        return _funnel_signature_table(table)
    if kind == "segment":
        return _segment_signature_table(table)
    if kind == "paths":
        return _paths_signature_table(table)
    raise ValueError("unknown analysis kind: {0}".format(kind))


# --- save-refs: measure and cache the DuckDB baselines ------------------------


def _refuse_existing_refs(scale):
    """Refuse to overwrite a populated refs file: the baseline is a function of
    the data, measured once. Rebuilding requires deleting the file first, an
    explicit human act (the same contract as the TPC-DS suite's oracle)."""
    path = _refs_path(scale)
    if not os.path.exists(path):
        return
    existing = duckdb.connect(path, read_only=True)
    try:
        count = existing.execute("SELECT count(*) FROM baselines").fetchone()[0]
    except duckdb.CatalogException:
        return
    finally:
        existing.close()
    if count > 0:
        raise SystemExit(
            "REFUSED: {0} already holds {1} baselines. Delete the file first if "
            "the DATA changed.".format(path, count)
        )


def _measure_baseline(source, analysis, timeout):
    """Run one baseline: warm once, then time the median of three runs, and
    capture the result signature. A baseline that exceeds `timeout` is
    interrupted (thread-safe Connection.interrupt) and reported as N/A with the
    reason, never silently dropped - the naive SQL not scaling IS a finding."""
    watchdog = threading.Timer(timeout, source.interrupt)
    watchdog.start()
    try:
        rows = source.execute(analysis["baseline_sql"]).fetchall()
        timings = _timed_runs(source, analysis["baseline_sql"], 3)
    except (duckdb.InterruptException, duckdb.Error) as error:
        return None, "baseline N/A: {0}".format(_short_reason(error, timeout))
    finally:
        watchdog.cancel()
    signature = _baseline_signature(analysis["kind"], rows)
    return {"baseline_ms": _median(timings), "signature": signature}, ""


def _short_reason(error, timeout):
    """A one-line reason for a baseline that could not be measured."""
    if isinstance(error, duckdb.InterruptException):
        return "exceeded {0}s".format(timeout)
    return str(error).strip().splitlines()[0]


def _timed_runs(source, sql, runs):
    """Time `runs` executions of a query, returning the list of milliseconds."""
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        source.execute(sql).fetchall()
        timings.append((time.perf_counter() - start) * 1000.0)
    return timings


def _median(values):
    """The median of a non-empty list of numbers."""
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def save_refs(options):
    """save-refs subcommand: measure every analysis's DuckDB baseline once and
    store timings and signatures into references_<scale>.duckdb."""
    scale = options.scale
    _refuse_existing_refs(scale)
    source = duckdb.connect(_db_path(scale), read_only=True)
    if options.memory_limit > 0:
        source.execute("SET memory_limit='{0}MB'".format(options.memory_limit))
    refs = duckdb.connect(_refs_path(scale))
    refs.execute(
        "CREATE OR REPLACE TABLE baselines "
        "(label VARCHAR, baseline_ms DOUBLE, signature VARCHAR, note VARCHAR)"
    )
    analyses = _analyses()
    print(
        "[{0}] measuring {1} DuckDB baselines (timeout {2}s each)".format(
            scale, len(analyses), options.baseline_timeout
        )
    )
    for analysis in analyses:
        _save_one_baseline(source, refs, analysis, options.baseline_timeout)
    source.close()
    refs.close()
    print("baselines written: {0}".format(_refs_path(scale)))


def _save_one_baseline(source, refs, analysis, timeout):
    """Measure one baseline and insert its row into the refs file."""
    measured, note = _measure_baseline(source, analysis, timeout)
    baseline_ms = None
    signature = None
    if measured is not None:
        baseline_ms = measured["baseline_ms"]
        signature = json.dumps(measured["signature"])
    refs.execute(
        "INSERT INTO baselines VALUES (?, ?, ?, ?)",
        [analysis["key"], baseline_ms, signature, note],
    )
    printed = "-" if baseline_ms is None else "{0:.1f}ms".format(baseline_ms)
    print("  {0:16} baseline {1}  {2}".format(analysis["key"], printed, note))


def _read_baseline(refs, key):
    """Read one cached baseline row (ms, signature, note) from the refs file."""
    row = refs.execute(
        "SELECT baseline_ms, signature, note FROM baselines WHERE label = ?", [key]
    ).fetchone()
    if row is None:
        raise SystemExit(
            "no cached baseline for '{0}'; run save-refs first".format(key)
        )
    signature = None if row[1] is None else json.loads(row[1])
    return row[0], signature, row[2]


# --- run: drive the Rust engine over the event view ---------------------------

# Hard, deterministic wall budgets per scale (seconds). A run past its budget is
# killed by a daemon watchdog that os._exits, so even a hung native call cannot
# outlive it. A run over budget is a regression to fix, not a budget to raise.
WALL_BUDGET_SECONDS = {"small": 180, "medium": 900, "large": 3600}


def _arm_watchdog(scale):
    """Kill the whole process when the scale's wall budget expires."""
    budget = WALL_BUDGET_SECONDS[scale]

    def _kill():
        """Print the budget-kill banner and terminate unconditionally."""
        sys.stderr.write(
            "WALL BUDGET EXCEEDED: {0} run past {1}s - killed. Fix the "
            "regression; the budget does not move.\n".format(scale, budget)
        )
        sys.stderr.flush()
        os._exit(124)

    timer = threading.Timer(budget, _kill)
    timer.daemon = True
    timer.start()
    print("[{0}] wall budget: {1}s (deterministic kill)".format(scale, budget))


def _store_dir(scale):
    """The event-view chunk store directory for a scale (the derived-structure
    sidecars live beside the chunks under it). The accelerator roots it at the
    config file's stem, so the `.config.yaml` config yields `.config.mv/`."""
    return os.path.join(DATA_DIR, "events_{0}.config.mv".format(scale))


def _sidecar_sizes(scale):
    """The on-disk bytes of the derived structures: (bitmaps, segment). Sums the
    sidecar files under the store, which are named by their generation."""
    bitmaps = 0
    segagg = 0
    for root, _dirs, files in os.walk(_store_dir(scale)):
        for name in files:
            full = os.path.join(root, name)
            if name.startswith("bitmaps-") and name.endswith(".fqb"):
                bitmaps += os.path.getsize(full)
            elif name.startswith("segagg-") and name.endswith(".arrow"):
                segagg += os.path.getsize(full)
    return bitmaps, segagg


def _peak_rss_mb():
    """The run process's peak resident set in MB. ru_maxrss is the whole
    process's high-water RSS (kilobytes on Linux), so it covers the engine, the
    loaded chunks, and PyArrow - an honest ceiling, not a per-structure figure."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _write_config(scale):
    """Write the engine config for a scale: one read-only DuckDB source over the
    dataset file. The event-view stores hang off this file's stem."""
    text = (
        "datasources:\n"
        "  {source}:\n"
        "    type: duckdb\n"
        "    path: {db_path}\n"
        "    read_only: true\n"
    ).format(source=SOURCE_NAME, db_path=_db_path(scale))
    path = _config_path(scale)
    with open(path, "w") as handle:
        handle.write(text)
    return path


def _create_view_sql():
    """The CREATE EVENT VIEW statement over the generated table, declaring all
    four roles (the seq tiebreak included)."""
    return (
        "CREATE EVENT VIEW {view} ENTITY {entity} TIMESTAMP {ts} EVENT {event} "
        "TIEBREAK {tiebreak} AS SELECT {entity}, {ts}, {event}, {tiebreak}, "
        "device, country FROM {source}.main.{table}"
    ).format(
        view=EVENT_VIEW,
        entity=ENTITY_COL,
        ts=TIMESTAMP_COL,
        event=EVENT_COL,
        tiebreak=TIEBREAK_COL,
        source=SOURCE_NAME,
        table=TABLE_NAME,
    )


def _drop_existing_view(runtime):
    """Drop a leftover event view from a prior run so CREATE starts clean. A
    missing view is the expected first-run state and is not an error; any other
    failure is real and re-raised."""
    try:
        runtime.execute("DROP EVENT VIEW {0}".format(EVENT_VIEW))
        print("dropped a pre-existing event view")
    except RuntimeError as error:
        if "does not exist" not in str(error):
            raise


def _build_view(runtime):
    """Create then refresh the event view, timing each build step. CREATE
    materializes (scan + validate + global sort + write chunks); REFRESH
    re-pulls and re-sorts. Returns (create_ms, refresh_ms)."""
    create_ms = _time_execute(runtime, _create_view_sql())
    refresh_ms = _time_execute(runtime, "REFRESH EVENT VIEW {0}".format(EVENT_VIEW))
    print(
        "built event view: CREATE {0:.0f}ms  REFRESH {1:.0f}ms".format(
            create_ms, refresh_ms
        )
    )
    return create_ms, refresh_ms


def _time_execute(runtime, sql):
    """Execute one statement and return its wall time in milliseconds. The Arrow
    result is materialized so the timing includes delivering every batch."""
    start = time.perf_counter()
    pa.table(runtime.execute(sql))
    return (time.perf_counter() - start) * 1000.0


def _time_analysis(runtime, analysis, warm_runs):
    """Run one analysis cold (first execution) then warm (median of warm_runs on
    the live runtime), returning (cold_ms, warm_ms, table)."""
    start = time.perf_counter()
    table = pa.table(runtime.execute(analysis["engine_sql"]))
    cold_ms = (time.perf_counter() - start) * 1000.0
    warm_timings = []
    for _ in range(warm_runs):
        start = time.perf_counter()
        table = pa.table(runtime.execute(analysis["engine_sql"]))
        warm_timings.append((time.perf_counter() - start) * 1000.0)
    warm_ms = None
    if warm_timings:
        warm_ms = _median(warm_timings)
    return cold_ms, warm_ms, table


def _evaluate_analysis(runtime, refs, analysis, warm_runs):
    """Time one analysis on the engine, read its cached DuckDB baseline, and
    cross-check the engine result against the baseline signature."""
    cold_ms, warm_ms, table = _time_analysis(runtime, analysis, warm_runs)
    baseline_ms, baseline_sig, note = _read_baseline(refs, analysis["key"])
    engine_sig = _engine_signature(analysis["kind"], table)
    match = _match_label(engine_sig, baseline_sig)
    metric_ms = warm_ms if warm_ms is not None else cold_ms
    ratio = None
    if baseline_ms:
        ratio = metric_ms / baseline_ms
    return {
        "label": analysis["label"],
        "cold_ms": cold_ms,
        "warm_ms": warm_ms,
        "baseline_ms": baseline_ms,
        "ratio": ratio,
        "match": match,
        "rows": table.num_rows,
        "note": note,
    }


def _match_label(engine_sig, baseline_sig):
    """Whether the engine and baseline signatures agree. A missing baseline
    signature (baseline N/A) is reported as N/A, never as a pass."""
    if baseline_sig is None:
        return "N/A"
    return "AGREE" if engine_sig == baseline_sig else "DIFFER"


def run(options):
    """run subcommand: build the event view and time every analysis on the Rust
    engine against the cached DuckDB baseline, then report."""
    import fedq

    scale = options.scale
    _require_dataset(scale)
    _arm_watchdog(scale)
    config_path = _write_config(scale)
    refs = duckdb.connect(_refs_path(scale), read_only=True)
    runtime = fedq.Runtime(config_path)
    analyses = _analyses()
    _print_run_banner(scale, analyses, options.warm_runs)
    _drop_existing_view(runtime)
    started = time.perf_counter()
    create_ms, refresh_ms = _build_view(runtime)
    results = []
    for analysis in analyses:
        result = _evaluate_analysis(runtime, refs, analysis, options.warm_runs)
        results.append(result)
        _print_result(result)
    total_ms = (time.perf_counter() - started) * 1000.0
    refs.close()
    _finish(scale, results, create_ms, refresh_ms, total_ms, options)


def _require_dataset(scale):
    """Fail loudly when the dataset or its refs are missing (generate/save-refs
    must run first) rather than building an empty view."""
    if not os.path.exists(_db_path(scale)):
        raise SystemExit(
            "no dataset at {0}; run 'generate --scale {1}' first".format(
                _db_path(scale), scale
            )
        )
    if not os.path.exists(_refs_path(scale)):
        raise SystemExit(
            "no baselines at {0}; run 'save-refs --scale {1}' first".format(
                _refs_path(scale), scale
            )
        )


def _print_run_banner(scale, analyses, warm_runs):
    """Print the run banner, the engine-execution budget, and column headings."""
    runs_each = 1 + warm_runs
    engine_executions = 2 + len(analyses) * runs_each
    print("\n==== events {0} (ours=fedq-rust vs cached DuckDB baseline) ====".format(scale))
    print(
        "[{0}] engine executions this invocation: {1} "
        "(1 CREATE + 1 REFRESH + {2} analyses x {3} run(s) each); "
        "baseline CACHED".format(
            scale, engine_executions, len(analyses), runs_each
        )
    )
    print(
        "{0:52} {1:>9} {2:>9} {3:>10} {4:>7} {5:>7}".format(
            "analysis", "cold", "warm", "duckdb", "ratio", "match"
        )
    )


def _ms_text(value):
    """A milliseconds cell like '123.4', or '-' when there is no timing."""
    if value is None:
        return "-"
    return "{0:.1f}".format(value)


def _ratio_text(value):
    """An ours/DuckDB ratio cell like '0.31x', or '-' when unavailable."""
    if value is None:
        return "-"
    return "{0:.2f}x".format(value)


def _print_result(result):
    """Print one analysis's outcome row."""
    print(
        "{0:52} {1:>9} {2:>9} {3:>10} {4:>7} {5:>7}".format(
            result["label"][:52],
            _ms_text(result["cold_ms"]),
            _ms_text(result["warm_ms"]),
            _ms_text(result["baseline_ms"]),
            _ratio_text(result["ratio"]),
            result["match"],
        ),
        flush=True,
    )


def _finish(scale, results, create_ms, refresh_ms, total_ms, options):
    """Print the summary and write the markdown report."""
    _print_summary(scale, results, total_ms)
    derived = _derived_facts(scale)
    _print_derived(derived)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, "events-{0}.md".format(scale))
    _write_report(scale, results, create_ms, refresh_ms, total_ms, derived, options, report_path)
    print("wrote report: {0}".format(report_path))


def _derived_facts(scale):
    """The derived-structure measurements for the report: sidecar sizes on disk
    and the run process's peak resident set."""
    bitmaps_bytes, segagg_bytes = _sidecar_sizes(scale)
    return {
        "bitmaps_bytes": bitmaps_bytes,
        "segagg_bytes": segagg_bytes,
        "peak_rss_mb": _peak_rss_mb(),
    }


def _print_derived(derived):
    """Print the derived-structure sidecar sizes and peak memory."""
    print(
        "derived sidecars: bitmaps {0}  segment {1}  |  peak RSS {2:.0f} MB".format(
            _human_bytes(derived["bitmaps_bytes"]),
            _human_bytes(derived["segagg_bytes"]),
            derived["peak_rss_mb"],
        )
    )


def _print_summary(scale, results, total_ms):
    """Print the AGREE/DIFFER tally and the wall total."""
    agree = 0
    differ = 0
    for result in results:
        if result["match"] == "AGREE":
            agree += 1
        elif result["match"] == "DIFFER":
            differ += 1
    print("-" * 96)
    print(
        "[{0}] {1} agree | {2} differ   (analysis wall {3:.1f}s)".format(
            scale, agree, differ, total_ms / 1000.0
        )
    )


def _report_row(result):
    """Render one analysis's markdown table row."""
    note = result["note"] if result["note"] else "-"
    return "| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} |".format(
        result["label"],
        _ms_text(result["cold_ms"]),
        _ms_text(result["warm_ms"]),
        _ms_text(result["baseline_ms"]),
        _ratio_text(result["ratio"]),
        result["match"],
        result["rows"],
        note.replace("|", "\\|"),
    )


def _report_lines(scale, results, create_ms, refresh_ms, total_ms, derived, options):
    """Assemble the full markdown report for a run."""
    shape = SCALES[scale]
    stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "# Synthetic event-analytics benchmark report ({0})".format(scale),
        "",
        "Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.",
        "Dataset: ~{0} events over ~{1} entities, {2} event types, "
        "sessionized across {3} days.".format(
            shape["events"], shape["entities"], len(EVENT_TYPES), WINDOW_DAYS
        ),
        "Baseline: pure-DuckDB SQL over the same file, cached once by save-refs "
        "in references_{0}.duckdb.".format(scale),
        "Build: CREATE {0:.0f}ms, REFRESH {1:.0f}ms (scan + global "
        "(entity, timestamp, tiebreak) sort + chunk write + derived "
        "sidecars).".format(create_ms, refresh_ms),
        "Derived sidecars on disk: bitmaps {0}, segment {1}. Peak run RSS "
        "{2:.0f} MB.".format(
            _human_bytes(derived["bitmaps_bytes"]),
            _human_bytes(derived["segagg_bytes"]),
            derived["peak_rss_mb"],
        ),
        "Warm runs per analysis: {0}. Generated: {1}.".format(
            options.warm_runs, stamp
        ),
        "",
        "Ratio is engine (warm when measured, else cold) / DuckDB baseline; "
        "below 1.0x the engine is faster. Match cross-checks the engine result "
        "against the baseline signature (AGREE = identical counts).",
        "",
        "| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        lines.append(_report_row(result))
    lines.append("")
    lines.append("Analysis wall total: {0:.1f}s.".format(total_ms / 1000.0))
    lines.append("")
    return lines


def _write_report(scale, results, create_ms, refresh_ms, total_ms, derived, options, path):
    """Write the per-analysis markdown report for this run."""
    lines = _report_lines(
        scale, results, create_ms, refresh_ms, total_ms, derived, options
    )
    with open(path, "w") as handle:
        handle.write("\n".join(lines))


# --- command line -------------------------------------------------------------

SUBCOMMANDS = ("generate", "save-refs", "run")


def _build_parser():
    """Build the argparse parser with one subparser per subcommand."""
    parser = argparse.ArgumentParser(
        description="Synthetic event-analytics benchmark (generate / save-refs / run)."
    )
    subparsers = parser.add_subparsers(dest="command")

    generate_parser = subparsers.add_parser(
        "generate", help="build the synthetic DuckDB dataset for a scale"
    )
    generate_parser.add_argument("--scale", choices=sorted(SCALES), default="small")
    generate_parser.add_argument("--events", type=int, default=None)
    generate_parser.add_argument("--entities", type=int, default=None)
    generate_parser.set_defaults(func=generate)

    refs_parser = subparsers.add_parser(
        "save-refs", help="measure and cache the DuckDB baselines for a scale"
    )
    refs_parser.add_argument("--scale", choices=sorted(SCALES), default="small")
    refs_parser.add_argument("--baseline-timeout", type=float, default=300.0)
    refs_parser.add_argument("--memory-limit", type=int, default=0)
    refs_parser.set_defaults(func=save_refs)

    run_parser = subparsers.add_parser(
        "run", help="drive the engine over the event view and report"
    )
    run_parser.add_argument("--scale", choices=sorted(SCALES), default="small")
    run_parser.add_argument("--warm-runs", type=int, default=3)
    run_parser.set_defaults(func=run)

    return parser


def _argv_with_default_command(argv):
    """Prepend the default ``run`` command when none is named."""
    if not argv:
        return ["run"]
    if argv[0] in SUBCOMMANDS or argv[0] in ("-h", "--help"):
        return argv
    return ["run"] + argv


def main():
    """Entry point: dispatch the selected subcommand (default ``run``)."""
    parser = _build_parser()
    args = parser.parse_args(_argv_with_default_command(sys.argv[1:]))
    args.func(args)


if __name__ == "__main__":
    main()
