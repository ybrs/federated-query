"""End-to-end event-analytics SQL surface tests.

Builds a small parquet event table, drives the engine through the native
``fedq`` module (CREATE EVENT DATASET, then every analysis statement), and
checks each result against a DuckDB oracle computed over the same parquet.
Also pins the loud-failure contract: unknown datasets, unknown properties,
unknown event names, and unsupported modes all raise.
"""

import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)

import fedq  # noqa: E402


def build_events_table():
    """A deterministic small event table exercising ties and nulls."""
    rows = []
    base = 1735689600000000  # 2025-01-01 UTC in microseconds
    day = 86400000000
    seq = 0
    for actor in range(1, 21):
        for step_index, name in enumerate(["signup", "view", "purchase"]):
            if actor % 4 == 0 and name == "purchase":
                continue
            rows.append((
                actor,
                base + (actor % 5) * day + step_index * 3600000000,
                name,
                seq,
                None if actor % 7 == 0 else ["ios", "android"][actor % 2],
                "DE" if actor % 3 == 0 else "US",
            ))
            seq += 1
    # A same-instant pair ordered only by the tiebreak.
    rows.append((1, base, "view", seq, "ios", "US"))
    seq += 1
    rows.append((1, base, "purchase", seq + 1, "ios", "US"))
    actors, ts, names, seqs, devices, countries = [], [], [], [], [], []
    for row in rows:
        actors.append(row[0])
        ts.append(row[1])
        names.append(row[2])
        seqs.append(row[3])
        devices.append(row[4])
        countries.append(row[5])
    return pa.table({
        "entity_id": pa.array(actors, type=pa.int64()),
        "ts": pa.array(ts, type=pa.timestamp("us")),
        "event_name": pa.array(names),
        "seq": pa.array(seqs, type=pa.int64()),
        "device": pa.array(devices),
        "country": pa.array(countries),
    })


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    """A runtime over a fresh store with the small dataset built once."""
    root = tmp_path_factory.mktemp("events-e2e")
    table = build_events_table()
    pq.write_table(table, root / "events.parquet")
    config = root / "config.yaml"
    config.write_text(
        "datasources:\n  ev:\n    type: parquet\n    dir: %s\n" % root
    )
    runtime = fedq.Runtime(str(config))
    status = pa.table(runtime.execute(
        "CREATE EVENT DATASET web FROM ev.main.events "
        "ACTOR entity_id TIME ts EVENT event_name TIEBREAK seq "
        "PROPERTIES (device, country) WITH (refresh_key = 'seq')"
    ))
    assert "CREATE EVENT DATASET" in status.column("status")[0].as_py()
    return runtime, duckdb.connect(), str(root / "events.parquet")


def run(runtime, sql):
    """Execute one statement into a pyarrow table."""
    return pa.table(runtime.execute(sql))


def test_show_event_datasets_lists_the_dataset(engine):
    runtime, _, _ = engine
    listing = run(runtime, "SHOW EVENT DATASETS").to_pydict()
    assert listing["name"] == ["web"]
    assert listing["tiebreak"] == ["seq"]
    assert listing["events"][0] == 57


def test_funnel_matches_the_oracle(engine):
    runtime, oracle, parquet = engine
    table = run(
        runtime,
        "FUNNEL ('signup', 'view', 'purchase') ON web WINDOW 1 DAY",
    ).to_pydict()
    # Oracle: strict-order chase per actor (each actor's stream is tiny, so
    # the greedy chain from the first signup is the exists-form depth).
    depths = oracle.execute("""
        WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
                    FROM read_parquet(?)),
        anchors AS (SELECT actor, ts t1, tb b1 FROM ev WHERE name = 'signup'),
        c2 AS (SELECT a.actor, a.t1, a.b1, l.t2, l.b2 FROM anchors a
               LEFT JOIN LATERAL (SELECT e.ts t2, e.tb b2 FROM ev e
                 WHERE e.actor = a.actor AND e.name = 'view'
                   AND (e.ts, e.tb) > (a.t1, a.b1)
                   AND e.ts - a.t1 <= INTERVAL 1 DAY
                 ORDER BY e.ts, e.tb LIMIT 1) l ON TRUE),
        c3 AS (SELECT c.*, l.t3 FROM c2 c
               LEFT JOIN LATERAL (SELECT e.ts t3 FROM ev e
                 WHERE e.actor = c.actor AND e.name = 'purchase'
                   AND (e.ts, e.tb) > (c.t2, c.b2)
                   AND e.ts - c.t1 <= INTERVAL 1 DAY
                 ORDER BY e.ts, e.tb LIMIT 1) l ON TRUE),
        per_actor AS (SELECT actor,
            max(CASE WHEN t3 IS NOT NULL THEN 3
                     WHEN t2 IS NOT NULL THEN 2 ELSE 1 END) AS depth
            FROM c3 GROUP BY actor)
        SELECT d, count(*) FROM per_actor, generate_series(1, 3) t(d)
        WHERE depth >= d GROUP BY d ORDER BY d
    """, [parquet]).fetchall()
    expected = {}
    for depth, count in depths:
        expected[depth] = count
    assert table["entered"] == [expected[1], expected[2], expected[3]]


def test_segment_uniques_matches_the_oracle(engine):
    runtime, oracle, parquet = engine
    table = run(
        runtime,
        "SEGMENT UNIQUES ON web EVENT 'signup' BUCKET DAY WHERE country = 'US'",
    ).to_pydict()
    expected = oracle.execute("""
        SELECT date_trunc('day', ts) AS bucket, count(DISTINCT entity_id)
        FROM read_parquet(?)
        WHERE event_name = 'signup' AND country = 'US'
        GROUP BY 1 ORDER BY 1
    """, [parquet]).fetchall()
    got = list(zip(table["bucket"], table["value"]))
    assert len(got) == len(expected)
    for (bucket, value), (oracle_bucket, oracle_value) in zip(got, expected):
        assert bucket == oracle_bucket
        assert value == oracle_value


def test_segment_count_preagg_matches_the_oracle(engine):
    runtime, oracle, parquet = engine
    table = run(runtime, "SEGMENT COUNT ON web BUCKET DAY").to_pydict()
    expected = oracle.execute("""
        SELECT date_trunc('day', ts), count(*) FROM read_parquet(?)
        GROUP BY 1 ORDER BY 1
    """, [parquet]).fetchall()
    assert table["value"] == [row[1] for row in expected]


def test_retention_matches_the_oracle(engine):
    runtime, oracle, parquet = engine
    table = run(
        runtime,
        "RETENTION ON web BIRTH 'signup' RETURN ANY EVENT PERIOD DAY",
    ).to_pydict()
    cells = {}
    for cohort, off, retained in oracle.execute("""
        WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
                    FROM read_parquet(?)),
        births AS (SELECT actor, min_by(ts, (ts, tb)) AS bts,
                          min_by(tb, (ts, tb)) AS btb,
                          date_trunc('day', min_by(ts, (ts, tb))) AS cohort
                   FROM ev WHERE name = 'signup' GROUP BY actor),
        rets AS (SELECT DISTINCT b.actor, b.cohort,
                        date_diff('day', b.cohort, date_trunc('day', e.ts)) AS off
                 FROM births b JOIN ev e USING (actor)
                 WHERE (e.ts, e.tb) > (b.bts, b.btb))
        SELECT cohort, off, count(*) FROM rets GROUP BY 1, 2
    """, [parquet]).fetchall():
        cells[(cohort, off)] = retained
    sizes = {}
    for cohort, size in oracle.execute("""
        WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
                    FROM read_parquet(?)),
        births AS (SELECT actor, date_trunc('day', min_by(ts, (ts, tb))) AS cohort
                   FROM ev WHERE name = 'signup' GROUP BY actor)
        SELECT cohort, count(*) FROM births GROUP BY 1
    """, [parquet]).fetchall():
        sizes[cohort] = size
    for index in range(len(table["cohort_start"])):
        cohort = table["cohort_start"][index]
        offset = table["period_offset"][index]
        assert table["cohort_size"][index] == sizes.get(cohort, 0)
        assert table["retained"][index] == cells.get((cohort, offset), 0)


def test_paths_matches_the_oracle(engine):
    runtime, oracle, parquet = engine
    table = run(
        runtime, "PATHS ON web STARTING AT 'signup' DEPTH 3 TOP 10"
    ).to_pydict()
    expected = oracle.execute("""
        WITH ev AS (SELECT entity_id AS actor, ts, seq AS tb, event_name AS name
                    FROM read_parquet(?)),
        collapsed AS (SELECT actor, ts, tb, name FROM
            (SELECT *, lag(name) OVER w AS prev FROM ev
             WINDOW w AS (PARTITION BY actor ORDER BY ts, tb))
            WHERE prev IS NULL OR prev <> name),
        strung AS (SELECT actor, name,
                          lead(name, 1) OVER w AS l1, lead(name, 2) OVER w AS l2
                   FROM collapsed
                   WINDOW w AS (PARTITION BY actor ORDER BY ts, tb))
        SELECT name || coalesce(' -> ' || l1, '') || coalesce(' -> ' || l2, '')
                 AS path, count(*) AS occ
        FROM strung WHERE name = 'signup' AND l1 IS NOT NULL
        GROUP BY 1 ORDER BY occ DESC, path LIMIT 10
    """, [parquet]).fetchall()
    got = list(zip(table["path"], table["occurrences"]))
    assert got == expected


def test_invalid_statements_raise(engine):
    runtime, _, _ = engine
    with pytest.raises(Exception, match="does not exist"):
        run(runtime, "FUNNEL ('a', 'b') ON nope WINDOW 1 DAY")
    with pytest.raises(Exception, match="no property"):
        run(runtime, "SEGMENT COUNT ON web BUCKET DAY WHERE bogus = 'x'")
    with pytest.raises(Exception, match="nearest"):
        run(runtime, "FUNNEL ('signup', 'porchase') ON web WINDOW 1 DAY")
    with pytest.raises(Exception, match="ANY_ORDER"):
        run(runtime, "FUNNEL ('signup', 'view') ON web WINDOW 1 DAY MODE ANY_ORDER")
    with pytest.raises(Exception, match="DEPTH"):
        run(runtime, "PATHS ON web DEPTH 99")


def test_refresh_and_drop_round_trip(engine, tmp_path):
    runtime, _, _ = engine
    status = run(runtime, "REFRESH EVENT DATASET web").to_pydict()
    # The parquet source is unchanged, so the delta past the watermark is
    # empty (the parquet connector carries no version token).
    assert "0 events appended" in status["status"][0]
    # DROP removes it; the next statement no longer resolves the name.
    run(runtime, "DROP EVENT DATASET web")
    with pytest.raises(Exception, match="does not exist"):
        run(runtime, "SEGMENT COUNT ON web BUCKET DAY")
