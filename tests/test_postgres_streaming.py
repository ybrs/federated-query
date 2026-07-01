"""THE streaming contract for remote sources — the most important perf invariant.

When the engine joins a PostgreSQL table, those rows must reach the coordinator
DuckDB. There are two ways to do it:

  WRONG: drain the whole result into our Python process first
         (``cursor.fetch_arrow_table()``), then hand DuckDB a finished table.
         For a 100M-row scan that buffers gigabytes in our process and blocks
         for seconds before DuckDB does any work.

  RIGHT: hand DuckDB a LAZY ``pyarrow.RecordBatchReader`` and let DuckDB pull
         batches itself — so it drives the fetch (and can spill, prune, or stop
         early) while we hold almost nothing.

``PostgreSQLDataSource.execute_query`` (ADBC path) MUST do the second. These
tests pin that: the data path must use the streaming reader, and the first batch
must come back without draining the entire result.
"""

import os
import time

import psycopg2
import pyarrow as pa
import pytest

from federated_query.datasources.postgresql import PostgreSQLDataSource

# Big enough to span several bounded ADBC batches (so a streamed first batch is
# a small fraction of the whole), small enough to build in ~a second.
PROBE_ROWS = 4_000_000


def _pg_config():
    """ADBC PostgreSQL config pointed at the local test harness."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": ["public"],
        "driver": "adbc",
    }


@pytest.fixture(scope="module")
def streaming_source():
    """An ADBC PostgreSQL source over a 2M-row probe table (dropped on teardown)."""
    pytest.importorskip("adbc_driver_postgresql")
    config = _pg_config()
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS stream_probe")
        cursor.execute(
            "CREATE TABLE stream_probe AS "
            f"SELECT g AS id FROM generate_series(1, {PROBE_ROWS}) g"
        )
    source = PostgreSQLDataSource(name="default", config=config)
    source.connect()
    yield source
    source.disconnect()
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS stream_probe")
    conn.close()


def test_execute_query_uses_streaming_reader_not_fetch_arrow_table(
    streaming_source, monkeypatch
):
    """The data path must pull through the lazy reader, never drain to a table."""
    import adbc_driver_postgresql.dbapi as dbapi

    calls = {"fetch_arrow_table": 0, "fetch_record_batch": 0}
    original_table = dbapi.Cursor.fetch_arrow_table
    original_reader = dbapi.Cursor.fetch_record_batch

    def spy_table(self, *args, **kwargs):
        calls["fetch_arrow_table"] += 1
        return original_table(self, *args, **kwargs)

    def spy_reader(self, *args, **kwargs):
        calls["fetch_record_batch"] += 1
        return original_reader(self, *args, **kwargs)

    monkeypatch.setattr(dbapi.Cursor, "fetch_arrow_table", spy_table)
    monkeypatch.setattr(dbapi.Cursor, "fetch_record_batch", spy_reader)

    list(streaming_source.execute_query("SELECT id FROM stream_probe LIMIT 5000"))

    assert (
        calls["fetch_record_batch"] >= 1
    ), "execute_query must stream via fetch_record_batch (lazy reader)"
    assert (
        calls["fetch_arrow_table"] == 0
    ), "execute_query must NOT drain the result with fetch_arrow_table"


def test_first_batch_arrives_without_draining_everything(streaming_source):
    """The first batch must be available long before the full result is drained.

    A draining implementation pulls every row on the first ``next()``, so the
    first batch costs ~the whole scan. A streaming reader yields the first batch
    after one chunk, a tiny fraction of the total.
    """
    generator = streaming_source.execute_query("SELECT id FROM stream_probe")

    started = time.perf_counter()
    first = next(generator)
    first_batch_ms = (time.perf_counter() - started) * 1000
    rest = list(generator)
    total_ms = (time.perf_counter() - started) * 1000

    total_rows = first.num_rows + sum(batch.num_rows for batch in rest)
    assert total_rows == PROBE_ROWS  # correctness: every row still arrives

    assert first_batch_ms < total_ms * 0.5, (
        f"first batch took {first_batch_ms:.0f} ms of {total_ms:.0f} ms total — "
        "the whole result was drained before the first batch (not streaming)"
    )
