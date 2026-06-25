"""ClickHouse connector — Arrow-native fetch and metadata.

Skipped unless ``clickhouse-connect`` is installed and a ClickHouse server is
reachable (host/port from ``CLICKHOUSE_HOST`` / ``CLICKHOUSE_PORT``, default
127.0.0.1:8123). Builds a small temp table, then exercises the connector's
execute_query (streamed Arrow), schema probe, and catalog metadata.
"""

import os

import pyarrow as pa
import pytest

from federated_query.datasources.clickhouse import ClickHouseDataSource

HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))


@pytest.fixture(scope="module")
def ch_source():
    """A connected ClickHouse source over a small temp table, dropped on teardown."""
    pytest.importorskip("clickhouse_connect")
    import clickhouse_connect

    try:
        admin = clickhouse_connect.get_client(host=HOST, port=PORT, username="default")
        admin.query("SELECT 1")
    except Exception:
        pytest.skip("no ClickHouse server reachable")
    admin.command("DROP TABLE IF EXISTS conn_probe")
    admin.command(
        "CREATE TABLE conn_probe (id Int64, name String, d Date) ENGINE = MergeTree "
        "ORDER BY id"
    )
    admin.command(
        "INSERT INTO conn_probe VALUES (1,'a','2026-01-01'),(2,'b','2026-01-02'),"
        "(3,'c','2026-01-03')"
    )
    source = ClickHouseDataSource(
        "ch", {"host": HOST, "port": PORT, "user": "default", "database": "default"}
    )
    source.connect()
    yield source
    source.disconnect()
    admin.command("DROP TABLE IF EXISTS conn_probe")


def test_execute_query_yields_arrow_batches(ch_source):
    """execute_query streams Arrow record batches with the real values."""
    batches = list(
        ch_source.execute_query("SELECT id, name FROM conn_probe ORDER BY id")
    )
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("name").to_pylist() == ["a", "b", "c"]


def test_get_query_schema_has_real_types(ch_source):
    """The zero-row probe reports real Arrow types, not all-string."""
    schema = ch_source.get_query_schema("SELECT id, name, d FROM conn_probe")
    assert schema.field("id").type == pa.int64()
    assert pa.types.is_string(schema.field("name").type)
    assert pa.types.is_date(schema.field("d").type)


def test_table_metadata_maps_clickhouse_types(ch_source):
    """system.columns types map to the engine's SQL type names."""
    assert "conn_probe" in ch_source.list_tables("default")
    meta = ch_source.get_table_metadata("default", "conn_probe")
    types = {column.name: column.data_type for column in meta.columns}
    assert types == {"id": "BIGINT", "name": "VARCHAR", "d": "DATE"}
