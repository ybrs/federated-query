"""The ADBC-backed PostgreSQL driver must be a drop-in for the psycopg2 path.

``driver: adbc`` fetches results through the Arrow-native ADBC driver, which
renders ``uuid`` as opaque binary and ``numeric`` as decimal. The connector
normalizes those to the engine's representation (uuid -> canonical string,
numeric -> float64), so results must be byte-for-byte identical to psycopg2.
"""

import pytest

import pyarrow as pa

from federated_query.datasources.postgresql import PostgreSQLDataSource


def _fetch(datasource, query) -> pa.Table:
    return pa.Table.from_batches(list(datasource.execute_query(query)))


def test_adbc_driver_matches_psycopg2(pg_datasource, setup_test_data):
    pytest.importorskip("adbc_driver_postgresql")
    with pg_datasource.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE pg.adbc_probe (id uuid, n int, amount numeric(10,2), label text)"
        )
        cursor.execute(
            "INSERT INTO pg.adbc_probe VALUES "
            "(gen_random_uuid(), 1, 10.50, 'a'), "
            "(NULL, 2, NULL, NULL), "
            "(gen_random_uuid(), 3, 99.99, 'c')"
        )
        conn.commit()

    adbc = PostgreSQLDataSource("adbc", {**pg_datasource.config, "driver": "adbc"})
    adbc.connect()
    try:
        query = "SELECT id, n, amount, label FROM pg.adbc_probe ORDER BY n"
        psycopg2_table = _fetch(pg_datasource, query)
        adbc_table = _fetch(adbc, query)

        assert adbc_table.schema.equals(psycopg2_table.schema)
        assert adbc_table.equals(psycopg2_table)
        # uuid normalized to a canonical string; NULL preserved
        assert adbc_table.schema.field("id").type == pa.string()
        assert adbc_table.column("id").to_pylist()[1] is None
        # numeric normalized to float64 (matching the psycopg2 path)
        assert adbc_table.schema.field("amount").type == pa.float64()

        # get_query_schema must agree with the executed batch schema
        assert adbc.get_query_schema(query).equals(adbc_table.schema)
    finally:
        adbc.disconnect()
        with pg_datasource.get_connection() as conn:
            conn.cursor().execute("DROP TABLE pg.adbc_probe")
            conn.commit()
