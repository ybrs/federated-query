"""The ADBC-backed PostgreSQL driver is a drop-in for the psycopg2 path.

``driver: adbc`` fetches results through the Arrow-native ADBC driver, which
renders ``uuid`` as opaque binary and ``numeric`` as an opaque decimal string.
The connector normalizes uuid to a canonical string on both paths, so every
non-decimal column is byte-for-byte identical to psycopg2.

Decimals are the one documented exception. The psycopg2 path reads a declared
``numeric(p, s)`` with its precision and scale, so it surfaces the exact
``decimal128(p, s)`` that keeps cross-source aggregation exact. The ADBC opaque
numeric string carries no precision/scale (and the schema probe fetches no
rows), so it cannot recover an exact decimal type and falls back to float64.
The decimal VALUES are still equal; only the ADBC column's type is float64.
"""

import pytest

import pyarrow as pa
import pyarrow.compute as pc

from federated_query.datasources.postgresql import PostgreSQLDataSource


def _fetch(datasource, query) -> pa.Table:
    """Run query on datasource and collect its Arrow batches into one Table.

    Calls datasource.execute_query(query) (an iterator of pyarrow RecordBatches)
    and returns them assembled as a single pyarrow.Table for easy comparison.
    """
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

        # uuid normalized to a canonical string; NULL preserved
        assert adbc_table.schema.field("id").type == pa.string()
        assert adbc_table.column("id").to_pylist()[1] is None
        # Every non-decimal column is byte-for-byte identical across drivers.
        keep = ["id", "n", "label"]
        assert adbc_table.select(keep).equals(psycopg2_table.select(keep))
        # psycopg2 surfaces the declared numeric(10,2) as exact decimal128; the
        # ADBC opaque numeric has no recoverable precision/scale, so it stays
        # float64. The decimal values still match once cast to a common type.
        assert psycopg2_table.schema.field("amount").type == pa.decimal128(10, 2)
        assert adbc_table.schema.field("amount").type == pa.float64()
        adbc_amount = pc.cast(adbc_table.column("amount"), pa.decimal128(10, 2))
        assert adbc_amount.to_pylist() == psycopg2_table.column("amount").to_pylist()

        # get_query_schema must agree with the executed batch schema
        assert adbc.get_query_schema(query).equals(adbc_table.schema)
    finally:
        adbc.disconnect()
        with pg_datasource.get_connection() as conn:
            conn.cursor().execute("DROP TABLE pg.adbc_probe")
            conn.commit()
