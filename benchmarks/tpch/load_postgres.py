"""Load the generated TPC-H tables into PostgreSQL from the DuckDB dataset.

The federated benchmark places some tables on PostgreSQL and some on DuckDB.
Rather than move data per placement, every one of the eight base tables is
loaded into BOTH sources once, byte-identical: the DuckDB file already holds
them, and DuckDB's own ``postgres`` extension copies each into PostgreSQL in a
single ``CREATE TABLE ... AS SELECT``. A placement then just decides which
source each table is READ from; the data is present in both.
"""

import argparse

import duckdb

from generate import _db_path, DEFAULT_DATA_DIR
from qualify import TPCH_TABLES


def _attach_dsn(options):
    """Build the libpq DSN DuckDB's postgres extension attaches through."""
    return (
        f"dbname={options.pg_database} user={options.pg_user} "
        f"password={options.pg_password} host={options.pg_host} "
        f"port={options.pg_port}"
    )


# Join-key columns indexed on every PostgreSQL table. A real federated
# deployment indexes its join keys; the DuckDB oracle reads the same indexed
# tables through its postgres scanner, so the comparison stays fair. Without an
# index Postgres evaluates a key semi-join as a full sequential scan, so the
# engine only pushes a dynamic filter into an INDEXED Postgres probe (it reads
# the table whole otherwise) - see fedqrs source_reduces_cheaply.
INDEX_COLUMNS = {
    "part": ["p_partkey"],
    "supplier": ["s_suppkey", "s_nationkey"],
    "partsupp": ["ps_partkey", "ps_suppkey"],
    "customer": ["c_custkey", "c_nationkey"],
    "orders": ["o_orderkey", "o_custkey"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey"],
    "nation": ["n_nationkey", "n_regionkey"],
    "region": ["r_regionkey"],
}


def _create_indexes(connection, schema):
    """Create a btree index on each join-key column (idempotent)."""
    for table, columns in sorted(INDEX_COLUMNS.items()):
        for column in columns:
            name = f"idx_{table}_{column}"
            connection.execute(
                f'CREATE INDEX IF NOT EXISTS {name} '
                f'ON pg.{schema}."{table}" ("{column}")'
            )
            print("indexed {0}.{1}".format(table, column))


def _load_table(connection, table, schema):
    """Copy one base table from the DuckDB file into PostgreSQL, replacing it."""
    target = f'pg.{schema}."{table}"'
    source = f'src.main."{table}"'
    connection.execute(f"DROP TABLE IF EXISTS {target}")
    # CREATE ... AS SELECT streams the DuckDB table straight into PostgreSQL
    # through the attached connection, so both sources hold identical rows.
    connection.execute(f"CREATE TABLE {target} AS SELECT * FROM {source}")
    rows = connection.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
    return rows


def _analyze(options):
    """Run ANALYZE so pg_class.reltuples and pg_stats are populated.

    The engine's cost-based optimizer reads its statistics from PostgreSQL's
    catalogs; a freshly CTAS'd table reports reltuples = -1 and has no pg_stats
    rows until ANALYZE runs, which would leave the optimizer on defaults.
    """
    import psycopg2

    connection = psycopg2.connect(
        host=options.pg_host, port=options.pg_port, dbname=options.pg_database,
        user=options.pg_user, password=options.pg_password,
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute("ANALYZE")
    connection.close()
    print("analyzed: pg_class.reltuples and pg_stats are populated")


def load(options):
    """Attach the DuckDB dataset (read-only) and PostgreSQL, load every table.

    A :memory: coordinator attaches the DuckDB file read-only and PostgreSQL
    read-write; a read-only DuckDB connection would force every attachment
    read-only and reject the CREATE TABLE into PostgreSQL.
    """
    db_path = _db_path(DEFAULT_DATA_DIR, options.scale_factor)
    connection = duckdb.connect(":memory:")
    connection.execute("INSTALL postgres")
    connection.execute("LOAD postgres")
    connection.execute(f"ATTACH '{db_path}' AS src (READ_ONLY)")
    connection.execute(f"ATTACH '{_attach_dsn(options)}' AS pg (TYPE postgres)")
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{options.pg_schema}")
    for table in sorted(TPCH_TABLES):
        rows = _load_table(connection, table, options.pg_schema)
        print("loaded {0:10} {1:>10} rows".format(table, rows))
    _create_indexes(connection, options.pg_schema)
    connection.close()
    _analyze(options)


def _parse_args():
    """Parse loader options (scale factor and PostgreSQL connection)."""
    parser = argparse.ArgumentParser(description="Load TPC-H into PostgreSQL.")
    parser.add_argument("--scale-factor", default="0.01")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-database", default="duckpoc")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")
    parser.add_argument("--pg-schema", default="public")
    return parser.parse_args()


def main():
    """Entry point: load the TPC-H tables into PostgreSQL."""
    load(_parse_args())


if __name__ == "__main__":
    main()
