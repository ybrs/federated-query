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
    connection.close()


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
