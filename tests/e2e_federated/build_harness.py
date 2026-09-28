"""Build every datasource the e2e suite reads. Run by build_test_harness.sh.

This is the ONLY code in the repository that creates test data. The tests read
what this builds and never seed anything themselves, so what a test runs against
is decided here, once, visibly, and is the same for every test that reads it.

It builds a fixed set of datasources - several instances of each kind, each
holding every table the corpus can name - plus the single config that declares
them. Placements are then expressed in the queries, so adding one costs nothing
here.

The build is destructive and complete: it removes ``.test-harness`` and drops
every ``fed_%`` schema first. A failure raises naming the source that failed, and
the verifier run before the tests refuses a suite whose data is incomplete.
"""

import os
import shutil
import sys
import time

import duckdb
import psycopg2

from tests.e2e_federated import harness
from tests.e2e_federated import placements as placement_mod


def connect_pg():
    """Open the autocommit PostgreSQL connection the build seeds through."""
    connection = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=harness.pg_port(),
        dbname=harness.pg_database(),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    connection.autocommit = True
    return connection


def clean(pg_connection):
    """Remove every source an earlier build left, on disk and in PostgreSQL."""
    if os.path.exists(harness.HARNESS_ROOT):
        shutil.rmtree(harness.HARNESS_ROOT)
    os.makedirs(os.path.join(harness.HARNESS_ROOT, "duck"))
    os.makedirs(os.path.join(harness.HARNESS_ROOT, "parquet"))
    cursor = pg_connection.cursor()
    cursor.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'fed_%'")
    rows = cursor.fetchall()
    for row in rows:
        cursor.execute("DROP SCHEMA IF EXISTS " + row[0] + " CASCADE")
    cursor.close()
    return len(rows)


def build_source(source, pg_connection):
    """Create one datasource with every table, dispatching on its kind."""
    if source.kind == placement_mod.DUCK:
        _build_duck(source)
    elif source.kind == placement_mod.PARQUET:
        _build_parquet(source)
    else:
        _build_pg(source, pg_connection)


def _build_duck(source):
    """Create the DuckDB file and seed its tables."""
    writer = duckdb.connect(source.location)
    _run_specs(writer, source.specs)
    writer.close()


def _build_parquet(source):
    """Seed an in-memory DuckDB and export one Parquet file per table."""
    os.makedirs(source.location)
    writer = duckdb.connect()
    _run_specs(writer, source.specs)
    for spec in source.specs:
        target = os.path.join(source.location, spec.name + ".parquet")
        writer.execute("COPY " + spec.name + " TO '" + target + "' (FORMAT PARQUET)")
    writer.close()


def _build_pg(source, pg_connection):
    """Create the PostgreSQL schema and seed its tables."""
    cursor = pg_connection.cursor()
    cursor.execute("CREATE SCHEMA " + source.schema)
    cursor.execute("SET search_path TO " + source.schema)
    _run_specs(cursor, source.specs)
    cursor.close()


def _run_specs(executor, specs):
    """Run each spec's DDL and inserts on a DuckDB or psycopg2 cursor."""
    for spec in specs:
        executor.execute(spec.ddl)
        for insert in spec.inserts:
            executor.execute(insert)


def write_config():
    """Write the single engine config declaring every datasource."""
    handle = open(harness.CONFIG_PATH, "w")
    handle.write(harness.config_text())
    handle.close()


def main():
    """Build every source in the manifest, reporting what was created."""
    sources = harness.manifest()
    tables = len(harness.ordered_specs())
    sys.stdout.write(
        "building %d datasources x %d tables into %s\n"
        % (len(sources), tables, harness.HARNESS_ROOT)
    )
    pg_connection = connect_pg()
    dropped = clean(pg_connection)
    sys.stdout.write("  cleaned %d leftover PostgreSQL schemas\n" % dropped)
    started = time.time()
    for name in sorted(sources):
        build_source(sources[name], pg_connection)
        sys.stdout.write("  built " + name + "\n")
    pg_connection.close()
    write_config()
    sys.stdout.write(
        "built %d datasources and 1 config in %.1fs\n"
        % (len(sources), time.time() - started)
    )


if __name__ == "__main__":
    main()
