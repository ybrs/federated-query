"""Verify every datasource the suite needs exists and holds the right rows.

Run before the tests. It creates nothing: it answers one question - is the data
the tests are about to read actually there - and when the answer is no it names
every missing or wrong source and exits non-zero.

The check is per table, not per source, because a source whose file exists but
whose tables were half written would otherwise pass and let a test read short
data and compare it against a full oracle. Row counts are recomputed from the
same specs the builder ran, so there is no written-down number to drift.
"""

import os
import sys

import duckdb

from tests.e2e_federated import build_harness
from tests.e2e_federated import harness
from tests.e2e_federated import placements as placement_mod


def check_source(source, pg_connection):
    """Return the list of problems with one source, empty when it is correct."""
    expected = harness.expected_row_counts(source.specs)
    if source.kind == placement_mod.DUCK:
        return _check_duck(source, expected)
    if source.kind == placement_mod.PARQUET:
        return _check_parquet(source, expected)
    return _check_pg(source, expected, pg_connection)


def _check_duck(source, expected):
    """Check the DuckDB file exists and every table has the expected rows."""
    if not os.path.exists(source.location):
        return [source.name + ": missing DuckDB file " + source.location]
    connection = duckdb.connect(source.location, read_only=True)
    problems = _compare_counts(source, expected, connection.execute)
    connection.close()
    return problems


def _check_parquet(source, expected):
    """Check every table's Parquet file exists and has the expected rows."""
    if not os.path.isdir(source.location):
        return [source.name + ": missing Parquet directory " + source.location]
    problems = []
    connection = duckdb.connect()
    for table in source.table_names():
        path = os.path.join(source.location, table + ".parquet")
        if not os.path.exists(path):
            problems.append(source.name + ": missing Parquet file " + path)
            continue
        rows = connection.execute(
            "SELECT count(*) FROM read_parquet('" + path + "')"
        ).fetchone()
        _note_count(problems, source, table, rows[0], expected[table])
    connection.close()
    return problems


def _check_pg(source, expected, pg_connection):
    """Check the PostgreSQL schema exists and every table has the expected rows."""
    cursor = pg_connection.cursor()
    cursor.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (source.schema,),
    )
    if cursor.fetchone() is None:
        cursor.close()
        return [source.name + ": missing PostgreSQL schema " + source.schema]
    problems = _compare_pg_counts(source, expected, cursor)
    cursor.close()
    return problems


def _compare_pg_counts(source, expected, cursor):
    """Compare each table's row count in a PostgreSQL schema to what is expected."""
    problems = []
    for table in source.table_names():
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (source.schema, table),
        )
        if cursor.fetchone() is None:
            problems.append(
                source.name + ": missing table " + source.schema + "." + table
            )
            continue
        cursor.execute("SELECT count(*) FROM " + source.schema + "." + table)
        _note_count(problems, source, table, cursor.fetchone()[0], expected[table])
    return problems


def _compare_counts(source, expected, execute):
    """Compare each table's row count through a DuckDB-style execute callable."""
    problems = []
    for table in source.table_names():
        found = execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = '" + table + "'"
        ).fetchone()
        if not found[0]:
            problems.append(source.name + ": missing table " + table)
            continue
        rows = execute("SELECT count(*) FROM " + table).fetchone()
        _note_count(problems, source, table, rows[0], expected[table])
    return problems


def _note_count(problems, source, table, actual, wanted):
    """Record a problem when a table's row count is not what the specs define."""
    if actual != wanted:
        problems.append(
            source.name + "." + table + ": has " + str(actual)
            + " rows, specs define " + str(wanted)
        )


def verify_all():
    """Return every problem across the whole manifest, empty when all is well."""
    sources = harness.manifest()
    pg_connection = build_harness.connect_pg()
    problems = []
    for name in sorted(sources):
        problems.extend(check_source(sources[name], pg_connection))
    pg_connection.close()
    return len(sources), problems


def main():
    """Verify the harness, printing every problem and exiting non-zero on any."""
    total, problems = verify_all()
    if not problems:
        sys.stdout.write("harness OK: %d datasources present and correct\n" % total)
        return
    sys.stderr.write(
        "HARNESS INCOMPLETE: %d problem(s) across %d datasources\n"
        % (len(problems), total)
    )
    for problem in problems:
        sys.stderr.write("  " + problem + "\n")
    sys.stderr.write("  fix: ./build_test_harness.sh\n")
    sys.exit(1)


if __name__ == "__main__":
    main()
