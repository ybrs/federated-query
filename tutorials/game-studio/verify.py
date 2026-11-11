"""Check every tutorial query through FedQ server against the original rows."""

import argparse
from decimal import Decimal
import json
import math
from numbers import Number

import duckdb
import psycopg2
import pyarrow.parquet as pq
import sqlglot

from schema import OUTPUT, ROOT, ROOT_TABLES, fixture_path


def execute(connection, sql):
    """Execute one server statement and preserve its result column names."""
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = []
        for column in cursor.description:
            columns.append(column.name)
        rows = []
        for values in cursor.fetchall():
            rows.append(dict(zip(columns, values, strict=True)))
        return rows


def compare_value(actual, expected):
    """Compare integer amounts exactly and allow floating expression rounding."""
    if isinstance(expected, int):
        return Decimal(str(actual)) == expected
    if isinstance(expected, Number):
        return math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-9)
    return actual == expected


def compare_rows(actual, expected):
    """Raise with both answers whenever row count, columns, or values differ."""
    if len(actual) != len(expected):
        raise ValueError(f"Row counts differ: {actual!r} != {expected!r}")
    for actual_row, expected_row in zip(actual, expected, strict=True):
        if actual_row.keys() != expected_row.keys():
            raise ValueError(f"Columns differ: {actual_row!r} != {expected_row!r}")
        compare_fields(actual_row, expected_row)


def compare_fields(actual, expected):
    """Check every named field with a precise mismatch diagnostic."""
    for name, value in actual.items():
        if not compare_value(value, expected[name]):
            raise ValueError(f"{name}: {value!r} != {expected[name]!r}")


def sortable_value(value):
    """Sort equivalent numeric types together while retaining NULL and text values."""
    if value is None:
        return (0, "")
    if isinstance(value, bool):
        return (1, Decimal(int(value)))
    if isinstance(value, Number):
        return (1, Decimal(str(value)))
    return (2, str(value))


def sortable_row(row):
    """Construct a deterministic row key without dropping duplicate records."""
    values = []
    for name in sorted(row):
        values.append(sortable_value(row[name]))
    return tuple(values)


def compare_results(actual, expected, sql):
    """Preserve declared outer ordering and compare unordered results as sorted bags."""
    if sqlglot.parse_one(sql, read="postgres").args.get("order") is None:
        actual = sorted(actual, key=sortable_row)
        expected = sorted(expected, key=sortable_row)
    compare_rows(actual, expected)


def register_fixtures(oracle, connection):
    """Check real source row counts and register the original fixture rows in DuckDB."""
    for name, relation in ROOT_TABLES.items():
        table = pq.read_table(fixture_path(name))
        actual = execute(connection, f"SELECT COUNT(*) AS rows FROM {relation}")
        compare_rows(actual, [{"rows": table.num_rows}])
        oracle.register(name, table)
        print(f"Source verified: {relation}: {table.num_rows} rows", flush=True)


def oracle_rows(oracle, sql):
    """Execute the same SQL against local fixtures and name each result field."""
    for name, relation in ROOT_TABLES.items():
        sql = sql.replace(relation, name)
    result = oracle.execute(sql)
    columns = []
    for column in result.description:
        columns.append(column[0])
    rows = []
    for values in result.fetchall():
        rows.append(dict(zip(columns, values, strict=True)))
    return rows


def reject_invalid_query(connection):
    """Require the server to reject an unknown column rather than answer it."""
    sql = "SELECT p.nonexistent_column FROM billing.public.players AS p"
    try:
        execute(connection, sql)
    except psycopg2.Error as error:
        if "nonexistent_column" not in str(error):
            raise
        print("Invalid column rejected", flush=True)
        return
    raise ValueError("The server answered an invalid column reference")


def verify_query(oracle, connection, path):
    """Compare a scenario and retain its plan and source fragment descriptions."""
    sql = path.read_text(encoding="ascii")
    plan = execute(connection, "EXPLAIN " + sql)
    actual = execute(connection, sql)
    compare_results(actual, oracle_rows(oracle, sql), sql)
    fragments = execute(connection, "SHOW PUSHDOWN FRAGMENTS")
    if not fragments:
        raise ValueError(f"No source fragments recorded for {path.name}")
    print(f"Verified: {path.name}: {len(actual)} result rows", flush=True)
    return actual, {"plan": plan, "fragments": fragments}


def encode_decimal(value):
    """Preserve exact wire-protocol decimals in JSON; reject unknown types."""
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def save_json(name, value):
    """Save a verified result or inspection document beside the generated config."""
    rendered = json.dumps(value, indent=2, default=encode_decimal) + "\n"
    (OUTPUT / name).write_text(rendered, encoding="ascii")


def verify_queries(oracle, connection):
    """Verify every shipped scenario and save answers, plans, and fragments."""
    queries = sorted((ROOT / "queries").glob("*.sql"))
    if not queries:
        raise FileNotFoundError("No tutorial SQL files found")
    results = {}
    inspections = {}
    execute(connection, "SET diagnostics.record_pushdown_fragments = true")
    for path in queries:
        result, inspection = verify_query(oracle, connection, path)
        results[path.stem] = result
        inspections[path.stem] = inspection
    save_json("results.json", results)
    save_json("inspections.json", inspections)


def main():
    """Verify all sources, scenarios, and invalid-column rejection via the server."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn", default="host=127.0.0.1 port=56432 " "dbname=main user=analyst"
    )
    connection = psycopg2.connect(parser.parse_args().dsn)
    connection.autocommit = True
    try:
        with duckdb.connect() as oracle:
            register_fixtures(oracle, connection)
            verify_queries(oracle, connection)
        reject_invalid_query(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
