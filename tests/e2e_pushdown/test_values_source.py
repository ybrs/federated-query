"""Execution tests for VALUES used as a table source.

``(VALUES (1, 2), (3, 4)) AS v(a, b)`` becomes a constant Values relation wrapped
in a derived table, so it works standalone, with WHERE, and joined to a real
table. Results are compared against the same query on the underlying DuckDB.

Note: ``SELECT *`` over a VALUES (or any derived) relation is a separate,
pre-existing limitation of the star expander (it only expands base tables) and
fails fast; these tests use explicit columns.
"""

import duckdb
import pytest

from federated_query.processor.query_preprocessor import StarExpansionError
from tests.e2e_pushdown.helpers import build_runtime
from tests.duckdb_tmp import duckdb_path

TABLE = "duckdb_primary.main.orders"


def _normalize(rows):
    out = []
    for row in rows:
        out.append(tuple(str(value) for value in row))
    out.sort()
    return out


def _federated_rows(runtime, sql):
    table = runtime.execute(sql)
    records = []
    for row in table.to_pylist():
        records.append(tuple(row[key] for key in sorted(row.keys())))
    return _normalize(records)


def _ground_truth_rows(connection, sql):
    cursor = connection.execute(sql)
    names = []
    for descriptor in cursor.description:
        names.append(descriptor[0])
    order = sorted(range(len(names)), key=lambda index: names[index])
    records = []
    for row in cursor.fetchall():
        records.append(tuple(row[index] for index in order))
    return _normalize(records)


def _reference():
    """A DuckDB holding the same orders data as the single-source fixture."""
    from tests.e2e_pushdown.conftest import _seed_orders

    connection = duckdb.connect(duckdb_path())
    _seed_orders(connection)
    return connection


VALUES_CASES = [
    (
        "SELECT a, b FROM (VALUES (1, 2), (3, 4)) AS v(a, b)",
        "SELECT a, b FROM (VALUES (1, 2), (3, 4)) AS v(a, b)",
    ),
    (
        "SELECT a, b FROM (VALUES (1, 'x'), (2, 'y'), (3, 'z')) AS v(a, b) WHERE a > 1",
        "SELECT a, b FROM (VALUES (1, 'x'), (2, 'y'), (3, 'z')) AS v(a, b) WHERE a > 1",
    ),
]


@pytest.mark.parametrize("federated_sql, reference_sql", VALUES_CASES)
def test_values_source_standalone(single_source_env, federated_sql, reference_sql):
    """A standalone VALUES relation matches DuckDB."""
    runtime = build_runtime(single_source_env)
    reference = duckdb.connect(duckdb_path())
    assert _federated_rows(runtime, federated_sql) == _ground_truth_rows(
        reference, reference_sql
    )


def test_values_joined_to_real_table(single_source_env):
    """A VALUES relation joins to a real source table."""
    runtime = build_runtime(single_source_env)
    federated_sql = (
        "SELECT o.order_id, labels.label "
        f"FROM {TABLE} o "
        "JOIN (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS labels(cid, label) "
        "ON o.customer_id = labels.cid"
    )
    reference_sql = (
        "SELECT o.order_id, labels.label "
        "FROM orders o "
        "JOIN (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS labels(cid, label) "
        "ON o.customer_id = labels.cid"
    )
    assert _federated_rows(runtime, federated_sql) == _ground_truth_rows(
        _reference(), reference_sql
    )


def test_star_over_values_fails_fast(single_source_env):
    """SELECT * over a derived VALUES relation is the shared derived-table limit."""
    runtime = build_runtime(single_source_env)
    with pytest.raises(StarExpansionError):
        runtime.execute("SELECT * FROM (VALUES (1, 2), (3, 4)) AS v(a, b)")
