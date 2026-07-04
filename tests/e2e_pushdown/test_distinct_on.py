"""Execution tests for DISTINCT ON.

A single-source DISTINCT ON pushes to the source, which keeps one row per ON-key
combination chosen by ORDER BY. Results are compared, IN ORDER, against the same
query on the underlying DuckDB (DISTINCT ON output order is meaningful). A
cross-source DISTINCT ON cannot be evaluated locally without the source's
ORDER BY semantics, so it fails fast.
"""

import pytest

from federated_query.parser.errors import UnsupportedSQLError

from tests.e2e_pushdown.helpers import build_runtime

SINGLE = "duckdb_primary.main.orders"


def _federated_rows(runtime, sql):
    """Execute sql through the engine; preserve row order, columns by key name."""
    table = runtime.execute(sql)
    rows = []
    for row in table.to_pylist():
        rows.append(tuple(str(row[key]) for key in sorted(row.keys())))
    return rows


def _ground_truth_rows(env, sql):
    """Execute sql on the underlying DuckDB; preserve row order, columns by name."""
    cursor = env.datasources[0].connection.execute(sql)
    names = []
    for descriptor in cursor.description:
        names.append(descriptor[0])
    order = sorted(range(len(names)), key=lambda index: names[index])
    rows = []
    for row in cursor.fetchall():
        rows.append(tuple(str(row[index]) for index in order))
    return rows


# Each query has an ORDER BY that starts with the ON keys, so the surviving row
# is deterministic and comparable against DuckDB.
DISTINCT_ON_CASES = [
    "SELECT DISTINCT ON (region) region, order_id FROM {T} ORDER BY region, price",
    "SELECT DISTINCT ON (region) region, price FROM {T} ORDER BY region, price DESC",
    "SELECT DISTINCT ON (region, status) region, status, order_id "
    "FROM {T} ORDER BY region, status, order_id",
    "SELECT DISTINCT ON (region) region, order_id FROM {T} "
    "WHERE price > 30 ORDER BY region, price",
]


@pytest.mark.parametrize("template", DISTINCT_ON_CASES)
def test_single_source_distinct_on(single_source_env, template):
    """DISTINCT ON pushes to the source and matches DuckDB, row order included."""
    runtime = build_runtime(single_source_env)
    federated = _federated_rows(runtime, template.format(T=SINGLE))
    expected = _ground_truth_rows(single_source_env, template.format(T="orders"))
    assert federated == expected


def test_plain_distinct_unaffected(single_source_env):
    """A plain DISTINCT (no ON) keeps working alongside DISTINCT ON support."""
    runtime = build_runtime(single_source_env)
    table = runtime.execute(f"SELECT DISTINCT region FROM {SINGLE}")
    assert table.num_rows == 3


def test_cross_source_distinct_on_fails_fast(multi_source_env, duckdb_engine):
    """A cross-source DISTINCT ON fails fast instead of returning a wrong answer."""
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT DISTINCT ON (o.region) o.region, o.order_id, c.segment "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "ORDER BY o.region, o.order_id"
    )
    with pytest.raises(UnsupportedSQLError):
        runtime.execute(sql)
