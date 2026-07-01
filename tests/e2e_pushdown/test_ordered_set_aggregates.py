"""Execution tests for ordered-set aggregates (WITHIN GROUP).

PERCENTILE_CONT/PERCENTILE_DISC/MODE (and MEDIAN, which the preprocessor
re-renders to PERCENTILE_CONT(0.5) WITHIN GROUP) execute both as a single-source
pushdown and through the cross-source merge engine. Each query is compared
against the same query on the underlying DuckDB (ground truth).
"""

import duckdb
import pytest

from tests.e2e_pushdown.conftest import _seed_customers, _seed_orders
from tests.e2e_pushdown.helpers import build_runtime

SINGLE = "duckdb_primary.main.orders"


def _normalize(rows):
    """Return rows as a sorted list of stringified value tuples."""
    out = []
    for row in rows:
        out.append(tuple(str(value) for value in row))
    out.sort()
    return out


def _federated_rows(runtime, sql):
    """Execute sql through the engine; columns ordered by sorted key name."""
    table = runtime.execute(sql)
    records = []
    for row in table.to_pylist():
        records.append(tuple(row[key] for key in sorted(row.keys())))
    return _normalize(records)


def _ground_truth_rows(connection, sql):
    """Execute sql on a DuckDB connection; columns ordered by sorted name."""
    cursor = connection.execute(sql)
    names = []
    for descriptor in cursor.description:
        names.append(descriptor[0])
    order = sorted(range(len(names)), key=lambda index: names[index])
    records = []
    for row in cursor.fetchall():
        records.append(tuple(row[index] for index in order))
    return _normalize(records)


# (federated aggregate, DuckDB reference aggregate) over orders grouped by region.
ORDERED_SET_CASES = [
    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)",
    "PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY price)",
    "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price DESC)",
    "MODE() WITHIN GROUP (ORDER BY status)",
    "MEDIAN(price)",
]


@pytest.mark.parametrize("aggregate", ORDERED_SET_CASES)
def test_single_source_ordered_set_aggregate(single_source_env, aggregate):
    """Each ordered-set aggregate pushes to the source and matches DuckDB."""
    runtime = build_runtime(single_source_env)
    federated_sql = f"SELECT region, {aggregate} AS m FROM {SINGLE} GROUP BY region"
    reference_sql = f"SELECT region, {aggregate} AS m FROM orders GROUP BY region"

    federated = _federated_rows(runtime, federated_sql)
    expected = _ground_truth_rows(
        single_source_env.datasources[0].connection, reference_sql
    )

    assert federated == expected


def test_cross_source_ordered_set_aggregate(multi_source_env):
    """A cross-source ordered-set aggregate runs in the merge engine and matches."""
    runtime = build_runtime(multi_source_env)
    federated_sql = (
        "SELECT o.region, "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.price) AS m "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "GROUP BY o.region"
    )
    reference_sql = (
        "SELECT o.region, "
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.price) AS m "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "GROUP BY o.region"
    )
    reference = duckdb.connect(":memory:")
    _seed_orders(reference)
    _seed_customers(reference)

    federated = _federated_rows(runtime, federated_sql)
    expected = _ground_truth_rows(reference, reference_sql)

    assert federated  # non-empty
    assert federated == expected
