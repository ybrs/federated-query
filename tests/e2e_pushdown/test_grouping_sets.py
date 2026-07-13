"""Execution tests for GROUP BY ROLLUP / CUBE / GROUPING SETS and GROUPING().

ROLLUP/CUBE are expanded into explicit grouping sets at parse time and rendered
as GROUP BY GROUPING SETS (...), pushed to the source for a single-source query
and run in the DuckDB merge engine for a cross-source one. Results (which include
super-aggregate rows with NULL keys) are compared against the same query on the
underlying DuckDB. Numeric cells are compared as floats because the merge engine
returns SUM as float64.
"""

import duckdb
import pytest

from tests.e2e_pushdown.conftest import _seed_customers, _seed_orders
from tests.e2e_pushdown.helpers import build_runtime
from tests.rust_runtime import assert_raises_engine_error
from tests.duckdb_tmp import duckdb_path

SINGLE = "duckdb_primary.main.orders"


def _cell(value):
    """Stringify a cell, normalizing numbers so int vs float64 compare equal."""
    if value is None:
        return "NULL"
    try:
        return str(float(value))
    except (TypeError, ValueError):
        return str(value)


def _normalize(rows):
    """Return rows as a sorted list of normalized value tuples."""
    out = []
    for row in rows:
        out.append(tuple(_cell(value) for value in row))
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


GROUPING_CASES = [
    "SELECT region, SUM(price) AS s FROM {T} GROUP BY ROLLUP (region)",
    "SELECT region, status, SUM(price) AS s FROM {T} GROUP BY ROLLUP (region, status)",
    "SELECT region, status, SUM(price) AS s FROM {T} GROUP BY CUBE (region, status)",
    "SELECT region, status, SUM(price) AS s FROM {T} "
    "GROUP BY GROUPING SETS ((region), (status), ())",
    "SELECT region, SUM(price) AS s, GROUPING(region) AS g "
    "FROM {T} GROUP BY ROLLUP (region)",
    "SELECT region, status, SUM(price) AS s FROM {T} GROUP BY region, ROLLUP (status)",
    # Regression: ORDER BY / WHERE pushdown must not drop the grouping sets
    # (the optimizer's scan rebuilds must carry them, or super-aggregate rows are dropped).
    "SELECT region, SUM(price) AS s FROM {T} GROUP BY ROLLUP (region) ORDER BY region",
    "SELECT region, SUM(price) AS s FROM {T} WHERE price > 30 GROUP BY ROLLUP (region)",
]


@pytest.mark.parametrize("template", GROUPING_CASES)
def test_single_source_grouping_sets(single_source_env, template):
    """ROLLUP/CUBE/GROUPING SETS/GROUPING() push to the source and match DuckDB."""
    runtime = build_runtime(single_source_env)
    federated = _federated_rows(runtime, template.format(T=SINGLE))
    expected = _ground_truth_rows(
        single_source_env.datasources[0].connection, template.format(T="orders")
    )
    assert federated == expected


def test_grouping_sets_over_same_source_join(single_source_env):
    """ROLLUP over a same-source join pushes as one query without dropping sets.

    Regression: this path (SingleSourcePushdown -> PhysicalRemoteQuery) rendered a
    flat GROUP BY and lost the super-aggregate rows.
    """
    runtime = build_runtime(single_source_env)
    federated_sql = (
        "SELECT o.region, SUM(o.price) AS s "
        "FROM duckdb_primary.main.orders o "
        "JOIN duckdb_primary.main.customers c ON o.customer_id = c.customer_id "
        "GROUP BY ROLLUP (o.region)"
    )
    reference_sql = (
        "SELECT o.region, SUM(o.price) AS s "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "GROUP BY ROLLUP (o.region)"
    )
    federated = _federated_rows(runtime, federated_sql)
    expected = _ground_truth_rows(
        single_source_env.datasources[0].connection, reference_sql
    )
    assert federated == expected


def test_cross_source_grouping_sets(multi_source_env):
    """A cross-source ROLLUP runs in the merge engine and matches DuckDB."""
    runtime = build_runtime(multi_source_env)
    federated_sql = (
        "SELECT o.region, SUM(o.price) AS s, GROUPING(o.region) AS g "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "GROUP BY ROLLUP (o.region)"
    )
    reference_sql = (
        "SELECT o.region, SUM(o.price) AS s, GROUPING(o.region) AS g "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "GROUP BY ROLLUP (o.region)"
    )
    reference = duckdb.connect(duckdb_path())
    _seed_orders(reference)
    _seed_customers(reference)

    federated = _federated_rows(runtime, federated_sql)
    expected = _ground_truth_rows(reference, reference_sql)
    assert federated
    assert federated == expected


def test_combining_multiple_constructs_fails_fast(single_source_env):
    """Combining several ROLLUP/CUBE/GROUPING SETS is rejected, not mis-expanded."""
    runtime = build_runtime(single_source_env)
    with assert_raises_engine_error():
        runtime.execute(
            f"SELECT region FROM {SINGLE} GROUP BY ROLLUP (region), CUBE (status)"
        )
