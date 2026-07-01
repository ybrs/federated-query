"""Execution tests for aggregate function rendering.

These execute the query through the engine and compare the result against the
same query run directly on the underlying DuckDB (ground truth). They guard the
fix for dialect-blind function rendering: aggregate names used to be derived
from the sqlglot class name (VariancePop -> VARIANCEPOP), which no source
accepts. EXPLAIN-only tests missed this because the bug is only visible when the
remote query actually executes.
"""

import pytest

from tests.e2e_pushdown.helpers import build_runtime

TABLE = "duckdb_primary.main.orders"


def _normalize(rows):
    """Return rows as a sorted list of stringified value tuples."""
    out = []
    for row in rows:
        out.append(tuple(str(value) for value in row))
    out.sort()
    return out


def _federated_rows(runtime, sql):
    """Execute sql through the engine and normalize the result rows."""
    table = runtime.execute(sql)
    records = []
    for row in table.to_pylist():
        records.append(tuple(row[key] for key in sorted(row.keys())))
    return _normalize(records)


def _ground_truth_rows(env, sql):
    """Execute sql directly on the underlying DuckDB and normalize the rows.

    Columns are reordered by name so the tuples line up with the engine result,
    which orders its columns by sorted key name.
    """
    cursor = env.datasources[0].connection.execute(sql)
    names = []
    for descriptor in cursor.description:
        names.append(descriptor[0])
    order = sorted(range(len(names)), key=lambda index: names[index])
    records = []
    for row in cursor.fetchall():
        records.append(tuple(row[index] for index in order))
    return _normalize(records)


# Each case: an aggregate expression that previously mis-rendered. The federated
# query qualifies the table; the reference query uses the bare table name.
RENDERING_CASES = [
    "VAR_POP(price)",
    "VAR_SAMP(price)",
    "VARIANCE(price)",
    "STDDEV_POP(price)",
    "STDDEV_SAMP(price)",
    "STDDEV(price)",
    "STRING_AGG(status, ',')",
    "ARRAY_AGG(status)",
    "BOOL_AND(price > 50)",
    "BOOL_OR(price > 50)",
    "SUM(price)",
    "AVG(price)",
    "COUNT(DISTINCT status)",
]


@pytest.mark.parametrize("aggregate", RENDERING_CASES)
def test_aggregate_renders_and_matches_ground_truth(single_source_env, aggregate):
    """Each aggregate executes and matches the same query on raw DuckDB."""
    runtime = build_runtime(single_source_env)
    federated_sql = f"SELECT region, {aggregate} AS agg FROM {TABLE} GROUP BY region"
    reference_sql = f"SELECT region, {aggregate} AS agg FROM orders GROUP BY region"

    federated = _federated_rows(runtime, federated_sql)
    expected = _ground_truth_rows(single_source_env, reference_sql)

    assert federated == expected
