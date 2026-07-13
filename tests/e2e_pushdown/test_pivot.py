"""Execution tests for PIVOT.

A single-aggregate static PIVOT is expanded into portable conditional
aggregation (``agg(CASE WHEN k = value THEN x END)``), so it pushes to any source
and produces the same result as a native PIVOT. Shapes the rewrite cannot handle
(UNPIVOT, multiple aggregates, COUNT(*), dynamic IN, non-star) fail fast.
"""

import duckdb
import pytest

from tests.e2e_pushdown.helpers import build_runtime
from tests.rust_runtime import assert_raises_engine_error
from tests.duckdb_tmp import duckdb_path

TABLE = "duckdb_primary.main.orders"


def _normalize(rows):
    out = []
    for row in rows:
        out.append(tuple("NULL" if v is None else str(v) for v in row))
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
    from tests.e2e_pushdown.conftest import _seed_orders

    connection = duckdb.connect(duckdb_path())
    _seed_orders(connection)
    return connection


# Each is compared against DuckDB's own native PIVOT over the same data.
PIVOT_CASES = [
    "SELECT * FROM {T} PIVOT (SUM(price) FOR region IN ('NA', 'EU', 'APAC'))",
    "SELECT * FROM {T} PIVOT (SUM(price) AS total FOR status IN ('processing', 'shipped'))",
    "SELECT * FROM {T} PIVOT (MAX(quantity) FOR region IN ('NA', 'EU'))",
]


@pytest.mark.parametrize("template", PIVOT_CASES)
def test_pivot_matches_native(single_source_env, template):
    """A supported PIVOT matches DuckDB's native PIVOT over the same data."""
    runtime = build_runtime(single_source_env)
    federated = _federated_rows(runtime, template.format(T=TABLE))
    expected = _ground_truth_rows(_reference(), template.format(T="orders"))
    assert federated == expected


UNSUPPORTED = [
    "SELECT * FROM {T} UNPIVOT (val FOR col IN (price, quantity))",
    "SELECT * FROM {T} PIVOT (SUM(price), COUNT(*) FOR region IN ('NA', 'EU'))",
    "SELECT * FROM {T} PIVOT (COUNT(*) FOR region IN ('NA', 'EU'))",
]


@pytest.mark.parametrize("template", UNSUPPORTED)
def test_unsupported_pivot_fails_fast(single_source_env, template):
    """Unsupported pivot shapes raise rather than silently drop the pivot."""
    runtime = build_runtime(single_source_env)
    with assert_raises_engine_error():
        runtime.execute(template.format(T=TABLE))
