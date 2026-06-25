"""Execution tests for named windows (WINDOW w AS (...)).

The preprocessor inlines each named window into its OVER w references, so a
named window behaves exactly like the inline form. Results are compared against
the same query on the underlying DuckDB (ground truth).
"""

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
    """Execute sql through the engine; columns ordered by sorted key name."""
    table = runtime.execute(sql)
    records = []
    for row in table.to_pylist():
        records.append(tuple(row[key] for key in sorted(row.keys())))
    return _normalize(records)


def _ground_truth_rows(env, sql):
    """Execute sql on the underlying DuckDB; columns ordered by sorted name."""
    cursor = env.datasources[0].connection.execute(sql)
    names = []
    for descriptor in cursor.description:
        names.append(descriptor[0])
    order = sorted(range(len(names)), key=lambda index: names[index])
    records = []
    for row in cursor.fetchall():
        records.append(tuple(row[index] for index in order))
    return _normalize(records)


# (federated SQL template, reference SQL template) keyed on {T}.
NAMED_WINDOW_CASES = [
    "SELECT order_id, SUM(price) OVER w AS s FROM {T} WINDOW w AS (PARTITION BY region)",
    "SELECT order_id, region, "
    "ROW_NUMBER() OVER w AS rn FROM {T} "
    "WINDOW w AS (PARTITION BY region ORDER BY price)",
    "SELECT order_id, region, "
    "RANK() OVER (w ORDER BY price DESC) AS r FROM {T} "
    "WINDOW w AS (PARTITION BY region)",
    "SELECT order_id, SUM(price) OVER w AS s, AVG(price) OVER w AS a "
    "FROM {T} WINDOW w AS (PARTITION BY region)",
]


def test_named_windows_match_ground_truth(single_source_env):
    """Each named-window query matches the same query on raw DuckDB."""
    runtime = build_runtime(single_source_env)
    for template in NAMED_WINDOW_CASES:
        federated = _federated_rows(runtime, template.format(T=TABLE))
        expected = _ground_truth_rows(single_source_env, template.format(T="orders"))
        assert federated == expected, template
