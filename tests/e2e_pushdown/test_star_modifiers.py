"""Execution tests for SELECT * EXCLUDE / EXCEPT / REPLACE.

The star expander honors these modifiers: EXCLUDE/EXCEPT drops the listed
columns, REPLACE substitutes a column's output with an expression in place.
Each query is compared against the same query run directly on the underlying
DuckDB (ground truth).
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


# (federated SQL, DuckDB reference SQL). DuckDB spells the modifier EXCLUDE.
STAR_CASES = [
    (
        f"SELECT * EXCLUDE (region) FROM {TABLE}",
        "SELECT * EXCLUDE (region) FROM orders",
    ),
    (
        f"SELECT * EXCEPT (region, status) FROM {TABLE}",
        "SELECT * EXCLUDE (region, status) FROM orders",
    ),
    (
        f"SELECT * REPLACE (price + 1 AS price) FROM {TABLE}",
        "SELECT * REPLACE (price + 1 AS price) FROM orders",
    ),
    (
        f"SELECT * EXCLUDE (status) REPLACE (price * 2 AS price) FROM {TABLE}",
        "SELECT * EXCLUDE (status) REPLACE (price * 2 AS price) FROM orders",
    ),
]


def test_star_modifiers_match_ground_truth(single_source_env):
    """SELECT * EXCLUDE/EXCEPT/REPLACE matches the same query on raw DuckDB."""
    runtime = build_runtime(single_source_env)
    for federated_sql, reference_sql in STAR_CASES:
        federated = _federated_rows(runtime, federated_sql)
        expected = _ground_truth_rows(single_source_env, reference_sql)
        assert federated == expected, federated_sql
