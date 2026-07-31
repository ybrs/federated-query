"""Differential tests for cost-based join reordering on the Rust engine.

Every query runs twice over the same DuckDB sources - once with join reordering
enabled (the default) and once with ``enable_join_reordering: false`` - and both
result sets must equal the single-connection DuckDB oracle. Reordering decides
which shape the engine executes, never which rows come back, so a reordering
rule that is wrong only when enabled shows up here as a mismatch between the two
runs rather than as a silently different answer.

The placement corpus in tests/e2e_federated proves the DEFAULT configuration
against the same oracle, but it never toggles the rule, so it cannot separate
"the answer is right" from "the answer is right because reordering happened to
be a no-op". That separation is what these tests add.
"""

import duckdb
import pytest

from tests.duckdb_tmp import duckdb_path
from tests.rust_runtime import RustRuntime

REORDER_OFF = {"enable_join_reordering": False}

# Row counts differ by an order of magnitude per table so the cost model has a
# genuinely preferable order to find; with equal-sized tables a reordering rule
# that did nothing would still pass.
_SEED_SQL = (
    "CREATE TABLE dim_small (k INTEGER, label VARCHAR);"
    "INSERT INTO dim_small SELECT g, 'l' || g FROM range(0, 8) t(g);"
    "CREATE TABLE dim_mid (k INTEGER, grp INTEGER);"
    "INSERT INTO dim_mid SELECT g, g % 5 FROM range(0, 200) t(g);"
    "CREATE TABLE fact_big (k INTEGER, mid_k INTEGER, amount INTEGER);"
    "INSERT INTO fact_big SELECT g % 8, g % 200, g FROM range(0, 4000) t(g);"
)

QUERIES = [
    # Three-table join whose cheapest order starts from the smallest dimension.
    "SELECT d.label, count(*) AS n, sum(f.amount) AS total "
    "FROM {p}.fact_big f "
    "JOIN {p}.dim_small d ON f.k = d.k "
    "JOIN {p}.dim_mid m ON f.mid_k = m.k "
    "GROUP BY d.label ORDER BY d.label",
    # A selective filter on the small side: the reordered plan should drive from
    # it, and the answer must not depend on whether it does.
    "SELECT d.label, sum(f.amount) AS total "
    "FROM {p}.fact_big f "
    "JOIN {p}.dim_small d ON f.k = d.k "
    "WHERE d.k < 3 "
    "GROUP BY d.label ORDER BY d.label",
    # Four-way join, where the search space is large enough that enabling and
    # disabling the rule genuinely produce different shapes.
    "SELECT m.grp, count(*) AS n "
    "FROM {p}.fact_big f "
    "JOIN {p}.dim_small d ON f.k = d.k "
    "JOIN {p}.dim_mid m ON f.mid_k = m.k "
    "JOIN {p}.dim_small d2 ON m.grp = d2.k "
    "GROUP BY m.grp ORDER BY m.grp",
]


@pytest.fixture(scope="module")
def reordering_env():
    """One seeded DuckDB file plus a read-only oracle connection over it."""
    path = duckdb_path()
    writer = duckdb.connect(path)
    writer.execute(_SEED_SQL)
    writer.close()
    oracle = duckdb.connect(path, read_only=True)
    yield path, oracle
    oracle.close()


def _rows(runtime, sql):
    """Execute through the engine and return its rows as tuples, in order."""
    table = runtime.execute(sql)
    rows = []
    for record in table.to_pylist():
        rows.append(tuple(record.values()))
    return rows


@pytest.mark.parametrize("template", QUERIES)
def test_reordering_never_changes_the_answer(reordering_env, template):
    """Enabled and disabled must both match the single-connection oracle."""
    path, oracle = reordering_env
    engine_sql = template.format(p="duckdb_primary.main")
    oracle_sql = template.format(p="main")

    expected = oracle.execute(oracle_sql).fetchall()

    reordered = _rows(RustRuntime([("duckdb_primary", path)]), engine_sql)
    assert reordered == expected

    plain = _rows(RustRuntime([("duckdb_primary", path)], REORDER_OFF), engine_sql)
    assert plain == expected


def test_disabling_reordering_is_actually_wired(reordering_env):
    """The flag must reach the engine, or the test above proves nothing.

    Both runtimes answer the same four-way join; this asserts the two plans are
    not textually identical, so `enable_join_reordering: false` demonstrably
    changed planning rather than being accepted and ignored.
    """
    path, _oracle = reordering_env
    sql = QUERIES[2].format(p="duckdb_primary.main")

    reordered_plan = RustRuntime([("duckdb_primary", path)]).explain_text(sql)
    plain_plan = RustRuntime([("duckdb_primary", path)], REORDER_OFF).explain_text(sql)

    assert reordered_plan != plain_plan
