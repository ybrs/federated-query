"""Parser safety-sweep tests.

The parser used to silently ignore SQL clauses it does not consume, which
returned wrong answers (DISTINCT ON, named WINDOW, TABLESAMPLE, SELECT *
EXCLUDE) or crashed deep in a source (GROUP BY ROLLUP, FETCH FIRST). Every such
clause must now fail fast with UnsupportedSQLError, and the clauses that already
work must keep working.
"""

import pytest

from federated_query.parser.errors import UnsupportedSQLError

from tests.e2e_pushdown.helpers import build_runtime

TABLE = "duckdb_primary.main.orders"


# Each case is a SQL fragment the engine must reject rather than misinterpret.
REJECTED_SQL = [
    f"SELECT order_id FROM {TABLE} ORDER BY price DESC FETCH FIRST 2 ROWS WITH TIES",
    f"SELECT region FROM {TABLE} GROUP BY ROLLUP (region), CUBE (status)",
    f"SELECT * FROM {TABLE} UNPIVOT (val FOR col IN (price, quantity))",
]


def test_fetch_first_only_works(single_source_env):
    """FETCH FIRST n ROWS ONLY behaves like LIMIT n (no ties)."""
    runtime = build_runtime(single_source_env)
    table = runtime.execute(
        f"SELECT order_id FROM {TABLE} ORDER BY order_id FETCH FIRST 3 ROWS ONLY"
    )
    assert table.num_rows == 3


@pytest.mark.parametrize("sql", REJECTED_SQL)
def test_unsupported_clause_fails_fast(single_source_env, sql):
    """An unconsumed clause raises UnsupportedSQLError instead of being dropped."""
    runtime = build_runtime(single_source_env)
    with pytest.raises(UnsupportedSQLError):
        runtime.execute(sql)


def test_qualify_still_executes(single_source_env):
    """QUALIFY works (postgres-dialect re-render rewrites it to a subquery)."""
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY price DESC) AS rn "
        f"FROM {TABLE} QUALIFY rn = 1"
    )
    table = runtime.execute(sql)
    # One row per customer (5 customers in the seeded data).
    assert table.num_rows == 5


def test_plain_distinct_still_executes(single_source_env):
    """A plain DISTINCT (no ON) is unaffected by the DISTINCT ON guard."""
    runtime = build_runtime(single_source_env)
    table = runtime.execute(f"SELECT DISTINCT region FROM {TABLE}")
    assert table.num_rows == 3
