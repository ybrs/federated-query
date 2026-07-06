"""Single-source rendering of a LEFT join whose nullable side is filtered.

The q13 shape: predicate pushdown legally moves a right-only ON conjunct into
the right input's scan. When the whole query then collapses into ONE remote
SQL, that scan filter MUST ride the JOIN condition - hoisted into the global
WHERE it evaluates NULL for every null-extended row and silently drops the
preserved side's unmatched rows (caught by the q13 benchmark differential:
zero-order customers vanished from the counts).
"""

from sqlglot import exp

from tests.e2e_pushdown.helpers import build_runtime, explain_datasource_query


Q13_SHAPE = (
    "SELECT C.customer_id, COUNT(O.order_id) AS order_count "
    "FROM duckdb_primary.main.customers C "
    "LEFT JOIN duckdb_primary.main.orders O "
    "ON C.customer_id = O.customer_id AND O.status <> 'pending' "
    "GROUP BY C.customer_id ORDER BY C.customer_id"
)


def test_nullable_side_filter_renders_in_the_on_clause(single_source_env):
    """The pushed-down right-side conjunct returns to the ON, never WHERE."""
    runtime = build_runtime(single_source_env)
    ast = explain_datasource_query(runtime, Q13_SHAPE)
    assert ast.args.get("where") is None, "filter leaked into the global WHERE"
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    on_sql = joins[0].args["on"].sql(dialect="postgres")
    assert "status" in on_sql, "the right-side conjunct must be in the ON"


def test_zero_match_preserved_rows_survive(single_source_env):
    """Differential: the engine's counts match DuckDB running the same query
    directly - including customers whose every order fails the ON filter
    (they must appear with a zero count, not vanish)."""
    runtime = build_runtime(single_source_env)
    table = runtime.execute(Q13_SHAPE)
    ours = []
    for customer_id, count in zip(
        table.column("customer_id").to_pylist(),
        table.column("order_count").to_pylist(),
    ):
        ours.append((customer_id, count))
    connection = single_source_env.datasources[0].connection
    rows = connection.execute(
        "SELECT C.customer_id, COUNT(O.order_id) "
        "FROM customers C LEFT JOIN orders O "
        "ON C.customer_id = O.customer_id AND O.status <> 'pending' "
        "GROUP BY C.customer_id ORDER BY C.customer_id"
    ).fetchall()
    expected = []
    for row in rows:
        expected.append((row[0], row[1]))
    assert ours == expected
