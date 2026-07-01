"""Cross-source LATERAL (dependent join) execution tests.

A LATERAL whose left and right live on different sources can't push to one
source. It runs in the merge engine: the left and the right's base relation are
materialized into Arrow and the LATERAL is executed by the in-memory DuckDB,
with the base reduced to the left's correlation domain (a dynamic filter) before
transfer. Results are checked against the same query on a single combined DuckDB
seeded with identical data.
"""

import duckdb

from tests.e2e_pushdown.conftest import _seed_orders, _seed_products
from tests.e2e_pushdown.helpers import build_runtime


def _reference():
    """A single DuckDB holding both tables, for the expected cross-source result."""
    con = duckdb.connect()
    _seed_orders(con)
    _seed_products(con)
    return con


def _assert_cross_source_matches(multi_source_env, engine_sql, reference_sql):
    """Compare the engine's cross-source result to the combined-DB reference."""
    runtime = build_runtime(multi_source_env)
    result = runtime.execute(engine_sql)
    engine_rows = list(zip(result.column(0).to_pylist(), result.column(1).to_pylist()))
    con = _reference()
    expected = con.execute(reference_sql).fetchall()
    con.close()
    assert sorted(engine_rows) == sorted(expected)


def test_cross_source_non_equi_scalar_aggregate(multi_source_env):
    """A non-equi correlated scalar aggregate across sources matches the source.

    Decorrelates to a LATERAL; the base products scan is reduced to the orders
    price domain (a ``< max`` range filter) before it crosses to its source.
    """
    engine_sql = (
        "SELECT o.order_id, "
        "  (SELECT MAX(p.base_price) FROM duckdb_products.main.products p "
        "   WHERE p.base_price < o.price) AS m "
        "FROM duckdb_orders.main.orders o"
    )
    reference_sql = (
        "SELECT o.order_id, "
        "  (SELECT MAX(p.base_price) FROM products p WHERE p.base_price < o.price) AS m "
        "FROM orders o"
    )
    _assert_cross_source_matches(multi_source_env, engine_sql, reference_sql)


def test_cross_source_user_left_join_lateral(multi_source_env):
    """A cross-source user ``LEFT JOIN LATERAL`` (equi) matches the source.

    The equi correlation pushes an ``IN (domain)`` semi-join reduction to the
    products source; non-matching outer rows survive with a NULL value.
    """
    engine_sql = (
        "SELECT o.order_id, t.name FROM duckdb_orders.main.orders o "
        "LEFT JOIN LATERAL ("
        "  SELECT p.name FROM duckdb_products.main.products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t ON true"
    )
    reference_sql = (
        "SELECT o.order_id, t.name FROM orders o "
        "LEFT JOIN LATERAL ("
        "  SELECT p.name FROM products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t ON true"
    )
    _assert_cross_source_matches(multi_source_env, engine_sql, reference_sql)


def test_cross_source_comma_lateral_is_inner(multi_source_env):
    engine_sql = (
        "SELECT o.order_id, t.name FROM duckdb_orders.main.orders o, "
        "LATERAL ("
        "  SELECT p.name FROM duckdb_products.main.products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t"
    )
    reference_sql = (
        "SELECT o.order_id, t.name FROM orders o, "
        "LATERAL ("
        "  SELECT p.name FROM products p "
        "  WHERE p.id = o.product_id ORDER BY p.name LIMIT 1"
        ") t"
    )
    _assert_cross_source_matches(multi_source_env, engine_sql, reference_sql)
