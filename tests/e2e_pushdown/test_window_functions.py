"""Window-function pushdown tests."""

import pytest
from sqlglot import exp

from federated_query.parser.errors import UnsupportedSQLError
from tests.e2e_pushdown.helpers import (
    is_func,
    build_runtime,
    explain_datasource_query,
)


def test_distinct_over_window_fails_fast(single_source_env):
    """SELECT DISTINCT over a window must not silently drop the DISTINCT.

    The window path runs in the merge engine and does not apply DISTINCT, so it
    fails fast rather than return duplicate rows.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT DISTINCT customer_id, "
        "SUM(price) OVER (PARTITION BY customer_id) AS s "
        "FROM duckdb_primary.main.orders"
    )
    with pytest.raises(UnsupportedSQLError):
        runtime.execute(sql)


def test_window_function_pushes_to_single_source(single_source_env):
    """A window-function projection pushes to the source as one remote query.

    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY price DESC) survives
    into the pushed remote SELECT with its partition and ordering intact.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY price DESC) AS rn "
        "FROM duckdb_primary.main.orders"
    )
    ast = explain_datasource_query(runtime, sql)

    windows = list(ast.find_all(exp.Window))
    assert len(windows) == 1
    window = windows[0]

    assert is_func(window.this, "ROW_NUMBER")

    partition = window.args.get("partition_by")
    assert [column.name.lower() for column in partition] == ["customer_id"]

    order = window.args.get("order")
    assert order is not None
    ordered = order.expressions[0]
    assert ordered.this.name.lower() == "price"
    assert ordered.args.get("desc") is True


def test_window_function_executes_correctly(single_source_env):
    """The pushed window query returns correct ROW_NUMBER values per partition.

    Highest price per customer is rank 1; each customer has exactly one rank 1.
    """
    runtime = build_runtime(single_source_env)
    sql = (
        "SELECT order_id, "
        "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY price DESC) AS rn "
        "FROM duckdb_primary.main.orders"
    )
    table = runtime.execute(sql)
    ranks = dict(
        zip(table.column("order_id").to_pylist(), table.column("rn").to_pylist())
    )

    # customer 1 has orders 6 (price 35) and 1 (price 25): order 6 ranks first.
    assert ranks[6] == 1
    assert ranks[1] == 2
    # customer 5 has orders 10 (price 200) and 5 (price 60): order 10 ranks first.
    assert ranks[10] == 1
    assert ranks[5] == 2
    # ten orders across five customers -> exactly five rank-1 rows.
    rank_one_count = 0
    for rank in ranks.values():
        if rank == 1:
            rank_one_count += 1
    assert rank_one_count == 5


def test_window_over_cross_source_join(multi_source_env):
    """A window over a cross-source join is computed in the merge engine.

    PARTITION BY a customers column and ORDER BY an orders column, so the window
    genuinely needs the two sources joined first, then ranked.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id, "
        "ROW_NUMBER() OVER (PARTITION BY c.segment ORDER BY o.price DESC) AS rn "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id"
    )
    table = runtime.execute(sql)
    ranks = dict(
        zip(table.column("order_id").to_pylist(), table.column("rn").to_pylist())
    )

    # enterprise (customers 1,2): orders 7,2,6,1 by price desc 90,50,35,25.
    assert ranks[7] == 1
    assert ranks[1] == 4
    # smb (customer 3): orders 3,8 by price desc 75,15.
    assert ranks[3] == 1
    # consumer (customers 4,5): orders 10,4,5,9 by price desc 200,125,60,10.
    assert ranks[10] == 1
    assert ranks[9] == 4


def test_window_in_derived_table_filtered_by_outer_in(multi_source_env):
    """A window inside a derived table referenced from WHERE is legal SQL.

    The WHERE-clause window rejection must prune at subquery boundaries: the
    rank() belongs to the derived table's own SELECT list (TPC-DS q70), only a
    window used DIRECTLY in this clause is invalid. Regions ranked by total
    price: EU 325, NA 195, APAC 165 - the top-2 filter keeps EU and NA rows.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.order_id FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "WHERE o.region IN ("
        "  SELECT region FROM ("
        "    SELECT region, rank() OVER (ORDER BY sum(price) DESC) AS ranking "
        "    FROM duckdb_orders.main.orders GROUP BY region) tmp "
        "  WHERE ranking <= 2)"
    )
    table = runtime.execute(sql)
    assert table.num_rows == 8


def test_grouping_inside_window_over_rollup_cross_source(multi_source_env):
    """rank() partitioned by GROUPING() over a ROLLUP runs cross-source.

    DataFusion cannot plan GROUPING() inside a window expression, so the
    aggregate materializes the window operands and the window runs over the
    materialized columns (the two-stage split; TPC-DS q70/q86). Detail rows
    (g=0) rank EU 1, NA 2, APAC 3 by total; the rollup row (g=1) ranks 1 in
    its own partition.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.region, grouping(o.region) AS g, sum(o.price) AS total, "
        "rank() OVER (PARTITION BY grouping(o.region) "
        "             ORDER BY sum(o.price) DESC) AS r "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "GROUP BY rollup(o.region) "
        "ORDER BY g, r"
    )
    table = runtime.execute(sql)
    rows = table.to_pylist()
    assert [row["region"] for row in rows] == ["EU", "NA", "APAC", None]
    assert [row["r"] for row in rows] == [1, 2, 3, 1]
    assert [row["g"] for row in rows] == [0, 0, 0, 1]


def test_order_by_aggregate_not_in_select_cross_source(multi_source_env):
    """ORDER BY an aggregate call absent from the SELECT list, cross-source.

    max(o.price) appears only in ORDER BY, so the binder materializes it as a
    hidden aggregate output the sort reads by name and the restore projection
    drops. Region maxima: EU 200, NA 125, APAC 90.
    """
    runtime = build_runtime(multi_source_env)
    sql = (
        "SELECT o.region, count(*) AS n "
        "FROM duckdb_orders.main.orders o "
        "JOIN duckdb_customers.main.customers c ON o.customer_id = c.customer_id "
        "GROUP BY o.region ORDER BY max(o.price) DESC"
    )
    table = runtime.execute(sql)
    assert table.column_names == ["region", "n"]
    assert table.column("region").to_pylist() == ["EU", "NA", "APAC"]
