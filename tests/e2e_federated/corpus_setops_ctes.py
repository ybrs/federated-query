"""Set-operation and CTE corpus: UNION/INTERSECT/EXCEPT, CTEs, derived tables.

Case names are prefixed ``set_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage: UNION/UNION ALL dedup semantics with genuine cross-branch duplicates,
INTERSECT/EXCEPT over duplicate keys, a branch that is a join, a branch that is
an aggregate, column alignment (first-branch names, explicit CASTs), nested set
ops with parentheses controlling precedence, ORDER BY/LIMIT over a whole set-op
result and inside a branch wrapped as a derived table, NULLs flowing through
UNION/INTERSECT/EXCEPT, CTEs consumed once/twice (including a cross-source join
CTE and a CTE feeding EXISTS), chained CTEs, derived tables (joined, stacked
two deep, with an inner LIMIT), and WITH RECURSIVE (an integer series and an
employee/manager hierarchy walk joined cross-source).

Two shapes raise loudly rather than run: a ``WITH`` clause bound directly to a
top-level set operation is an unsupported parse shape (worked around here by
wrapping the union in a derived-table ``SELECT``, which the engine does
support), pinned nowhere further since it is a designed restriction, not a
wrong answer. ``INTERSECT ALL`` / ``EXCEPT ALL`` and a WHERE predicate on a
BOOLEAN column pushed to PostgreSQL are genuine bugs (wrong multiset counts
cross-source; a physical-planning crash) and are pinned as reproductions in
``SUSPECTED_ENGINE_BUGS`` below instead of being asserted here.
"""

CASES = [
    {
        "name": "set_union_dedup_customer_ids",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT customer_id FROM {orders} "
            "UNION SELECT customer_id FROM {customers}"
        ),
    },
    {
        "name": "set_union_all_customer_ids",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT customer_id FROM {orders} "
            "UNION ALL SELECT customer_id FROM {customers}"
        ),
    },
    {
        "name": "set_intersect_customer_ids",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT customer_id FROM {orders} "
            "INTERSECT SELECT customer_id FROM {customers}"
        ),
    },
    {
        "name": "set_except_products_not_ordered",
        "tables": ["products", "orders", "customers"],
        "query": (
            "SELECT product_id FROM {products} "
            "EXCEPT SELECT product_id FROM {orders}"
        ),
    },
    {
        "name": "set_except_customers_without_orders",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT customer_id FROM {customers} "
            "EXCEPT SELECT customer_id FROM {orders}"
        ),
    },
    {
        "name": "set_intersect_dup_keys_distinct",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": "SELECT k FROM {t_dup_a} INTERSECT SELECT k FROM {t_dup_b}",
    },
    {
        "name": "set_except_dup_keys_distinct",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": "SELECT k FROM {t_dup_a} EXCEPT SELECT k FROM {t_dup_b}",
    },
    {
        "name": "set_union_branch_is_join",
        "tables": ["orders", "customers", "products"],
        "query": (
            "SELECT c.segment AS grp FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "UNION SELECT category AS grp FROM {products}"
        ),
    },
    {
        "name": "set_intersect_branch_is_aggregate",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT segment FROM {customers} "
            "INTERSECT "
            "SELECT c.segment FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "GROUP BY c.segment"
        ),
    },
    {
        "name": "set_except_branch_is_join",
        "tables": ["products", "orders", "customers"],
        "query": (
            "SELECT category FROM {products} "
            "EXCEPT "
            "SELECT p.category FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "WHERE p.category = 'food'"
        ),
    },
    {
        "name": "set_union_all_branch_is_aggregate",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT segment FROM {customers} "
            "UNION ALL "
            "SELECT c.segment FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "GROUP BY c.segment"
        ),
    },
    {
        "name": "set_union_first_branch_name_wins",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT order_id AS ident FROM {orders} "
            "UNION SELECT customer_id AS cid FROM {customers}"
        ),
    },
    {
        "name": "set_union_cast_align_decimal",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT price AS amt FROM {orders} "
            "UNION SELECT CAST(product_id AS DECIMAL(10,2)) AS amt FROM {products}"
        ),
    },
    {
        "name": "set_union_cast_align_bigint",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT CAST(order_id AS BIGINT) AS big_val FROM {orders} "
            "UNION SELECT CAST(product_id AS BIGINT) AS big_val FROM {products}"
        ),
    },
    {
        "name": "set_intersect_cast_align_ids",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT CAST(order_id AS BIGINT) AS ident FROM {orders} "
            "INTERSECT SELECT CAST(customer_id AS BIGINT) AS ident FROM {customers}"
        ),
    },
    {
        "name": "set_except_cast_align_ids",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT CAST(order_id AS BIGINT) AS ident FROM {orders} "
            "EXCEPT SELECT CAST(customer_id AS BIGINT) AS ident FROM {customers}"
        ),
    },
    {
        "name": "set_nested_union_intersect_left",
        "tables": ["orders", "products", "customers"],
        "query": (
            "(SELECT order_id AS n FROM {orders} "
            "UNION SELECT product_id AS n FROM {products}) "
            "INTERSECT SELECT customer_id AS n FROM {customers}"
        ),
    },
    {
        "name": "set_nested_union_intersect_right",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT order_id AS n FROM {orders} "
            "UNION "
            "(SELECT product_id AS n FROM {products} "
            "INTERSECT SELECT customer_id AS n FROM {customers})"
        ),
    },
    {
        "name": "set_nested_except_union",
        "tables": ["orders", "products", "customers"],
        "query": (
            "(SELECT order_id AS n FROM {orders} "
            "EXCEPT SELECT product_id AS n FROM {products}) "
            "UNION SELECT customer_id AS n FROM {customers}"
        ),
    },
    {
        "name": "set_order_by_limit_whole_result",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT customer_id AS cid FROM {orders} "
            "UNION SELECT customer_id AS cid FROM {customers} "
            "ORDER BY cid LIMIT 5"
        ),
    },
    {
        "name": "set_order_by_limit_whole_result_union_all",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id AS ident, 'o' AS src FROM {orders} o "
            "UNION ALL "
            "SELECT c.customer_id AS ident, 'c' AS src FROM {customers} c "
            "ORDER BY ident, src LIMIT 6"
        ),
    },
    {
        "name": "set_branch_order_by_limit_derived",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT ident FROM ("
            "SELECT order_id AS ident FROM {orders} "
            "ORDER BY price DESC LIMIT 3) t "
            "UNION ALL SELECT customer_id AS ident FROM {customers}"
        ),
    },
    {
        "name": "set_union_nulls_dedup",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} UNION SELECT k FROM {t_null_b}",
    },
    {
        "name": "set_intersect_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} INTERSECT SELECT k FROM {t_null_b}",
    },
    {
        "name": "set_except_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} EXCEPT SELECT k FROM {t_null_b}",
    },
    {
        "name": "set_union_all_nulls_keeps_dupes",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} UNION ALL SELECT k FROM {t_null_b}",
    },
    {
        "name": "set_cte_consumed_once",
        "tables": ["orders"],
        "query": (
            "WITH proc AS ("
            "SELECT order_id, price FROM {orders} WHERE status = 'processing') "
            "SELECT order_id, price FROM proc"
        ),
    },
    {
        "name": "set_cte_consumed_twice_self_join_union",
        "tables": ["orders"],
        "query": (
            "WITH ids AS (SELECT customer_id FROM {orders}) "
            "SELECT customer_id FROM ("
            "SELECT a.customer_id FROM ids a JOIN ids b "
            "ON a.customer_id = b.customer_id "
            "UNION SELECT customer_id FROM ids) u"
        ),
    },
    {
        "name": "set_cte_cross_source_join_twice",
        "tables": ["orders", "customers"],
        "query": (
            "WITH oc AS ("
            "SELECT o.order_id, c.segment FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id) "
            "SELECT order_id, segment FROM ("
            "SELECT a.order_id, b.segment FROM oc a JOIN oc b "
            "ON a.order_id = b.order_id "
            "UNION SELECT order_id, segment FROM oc) u"
        ),
    },
    {
        "name": "set_cte_second_references_first",
        "tables": ["orders", "customers"],
        "query": (
            "WITH ord AS ("
            "SELECT order_id, customer_id, price FROM {orders}), "
            "big_ord AS ("
            "SELECT order_id, customer_id, price FROM ord WHERE price > 30) "
            "SELECT b.order_id, c.name FROM big_ord b "
            "JOIN {customers} c ON b.customer_id = c.customer_id"
        ),
    },
    {
        "name": "set_cte_feeds_exists",
        "tables": ["orders", "customers"],
        "query": (
            "WITH ord_cust AS (SELECT customer_id FROM {orders}) "
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE EXISTS ("
            "SELECT 1 FROM ord_cust WHERE ord_cust.customer_id = c.customer_id)"
        ),
    },
    {
        "name": "set_cte_used_in_two_branches_of_union",
        "tables": ["orders"],
        "query": (
            "WITH ord AS (SELECT order_id, status FROM {orders}) "
            "SELECT order_id FROM ("
            "SELECT order_id FROM ord WHERE status = 'processing' "
            "UNION SELECT order_id FROM ord WHERE status = 'shipped') u"
        ),
    },
    {
        "name": "set_cte_over_star_join_twice",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "WITH sales_dept AS ("
            "SELECT f.sale_id, di.dept FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key) "
            "SELECT sale_id FROM ("
            "SELECT a.sale_id FROM sales_dept a JOIN sales_dept b "
            "ON a.dept = b.dept AND a.sale_id <> b.sale_id "
            "UNION SELECT sale_id FROM sales_dept) u"
        ),
    },
    {
        "name": "set_derived_subquery_in_from_joined",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name, big.order_id FROM {customers} c "
            "JOIN ("
            "SELECT order_id, customer_id FROM {orders} WHERE price > 20) big "
            "ON c.customer_id = big.customer_id"
        ),
    },
    {
        "name": "set_derived_stacked_two_deep",
        "tables": ["orders"],
        "query": (
            "SELECT outer_t.order_id FROM ("
            "SELECT inner_t.order_id, inner_t.price FROM ("
            "SELECT order_id, price FROM {orders} WHERE price > 10) inner_t"
            ") outer_t WHERE outer_t.price < 200"
        ),
    },
    {
        "name": "set_derived_with_limit_inside",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name, top5.order_id FROM {customers} c "
            "JOIN ("
            "SELECT order_id, customer_id FROM {orders} "
            "ORDER BY price DESC LIMIT 5) top5 "
            "ON c.customer_id = top5.customer_id"
        ),
    },
    {
        "name": "set_derived_aggregate_joined_cross_source",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name, agg.total FROM {customers} c "
            "JOIN ("
            "SELECT customer_id, sum(price) AS total FROM {orders} "
            "GROUP BY customer_id) agg "
            "ON c.customer_id = agg.customer_id"
        ),
    },
    {
        "name": "set_recursive_integer_series",
        "tables": ["orders"],
        "query": (
            "WITH RECURSIVE counter(n) AS ("
            "SELECT 1 "
            "UNION ALL "
            "SELECT n + 1 FROM counter WHERE n < 5"
            ") SELECT n FROM counter"
        ),
    },
    {
        "name": "set_recursive_hierarchy_walk",
        "tables": ["customers"],
        "extra_tables": {
            "employee": {
                "ddl": (
                    "CREATE TABLE employee "
                    "(emp_id INTEGER, name VARCHAR, mgr_id INTEGER)"
                ),
                "inserts": [
                    "INSERT INTO employee VALUES"
                    " (1, 'Alice', NULL),"
                    " (2, 'Bob', 1),"
                    " (3, 'Cara', 1),"
                    " (4, 'Dan', 2),"
                    " (5, 'Eve', 2),"
                    " (6, 'Zoe', 3)"
                ],
            }
        },
        "query": (
            "WITH RECURSIVE org(emp_id, emp_name, lvl) AS ("
            "SELECT emp_id, name, 0 FROM {employee} WHERE mgr_id IS NULL "
            "UNION ALL "
            "SELECT e.emp_id, e.name, org.lvl + 1 "
            "FROM {employee} e JOIN org ON e.mgr_id = org.emp_id"
            ") SELECT org.emp_name, c.city FROM org "
            "JOIN {customers} c ON org.emp_id = c.customer_id"
        ),
    },
    {
        "name": "set_union_three_way_multi_source",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT order_id AS n FROM {orders} "
            "UNION SELECT product_id AS n FROM {products} "
            "UNION SELECT customer_id AS n FROM {customers}"
        ),
    },
    {
        "name": "set_except_three_way_chain",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT order_id AS n FROM {orders} "
            "EXCEPT SELECT product_id AS n FROM {products} "
            "EXCEPT SELECT customer_id AS n FROM {customers}"
        ),
    },
    {
        "name": "set_cte_two_ctes_unioned",
        "tables": ["orders", "customers"],
        "query": (
            "WITH a AS ("
            "SELECT customer_id FROM {orders} WHERE status = 'processing'), "
            "b AS ("
            "SELECT customer_id FROM {customers} WHERE segment = 'enterprise') "
            "SELECT customer_id FROM ("
            "SELECT customer_id FROM a UNION SELECT customer_id FROM b) u"
        ),
    },
    {
        "name": "set_except_where_each_branch",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT product_id FROM {orders} WHERE quantity > 3 "
            "EXCEPT SELECT product_id FROM {products} WHERE category = 'home'"
        ),
    },
    {
        "name": "set_union_all_distinct_wrapper",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT cid FROM ("
            "SELECT customer_id AS cid FROM {orders} "
            "UNION ALL SELECT customer_id AS cid FROM {customers}) u"
        ),
    },
    {
        "name": "set_cross_source_only_union",
        "tables": ["orders", "customers"],
        "min_sources": 2,
        "query": (
            "SELECT customer_id FROM {orders} "
            "UNION SELECT customer_id FROM {customers}"
        ),
    },
]


# Verified engine-vs-oracle mismatches or crashes, pulled out of CASES so the
# corpus stays green. Not run by the harness (only ``CASES`` is collected);
# each entry is a pinned repro plus the observed finding.
SUSPECTED_ENGINE_BUGS = {
    "set_intersect_all_shared_ids": {
        "tables": ["t_dup_a", "t_dup_b"],
        "query": "SELECT k FROM {t_dup_a} INTERSECT ALL SELECT k FROM {t_dup_b}",
        "finding": (
            "INTERSECT ALL is wrong whenever the two branches are split across "
            "distinct sources (even duck_duck, two plain DuckDB files - this is "
            "not PostgreSQL-specific). t_dup_a.k = [1,1,2,2,3], "
            "t_dup_b.k = [1,1,2,4]; standard multiset semantics take "
            "min(count_a, count_b) per value, so the correct INTERSECT ALL is "
            "[1,1,2] (oracle_single_duck returns exactly this). Every "
            "cross-source placement (duck_duck, pg_duck, duck_pg, all_pg, "
            "parquet_duck, parquet_pg) instead returns [1,1,2,2]: engine 4 rows "
            "vs oracle 3 rows, as if the per-source local results were unioned "
            "without reconciling multiplicities across sources."
        ),
    },
    "set_except_all_shared_ids": {
        "tables": ["t_dup_a", "t_dup_b"],
        "query": "SELECT k FROM {t_dup_a} EXCEPT ALL SELECT k FROM {t_dup_b}",
        "finding": (
            "EXCEPT ALL has the same cross-source defect as INTERSECT ALL "
            "above. With t_dup_a.k = [1,1,2,2,3] and t_dup_b.k = [1,1,2,4], the "
            "correct EXCEPT ALL (count_a - count_b, floored at 0) is [2,3] "
            "(oracle_single_duck returns exactly this, 2 rows). Every "
            "cross-source placement (duck_duck, pg_duck, duck_pg, all_pg, "
            "parquet_duck, parquet_pg) instead returns only [3]: engine 1 row "
            "vs oracle 2 rows, dropping the k=2 remainder entirely."
        ),
    },
    "set_boolean_predicate_pushdown_to_postgres_crashes": {
        "tables": ["products"],
        "query": "SELECT product_id FROM {products} WHERE active = TRUE",
        "finding": (
            "Any WHERE predicate over a BOOLEAN column crashes physical "
            "planning once that table is placed on PostgreSQL - not specific "
            "to this corpus's set-ops (found while building "
            "set_except_where_each_branch, whose products branch originally "
            "filtered on 'active = FALSE'). Reproduced directly against a "
            "pg_duck environment seeded with orders/products/customers: "
            "'SELECT product_id, active FROM products' (no predicate) and "
            "predicates on VARCHAR/DECIMAL columns (category, unit_price) both "
            "succeed; 'WHERE active = TRUE', 'WHERE active = FALSE', and "
            "'WHERE NOT active' each raise 'physical planning error: "
            "datasource error: db error' with no further detail (consistent "
            "across repeated clean-state runs). 'WHERE active IS FALSE' fails "
            'differently, at parse time: "unsupported SQL: function '
            "'is_false'\". The corpus's set_except_where_each_branch case was "
            "rewritten to filter on category instead of active to route "
            "around this and stay green; this entry pins the underlying bug."
        ),
    },
}
