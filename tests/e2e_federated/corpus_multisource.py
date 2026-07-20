"""Multi-source corpus: shapes whose work spans three or four data sources.

Case names are prefixed ``multi_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement. Each
case carries ``min_sources`` 3 or 4 so it runs only where its tables genuinely
spread over that many distinct sources - the three- and four-source placements
(``tri_source``, ``tri_source_rev``, ``quad_source``). A case with ``min_sources``
4 runs only under ``quad_source``.

Coverage: per-source filters on every leg of a three- and four-table join, a
filter applied after joining through a middle source, OR-filters spanning two
and three sources (coordinator-only, cannot push), and NOT/IS NULL mixes per
source; three-branch UNION/UNION ALL/INTERSECT/EXCEPT (and the ALL variants)
each branch on a different source, a union of joins over different source pairs,
nested set ops across three sources, and a union of three sources consumed by a
join against a fourth; four- and five-table join chains crossing a source
boundary at each hop, a star with a fact and three dimensions on four sources, a
snowflake, two facts joined through a shared dimension, a self-join joined to two
further sources, and a join key flowing through a middle table under an alias;
and mixed shapes - an aggregate grouped by columns from two sources, EXISTS/IN
whose subquery lives on a third source, a two-source-join CTE consumed by
branches joining different third sources, derived tables from three sources
unioned then aggregated, two aggregated derived tables from different sources
joined, and a semi-join reduction across three sources.

To keep the per-(tables, placement) environment cache small, cases reuse a
handful of table sets; a case sometimes declares a table its query does not
touch so it shares another case's seeded environment. Boolean columns are never
placed in WHERE or GROUP BY: a boolean column of a PostgreSQL-hosted table
breaks the planner's statistics probe (documented in
``corpus_aggregates.SUSPECTED_ENGINE_BUGS``), which is unrelated to the
multi-source behavior these cases exercise.
"""

_DIM_DEPT = {
    "ddl": "CREATE TABLE dim_dept (dept VARCHAR, region VARCHAR)",
    "inserts": [
        "INSERT INTO dim_dept VALUES"
        " ('hardware', 'east'),"
        " ('media', 'west'),"
        " ('toys', 'north')"
    ],
}

CASES = [
    # Per-source filters: every leg of a three- and four-table join contributes
    # a predicate, plus a filter applied only after the join and OR/NULL mixes.
    {
        "name": "multi_filter_each_leg_trio",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.quantity >= 2 AND p.category = 'clothing' "
            "AND c.segment = 'enterprise'"
        ),
    },
    {
        "name": "multi_filter_each_leg_four",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 4,
        "query": (
            "SELECT o.order_id, p.name AS product, c.city, l.description "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {t_lookup} l ON o.status = l.code "
            "WHERE o.quantity >= 2 AND p.unit_price > 15 "
            "AND c.city <> 'Paris' AND l.description <> 'called off'"
        ),
    },
    {
        "name": "multi_filter_after_join_through_b",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT sub.order_id, sub.cust FROM ("
            "SELECT o.order_id AS order_id, c.name AS cust, "
            "c.segment AS segment, p.category AS category "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id"
            ") sub WHERE sub.segment = 'consumer' AND sub.category = 'home'"
        ),
    },
    {
        "name": "multi_filter_or_two_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, c.name AS cust "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.status = 'shipped' OR c.city = 'London'"
        ),
    },
    {
        "name": "multi_filter_or_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.segment "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.status = 'cancelled' OR c.segment = 'consumer' "
            "OR p.category = 'food'"
        ),
    },
    {
        "name": "multi_filter_or_and_mix",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.category, c.segment "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE (o.status = 'processing' AND c.segment = 'enterprise') "
            "OR p.category = 'food'"
        ),
    },
    {
        "name": "multi_filter_between_across",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.price BETWEEN 20 AND 100 AND p.unit_price >= 20 "
            "AND c.segment IN ('enterprise', 'smb')"
        ),
    },
    {
        "name": "multi_filter_in_list_per_source",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 4,
        "query": (
            "SELECT o.order_id, p.name AS product, c.city, l.description "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {t_lookup} l ON o.status = l.code "
            "WHERE o.status IN ('processing', 'shipped') "
            "AND p.category IN ('clothing', 'electronics') "
            "AND c.city IN ('New York', 'London', 'Boston') "
            "AND l.code IN ('processing', 'shipped')"
        ),
    },
    {
        "name": "multi_filter_not_isnull_per_source",
        "tables": ["t_null_a", "t_null_b", "t_dup_a", "t_dup_b"],
        "min_sources": 3,
        "query": (
            "SELECT a.val, b.label, da.tag "
            "FROM {t_null_a} a "
            "JOIN {t_null_b} b ON a.id = b.id "
            "JOIN {t_dup_a} da ON a.id = da.k "
            "WHERE a.k IS NOT NULL AND b.label NOT LIKE 'b-null%' "
            "AND da.tag IS NOT NULL"
        ),
    },
    {
        "name": "multi_filter_isnull_outer_join",
        "tables": ["t_null_a", "t_null_b", "t_dup_a", "t_dup_b"],
        "min_sources": 3,
        "query": (
            "SELECT a.id, b.label, da.tag "
            "FROM {t_null_a} a "
            "LEFT JOIN {t_null_b} b ON a.k = b.k "
            "LEFT JOIN {t_dup_a} da ON a.id = da.k "
            "WHERE b.k IS NULL OR da.k IS NULL"
        ),
    },
    # Three-branch set operations, each branch on a different source; a union of
    # joins over different pairs; nested set ops; a union consumed by a fourth.
    {
        "name": "multi_union_three_sources_dedup",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT customer_id AS v FROM {orders} "
            "UNION SELECT customer_id AS v FROM {customers} "
            "UNION SELECT product_id - 100 AS v FROM {products}"
        ),
    },
    {
        "name": "multi_union_all_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT customer_id AS v FROM {orders} "
            "UNION ALL SELECT customer_id AS v FROM {customers} "
            "UNION ALL SELECT product_id - 100 AS v FROM {products}"
        ),
    },
    {
        "name": "multi_intersect_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT customer_id AS v FROM {orders} "
            "INTERSECT SELECT customer_id AS v FROM {customers} "
            "INTERSECT SELECT product_id - 100 AS v FROM {products}"
        ),
    },
    {
        "name": "multi_except_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT product_id - 100 AS v FROM {products} "
            "EXCEPT SELECT customer_id AS v FROM {orders} "
            "EXCEPT SELECT customer_id AS v FROM {customers}"
        ),
    },
    {
        "name": "multi_intersect_all_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT customer_id AS v FROM {orders} "
            "INTERSECT ALL SELECT customer_id AS v FROM {customers} "
            "INTERSECT ALL SELECT product_id - 100 AS v FROM {products}"
        ),
    },
    {
        "name": "multi_except_all_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT customer_id AS v FROM {orders} "
            "EXCEPT ALL SELECT customer_id AS v FROM {customers} "
            "EXCEPT ALL SELECT product_id - 100 AS v FROM {products}"
        ),
    },
    {
        "name": "multi_setops_nested_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "(SELECT product_id - 100 AS v FROM {products} "
            "EXCEPT SELECT customer_id AS v FROM {orders}) "
            "UNION SELECT customer_id AS v FROM {customers}"
        ),
    },
    {
        "name": "multi_union_of_join_pairs",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT c.segment AS lbl, o.order_id AS val "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "UNION ALL "
            "SELECT di.dept AS lbl, f.sale_id AS val "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "multi_union_left_join_branches",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT c.segment AS lbl, o.order_id AS val "
            "FROM {orders} o LEFT JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "UNION ALL "
            "SELECT di.dept AS lbl, f.sale_id AS val "
            "FROM {fact_sales} f LEFT JOIN {dim_item} di "
            "ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "multi_union_consumed_by_join_fourth",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 4,
        "query": (
            "SELECT o.order_id AS order_id, u.k AS k FROM ("
            "SELECT item_key AS k FROM {fact_sales} "
            "UNION SELECT item_key AS k FROM {dim_item} "
            "UNION SELECT customer_id + 10 AS k FROM {customers}"
            ") u JOIN {orders} o ON o.product_id - 90 = u.k"
        ),
    },
    # Join chains and stars crossing a source boundary at each hop.
    {
        "name": "multi_chain_snowflake_4",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "extra_tables": {"dim_dept": _DIM_DEPT},
        "min_sources": 3,
        "query": (
            "SELECT d.cal_date, di.item_name, dd.region, f.amount "
            "FROM {dim_day} d "
            "JOIN {fact_sales} f ON d.day_key = f.day_key "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "JOIN {dim_dept} dd ON di.dept = dd.dept"
        ),
    },
    {
        "name": "multi_key_alias_flow",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "extra_tables": {"dim_dept": _DIM_DEPT},
        "min_sources": 3,
        "query": (
            "SELECT f.sale_id, m.item_name, dd.region "
            "FROM {fact_sales} f "
            "JOIN (SELECT item_key AS ik, item_name, dept FROM {dim_item}) m "
            "ON f.item_key = m.ik "
            "JOIN {dim_dept} dd ON m.dept = dd.dept"
        ),
    },
    {
        "name": "multi_chain_5_cross_boundary",
        "tables": ["orders", "products", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust, "
            "di.item_name "
            "FROM {customers} c "
            "JOIN {orders} o ON c.customer_id = o.customer_id "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {fact_sales} f ON f.item_key = o.product_id - 90 "
            "JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "multi_star_orders_three_dims",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 4,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust, "
            "l.description AS descr "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {t_lookup} l ON o.status = l.code"
        ),
    },
    {
        "name": "multi_star_orders_three_dims_left",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust, "
            "l.description AS descr "
            "FROM {orders} o "
            "LEFT JOIN {products} p ON o.product_id = p.product_id "
            "LEFT JOIN {customers} c ON o.customer_id = c.customer_id "
            "LEFT JOIN {t_lookup} l ON o.status = l.code"
        ),
    },
    {
        "name": "multi_chain_left_boundary",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT d.cal_date, f.sale_id, di.item_name "
            "FROM {dim_day} d "
            "LEFT JOIN {fact_sales} f ON d.day_key = f.day_key "
            "LEFT JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "multi_two_facts_shared_dim",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT di.item_name, f.amount, o.order_id "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "JOIN {orders} o ON o.product_id - 90 = di.item_key"
        ),
    },
    {
        "name": "multi_selfjoin_across_three",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o1.order_id AS a_id, o2.order_id AS b_id, "
            "c.name AS cust, p.name AS product "
            "FROM {orders} o1 "
            "JOIN {orders} o2 ON o1.customer_id = o2.customer_id "
            "AND o1.order_id < o2.order_id "
            "JOIN {customers} c ON o1.customer_id = c.customer_id "
            "JOIN {products} p ON o1.product_id = p.product_id"
        ),
    },
    # Aggregates, subqueries, CTEs and semi-joins whose inputs span sources.
    {
        "name": "multi_agg_group_two_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT c.segment AS seg, p.category AS cat, count(*) AS n, "
            "sum(o.price) AS total "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {products} p ON o.product_id = p.product_id "
            "GROUP BY c.segment, p.category"
        ),
    },
    {
        "name": "multi_agg_having_two_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT c.segment AS seg, p.category AS cat, "
            "sum(o.quantity) AS q "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {products} p ON o.product_id = p.product_id "
            "GROUP BY c.segment, p.category HAVING sum(o.quantity) > 3"
        ),
    },
    {
        "name": "multi_agg_four_source_having",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 4,
        "query": (
            "SELECT l.description AS descr, count(*) AS n "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {t_lookup} l ON o.status = l.code "
            "GROUP BY l.description HAVING count(*) >= 2"
        ),
    },
    {
        "name": "multi_agg_star_three",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT d.yr AS yr, di.dept AS dept, sum(f.amount) AS total, "
            "count(*) AS n "
            "FROM {fact_sales} f "
            "JOIN {dim_day} d ON f.day_key = d.day_key "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "GROUP BY d.yr, di.dept"
        ),
    },
    {
        "name": "multi_exists_outer_two_sub_third",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE EXISTS (SELECT 1 FROM {products} p "
            "WHERE p.product_id = o.product_id AND p.category = 'electronics')"
        ),
    },
    {
        "name": "multi_not_exists_outer_two_sub_third",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE NOT EXISTS (SELECT 1 FROM {products} p "
            "WHERE p.product_id = o.product_id AND p.category = 'electronics')"
        ),
    },
    {
        "name": "multi_in_subquery_third",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, c.name AS cust "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.product_id IN (SELECT p.product_id FROM {products} p "
            "WHERE p.category = 'home')"
        ),
    },
    {
        "name": "multi_scalar_subquery_third",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, c.name AS cust, "
            "(SELECT p.name FROM {products} p "
            "WHERE p.product_id = o.product_id) AS pname "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "multi_exists_and_in_two_subs",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 4,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE EXISTS (SELECT 1 FROM {products} p "
            "WHERE p.product_id = o.product_id AND p.unit_price > 100) "
            "AND o.status IN (SELECT l.code FROM {t_lookup} l "
            "WHERE l.description <> 'called off')"
        ),
    },
    {
        "name": "multi_cte_two_branches_diff_third",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "min_sources": 3,
        "query": (
            "WITH oc AS ("
            "SELECT o.order_id AS order_id, o.product_id AS product_id, "
            "o.status AS status FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id) "
            "SELECT w.oid, w.lbl FROM ("
            "SELECT oc.order_id AS oid, p.name AS lbl "
            "FROM oc JOIN {products} p ON oc.product_id = p.product_id "
            "UNION ALL "
            "SELECT oc.order_id AS oid, l.description AS lbl "
            "FROM oc JOIN {t_lookup} l ON oc.status = l.code"
            ") w"
        ),
    },
    {
        "name": "multi_cte_join_third",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "WITH oc AS ("
            "SELECT o.order_id AS order_id, o.product_id AS product_id "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id) "
            "SELECT oc.order_id AS oid, di.item_name AS item "
            "FROM oc "
            "JOIN {fact_sales} f ON f.item_key = oc.product_id - 90 "
            "JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "multi_derived_union_aggregated",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT src, count(*) AS n FROM ("
            "SELECT 'o' AS src, customer_id AS v FROM {orders} "
            "UNION ALL SELECT 'c' AS src, customer_id AS v FROM {customers} "
            "UNION ALL SELECT 'p' AS src, product_id - 100 AS v FROM {products}"
            ") u GROUP BY src"
        ),
    },
    {
        "name": "multi_derived_two_aggs_joined",
        "tables": ["orders", "customers", "fact_sales", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT a.ik AS ik, a.cnt AS cnt, b.amt AS amt FROM ("
            "SELECT o.product_id - 90 AS ik, count(*) AS cnt "
            "FROM {orders} o GROUP BY o.product_id - 90) a "
            "JOIN ("
            "SELECT f.item_key AS ik, sum(f.amount) AS amt "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "GROUP BY f.item_key) b ON a.ik = b.ik"
        ),
    },
    {
        "name": "multi_semijoin_reduction_three",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "min_sources": 3,
        "query": (
            "SELECT di.item_name, f.amount "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "WHERE f.day_key IN (SELECT d.day_key FROM {dim_day} d "
            "WHERE d.cal_date >= DATE '2024-01-02')"
        ),
    },
    {
        "name": "multi_case_expr_three_sources",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, "
            "CASE WHEN c.segment = 'enterprise' THEN p.category "
            "ELSE o.status END AS lbl "
            "FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {products} p ON o.product_id = p.product_id"
        ),
    },
    {
        "name": "multi_full_join_three",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust "
            "FROM {orders} o "
            "FULL JOIN {products} p ON o.product_id = p.product_id "
            "FULL JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "multi_order_by_limit_three",
        "tables": ["orders", "products", "customers"],
        "min_sources": 3,
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS cust "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 6"
        ),
    },
]
