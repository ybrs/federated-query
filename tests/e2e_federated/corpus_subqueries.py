"""Subquery corpus: EXISTS/IN/quantified/scalar, correlated across sources.

Case names are prefixed ``subq_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage: EXISTS/NOT EXISTS correlated on one and on multiple outer columns;
IN/NOT IN uncorrelated and correlated, with a NULL-bearing inner side for the
three-valued NOT IN case; quantified comparisons (> ANY, = ANY, < ALL, <> ALL)
with empty and NULL-bearing inner sets, uncorrelated and correlated; scalar
subqueries uncorrelated and correlated (MAX/MIN/COUNT/AVG/top-1-via-LIMIT),
including COUNT over an empty correlation yielding 0; a correlated scalar
against an aggregate over a JOIN of two inner tables; subqueries nested two
levels deep and inside a derived table; OR-of-EXISTS and EXISTS-OR-IN
disjunctions; a correlated comparison through a non-equi predicate, including
one that forces the Neumann-Kemper dependent-join path; duplicate inner keys
under EXISTS/IN (no outer fan-out); and two expect_error guards.
"""

_T_ORDER_FLAG = {
    "ddl": (
        "CREATE TABLE t_order_flag ("
        " customer_id INTEGER, product_id INTEGER, flag VARCHAR)"
    ),
    "inserts": [
        "INSERT INTO t_order_flag VALUES"
        " (1, 101, 'A'),"
        " (2, 102, 'B'),"
        " (3, 999, 'C'),"
        " (9, 101, 'D'),"
        " (1, 999, 'E')"
    ],
}

CASES = [
    {
        "name": "subq_exists_correlated_single_col",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.product_id = p.product_id)"
        ),
    },
    {
        "name": "subq_not_exists_correlated_single_col",
        "tables": ["dim_item", "fact_sales"],
        "query": (
            "SELECT di.item_key, di.item_name FROM {dim_item} di "
            "WHERE NOT EXISTS (SELECT 1 FROM {fact_sales} f "
            "WHERE f.item_key = di.item_key AND f.amount > 250)"
        ),
    },
    {
        "name": "subq_exists_correlated_multi_col",
        "tables": ["orders"],
        "extra_tables": {"t_order_flag": _T_ORDER_FLAG},
        "query": (
            "SELECT o.order_id, o.customer_id, o.product_id FROM {orders} o "
            "WHERE EXISTS (SELECT 1 FROM {t_order_flag} fl "
            "WHERE fl.customer_id = o.customer_id "
            "AND fl.product_id = o.product_id)"
        ),
    },
    {
        "name": "subq_not_exists_correlated_multi_col",
        "tables": ["orders"],
        "extra_tables": {"t_order_flag": _T_ORDER_FLAG},
        "query": (
            "SELECT o.order_id, o.customer_id, o.product_id FROM {orders} o "
            "WHERE NOT EXISTS (SELECT 1 FROM {t_order_flag} fl "
            "WHERE fl.customer_id = o.customer_id "
            "AND fl.product_id = o.product_id)"
        ),
    },
    {
        "name": "subq_exists_or_exists_same_key",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id AND o.status = 'processing') "
            "OR EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id AND o.status = 'shipped')"
        ),
    },
    {
        "name": "subq_exists_or_in_mixed",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id AND o.status = 'processing') "
            "OR c.customer_id IN "
            "(SELECT customer_id FROM {orders} WHERE status = 'cancelled')"
        ),
    },
    {
        "name": "subq_or_not_exists_exists_same_key",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE NOT EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id AND o.status = 'cancelled') "
            "OR EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id AND o.status = 'processing')"
        ),
    },
    {
        "name": "subq_exists_join_of_two_inner_tables",
        "tables": ["customers", "orders", "products"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE EXISTS (SELECT 1 FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "WHERE o.customer_id = c.customer_id AND p.category = 'electronics')"
        ),
    },
    {
        "name": "subq_exists_multi_outer_tables",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "WHERE EXISTS (SELECT 1 FROM {customers} c "
            "WHERE c.customer_id = o.customer_id AND c.city = 'London')"
        ),
    },
    {
        "name": "subq_in_uncorrelated",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.customer_id IN "
            "(SELECT customer_id FROM {customers} WHERE segment = 'enterprise')"
        ),
    },
    {
        "name": "subq_not_in_uncorrelated_no_nulls",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE c.customer_id NOT IN "
            "(SELECT customer_id FROM {orders} WHERE status = 'cancelled')"
        ),
    },
    {
        "name": "subq_not_in_uncorrelated_products_orders",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE p.product_id NOT IN "
            "(SELECT product_id FROM {orders} WHERE status = 'shipped')"
        ),
    },
    {
        "name": "subq_in_inner_has_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.val FROM {t_null_a} a "
            "WHERE a.k IN (SELECT k FROM {t_null_b})"
        ),
    },
    {
        "name": "subq_not_in_inner_has_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.val FROM {t_null_a} a "
            "WHERE a.k NOT IN (SELECT k FROM {t_null_b})"
        ),
    },
    {
        "name": "subq_in_empty_inner",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.order_id IN (SELECT id FROM {t_empty})"
        ),
    },
    {
        "name": "subq_not_in_empty_inner",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.order_id NOT IN (SELECT id FROM {t_empty})"
        ),
    },
    {
        "name": "subq_correlated_in",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, o.customer_id FROM {orders} o "
            "WHERE o.customer_id IN (SELECT c.customer_id FROM {customers} c "
            "WHERE c.customer_id = o.customer_id AND c.segment = 'enterprise')"
        ),
    },
    {
        "name": "subq_correlated_not_in",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, o.customer_id FROM {orders} o "
            "WHERE o.customer_id NOT IN (SELECT c.customer_id FROM {customers} c "
            "WHERE c.customer_id = o.customer_id AND c.segment = 'smb')"
        ),
    },
    {
        "name": "subq_in_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k, a.tag FROM {t_dup_a} a "
            "WHERE a.k IN (SELECT k FROM {t_dup_b})"
        ),
    },
    {
        "name": "subq_exists_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k, a.tag FROM {t_dup_a} a "
            "WHERE EXISTS (SELECT 1 FROM {t_dup_b} b WHERE b.k = a.k)"
        ),
    },
    {
        "name": "subq_not_exists_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k, a.tag FROM {t_dup_a} a "
            "WHERE NOT EXISTS (SELECT 1 FROM {t_dup_b} b WHERE b.k = a.k)"
        ),
    },
    {
        "name": "subq_exists_lookup_status",
        "tables": ["orders", "t_lookup"],
        "query": (
            "SELECT o.order_id, o.status FROM {orders} o "
            "WHERE EXISTS (SELECT 1 FROM {t_lookup} l "
            "WHERE l.code = o.status AND l.description = 'in progress')"
        ),
    },
    {
        "name": "subq_not_exists_lookup_status",
        "tables": ["orders", "t_lookup"],
        "query": (
            "SELECT o.order_id, o.status FROM {orders} o "
            "WHERE NOT EXISTS (SELECT 1 FROM {t_lookup} l "
            "WHERE l.code = o.status AND l.description = 'called off')"
        ),
    },
    {
        "name": "subq_in_correlated_dim_fact",
        "tables": ["dim_item", "fact_sales"],
        "query": (
            "SELECT di.item_key, di.item_name FROM {dim_item} di "
            "WHERE di.item_key IN (SELECT f.item_key FROM {fact_sales} f "
            "WHERE f.item_key = di.item_key AND f.amount > 50)"
        ),
    },
    {
        "name": "subq_gt_any_uncorrelated",
        "tables": ["orders", "products"],
        "query": (
            "SELECT o.order_id, o.price FROM {orders} o "
            "WHERE o.price > ANY (SELECT unit_price FROM {products})"
        ),
    },
    {
        "name": "subq_eq_any_uncorrelated",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE c.customer_id = ANY (SELECT customer_id FROM {orders})"
        ),
    },
    {
        "name": "subq_lt_all_uncorrelated",
        "tables": ["orders", "products"],
        "query": (
            "SELECT o.order_id, o.price FROM {orders} o "
            "WHERE o.price < ALL "
            "(SELECT unit_price FROM {products} WHERE category = 'electronics')"
        ),
    },
    {
        "name": "subq_ne_all_uncorrelated",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE c.customer_id <> ALL "
            "(SELECT customer_id FROM {orders} WHERE status = 'cancelled')"
        ),
    },
    {
        "name": "subq_gt_any_empty_inner",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.quantity > ANY (SELECT id FROM {t_empty})"
        ),
    },
    {
        "name": "subq_lt_all_empty_inner",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.quantity < ALL (SELECT id FROM {t_empty})"
        ),
    },
    {
        "name": "subq_all_uncorrelated_filtered_empty",
        "tables": ["products"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE p.unit_price > ALL (SELECT unit_price FROM {products} "
            "WHERE category = 'nonexistent_category')"
        ),
    },
    {
        "name": "subq_eq_any_null_bearing_inner",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.k FROM {t_null_a} a "
            "WHERE a.k = ANY (SELECT k FROM {t_null_b})"
        ),
    },
    {
        "name": "subq_ne_all_null_bearing_inner",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.k FROM {t_null_a} a "
            "WHERE a.k <> ALL (SELECT k FROM {t_null_b})"
        ),
    },
    {
        "name": "subq_gt_any_correlated",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE p.unit_price > ANY (SELECT o.price FROM {orders} o "
            "WHERE o.product_id = p.product_id)"
        ),
    },
    {
        "name": "subq_lt_all_correlated",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE p.unit_price < ALL (SELECT o.price FROM {orders} o "
            "WHERE o.product_id = p.product_id)"
        ),
    },
    {
        "name": "subq_eq_any_correlated",
        "tables": ["dim_item", "fact_sales"],
        "query": (
            "SELECT di.item_key, di.item_name FROM {dim_item} di "
            "WHERE di.item_key = ANY (SELECT f.item_key FROM {fact_sales} f "
            "WHERE f.item_key = di.item_key AND f.amount > 50)"
        ),
    },
    {
        "name": "subq_scalar_uncorrelated_where",
        "tables": ["orders", "products"],
        "query": (
            "SELECT o.order_id, o.price FROM {orders} o "
            "WHERE o.price > "
            "(SELECT CAST(AVG(unit_price) AS DECIMAL(10,2)) FROM {products})"
        ),
    },
    {
        "name": "subq_scalar_uncorrelated_select_list",
        "tables": ["orders", "products"],
        "query": (
            "SELECT o.order_id, (SELECT MAX(unit_price) FROM {products}) "
            "AS max_price FROM {orders} o"
        ),
    },
    {
        "name": "subq_scalar_correlated_max_select_list",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, "
            "(SELECT MAX(o.price) FROM {orders} o "
            "WHERE o.customer_id = c.customer_id) AS max_price "
            "FROM {customers} c"
        ),
    },
    {
        "name": "subq_scalar_correlated_min_select_list",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, "
            "(SELECT MIN(o.price) FROM {orders} o "
            "WHERE o.product_id = p.product_id) AS min_order_price "
            "FROM {products} p"
        ),
    },
    {
        "name": "subq_scalar_correlated_count_select_list_empty_is_zero",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, "
            "(SELECT COUNT(*) FROM {orders} o "
            "WHERE o.customer_id = c.customer_id) AS n_orders "
            "FROM {customers} c"
        ),
    },
    {
        "name": "subq_scalar_correlated_count_where",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE (SELECT COUNT(*) FROM {orders} o "
            "WHERE o.customer_id = c.customer_id) > 1"
        ),
    },
    {
        "name": "subq_scalar_correlated_avg_where",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE (SELECT CAST(AVG(o.price) AS DECIMAL(10,2)) FROM {orders} o "
            "WHERE o.customer_id = c.customer_id) > 50"
        ),
    },
    {
        "name": "subq_scalar_correlated_max_where_top1_per_group",
        "tables": ["orders"],
        "query": (
            "SELECT o.order_id, o.customer_id, o.price FROM {orders} o "
            "WHERE o.price = (SELECT MAX(o2.price) FROM {orders} o2 "
            "WHERE o2.customer_id = o.customer_id)"
        ),
    },
    {
        "name": "subq_scalar_correlated_limit1_top_price",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, "
            "(SELECT o.price FROM {orders} o "
            "WHERE o.customer_id = c.customer_id "
            "ORDER BY o.price DESC LIMIT 1) AS top_price "
            "FROM {customers} c"
        ),
    },
    {
        "name": "subq_scalar_correlated_join_of_two_inner_tables",
        "tables": ["customers", "orders", "products"],
        "query": (
            "SELECT c.customer_id, "
            "(SELECT MAX(p.unit_price) FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "WHERE o.customer_id = c.customer_id) AS max_item_price "
            "FROM {customers} c"
        ),
    },
    {
        "name": "subq_scalar_correlated_sum_join_of_two_inner_tables",
        "tables": ["dim_day", "fact_sales", "dim_item"],
        "query": (
            "SELECT dd.day_key, "
            "(SELECT SUM(f.amount) FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "WHERE f.day_key = dd.day_key AND di.dept = 'hardware') "
            "AS hardware_amount FROM {dim_day} dd"
        ),
    },
    {
        "name": "subq_scalar_correlated_count_over_join_star",
        "tables": ["dim_item", "fact_sales"],
        "query": (
            "SELECT di.item_key, di.item_name, "
            "(SELECT COUNT(*) FROM {fact_sales} f "
            "WHERE f.item_key = di.item_key AND f.amount > 100) AS n_big "
            "FROM {dim_item} di"
        ),
    },
    {
        "name": "subq_nested_two_levels_in",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.customer_id IN (SELECT c.customer_id FROM {customers} c "
            "WHERE c.segment = 'enterprise' AND c.customer_id NOT IN "
            "(SELECT customer_id FROM {orders} WHERE status = 'cancelled'))"
        ),
    },
    {
        # The innermost subquery's correlation to o skips over the middle
        # subquery's scope (a "skip-level" correlation); the decorrelator
        # raises on this shape rather than emit a wrong plan.
        "name": "subq_error_nested_skip_level_correlation",
        "tables": ["orders", "products"],
        "expect_error": "unsupported position",
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE o.price > (SELECT CAST(AVG(p.unit_price) AS DECIMAL(10,2)) "
            "FROM {products} p WHERE p.category = "
            "(SELECT category FROM {products} WHERE product_id = o.product_id))"
        ),
    },
    {
        "name": "subq_derived_table_scalar_count",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT sub.customer_id, sub.n_orders FROM "
            "(SELECT c.customer_id, "
            "(SELECT COUNT(*) FROM {orders} o "
            "WHERE o.customer_id = c.customer_id) AS n_orders "
            "FROM {customers} c) AS sub "
            "WHERE sub.n_orders > 1"
        ),
    },
    {
        "name": "subq_derived_table_exists_filter",
        "tables": ["dim_item", "fact_sales"],
        "query": (
            "SELECT d.item_key, d.dept FROM "
            "(SELECT di.item_key, di.dept FROM {dim_item} di "
            "WHERE EXISTS (SELECT 1 FROM {fact_sales} f "
            "WHERE f.item_key = di.item_key AND f.amount > 100)) AS d"
        ),
    },
    {
        "name": "subq_correlated_nonequi_comparison",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.product_id = p.product_id AND o.price > p.unit_price)"
        ),
    },
    {
        "name": "subq_correlated_nonequi_dependent_join",
        "tables": ["products", "orders"],
        "query": (
            "SELECT p.product_id, p.name FROM {products} p "
            "WHERE p.unit_price > "
            "(SELECT CAST(AVG(o.price) AS DECIMAL(10,2)) FROM {orders} o "
            "WHERE o.product_id <> p.product_id)"
        ),
    },
    {
        "name": "subq_error_correlated_nonexistent_column",
        "tables": ["orders", "customers"],
        "expect_error": "bogus_col",
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE EXISTS (SELECT 1 FROM {customers} c "
            "WHERE c.customer_id = o.customer_id AND o.bogus_col = 1)"
        ),
    },
    {
        "name": "subq_error_multicol_scalar_subquery",
        "tables": ["orders", "customers"],
        "expect_error": "exactly one column",
        "query": (
            "SELECT o.order_id, "
            "(SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE c.customer_id = o.customer_id) AS bad "
            "FROM {orders} o"
        ),
    },
]
