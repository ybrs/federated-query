"""Window, expression, and negative corpus: windows over joins, CASE/CAST,
date and string functions, ORDER BY/LIMIT edges, and queries that must raise.

Case names are prefixed ``win_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.
"""

CASES = [
    # Windows over cross-source join results (orders/customers, fact/dim star).
    {
        "name": "win_row_number_partition_other_source",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "ROW_NUMBER() OVER (PARTITION BY c.segment ORDER BY o.order_id) AS rn "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_rank_partition_with_tiebreak",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "RANK() OVER (PARTITION BY c.segment ORDER BY o.price DESC, o.order_id) "
            "AS rnk FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_dense_rank_partition",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.city, "
            "DENSE_RANK() OVER (PARTITION BY c.city ORDER BY o.price DESC, o.order_id) "
            "AS drnk FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_running_sum_partition_order",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "SUM(o.price) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS running_total "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_running_count_explicit_frame",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, COUNT(*) OVER ("
            "PARTITION BY c.segment ORDER BY o.order_id "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_star_running_sum_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT f.sale_id, di.dept, "
            "SUM(f.amount) OVER (PARTITION BY di.dept ORDER BY f.sale_id) "
            "AS running_total "
            "FROM {fact_sales} f JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "win_star_row_number_day",
        "tables": ["fact_sales", "dim_day"],
        "query": (
            "SELECT f.sale_id, d.day_key, "
            "ROW_NUMBER() OVER (PARTITION BY d.day_key ORDER BY f.sale_id) AS rn "
            "FROM {fact_sales} f JOIN {dim_day} d ON f.day_key = d.day_key"
        ),
    },
    {
        "name": "win_rank_over_star_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT f.sale_id, di.dept, "
            "RANK() OVER (PARTITION BY di.dept ORDER BY f.amount DESC, f.sale_id) "
            "AS rnk "
            "FROM {fact_sales} f JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "win_partition_by_expression",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, ROW_NUMBER() OVER ("
            "PARTITION BY EXTRACT(MONTH FROM o.order_date) ORDER BY o.order_id) "
            "AS rn "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_partition_by_two_cols",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, c.city, ROW_NUMBER() OVER ("
            "PARTITION BY c.segment, c.city ORDER BY o.order_id) AS rn "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_sum_over_whole_partition_no_order",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "SUM(o.price) OVER (PARTITION BY c.segment) AS segment_total "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_multi_window_same_select",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, c.city, "
            "ROW_NUMBER() OVER (PARTITION BY c.segment ORDER BY o.order_id) AS rn, "
            "RANK() OVER (PARTITION BY c.city ORDER BY o.price DESC, o.order_id) "
            "AS rnk "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_named_window_clause",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "SUM(o.price) OVER w AS seg_total, COUNT(*) OVER w AS seg_n "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WINDOW w AS (PARTITION BY c.segment)"
        ),
    },
    {
        "name": "win_top_n_per_group",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT order_id, segment, price FROM ("
            "SELECT o.order_id AS order_id, c.segment AS segment, "
            "o.price AS price, ROW_NUMBER() OVER ("
            "PARTITION BY c.segment ORDER BY o.price DESC, o.order_id) AS rn "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
            ") ranked WHERE rn <= 2"
        ),
    },
    {
        "name": "win_top_n_per_group_star",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT sale_id, dept, amount FROM ("
            "SELECT f.sale_id AS sale_id, di.dept AS dept, f.amount AS amount, "
            "ROW_NUMBER() OVER (PARTITION BY di.dept ORDER BY f.amount DESC, f.sale_id) "
            "AS rn "
            "FROM {fact_sales} f JOIN {dim_item} di ON f.item_key = di.item_key"
            ") ranked WHERE rn = 1"
        ),
    },
    {
        "name": "win_cross_source_left_join_lateral",
        "tables": ["orders", "customers"],
        "min_sources": 2,
        "query": (
            "SELECT o.order_id, t.segment FROM {orders} o "
            "LEFT JOIN LATERAL ("
            "  SELECT c.segment FROM {customers} c "
            "  WHERE c.customer_id = o.customer_id ORDER BY c.segment LIMIT 1"
            ") t ON true"
        ),
    },
    # Offset and value window functions over cross-source join results, each
    # with a deterministic partition + ORDER BY (o.order_id is unique).
    {
        "name": "win_lag_price_prior_row",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "LAG(o.price, 1) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS prev_price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_lead_price_next_row_default",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "LEAD(o.price, 1, 0) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS next_price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_first_value_price_in_partition",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "FIRST_VALUE(o.price) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS first_price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_last_value_price_running_frame",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "LAST_VALUE(o.price) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS last_price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_ntile_two_buckets",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "NTILE(2) OVER (PARTITION BY c.segment ORDER BY o.order_id) AS bucket "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    # Expressions over cross-source joins: CASE, COALESCE/NULLIF, CASTs,
    # string functions, date functions, decimal arithmetic.
    {
        "name": "win_case_searched",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, "
            "CASE WHEN o.status = 'shipped' THEN 'S' "
            "WHEN o.status = 'processing' THEN 'P' ELSE 'O' END AS status_code "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_case_simple",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, "
            "CASE o.status WHEN 'shipped' THEN 1 WHEN 'processing' THEN 2 ELSE 0 END "
            "AS status_rank "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_case_with_null_else",
        "tables": ["customers"],
        "query": (
            "SELECT c.customer_id, "
            "CASE WHEN c.segment = 'enterprise' THEN c.city END AS ent_city "
            "FROM {customers} c"
        ),
    },
    {
        "name": "win_coalesce_null_join",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.val, COALESCE(b.label, 'none') AS label_or_default "
            "FROM {t_null_a} a LEFT JOIN {t_null_b} b ON a.id = b.id"
        ),
    },
    {
        "name": "win_coalesce_three_args",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, COALESCE(a.k, b.k, -1) AS k_or_default "
            "FROM {t_null_a} a LEFT JOIN {t_null_b} b ON a.id = b.id"
        ),
    },
    {
        "name": "win_nullif_quantity",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, NULLIF(o.quantity, 3) AS qty_or_null "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_cast_int_to_bigint",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, CAST(o.order_id AS BIGINT) AS order_id_big, "
            "CAST(o.quantity AS BIGINT) AS qty_big "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_cast_decimal_to_varchar",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, CAST(o.price AS VARCHAR) AS price_text "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_cast_int_to_decimal",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, CAST(o.quantity AS DECIMAL(10, 2)) AS qty_dec "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_cast_date_to_varchar",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, CAST(o.order_date AS VARCHAR) AS order_date_text "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_cast_varchar_int_roundtrip",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, "
            "CAST(CAST(o.order_id AS VARCHAR) AS INTEGER) AS oid_roundtrip "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_upper_lower_names",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, UPPER(c.name) AS name_upper, "
            "LOWER(c.city) AS city_lower "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_substr_and_length",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, SUBSTR(c.name, 1, 3) AS name_prefix, "
            "LENGTH(c.name) AS name_len "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_trim_text_edges",
        "tables": ["t_text"],
        "query": "SELECT s_id, TRIM(s) AS trimmed, LENGTH(s) AS len FROM {t_text}",
    },
    {
        "name": "win_concat_function",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, CONCAT(c.name, '-', c.city) AS name_city "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_like_percent",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT c.customer_id, c.name "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE c.name LIKE 'A%'"
        ),
    },
    {
        "name": "win_like_underscore",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT c.customer_id, c.city "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE c.city LIKE '_ondon'"
        ),
    },
    {
        "name": "win_extract_year_month",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, EXTRACT(YEAR FROM o.order_date) AS yr, "
            "EXTRACT(MONTH FROM o.order_date) AS mo "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_date_comparison",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.order_date > DATE '2024-03-01'"
        ),
    },
    {
        "name": "win_date_trunc_month",
        "tables": ["t_dates"],
        "query": "SELECT d_id, DATE_TRUNC('month', d_ts) AS month_start FROM {t_dates}",
    },
    {
        "name": "win_arithmetic_decimal_exact",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, o.price * o.quantity AS line_total, "
            "o.price + o.quantity AS mixed_sum "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    # ORDER BY edges: alias, ordinal, expression not in SELECT, NULLS
    # FIRST/LAST, mixed multi-key, LIMIT/OFFSET combinations.
    {
        "name": "win_order_by_alias",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, o.price AS amt "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY amt DESC, o.order_id"
        ),
    },
    {
        "name": "win_order_by_ordinal",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, c.segment "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY 2, 1"
        ),
    },
    {
        "name": "win_order_by_expr_not_in_select",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, o.status "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY LENGTH(c.name) DESC, o.order_id"
        ),
    },
    {
        "name": "win_order_by_case_expression",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, o.status "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY CASE WHEN o.status = 'shipped' THEN 0 ELSE 1 END, "
            "o.order_id"
        ),
    },
    {
        "name": "win_order_by_nulls_first",
        "tables": ["t_null_a"],
        "order_sensitive": True,
        "query": "SELECT id, k FROM {t_null_a} ORDER BY k NULLS FIRST, id",
    },
    {
        "name": "win_order_by_nulls_last",
        "tables": ["t_null_a"],
        "order_sensitive": True,
        "query": "SELECT id, k FROM {t_null_a} ORDER BY k DESC NULLS LAST, id",
    },
    {
        "name": "win_order_by_multi_key_mixed",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, c.segment, o.price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY c.segment ASC, o.price DESC, o.order_id ASC"
        ),
    },
    {
        "name": "win_limit_offset_combo",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 3 OFFSET 2"
        ),
    },
    {
        "name": "win_offset_without_limit",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id OFFSET 5"
        ),
    },
    {
        "name": "win_limit_zero",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 0"
        ),
    },
    # SELECT DISTINCT over a cross-source join, and DISTINCT with NULLs.
    {
        "name": "win_distinct_cross_source",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT c.segment "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_distinct_multi_col",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT c.segment, o.status "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_distinct_star_pair",
        "tables": ["fact_sales", "dim_day"],
        "query": (
            "SELECT DISTINCT d.yr, d.quarter "
            "FROM {fact_sales} f JOIN {dim_day} d ON f.day_key = d.day_key"
        ),
    },
    {
        "name": "win_distinct_with_nulls",
        "tables": ["t_null_a"],
        "query": "SELECT DISTINCT k FROM {t_null_a}",
    },
    # Negative cases: each must raise in every placement that reaches it.
    {
        "name": "win_unknown_qualifier_raises",
        "tables": ["orders", "customers"],
        "expect_error": "not found in scope",
        "query": (
            "SELECT ghost.name FROM {orders} o "
            "JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "win_unknown_column_raises",
        "tables": ["orders"],
        "expect_error": "not found in table",
        "query": "SELECT o.nonexistent_col FROM {orders} o",
    },
    {
        "name": "win_ambiguous_column_raises",
        "tables": ["orders", "products"],
        "expect_error": "ambiguous",
        "query": (
            "SELECT product_id FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id"
        ),
    },
    {
        "name": "win_group_by_non_aggregated_raises",
        "tables": ["orders"],
        "expect_error": "must appear in the GROUP BY clause",
        "query": (
            "SELECT o.customer_id, o.status, count(*) AS n "
            "FROM {orders} o GROUP BY o.customer_id"
        ),
    },
    {
        "name": "win_window_in_where_raises",
        "tables": ["orders"],
        "expect_error": "not allowed in WHERE",
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE ROW_NUMBER() OVER (ORDER BY o.order_id) > 1"
        ),
    },
    {
        "name": "win_running_avg_partition_order",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "AVG(o.price) OVER (PARTITION BY c.segment ORDER BY o.order_id) "
            "AS running_avg "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
]

# LAG, LEAD, FIRST_VALUE, LAST_VALUE, and NTILE are supported window functions
# (win_lag_price_prior_row, win_lead_price_next_row_default,
# win_first_value_price_in_partition, win_last_value_price_running_frame,
# win_ntile_two_buckets above); they run wherever ROW_NUMBER does. The `||`
# string-concatenation operator lowers to the `||` execution operator and runs
# in every placement, cross-source included; CONCAT(...) remains available and
# is exercised by win_concat_function.
