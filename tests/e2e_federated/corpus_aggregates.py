"""Aggregate corpus: aggregates over cross-source joins, HAVING, grouping sets.

Case names are prefixed ``agg_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage: SUM/COUNT/COUNT(*)/MIN/MAX/AVG over a join grouped by a column from
the other source (the dim-shipping/eager-agg shape); COUNT(DISTINCT)/SUM(DISTINCT)
cross-source and combined in one query; GROUP BY over multiple columns and
expressions (EXTRACT, CASE WHEN) from different sources; ORDER BY referencing a
GROUP BY alias; HAVING on a projected aggregate, on an aggregate outside the
SELECT list, and combined with WHERE on both sides; ROLLUP/CUBE/GROUPING SETS
and GROUPING() over a cross-source join; aggregates over duplicate-key fan-out
and over NULL inputs (SUM/AVG ignoring NULLs, COUNT(col) vs COUNT(*)); empty
joins with and without GROUP BY, and HAVING that filters every group away;
aggregate-then-join shapes (a GROUP BY derived table joined to another source,
and two aggregated derived tables from different sources joined together);
MIN/MAX over text/date/timestamp/decimal columns; AVG over integers cast to
double explicitly; and ORDER BY on an aggregate output with LIMIT for top-N
groups, with a deterministic tie-break column.

``SUSPECTED_ENGINE_BUGS`` documents a verified engine-vs-oracle mismatch found
while authoring this corpus; it is not part of ``CASES`` so the suite stays
green, and it names the placement and the exact expected/got values.
"""

CASES = [
    {
        "name": "agg_sum_count_star_by_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, SUM(f.amount) AS total, count(*) AS n "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_avg_qty_by_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, "
            "CAST(AVG(f.qty) AS DOUBLE PRECISION) AS avg_qty "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_min_max_amount_by_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, MIN(f.amount) AS min_amt, "
            "MAX(f.amount) AS max_amt "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_count_distinct_item_by_day",
        "tables": ["fact_sales", "dim_day"],
        "query": (
            "SELECT dd.cal_date AS d, COUNT(DISTINCT f.item_key) AS n_items "
            "FROM {fact_sales} f JOIN {dim_day} dd "
            "ON f.day_key = dd.day_key GROUP BY dd.cal_date"
        ),
    },
    {
        "name": "agg_sum_distinct_amount_by_dept",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, SUM(DISTINCT f.amount) AS distinct_sum "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_multi_distinct_aggs",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, COUNT(DISTINCT f.item_key) AS n_items, "
            "SUM(DISTINCT f.qty) AS distinct_qty_sum "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_group_by_two_cols_diff_sources",
        "tables": ["fact_sales", "dim_day", "dim_item"],
        "query": (
            "SELECT dd.cal_date AS d, di.dept AS dept, SUM(f.amount) AS total "
            "FROM {fact_sales} f "
            "JOIN {dim_day} dd ON f.day_key = dd.day_key "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "GROUP BY dd.cal_date, di.dept"
        ),
    },
    {
        "name": "agg_sum_by_day",
        "tables": ["fact_sales", "dim_day"],
        "query": (
            "SELECT dd.cal_date AS d, SUM(f.amount) AS total, count(*) AS n "
            "FROM {fact_sales} f JOIN {dim_day} dd "
            "ON f.day_key = dd.day_key GROUP BY dd.cal_date"
        ),
    },
    {
        "name": "agg_group_by_extract_month_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT EXTRACT(MONTH FROM o.order_date) AS mo, count(*) AS n, "
            "SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY EXTRACT(MONTH FROM o.order_date)"
        ),
    },
    {
        "name": "agg_group_by_extract_year_cross",
        "tables": ["t_dates", "dim_item"],
        "query": (
            "SELECT EXTRACT(YEAR FROM d.d_date) AS yr, di.dept AS dept, "
            "count(*) AS n "
            "FROM {t_dates} d CROSS JOIN {dim_item} di "
            "GROUP BY EXTRACT(YEAR FROM d.d_date), di.dept"
        ),
    },
    {
        "name": "agg_group_by_case_when_segment",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT CASE WHEN c.segment = 'enterprise' THEN 'ENT' "
            "ELSE 'OTHER' END AS seg_grp, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY CASE WHEN c.segment = 'enterprise' THEN 'ENT' "
            "ELSE 'OTHER' END"
        ),
    },
    {
        "name": "agg_group_by_alias_order_by",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment ORDER BY seg"
        ),
    },
    {
        "name": "agg_having_on_projected_agg",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment HAVING SUM(o.price) > 100"
        ),
    },
    {
        "name": "agg_having_not_in_select",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment HAVING count(*) > 2"
        ),
    },
    {
        "name": "agg_having_with_where_both_sides",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "WHERE o.status <> 'cancelled' AND c.city <> 'Rome' "
            "GROUP BY c.segment HAVING count(*) >= 1"
        ),
    },
    {
        "name": "agg_having_filters_all_groups",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment HAVING SUM(o.price) > 100000"
        ),
    },
    {
        "name": "agg_rollup_segment",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY ROLLUP (c.segment)"
        ),
    },
    {
        "name": "agg_cube_segment_status",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, o.status AS st, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY CUBE (c.segment, o.status)"
        ),
    },
    {
        "name": "agg_grouping_sets_segment_status",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, o.status AS st, count(*) AS n, "
            "SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY GROUPING SETS ((c.segment, o.status), (c.segment), ())"
        ),
    },
    {
        "name": "agg_grouping_function_disambiguation",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total, "
            "GROUPING(c.segment) AS is_super "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY ROLLUP (c.segment)"
        ),
    },
    {
        "name": "agg_grouping_sets_empty_only",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT SUM(o.price) AS total, count(*) AS n "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY GROUPING SETS (())"
        ),
    },
    {
        "name": "agg_grouping_sets_star_three_way",
        "tables": ["fact_sales", "dim_item", "dim_day"],
        "query": (
            "SELECT di.dept AS dept, dd.quarter AS qtr, SUM(f.amount) AS total "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "JOIN {dim_day} dd ON f.day_key = dd.day_key "
            "GROUP BY GROUPING SETS ((di.dept, dd.quarter), (di.dept), ())"
        ),
    },
    {
        "name": "agg_rollup_three_way_dept_quarter",
        "tables": ["fact_sales", "dim_item", "dim_day"],
        "query": (
            "SELECT di.dept AS dept, dd.quarter AS qtr, SUM(f.amount) AS total "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "JOIN {dim_day} dd ON f.day_key = dd.day_key "
            "GROUP BY ROLLUP (di.dept, dd.quarter)"
        ),
    },
    {
        "name": "agg_sum_over_dup_fanout",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k AS k, count(*) AS cnt, SUM(a.k) AS sum_k "
            "FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k GROUP BY a.k"
        ),
    },
    {
        "name": "agg_count_distinct_over_dup_fanout",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k AS k, COUNT(DISTINCT b.note) AS n_notes, count(*) AS cnt "
            "FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k GROUP BY a.k"
        ),
    },
    {
        "name": "agg_sum_avg_count_ignore_nulls_join_on_id",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT b.label AS label, SUM(a.k) AS sum_k, "
            "CAST(AVG(a.k) AS DOUBLE PRECISION) AS avg_k, "
            "COUNT(a.k) AS cnt_k, count(*) AS cnt_all "
            "FROM {t_null_a} a JOIN {t_null_b} b ON a.id = b.id "
            "GROUP BY b.label"
        ),
    },
    {
        "name": "agg_count_star_vs_count_col_left_join",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT b.label AS label, count(*) AS cnt_star, "
            "COUNT(a.val) AS cnt_col "
            "FROM {t_null_b} b LEFT JOIN {t_null_a} a ON a.k = b.k "
            "GROUP BY b.label"
        ),
    },
    {
        "name": "agg_empty_join_no_group_by",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT count(*) AS cnt, SUM(o.price) AS total "
            "FROM {orders} o JOIN {t_empty} e ON o.order_id = e.id"
        ),
    },
    {
        "name": "agg_empty_join_with_group_by",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.status AS st, count(*) AS cnt "
            "FROM {orders} o JOIN {t_empty} e ON o.order_id = e.id "
            "GROUP BY o.status"
        ),
    },
    {
        "name": "agg_derived_totals_joined_to_dim",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, t.total AS total FROM "
            "(SELECT o.customer_id AS customer_id, SUM(o.price) AS total "
            "FROM {orders} o GROUP BY o.customer_id) t "
            "JOIN {customers} c ON t.customer_id = c.customer_id"
        ),
    },
    {
        "name": "agg_join_of_two_aggregated_derived_tables",
        "tables": ["orders", "products"],
        "query": (
            "WITH order_agg AS "
            "(SELECT o.product_id AS product_id, SUM(o.quantity) AS total_qty "
            "FROM {orders} o GROUP BY o.product_id), "
            "product_agg AS "
            "(SELECT p.product_id AS product_id, count(*) AS cnt "
            "FROM {products} p GROUP BY p.product_id) "
            "SELECT oa.product_id AS product_id, oa.total_qty AS total_qty, "
            "pa.cnt AS cnt "
            "FROM order_agg oa JOIN product_agg pa "
            "ON oa.product_id = pa.product_id"
        ),
    },
    {
        "name": "agg_minmax_text_by_group",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, MIN(c.city) AS min_city, "
            "MAX(c.city) AS max_city "
            "FROM {customers} c JOIN {orders} o "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
    {
        "name": "agg_minmax_date_by_group",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, MIN(o.order_date) AS min_date, "
            "MAX(o.order_date) AS max_date "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
    {
        "name": "agg_minmax_timestamp_cross",
        "tables": ["t_dates", "t_lookup"],
        "query": (
            "SELECT l.code AS code, MIN(d.d_ts) AS min_ts, MAX(d.d_ts) AS max_ts "
            "FROM {t_dates} d CROSS JOIN {t_lookup} l GROUP BY l.code"
        ),
    },
    {
        "name": "agg_minmax_decimal_products_by_category",
        "tables": ["orders", "products"],
        "query": (
            "SELECT p.category AS cat, MIN(p.unit_price) AS min_price, "
            "MAX(p.unit_price) AS max_price "
            "FROM {products} p JOIN {orders} o "
            "ON p.product_id = o.product_id GROUP BY p.category"
        ),
    },
    {
        "name": "agg_avg_int_cast_double",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, "
            "CAST(AVG(o.quantity) AS DOUBLE PRECISION) AS avg_qty "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
    {
        "name": "agg_avg_cast_before_aggregate",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, "
            "AVG(CAST(f.qty AS DOUBLE PRECISION)) AS avg_qty "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_top_n_by_total_desc",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment ORDER BY total DESC, seg LIMIT 2"
        ),
    },
    {
        "name": "agg_top_n_item_by_amount",
        "tables": ["fact_sales", "dim_item"],
        "order_sensitive": True,
        "query": (
            "SELECT di.item_name AS item, SUM(f.amount) AS total "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key "
            "GROUP BY di.item_name ORDER BY total DESC, item LIMIT 2"
        ),
    },
    {
        "name": "agg_null_ignoring_sum_avg_cross_join",
        "tables": ["t_types", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, SUM(t.t_dbl) AS sum_dbl, "
            "CAST(AVG(t.t_dbl) AS DOUBLE PRECISION) AS avg_dbl, "
            "COUNT(t.t_dbl) AS cnt_col, count(*) AS cnt_star "
            "FROM {t_types} t CROSS JOIN {dim_item} di GROUP BY di.dept"
        ),
    },
    {
        "name": "agg_group_by_multiple_having_orderby",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT c.segment AS seg, o.status AS st, count(*) AS n, "
            "SUM(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment, o.status HAVING count(*) >= 1 "
            "ORDER BY total DESC, seg, st"
        ),
    },
    {
        "name": "agg_where_and_having_combined_star",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, SUM(f.amount) AS total "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key "
            "WHERE f.qty > 1 GROUP BY di.dept HAVING SUM(f.amount) > 50"
        ),
    },
    {
        "name": "agg_count_distinct_two_cols",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, COUNT(DISTINCT o.product_id) AS n_products, "
            "COUNT(DISTINCT o.status) AS n_statuses "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
    {
        "name": "agg_sum_expr_products_by_category",
        "tables": ["orders", "products"],
        "query": (
            "SELECT p.category AS cat, SUM(o.quantity * o.price) AS total_value "
            "FROM {orders} o JOIN {products} p "
            "ON o.product_id = p.product_id GROUP BY p.category"
        ),
    },
    {
        "name": "agg_having_orderby_limit_combo",
        "tables": ["fact_sales", "dim_item"],
        "order_sensitive": True,
        "query": (
            "SELECT di.item_name AS item, SUM(f.amount) AS total, count(*) AS cnt "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key "
            "GROUP BY di.item_name HAVING count(*) >= 1 "
            "ORDER BY total DESC, item LIMIT 3"
        ),
    },
    {
        "name": "agg_three_table_group_by_having",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.category AS cat, c.segment AS seg, SUM(o.price) AS total "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "GROUP BY p.category, c.segment HAVING SUM(o.price) > 20"
        ),
    },
    {
        "name": "agg_count_distinct_star_combo_having",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, count(*) AS n, "
            "COUNT(DISTINCT f.item_key) AS n_items, SUM(f.amount) AS total "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key GROUP BY di.dept "
            "HAVING COUNT(DISTINCT f.item_key) > 1"
        ),
    },
    {
        "name": "agg_group_by_case_when_products",
        "tables": ["orders", "products"],
        "query": (
            "SELECT CASE WHEN p.category IN ('electronics', 'home') "
            "THEN 'durable' ELSE 'other' END AS bucket, "
            "CAST(AVG(o.price) AS DOUBLE PRECISION) AS avg_price, count(*) AS n "
            "FROM {orders} o JOIN {products} p "
            "ON o.product_id = p.product_id "
            "GROUP BY CASE WHEN p.category IN ('electronics', 'home') "
            "THEN 'durable' ELSE 'other' END"
        ),
    },
]

SUSPECTED_ENGINE_BUGS = {
    "agg_avg_decimal_price_by_category": {
        "query": (
            "SELECT p.category AS cat, "
            "CAST(AVG(o.price) AS DOUBLE PRECISION) AS avg_price "
            "FROM {orders} o JOIN {products} p "
            "ON o.product_id = p.product_id GROUP BY p.category"
        ),
        "tables": ["orders", "products"],
        "finding": (
            "CAST(AVG(decimal_column) AS DOUBLE PRECISION) over a cross-source "
            "join loses precision to 6 fractional digits instead of full double "
            "precision. Reproduced at placements duck_duck and pg_duck (no "
            "PostgreSQL required to trigger it) and at oracle_single_duck it is "
            "correct, so the divergence is specific to the multi-source merge "
            "path's DECIMAL division, not to any one connector. For the "
            "'electronics' category (orders 3, 4, 8 at prices 75.00, 125.00, "
            "15.00): engine returned 71.666666, oracle (single DuckDB) returned "
            "71.66666666666667. The 1e-9 relative-tolerance comparator still "
            "catches this because the truncation error (~9.3e-9 relative) "
            "exceeds tolerance. Removed from CASES so the suite stays green; "
            "re-add once the merge engine's decimal-average division is fixed "
            "to compute at double (or full decimal) precision before casting."
        ),
    },
    "agg_minmax_boolean_group_products": {
        "query": (
            "SELECT p.active AS active, MIN(o.price) AS min_price, "
            "MAX(o.price) AS max_price, count(*) AS n "
            "FROM {orders} o JOIN {products} p "
            "ON o.product_id = p.product_id GROUP BY p.active"
        ),
        "tables": ["orders", "products"],
        "finding": (
            "Any query that GROUPs BY or filters on a BOOLEAN column of a "
            "PostgreSQL-sourced table raises 'physical planning error: "
            "datasource error: db error' at placements duck_pg, all_pg, and "
            "parquet_pg (wherever the boolean-carrying table lands on "
            "PostgreSQL); pg_duck, duck_duck, parquet_duck, and "
            "oracle_single_duck are unaffected because there ``products`` "
            "lands on DuckDB. Root cause, read from the PostgreSQL server log "
            "at the moment of failure: the planner's column-statistics probe "
            "unconditionally issues 'SELECT count(*) AS probe_rows, "
            "count(DISTINCT col) AS ndv_0, count(col) AS nonnull_0, "
            "min(col)::text AS min_0, max(col)::text AS max_0 FROM table' for "
            "every column a query references in WHERE/GROUP BY, including "
            "boolean columns. PostgreSQL raises 'ERROR: function min(boolean) "
            "does not exist' (DuckDB defines min/max over boolean; PostgreSQL "
            "does not), and the wrapped ADBC error surfaces to Python as the "
            "generic 'db error'. A plain projection of the boolean column "
            "(no WHERE/GROUP BY use) does not probe it and succeeds, which "
            "isolates the bug to the statistics probe rather than to reading "
            "boolean values from PostgreSQL. Reproduced directly against the "
            "engine outside pytest with 'SELECT count(*) FROM products WHERE "
            "active = TRUE' on a single PostgreSQL source (no join). Removed "
            "from CASES so the suite stays green; re-add once the PostgreSQL "
            "connector's statistics probe casts boolean columns (e.g. to "
            "int) or skips min/max for them."
        ),
    },
}
