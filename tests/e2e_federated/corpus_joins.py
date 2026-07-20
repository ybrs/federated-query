"""Join-shape corpus: join types, chains, self-joins, non-equi, star shapes.

Case names are prefixed ``join_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage: INNER/LEFT/RIGHT/FULL/CROSS with both operand orders, USING clauses,
comma-joins, multi-column keys, equality mixed with inequality, non-equi and
range joins, self-joins (alone and alongside a second source), 3/4/5-table
chains, a star and a snowflake (the latter via an inline dimension-of-a-
dimension table), joins on expressions (LOWER/UPPER, arithmetic, CAST),
anti-join shape, filters before/after the join, projections dropping join
keys, and duplicate output column names disambiguated via aliases.

A ``tables`` list sometimes names a table beyond what its query touches (for
instance a two-table join declaring the full orders/products/customers trio).
This is deliberate: the harness's per-(tables, placement) environment cache is
keyed on the declared table set, so piggybacking on a set another case (or the
sanity module) already seeded avoids opening yet another PostgreSQL schema and
connection for every placement - PostgreSQL's connection ceiling is shared
across the whole session.
"""

CASES = [
    {
        "name": "join_inner_orders_customers",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, o.status, c.name AS cust_name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_inner_customers_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name AS cust_name, o.order_id "
            "FROM {customers} c JOIN {orders} o "
            "ON c.customer_id = o.customer_id"
        ),
    },
    {
        "name": "join_left_orders_customers",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o LEFT JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_left_customers_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.customer_id, c.city, o.order_id "
            "FROM {customers} c LEFT JOIN {orders} o "
            "ON c.customer_id = o.customer_id"
        ),
    },
    {
        "name": "join_right_orders_customers",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o RIGHT JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_right_customers_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name, o.order_id "
            "FROM {customers} c RIGHT JOIN {orders} o "
            "ON c.customer_id = o.customer_id"
        ),
    },
    {
        "name": "join_full_orders_customers",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o FULL JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_full_customers_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.name, o.order_id "
            "FROM {customers} c FULL JOIN {orders} o "
            "ON c.customer_id = o.customer_id"
        ),
    },
    {
        "name": "join_using_customer_id",
        "tables": ["orders", "customers"],
        "min_sources": 2,
        "expect_error": "cross-source NATURAL/USING join is not supported",
        "query": (
            "SELECT order_id, name FROM {orders} "
            "JOIN {customers} USING (customer_id)"
        ),
    },
    {
        "name": "join_self_plus_second_source",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o1.order_id AS order_a, o2.order_id AS order_b, c.name "
            "FROM {orders} o1 "
            "JOIN {orders} o2 "
            "ON o1.customer_id = o2.customer_id AND o1.order_id < o2.order_id "
            "JOIN {customers} c ON o1.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_noneq_less_than",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.customer_id "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id < c.customer_id"
        ),
    },
    {
        "name": "join_eq_and_inequality_price",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id AND o.price > 20"
        ),
    },
    {
        "name": "join_expr_plus_one",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.customer_id "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id + 1 = c.customer_id"
        ),
    },
    {
        "name": "join_comma_style_basic",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o, {customers} c "
            "WHERE o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_anti_customers_no_orders",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.customer_id, c.name "
            "FROM {customers} c LEFT JOIN {orders} o "
            "ON c.customer_id = o.customer_id "
            "WHERE o.order_id IS NULL"
        ),
    },
    {
        "name": "join_filter_both_sides_before",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "WHERE o.status = 'processing' AND c.segment = 'enterprise'"
        ),
    },
    {
        "name": "join_filter_after_via_subquery",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT sub.order_id, sub.cust_name FROM ("
            "SELECT o.order_id, o.price, c.name AS cust_name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
            ") sub WHERE sub.price > 50"
        ),
    },
    {
        "name": "join_projection_drops_keys",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.status, c.city "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_filter_left_only",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "WHERE o.quantity >= 5"
        ),
    },
    {
        "name": "join_filter_right_only",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "WHERE c.city <> 'Paris'"
        ),
    },
    {
        "name": "join_self_orders_same_customer",
        "tables": ["orders"],
        "query": (
            "SELECT o1.order_id AS order_a, o2.order_id AS order_b "
            "FROM {orders} o1 JOIN {orders} o2 "
            "ON o1.customer_id = o2.customer_id AND o1.order_id < o2.order_id"
        ),
    },
    {
        "name": "join_multi_col_key_self_orders",
        "tables": ["orders"],
        "query": (
            "SELECT o1.order_id AS order_a, o2.order_id AS order_b "
            "FROM {orders} o1 JOIN {orders} o2 "
            "ON o1.customer_id = o2.customer_id "
            "AND o1.status = o2.status "
            "AND o1.order_id < o2.order_id"
        ),
    },
    {
        "name": "join_self_three_way",
        "tables": ["orders"],
        "query": (
            "SELECT o1.order_id AS o1_id, o2.order_id AS o2_id, "
            "o3.order_id AS o3_id "
            "FROM {orders} o1 "
            "JOIN {orders} o2 "
            "ON o1.customer_id = o2.customer_id AND o1.order_id < o2.order_id "
            "JOIN {orders} o3 "
            "ON o2.customer_id = o3.customer_id AND o2.order_id < o3.order_id"
        ),
    },
    {
        "name": "join_self_customers_same_segment",
        "tables": ["customers"],
        "query": (
            "SELECT c1.customer_id AS c1_id, c2.customer_id AS c2_id, c1.segment "
            "FROM {customers} c1 JOIN {customers} c2 "
            "ON c1.segment = c2.segment AND c1.customer_id < c2.customer_id"
        ),
    },
    {
        "name": "join_duplicate_name_self_join",
        "tables": ["customers"],
        "query": (
            "SELECT c1.name AS name1, c2.name AS name2, "
            "c1.city AS city1, c2.city AS city2 "
            "FROM {customers} c1 JOIN {customers} c2 "
            "ON c1.segment = c2.segment AND c1.customer_id <> c2.customer_id"
        ),
    },
    {
        "name": "join_noneq_between",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name "
            "FROM {orders} o JOIN {products} p "
            "ON o.price BETWEEN p.unit_price - 5 AND p.unit_price + 5"
        ),
    },
    {
        "name": "join_noneq_between_swapped",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name, o.order_id "
            "FROM {products} p JOIN {orders} o "
            "ON o.price BETWEEN p.unit_price - 5 AND p.unit_price + 5"
        ),
    },
    {
        "name": "join_noneq_range_two_inequalities",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name "
            "FROM {orders} o JOIN {products} p "
            "ON p.unit_price - 5 <= o.price AND o.price <= p.unit_price + 5"
        ),
    },
    {
        "name": "join_using_product_id",
        "tables": ["orders", "products"],
        "min_sources": 2,
        "expect_error": "cross-source NATURAL/USING join is not supported",
        "query": (
            "SELECT order_id, name FROM {orders} " "JOIN {products} USING (product_id)"
        ),
    },
    {
        "name": "join_anti_products_never_ordered",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.product_id, p.name "
            "FROM {products} p LEFT JOIN {orders} o "
            "ON p.product_id = o.product_id "
            "WHERE o.order_id IS NULL"
        ),
    },
    {
        "name": "join_expr_cast_varchar",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name "
            "FROM {orders} o JOIN {products} p "
            "ON CAST(o.product_id AS VARCHAR) = CAST(p.product_id AS VARCHAR)"
        ),
    },
    {
        "name": "join_comma_style_filtered",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name, o.order_id "
            "FROM {products} p, {orders} o "
            "WHERE p.product_id = o.product_id AND p.category = 'electronics'"
        ),
    },
    {
        "name": "join_right_join_using",
        "tables": ["orders", "products"],
        "min_sources": 2,
        "expect_error": "cross-source NATURAL/USING join is not supported",
        "query": (
            "SELECT order_id, name FROM {products} "
            "RIGHT JOIN {orders} USING (product_id)"
        ),
    },
    {
        "name": "join_full_join_using",
        "tables": ["orders", "products"],
        "min_sources": 2,
        "expect_error": "cross-source NATURAL/USING join is not supported",
        "query": (
            "SELECT order_id, name FROM {orders} "
            "FULL JOIN {products} USING (product_id)"
        ),
    },
    {
        "name": "join_cross_products_customers",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name, c.name AS cust_name "
            "FROM {products} p CROSS JOIN {customers} c"
        ),
    },
    {
        "name": "join_cross_filtered",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name, c.name AS cust_name "
            "FROM {products} p, {customers} c "
            "WHERE p.active = TRUE AND c.segment = 'enterprise'"
        ),
    },
    {
        "name": "join_cross_lookup_dimitem",
        "tables": ["t_lookup", "dim_item"],
        "query": (
            "SELECT l.code, di.item_name "
            "FROM {t_lookup} l CROSS JOIN {dim_item} di "
            "WHERE di.dept = 'hardware'"
        ),
    },
    {
        "name": "join_expr_lower_text_key",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "query": (
            "SELECT o.order_id, l.description "
            "FROM {orders} o JOIN {t_lookup} l "
            "ON lower(o.status) = lower(l.code)"
        ),
    },
    {
        "name": "join_expr_upper_text_key",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "query": (
            "SELECT o.order_id, l.description "
            "FROM {orders} o JOIN {t_lookup} l "
            "ON upper(o.status) = upper(l.code)"
        ),
    },
    {
        "name": "join_three_table_chain_left",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS customer "
            "FROM {orders} o "
            "LEFT JOIN {products} p ON o.product_id = p.product_id "
            "LEFT JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_duplicate_name_via_alias",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name AS product_name, c.name AS customer_name, o.order_id "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "join_filter_and_projection_combo",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.category, c.segment "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.quantity > 1 AND p.unit_price > 50"
        ),
    },
    {
        "name": "join_four_table_chain",
        "tables": ["orders", "products", "customers", "t_lookup"],
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS customer, "
            "l.description "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {t_lookup} l ON o.status = l.code"
        ),
    },
    {
        "name": "join_five_table_chain",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS customer, "
            "di.item_name "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "JOIN {fact_sales} f ON f.item_key = o.product_id - 90 "
            "JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "join_star_fact_day_item",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT d.cal_date, di.item_name, f.amount "
            "FROM {fact_sales} f "
            "JOIN {dim_day} d ON f.day_key = d.day_key "
            "JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "join_eq_and_inequality_qty",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT f.sale_id, di.item_name "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key AND f.qty >= 2"
        ),
    },
    {
        "name": "join_expr_cast_bigint",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT f.sale_id, d.cal_date "
            "FROM {fact_sales} f JOIN {dim_day} d "
            "ON CAST(f.day_key AS BIGINT) = CAST(d.day_key AS BIGINT)"
        ),
    },
    {
        "name": "join_using_item_key",
        "tables": ["fact_sales", "dim_item"],
        "min_sources": 2,
        "expect_error": "cross-source NATURAL/USING join is not supported",
        "query": (
            "SELECT sale_id, item_name FROM {fact_sales} "
            "JOIN {dim_item} USING (item_key)"
        ),
    },
    {
        "name": "join_star_left_chain",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT f.sale_id, d.cal_date, di.item_name "
            "FROM {fact_sales} f "
            "LEFT JOIN {dim_day} d ON f.day_key = d.day_key "
            "LEFT JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "join_left_cast_daykey",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT f.sale_id, d.cal_date "
            "FROM {fact_sales} f LEFT JOIN {dim_day} d "
            "ON CAST(f.day_key AS BIGINT) = CAST(d.day_key AS BIGINT)"
        ),
    },
    {
        "name": "join_star_full_chain",
        "tables": [
            "orders",
            "products",
            "customers",
            "fact_sales",
            "dim_item",
            "dim_day",
        ],
        "query": (
            "SELECT f.sale_id, d.cal_date, di.item_name "
            "FROM {fact_sales} f "
            "FULL JOIN {dim_day} d ON f.day_key = d.day_key "
            "FULL JOIN {dim_item} di ON f.item_key = di.item_key"
        ),
    },
    {
        "name": "join_snowflake_dept",
        "tables": ["fact_sales", "dim_item"],
        "extra_tables": {
            "dim_dept": {
                "ddl": ("CREATE TABLE dim_dept (dept VARCHAR, region VARCHAR)"),
                "inserts": [
                    "INSERT INTO dim_dept VALUES"
                    " ('hardware', 'east'),"
                    " ('media', 'west'),"
                    " ('toys', 'north')"
                ],
            }
        },
        "query": (
            "SELECT f.sale_id, di.item_name, dd.region "
            "FROM {fact_sales} f "
            "JOIN {dim_item} di ON f.item_key = di.item_key "
            "JOIN {dim_dept} dd ON di.dept = dd.dept"
        ),
    },
    {
        "name": "join_snowflake_dept_left",
        "tables": ["fact_sales", "dim_item"],
        "extra_tables": {
            "dim_dept": {
                "ddl": ("CREATE TABLE dim_dept (dept VARCHAR, region VARCHAR)"),
                "inserts": [
                    "INSERT INTO dim_dept VALUES"
                    " ('hardware', 'east'),"
                    " ('media', 'west'),"
                    " ('toys', 'north')"
                ],
            }
        },
        "query": (
            "SELECT dd.dept, dd.region, di.item_name "
            "FROM {dim_dept} dd LEFT JOIN {dim_item} di ON dd.dept = di.dept"
        ),
    },
    {
        "name": "join_snowflake_dept_right",
        "tables": ["fact_sales", "dim_item"],
        "extra_tables": {
            "dim_dept": {
                "ddl": ("CREATE TABLE dim_dept (dept VARCHAR, region VARCHAR)"),
                "inserts": [
                    "INSERT INTO dim_dept VALUES"
                    " ('hardware', 'east'),"
                    " ('media', 'west'),"
                    " ('toys', 'north')"
                ],
            }
        },
        "query": (
            "SELECT di.item_name, dd.region "
            "FROM {dim_item} di RIGHT JOIN {dim_dept} dd ON di.dept = dd.dept"
        ),
    },
    {
        "name": "join_multi_col_key_null_tables",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.val, b.label "
            "FROM {t_null_a} a JOIN {t_null_b} b "
            "ON a.id = b.id AND a.k = b.k"
        ),
    },
    {
        "name": "join_right_join_multi_col",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.val, b.label "
            "FROM {t_null_a} a RIGHT JOIN {t_null_b} b "
            "ON a.id = b.id AND a.k = b.k"
        ),
    },
    {
        "name": "join_self_order_date_range",
        "tables": ["orders"],
        "query": (
            "SELECT o1.order_id AS id1, o2.order_id AS id2 "
            "FROM {orders} o1 JOIN {orders} o2 "
            "ON o1.order_date <= o2.order_date AND o1.order_id <> o2.order_id"
        ),
    },
    {
        "name": "join_self_quantity_less_than",
        "tables": ["orders"],
        "query": (
            "SELECT o1.order_id AS id1, o2.order_id AS id2 "
            "FROM {orders} o1 JOIN {orders} o2 "
            "ON o1.quantity < o2.quantity AND o1.order_id <> o2.order_id"
        ),
    },
    {
        "name": "join_full_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.tag, b.note "
            "FROM {t_dup_a} a FULL JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "join_left_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.tag, b.note "
            "FROM {t_dup_a} a LEFT JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "join_right_dup_keys",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.tag, b.note "
            "FROM {t_dup_a} a RIGHT JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    # A boolean-column filter (`p.active = TRUE`) alongside a join. The boolean
    # column is probed for statistics; the probe omits min/max for it (Postgres
    # has none) so the filter pushes to a PostgreSQL-hosted products table.
    {
        "name": "join_comma_style_filtered_boolean_probe",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.name, o.order_id "
            "FROM {products} p, {orders} o "
            "WHERE p.product_id = o.product_id AND p.active = TRUE"
        ),
    },
    {
        "name": "join_filter_and_projection_combo_boolean_probe",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT p.category, c.segment "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE o.quantity > 1 AND p.active = TRUE"
        ),
    },
]
