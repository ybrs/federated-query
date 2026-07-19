"""The sanity seed corpus: one case per major cross-source behavior.

Every case references its tables as ``{table_name}`` placeholders; the harness
qualifies them per placement. Table sets are reused across cases (for example the
orders/customers pair backs all join-type cases) so the per-(tables, placement)
environment cache is hit often and the whole corpus runs quickly.

Coverage: the five join types cross-source, null-key and duplicate-key joins,
empty tables on each side, a three-table chain, aggregate-over-join grouped by a
dimension column, a star aggregate, ORDER BY + LIMIT, EXISTS and NOT EXISTS,
UNION ALL, a scalar subquery, a full type round-trip, ASCII text edge values, a
quoted reserved-word column, a cross-source-only case (``min_sources``), and an
invalid-qualifier case that must raise in every placement.
"""

CASES = [
    {
        "name": "inner_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "left_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.customer_id, o.order_id "
            "FROM {customers} c LEFT JOIN {orders} o "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "right_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o RIGHT JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "full_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o FULL JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "cross_join",
        "tables": ["t_lookup", "dim_item"],
        "query": (
            "SELECT l.code, di.item_name "
            "FROM {t_lookup} l CROSS JOIN {dim_item} di"
        ),
    },
    {
        "name": "null_key_inner_join",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.val, b.label "
            "FROM {t_null_a} a JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "null_key_left_join",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.k, b.label "
            "FROM {t_null_a} a LEFT JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "dup_key_fanout",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.tag, b.note "
            "FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "empty_on_right",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id, e.val "
            "FROM {orders} o LEFT JOIN {t_empty} e ON o.order_id = e.id"
        ),
    },
    {
        "name": "empty_on_left",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT e.id, o.order_id "
            "FROM {t_empty} e LEFT JOIN {orders} o ON e.id = o.order_id"
        ),
    },
    {
        "name": "empty_inner_join",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id "
            "FROM {orders} o JOIN {t_empty} e ON o.order_id = e.id"
        ),
    },
    {
        "name": "three_table_chain",
        "tables": ["orders", "products", "customers"],
        "query": (
            "SELECT o.order_id, p.name AS product, c.name AS customer "
            "FROM {orders} o "
            "JOIN {products} p ON o.product_id = p.product_id "
            "JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "aggregate_group_by_dim",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, count(*) AS n, sum(o.price) AS total "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "GROUP BY c.segment"
        ),
    },
    {
        "name": "star_aggregate_group_by",
        "tables": ["fact_sales", "dim_item"],
        "query": (
            "SELECT di.dept AS dept, sum(f.amount) AS total, count(*) AS n "
            "FROM {fact_sales} f JOIN {dim_item} di "
            "ON f.item_key = di.item_key "
            "GROUP BY di.dept"
        ),
    },
    {
        "name": "order_by_limit",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, c.name AS customer "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 5"
        ),
    },
    {
        "name": "exists_semi",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE EXISTS (SELECT 1 FROM {customers} c "
            "WHERE c.customer_id = o.customer_id)"
        ),
    },
    {
        "name": "not_exists_anti",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.customer_id, c.name FROM {customers} c "
            "WHERE NOT EXISTS (SELECT 1 FROM {orders} o "
            "WHERE o.customer_id = c.customer_id)"
        ),
    },
    {
        "name": "union_all",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT customer_id AS cid FROM {orders} "
            "UNION ALL "
            "SELECT customer_id AS cid FROM {customers}"
        ),
    },
    {
        "name": "scalar_subquery",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, "
            "(SELECT c.name FROM {customers} c "
            "WHERE c.customer_id = o.customer_id) AS cname "
            "FROM {orders} o"
        ),
    },
    {
        "name": "order_sensitive_single",
        "tables": ["orders"],
        "order_sensitive": True,
        "query": (
            "SELECT order_id, price FROM {orders} "
            "ORDER BY price DESC, order_id LIMIT 4"
        ),
    },
    {
        "name": "quoted_keyword_column",
        "tables": ["customers"],
        "query": 'SELECT c.name, c."group" AS grp FROM {customers} c',
    },
    {
        "name": "all_types_roundtrip",
        "tables": ["t_types"],
        "query": (
            "SELECT t_int, t_big, t_dbl, t_dec, t_str, t_bool, t_date, t_ts "
            "FROM {t_types}"
        ),
    },
    {
        "name": "text_edge_values",
        "tables": ["t_text"],
        "query": "SELECT s_id, s FROM {t_text}",
    },
    {
        "name": "cross_source_only_join",
        "tables": ["orders", "customers"],
        "min_sources": 2,
        "query": (
            "SELECT o.order_id, c.segment "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "bogus_qualifier_raises",
        "tables": ["orders"],
        "expect_error": "bogus",
        "query": "SELECT bogus.col FROM {orders} o",
    },
]
