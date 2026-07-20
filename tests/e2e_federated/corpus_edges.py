"""Edge-value corpus: NULL keys, duplicate keys, empty tables, type edges.

Case names are prefixed ``edge_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage: NULL join keys under every join type (null on the left, on the
right, on both, and the null-safe ``IS NOT DISTINCT FROM`` operator) plus
NULLs through arithmetic, comparison, COALESCE, and GROUP BY; duplicate-key
fan-out (many-to-many, a self-join, DISTINCT and aggregate over the fan-out,
LEFT/FULL with an unmatched key on each side); empty tables on the left,
right, and both sides of every join type plus a runtime-emptied join and an
aggregate over zero rows; single-row joins (including a single-row FULL JOIN
with no match) built by filtering a library table down to one row; type
edges from ``t_types`` (bigint and decimal precision, double arithmetic,
date/timestamp join keys, case-sensitive varchar equality, a boolean-column
join key); cross-type joins through an explicit CAST; a ``||`` string
concatenation over a CAST; a null-safe ``IS DISTINCT FROM`` WHERE predicate;
and ``t_text`` edges (NULLIF/TRIM on blank-vs-space values, a quoted value,
LIKE wildcard matches, and LIKE ... ESCAPE matching a literal wildcard).
"""

CASES = [
    # NULL join keys: t_null_a/t_null_b, every join type plus null-safe
    # equality, arithmetic, and GROUP BY over a NULL key.
    {
        "name": "edge_null_inner_no_match_default",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.val, b.label "
            "FROM {t_null_a} a JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_null_left_join_null_on_left",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.k, b.label "
            "FROM {t_null_a} a LEFT JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_null_right_join_null_on_right",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.val, b.id, b.k "
            "FROM {t_null_a} a RIGHT JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_null_full_join_both_sides",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id AS a_id, a.k AS a_k, b.id AS b_id, b.k AS b_k "
            "FROM {t_null_a} a FULL JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_null_safe_equal_join",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, b.id AS b_id FROM {t_null_a} a "
            "JOIN {t_null_b} b ON a.k IS NOT DISTINCT FROM b.k"
        ),
    },
    {
        "name": "edge_null_safe_not_equal_filter",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, a.k AS a_k, b.k AS b_k "
            "FROM {t_null_a} a JOIN {t_null_b} b ON a.id = b.id "
            "WHERE a.k IS DISTINCT FROM b.k"
        ),
    },
    {
        "name": "edge_null_both_sides_no_match_assert",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT count(*) AS n FROM {t_null_a} a "
            "JOIN {t_null_b} b ON a.k = b.k "
            "WHERE a.k IS NULL OR b.k IS NULL"
        ),
    },
    {
        "name": "edge_null_arithmetic_plus_one",
        "tables": ["t_null_a"],
        "query": "SELECT id, k + 1 AS k_plus_one FROM {t_null_a}",
    },
    {
        "name": "edge_null_comparison_unknown_filtered",
        "tables": ["t_null_a"],
        "query": "SELECT id FROM {t_null_a} WHERE k > 15",
    },
    {
        "name": "edge_null_is_null_filter",
        "tables": ["t_null_a"],
        "query": "SELECT id, val FROM {t_null_a} WHERE k IS NULL",
    },
    {
        "name": "edge_null_coalesce_default",
        "tables": ["t_null_a"],
        "query": "SELECT id, COALESCE(k, -1) AS k_or_default FROM {t_null_a}",
    },
    {
        "name": "edge_null_group_by_key",
        "tables": ["t_null_a"],
        "query": "SELECT k, count(*) AS n FROM {t_null_a} GROUP BY k",
    },
    # Duplicate keys: t_dup_a/t_dup_b many-to-many fan-out, a self-join, and
    # DISTINCT/aggregate/LEFT/FULL views over the same fan-out.
    {
        "name": "edge_dup_inner_fanout",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.tag, b.note FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_dup_fanout_distinct",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": ("SELECT DISTINCT a.k FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k"),
    },
    {
        "name": "edge_dup_fanout_aggregate",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k, count(*) AS n "
            "FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k "
            "GROUP BY a.k"
        ),
    },
    {
        "name": "edge_dup_left_join_unmatched_key",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k, a.tag, b.note "
            "FROM {t_dup_a} a LEFT JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_dup_self_join_fanout",
        "tables": ["t_dup_a"],
        "query": (
            "SELECT x.tag AS tag_x, y.tag AS tag_y "
            "FROM {t_dup_a} x JOIN {t_dup_a} y ON x.k = y.k"
        ),
    },
    {
        "name": "edge_dup_full_join_unmatched_both_sides",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT a.k AS a_k, b.k AS b_k "
            "FROM {t_dup_a} a FULL JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    # Empty tables: t_empty on the left, right, and both sides of every join
    # type, a runtime-emptied join, and an aggregate over zero rows.
    {
        "name": "edge_empty_both_inner_self",
        "tables": ["t_empty"],
        "query": (
            "SELECT e1.id, e2.val "
            "FROM {t_empty} e1 JOIN {t_empty} e2 ON e1.id = e2.id"
        ),
    },
    {
        "name": "edge_empty_both_left_self",
        "tables": ["t_empty"],
        "query": (
            "SELECT e1.id, e2.val "
            "FROM {t_empty} e1 LEFT JOIN {t_empty} e2 ON e1.id = e2.id"
        ),
    },
    {
        "name": "edge_empty_both_full_self",
        "tables": ["t_empty"],
        "query": (
            "SELECT e1.id AS id1, e2.id AS id2 "
            "FROM {t_empty} e1 FULL JOIN {t_empty} e2 ON e1.id = e2.id"
        ),
    },
    {
        "name": "edge_empty_full_join_right_side",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT o.order_id, e.val "
            "FROM {orders} o FULL JOIN {t_empty} e ON o.order_id = e.id"
        ),
    },
    {
        "name": "edge_empty_full_join_left_side",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT e.val, o.order_id "
            "FROM {t_empty} e FULL JOIN {orders} o ON e.id = o.order_id"
        ),
    },
    {
        "name": "edge_empty_right_join_empty_left",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT e.val, o.order_id "
            "FROM {t_empty} e RIGHT JOIN {orders} o ON e.id = o.order_id"
        ),
    },
    {
        "name": "edge_empty_runtime_filter_emptied",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o LEFT JOIN {customers} c "
            "ON o.customer_id = c.customer_id AND c.customer_id > 100"
        ),
    },
    {
        "name": "edge_empty_aggregate_over_empty_join",
        "tables": ["orders", "t_empty"],
        "query": (
            "SELECT count(*) AS n, sum(o.price) AS total "
            "FROM {orders} o JOIN {t_empty} e ON o.order_id = e.id"
        ),
    },
    # Single-row joins: a library table filtered down to one row on each side.
    {
        "name": "edge_single_row_cross_join",
        "tables": ["dim_day", "dim_item"],
        "query": (
            "SELECT d.cal_date, i.item_name "
            "FROM (SELECT * FROM {dim_day} WHERE day_key = 20240101) d "
            "CROSS JOIN (SELECT * FROM {dim_item} WHERE item_key = 11) i"
        ),
    },
    {
        "name": "edge_single_row_inner_match",
        "tables": ["fact_sales", "dim_day"],
        "query": (
            "SELECT f.sale_id, d.cal_date "
            "FROM (SELECT * FROM {fact_sales} WHERE sale_id = 1) f "
            "JOIN (SELECT * FROM {dim_day} WHERE day_key = 20240101) d "
            "ON f.day_key = d.day_key"
        ),
    },
    {
        "name": "edge_single_row_left_join_empty",
        "tables": ["dim_day", "t_empty"],
        "query": (
            "SELECT d.day_key, e.val "
            "FROM (SELECT * FROM {dim_day} WHERE day_key = 20240101) d "
            "LEFT JOIN {t_empty} e ON d.day_key = e.id"
        ),
    },
    {
        "name": "edge_single_row_full_join_no_match",
        "tables": ["dim_day", "dim_item"],
        "query": (
            "SELECT d.day_key, i.item_key "
            "FROM (SELECT * FROM {dim_day} WHERE day_key = 20240101) d "
            "FULL JOIN (SELECT * FROM {dim_item} WHERE item_key = 12) i "
            "ON d.day_key = i.item_key"
        ),
    },
    # Type edges: bigint and decimal precision, double arithmetic,
    # date/timestamp join keys, and case-sensitive varchar equality.
    {
        "name": "edge_types_bigint_sum_precision",
        "tables": ["t_types"],
        "query": "SELECT sum(t_big) AS total_big FROM {t_types} WHERE t_big IS NOT NULL",
    },
    {
        "name": "edge_types_decimal_exact_sum",
        "tables": ["t_types"],
        "query": "SELECT sum(t_dec) AS total_dec FROM {t_types} WHERE t_dec IS NOT NULL",
    },
    {
        "name": "edge_types_double_arithmetic_null",
        "tables": ["t_types"],
        "query": "SELECT t_int, t_dbl * 2 AS doubled FROM {t_types}",
    },
    {
        "name": "edge_types_date_join_key",
        "tables": ["dim_day", "t_dates"],
        "query": (
            "SELECT dd.day_key, td.d_id "
            "FROM {dim_day} dd JOIN {t_dates} td ON dd.cal_date = td.d_date"
        ),
    },
    {
        "name": "edge_types_timestamp_self_join",
        "tables": ["t_dates"],
        "query": (
            "SELECT d1.d_id AS id1, d2.d_id AS id2 "
            "FROM {t_dates} d1 JOIN {t_dates} d2 ON d1.d_ts = d2.d_ts"
        ),
    },
    {
        "name": "edge_types_varchar_case_sensitive_match",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE s = 'Mixed CASE'",
    },
    {
        "name": "edge_types_varchar_case_sensitive_no_match",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE s = 'mixed case'",
    },
    {
        "name": "edge_types_null_row_roundtrip",
        "tables": ["t_types"],
        "query": (
            "SELECT t_int, t_big, t_dbl, t_dec, t_str, t_bool, t_date, t_ts "
            "FROM {t_types} WHERE t_str IS NULL"
        ),
    },
    # Cross-type joins through an explicit CAST.
    {
        "name": "edge_cast_int_to_bigint_join",
        "tables": ["orders", "t_types"],
        "query": (
            "SELECT o.order_id, t.t_str FROM {orders} o "
            "JOIN {t_types} t "
            "ON CAST(o.order_id AS BIGINT) = CAST(t.t_int AS BIGINT)"
        ),
    },
    {
        "name": "edge_cast_varchar_int_compare",
        "tables": ["orders"],
        "query": "SELECT order_id FROM {orders} WHERE CAST(order_id AS VARCHAR) = '5'",
    },
    # t_text edges: NULLIF/TRIM on blank-vs-space, a quoted value, a LIKE
    # wildcard match, a tab-embedded value, and LIKE ... ESCAPE matching a
    # literal wildcard, each result-checked against the DuckDB oracle.
    {
        "name": "edge_text_nullif_empty_vs_space",
        "tables": ["t_text"],
        "query": (
            "SELECT s_id, NULLIF(s, '') AS maybe_null "
            "FROM {t_text} WHERE s_id IN (1, 2)"
        ),
    },
    {
        "name": "edge_text_trim_spaces_equal_empty",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE trim(s) = ''",
    },
    {
        "name": "edge_text_quote_in_value_match",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE s = 'has''quote'",
    },
    {
        "name": "edge_text_like_wildcard_percent_underscore",
        "tables": ["t_lookup"],
        "query": "SELECT code FROM {t_lookup} WHERE code LIKE 's_i%'",
    },
    {
        "name": "edge_text_like_wildcard_underscore_only",
        "tables": ["t_lookup"],
        "query": "SELECT code FROM {t_lookup} WHERE code LIKE '_h%'",
    },
    {
        "name": "edge_text_tab_char_roundtrip",
        "tables": ["t_text"],
        "query": "SELECT s_id, s FROM {t_text} WHERE s_id = 7",
    },
    {
        "name": "edge_text_like_escape_percent_literal",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE s LIKE '%per\\%cent%' ESCAPE '\\'",
    },
    {
        "name": "edge_text_like_escape_underscore_literal",
        "tables": ["t_text"],
        "query": "SELECT s_id FROM {t_text} WHERE s LIKE '%under\\_score%' ESCAPE '\\'",
    },
    # Cross-source-only variants (min_sources=2) of the NULL-key and
    # duplicate-key fan-out joins above.
    {
        "name": "edge_null_cross_source_only",
        "tables": ["t_null_a", "t_null_b"],
        "min_sources": 2,
        "query": (
            "SELECT a.val, b.label " "FROM {t_null_a} a JOIN {t_null_b} b ON a.k = b.k"
        ),
    },
    {
        "name": "edge_dup_cross_source_only",
        "tables": ["t_dup_a", "t_dup_b"],
        "min_sources": 2,
        "query": (
            "SELECT a.tag, b.note " "FROM {t_dup_a} a JOIN {t_dup_b} b ON a.k = b.k"
        ),
    },
    # A boolean-column equality join key: the referenced boolean column drives
    # a statistics probe that omits min/max (Postgres has none for booleans)
    # yet still pushes the equality predicate to a PostgreSQL source.
    {
        "name": "edge_types_boolean_join_key",
        "tables": ["products", "t_types"],
        "query": (
            "SELECT p.name, t.t_str "
            "FROM {products} p JOIN {t_types} t ON p.active = t.t_bool"
        ),
    },
    # The `||` string-concatenation operator over a CAST, exercised on every
    # connector including Parquet (it lowers to the `||` execution operator).
    {
        "name": "edge_null_concat_cast",
        "tables": ["t_null_a"],
        "query": (
            "SELECT id, CAST(k AS VARCHAR) || '-suffix' AS tagged FROM {t_null_a}"
        ),
    },
]
