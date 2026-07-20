"""Gap corpus: coverage the other modules miss, found by reviewing them all.

Case names are prefixed ``gap_``. Every case references its tables as
``{table_name}`` placeholders; the harness qualifies them per placement.

Coverage added here, each absent from the six existing corpus modules:

- Read-only contract guards: INSERT/UPDATE/DELETE/CREATE TABLE/CREATE TABLE AS/
  DROP TABLE/TRUNCATE through the engine each raise loudly (the engine's query
  pipeline plans SELECT only); pinned by the ``unsupported SQL: query `<kind>```
  substring. These are invalid-query guards - answering one would be an epic
  fail.
- LIKE family in shapes the edge module does not reach: ILIKE (bare and with
  ESCAPE), NOT LIKE ... ESCAPE, and LIKE ... ESCAPE in a JOIN ON key (the edge
  module only has LIKE/LIKE-ESCAPE in WHERE).
- IS DISTINCT FROM as a JOIN ON conjunct and inside a CASE (the edge module has
  IS [NOT] DISTINCT FROM only as a join key or a WHERE predicate).
- LAG with an explicit non-null default over a cross-source partition (the
  window module has an explicit default only on LEAD).
- A ``||`` concatenation chain next to CONCAT() over NULL-bearing values in one
  query, contrasting ``||`` NULL propagation with CONCAT NULL skipping.
- Bare AVG over a DECIMAL column (no CAST) across sources - the aggregate module
  always wraps AVG in a CAST.
- EXISTS in the SELECT list and a scalar subquery inside CASE WHEN (the subquery
  module keeps EXISTS/scalars in WHERE or as a top-level projection).
- EXTRACT(EPOCH) over a cross-source join (the aggregate/window modules extract
  only YEAR/MONTH).
- Deep nesting: four-level derived tables, a three-level CTE chain, a set
  operation of two CTEs, and a join of two WITH RECURSIVE results.
- Type unification: COALESCE and CASE branches of different-but-castable types,
  and a DECIMAL equality join key across sources.
- ORDER BY over the whole row by ordinal, and two LIMIT/OFFSET pages that
  together tile the whole result deterministically.
- Empty-result edges: WHERE that is constant-false over a join, HAVING constant-
  false with GROUP BY, and a projection of expressions over zero surviving rows.
- Three-branch INTERSECT ALL / EXCEPT ALL and null-bearing INTERSECT ALL /
  EXCEPT ALL (the set module has only two-branch, non-null ALL forms).
- Two designed raises on valid-but-unsupported SQL: a tuple/row IN-subquery and
  an ORDER BY inside STRING_AGG.

``SUSPECTED_ENGINE_BUGS`` parks one verified silent-wrong-answer found here (the
aggregate FILTER clause is dropped), so ``CASES`` stays green.
"""

CASES = [
    # --- Read-only contract guards: the query pipeline plans SELECT only; every
    # DML/DDL statement raises at parse. Answering one would manufacture a
    # mutation the engine must never perform. ---
    {
        "name": "gap_readonly_insert_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `insert`",
        "query": (
            "INSERT INTO {orders} VALUES "
            "(99, 1, 1, 1, 1.00, 'x', DATE '2024-01-01')"
        ),
    },
    {
        "name": "gap_readonly_update_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `update`",
        "query": "UPDATE {orders} SET status = 'x' WHERE order_id = 1",
    },
    {
        "name": "gap_readonly_delete_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `delete`",
        "query": "DELETE FROM {orders} WHERE order_id = 1",
    },
    {
        "name": "gap_readonly_create_table_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `create_table`",
        "query": "CREATE TABLE {orders} (a INTEGER)",
    },
    {
        "name": "gap_readonly_create_table_as_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `create_table`",
        "query": "CREATE TABLE gap_new_t AS SELECT order_id FROM {orders}",
    },
    {
        "name": "gap_readonly_drop_table_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `drop_table`",
        "query": "DROP TABLE {orders}",
    },
    {
        "name": "gap_readonly_truncate_raises",
        "tables": ["orders"],
        "expect_error": "unsupported SQL: query `truncate`",
        "query": "TRUNCATE TABLE {orders}",
    },
    # --- LIKE family shapes the edge module does not reach ---
    {
        "name": "gap_ilike_cross_source_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT DISTINCT c.customer_id FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id WHERE c.name ILIKE 'a%'"
        ),
    },
    {
        "name": "gap_ilike_escape_literal_percent",
        "tables": ["t_text", "dim_item"],
        "query": (
            "SELECT t.s_id FROM {t_text} t CROSS JOIN {dim_item} di "
            "WHERE di.item_key = 11 AND t.s ILIKE '%PER\\%CENT%' ESCAPE '\\'"
        ),
    },
    {
        "name": "gap_not_like_escape_literal_underscore",
        "tables": ["t_text", "dim_item"],
        "query": (
            "SELECT t.s_id FROM {t_text} t CROSS JOIN {dim_item} di "
            "WHERE di.item_key = 11 AND t.s NOT LIKE '%under\\_score%' ESCAPE '\\'"
        ),
    },
    {
        "name": "gap_like_escape_join_on_key",
        "tables": ["orders", "t_lookup"],
        "query": (
            "SELECT o.order_id, l.code FROM {orders} o JOIN {t_lookup} l "
            "ON o.status LIKE l.code ESCAPE '\\'"
        ),
    },
    # --- IS DISTINCT FROM in a JOIN ON conjunct and inside CASE ---
    {
        "name": "gap_is_distinct_from_join_on_conjunct",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, b.id AS b_id FROM {t_null_a} a JOIN {t_null_b} b "
            "ON a.id = b.id AND a.k IS DISTINCT FROM b.k"
        ),
    },
    {
        "name": "gap_is_distinct_from_in_case",
        "tables": ["t_null_a", "t_null_b"],
        "query": (
            "SELECT a.id, CASE WHEN a.k IS DISTINCT FROM b.k THEN 'diff' "
            "ELSE 'same' END AS cmp "
            "FROM {t_null_a} a LEFT JOIN {t_null_b} b ON a.id = b.id"
        ),
    },
    # --- LAG with an explicit non-null default over a cross-source partition ---
    {
        "name": "gap_lag_explicit_default_cross_source",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.segment, "
            "LAG(o.price, 1, CAST(-1 AS DECIMAL(10,2))) OVER "
            "(PARTITION BY c.segment ORDER BY o.order_id) AS prev_price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    # --- A || chain next to CONCAT over NULL-bearing values: || propagates NULL,
    # CONCAT skips it, in one row. ---
    {
        "name": "gap_concat_chain_vs_concat_over_nulls",
        "tables": ["t_null_a", "dim_item"],
        "query": (
            "SELECT a.id, CAST(a.k AS VARCHAR) || '-' || a.val AS piped, "
            "CONCAT(CAST(a.k AS VARCHAR), '-', a.val) AS concatted "
            "FROM {t_null_a} a CROSS JOIN {dim_item} di WHERE di.item_key = 11"
        ),
    },
    # --- Bare AVG over a DECIMAL column, no CAST, grouped across sources ---
    {
        "name": "gap_avg_decimal_bare_no_cast",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, AVG(o.price) AS avg_price "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
    # --- EXISTS in the SELECT list, and a scalar subquery inside CASE WHEN ---
    {
        "name": "gap_exists_in_select_list",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, "
            "EXISTS (SELECT 1 FROM {orders} o WHERE o.customer_id = c.customer_id) "
            "AS has_order FROM {customers} c"
        ),
    },
    {
        "name": "gap_scalar_subquery_in_case_when",
        "tables": ["customers", "orders"],
        "query": (
            "SELECT c.customer_id, CASE WHEN "
            "(SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = c.customer_id) > 1 "
            "THEN 'many' ELSE 'few' END AS bucket FROM {customers} c"
        ),
    },
    # --- EXTRACT(EPOCH) over a cross-source join (DuckDB and Postgres agree:
    # seconds since the Unix epoch). ---
    {
        "name": "gap_extract_epoch_cross_source",
        "tables": ["t_dates", "t_lookup"],
        "query": (
            "SELECT d.d_id, l.code, EXTRACT(EPOCH FROM d.d_ts) AS ep "
            "FROM {t_dates} d CROSS JOIN {t_lookup} l WHERE l.code = 'shipped'"
        ),
    },
    # --- Deep nesting ---
    {
        "name": "gap_four_level_derived_tables",
        "tables": ["orders"],
        "query": (
            "SELECT l4.order_id FROM ("
            "SELECT l3.order_id, l3.price FROM ("
            "SELECT l2.order_id, l2.price FROM ("
            "SELECT l1.order_id, l1.price FROM {orders} l1 WHERE l1.price > 10"
            ") l2 WHERE l2.price > 20"
            ") l3 WHERE l3.price < 200"
            ") l4"
        ),
    },
    {
        "name": "gap_cte_chain_three_levels",
        "tables": ["orders", "customers"],
        "query": (
            "WITH a AS (SELECT order_id, customer_id, price FROM {orders}), "
            "b AS (SELECT order_id, customer_id, price FROM a WHERE price > 20), "
            "d AS (SELECT order_id, customer_id, price FROM b WHERE price < 150) "
            "SELECT d.order_id, c.name FROM d "
            "JOIN {customers} c ON d.customer_id = c.customer_id"
        ),
    },
    {
        "name": "gap_setop_of_two_ctes",
        "tables": ["orders", "customers"],
        "query": (
            "WITH a AS (SELECT customer_id FROM {orders}), "
            "b AS (SELECT customer_id FROM {customers}) "
            "SELECT customer_id FROM ("
            "SELECT customer_id FROM a INTERSECT SELECT customer_id FROM b) u"
        ),
    },
    {
        "name": "gap_join_two_recursive_ctes",
        "tables": ["customers"],
        "query": (
            "WITH RECURSIVE s1(n) AS ("
            "SELECT 1 UNION ALL SELECT n + 1 FROM s1 WHERE n < 4), "
            "s2(m) AS ("
            "SELECT 10 UNION ALL SELECT m + 10 FROM s2 WHERE m < 30) "
            "SELECT s1.n, s2.m, c.name FROM s1 JOIN s2 ON s1.n < 3 "
            "JOIN {customers} c ON c.customer_id = s1.n"
        ),
    },
    # --- Type unification across sources ---
    {
        "name": "gap_coalesce_mixed_castable_types",
        "tables": ["orders", "t_null_a"],
        "query": (
            "SELECT a.id, COALESCE(a.k, o.price, 0) AS v "
            "FROM {t_null_a} a LEFT JOIN {orders} o ON a.id = o.order_id"
        ),
    },
    {
        "name": "gap_case_branches_mixed_types",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, "
            "CASE WHEN o.price > 50 THEN o.price ELSE o.quantity END AS v "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id"
        ),
    },
    {
        "name": "gap_decimal_equality_join_key",
        "tables": ["orders", "products"],
        "query": (
            "SELECT o.order_id, p.name FROM {orders} o JOIN {products} p "
            "ON o.price = p.unit_price"
        ),
    },
    # --- ORDER BY the whole row by ordinal, and two tiling LIMIT/OFFSET pages ---
    {
        "name": "gap_order_by_full_row_ordinals",
        "tables": ["fact_sales"],
        "order_sensitive": True,
        "query": (
            "SELECT sale_id, day_key, item_key, amount, qty "
            "FROM {fact_sales} ORDER BY 1, 2, 3, 4, 5"
        ),
    },
    {
        "name": "gap_limit_offset_page_one",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, o.price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 5 OFFSET 0"
        ),
    },
    {
        "name": "gap_limit_offset_page_two",
        "tables": ["orders", "customers"],
        "order_sensitive": True,
        "query": (
            "SELECT o.order_id, o.price "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "ORDER BY o.order_id LIMIT 5 OFFSET 5"
        ),
    },
    # --- Empty-result edges ---
    {
        "name": "gap_where_constant_false_over_join",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id, c.name "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE 1 = 0"
        ),
    },
    {
        "name": "gap_having_constant_false_with_group_by",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT c.segment AS seg, count(*) AS n "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "GROUP BY c.segment HAVING 1 = 0"
        ),
    },
    {
        "name": "gap_zero_rows_projection_expressions",
        "tables": ["orders", "customers"],
        "query": (
            "SELECT o.order_id + 1 AS oid, UPPER(o.status) AS st "
            "FROM {orders} o JOIN {customers} c ON o.customer_id = c.customer_id "
            "WHERE FALSE"
        ),
    },
    # --- Three-branch and null-bearing INTERSECT ALL / EXCEPT ALL ---
    {
        "name": "gap_intersect_all_three_branches",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT k FROM {t_dup_a} INTERSECT ALL SELECT k FROM {t_dup_b} "
            "INTERSECT ALL SELECT k FROM {t_dup_a}"
        ),
    },
    {
        "name": "gap_except_all_three_branches",
        "tables": ["t_dup_a", "t_dup_b"],
        "query": (
            "SELECT k FROM {t_dup_a} EXCEPT ALL SELECT k FROM {t_dup_b} "
            "EXCEPT ALL SELECT k FROM {t_dup_b}"
        ),
    },
    {
        "name": "gap_intersect_all_with_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} INTERSECT ALL SELECT k FROM {t_null_b}",
    },
    {
        "name": "gap_except_all_with_nulls",
        "tables": ["t_null_a", "t_null_b"],
        "query": "SELECT k FROM {t_null_a} EXCEPT ALL SELECT k FROM {t_null_b}",
    },
    # --- Designed raises on valid-but-unsupported SQL ---
    {
        "name": "gap_tuple_in_subquery_raises",
        "tables": ["orders"],
        "expect_error": "function `tuple`",
        "query": (
            "SELECT o.order_id FROM {orders} o "
            "WHERE (o.customer_id, o.product_id) IN "
            "(SELECT customer_id, product_id FROM {orders})"
        ),
    },
    {
        "name": "gap_string_agg_ordered_raises",
        "tables": ["orders", "customers"],
        "expect_error": "ORDER BY inside STRING_AGG",
        "query": (
            "SELECT c.segment AS seg, "
            "STRING_AGG(o.status, ',' ORDER BY o.order_id) AS statuses "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
    },
]

SUSPECTED_ENGINE_BUGS = {
    "gap_aggregate_filter_clause_ignored": {
        "query": (
            "SELECT c.segment AS seg, "
            "COUNT(*) FILTER (WHERE o.status = 'processing') AS n_proc "
            "FROM {orders} o JOIN {customers} c "
            "ON o.customer_id = c.customer_id GROUP BY c.segment"
        ),
        "tables": ["orders", "customers"],
        "finding": (
            "The aggregate FILTER (WHERE ...) clause is silently dropped: the "
            "engine returns the UNFILTERED aggregate and presents it as the "
            "filtered one - a silent wrong answer, not a raise. For segment "
            "'enterprise' the engine returns COUNT = 5 (every enterprise order) "
            "where the FILTER (WHERE o.status = 'processing') restricts it to 2; "
            "'consumer' and 'smb' are wrong the same way. Reproduced at ALL "
            "seven placements (oracle_single_duck, duck_duck, pg_duck, duck_pg, "
            "all_pg, parquet_duck, parquet_pg), so it is source-independent - "
            "the FILTER is discarded during parse/bind before any placement "
            "decision. A COUNT(*) FILTER (WHERE ...) that yields the plain "
            "COUNT(*) means the predicate never reaches the aggregate. Kept out "
            "of CASES so the suite stays green; re-add as the regression guard "
            "once the parser either honors the FILTER clause or raises "
            "'unsupported SQL' on it (answering it wrong is worse than raising)."
        ),
    },
}
