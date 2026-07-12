# Plan comparison: OLD (python) vs NEW (rust) engine

Federated split pg-dims (dims on PostgreSQL, facts on DuckDB). TPC-H scale 0.01, TPC-DS scale 0.1. Structural comparison of the SQL each engine pushes to each source.

## TPCH: 22 compared, 9 divergent, 0 new-errored

| query | old #isl | new #isl | old ship | new ship | old push | new push | new err | divergence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 1 | 1 | 0 | 0 | yes | yes | no | no |
| q02 | 7 | 7 | 0 | 0 | yes | yes | no | no |
| q03 | 2 | 3 | 0 | 0 | yes | no | no | YES: OLD pushes agg/join, NEW leaves it in coordinator |
| q04 | 1 | 1 | 0 | 0 | yes | yes | no | no |
| q05 | 5 | 4 | 0 | 0 | yes | yes | no | YES: different island split (5 vs 4 islands) |
| q06 | 1 | 1 | 0 | 0 | yes | yes | no | no |
| q07 | 5 | 5 | 0 | 0 | yes | yes | no | no |
| q08 | 7 | 8 | 0 | 0 | yes | no | no | YES: OLD pushes agg/join, NEW leaves it in coordinator |
| q09 | 6 | 5 | 0 | 0 | no | yes | no | YES: NEW pushes agg/join, OLD leaves it in coordinator |
| q10 | 3 | 3 | 0 | 0 | yes | yes | no | YES: different island split (3 vs 3 islands) |
| q11 | 4 | 2 | 0 | 0 | yes | yes | no | YES: different island split (4 vs 2 islands) |
| q12 | 1 | 1 | 0 | 0 | yes | yes | no | no |
| q13 | 2 | 2 | 0 | 0 | no | no | no | no |
| q14 | 2 | 2 | 0 | 0 | no | no | no | no |
| q15 | 3 | 2 | 0 | 0 | yes | yes | no | YES: different island split (3 vs 2 islands) |
| q16 | 3 | 2 | 0 | 0 | no | no | no | YES: different island split (3 vs 2 islands) |
| q17 | 3 | 3 | 0 | 0 | yes | yes | no | no |
| q18 | 3 | 3 | 0 | 0 | yes | yes | no | no |
| q19 | 2 | 2 | 0 | 0 | no | no | no | no |
| q20 | 5 | 5 | 0 | 0 | yes | yes | no | no |
| q21 | 3 | 3 | 0 | 0 | yes | yes | no | YES: different island split (3 vs 3 islands) |
| q22 | 3 | 3 | 0 | 0 | yes | yes | no | no |

### Systematic divergences (tpch)

**different island split (join/reduction placement differs)** - 6 queries: q05, q10, q11, q15, q16, q21
- example q05:
  - OLD: SELECT "orders"."o_orderdate" AS "o_orderdate", "orders"."o_custkey" AS "o_custkey", "orders"."o_orderkey" AS "o_orderkey", "lineitem"."l_extendedprice" AS "l_extendedprice", "lineitem"."l_discount" A || SELECT "s_nationkey", "s_suppkey" FROM "public"."supplier" AS "supplier" || SELECT "n_name", "n_regionkey", "n_nationkey" FROM "public"."nation" AS "nation" || SELECT "r_name", "r_regionkey" FROM "public"."region" AS "region" WHERE ("region"."r_name" = 'ASIA') || SELECT "c_nationkey", "c_custkey" FROM "public"."customer" AS "customer"
  - NEW: SELECT "c_custkey", "c_nationkey" FROM "public"."customer" AS "customer" || SELECT "nation"."n_nationkey" AS "n_nationkey", "nation"."n_name" AS "n_name", "nation"."n_regionkey" AS "n_regionkey", "region"."r_regionkey" AS "r_regionkey", "region"."r_name" AS "r_name", "supplie || SELECT "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount" FROM "main"."lineitem" AS "lineitem" WHERE l_suppkey IN (51, 81, 68, 100, 35, 54, 99, 39, 50, 27, 43, 75, 89, 98, 12, 41, 80, 82, 96,  || SELECT "o_orderkey", "o_custkey", "o_orderdate" FROM "main"."orders" AS "orders" WHERE ((orders.o_orderdate >= CAST('1994-01-01' AS DATE)) AND (orders.o_orderdate < CAST('1995-01-01' AS DATE)))

**OLD pushes agg/join, NEW leaves it in coordinator** - 2 queries: q03, q08
- example q03:
  - OLD: SELECT "orders"."o_orderdate" AS "o_orderdate", "orders"."o_shippriority" AS "o_shippriority", "orders"."o_orderkey" AS "o_orderkey", "orders"."o_custkey" AS "o_custkey", "lineitem"."l_orderkey" AS "l || SELECT "c_mktsegment", "c_custkey" FROM "public"."customer" AS "customer" WHERE ("customer"."c_mktsegment" = 'BUILDING')
  - NEW: SELECT "c_custkey", "c_mktsegment" FROM "public"."customer" AS "customer" WHERE ("customer"."c_mktsegment" = 'BUILDING') || SELECT "o_orderkey", "o_custkey", "o_orderdate", "o_shippriority" FROM "main"."orders" AS "orders" WHERE ((orders.o_orderdate < CAST('1995-03-15' AS DATE)) AND o_custkey IN (13, 47, 108, 109, 170, 173 || SELECT "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate" FROM "main"."lineitem" AS "lineitem" WHERE ((lineitem.l_shipdate > CAST('1995-03-15' AS DATE)) AND l_orderkey IN (164, 231, 1952, 195

**NEW pushes agg/join, OLD leaves it in coordinator** - 1 queries: q09
- example q09:
  - OLD: SELECT "p_name", "p_partkey" FROM "public"."part" AS "part" WHERE ("part"."p_name" LIKE '%green%') || SELECT "l_extendedprice", "l_quantity", "l_discount", "l_orderkey", "l_partkey", "l_suppkey" FROM "main"."lineitem" AS "lineitem" || SELECT "s_nationkey", "s_suppkey" FROM "public"."supplier" AS "supplier" || SELECT "n_name", "n_nationkey" FROM "public"."nation" AS "nation" || SELECT "o_orderdate", "o_orderkey" FROM "main"."orders" AS "orders" || SELECT "ps_supplycost", "ps_partkey", "ps_suppkey" FROM "main"."partsupp" AS "partsupp"
  - NEW: SELECT "p_partkey", "p_name" FROM "public"."part" AS "part" WHERE ("part"."p_name" LIKE '%green%') || SELECT "l_orderkey", "l_partkey", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount" FROM "main"."lineitem" AS "lineitem" || SELECT "supplier"."s_suppkey" AS "s_suppkey", "supplier"."s_nationkey" AS "s_nationkey", "nation"."n_nationkey" AS "n_nationkey", "nation"."n_name" AS "n_name" FROM "public"."supplier" AS "supplier" I || SELECT "o_orderkey", "o_orderdate" FROM "main"."orders" AS "orders" || SELECT "ps_partkey", "ps_suppkey", "ps_supplycost" FROM "main"."partsupp" AS "partsupp"

## TPCDS: 99 compared, 70 divergent, 34 new-errored

| query | old #isl | new #isl | old ship | new ship | old push | new push | new err | divergence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 6 | 3 | 0 | 0 | no | no | no | YES: different island split (6 vs 3 islands) |
| q02 | 8 | 6 | 0 | 0 | no | no | no | YES: different island split (8 vs 6 islands) |
| q03 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q04 | 54 | 7 | 2 | 2 | yes | yes | no | YES: different island split (54 vs 7 islands) |
| q05 | 12 | 10 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: Error during planning: function 'rollup' not supporte |
| q06 | 7 | 7 | 0 | 0 | yes | yes | no | no |
| q07 | 5 | 5 | 4 | 4 | yes | yes | no | no |
| q08 | 5 | 4 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: adbc execute: InvalidArguments: Failed to prepare que |
| q09 | 16 | 16 | 0 | 0 | yes | yes | no | no |
| q10 | 9 | 7 | 0 | 0 | no | no | no | YES: different island split (9 vs 7 islands) |
| q11 | 24 | 5 | 2 | 2 | yes | yes | no | YES: different island split (24 vs 5 islands) |
| q12 | 3 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q13 | 6 | 6 | 0 | 0 | no | no | no | no |
| q14 | 57 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `union` |
| q15 | 4 | 4 | 0 | 0 | no | no | no | no |
| q16 | 4 | 4 | 0 | 0 | no | no | yes | YES: NEW errors: execution error: type_coercion |
| q17 | 6 | 0 | 5 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: function `stddev_samp` |
| q18 | 5 | 5 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: Error during planning: function 'rollup' not supporte |
| q19 | 6 | 6 | 0 | 0 | no | no | no | no |
| q20 | 3 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q21 | 4 | 4 | 3 | 3 | yes | yes | no | no |
| q22 | 3 | 3 | 0 | 0 | no | no | yes | YES: NEW errors: execution error: Error during planning: function 'rollup' not supporte |
| q23 | 22 | 13 | 2 | 2 | yes | yes | no | YES: different island split (22 vs 13 islands) |
| q24 | 8 | 4 | 0 | 0 | yes | yes | no | YES: different island split (8 vs 4 islands) |
| q25 | 6 | 5 | 5 | 5 | yes | yes | no | YES: different island split (6 vs 5 islands) |
| q26 | 5 | 5 | 4 | 4 | yes | yes | no | no |
| q27 | 15 | 5 | 0 | 0 | no | no | no | YES: different island split (15 vs 5 islands) |
| q28 | 6 | 6 | 0 | 0 | yes | yes | no | no |
| q29 | 6 | 6 | 5 | 5 | yes | yes | no | no |
| q30 | 7 | 5 | 0 | 0 | yes | yes | no | YES: different island split (7 vs 5 islands) |
| q31 | 18 | 5 | 2 | 2 | yes | yes | no | YES: different island split (18 vs 5 islands) |
| q32 | 5 | 0 | 1 | 0 | yes | no | yes | YES: NEW errors: panic: ColumnRef type must be set during binding |
| q33 | 12 | 10 | 0 | 0 | no | no | no | YES: different island split (12 vs 10 islands) |
| q34 | 5 | 5 | 3 | 3 | yes | yes | no | no |
| q35 | 9 | 6 | 0 | 0 | no | no | no | YES: different island split (9 vs 6 islands) |
| q36 | 12 | 0 | 3 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q37 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q38 | 9 | 5 | 0 | 0 | no | no | no | YES: different island split (9 vs 5 islands) |
| q39 | 8 | 0 | 3 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: function `stddev_samp` |
| q40 | 4 | 0 | 0 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: Implicit JOIN |
| q41 | 1 | 1 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: adbc execute: InvalidArguments: Failed to prepare que |
| q42 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q43 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q44 | 6 | 0 | 0 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q45 | 10 | 8 | 0 | 0 | no | no | no | YES: different island split (10 vs 8 islands) |
| q46 | 6 | 6 | 4 | 4 | yes | yes | no | no |
| q47 | 12 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q48 | 5 | 5 | 0 | 0 | no | no | no | no |
| q49 | 6 | 0 | 0 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: Implicit JOIN |
| q50 | 4 | 4 | 0 | 0 | yes | yes | no | no |
| q51 | 4 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q52 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q53 | 4 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q54 | 10 | 9 | 0 | 0 | yes | yes | no | YES: different island split (10 vs 9 islands) |
| q55 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q56 | 12 | 10 | 0 | 0 | no | no | no | YES: different island split (12 vs 10 islands) |
| q57 | 12 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q58 | 15 | 13 | 0 | 0 | no | no | no | YES: different island split (15 vs 13 islands) |
| q59 | 8 | 5 | 0 | 0 | no | no | no | YES: different island split (8 vs 5 islands) |
| q60 | 12 | 10 | 0 | 0 | no | no | no | YES: different island split (12 vs 10 islands) |
| q61 | 13 | 8 | 11 | 11 | yes | yes | no | YES: different island split (13 vs 8 islands) |
| q62 | 5 | 5 | 0 | 0 | no | no | no | no |
| q63 | 4 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q64 | 34 | 25 | 0 | 0 | yes | yes | no | YES: different island split (34 vs 25 islands) |
| q65 | 6 | 4 | 2 | 0 | yes | no | no | YES: OLD ships dim into duck, NEW does not |
| q66 | 10 | 7 | 0 | 0 | no | no | no | YES: different island split (10 vs 7 islands) |
| q67 | 4 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q68 | 7 | 6 | 4 | 4 | yes | yes | no | YES: different island split (7 vs 6 islands) |
| q69 | 9 | 7 | 0 | 0 | no | no | no | YES: different island split (9 vs 7 islands) |
| q70 | 6 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q71 | 8 | 6 | 0 | 0 | no | no | no | YES: different island split (8 vs 6 islands) |
| q72 | 11 | 11 | 0 | 0 | no | no | no | no |
| q73 | 5 | 5 | 3 | 3 | yes | yes | no | no |
| q74 | 24 | 5 | 2 | 2 | yes | yes | no | YES: different island split (24 vs 5 islands) |
| q75 | 24 | 13 | 0 | 0 | no | no | no | YES: different island split (24 vs 13 islands) |
| q76 | 9 | 9 | 0 | 0 | no | no | no | no |
| q77 | 16 | 10 | 3 | 3 | yes | yes | yes | YES: NEW errors: execution error: Error during planning: function 'rollup' not supporte |
| q78 | 6 | 4 | 0 | 0 | yes | yes | no | YES: different island split (6 vs 4 islands) |
| q79 | 5 | 5 | 3 | 3 | yes | yes | no | no |
| q80 | 15 | 0 | 0 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: Implicit JOIN |
| q81 | 7 | 2 | 0 | 0 | yes | no | no | YES: OLD pushes agg/join, NEW leaves it in coordinator |
| q82 | 3 | 3 | 2 | 2 | yes | yes | no | no |
| q83 | 9 | 9 | 0 | 0 | no | no | no | no |
| q84 | 2 | 2 | 0 | 0 | yes | yes | no | no |
| q85 | 7 | 8 | 0 | 0 | yes | no | yes | YES: NEW errors: execution error: type_coercion |
| q86 | 3 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q87 | 9 | 5 | 0 | 0 | no | no | no | YES: different island split (9 vs 5 islands) |
| q88 | 32 | 18 | 24 | 24 | yes | yes | no | YES: different island split (32 vs 18 islands) |
| q89 | 4 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q90 | 8 | 5 | 0 | 0 | no | no | no | YES: different island split (8 vs 5 islands) |
| q91 | 4 | 4 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: type_coercion |
| q92 | 5 | 5 | 0 | 0 | no | no | yes | YES: NEW errors: execution error: type_coercion |
| q93 | 2 | 0 | 0 | 0 | yes | no | yes | YES: NEW errors: parse error: unsupported SQL: Implicit JOIN |
| q94 | 4 | 4 | 0 | 0 | no | no | yes | YES: NEW errors: execution error: type_coercion |
| q95 | 7 | 6 | 0 | 0 | yes | yes | yes | YES: NEW errors: execution error: type_coercion |
| q96 | 4 | 4 | 3 | 3 | yes | yes | no | no |
| q97 | 4 | 3 | 2 | 0 | yes | no | no | YES: OLD ships dim into duck, NEW does not |
| q98 | 3 | 0 | 0 | 0 | no | no | yes | YES: NEW errors: parse error: unsupported SQL: function `window_function` |
| q99 | 5 | 5 | 4 | 0 | yes | no | no | YES: OLD ships dim into duck, NEW does not |

### Systematic divergences (tpcds)

**different island split (join/reduction placement differs)** - 32 queries: q01, q02, q04, q10, q11, q23, q24, q25, q27, q30, q31, q33, q35, q38, q45, q54, q56, q58, q59, q60, q61, q64, q66, q68, q69, q71, q74, q75, q78, q87, q88, q90
- example q01:
  - OLD: SELECT "sr_customer_sk", "sr_store_sk", "sr_return_amt", "sr_returned_date_sk" FROM "main"."store_returns" AS "store_returns" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000) || SELECT "s_state", "s_store_sk" FROM "public"."store" AS "store" WHERE ("store"."s_state" = 'TN') || SELECT "c_customer_id", "c_customer_sk" FROM "public"."customer" AS "customer" || SELECT "sr_customer_sk", "sr_store_sk", "sr_return_amt", "sr_returned_date_sk" FROM "main"."store_returns" AS "store_returns" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000)
  - NEW: SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000) || SELECT "sr_returned_date_sk", "sr_customer_sk", "sr_store_sk", "sr_return_amt" FROM "main"."store_returns" AS "store_returns" WHERE sr_returned_date_sk IN (2451554, 2451567, 2451572, 2451581, 2451586, || SELECT "s_store_sk", "s_state" FROM "public"."store" AS "store" WHERE (("store"."s_state" = 'TN') AND "s_store_sk" IN (1))

**NEW parse gap: window functions unsupported** - 14 queries: q12, q20, q36, q44, q47, q51, q53, q57, q63, q67, q70, q86, q89, q98
- example q12:
  - OLD: SELECT "ws_ext_sales_price", "ws_sold_date_sk", "ws_item_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_date", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN CAST('1999-02-22' AS DATE) AND CAST('1999-03-24' AS DATE)) || SELECT "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price", "i_item_sk" FROM "public"."item" AS "item" WHERE ("item"."i_category" IN ('Sports', 'Books', 'Home'))
  - NEW: ERROR: RuntimeError: parse error: unsupported SQL: function `window_function`

**NEW execution gap: type_coercion failure** - 6 queries: q16, q85, q91, q92, q94, q95
- example q16:
  - OLD: SELECT "cs1"."cs_ext_ship_cost" AS "cs_ext_ship_cost", "cs1"."cs_net_profit" AS "cs_net_profit", "cs1"."cs_order_number" AS "cs_order_number", "cs1"."cs_ship_date_sk" AS "cs_ship_date_sk", "cs1"."cs_s || SELECT "ca_state", "ca_address_sk" FROM "public"."customer_address" AS "customer_address" WHERE ("customer_address"."ca_state" = 'GA') || SELECT "d_date", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN '2002-02-01' AND CAST('2002-04-02' AS DATE)) || SELECT "cc_county", "cc_call_center_sk" FROM "public"."call_center" AS "call_center" WHERE ("call_center"."cc_county" = 'Williamson County')
  - NEW: ERROR: RuntimeError: execution error: type_coercion

**NEW parse gap: comma/implicit JOIN unsupported** - 4 queries: q40, q49, q80, q93
- example q40:
  - OLD: SELECT "catalog_sales"."cs_order_number" AS "cs_order_number", "catalog_sales"."cs_item_sk" AS "cs_item_sk", "catalog_sales"."cs_sold_date_sk" AS "cs_sold_date_sk", "catalog_sales"."cs_sales_price" AS || SELECT "i_item_id", "i_current_price", "i_item_sk" FROM "public"."item" AS "item" WHERE ("item"."i_current_price" BETWEEN 0.99 AND 1.49) || SELECT "d_date", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN CAST('2000-02-10' AS DATE) AND CAST('2000-04-10' AS DATE)) || SELECT "w_state", "w_warehouse_sk" FROM "public"."warehouse" AS "warehouse"
  - NEW: ERROR: RuntimeError: parse error: unsupported SQL: Implicit JOIN

**NEW planning gap: ROLLUP unsupported** - 4 queries: q05, q18, q22, q77
- example q05:
  - OLD: SELECT "store_sales"."ss_store_sk" AS "store_sk", "store_sales"."ss_sold_date_sk" AS "date_sk", "store_sales"."ss_ext_sales_price" AS "sales_price", "store_sales"."ss_net_profit" AS "profit", CAST(0 A || SELECT "store_returns"."sr_store_sk" AS "store_sk", "store_returns"."sr_returned_date_sk" AS "date_sk", CAST(0 AS DECIMAL(7, 2)) AS "sales_price", CAST(0 AS DECIMAL(7, 2)) AS "profit", "store_returns" || SELECT "d_date_sk", "d_date" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN CAST('2000-08-23' AS DATE) AND CAST('2000-09-06' AS DATE)) || SELECT "s_store_id", "s_store_sk" FROM "public"."store" AS "store" || SELECT "catalog_sales"."cs_catalog_page_sk" AS "page_sk", "catalog_sales"."cs_sold_date_sk" AS "date_sk", "catalog_sales"."cs_ext_sales_price" AS "sales_price", "catalog_sales"."cs_net_profit" AS "pro || SELECT "catalog_returns"."cr_catalog_page_sk" AS "page_sk", "catalog_returns"."cr_returned_date_sk" AS "date_sk", CAST(0 AS DECIMAL(7, 2)) AS "sales_price", CAST(0 AS DECIMAL(7, 2)) AS "profit", "cata || SELECT "d_date_sk", "d_date" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN CAST('2000-08-23' AS DATE) AND CAST('2000-09-06' AS DATE)) || SELECT "cp_catalog_page_id", "cp_catalog_page_sk" FROM "public"."catalog_page" AS "catalog_page" || SELECT "web_sales"."ws_web_site_sk" AS "wsr_web_site_sk", "web_sales"."ws_sold_date_sk" AS "date_sk", "web_sales"."ws_ext_sales_price" AS "sales_price", "web_sales"."ws_net_profit" AS "profit", CAST(0 || SELECT "web_sales"."ws_web_site_sk" AS "wsr_web_site_sk", "web_returns"."wr_returned_date_sk" AS "date_sk", CAST(0 AS DECIMAL(7, 2)) AS "sales_price", CAST(0 AS DECIMAL(7, 2)) AS "profit", "web_return || SELECT "d_date_sk", "d_date" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN CAST('2000-08-23' AS DATE) AND CAST('2000-09-06' AS DATE)) || SELECT "web_site_id", "web_site_sk" FROM "public"."web_site" AS "web_site"
  - NEW: ERROR: RuntimeError: execution error: Error during planning: function 'rollup' not supported

**OLD ships dim into duck, NEW does not** - 3 queries: q65, q97, q99
- example q65:
  - OLD: SELECT "s_store_name", "s_store_sk" FROM "public"."store" AS "store" || SELECT "d_date_sk", "d_month_seq" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_month_seq" BETWEEN 1176 AND (1176 + 11)) || SELECT "store_sales"."ss_store_sk" AS "ss_store_sk", "store_sales"."ss_item_sk" AS "ss_item_sk", SUM("store_sales"."ss_sales_price") AS "revenue" FROM "main"."store_sales" AS "store_sales" INNER JOIN  || SELECT "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand", "i_item_sk" FROM "public"."item" AS "item" || SELECT "d_date_sk", "d_month_seq" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_month_seq" BETWEEN 1176 AND (1176 + 11)) || SELECT "sa"."ss_store_sk" AS "ss_store_sk", AVG("sa"."revenue") AS "ave" FROM (SELECT "store_sales"."ss_store_sk" AS "ss_store_sk", "store_sales"."ss_item_sk" AS "ss_item_sk", SUM("store_sales"."ss_sa
  - NEW: SELECT "d_date_sk", "d_month_seq" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_month_seq" BETWEEN 1176 AND (1176 + 11)) || SELECT "ss_sold_date_sk", "ss_item_sk", "ss_store_sk", "ss_sales_price" FROM "main"."store_sales" AS "store_sales" WHERE ss_sold_date_sk IN (2450830, 2450841, 2450873, 2450875, 2450923, 2450926, 24509 || SELECT "s_store_sk", "s_store_name" FROM "public"."store" AS "store" WHERE "s_store_sk" IN (1) || SELECT "i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand" FROM "public"."item" AS "item" WHERE "i_item_sk" IN (1100, 22, 1556, 727, 937, 535, 859, 1760, 830, 638, 1006, 277, 

**NEW errors: execution error: adbc execute: InvalidArguments: Failed to prepare que** - 2 queries: q08, q41
- example q08:
  - OLD: SELECT SUBSTRING("customer_address"."ca_zip", 1, 5) AS "ca_zip" FROM "public"."customer_address" AS "customer_address" WHERE (SUBSTRING("customer_address"."ca_zip", 1, 5) IN ('24128', '76232', '65084' || SELECT SUBSTRING("customer_address"."ca_zip", 1, 5) AS "ca_zip", COUNT(*) AS "cnt" FROM "public"."customer_address" AS "customer_address" INNER JOIN "public"."customer" AS "customer" ON ("customer_add || SELECT "ss_net_profit", "ss_store_sk", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_year", "d_qoy", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE (("date_dim"."d_qoy" = 2) AND ("date_dim"."d_year" = 1998)) || SELECT "s_store_name", "s_zip", "s_store_sk" FROM "public"."store" AS "store"
  - NEW: ERROR: RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  count(*) must be used to call a parameterless aggregate function

**NEW parse gap: stddev_samp unsupported** - 2 queries: q17, q39
- example q17:
  - OLD: SELECT "d_quarter_name", "d_date_sk" FROM "public"."date_dim" AS "d1" WHERE ("d1"."d_quarter_name" = '2001Q1') || SELECT "d_quarter_name", "d_date_sk" FROM "public"."date_dim" AS "d3" WHERE ("d3"."d_quarter_name" IN ('2001Q1', '2001Q2', '2001Q3')) || SELECT "d_quarter_name", "d_date_sk" FROM "public"."date_dim" AS "d2" WHERE ("d2"."d_quarter_name" IN ('2001Q1', '2001Q2', '2001Q3')) || SELECT "i_item_id", "i_item_desc", "i_item_sk" FROM "public"."item" AS "item" || SELECT "s_state", "s_store_sk" FROM "public"."store" AS "store" || SELECT "item"."i_item_id" AS "i_item_id", "item"."i_item_desc" AS "i_item_desc", "store"."s_state" AS "s_state", COUNT("store_sales"."ss_quantity") AS "store_sales_quantitycount", AVG("store_sales"."s
  - NEW: ERROR: RuntimeError: parse error: unsupported SQL: function `stddev_samp`

**NEW panic: ColumnRef type unset during binding** - 1 queries: q32
- example q32:
  - OLD: SELECT "cs_ext_discount_amt", "cs_sold_date_sk", "cs_item_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "i_manufact_id", "i_item_sk" FROM "public"."item" AS "item" WHERE ("item"."i_manufact_id" = 977) || SELECT "d_date_sk", "d_date" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN '2000-01-27' AND CAST('2000-04-26' AS DATE)) || SELECT "d_date_sk", "d_date" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_date" BETWEEN '2000-01-27' AND CAST('2000-04-26' AS DATE)) || SELECT "catalog_sales"."cs_item_sk" AS "__subq_32_g0", AVG("catalog_sales"."cs_ext_discount_amt") AS "__subq_32_v0" FROM "main"."catalog_sales" AS "catalog_sales" INNER JOIN "temp"."__fedq_ship_1" AS 
  - NEW: ERROR: PanicException: ColumnRef type must be set during binding

**NEW parse gap: UNION in this position unsupported** - 1 queries: q14
- example q14:
  - OLD: SELECT "ss_quantity", "ss_list_price", "ss_sold_date_sk", "ss_item_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "i_item_sk", "i_category_id", "i_brand_id", "i_class_id" FROM "public"."item" AS "item" || SELECT "ss_item_sk", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d1" WHERE ("d1"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iss" || SELECT "cs_item_sk", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d2" WHERE ("d2"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "ics" || SELECT "ws_item_sk", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d3" WHERE ("d3"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iws" || SELECT "d_moy", "d_year", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE (("date_dim"."d_year" = (1999 + 2)) AND ("date_dim"."d_moy" = 11)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "item" || SELECT "ss_quantity", "ss_list_price", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "cs_quantity", "cs_list_price", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "ws_quantity", "ws_list_price", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "cs_quantity", "cs_list_price", "cs_sold_date_sk", "cs_item_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "i_item_sk", "i_category_id", "i_brand_id", "i_class_id" FROM "public"."item" AS "item" || SELECT "ss_item_sk", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d1" WHERE ("d1"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iss" || SELECT "cs_item_sk", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d2" WHERE ("d2"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "ics" || SELECT "ws_item_sk", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d3" WHERE ("d3"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iws" || SELECT "d_moy", "d_year", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE (("date_dim"."d_year" = (1999 + 2)) AND ("date_dim"."d_moy" = 11)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "item" || SELECT "ss_quantity", "ss_list_price", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "cs_quantity", "cs_list_price", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "ws_quantity", "ws_list_price", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "ws_quantity", "ws_list_price", "ws_sold_date_sk", "ws_item_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "i_item_sk", "i_category_id", "i_brand_id", "i_class_id" FROM "public"."item" AS "item" || SELECT "ss_item_sk", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d1" WHERE ("d1"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iss" || SELECT "cs_item_sk", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d2" WHERE ("d2"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "ics" || SELECT "ws_item_sk", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_year", "d_date_sk" FROM "public"."date_dim" AS "d3" WHERE ("d3"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "iws" || SELECT "d_moy", "d_year", "d_date_sk" FROM "public"."date_dim" AS "date_dim" WHERE (("date_dim"."d_year" = (1999 + 2)) AND ("date_dim"."d_moy" = 11)) || SELECT "i_brand_id", "i_class_id", "i_category_id", "i_item_sk" FROM "public"."item" AS "item" || SELECT "ss_quantity", "ss_list_price", "ss_sold_date_sk" FROM "main"."store_sales" AS "store_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "cs_quantity", "cs_list_price", "cs_sold_date_sk" FROM "main"."catalog_sales" AS "catalog_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2)) || SELECT "ws_quantity", "ws_list_price", "ws_sold_date_sk" FROM "main"."web_sales" AS "web_sales" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" BETWEEN 1999 AND (1999 + 2))
  - NEW: ERROR: RuntimeError: parse error: unsupported SQL: function `union`

**OLD pushes agg/join, NEW leaves it in coordinator** - 1 queries: q81
- example q81:
  - OLD: SELECT "customer_address"."ca_street_number" AS "ca_street_number", "customer_address"."ca_street_name" AS "ca_street_name", "customer_address"."ca_street_type" AS "ca_street_type", "customer_address" || SELECT "cr_returning_customer_sk", "cr_return_amt_inc_tax", "cr_returning_addr_sk", "cr_returned_date_sk" FROM "main"."catalog_returns" AS "catalog_returns" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000) || SELECT "ca_state", "ca_address_sk" FROM "public"."customer_address" AS "customer_address" || SELECT "cr_returning_customer_sk", "cr_return_amt_inc_tax", "cr_returning_addr_sk", "cr_returned_date_sk" FROM "main"."catalog_returns" AS "catalog_returns" || SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000) || SELECT "ca_state", "ca_address_sk" FROM "public"."customer_address" AS "customer_address"
  - NEW: SELECT "d_date_sk", "d_year" FROM "public"."date_dim" AS "date_dim" WHERE ("date_dim"."d_year" = 2000) || SELECT "cr_returned_date_sk", "cr_returning_customer_sk", "cr_returning_addr_sk", "cr_return_amt_inc_tax" FROM "main"."catalog_returns" AS "catalog_returns" WHERE cr_returned_date_sk IN (2451553, 2451
