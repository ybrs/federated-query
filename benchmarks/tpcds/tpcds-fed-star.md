# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness is differential: fedq reads each table from its placed source while DuckDB reads the SAME split through its postgres connector, so both compute the exact federated answer and a MISMATCH is a real cross-source engine bug. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 23 | MISMATCH 0 | ERROR 76 | cross-source 97

### Failure clusters

### Other (38)
Queries: q02, q03, q05, q12, q14, q16, q20, q27, q28, q33, q36, q42, q44, q47, q51, q52, q53, q56, q57, q58, q60, q63, q66, q67, q71, q75, q76, q77, q80, q85, q86, q88, q89, q92, q94, q95, q96, q98

- UnsupportedIR: physical node PhysicalUnion not supported yet

### Binding: reference not in scope (30)
Queries: q01, q04, q06, q08, q10, q11, q15, q18, q19, q23, q24, q30, q34, q35, q38, q45, q46, q49, q54, q61, q64, q68, q69, q73, q74, q79, q81, q84, q87, q91

- BindingError: Column 'c_customer_sk' not found in any table in scope

### Unsupported function NULLIF (5)
Queries: q31, q59, q78, q83, q90

- ExpressionEvaluationError: Unsupported function: NULLIF

### Out of memory (1)
Queries: q72

- oracle: OutOfMemoryException: Out of Memory Error: Failed to allocate block of 2147483648 bytes (bad allocation)

### Simple CASE unsupported (1)
Queries: q39

- UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...)

### Window in WHERE (1)
Queries: q70

- UnsupportedSQLError: window functions are not allowed in WHERE

### Per-query matrix

| Query | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- |
| q01 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q02 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q03 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_ext_sales_price. Valid fields are in_0.ss_sold_date_sk, in_0.ss_item_sk, in_0.i_brand_id, in_0.i_brand, in_0.i_manufact_id, in_0.i_item_sk, in_0.d_year, in_0.d_moy, in_0.d_date_sk. |
| q04 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q05 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q06 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q07 | PASS | cross | 100 / 100 | rows and values match |
| q08 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q09 | PASS | cross | 1 / 1 | rows and values match |
| q10 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q11 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q12 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q13 | PASS | cross | 1 / 1 | rows and values match |
| q14 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q15 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q16 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.cs_ext_ship_cost. Valid fields are in_0.cs_call_center_sk, in_0.cs_order_number, in_0.cs_ship_addr_sk, in_0.cs_warehouse_sk, in_0.cs_ship_date_sk, in_0.ca_address_sk, in_0.ca_state, in_0.d_date_sk, in_0.d_date, in_0.cc_call_center_sk, in_0.cc_county. |
| q17 | PASS | cross | 1 / 1 | rows and values match |
| q18 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q19 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q20 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q21 | PASS | cross | 32 / 32 | rows and values match |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q24 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q25 | PASS | cross | 0 / 0 | rows and values match |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q28 | ERROR | single | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q29 | PASS | cross | 0 / 0 | rows and values match |
| q30 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q31 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q34 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q35 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q36 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q37 | PASS | cross | 0 / 0 | rows and values match |
| q38 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q39 | ERROR | cross | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | cross | 44 / 44 | rows and values match |
| q41 | PASS | single | 0 / 0 | rows and values match |
| q42 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_ext_sales_price. Valid fields are in_0.ss_sold_date_sk, in_0.ss_item_sk, in_0.i_category_id, in_0.i_category, in_0.i_manager_id, in_0.i_item_sk, in_0.d_year, in_0.d_moy, in_0.d_date_sk. |
| q43 | PASS | cross | 1 / 1 | rows and values match |
| q44 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q45 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q46 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q47 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q52 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_ext_sales_price. Valid fields are in_0.ss_sold_date_sk, in_0.ss_item_sk, in_0.i_brand_id, in_0.i_brand, in_0.i_manager_id, in_0.i_item_sk, in_0.d_year, in_0.d_moy, in_0.d_date_sk. |
| q53 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q54 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q55 | PASS | cross | 20 / 20 | rows and values match |
| q56 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q57 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q58 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q59 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q60 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q61 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q62 | PASS | cross | 6 / 6 | rows and values match |
| q63 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q64 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q67 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q68 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q69 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q72 | ERROR | cross | - | oracle: OutOfMemoryException: Out of Memory Error: Failed to allocate block of 2147483648 bytes (bad allocation) |
| q73 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q74 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q75 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q76 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q77 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q78 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q79 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q80 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q81 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q84 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q85 | ERROR | cross | - | RuntimeError: type_coercion |
| q86 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q87 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q88 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q89 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q90 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q91 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q92 | ERROR | cross | - | RuntimeError: type_coercion |
| q93 | PASS | cross | 0 / 0 | rows and values match |
| q94 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ws_ext_ship_cost. Valid fields are in_0.ws_web_site_sk, in_0.ws_order_number, in_0.ws_ship_addr_sk, in_0.ws_warehouse_sk, in_0.ws_ship_date_sk, in_0.ca_address_sk, in_0.ca_state, in_0.d_date_sk, in_0.d_date, in_0.web_site_sk, in_0.web_company_name. |
| q95 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ws_ext_ship_cost. Valid fields are in_0.ws_order_number, in_0.ws_warehouse_sk, in_0.ws_web_site_sk, in_0.ws_ship_addr_sk, in_0.ws_ship_date_sk, in_0.d_date_sk, in_0.d_date, in_0.web_site_sk, in_0.web_company_name, in_0.ca_address_sk, in_0.ca_state. |
| q96 | ERROR | cross | - | AttributeError: 'NoneType' object has no attribute 'datasource' |
| q97 | PASS | cross | 1 / 1 | rows and values match |
| q98 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q99 | PASS | cross | 6 / 6 | rows and values match |


### Timings (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| query | engine ms | duck ms | ratio |
|---|---|---|---|
| q07 | 65.2 | 28.2 | 2.31x |
| q09 | 122.6 | 8.8 | 13.93x |
| q13 | 107.8 | 47.1 | 2.29x |
| q17 | 135.1 | 32.4 | 4.18x |
| q21 | 61.3 | 11.4 | 5.39x |
| q22 | 49.0 | 20.5 | 2.39x |
| q25 | 91.0 | 24.0 | 3.80x |
| q26 | 67.9 | 24.5 | 2.77x |
| q29 | 133.3 | 25.7 | 5.18x |
| q32 | 42.0 | 11.7 | 3.58x |
| q37 | 28.8 | 12.3 | 2.34x |
| q40 | 64.2 | 15.5 | 4.14x |
| q41 | 37.8 | 9.0 | 4.20x |
| q43 | 47.3 | 17.4 | 2.72x |
| q48 | 108.3 | 51.7 | 2.10x |
| q50 | 70.3 | 21.3 | 3.30x |
| q55 | 36.7 | 12.1 | 3.04x |
| q62 | 77.4 | 14.9 | 5.18x |
| q65 | 77.8 | 17.9 | 4.34x |
| q82 | 38.5 | 9.1 | 4.24x |
| q93 | 25.5 | 4.6 | 5.54x |
| q97 | 55.6 | 17.1 | 3.25x |
| q99 | 96.6 | 16.7 | 5.77x |

Geomean ratio (engine/duck): 3.77x over 23 queries.