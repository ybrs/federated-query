# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each engine run is an isolated child process; the DuckDB oracle (with PostgreSQL attached) runs in the parent.

Correctness is differential: fedq reads each table from its placed source while DuckDB reads the SAME split through its postgres connector, so both compute the exact federated answer and a MISMATCH is a real cross-source engine bug. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 23 | MISMATCH 0 | ERROR 76 | cross-source 97

### Failure clusters

### Binding: reference not in scope (30)
Queries: q01, q04, q06, q08, q10, q11, q15, q18, q19, q23, q24, q30, q34, q35, q38, q45, q46, q49, q54, q61, q64, q68, q69, q73, q74, q79, q81, q84, q87, q91

- BindingError: Column 'c_customer_sk' not found in any table in scope

### Other (25)
Queries: q02, q03, q05, q12, q14, q16, q20, q27, q36, q42, q52, q58, q66, q71, q75, q76, q77, q80, q85, q86, q92, q94, q95, q96, q98

- UnsupportedIR: physical node PhysicalUnion not supported yet

### Star over subquery/CTE (14)
Queries: q21, q28, q33, q44, q47, q51, q53, q56, q57, q60, q63, q67, q88, q89

- StarExpansionError: Star expansion only supports base tables

### Unsupported function NULLIF (5)
Queries: q31, q59, q78, q83, q90

- ExpressionEvaluationError: Unsupported function: NULLIF

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
| q21 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q24 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q25 | PASS | cross | 0 / 0 | rows and values match |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q28 | ERROR | single | - | StarExpansionError: Star expansion only supports base tables |
| q29 | PASS | cross | 0 / 0 | rows and values match |
| q30 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q31 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
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
| q44 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q45 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q46 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q47 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q52 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_ext_sales_price. Valid fields are in_0.ss_sold_date_sk, in_0.ss_item_sk, in_0.i_brand_id, in_0.i_brand, in_0.i_manager_id, in_0.i_item_sk, in_0.d_year, in_0.d_moy, in_0.d_date_sk. |
| q53 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q54 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q55 | PASS | cross | 20 / 20 | rows and values match |
| q56 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q57 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q58 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q59 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q60 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q61 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q62 | PASS | cross | 6 / 6 | rows and values match |
| q63 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q64 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q67 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q68 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q69 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q72 | PASS | cross | 50 / 50 | rows and values match |
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
| q88 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q89 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
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
