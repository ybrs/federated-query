# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 60.0s, memory cap 12288 MB. Each engine run is an isolated child process; the DuckDB oracle (with PostgreSQL attached) runs in the parent.

Correctness is differential: fedq reads each table from its placed source while DuckDB reads the SAME split through its postgres connector, so both compute the exact federated answer and a MISMATCH is a real cross-source engine bug. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 15 | MISMATCH 0 | ERROR 84 | cross-source 97

### Failure clusters

### Binding: reference not in scope (25)
Queries: q11, q15, q18, q19, q23, q24, q30, q34, q35, q38, q45, q46, q49, q54, q61, q64, q68, q69, q73, q74, q79, q81, q84, q87, q91

- BindingError: Column 'c_customer_sk' not found in any table in scope

### Star over subquery/CTE (14)
Queries: q21, q28, q33, q44, q47, q51, q53, q56, q57, q60, q63, q67, q88, q89

- StarExpansionError: Star expansion only supports base tables

### Out of memory (7)
Queries: q05, q27, q31, q36, q77, q80, q83

- InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes

### Timeout (7)
Queries: q01, q02, q10, q14, q59, q95, q97

- Timeout: exceeded 60.0s

### Other (6)
Queries: q17, q25, q29, q58, q66, q72

- Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_quarter_name) in ss_sold_date_sk: int64

### Cross-source column resolution (5)
Queries: q03, q16, q42, q52, q94

- ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manufact_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')]

### DuckDB binder (oracle side) (4)
Queries: q55, q85, q92, q96

- BinderException: Binder Error: Referenced column "i_brand_id" not found in FROM clause!

### Decimal precision (3)
Queries: q43, q76, q86

- ArrowInvalid: Decimal value does not fit in precision 7

### Internal exception (3)
Queries: q12, q20, q98

- InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]})

### Join-key orientation (3)
Queries: q62, q71, q99

- ValueError: cannot orient join keys 'ws_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name

### Unsupported function NULLIF (3)
Queries: q75, q78, q90

- ExpressionEvaluationError: Unsupported function: NULLIF

### Postgres text decode (UnicodeDecodeError) (1)
Queries: q04

- UnicodeDecodeError: 'ascii' codec can't decode byte 0xc3 in position 1: ordinal not in range(128)

### Scalar subquery > 1 row (1)
Queries: q06

- CardinalityViolationError: Scalar subquery returned more than one row

### Simple CASE unsupported (1)
Queries: q39

- UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...)

### Window in WHERE (1)
Queries: q70

- UnsupportedSQLError: window functions are not allowed in WHERE

### Per-query matrix

| Query | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- |
| q01 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q02 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q03 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manufact_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q04 | ERROR | cross | - | UnicodeDecodeError: 'ascii' codec can't decode byte 0xc3 in position 1: ordinal not in range(128) |
| q05 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q06 | ERROR | cross | - | CardinalityViolationError: Scalar subquery returned more than one row |
| q07 | PASS | cross | 100 / 100 | rows and values match |
| q08 | PASS | cross | 0 / 0 | rows and values match |
| q09 | PASS | cross | 1 / 1 | rows and values match |
| q10 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q11 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q12 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q13 | PASS | cross | 1 / 1 | rows and values match |
| q14 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q15 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q16 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference cs1.cs_ext_ship_cost; relation exposes [('call_center', 'cc_call_center_id'), ('call_center', 'cc_call_center_sk'), ('call_center', 'cc_city'), ('call_center', 'cc_class'), ('call_center', 'cc_closed_date_sk'), ('call_center', 'cc_company'), ('call_center', 'cc_company_name'), ('call_center', 'cc_country'), ('call_center', 'cc_county'), ('call_center', 'cc_division'), ('call_center', 'cc_division_name'), ('call_center', 'cc_employees'), ('call_center', 'cc_gmt_offset'), ('call_center', 'cc_hours'), ('call_center', 'cc_manager'), ('call_center', 'cc_market_manager'), ('call_center', 'cc_mkt_class'), ('call_center', 'cc_mkt_desc'), ('call_center', 'cc_mkt_id'), ('call_center', 'cc_name'), ('call_center', 'cc_open_date_sk'), ('call_center', 'cc_rec_end_date'), ('call_center', 'cc_rec_start_date'), ('call_center', 'cc_sq_ft'), ('call_center', 'cc_state'), ('call_center', 'cc_street_name'), ('call_center', 'cc_street_number'), ('call_center', 'cc_street_type'), ('call_center', 'cc_suite_number'), ('call_center', 'cc_tax_percentage'), ('call_center', 'cc_zip'), ('cs1', 'cs_call_center_sk'), ('cs1', 'cs_order_number'), ('cs1', 'cs_ship_addr_sk'), ('cs1', 'cs_ship_date_sk'), ('cs1', 'cs_warehouse_sk'), ('customer_address', 'ca_address_id'), ('customer_address', 'ca_address_sk'), ('customer_address', 'ca_city'), ('customer_address', 'ca_country'), ('customer_address', 'ca_county'), ('customer_address', 'ca_gmt_offset'), ('customer_address', 'ca_location_type'), ('customer_address', 'ca_state'), ('customer_address', 'ca_street_name'), ('customer_address', 'ca_street_number'), ('customer_address', 'ca_street_type'), ('customer_address', 'ca_suite_number'), ('customer_address', 'ca_zip'), ('date_dim', 'd_current_day'), ('date_dim', 'd_current_month'), ('date_dim', 'd_current_quarter'), ('date_dim', 'd_current_week'), ('date_dim', 'd_current_year'), ('date_dim', 'd_date'), ('date_dim', 'd_date_id'), ('date_dim', 'd_date_sk'), ('date_dim', 'd_day_name'), ('date_dim', 'd_dom'), ('date_dim', 'd_dow'), ('date_dim', 'd_first_dom'), ('date_dim', 'd_following_holiday'), ('date_dim', 'd_fy_quarter_seq'), ('date_dim', 'd_fy_week_seq'), ('date_dim', 'd_fy_year'), ('date_dim', 'd_holiday'), ('date_dim', 'd_last_dom'), ('date_dim', 'd_month_seq'), ('date_dim', 'd_moy'), ('date_dim', 'd_qoy'), ('date_dim', 'd_quarter_name'), ('date_dim', 'd_quarter_seq'), ('date_dim', 'd_same_day_lq'), ('date_dim', 'd_same_day_ly'), ('date_dim', 'd_week_seq'), ('date_dim', 'd_weekend'), ('date_dim', 'd_year')] |
| q17 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_quarter_name) in ss_sold_date_sk: int64 |
| q18 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q19 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q20 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q21 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q24 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q25 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_year) in ss_sold_date_sk: int64 |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q28 | ERROR | single | - | StarExpansionError: Star expansion only supports base tables |
| q29 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_year) in ss_sold_date_sk: int64 |
| q30 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q31 | ERROR | cross | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q34 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q35 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q36 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q37 | PASS | cross | 0 / 0 | rows and values match |
| q38 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q39 | ERROR | cross | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | cross | 44 / 44 | rows and values match |
| q41 | PASS | single | 0 / 0 | rows and values match |
| q42 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_category'), ('item', 'i_category_id'), ('item', 'i_item_sk'), ('item', 'i_manager_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q43 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q44 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q45 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q46 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q47 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q52 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manager_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q53 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q54 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q55 | ERROR | cross | - | BinderException: Binder Error: Referenced column "i_brand_id" not found in FROM clause! |
| q56 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q57 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q58 | ERROR | cross | - | ArrowNotImplementedError: Function 'equal' has no kernel matching input types (date32[day], string) |
| q59 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q60 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q61 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q62 | ERROR | cross | - | ValueError: cannot orient join keys 'ws_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |
| q63 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q64 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | ERROR | cross | - | ArrowInvalid: Failed to parse string: 'DHL,BARIAN' as a scalar of type int64 |
| q67 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q68 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q69 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | cross | - | ValueError: cannot orient join keys 'sold_item_sk' / 'i_item_sk' to a join side; neither resolves by qualifier or by column name |
| q72 | ERROR | cross | - | ArrowNotImplementedError: Function 'add' has no kernel matching input types (timestamp[s], int64) |
| q73 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q74 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q75 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q76 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q77 | ERROR | cross | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q78 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q79 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q80 | ERROR | cross | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q81 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | ERROR | cross | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q84 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q85 | ERROR | cross | - | BinderException: Binder Error: Referenced column "r_reason_desc" not found in FROM clause! |
| q86 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q87 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q88 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q89 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q90 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q91 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q92 | ERROR | cross | - | BinderException: Binder Error: Referenced column "ws_ext_discount_amt" not found in FROM clause! |
| q93 | PASS | cross | 0 / 0 | rows and values match |
| q94 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference ws1.ws_ext_ship_cost; relation exposes [('customer_address', 'ca_address_id'), ('customer_address', 'ca_address_sk'), ('customer_address', 'ca_city'), ('customer_address', 'ca_country'), ('customer_address', 'ca_county'), ('customer_address', 'ca_gmt_offset'), ('customer_address', 'ca_location_type'), ('customer_address', 'ca_state'), ('customer_address', 'ca_street_name'), ('customer_address', 'ca_street_number'), ('customer_address', 'ca_street_type'), ('customer_address', 'ca_suite_number'), ('customer_address', 'ca_zip'), ('date_dim', 'd_current_day'), ('date_dim', 'd_current_month'), ('date_dim', 'd_current_quarter'), ('date_dim', 'd_current_week'), ('date_dim', 'd_current_year'), ('date_dim', 'd_date'), ('date_dim', 'd_date_id'), ('date_dim', 'd_date_sk'), ('date_dim', 'd_day_name'), ('date_dim', 'd_dom'), ('date_dim', 'd_dow'), ('date_dim', 'd_first_dom'), ('date_dim', 'd_following_holiday'), ('date_dim', 'd_fy_quarter_seq'), ('date_dim', 'd_fy_week_seq'), ('date_dim', 'd_fy_year'), ('date_dim', 'd_holiday'), ('date_dim', 'd_last_dom'), ('date_dim', 'd_month_seq'), ('date_dim', 'd_moy'), ('date_dim', 'd_qoy'), ('date_dim', 'd_quarter_name'), ('date_dim', 'd_quarter_seq'), ('date_dim', 'd_same_day_lq'), ('date_dim', 'd_same_day_ly'), ('date_dim', 'd_week_seq'), ('date_dim', 'd_weekend'), ('date_dim', 'd_year'), ('web_site', 'web_city'), ('web_site', 'web_class'), ('web_site', 'web_close_date_sk'), ('web_site', 'web_company_id'), ('web_site', 'web_company_name'), ('web_site', 'web_country'), ('web_site', 'web_county'), ('web_site', 'web_gmt_offset'), ('web_site', 'web_manager'), ('web_site', 'web_market_manager'), ('web_site', 'web_mkt_class'), ('web_site', 'web_mkt_desc'), ('web_site', 'web_mkt_id'), ('web_site', 'web_name'), ('web_site', 'web_open_date_sk'), ('web_site', 'web_rec_end_date'), ('web_site', 'web_rec_start_date'), ('web_site', 'web_site_id'), ('web_site', 'web_site_sk'), ('web_site', 'web_state'), ('web_site', 'web_street_name'), ('web_site', 'web_street_number'), ('web_site', 'web_street_type'), ('web_site', 'web_suite_number'), ('web_site', 'web_tax_percentage'), ('web_site', 'web_zip'), ('ws1', 'ws_order_number'), ('ws1', 'ws_ship_addr_sk'), ('ws1', 'ws_ship_date_sk'), ('ws1', 'ws_warehouse_sk'), ('ws1', 'ws_web_site_sk')] |
| q95 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q96 | ERROR | cross | - | BinderException: Binder Error: Referenced column "s_store_sk" not found in FROM clause! |
| q97 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q98 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q99 | ERROR | cross | - | ValueError: cannot orient join keys 'cs_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |

## Placement: adversarial

[adversarial] Total 99 | PASS 14 | MISMATCH 0 | ERROR 85 | cross-source 95

### Failure clusters

### Binding: reference not in scope (30)
Queries: q01, q04, q06, q08, q10, q11, q15, q18, q19, q23, q24, q30, q34, q35, q38, q45, q46, q49, q54, q61, q64, q68, q69, q73, q74, q79, q81, q84, q87, q91

- BindingError: Column 'c_customer_sk' not found in any table in scope

### Star over subquery/CTE (14)
Queries: q21, q28, q33, q44, q47, q51, q53, q56, q57, q60, q63, q67, q88, q89

- StarExpansionError: Star expansion only supports base tables

### Out of memory (7)
Queries: q05, q27, q31, q36, q77, q80, q83

- InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 4194304 bytes

### Other (6)
Queries: q17, q25, q29, q58, q66, q72

- Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_quarter_name) in ss_sold_date_sk: int64

### Cross-source column resolution (5)
Queries: q03, q16, q42, q52, q94

- ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manufact_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')]

### Timeout (5)
Queries: q02, q14, q59, q95, q97

- Timeout: exceeded 60.0s

### DuckDB binder (oracle side) (4)
Queries: q55, q85, q92, q96

- BinderException: Binder Error: Referenced column "i_brand_id" not found in FROM clause!

### Decimal precision (3)
Queries: q43, q76, q86

- ArrowInvalid: Decimal value does not fit in precision 7

### Internal exception (3)
Queries: q12, q20, q98

- InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]})

### Join-key orientation (3)
Queries: q62, q71, q99

- ValueError: cannot orient join keys 'ws_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name

### Unsupported function NULLIF (3)
Queries: q75, q78, q90

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
| q02 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q03 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manufact_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q04 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q05 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 4194304 bytes |
| q06 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q07 | PASS | cross | 100 / 100 | rows and values match |
| q08 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q09 | PASS | single | 1 / 1 | rows and values match |
| q10 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q11 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q12 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q13 | PASS | cross | 1 / 1 | rows and values match |
| q14 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q15 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q16 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference cs1.cs_ext_ship_cost; relation exposes [('call_center', 'cc_call_center_id'), ('call_center', 'cc_call_center_sk'), ('call_center', 'cc_city'), ('call_center', 'cc_class'), ('call_center', 'cc_closed_date_sk'), ('call_center', 'cc_company'), ('call_center', 'cc_company_name'), ('call_center', 'cc_country'), ('call_center', 'cc_county'), ('call_center', 'cc_division'), ('call_center', 'cc_division_name'), ('call_center', 'cc_employees'), ('call_center', 'cc_gmt_offset'), ('call_center', 'cc_hours'), ('call_center', 'cc_manager'), ('call_center', 'cc_market_manager'), ('call_center', 'cc_mkt_class'), ('call_center', 'cc_mkt_desc'), ('call_center', 'cc_mkt_id'), ('call_center', 'cc_name'), ('call_center', 'cc_open_date_sk'), ('call_center', 'cc_rec_end_date'), ('call_center', 'cc_rec_start_date'), ('call_center', 'cc_sq_ft'), ('call_center', 'cc_state'), ('call_center', 'cc_street_name'), ('call_center', 'cc_street_number'), ('call_center', 'cc_street_type'), ('call_center', 'cc_suite_number'), ('call_center', 'cc_tax_percentage'), ('call_center', 'cc_zip'), ('cs1', 'cs_call_center_sk'), ('cs1', 'cs_order_number'), ('cs1', 'cs_ship_addr_sk'), ('cs1', 'cs_ship_date_sk'), ('cs1', 'cs_warehouse_sk'), ('customer_address', 'ca_address_id'), ('customer_address', 'ca_address_sk'), ('customer_address', 'ca_city'), ('customer_address', 'ca_country'), ('customer_address', 'ca_county'), ('customer_address', 'ca_gmt_offset'), ('customer_address', 'ca_location_type'), ('customer_address', 'ca_state'), ('customer_address', 'ca_street_name'), ('customer_address', 'ca_street_number'), ('customer_address', 'ca_street_type'), ('customer_address', 'ca_suite_number'), ('customer_address', 'ca_zip'), ('date_dim', 'd_current_day'), ('date_dim', 'd_current_month'), ('date_dim', 'd_current_quarter'), ('date_dim', 'd_current_week'), ('date_dim', 'd_current_year'), ('date_dim', 'd_date'), ('date_dim', 'd_date_id'), ('date_dim', 'd_date_sk'), ('date_dim', 'd_day_name'), ('date_dim', 'd_dom'), ('date_dim', 'd_dow'), ('date_dim', 'd_first_dom'), ('date_dim', 'd_following_holiday'), ('date_dim', 'd_fy_quarter_seq'), ('date_dim', 'd_fy_week_seq'), ('date_dim', 'd_fy_year'), ('date_dim', 'd_holiday'), ('date_dim', 'd_last_dom'), ('date_dim', 'd_month_seq'), ('date_dim', 'd_moy'), ('date_dim', 'd_qoy'), ('date_dim', 'd_quarter_name'), ('date_dim', 'd_quarter_seq'), ('date_dim', 'd_same_day_lq'), ('date_dim', 'd_same_day_ly'), ('date_dim', 'd_week_seq'), ('date_dim', 'd_weekend'), ('date_dim', 'd_year')] |
| q17 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_quarter_name) in ss_sold_date_sk: int64 |
| q18 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q19 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q20 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q21 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q24 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q25 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_year) in ss_sold_date_sk: int64 |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q28 | ERROR | single | - | StarExpansionError: Star expansion only supports base tables |
| q29 | ERROR | cross | - | Error: ArrowInvalid: Multiple matches for FieldRef.Name(right_d_year) in ss_sold_date_sk: int64 |
| q30 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q31 | ERROR | cross | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q34 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q35 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q36 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q37 | PASS | cross | 0 / 0 | rows and values match |
| q38 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q39 | ERROR | cross | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | cross | 44 / 44 | rows and values match |
| q41 | PASS | single | 0 / 0 | rows and values match |
| q42 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_category'), ('item', 'i_category_id'), ('item', 'i_item_sk'), ('item', 'i_manager_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q43 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q44 | ERROR | single | - | StarExpansionError: Star expansion only supports base tables |
| q45 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q46 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q47 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q52 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference store_sales.ss_ext_sales_price; relation exposes [('dt', 'd_date_sk'), ('dt', 'd_moy'), ('dt', 'd_year'), ('item', 'i_brand'), ('item', 'i_brand_id'), ('item', 'i_item_sk'), ('item', 'i_manager_id'), ('store_sales', 'ss_item_sk'), ('store_sales', 'ss_sold_date_sk')] |
| q53 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q54 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q55 | ERROR | cross | - | BinderException: Binder Error: Referenced column "i_brand_id" not found in FROM clause! |
| q56 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q57 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q58 | ERROR | cross | - | ArrowNotImplementedError: Function 'equal' has no kernel matching input types (date32[day], string) |
| q59 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q60 | ERROR | cross | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q61 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q62 | ERROR | cross | - | ValueError: cannot orient join keys 'ws_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |
| q63 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q64 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | ERROR | cross | - | ArrowInvalid: Failed to parse string: 'DHL,BARIAN' as a scalar of type int64 |
| q67 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q68 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q69 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in table 'c' |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | cross | - | ValueError: cannot orient join keys 'sold_item_sk' / 'i_item_sk' to a join side; neither resolves by qualifier or by column name |
| q72 | ERROR | cross | - | ArrowNotImplementedError: Function 'add' has no kernel matching input types (timestamp[s], int64) |
| q73 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q74 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q75 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q76 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q77 | ERROR | cross | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 2097152 bytes |
| q78 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q79 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q80 | ERROR | cross | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q81 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | ERROR | cross | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 134217728 bytes |
| q84 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q85 | ERROR | cross | - | BinderException: Binder Error: Referenced column "r_reason_desc" not found in FROM clause! |
| q86 | ERROR | cross | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q87 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q88 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q89 | ERROR | cross | - | StarExpansionError: Star expansion only supports base tables |
| q90 | ERROR | cross | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q91 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q92 | ERROR | cross | - | BinderException: Binder Error: Referenced column "ws_ext_discount_amt" not found in FROM clause! |
| q93 | PASS | cross | 0 / 0 | rows and values match |
| q94 | ERROR | cross | - | ColumnResolutionError: Unresolved column reference ws1.ws_ext_ship_cost; relation exposes [('customer_address', 'ca_address_id'), ('customer_address', 'ca_address_sk'), ('customer_address', 'ca_city'), ('customer_address', 'ca_country'), ('customer_address', 'ca_county'), ('customer_address', 'ca_gmt_offset'), ('customer_address', 'ca_location_type'), ('customer_address', 'ca_state'), ('customer_address', 'ca_street_name'), ('customer_address', 'ca_street_number'), ('customer_address', 'ca_street_type'), ('customer_address', 'ca_suite_number'), ('customer_address', 'ca_zip'), ('date_dim', 'd_current_day'), ('date_dim', 'd_current_month'), ('date_dim', 'd_current_quarter'), ('date_dim', 'd_current_week'), ('date_dim', 'd_current_year'), ('date_dim', 'd_date'), ('date_dim', 'd_date_id'), ('date_dim', 'd_date_sk'), ('date_dim', 'd_day_name'), ('date_dim', 'd_dom'), ('date_dim', 'd_dow'), ('date_dim', 'd_first_dom'), ('date_dim', 'd_following_holiday'), ('date_dim', 'd_fy_quarter_seq'), ('date_dim', 'd_fy_week_seq'), ('date_dim', 'd_fy_year'), ('date_dim', 'd_holiday'), ('date_dim', 'd_last_dom'), ('date_dim', 'd_month_seq'), ('date_dim', 'd_moy'), ('date_dim', 'd_qoy'), ('date_dim', 'd_quarter_name'), ('date_dim', 'd_quarter_seq'), ('date_dim', 'd_same_day_lq'), ('date_dim', 'd_same_day_ly'), ('date_dim', 'd_week_seq'), ('date_dim', 'd_weekend'), ('date_dim', 'd_year'), ('web_site', 'web_city'), ('web_site', 'web_class'), ('web_site', 'web_close_date_sk'), ('web_site', 'web_company_id'), ('web_site', 'web_company_name'), ('web_site', 'web_country'), ('web_site', 'web_county'), ('web_site', 'web_gmt_offset'), ('web_site', 'web_manager'), ('web_site', 'web_market_manager'), ('web_site', 'web_mkt_class'), ('web_site', 'web_mkt_desc'), ('web_site', 'web_mkt_id'), ('web_site', 'web_name'), ('web_site', 'web_open_date_sk'), ('web_site', 'web_rec_end_date'), ('web_site', 'web_rec_start_date'), ('web_site', 'web_site_id'), ('web_site', 'web_site_sk'), ('web_site', 'web_state'), ('web_site', 'web_street_name'), ('web_site', 'web_street_number'), ('web_site', 'web_street_type'), ('web_site', 'web_suite_number'), ('web_site', 'web_tax_percentage'), ('web_site', 'web_zip'), ('ws1', 'ws_order_number'), ('ws1', 'ws_ship_addr_sk'), ('ws1', 'ws_ship_date_sk'), ('ws1', 'ws_warehouse_sk'), ('ws1', 'ws_web_site_sk')] |
| q95 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q96 | ERROR | cross | - | BinderException: Binder Error: Referenced column "s_store_sk" not found in FROM clause! |
| q97 | ERROR | cross | - | Timeout: exceeded 60.0s |
| q98 | ERROR | cross | - | InternalException: INTERNAL Error: Failed to bind column reference "" [18.1] (bindings: {#[21.0], #[21.1], #[21.2], #[21.3], #[21.4], #[21.5], #[19.0], #[18.0]}) |
| q99 | ERROR | cross | - | ValueError: cannot orient join keys 'cs_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |
