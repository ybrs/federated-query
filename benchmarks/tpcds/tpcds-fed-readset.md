# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness is differential: fedq reads each table from its placed source while DuckDB reads the SAME split through its postgres connector, so both compute the exact federated answer and a MISMATCH is a real cross-source engine bug. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 30 | MISMATCH 0 | ERROR 69 | cross-source 97

### Failure clusters

### Other (37)
Queries: q02, q05, q12, q14, q16, q20, q27, q28, q33, q36, q42, q44, q47, q51, q53, q56, q57, q58, q60, q63, q66, q67, q71, q75, q76, q77, q78, q80, q85, q86, q88, q89, q92, q94, q95, q96, q98

- UnsupportedIR: physical node PhysicalUnion not supported yet

### Binding: reference not in scope (30)
Queries: q01, q04, q06, q08, q10, q11, q15, q18, q19, q23, q24, q30, q34, q35, q38, q45, q46, q49, q54, q61, q64, q68, q69, q73, q74, q79, q81, q84, q87, q91

- BindingError: Column 'c_customer_sk' not found in any table in scope

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
| q03 | PASS | cross | 15 / 15 | rows and values match |
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
| q16 | ERROR | cross | - | RuntimeError: Error during planning: Mismatch between schema and batches |
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
| q31 | PASS | cross | 1 / 1 | rows and values match |
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
| q42 | ERROR | cross | - | RuntimeError: type_coercion |
| q43 | PASS | cross | 1 / 1 | rows and values match |
| q44 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q45 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q46 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q47 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q52 | PASS | cross | 11 / 11 | rows and values match |
| q53 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q54 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q55 | PASS | cross | 20 / 20 | rows and values match |
| q56 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q57 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q58 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q59 | PASS | cross | 100 / 100 | rows and values match |
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
| q72 | PASS | cross | 50 / 50 | rows and values match |
| q73 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q74 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q75 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q76 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q77 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q78 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_qty. Valid fields are in_0.ss_sold_year, in_0.ss_item_sk, in_0.ss_customer_sk, in_0.ratio, in_0.store_qty, in_0.store_wholesale_cost, in_0.store_sales_price, in_0.other_chan_qty, in_0.other_chan_wholesale_cost, in_0.other_chan_sales_price. |
| q79 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q80 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q81 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | PASS | cross | 0 / 0 | rows and values match |
| q84 | ERROR | cross | - | BindingError: Column 'c_current_addr_sk' not found in any table in scope |
| q85 | ERROR | cross | - | RuntimeError: type_coercion |
| q86 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q87 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in table 'customer' |
| q88 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q89 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q90 | PASS | cross | 1 / 1 | rows and values match |
| q91 | ERROR | cross | - | BindingError: Column 'c_customer_sk' not found in any table in scope |
| q92 | ERROR | cross | - | RuntimeError: type_coercion |
| q93 | PASS | cross | 0 / 0 | rows and values match |
| q94 | ERROR | cross | - | RuntimeError: Error during planning: Mismatch between schema and batches |
| q95 | ERROR | cross | - | RuntimeError: Error during planning: Mismatch between schema and batches |
| q96 | ERROR | cross | - | RuntimeError: type_coercion |
| q97 | PASS | cross | 1 / 1 | rows and values match |
| q98 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q99 | PASS | cross | 6 / 6 | rows and values match |


### Timings (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| query | engine ms | duck ms | ratio |
|---|---|---|---|
| q03 | 34.8 | 19.3 | 1.80x |
| q07 | 75.0 | 63.5 | 1.18x |
| q09 | 129.5 | 11.6 | 11.20x |
| q13 | 112.1 | 59.4 | 1.89x |
| q17 | 139.4 | 42.7 | 3.26x |
| q21 | 66.2 | 21.2 | 3.13x |
| q22 | 44.6 | 23.6 | 1.89x |
| q25 | 90.2 | 36.6 | 2.46x |
| q26 | 60.8 | 45.4 | 1.34x |
| q29 | 128.9 | 37.1 | 3.47x |
| q31 | 296.7 | 39.7 | 7.48x |
| q32 | 39.0 | 20.6 | 1.89x |
| q37 | 27.8 | 19.6 | 1.42x |
| q40 | 58.2 | 23.4 | 2.49x |
| q41 | 36.8 | 6.5 | 5.65x |
| q43 | 43.8 | 26.3 | 1.67x |
| q48 | 105.4 | 65.7 | 1.60x |
| q50 | 72.5 | 32.3 | 2.24x |
| q52 | 33.8 | 17.8 | 1.89x |
| q55 | 36.1 | 23.8 | 1.52x |
| q59 | 4251.8 | 55.2 | 77.04x |
| q62 | 74.3 | 18.5 | 4.01x |
| q65 | 73.1 | 27.1 | 2.70x |
| q72 | 169.3 | 5521.5 | 0.03x |
| q82 | 26.2 | 14.7 | 1.78x |
| q83 | 172.8 | 63.7 | 2.71x |
| q90 | 45.1 | 39.5 | 1.14x |
| q93 | 25.5 | 4.2 | 6.01x |
| q97 | 57.1 | 23.5 | 2.43x |
| q99 | 90.8 | 27.6 | 3.29x |

Geomean ratio (engine/duck): 2.42x over 30 queries.