# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 40 | MISMATCH 1 | ERROR 58 | cross-source 97

### Failure clusters

### Other (55)
Queries: q02, q04, q05, q06, q08, q10, q11, q12, q13, q14, q16, q20, q23, q27, q28, q33, q35, q36, q38, q42, q44, q45, q47, q48, q51, q53, q54, q56, q57, q58, q60, q63, q64, q66, q67, q71, q74, q75, q76, q77, q78, q79, q80, q84, q85, q86, q87, q88, q89, q91, q92, q94, q95, q96, q98

- UnsupportedIR: physical node PhysicalUnion not supported yet

### Binding: reference not in scope (1)
Queries: q49

- BindingError: Table 'catalog' not found in scope for column 'return_rank'

### Simple CASE unsupported (1)
Queries: q39

- UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...)

### Window in WHERE (1)
Queries: q70

- UnsupportedSQLError: window functions are not allowed in WHERE

### Wrong result: row values (1)
Queries: q18

- row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00')

### Per-query matrix

| Query | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- |
| q01 | PASS | cross | 100 / 100 | rows and values match |
| q02 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q03 | PASS | cross | 15 / 15 | rows and values match |
| q04 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q05 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q06 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q07 | PASS | cross | 100 / 100 | rows and values match |
| q08 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q09 | PASS | cross | 1 / 1 | rows and values match |
| q10 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q11 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q12 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q13 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q14 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q15 | PASS | cross | 33 / 33 | rows and values match |
| q16 | ERROR | cross | - | RuntimeError: Error during planning: Mismatch between schema and batches |
| q17 | PASS | cross | 1 / 1 | rows and values match |
| q18 | MISMATCH | cross | 100 / 100 | row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00') |
| q19 | PASS | cross | 10 / 10 | rows and values match |
| q20 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q21 | PASS | cross | 32 / 32 | rows and values match |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q24 | PASS | cross | 0 / 0 | rows and values match |
| q25 | PASS | cross | 0 / 0 | rows and values match |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q28 | ERROR | single | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q29 | PASS | cross | 0 / 0 | rows and values match |
| q30 | PASS | cross | 21 / 21 | rows and values match |
| q31 | PASS | cross | 1 / 1 | rows and values match |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q34 | PASS | cross | 53 / 53 | rows and values match |
| q35 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q36 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q37 | PASS | cross | 0 / 0 | rows and values match |
| q38 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSetOperation not supported yet |
| q39 | ERROR | cross | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | cross | 44 / 44 | rows and values match |
| q41 | PASS | single | 0 / 0 | rows and values match |
| q42 | ERROR | cross | - | RuntimeError: type_coercion |
| q43 | PASS | cross | 1 / 1 | rows and values match |
| q44 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q45 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q46 | PASS | cross | 100 / 100 | rows and values match |
| q47 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q48 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q49 | ERROR | cross | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q52 | PASS | cross | 11 / 11 | rows and values match |
| q53 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q54 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q55 | PASS | cross | 20 / 20 | rows and values match |
| q56 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q57 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q58 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSingleRowGuard not supported yet |
| q59 | PASS | cross | 100 / 100 | rows and values match |
| q60 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q61 | PASS | cross | 1 / 1 | rows and values match |
| q62 | PASS | cross | 6 / 6 | rows and values match |
| q63 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q64 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.right_cnt. Valid fields are in_0.product_name, in_0.store_name, in_0.store_zip, in_0.b_street_number, in_0.b_street_name, in_0.b_city, in_0.b_zip, in_0.c_street_number, in_0.c_street_name, in_0.c_city, in_0.c_zip, in_0.cs1syear, in_0.cs1cnt, in_0.s11, in_0.s21, in_0.s31, in_0.s12, in_0.s22, in_0.s32, in_0.syear, in_0.cnt. |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q67 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q68 | PASS | cross | 100 / 100 | rows and values match |
| q69 | PASS | cross | 71 / 71 | rows and values match |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q72 | PASS | cross | 50 / 50 | rows and values match |
| q73 | PASS | cross | 0 / 0 | rows and values match |
| q74 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q75 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q76 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q77 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q78 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.ss_qty. Valid fields are in_0.ss_sold_year, in_0.ss_item_sk, in_0.ss_customer_sk, in_0.ratio, in_0.store_qty, in_0.store_wholesale_cost, in_0.store_sales_price, in_0.other_chan_qty, in_0.other_chan_wholesale_cost, in_0.other_chan_sales_price. |
| q79 | ERROR | cross | - | RuntimeError: type_coercion |
| q80 | ERROR | cross | - | UnsupportedIR: physical node PhysicalUnion not supported yet |
| q81 | PASS | cross | 34 / 34 | rows and values match |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | PASS | cross | 0 / 0 | rows and values match |
| q84 | ERROR | cross | - | RuntimeError: Schema error: No field named in_0.c_customer_id. Did you mean 'in_0.customer_id'?. |
| q85 | ERROR | cross | - | RuntimeError: type_coercion |
| q86 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q87 | ERROR | cross | - | UnsupportedIR: physical node PhysicalSetOperation not supported yet |
| q88 | ERROR | cross | - | UnsupportedIR: join type JoinType.CROSS not yet supported |
| q89 | ERROR | cross | - | UnsupportedIR: expression WindowExpr not supported in IR |
| q90 | PASS | cross | 1 / 1 | rows and values match |
| q91 | ERROR | cross | - | RuntimeError: type_coercion |
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
| q01 | 60.5 | 21.9 | 2.76x |
| q03 | 37.7 | 26.0 | 1.45x |
| q07 | 65.3 | 64.7 | 1.01x |
| q09 | 123.1 | 7.8 | 15.79x |
| q15 | 39.8 | 23.1 | 1.72x |
| q17 | 134.6 | 35.6 | 3.78x |
| q19 | 61.1 | 25.4 | 2.40x |
| q21 | 64.5 | 17.9 | 3.61x |
| q22 | 50.9 | 30.9 | 1.65x |
| q24 | 160.8 | 15.6 | 10.29x |
| q25 | 89.1 | 36.0 | 2.48x |
| q26 | 62.3 | 60.5 | 1.03x |
| q29 | 135.1 | 49.4 | 2.74x |
| q30 | 131.3 | 31.4 | 4.19x |
| q31 | 298.6 | 38.2 | 7.82x |
| q32 | 40.8 | 25.2 | 1.62x |
| q34 | 71.5 | 37.8 | 1.89x |
| q37 | 26.0 | 19.1 | 1.36x |
| q40 | 57.6 | 21.9 | 2.63x |
| q41 | 41.0 | 6.5 | 6.29x |
| q43 | 43.9 | 22.9 | 1.91x |
| q46 | 128.8 | 31.0 | 4.16x |
| q50 | 71.5 | 32.2 | 2.22x |
| q52 | 34.0 | 29.2 | 1.16x |
| q55 | 36.8 | 29.0 | 1.27x |
| q59 | 4319.3 | 72.7 | 59.39x |
| q61 | 133.0 | 35.1 | 3.78x |
| q62 | 68.1 | 31.0 | 2.20x |
| q65 | 74.4 | 29.6 | 2.51x |
| q68 | 110.2 | 41.6 | 2.65x |
| q69 | 139.3 | 93.7 | 1.49x |
| q72 | 169.1 | 62.6 | 2.70x |
| q73 | 71.9 | 32.5 | 2.21x |
| q81 | 148.1 | 32.1 | 4.61x |
| q82 | 26.4 | 14.0 | 1.88x |
| q83 | 169.5 | 64.5 | 2.63x |
| q90 | 77.8 | 42.5 | 1.83x |
| q93 | 25.3 | 4.5 | 5.66x |
| q97 | 56.9 | 25.8 | 2.20x |
| q99 | 68.2 | 20.8 | 3.28x |

Geomean ratio (engine/duck): 2.83x over 40 queries.