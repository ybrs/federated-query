# TPC-DS benchmark report

Scale factor 0.1, single DuckDB source, per-query timeout 60.0s, memory cap 12288 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared row by row with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches in order.

## Summary

Total 99 | PASS 42 | MISMATCH 0 | ERROR 57

## Failure clusters

### Other (24)
Queries: q06, q21, q28, q33, q39, q44, q47, q51, q53, q54, q56, q57, q60, q61, q63, q66, q67, q70, q75, q76, q85, q88, q89, q90

- CardinalityViolationError: Scalar subquery returned more than one row

### Out of memory (16)
Queries: q04, q05, q11, q18, q23, q24, q27, q30, q31, q36, q45, q74, q77, q80, q81, q83

- OSError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes

### Timeout (9)
Queries: q01, q02, q10, q14, q35, q59, q64, q95, q97

- Timeout: exceeded 60.0s

### Join-key orientation (5)
Queries: q46, q62, q68, q71, q99

- ValueError: cannot orient join keys 'ss_customer_sk' / 'c_customer_sk' to a join side; neither resolves by qualifier or by column name

### Not implemented (2)
Queries: q58, q72

- ArrowNotImplementedError: Function 'equal' has no kernel matching input types (date32[day], string)

### Binding: reference not in scope (1)
Queries: q49

- BindingError: Table 'catalog' not found in scope for column 'return_rank'

## Per-query matrix

| Query | Status | Rows engine/oracle | Detail |
| --- | --- | --- | --- |
| q01 | ERROR | - | Timeout: exceeded 60.0s |
| q02 | ERROR | - | Timeout: exceeded 60.0s |
| q03 | PASS | 15 / 15 | rows and values match |
| q04 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q05 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 1048576 bytes |
| q06 | ERROR | - | CardinalityViolationError: Scalar subquery returned more than one row |
| q07 | PASS | 100 / 100 | rows and values match |
| q08 | PASS | 0 / 0 | rows and values match |
| q09 | PASS | 1 / 1 | rows and values match |
| q10 | ERROR | - | Timeout: exceeded 60.0s |
| q11 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q12 | PASS | 100 / 100 | rows and values match |
| q13 | PASS | 1 / 1 | rows and values match |
| q14 | ERROR | - | Timeout: exceeded 60.0s |
| q15 | PASS | 33 / 33 | rows and values match |
| q16 | PASS | 1 / 1 | rows and values match |
| q17 | PASS | 1 / 1 | rows and values match |
| q18 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q19 | PASS | 10 / 10 | rows and values match |
| q20 | PASS | 100 / 100 | rows and values match |
| q21 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q22 | PASS | 100 / 100 | rows and values match |
| q23 | ERROR | - | OutOfMemoryException: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q24 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q25 | PASS | 0 / 0 | rows and values match |
| q26 | PASS | 100 / 100 | rows and values match |
| q27 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q28 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q29 | PASS | 0 / 0 | rows and values match |
| q30 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q31 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q32 | PASS | 1 / 1 | rows and values match |
| q33 | ERROR | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q34 | PASS | 53 / 53 | rows and values match |
| q35 | ERROR | - | Timeout: exceeded 60.0s |
| q36 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q37 | PASS | 0 / 0 | rows and values match |
| q38 | PASS | 1 / 1 | rows and values match |
| q39 | ERROR | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | 44 / 44 | rows and values match |
| q41 | PASS | 0 / 0 | rows and values match |
| q42 | PASS | 4 / 4 | rows and values match |
| q43 | PASS | 1 / 1 | rows and values match |
| q44 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q45 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 4194304 bytes |
| q46 | ERROR | - | ValueError: cannot orient join keys 'ss_customer_sk' / 'c_customer_sk' to a join side; neither resolves by qualifier or by column name |
| q47 | ERROR | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q48 | PASS | 1 / 1 | rows and values match |
| q49 | ERROR | - | BindingError: Table 'catalog' not found in scope for column 'return_rank' |
| q50 | PASS | 1 / 1 | rows and values match |
| q51 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q52 | PASS | 11 / 11 | rows and values match |
| q53 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q54 | ERROR | - | CardinalityViolationError: Scalar subquery returned more than one row |
| q55 | PASS | 20 / 20 | rows and values match |
| q56 | ERROR | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q57 | ERROR | - | StarExpansionError: Missing catalog metadata for default.public.v2 |
| q58 | ERROR | - | ArrowNotImplementedError: Function 'equal' has no kernel matching input types (date32[day], string) |
| q59 | ERROR | - | Timeout: exceeded 60.0s |
| q60 | ERROR | - | StarExpansionError: Missing catalog metadata for default.public.ss |
| q61 | ERROR | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q62 | ERROR | - | ValueError: cannot orient join keys 'ws_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |
| q63 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q64 | ERROR | - | Timeout: exceeded 60.0s |
| q65 | PASS | 0 / 0 | rows and values match |
| q66 | ERROR | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q67 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q68 | ERROR | - | ValueError: cannot orient join keys 'ss_customer_sk' / 'c_customer_sk' to a join side; neither resolves by qualifier or by column name |
| q69 | PASS | 71 / 71 | rows and values match |
| q70 | ERROR | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | ERROR | - | ValueError: cannot orient join keys 'sold_item_sk' / 'i_item_sk' to a join side; neither resolves by qualifier or by column name |
| q72 | ERROR | - | ArrowNotImplementedError: Function 'add' has no kernel matching input types (timestamp[s], int64) |
| q73 | PASS | 0 / 0 | rows and values match |
| q74 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q75 | ERROR | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q76 | ERROR | - | ArrowInvalid: Decimal value does not fit in precision 7 |
| q77 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q78 | PASS | 100 / 100 | rows and values match |
| q79 | PASS | 100 / 100 | rows and values match |
| q80 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q81 | ERROR | - | OutOfMemoryException: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q82 | PASS | 0 / 0 | rows and values match |
| q83 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 134217728 bytes |
| q84 | PASS | 6 / 6 | rows and values match |
| q85 | ERROR | - | BinderException: Binder Error: Referenced column "r_reason_desc" not found in FROM clause! |
| q86 | PASS | 100 / 100 | rows and values match |
| q87 | PASS | 1 / 1 | rows and values match |
| q88 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q89 | ERROR | - | StarExpansionError: Star expansion only supports base tables |
| q90 | ERROR | - | ExpressionEvaluationError: Unsupported function: NULLIF |
| q91 | PASS | 0 / 0 | rows and values match |
| q92 | PASS | 1 / 1 | rows and values match |
| q93 | PASS | 0 / 0 | rows and values match |
| q94 | PASS | 1 / 1 | rows and values match |
| q95 | ERROR | - | Timeout: exceeded 60.0s |
| q96 | PASS | 1 / 1 | rows and values match |
| q97 | ERROR | - | Timeout: exceeded 60.0s |
| q98 | PASS | 250 / 250 | rows and values match |
| q99 | ERROR | - | ValueError: cannot orient join keys 'cs_warehouse_sk' / 'w_warehouse_sk' to a join side; neither resolves by qualifier or by column name |
