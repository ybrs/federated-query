# TPC-H benchmark report

Scale factor 0.01, single DuckDB source, per-query timeout 30.0s, memory cap 8192 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared as a multiset of rows with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches; row order is not compared.

## Summary

Total 22 | PASS 15 | MISMATCH 1 | ERROR 6

## Failure clusters

### Out of memory (3)
Queries: q16, q18, q21

- OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes

### Other (2)
Queries: q08, q09

- ExpressionEvaluationError: Unsupported expression for local evaluation: Extract

### Decorrelation limitation (1)
Queries: q20

- DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY

### Wrong result: row values (1)
Queries: q11

- row 0 differs: engine=('1376.00', '13271249.89', '950254254.44') oracle=('1376.00', '13271249.89')

## Per-query matrix

| Query | Status | Rows engine/oracle | Detail |
| --- | --- | --- | --- |
| q01 | PASS | 4 / 4 | rows and values match |
| q02 | PASS | 4 / 4 | rows and values match |
| q03 | PASS | 10 / 10 | rows and values match |
| q04 | PASS | 5 / 5 | rows and values match |
| q05 | PASS | 5 / 5 | rows and values match |
| q06 | PASS | 1 / 1 | rows and values match |
| q07 | PASS | 4 / 4 | rows and values match |
| q08 | ERROR | - | ExpressionEvaluationError: Unsupported expression for local evaluation: Extract |
| q09 | ERROR | - | ExpressionEvaluationError: Unsupported expression for local evaluation: Extract |
| q10 | PASS | 20 / 20 | rows and values match |
| q11 | MISMATCH | 359 / 359 | row 0 differs: engine=('1376.00', '13271249.89', '950254254.44') oracle=('1376.00', '13271249.89') |
| q12 | PASS | 2 / 2 | rows and values match |
| q13 | PASS | 32 / 32 | rows and values match |
| q14 | PASS | 1 / 1 | rows and values match |
| q15 | PASS | 1 / 1 | rows and values match |
| q16 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q17 | PASS | 1 / 1 | rows and values match |
| q18 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q19 | PASS | 1 / 1 | rows and values match |
| q20 | ERROR | - | DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY |
| q21 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 33554432 bytes |
| q22 | PASS | 7 / 7 | rows and values match |
