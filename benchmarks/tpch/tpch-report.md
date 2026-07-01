# TPC-H benchmark report

Scale factor 0.01, single DuckDB source, per-query timeout 60.0s, memory cap 12288 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared as a multiset of rows with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches; row order is not compared.

## Summary

Total 22 | PASS 4 | MISMATCH 0 | ERROR 18

## Failure clusters

### Recursion (11)
Queries: q02, q03, q04, q05, q10, q12, q14, q16, q17, q18, q21

- RecursionError: maximum recursion depth exceeded

### Out of memory (3)
Queries: q07, q08, q09

- OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes

### Binding: reference not in scope (2)
Queries: q13, q15

- BindingError: Column 'c_count' not found in any table in scope

### Decorrelation limitation (1)
Queries: q20

- DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY

### Join-key orientation (1)
Queries: q11

- ValueError: cannot orient join keys 'ps_suppkey' / 's_suppkey' to a join side; neither resolves by qualifier or by column name

## Per-query matrix

| Query | Status | Rows engine/oracle | Detail |
| --- | --- | --- | --- |
| q01 | PASS | 4 / 4 | rows and values match |
| q02 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q03 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q04 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q05 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q06 | PASS | 1 / 1 | rows and values match |
| q07 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q08 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q09 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 134217728 bytes |
| q10 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q11 | ERROR | - | ValueError: cannot orient join keys 'ps_suppkey' / 's_suppkey' to a join side; neither resolves by qualifier or by column name |
| q12 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q13 | ERROR | - | BindingError: Column 'c_count' not found in any table in scope |
| q14 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q15 | ERROR | - | BindingError: Column 'supplier_no' not found in any table in scope |
| q16 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q17 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q18 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q19 | PASS | 1 / 1 | rows and values match |
| q20 | ERROR | - | DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY |
| q21 | ERROR | - | RecursionError: maximum recursion depth exceeded |
| q22 | PASS | 7 / 7 | rows and values match |
