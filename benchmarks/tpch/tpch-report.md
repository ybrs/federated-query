# TPC-H benchmark report

Scale factor 0.01, single DuckDB source, per-query timeout 20.0s, memory cap 8192 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared as a multiset of rows with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches; row order is not compared.

## Summary

Total 22 | PASS 11 | MISMATCH 2 | ERROR 9

## Failure clusters

### Out of memory (3)
Queries: q16, q18, q21

- OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes

### Binding: reference not in scope (2)
Queries: q13, q15

- BindingError: Column 'c_count' not found in any table in scope

### Other (2)
Queries: q08, q09

- ExpressionEvaluationError: Unsupported expression for local evaluation: Extract

### Decorrelation limitation (1)
Queries: q20

- DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY

### Timeout (1)
Queries: q19

- Timeout: exceeded 20.0s

### Wrong result: row order (1)
Queries: q02

- rows match as a set but order differs at row 1: engine=('287.16', 'Supplier#000000052', 'ROMANIA', '323.00', 'Manufacturer#4', '5oGr3pj2sprZNwho8CFW2haaObd0', '29-974-934-4713', 'arefully silent pinto beans use furiously furiously even deposits. regular packages are furious') oracle=('1883.37', 'Supplier#000000086', 'ROMANIA', '1015.00', 'Manufacturer#4', 'iZLKKWaQADe', '29-903-665-7065', ' foxes boost after the carefully silent asymptotes. slyl')

### Wrong result: row values (1)
Queries: q11

- row 0 differs: engine=('1.00', '1456050.96', '950254254.44') oracle=('1376.00', '13271249.89')

## Per-query matrix

| Query | Status | Rows engine/oracle | Detail |
| --- | --- | --- | --- |
| q01 | PASS | 4 / 4 | rows and values match |
| q02 | MISMATCH | 4 / 4 | rows match as a set but order differs at row 1: engine=('287.16', 'Supplier#000000052', 'ROMANIA', '323.00', 'Manufacturer#4', '5oGr3pj2sprZNwho8CFW2haaObd0', '29-974-934-4713', 'arefully silent pinto beans use furiously furiously even deposits. regular packages are furious') oracle=('1883.37', 'Supplier#000000086', 'ROMANIA', '1015.00', 'Manufacturer#4', 'iZLKKWaQADe', '29-903-665-7065', ' foxes boost after the carefully silent asymptotes. slyl') |
| q03 | PASS | 10 / 10 | rows and values match |
| q04 | PASS | 5 / 5 | rows and values match |
| q05 | PASS | 5 / 5 | rows and values match |
| q06 | PASS | 1 / 1 | rows and values match |
| q07 | PASS | 4 / 4 | rows and values match |
| q08 | ERROR | - | ExpressionEvaluationError: Unsupported expression for local evaluation: Extract |
| q09 | ERROR | - | ExpressionEvaluationError: Unsupported expression for local evaluation: Extract |
| q10 | PASS | 20 / 20 | rows and values match |
| q11 | MISMATCH | 359 / 359 | row 0 differs: engine=('1.00', '1456050.96', '950254254.44') oracle=('1376.00', '13271249.89') |
| q12 | PASS | 2 / 2 | rows and values match |
| q13 | ERROR | - | BindingError: Column 'c_count' not found in any table in scope |
| q14 | PASS | 1 / 1 | rows and values match |
| q15 | ERROR | - | BindingError: Column 'supplier_no' not found in any table in scope |
| q16 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q17 | PASS | 1 / 1 | rows and values match |
| q18 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 8388608 bytes |
| q19 | ERROR | - | Timeout: exceeded 20.0s |
| q20 | ERROR | - | DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY |
| q21 | ERROR | - | OSError: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q22 | PASS | 7 / 7 | rows and values match |
