# TPC-H benchmark report

Scale factor 0.01, single DuckDB source, per-query timeout 45.0s, memory cap 12288 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared as a multiset of rows with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches; row order is not compared.

## Summary

Total 22 | PASS 7 | MISMATCH 0 | ERROR 15

## Failure clusters

### Join-key orientation (7)
Queries: q02, q03, q05, q10, q11, q17, q18

- ValueError: cannot orient join keys 's_suppkey' / 'ps_suppkey' to a join side; neither resolves by qualifier or by column name

### Out of memory (3)
Queries: q07, q08, q09

- InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes

### Binding: reference not in scope (2)
Queries: q13, q15

- BindingError: Column 'c_count' not found in any table in scope

### Decorrelation limitation (1)
Queries: q20

- DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY

### Other (1)
Queries: q21

- OSError: INTERRUPT Error: Interrupted!

### Timeout (1)
Queries: q19

- Timeout: exceeded 45.0s

## Per-query matrix

| Query | Status | Rows engine/oracle | Detail |
| --- | --- | --- | --- |
| q01 | PASS | 4 / 4 | rows and values match |
| q02 | ERROR | - | ValueError: cannot orient join keys 's_suppkey' / 'ps_suppkey' to a join side; neither resolves by qualifier or by column name |
| q03 | ERROR | - | ValueError: cannot orient join keys 'c_custkey' / 'o_custkey' to a join side; neither resolves by qualifier or by column name |
| q04 | PASS | 5 / 5 | rows and values match |
| q05 | ERROR | - | ValueError: cannot orient join keys 'c_custkey' / 'o_custkey' to a join side; neither resolves by qualifier or by column name |
| q06 | PASS | 1 / 1 | rows and values match |
| q07 | ERROR | - | InvalidInputException: Invalid Input Error: arrow_scan: get_next failed(): IOError: IOError: Out of Memory Error: ArrowBuffer: failed to allocate 16777216 bytes |
| q08 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q09 | ERROR | - | OSError: Out of Memory Error: ArrowBuffer: failed to allocate 67108864 bytes |
| q10 | ERROR | - | ValueError: cannot orient join keys 'c_custkey' / 'o_custkey' to a join side; neither resolves by qualifier or by column name |
| q11 | ERROR | - | ValueError: cannot orient join keys 'ps_suppkey' / 's_suppkey' to a join side; neither resolves by qualifier or by column name |
| q12 | PASS | 2 / 2 | rows and values match |
| q13 | ERROR | - | BindingError: Column 'c_count' not found in any table in scope |
| q14 | PASS | 1 / 1 | rows and values match |
| q15 | ERROR | - | BindingError: Column 'supplier_no' not found in any table in scope |
| q16 | PASS | 296 / 296 | rows and values match |
| q17 | ERROR | - | ValueError: cannot orient join keys 'p_partkey' / '__subq_0_g0' to a join side; neither resolves by qualifier or by column name |
| q18 | ERROR | - | ValueError: cannot orient join keys 'o_orderkey' / '__subq_0_v0' to a join side; neither resolves by qualifier or by column name |
| q19 | ERROR | - | Timeout: exceeded 45.0s |
| q20 | ERROR | - | DecorrelationError: Correlation key "lineitem"."l_suppkey" is not part of the subquery's GROUP BY |
| q21 | ERROR | - | OSError: INTERRUPT Error: Interrupted! |
| q22 | PASS | 7 / 7 | rows and values match |
