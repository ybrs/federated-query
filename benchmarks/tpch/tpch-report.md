# TPC-H benchmark report

Scale factor 0.01, single DuckDB source, per-query timeout 60.0s, memory cap 0 MB.

Correctness is differential against DuckDB: each query runs through the engine and directly in DuckDB, and the two result sets are compared as a multiset of rows with every column value normalized (numbers rounded to 2 decimals, CHAR padding stripped). PASS means every row and every value matches; row order is not compared.

## Summary

Total 22 | PASS 22 | MISMATCH 0 | ERROR 0

## Failure clusters

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
| q08 | PASS | 2 / 2 | rows and values match |
| q09 | PASS | 173 / 173 | rows and values match |
| q10 | PASS | 20 / 20 | rows and values match |
| q11 | PASS | 359 / 359 | rows and values match |
| q12 | PASS | 2 / 2 | rows and values match |
| q13 | PASS | 32 / 32 | rows and values match |
| q14 | PASS | 1 / 1 | rows and values match |
| q15 | PASS | 1 / 1 | rows and values match |
| q16 | PASS | 296 / 296 | rows and values match |
| q17 | PASS | 1 / 1 | rows and values match |
| q18 | PASS | 2 / 2 | rows and values match |
| q19 | PASS | 1 / 1 | rows and values match |
| q20 | PASS | 1 / 1 | rows and values match |
| q21 | PASS | 1 / 1 | rows and values match |
| q22 | PASS | 7 / 7 | rows and values match |
