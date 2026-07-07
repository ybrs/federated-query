# TPC-DS federated benchmark report

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 91 | MISMATCH 1 | ERROR 7 | cross-source 97

### Failure clusters

### Other (5)
Queries: q10, q23, q35, q45, q86

- RuntimeError: Resources exhausted: Failed to allocate additional 279.6 KB for fedq_collect with 32.0 GB already allocated for this reservation - 200.7 KB remain available for the total memory pool: fair(pool_size: 32.0 GB)

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
| q02 | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | PASS | cross | 15 / 15 | rows and values match |
| q04 | PASS | cross | 0 / 0 | rows and values match |
| q05 | PASS | cross | 100 / 100 | rows and values match |
| q06 | PASS | cross | 10 / 10 | rows and values match |
| q07 | PASS | cross | 100 / 100 | rows and values match |
| q08 | PASS | cross | 0 / 0 | rows and values match |
| q09 | PASS | cross | 1 / 1 | rows and values match |
| q10 | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 279.6 KB for fedq_collect with 32.0 GB already allocated for this reservation - 200.7 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q11 | PASS | cross | 4 / 4 | rows and values match |
| q12 | PASS | cross | 100 / 100 | rows and values match |
| q13 | PASS | cross | 1 / 1 | rows and values match |
| q14 | PASS | cross | 100 / 100 | rows and values match |
| q15 | PASS | cross | 33 / 33 | rows and values match |
| q16 | PASS | cross | 1 / 1 | rows and values match |
| q17 | PASS | cross | 1 / 1 | rows and values match |
| q18 | MISMATCH | cross | 100 / 100 | row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00') |
| q19 | PASS | cross | 10 / 10 | rows and values match |
| q20 | PASS | cross | 100 / 100 | rows and values match |
| q21 | PASS | cross | 32 / 32 | rows and values match |
| q22 | PASS | cross | 100 / 100 | rows and values match |
| q23 | ERROR | cross | - | RuntimeError: type_coercion |
| q24 | PASS | cross | 0 / 0 | rows and values match |
| q25 | PASS | cross | 0 / 0 | rows and values match |
| q26 | PASS | cross | 100 / 100 | rows and values match |
| q27 | PASS | cross | 100 / 100 | rows and values match |
| q28 | PASS | single | 1 / 1 | rows and values match |
| q29 | PASS | cross | 0 / 0 | rows and values match |
| q30 | PASS | cross | 21 / 21 | rows and values match |
| q31 | PASS | cross | 1 / 1 | rows and values match |
| q32 | PASS | cross | 1 / 1 | rows and values match |
| q33 | PASS | cross | 100 / 100 | rows and values match |
| q34 | PASS | cross | 53 / 53 | rows and values match |
| q35 | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 311.9 KB for fedq_collect with 32.0 GB already allocated for this reservation - 196.2 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q36 | PASS | cross | 100 / 100 | rows and values match |
| q37 | PASS | cross | 0 / 0 | rows and values match |
| q38 | PASS | cross | 1 / 1 | rows and values match |
| q39 | ERROR | cross | - | UnsupportedSQLError: simple CASE (CASE operand WHEN ...) is not supported; use a searched CASE (CASE WHEN operand = value ...) |
| q40 | PASS | cross | 44 / 44 | rows and values match |
| q41 | PASS | single | 0 / 0 | rows and values match |
| q42 | PASS | cross | 4 / 4 | rows and values match |
| q43 | PASS | cross | 1 / 1 | rows and values match |
| q44 | PASS | cross | 0 / 0 | rows and values match |
| q45 | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 361.1 KB for fedq_collect with 32.0 GB already allocated for this reservation - 180.7 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q46 | PASS | cross | 100 / 100 | rows and values match |
| q47 | PASS | cross | 100 / 100 | rows and values match |
| q48 | PASS | cross | 1 / 1 | rows and values match |
| q49 | PASS | cross | 2 / 2 | rows and values match |
| q50 | PASS | cross | 1 / 1 | rows and values match |
| q51 | PASS | cross | 100 / 100 | rows and values match |
| q52 | PASS | cross | 11 / 11 | rows and values match |
| q53 | PASS | cross | 100 / 100 | rows and values match |
| q54 | PASS | cross | 0 / 0 | rows and values match |
| q55 | PASS | cross | 20 / 20 | rows and values match |
| q56 | PASS | cross | 38 / 38 | rows and values match |
| q57 | PASS | cross | 100 / 100 | rows and values match |
| q58 | PASS | cross | 0 / 0 | rows and values match |
| q59 | PASS | cross | 100 / 100 | rows and values match |
| q60 | PASS | cross | 100 / 100 | rows and values match |
| q61 | PASS | cross | 1 / 1 | rows and values match |
| q62 | PASS | cross | 6 / 6 | rows and values match |
| q63 | PASS | cross | 100 / 100 | rows and values match |
| q64 | PASS | cross | 0 / 0 | rows and values match |
| q65 | PASS | cross | 0 / 0 | rows and values match |
| q66 | PASS | cross | 1 / 1 | rows and values match |
| q67 | PASS | cross | 100 / 100 | rows and values match |
| q68 | PASS | cross | 100 / 100 | rows and values match |
| q69 | PASS | cross | 71 / 71 | rows and values match |
| q70 | ERROR | cross | - | UnsupportedSQLError: window functions are not allowed in WHERE |
| q71 | PASS | cross | 56 / 56 | rows and values match |
| q72 | PASS | cross | 50 / 50 | rows and values match |
| q73 | PASS | cross | 0 / 0 | rows and values match |
| q74 | PASS | cross | 3 / 3 | rows and values match |
| q75 | PASS | cross | 25 / 25 | rows and values match |
| q76 | PASS | cross | 100 / 100 | rows and values match |
| q77 | PASS | cross | 10 / 10 | rows and values match |
| q78 | PASS | cross | 100 / 100 | rows and values match |
| q79 | PASS | cross | 100 / 100 | rows and values match |
| q80 | PASS | cross | 100 / 100 | rows and values match |
| q81 | PASS | cross | 34 / 34 | rows and values match |
| q82 | PASS | cross | 0 / 0 | rows and values match |
| q83 | PASS | cross | 0 / 0 | rows and values match |
| q84 | PASS | cross | 6 / 6 | rows and values match |
| q85 | PASS | cross | 0 / 0 | rows and values match |
| q86 | ERROR | cross | - | RuntimeError: This feature is not implemented: Physical plan does not support logical expression AggregateFunction(AggregateFunction { func: AggregateUDF { inner: Grouping { signature: Signature { type_signature: VariadicAny, volatility: Immutable, parameter_names: None } } }, params: AggregateFunctionParams { args: [Column(Column { relation: Some(Bare { table: "in_0" }), name: "i_category" })], distinct: false, filter: None, order_by: [], null_treatment: None } }) |
| q87 | PASS | cross | 1 / 1 | rows and values match |
| q88 | PASS | cross | 1 / 1 | rows and values match |
| q89 | PASS | cross | 100 / 100 | rows and values match |
| q90 | PASS | cross | 1 / 1 | rows and values match |
| q91 | PASS | cross | 0 / 0 | rows and values match |
| q92 | PASS | cross | 1 / 1 | rows and values match |
| q93 | PASS | cross | 0 / 0 | rows and values match |
| q94 | PASS | cross | 1 / 1 | rows and values match |
| q95 | PASS | cross | 1 / 1 | rows and values match |
| q96 | PASS | cross | 1 / 1 | rows and values match |
| q97 | PASS | cross | 1 / 1 | rows and values match |
| q98 | PASS | cross | 250 / 250 | rows and values match |
| q99 | PASS | cross | 6 / 6 | rows and values match |


### Timings (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| query | engine ms | duck ms | ratio |
|---|---|---|---|
| q01 | 62.6 | 20.7 | 3.03x |
| q02 | 2540.7 | 45.3 | 56.11x |
| q03 | 34.8 | 25.6 | 1.36x |
| q04 | 1522.9 | 68.7 | 22.17x |
| q05 | 167.1 | 35.7 | 4.68x |
| q06 | 147.4 | 44.0 | 3.35x |
| q07 | 70.0 | 66.7 | 1.05x |
| q08 | 81.1 | 30.4 | 2.67x |
| q09 | 122.1 | 8.4 | 14.51x |
| q11 | 537.4 | 54.1 | 9.94x |
| q12 | 34.1 | 19.1 | 1.78x |
| q13 | 99.4 | 52.7 | 1.89x |
| q14 | 725.5 | 96.7 | 7.50x |
| q15 | 40.9 | 21.7 | 1.88x |
| q16 | 62.4 | 18.9 | 3.29x |
| q17 | 125.3 | 35.1 | 3.57x |
| q19 | 56.3 | 36.5 | 1.54x |
| q20 | 35.2 | 20.7 | 1.70x |
| q21 | 57.0 | 16.0 | 3.56x |
| q22 | 47.1 | 28.7 | 1.64x |
| q24 | 140.4 | 15.7 | 8.95x |
| q25 | 88.1 | 33.7 | 2.61x |
| q26 | 61.0 | 68.3 | 0.89x |
| q27 | 236.3 | 67.0 | 3.53x |
| q28 | 74.3 | 9.7 | 7.65x |
| q29 | 127.3 | 49.1 | 2.59x |
| q30 | 135.7 | 34.7 | 3.91x |
| q31 | 303.0 | 39.7 | 7.63x |
| q32 | 37.7 | 21.4 | 1.76x |
| q33 | 211.0 | 38.5 | 5.49x |
| q34 | 67.9 | 28.2 | 2.41x |
| q36 | 140.4 | 31.5 | 4.46x |
| q37 | 26.0 | 14.1 | 1.84x |
| q38 | 92.2 | 41.9 | 2.20x |
| q40 | 60.1 | 17.4 | 3.45x |
| q41 | 37.7 | 6.3 | 5.97x |
| q42 | 33.4 | 17.1 | 1.95x |
| q43 | 45.5 | 28.0 | 1.63x |
| q44 | 107.4 | 5.2 | 20.73x |
| q46 | 117.7 | 30.5 | 3.86x |
| q47 | 572.3 | 38.1 | 15.00x |
| q48 | 101.7 | 66.8 | 1.52x |
| q49 | 144.0 | 33.6 | 4.28x |
| q50 | 68.3 | 31.6 | 2.16x |
| q51 | 140.2 | 49.1 | 2.85x |
| q52 | 36.7 | 18.9 | 1.94x |
| q53 | 59.8 | 36.0 | 1.66x |
| q54 | 185.3 | 51.0 | 3.63x |
| q55 | 33.2 | 20.7 | 1.61x |
| q56 | 209.5 | 39.5 | 5.31x |
| q57 | 444.3 | 41.1 | 10.82x |
| q58 | 237.1 | 62.9 | 3.77x |
| q59 | 4091.5 | 53.3 | 76.80x |
| q60 | 145.9 | 38.8 | 3.76x |
| q61 | 126.1 | 35.0 | 3.60x |
| q62 | 67.4 | 25.9 | 2.60x |
| q63 | 60.2 | 23.7 | 2.54x |
| q64 | 3309.3 | 97.0 | 34.11x |
| q65 | 81.7 | 28.1 | 2.91x |
| q66 | 1032.0 | 72.1 | 14.30x |
| q67 | 229.6 | 72.7 | 3.16x |
| q68 | 100.6 | 34.9 | 2.88x |
| q69 | 126.3 | 80.7 | 1.57x |
| q71 | 87.3 | 57.3 | 1.52x |
| q72 | 180.3 | 65.5 | 2.75x |
| q73 | 72.4 | 36.9 | 1.96x |
| q74 | 365.6 | 41.3 | 8.86x |
| q75 | 669.3 | 38.7 | 17.28x |
| q76 | 89.6 | 32.5 | 2.76x |
| q77 | 155.5 | 36.9 | 4.21x |
| q78 | 186.5 | 39.5 | 4.72x |
| q79 | 93.8 | 34.4 | 2.73x |
| q80 | 222.9 | 45.2 | 4.93x |
| q81 | 92.8 | 33.4 | 2.78x |
| q82 | 26.0 | 19.6 | 1.32x |
| q83 | 172.4 | 61.8 | 2.79x |
| q84 | 40.6 | 41.5 | 0.98x |
| q85 | 144.9 | 91.3 | 1.59x |
| q87 | 89.9 | 43.5 | 2.07x |
| q88 | 369.3 | 210.5 | 1.75x |
| q89 | 63.8 | 30.6 | 2.08x |
| q90 | 70.0 | 39.2 | 1.78x |
| q91 | 64.1 | 51.4 | 1.25x |
| q92 | 42.5 | 21.2 | 2.01x |
| q93 | 24.6 | 3.9 | 6.25x |
| q94 | 56.1 | 16.3 | 3.46x |
| q95 | 56.2 | 35.5 | 1.59x |
| q96 | 37.0 | 26.2 | 1.41x |
| q97 | 51.6 | 24.0 | 2.15x |
| q98 | 35.6 | 20.7 | 1.72x |
| q99 | 72.8 | 20.5 | 3.55x |

Geomean ratio (engine/duck): 3.41x over 91 queries.