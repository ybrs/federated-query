# TPC-DS federated benchmark report

Commit: `f0627d9` - tpcds: commit-named reports with inline timings, like tpch  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 15:02
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 30.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 95 | MISMATCH 1 | ERROR 3 | cross-source 97

### Failure clusters

### Other (3)
Queries: q10, q35, q45

- RuntimeError: Resources exhausted: Failed to allocate additional 283.2 KB for fedq_collect with 32.0 GB already allocated for this reservation - 87.9 KB remain available for the total memory pool: fair(pool_size: 32.0 GB)

### Wrong result: row values (1)
Queries: q18

- row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00')

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 61.3 | 23.2 | 2.64x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 2393.1 | 42.5 | 56.33x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 30.9 | 30.2 | 1.02x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 1512.6 | 67.7 | 22.34x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 160.0 | 35.9 | 4.46x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 126.6 | 48.9 | 2.59x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 67.5 | 80.8 | 0.84x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 82.8 | 34.3 | 2.41x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 126.2 | 8.1 | 15.55x | PASS | cross | 1 / 1 | rows and values match |
| q10 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 283.2 KB for fedq_collect with 32.0 GB already allocated for this reservation - 87.9 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q11 | 577.9 | 57.0 | 10.15x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 42.0 | 18.9 | 2.22x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 96.0 | 45.1 | 2.13x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 729.6 | 89.7 | 8.14x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 43.3 | 33.8 | 1.28x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 62.5 | 18.5 | 3.38x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 124.2 | 36.3 | 3.42x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 110.7 | 67.2 | 1.65x | MISMATCH | cross | 100 / 100 | row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00') |
| q19 | 59.8 | 31.4 | 1.90x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 36.1 | 19.5 | 1.85x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 55.8 | 20.9 | 2.67x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 46.0 | 32.1 | 1.43x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 680.5 | 80.7 | 8.43x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 139.2 | 14.9 | 9.33x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 87.7 | 36.8 | 2.38x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 58.6 | 57.6 | 1.02x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 225.0 | 46.8 | 4.81x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 74.7 | 9.7 | 7.68x | PASS | single | 1 / 1 | rows and values match |
| q29 | 92.8 | 45.4 | 2.05x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 93.5 | 31.8 | 2.94x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 311.9 | 40.3 | 7.74x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 37.1 | 22.7 | 1.63x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 148.8 | 40.8 | 3.64x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 72.5 | 26.0 | 2.79x | PASS | cross | 53 / 53 | rows and values match |
| q35 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 311.9 KB for fedq_collect with 32.0 GB already allocated for this reservation - 196.2 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q36 | 146.3 | 25.6 | 5.72x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 33.3 | 14.2 | 2.35x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 93.4 | 41.7 | 2.24x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 225.7 | 29.2 | 7.72x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 63.7 | 17.3 | 3.68x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 40.5 | 6.8 | 5.99x | PASS | single | 0 / 0 | rows and values match |
| q42 | 42.2 | 21.2 | 1.99x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 45.6 | 23.8 | 1.91x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 107.8 | 9.4 | 11.47x | PASS | cross | 0 / 0 | rows and values match |
| q45 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 361.1 KB for fedq_collect with 32.0 GB already allocated for this reservation - 180.7 KB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q46 | 109.3 | 33.4 | 3.27x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 630.9 | 38.2 | 16.51x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 101.8 | 66.0 | 1.54x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 149.9 | 33.4 | 4.49x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 67.4 | 32.5 | 2.08x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 157.2 | 49.2 | 3.20x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 35.2 | 26.2 | 1.34x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 58.7 | 26.2 | 2.24x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 171.0 | 46.2 | 3.71x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 33.4 | 27.1 | 1.23x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 150.5 | 39.1 | 3.85x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 453.5 | 51.5 | 8.80x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 211.6 | 64.9 | 3.26x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 4018.5 | 75.1 | 53.51x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 155.6 | 41.2 | 3.78x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 126.2 | 41.5 | 3.04x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 70.2 | 32.8 | 2.14x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 58.9 | 24.9 | 2.36x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 3417.6 | 97.3 | 35.11x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 73.0 | 27.7 | 2.64x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 1089.9 | 69.6 | 15.65x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 226.0 | 77.2 | 2.93x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 105.0 | 40.3 | 2.61x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 124.8 | 108.9 | 1.15x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 73.2 | 35.8 | 2.05x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 91.3 | 57.4 | 1.59x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 161.7 | 67.2 | 2.41x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 65.7 | 26.3 | 2.49x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 364.7 | 40.1 | 9.09x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 654.4 | 38.8 | 16.87x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 86.2 | 35.1 | 2.46x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 166.2 | 35.2 | 4.73x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 190.3 | 38.9 | 4.90x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 89.0 | 31.6 | 2.81x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 214.5 | 50.4 | 4.26x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 97.4 | 42.6 | 2.29x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 27.0 | 19.3 | 1.40x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 172.7 | 62.2 | 2.78x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 41.2 | 28.8 | 1.43x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 154.9 | 68.6 | 2.26x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 43.5 | 19.0 | 2.29x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 97.7 | 74.4 | 1.31x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 378.2 | 189.0 | 2.00x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 63.4 | 29.6 | 2.15x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 75.0 | 38.8 | 1.93x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 65.1 | 50.1 | 1.30x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 45.8 | 23.8 | 1.92x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 26.0 | 4.2 | 6.23x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 56.2 | 15.8 | 3.55x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 57.9 | 40.4 | 1.43x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 51.0 | 33.9 | 1.51x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 68.8 | 30.7 | 2.24x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 47.6 | 27.1 | 1.75x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 78.6 | 27.4 | 2.87x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 24856.5 | 3870.5 | 6.42x | 3.32x | 95 |
