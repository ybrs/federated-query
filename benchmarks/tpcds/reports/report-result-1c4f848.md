# TPC-DS federated benchmark report

Commit: `1c4f848` - tpcds compare: order-only differences with identical multisets are a match  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 17:49
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 10, PostgreSQL + DuckDB split, per-query timeout 300.0s, memory cap 40000 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 95 | MISMATCH 0 | ERROR 4 | cross-source 97

### Failure clusters

### Other (3)
Queries: q23, q67, q78

- RuntimeError: Resources exhausted: Failed to allocate additional 80.2 MB for fedq_collect with 31.4 GB already allocated for this reservation - 70.1 MB remain available for the total memory pool: fair(pool_size: 32.0 GB)

### Memory limit (killed) (1)
Queries: q64

- Killed: worker exited with code 137 (likely memory limit)

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 173.4 | 108.0 | 1.61x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 1029.4 | 243.4 | 4.23x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 93.6 | 57.9 | 1.62x | PASS | cross | 100 / 100 | rows and values match |
| q04 | 11346.6 | 1539.7 | 7.37x | PASS | cross | 100 / 100 | rows and values match |
| q05 | 3801.3 | 156.7 | 24.26x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 4276.5 | 215.8 | 19.81x | PASS | cross | 51 / 51 | rows and values match |
| q07 | 285.2 | 251.6 | 1.13x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 381.1 | 160.2 | 2.38x | PASS | cross | 9 / 9 | rows and values match |
| q09 | 442.1 | 379.6 | 1.16x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 314.5 | 456.8 | 0.69x | PASS | cross | 100 / 100 | rows and values match |
| q11 | 6741.1 | 869.9 | 7.75x | PASS | cross | 100 / 100 | rows and values match |
| q12 | 74.9 | 58.8 | 1.27x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 384.7 | 353.5 | 1.09x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 7012.2 | 3273.9 | 2.14x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 191.1 | 143.5 | 1.33x | PASS | cross | 100 / 100 | rows and values match |
| q16 | 351.8 | 56.8 | 6.20x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 1187.0 | 219.5 | 5.41x | PASS | cross | 0 / 0 | rows and values match |
| q18 | 598.9 | 475.8 | 1.26x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 298.5 | 2329.2 | 0.13x | PASS | cross | 100 / 100 | rows and values match |
| q20 | 72.2 | 63.0 | 1.15x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 672.6 | 73.5 | 9.16x | PASS | cross | 100 / 100 | rows and values match |
| q22 | 3640.8 | 6089.1 | 0.60x | PASS | cross | 100 / 100 | rows and values match |
| q23 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 80.2 MB for fedq_collect with 31.4 GB already allocated for this reservation - 70.1 MB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q24 | 701.7 | 548.2 | 1.28x | PASS | cross | 5 / 5 | rows and values match |
| q25 | 1000.8 | 147.4 | 6.79x | PASS | cross | 2 / 2 | rows and values match |
| q26 | 221.4 | 209.0 | 1.06x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 313.3 | 239.3 | 1.31x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 419.3 | 383.9 | 1.09x | PASS | single | 1 / 1 | rows and values match |
| q29 | 212.1 | 225.9 | 0.94x | PASS | cross | 8 / 8 | rows and values match |
| q30 | 238.4 | 244.7 | 0.97x | PASS | cross | 100 / 100 | rows and values match |
| q31 | 3679.7 | 188.7 | 19.50x | PASS | cross | 307 / 307 | rows and values match |
| q32 | 107.7 | 34.7 | 3.11x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 568.4 | 185.8 | 3.06x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 370.5 | 142.0 | 2.61x | PASS | cross | 1560 / 1560 | rows and values match |
| q35 | 782.4 | 556.7 | 1.41x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 675.6 | 142.4 | 4.75x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 219.5 | 183.3 | 1.20x | PASS | cross | 2 / 2 | rows and values match |
| q38 | 1830.7 | 393.4 | 4.65x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 8157.0 | 425.4 | 19.18x | PASS | cross | 2433 / 2433 | rows and values match |
| q40 | 335.2 | 68.1 | 4.92x | PASS | cross | 100 / 100 | rows and values match |
| q41 | 80.1 | 55.7 | 1.44x | PASS | single | 41 / 41 | rows and values match |
| q42 | 133.5 | 69.3 | 1.93x | PASS | cross | 11 / 11 | rows and values match |
| q43 | 321.0 | 118.1 | 2.72x | PASS | cross | 18 / 18 | rows and values match |
| q44 | 1764.3 | 133.3 | 13.24x | PASS | cross | 10 / 10 | rows and values match |
| q45 | 440.8 | 152.9 | 2.88x | PASS | cross | 49 / 49 | rows and values match |
| q46 | 1073.6 | 259.0 | 4.15x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 1643.7 | 1065.6 | 1.54x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 434.9 | 370.3 | 1.17x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 216.0 | 103.3 | 2.09x | PASS | cross | 46 / 46 | rows and values match |
| q50 | 171.3 | 265.2 | 0.65x | PASS | cross | 51 / 51 | rows and values match |
| q51 | 2241.1 | 1682.4 | 1.33x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 131.6 | 67.7 | 1.94x | PASS | cross | 100 / 100 | rows and values match |
| q53 | 228.9 | 97.6 | 2.34x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 1034.4 | 208.4 | 4.96x | PASS | cross | 1 / 1 | rows and values match |
| q55 | 120.7 | 68.7 | 1.76x | PASS | cross | 100 / 100 | rows and values match |
| q56 | 468.8 | 173.0 | 2.71x | PASS | cross | 100 / 100 | rows and values match |
| q57 | 979.0 | 400.6 | 2.44x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 2625.1 | 155.2 | 16.91x | PASS | cross | 31 / 31 | rows and values match |
| q59 | 1930.1 | 461.3 | 4.18x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 592.0 | 189.8 | 3.12x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 510.8 | 297.5 | 1.72x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 431.9 | 552.4 | 0.78x | PASS | cross | 100 / 100 | rows and values match |
| q63 | 238.0 | 100.9 | 2.36x | PASS | cross | 100 / 100 | rows and values match |
| q64 | - | - | - | ERROR | cross | - | Killed: worker exited with code 137 (likely memory limit) |
| q65 | 1535.9 | 547.6 | 2.81x | PASS | cross | 100 / 100 | rows and values match |
| q66 | 719.3 | 139.6 | 5.15x | PASS | cross | 10 / 10 | rows and values match |
| q67 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 70.5 MB for fedq_collect with 31.7 GB already allocated for this reservation - 19.1 MB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q68 | 1173.9 | 252.6 | 4.65x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 675.2 | 393.4 | 1.72x | PASS | cross | 100 / 100 | rows and values match |
| q70 | 2548.3 | 208.4 | 12.23x | PASS | cross | 7 / 7 | rows and values match |
| q71 | 222.0 | 155.5 | 1.43x | PASS | cross | 9655 / 9655 | rows and values match |
| q72 | 6261.4 | 16248.2 | 0.39x | PASS | cross | 100 / 100 | rows and values match |
| q73 | 367.5 | 150.1 | 2.45x | PASS | cross | 17 / 17 | rows and values match |
| q74 | 3574.8 | 554.1 | 6.45x | PASS | cross | 100 / 100 | rows and values match |
| q75 | 2218.3 | 547.9 | 4.05x | PASS | cross | 100 / 100 | rows and values match |
| q76 | 312.3 | 158.4 | 1.97x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 292.3 | 108.4 | 2.70x | PASS | cross | 100 / 100 | rows and values match |
| q78 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 129.0 MB for fedq_collect with 31.2 GB already allocated for this reservation - 104.6 MB remain available for the total memory pool: fair(pool_size: 32.0 GB) |
| q79 | 2934.5 | 191.6 | 15.31x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 572.9 | 281.5 | 2.03x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 392.5 | 288.2 | 1.36x | PASS | cross | 100 / 100 | rows and values match |
| q82 | 283.5 | 352.2 | 0.80x | PASS | cross | 6 / 6 | rows and values match |
| q83 | 391.1 | 93.6 | 4.18x | PASS | cross | 100 / 100 | rows and values match |
| q84 | 136.1 | 212.5 | 0.64x | PASS | cross | 100 / 100 | rows and values match |
| q85 | 342.3 | 579.7 | 0.59x | PASS | cross | 15 / 15 | rows and values match |
| q86 | 185.1 | 79.8 | 2.32x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 1794.2 | 460.0 | 3.90x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 841.8 | 720.5 | 1.17x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 300.5 | 129.1 | 2.33x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 94.3 | 57.2 | 1.65x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 188.8 | 378.2 | 0.50x | PASS | cross | 12 / 12 | rows and values match |
| q92 | 92.7 | 45.4 | 2.04x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 127.5 | 311.5 | 0.41x | PASS | cross | 100 / 100 | rows and values match |
| q94 | 207.3 | 65.6 | 3.16x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 6654.4 | 1069.5 | 6.22x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 83.7 | 53.6 | 1.56x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 617.1 | 349.6 | 1.77x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 104.9 | 152.5 | 0.69x | PASS | cross | 15076 / 15076 | rows and values match |
| q99 | 716.3 | 931.4 | 0.77x | PASS | cross | 100 / 100 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 117977.6 | 55211.1 | 2.14x | 2.28x | 95 |
