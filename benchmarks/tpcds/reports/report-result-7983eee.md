# TPC-DS federated benchmark report

Commit: `7983eee` - handoff: refresh stale suite count  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 17:08
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 1, PostgreSQL + DuckDB split, per-query timeout 120.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 99 | MISMATCH 0 | ERROR 0 | cross-source 97

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 67.8 | 51.0 | 1.33x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 207.1 | 64.7 | 3.20x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 42.4 | 29.7 | 1.43x | PASS | cross | 89 / 89 | rows and values match |
| q04 | 1451.2 | 217.9 | 6.66x | PASS | cross | 6 / 6 | rows and values match |
| q05 | 423.7 | 45.8 | 9.26x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 468.8 | 88.0 | 5.32x | PASS | cross | 46 / 46 | rows and values match |
| q07 | 146.4 | 209.8 | 0.70x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 149.2 | 80.7 | 1.85x | PASS | cross | 5 / 5 | rows and values match |
| q09 | 143.1 | 41.7 | 3.43x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 212.1 | 419.7 | 0.51x | PASS | cross | 6 / 6 | rows and values match |
| q11 | 784.6 | 148.6 | 5.28x | PASS | cross | 90 / 90 | rows and values match |
| q12 | 43.3 | 40.2 | 1.08x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 220.5 | 217.7 | 1.01x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 918.4 | 451.3 | 2.04x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 74.8 | 61.0 | 1.23x | PASS | cross | 100 / 100 | rows and values match |
| q16 | 106.9 | 36.8 | 2.91x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 215.7 | 77.3 | 2.79x | PASS | cross | 0 / 0 | rows and values match |
| q18 | 279.1 | 381.1 | 0.73x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 94.8 | 87.8 | 1.08x | PASS | cross | 100 / 100 | rows and values match |
| q20 | 40.1 | 45.4 | 0.88x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 97.9 | 37.8 | 2.59x | PASS | cross | 100 / 100 | rows and values match |
| q22 | 307.6 | 330.0 | 0.93x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 939.9 | 302.2 | 3.11x | PASS | cross | 4 / 4 | rows and values match |
| q24 | 169.8 | 113.2 | 1.50x | PASS | cross | 1 / 1 | rows and values match |
| q25 | 174.9 | 73.6 | 2.38x | PASS | cross | 1 / 1 | rows and values match |
| q26 | 142.1 | 195.2 | 0.73x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 155.3 | 212.5 | 0.73x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 118.6 | 51.6 | 2.30x | PASS | single | 1 / 1 | rows and values match |
| q29 | 111.3 | 143.5 | 0.78x | PASS | cross | 1 / 1 | rows and values match |
| q30 | 93.9 | 95.5 | 0.98x | PASS | cross | 100 / 100 | rows and values match |
| q31 | 368.6 | 96.1 | 3.84x | PASS | cross | 44 / 44 | rows and values match |
| q32 | 59.6 | 33.3 | 1.79x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 255.0 | 130.2 | 1.96x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 112.3 | 65.4 | 1.72x | PASS | cross | 455 / 455 | rows and values match |
| q35 | 462.1 | 317.8 | 1.45x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 119.7 | 54.2 | 2.21x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 53.6 | 34.9 | 1.54x | PASS | cross | 1 / 1 | rows and values match |
| q38 | 193.6 | 76.8 | 2.52x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 776.4 | 68.6 | 11.32x | PASS | cross | 243 / 243 | rows and values match |
| q40 | 80.4 | 36.1 | 2.23x | PASS | cross | 100 / 100 | rows and values match |
| q41 | 50.5 | 25.0 | 2.02x | PASS | single | 4 / 4 | rows and values match |
| q42 | 49.4 | 34.1 | 1.45x | PASS | cross | 10 / 10 | rows and values match |
| q43 | 73.9 | 37.1 | 1.99x | PASS | cross | 6 / 6 | rows and values match |
| q44 | 216.6 | 33.6 | 6.44x | PASS | cross | 10 / 10 | rows and values match |
| q45 | 180.7 | 67.4 | 2.68x | PASS | cross | 19 / 19 | rows and values match |
| q46 | 199.7 | 93.4 | 2.14x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 305.2 | 126.9 | 2.40x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 205.5 | 206.5 | 1.00x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 164.7 | 40.6 | 4.06x | PASS | cross | 34 / 34 | rows and values match |
| q50 | 83.1 | 81.6 | 1.02x | PASS | cross | 6 / 6 | rows and values match |
| q51 | 270.1 | 205.8 | 1.31x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 53.0 | 29.7 | 1.79x | PASS | cross | 100 / 100 | rows and values match |
| q53 | 82.1 | 40.4 | 2.03x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 197.2 | 76.6 | 2.57x | PASS | cross | 1 / 1 | rows and values match |
| q55 | 50.8 | 34.5 | 1.47x | PASS | cross | 100 / 100 | rows and values match |
| q56 | 218.1 | 111.9 | 1.95x | PASS | cross | 100 / 100 | rows and values match |
| q57 | 172.0 | 91.3 | 1.88x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 382.0 | 85.9 | 4.45x | PASS | cross | 5 / 5 | rows and values match |
| q59 | 287.2 | 116.6 | 2.46x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 239.9 | 111.0 | 2.16x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 189.2 | 97.3 | 1.94x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 120.4 | 78.0 | 1.54x | PASS | cross | 100 / 100 | rows and values match |
| q63 | 93.1 | 53.2 | 1.75x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 822.4 | 482.8 | 1.70x | PASS | cross | 2 / 2 | rows and values match |
| q65 | 147.6 | 65.8 | 2.24x | PASS | cross | 100 / 100 | rows and values match |
| q66 | 286.7 | 86.1 | 3.33x | PASS | cross | 5 / 5 | rows and values match |
| q67 | 456.5 | 482.6 | 0.95x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 196.8 | 95.0 | 2.07x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 183.9 | 284.0 | 0.65x | PASS | cross | 100 / 100 | rows and values match |
| q70 | 119.7 | 50.6 | 2.37x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 93.3 | 74.9 | 1.25x | PASS | cross | 1031 / 1031 | rows and values match |
| q72 | 721.3 | 788.2 | 0.92x | PASS | cross | 100 / 100 | rows and values match |
| q73 | 90.0 | 63.1 | 1.43x | PASS | cross | 1 / 1 | rows and values match |
| q74 | 371.1 | 100.4 | 3.70x | PASS | cross | 92 / 92 | rows and values match |
| q75 | 371.0 | 101.3 | 3.66x | PASS | cross | 100 / 100 | rows and values match |
| q76 | 127.0 | 58.9 | 2.16x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 155.7 | 41.7 | 3.73x | PASS | cross | 44 / 44 | rows and values match |
| q78 | 1082.5 | 137.6 | 7.87x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 261.9 | 44.7 | 5.86x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 224.5 | 87.4 | 2.57x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 110.3 | 108.8 | 1.01x | PASS | cross | 100 / 100 | rows and values match |
| q82 | 62.5 | 56.0 | 1.12x | PASS | cross | 2 / 2 | rows and values match |
| q83 | 205.0 | 84.2 | 2.44x | PASS | cross | 24 / 24 | rows and values match |
| q84 | 105.3 | 153.5 | 0.69x | PASS | cross | 16 / 16 | rows and values match |
| q85 | 286.1 | 388.5 | 0.74x | PASS | cross | 1 / 1 | rows and values match |
| q86 | 58.3 | 42.2 | 1.38x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 205.7 | 81.9 | 2.51x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 399.6 | 269.3 | 1.48x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 95.5 | 46.2 | 2.07x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 79.8 | 42.4 | 1.88x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 141.0 | 194.0 | 0.73x | PASS | cross | 2 / 2 | rows and values match |
| q92 | 59.6 | 41.7 | 1.43x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 42.8 | 39.0 | 1.10x | PASS | cross | 100 / 100 | rows and values match |
| q94 | 107.5 | 45.9 | 2.34x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 639.0 | 202.1 | 3.16x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 42.7 | 28.1 | 1.52x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 108.4 | 58.6 | 1.85x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 46.1 | 53.0 | 0.87x | PASS | cross | 2521 / 2521 | rows and values match |
| q99 | 119.2 | 107.6 | 1.11x | PASS | cross | 90 / 90 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 23393.5 | 12230.8 | 1.91x | 1.86x | 99 |
