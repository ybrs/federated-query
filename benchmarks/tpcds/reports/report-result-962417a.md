# TPC-DS federated benchmark report

Commit: `962417a` - benchmark numbers  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 16:08
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 60.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 99 | MISMATCH 0 | ERROR 0 | cross-source 97

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 58.3 | 20.9 | 2.80x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 135.4 | 44.8 | 3.02x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 31.4 | 20.3 | 1.55x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 1209.5 | 66.4 | 18.22x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 149.0 | 33.8 | 4.41x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 132.3 | 47.9 | 2.76x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 80.7 | 80.8 | 1.00x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 89.1 | 33.4 | 2.67x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 108.1 | 8.2 | 13.13x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 562.1 | 90.6 | 6.20x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 532.0 | 60.0 | 8.86x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 38.8 | 19.5 | 1.99x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 96.0 | 46.5 | 2.06x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 710.9 | 92.5 | 7.69x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 41.5 | 21.5 | 1.93x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 70.9 | 19.6 | 3.61x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 122.2 | 37.4 | 3.26x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 118.8 | 85.7 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 55.4 | 24.8 | 2.23x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 35.4 | 18.8 | 1.88x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 50.7 | 20.2 | 2.51x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 46.7 | 23.7 | 1.97x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 456.3 | 73.5 | 6.21x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 79.9 | 23.0 | 3.47x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 95.3 | 36.0 | 2.65x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 59.1 | 80.6 | 0.73x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 157.5 | 53.0 | 2.97x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 76.7 | 13.8 | 5.56x | PASS | single | 1 / 1 | rows and values match |
| q29 | 85.3 | 48.0 | 1.78x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 99.7 | 31.1 | 3.20x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 251.9 | 38.6 | 6.53x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 38.3 | 21.3 | 1.80x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 193.8 | 38.4 | 5.05x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 65.5 | 26.7 | 2.45x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 571.6 | 96.4 | 5.93x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 130.0 | 25.7 | 5.06x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 25.5 | 16.6 | 1.53x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 94.0 | 41.6 | 2.26x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 89.6 | 31.0 | 2.89x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 60.7 | 16.7 | 3.64x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 36.6 | 6.3 | 5.77x | PASS | single | 0 / 0 | rows and values match |
| q42 | 33.7 | 17.7 | 1.91x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 44.2 | 33.5 | 1.32x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 67.0 | 5.1 | 13.12x | PASS | cross | 0 / 0 | rows and values match |
| q45 | 141.1 | 32.7 | 4.32x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 115.7 | 32.9 | 3.52x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 198.4 | 37.4 | 5.30x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 99.5 | 48.0 | 2.07x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 134.0 | 34.1 | 3.93x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 69.7 | 34.2 | 2.04x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 100.4 | 50.6 | 1.98x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 39.7 | 18.0 | 2.20x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 60.8 | 23.5 | 2.59x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 112.6 | 52.3 | 2.15x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 36.2 | 22.3 | 1.62x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 204.9 | 39.2 | 5.23x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 167.2 | 40.3 | 4.15x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 211.2 | 64.7 | 3.26x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 157.7 | 81.6 | 1.93x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 260.3 | 46.7 | 5.58x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 122.5 | 36.2 | 3.38x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 65.5 | 25.9 | 2.53x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 59.9 | 22.6 | 2.65x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 398.1 | 100.9 | 3.95x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 63.9 | 26.1 | 2.44x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 223.8 | 95.9 | 2.33x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 100.5 | 68.6 | 1.47x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 92.6 | 31.4 | 2.95x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 123.7 | 110.5 | 1.12x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 76.4 | 28.6 | 2.68x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 92.6 | 56.9 | 1.63x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 151.6 | 73.2 | 2.07x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 63.2 | 25.7 | 2.46x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 366.0 | 40.7 | 8.99x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 302.0 | 39.2 | 7.69x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 84.3 | 36.6 | 2.30x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 139.9 | 35.1 | 3.98x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 185.6 | 39.0 | 4.76x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 77.8 | 31.1 | 2.50x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 172.9 | 51.6 | 3.35x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 83.0 | 36.4 | 2.28x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 25.9 | 14.8 | 1.75x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 170.0 | 66.5 | 2.56x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 40.7 | 30.1 | 1.35x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 138.7 | 100.5 | 1.38x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 45.2 | 25.3 | 1.79x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 92.5 | 42.3 | 2.19x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 374.3 | 193.2 | 1.94x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 60.0 | 27.2 | 2.20x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 74.1 | 38.9 | 1.90x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 70.1 | 80.5 | 0.87x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 48.1 | 22.7 | 2.12x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 22.2 | 4.2 | 5.23x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 59.1 | 23.6 | 2.50x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 63.7 | 29.7 | 2.14x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 42.9 | 37.1 | 1.15x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 60.3 | 30.4 | 1.98x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 37.5 | 25.8 | 1.46x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 64.4 | 27.7 | 2.33x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 13932.2 | 4183.0 | 3.33x | 2.80x | 99 |
