# TPC-DS federated benchmark report

Commit: `62d153d` - handoff: CTE materialize-once done - geomean 2.48x, totals 11.0s  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 16:42
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
| q01 | 47.3 | 25.8 | 1.83x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 105.7 | 45.0 | 2.35x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 34.8 | 27.3 | 1.28x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 282.8 | 67.5 | 4.19x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 151.5 | 34.0 | 4.45x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 127.4 | 49.8 | 2.56x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 76.4 | 103.7 | 0.74x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 91.9 | 40.4 | 2.28x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 122.3 | 8.7 | 14.07x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 137.3 | 102.5 | 1.34x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 153.9 | 55.2 | 2.79x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 39.4 | 28.4 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 115.2 | 61.6 | 1.87x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 399.9 | 111.2 | 3.60x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 46.2 | 34.5 | 1.34x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 75.5 | 21.5 | 3.51x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 136.8 | 39.0 | 3.51x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 111.6 | 76.1 | 1.47x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 62.1 | 36.0 | 1.72x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 47.0 | 21.1 | 2.23x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 51.2 | 23.3 | 2.20x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 47.0 | 32.9 | 1.43x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 322.2 | 84.2 | 3.83x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 53.8 | 15.4 | 3.49x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 82.3 | 35.2 | 2.33x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 63.0 | 71.6 | 0.88x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 78.1 | 70.4 | 1.11x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 84.3 | 9.9 | 8.49x | PASS | single | 1 / 1 | rows and values match |
| q29 | 102.1 | 51.1 | 2.00x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 68.6 | 38.3 | 1.79x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 128.7 | 39.1 | 3.30x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 37.0 | 23.9 | 1.55x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 196.5 | 43.9 | 4.47x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 69.7 | 33.9 | 2.06x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 178.3 | 95.9 | 1.86x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 70.8 | 30.7 | 2.31x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 26.8 | 22.2 | 1.21x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 97.4 | 46.5 | 2.09x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 59.4 | 21.7 | 2.74x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 57.7 | 18.5 | 3.11x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 38.8 | 7.5 | 5.21x | PASS | single | 0 / 0 | rows and values match |
| q42 | 35.6 | 24.7 | 1.44x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 45.1 | 26.3 | 1.72x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 82.0 | 5.3 | 15.51x | PASS | cross | 0 / 0 | rows and values match |
| q45 | 116.3 | 25.9 | 4.48x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 103.4 | 40.9 | 2.53x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 111.1 | 42.3 | 2.63x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 108.2 | 67.2 | 1.61x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 138.0 | 34.6 | 3.99x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 67.0 | 35.1 | 1.91x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 92.9 | 51.6 | 1.80x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 35.0 | 23.8 | 1.47x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 76.9 | 24.8 | 3.11x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 121.1 | 53.6 | 2.26x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 37.6 | 18.4 | 2.05x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 155.1 | 42.5 | 3.65x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 95.7 | 39.5 | 2.42x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 242.3 | 66.1 | 3.66x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 147.1 | 77.8 | 1.89x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 170.5 | 44.0 | 3.87x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 132.0 | 41.9 | 3.15x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 66.5 | 34.5 | 1.93x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 63.1 | 36.6 | 1.72x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 235.3 | 108.6 | 2.17x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 75.6 | 28.9 | 2.62x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 236.7 | 76.9 | 3.08x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 103.6 | 71.0 | 1.46x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 97.5 | 33.3 | 2.93x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 125.8 | 81.5 | 1.54x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 81.6 | 36.3 | 2.25x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 97.5 | 64.5 | 1.51x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 159.7 | 68.9 | 2.32x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 62.5 | 25.9 | 2.41x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 116.4 | 42.4 | 2.75x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 167.6 | 38.3 | 4.37x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 99.1 | 34.3 | 2.89x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 144.3 | 43.3 | 3.33x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 192.6 | 42.2 | 4.57x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 84.2 | 36.9 | 2.28x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 191.8 | 46.7 | 4.11x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 65.4 | 34.6 | 1.89x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 28.6 | 15.2 | 1.88x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 177.3 | 62.6 | 2.83x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 42.6 | 44.5 | 0.96x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 152.9 | 76.8 | 1.99x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 57.2 | 30.1 | 1.90x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 111.8 | 44.2 | 2.53x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 391.3 | 194.2 | 2.02x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 66.0 | 31.4 | 2.11x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 88.4 | 39.7 | 2.22x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 67.0 | 58.6 | 1.14x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 50.0 | 21.8 | 2.30x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 25.6 | 4.2 | 6.15x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 79.3 | 21.7 | 3.66x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 53.2 | 34.6 | 1.54x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 43.7 | 34.1 | 1.28x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 79.4 | 26.8 | 2.96x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 42.7 | 28.2 | 1.51x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 69.6 | 24.9 | 2.79x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 10484.9 | 4370.7 | 2.40x | 2.39x | 99 |
