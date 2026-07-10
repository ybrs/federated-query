# TPC-DS federated benchmark report

Commit: `0ed2f39` - plan: parallel reads (cross-step scheduler; investigation + phased design)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 04:17
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 120.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: adversarial

[adversarial] Total 99 | PASS 99 | MISMATCH 0 | ERROR 0 | cross-source 95

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 48.0 | 35.8 | 1.34x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 120.6 | 56.6 | 2.13x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 26.3 | 17.5 | 1.51x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 414.1 | 99.1 | 4.18x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 201.6 | 74.6 | 2.70x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 67.0 | 51.9 | 1.29x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 47.6 | 64.7 | 0.74x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 119.6 | 25.8 | 4.63x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 59.6 | 8.3 | 7.16x | PASS | single | 1 / 1 | rows and values match |
| q10 | 120.1 | 114.4 | 1.05x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 263.0 | 59.0 | 4.46x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 41.1 | 20.9 | 1.97x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 96.4 | 65.6 | 1.47x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 344.6 | 112.2 | 3.07x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 76.5 | 42.9 | 1.78x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 168.2 | 72.7 | 2.31x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 83.2 | 79.2 | 1.05x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 125.5 | 147.6 | 0.85x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 45.1 | 24.5 | 1.84x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 61.8 | 39.8 | 1.55x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 76.1 | 85.7 | 0.89x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 68.5 | 78.3 | 0.87x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 277.4 | 102.1 | 2.72x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 58.3 | 17.1 | 3.41x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 91.7 | 69.8 | 1.31x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 86.0 | 91.0 | 0.95x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 67.9 | 45.5 | 1.49x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 70.7 | 10.0 | 7.06x | PASS | single | 1 / 1 | rows and values match |
| q29 | 169.7 | 65.1 | 2.61x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 121.4 | 38.0 | 3.20x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 135.4 | 35.7 | 3.79x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 28.1 | 51.1 | 0.55x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 106.4 | 53.8 | 1.98x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 76.0 | 28.9 | 2.63x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 126.6 | 92.0 | 1.38x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 54.8 | 28.1 | 1.95x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 269.7 | 82.0 | 3.29x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 83.7 | 56.2 | 1.49x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 268.6 | 85.0 | 3.16x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 70.0 | 51.2 | 1.37x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 38.7 | 4.3 | 8.91x | PASS | single | 0 / 0 | rows and values match |
| q42 | 27.2 | 22.7 | 1.20x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 42.2 | 29.0 | 1.45x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 60.6 | 2.7 | 22.38x | PASS | single | 0 / 0 | rows and values match |
| q45 | 86.9 | 28.1 | 3.09x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 59.4 | 37.4 | 1.59x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 87.0 | 44.8 | 1.94x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 98.2 | 60.5 | 1.62x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 148.9 | 80.5 | 1.85x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 64.7 | 40.7 | 1.59x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 80.8 | 50.0 | 1.62x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 32.6 | 27.0 | 1.21x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 71.3 | 26.5 | 2.69x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 110.3 | 52.6 | 2.10x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 29.3 | 16.0 | 1.83x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 108.0 | 58.3 | 1.85x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 108.1 | 57.4 | 1.88x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 186.0 | 80.5 | 2.31x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 124.7 | 66.6 | 1.87x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 117.0 | 59.1 | 1.98x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 87.1 | 38.7 | 2.25x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 66.2 | 23.7 | 2.79x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 75.6 | 25.4 | 2.97x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 211.4 | 91.9 | 2.30x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 69.8 | 26.6 | 2.62x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 318.8 | 107.7 | 2.96x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 97.6 | 64.1 | 1.52x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 71.3 | 37.8 | 1.89x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 125.1 | 88.6 | 1.41x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 79.5 | 28.1 | 2.84x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 100.3 | 81.0 | 1.24x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 188.8 | 167.1 | 1.13x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 72.9 | 22.7 | 3.21x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 221.5 | 40.7 | 5.45x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 163.0 | 71.2 | 2.29x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 85.8 | 55.9 | 1.54x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 104.5 | 81.9 | 1.28x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 164.7 | 76.6 | 2.15x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 54.3 | 25.0 | 2.17x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 180.5 | 99.2 | 1.82x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 58.3 | 26.9 | 2.17x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 36.3 | 73.7 | 0.49x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 136.1 | 82.4 | 1.65x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 66.5 | 34.2 | 1.94x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 140.2 | - | - | PASS | cross | 0 / 0 | rows and values match |
| q86 | 44.7 | 19.4 | 2.31x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 79.6 | 55.3 | 1.44x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 323.6 | 227.7 | 1.42x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 85.6 | 31.7 | 2.70x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 63.1 | 39.7 | 1.59x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 90.8 | 52.5 | 1.73x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 36.9 | 20.0 | 1.84x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 37.2 | 11.9 | 3.13x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 54.6 | 16.7 | 3.27x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 51.9 | 29.1 | 1.79x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 30.3 | 22.7 | 1.33x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 55.0 | 38.9 | 1.42x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 43.4 | 19.2 | 2.26x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 108.2 | 39.4 | 2.74x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [adversarial]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 10459.7 | 5319.8 | 1.97x | 2.01x | 98 |
