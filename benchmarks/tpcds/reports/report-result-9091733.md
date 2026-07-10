# TPC-DS federated benchmark report

Commit: `9091733` - docs: eager aggregation Phase A landed + gate findings  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 16:45
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
| q01 | 34.2 | 36.6 | 0.93x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 85.2 | 56.4 | 1.51x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 20.6 | 17.6 | 1.17x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 136.8 | 101.0 | 1.35x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 96.5 | 74.3 | 1.30x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 45.6 | 45.5 | 1.00x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 39.3 | 53.7 | 0.73x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 57.2 | 31.4 | 1.83x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 25.9 | 7.2 | 3.61x | PASS | single | 1 / 1 | rows and values match |
| q10 | 75.8 | 106.6 | 0.71x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 76.4 | 50.0 | 1.53x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 32.1 | 17.0 | 1.89x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 56.5 | 44.8 | 1.26x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 202.1 | 115.0 | 1.76x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 41.2 | 41.0 | 1.00x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 109.9 | 79.2 | 1.39x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 59.1 | 66.5 | 0.89x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 81.1 | 138.0 | 0.59x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 24.4 | 40.4 | 0.60x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 39.4 | 39.0 | 1.01x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 56.4 | 76.3 | 0.74x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 42.4 | 78.4 | 0.54x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 202.3 | 107.6 | 1.88x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 41.7 | 15.1 | 2.77x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 48.6 | 70.7 | 0.69x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 70.9 | 116.9 | 0.61x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 45.4 | 65.2 | 0.70x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 31.3 | 10.8 | 2.90x | PASS | single | 1 / 1 | rows and values match |
| q29 | 137.6 | 64.2 | 2.15x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 44.2 | 36.0 | 1.23x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 62.8 | 33.2 | 1.89x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 16.4 | 46.8 | 0.35x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 67.5 | 51.0 | 1.32x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 31.9 | 33.2 | 0.96x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 73.9 | 130.5 | 0.57x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 35.8 | 34.0 | 1.05x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 247.1 | 88.5 | 2.79x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 49.1 | 53.4 | 0.92x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 144.6 | 78.5 | 1.84x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 42.1 | 51.6 | 0.82x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 24.3 | 5.3 | 4.62x | PASS | single | 0 / 0 | rows and values match |
| q42 | 20.0 | 16.7 | 1.20x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 24.2 | 22.6 | 1.07x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 31.0 | 2.6 | 11.84x | PASS | single | 0 / 0 | rows and values match |
| q45 | 53.4 | 27.5 | 1.95x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 46.2 | 35.7 | 1.29x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 61.7 | 33.6 | 1.84x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 55.4 | 67.3 | 0.82x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 91.8 | 77.2 | 1.19x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 68.4 | 39.3 | 1.74x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 52.0 | 44.3 | 1.17x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 18.9 | 17.0 | 1.11x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 40.1 | 35.3 | 1.14x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 79.0 | 56.4 | 1.40x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 17.7 | 27.4 | 0.64x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 65.9 | 56.8 | 1.16x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 78.7 | 53.3 | 1.48x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 136.0 | 83.8 | 1.62x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 77.7 | 55.7 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 72.1 | 56.1 | 1.29x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 35.9 | 29.4 | 1.22x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 40.7 | 27.1 | 1.51x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 37.2 | 23.5 | 1.58x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 143.9 | 121.6 | 1.18x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 44.6 | 27.4 | 1.63x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 208.7 | 108.3 | 1.93x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 56.3 | 74.2 | 0.76x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 36.5 | 28.0 | 1.30x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 73.0 | 105.4 | 0.69x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 48.9 | 33.3 | 1.47x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 69.8 | 81.8 | 0.85x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 147.6 | 192.0 | 0.77x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 27.7 | 29.8 | 0.93x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 67.2 | 42.7 | 1.58x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 130.9 | 71.9 | 1.82x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 52.6 | 46.7 | 1.13x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 72.7 | 73.2 | 0.99x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 102.6 | 74.7 | 1.37x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 34.9 | 25.6 | 1.36x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 113.1 | 83.8 | 1.35x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 43.3 | 24.3 | 1.78x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 18.0 | 64.6 | 0.28x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 81.5 | 72.0 | 1.13x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 51.5 | 32.2 | 1.60x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 79.8 | - | - | PASS | cross | 0 / 0 | rows and values match |
| q86 | 30.8 | 21.6 | 1.43x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 48.1 | 54.1 | 0.89x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 84.4 | 170.7 | 0.49x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 38.2 | 23.6 | 1.62x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 24.3 | 37.1 | 0.65x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 48.0 | 57.8 | 0.83x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 22.6 | 21.1 | 1.07x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 27.0 | 12.6 | 2.14x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 31.4 | 16.7 | 1.89x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 29.1 | 39.8 | 0.73x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 16.0 | 24.5 | 0.65x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 44.2 | 44.3 | 1.00x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 27.4 | 24.3 | 1.13x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 86.5 | 40.4 | 2.14x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [adversarial]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 6243.0 | 5298.6 | 1.18x | 1.21x | 98 |
