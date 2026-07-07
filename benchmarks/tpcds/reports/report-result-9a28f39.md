# TPC-DS federated benchmark report

Commit: `9a28f39` - handoff: Phase 1 of disjunctive decorrelation done - PASS 98 | MISMATCH 1 | ERROR 0  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 15:45
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
| q01 | 68.4 | 21.8 | 3.14x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 2552.4 | 47.7 | 53.49x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 38.2 | 25.4 | 1.51x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 1585.4 | 71.5 | 22.17x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 164.6 | 34.9 | 4.72x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 140.1 | 37.2 | 3.76x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 85.2 | 83.0 | 1.03x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 108.6 | 28.4 | 3.83x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 144.3 | 9.7 | 14.84x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 607.7 | 113.6 | 5.35x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 609.0 | 72.4 | 8.41x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 41.6 | 26.1 | 1.59x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 104.3 | 65.6 | 1.59x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 785.3 | 110.8 | 7.09x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 48.1 | 32.7 | 1.47x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 82.2 | 20.9 | 3.93x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 155.6 | 37.6 | 4.14x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 143.3 | 80.4 | 1.78x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 70.0 | 34.9 | 2.01x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 40.8 | 20.9 | 1.96x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 63.4 | 23.0 | 2.75x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 48.2 | 24.9 | 1.93x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 701.2 | 85.3 | 8.22x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 155.2 | 19.7 | 7.90x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 92.2 | 39.0 | 2.36x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 65.8 | 76.4 | 0.86x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 303.7 | 70.9 | 4.28x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 108.2 | 18.4 | 5.89x | PASS | single | 1 / 1 | rows and values match |
| q29 | 90.2 | 49.6 | 1.82x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 106.5 | 42.5 | 2.51x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 315.5 | 41.8 | 7.55x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 43.9 | 22.1 | 1.99x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 164.2 | 39.7 | 4.13x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 74.6 | 25.7 | 2.90x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 595.7 | 89.1 | 6.69x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 164.9 | 23.5 | 7.02x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 27.0 | 17.6 | 1.54x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 92.9 | 42.8 | 2.17x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 255.9 | 21.7 | 11.77x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 63.4 | 23.8 | 2.66x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 37.3 | 6.9 | 5.44x | PASS | single | 0 / 0 | rows and values match |
| q42 | 36.5 | 27.3 | 1.34x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 47.0 | 33.9 | 1.39x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 106.9 | 5.2 | 20.74x | PASS | cross | 0 / 0 | rows and values match |
| q45 | 181.5 | 36.6 | 4.96x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 116.4 | 35.9 | 3.24x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 587.7 | 47.0 | 12.51x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 101.9 | 45.5 | 2.24x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 143.2 | 34.1 | 4.19x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 72.7 | 40.6 | 1.79x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 140.7 | 50.7 | 2.78x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 35.6 | 18.6 | 1.92x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 62.5 | 22.8 | 2.74x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 185.9 | 47.7 | 3.90x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 40.2 | 20.0 | 2.01x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 167.8 | 42.6 | 3.94x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 452.9 | 52.1 | 8.70x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 218.7 | 65.1 | 3.36x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 3989.4 | 74.5 | 53.56x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 153.4 | 40.1 | 3.82x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 121.4 | 36.8 | 3.30x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 68.3 | 27.0 | 2.53x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 57.6 | 35.0 | 1.64x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 3383.8 | 100.4 | 33.70x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 73.3 | 26.5 | 2.76x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 1056.7 | 75.0 | 14.10x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 228.0 | 78.2 | 2.92x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 104.3 | 38.5 | 2.71x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 130.6 | 86.5 | 1.51x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 70.2 | 33.5 | 2.09x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 87.6 | 62.6 | 1.40x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 167.6 | 67.3 | 2.49x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 77.4 | 25.7 | 3.01x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 372.9 | 47.4 | 7.87x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 650.2 | 37.3 | 17.43x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 86.4 | 34.5 | 2.50x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 156.1 | 33.1 | 4.72x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 188.9 | 36.9 | 5.12x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 87.7 | 29.4 | 2.98x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 235.5 | 46.0 | 5.12x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 96.0 | 31.9 | 3.01x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 26.3 | 17.8 | 1.48x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 205.0 | 60.8 | 3.37x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 43.2 | 36.1 | 1.20x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 151.4 | 83.4 | 1.82x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 44.0 | 20.1 | 2.18x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 94.4 | 42.6 | 2.22x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 409.7 | 274.2 | 1.49x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 79.7 | 33.3 | 2.39x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 73.3 | 38.9 | 1.88x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 70.9 | 50.5 | 1.40x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 42.1 | 23.1 | 1.82x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 22.6 | 3.8 | 5.93x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 55.8 | 15.6 | 3.59x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 58.3 | 30.2 | 1.93x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 38.2 | 26.0 | 1.47x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 54.1 | 29.3 | 1.85x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 38.3 | 21.2 | 1.81x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 69.9 | 20.5 | 3.41x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 27063.9 | 4337.0 | 6.24x | 3.44x | 99 |
