# TPC-DS federated benchmark report

Commit: `93cf98e` - handoff: schema memoization done (geomean 2.80x); CTE materialize-once is next  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 16:28
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
| q01 | 47.9 | 31.7 | 1.51x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 117.8 | 48.5 | 2.43x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 35.4 | 29.7 | 1.19x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 288.5 | 69.2 | 4.17x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 147.4 | 36.7 | 4.02x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 117.1 | 34.5 | 3.39x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 70.4 | 85.6 | 0.82x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 83.3 | 34.5 | 2.41x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 99.9 | 10.1 | 9.91x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 531.7 | 117.6 | 4.52x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 171.4 | 55.4 | 3.09x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 38.8 | 20.0 | 1.94x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 103.8 | 52.3 | 1.98x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 372.0 | 103.1 | 3.61x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 38.9 | 33.3 | 1.17x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 65.0 | 17.9 | 3.64x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 121.9 | 39.1 | 3.12x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 109.4 | 80.9 | 1.35x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 53.3 | 35.9 | 1.49x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 36.0 | 19.7 | 1.83x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 51.9 | 21.2 | 2.45x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 49.8 | 24.4 | 2.04x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 301.6 | 72.8 | 4.14x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 56.8 | 19.1 | 2.97x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 79.7 | 37.3 | 2.14x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 66.1 | 62.4 | 1.06x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 72.9 | 68.8 | 1.06x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 80.3 | 9.3 | 8.61x | PASS | single | 1 / 1 | rows and values match |
| q29 | 85.5 | 47.4 | 1.80x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 62.3 | 30.8 | 2.02x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 116.9 | 43.9 | 2.67x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 35.4 | 24.2 | 1.46x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 243.5 | 39.2 | 6.21x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 67.6 | 28.6 | 2.36x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 593.8 | 79.6 | 7.46x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 67.1 | 25.8 | 2.61x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 27.8 | 19.2 | 1.45x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 89.8 | 41.2 | 2.18x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 58.0 | 31.6 | 1.83x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 148.3 | 25.0 | 5.94x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 37.2 | 6.6 | 5.64x | PASS | single | 0 / 0 | rows and values match |
| q42 | 36.1 | 18.6 | 1.94x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 44.4 | 33.8 | 1.31x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 69.6 | 4.7 | 14.67x | PASS | cross | 0 / 0 | rows and values match |
| q45 | 122.3 | 29.6 | 4.14x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 112.4 | 30.7 | 3.66x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 105.8 | 42.9 | 2.47x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 100.1 | 64.1 | 1.56x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 138.9 | 32.9 | 4.22x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 67.8 | 30.9 | 2.20x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 93.9 | 48.4 | 1.94x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 35.5 | 17.7 | 2.01x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 61.1 | 26.1 | 2.34x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 140.9 | 48.7 | 2.89x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 36.5 | 24.4 | 1.49x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 161.8 | 40.4 | 4.00x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 100.2 | 46.4 | 2.16x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 237.0 | 67.1 | 3.53x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 110.5 | 79.5 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 182.0 | 38.4 | 4.74x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 116.2 | 35.8 | 3.25x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 64.1 | 34.1 | 1.88x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 58.1 | 35.3 | 1.64x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 233.8 | 87.9 | 2.66x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 63.5 | 28.8 | 2.21x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 233.2 | 73.8 | 3.16x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 104.2 | 61.7 | 1.69x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 109.9 | 41.5 | 2.65x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 140.1 | 95.0 | 1.48x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 73.0 | 33.5 | 2.18x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 88.4 | 57.0 | 1.55x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 155.2 | 68.5 | 2.27x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 64.7 | 26.2 | 2.47x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 123.3 | 42.8 | 2.88x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 156.7 | 39.3 | 3.98x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 85.2 | 34.0 | 2.51x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 150.6 | 34.5 | 4.36x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 198.6 | 40.2 | 4.94x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 79.3 | 28.3 | 2.80x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 163.5 | 47.2 | 3.46x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 89.0 | 32.7 | 2.72x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 33.9 | 14.7 | 2.31x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 172.4 | 62.9 | 2.74x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 44.5 | 27.9 | 1.60x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 137.4 | 77.0 | 1.78x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 43.0 | 19.7 | 2.18x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 94.3 | 52.9 | 1.78x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 363.6 | 204.7 | 1.78x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 61.8 | 24.1 | 2.56x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 81.5 | 39.1 | 2.08x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 74.3 | 60.4 | 1.23x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 42.4 | 23.8 | 1.78x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 22.2 | 4.5 | 4.95x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 60.2 | 24.9 | 2.42x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 48.7 | 29.3 | 1.66x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 38.0 | 24.7 | 1.54x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 53.7 | 29.6 | 1.81x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 40.6 | 20.7 | 1.96x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 66.4 | 26.1 | 2.54x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 11028.5 | 4184.5 | 2.64x | 2.48x | 99 |
