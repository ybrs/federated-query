# TPC-DS federated benchmark report

Commit: `01531ae` - handoff: parallel reads Phase A landed; Phase B pointers  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 04:36
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
| q01 | 42.3 | 30.5 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 108.6 | 53.1 | 2.04x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 26.5 | 16.7 | 1.59x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 413.6 | 99.2 | 4.17x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 148.6 | 72.6 | 2.05x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 64.2 | 33.2 | 1.93x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 49.8 | 60.0 | 0.83x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 117.9 | 25.2 | 4.68x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 48.6 | 7.3 | 6.62x | PASS | single | 1 / 1 | rows and values match |
| q10 | 110.9 | 113.8 | 0.97x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 227.2 | 54.6 | 4.16x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 38.9 | 17.7 | 2.20x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 99.9 | 65.1 | 1.53x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 339.5 | 111.8 | 3.04x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 52.9 | 42.1 | 1.26x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 127.0 | 72.1 | 1.76x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 91.4 | 65.2 | 1.40x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 108.0 | 129.3 | 0.83x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 44.2 | 26.1 | 1.69x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 58.3 | 38.5 | 1.51x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 102.9 | 77.6 | 1.32x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 54.9 | 78.9 | 0.70x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 236.7 | 98.9 | 2.39x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 58.7 | 16.0 | 3.67x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 88.0 | 70.8 | 1.24x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 69.1 | 97.9 | 0.71x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 66.7 | 55.4 | 1.20x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 72.0 | 10.1 | 7.15x | PASS | single | 1 / 1 | rows and values match |
| q29 | 167.9 | 66.5 | 2.52x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 115.4 | 31.5 | 3.66x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 125.5 | 34.2 | 3.66x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 31.8 | 48.7 | 0.65x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 107.8 | 56.4 | 1.91x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 77.3 | 35.3 | 2.19x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 121.6 | 96.7 | 1.26x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 55.8 | 30.4 | 1.84x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 254.3 | 88.9 | 2.86x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 77.9 | 61.7 | 1.26x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 207.2 | 78.9 | 2.63x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 62.0 | 43.7 | 1.42x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 42.1 | 4.5 | 9.41x | PASS | single | 0 / 0 | rows and values match |
| q42 | 26.7 | 18.3 | 1.45x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 40.0 | 23.0 | 1.74x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 56.7 | 2.7 | 20.88x | PASS | single | 0 / 0 | rows and values match |
| q45 | 83.8 | 21.5 | 3.89x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 60.2 | 26.2 | 2.29x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 80.1 | 40.7 | 1.97x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 85.6 | 63.6 | 1.35x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 138.1 | 91.8 | 1.50x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 68.5 | 38.2 | 1.79x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 80.1 | 47.8 | 1.67x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 26.3 | 18.5 | 1.42x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 72.1 | 21.7 | 3.33x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 112.3 | 48.4 | 2.32x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 26.0 | 16.8 | 1.55x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 113.3 | 60.7 | 1.87x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 103.1 | 57.7 | 1.79x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 181.0 | 84.3 | 2.15x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 114.4 | 63.1 | 1.81x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 108.0 | 54.5 | 1.98x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 76.5 | 34.4 | 2.22x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 64.5 | 18.7 | 3.45x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 75.2 | 23.5 | 3.19x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 212.8 | 93.9 | 2.27x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 68.2 | 28.7 | 2.37x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 257.6 | 125.0 | 2.06x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 96.0 | 75.5 | 1.27x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 68.5 | 27.0 | 2.54x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 129.2 | 94.0 | 1.37x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 80.1 | 29.9 | 2.67x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 82.7 | 79.9 | 1.04x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 195.5 | 181.4 | 1.08x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 70.3 | 31.0 | 2.27x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 180.2 | 44.4 | 4.06x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 172.7 | 77.5 | 2.23x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 80.9 | 47.3 | 1.71x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 102.3 | 72.4 | 1.41x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 160.7 | 78.4 | 2.05x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 65.5 | 28.9 | 2.27x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 185.0 | 86.1 | 2.15x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 55.8 | 27.3 | 2.05x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 34.0 | 64.4 | 0.53x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 124.5 | 71.4 | 1.74x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 61.3 | 41.7 | 1.47x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 133.1 | - | - | PASS | cross | 0 / 0 | rows and values match |
| q86 | 44.4 | 19.7 | 2.25x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 73.0 | 54.0 | 1.35x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 312.3 | 275.1 | 1.14x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 69.3 | 33.0 | 2.10x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 57.0 | 37.4 | 1.52x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 75.3 | 71.3 | 1.06x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 35.9 | 21.3 | 1.69x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 35.9 | 12.5 | 2.87x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 54.6 | 25.5 | 2.14x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 52.5 | 28.4 | 1.85x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 31.7 | 23.1 | 1.37x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 59.7 | 43.4 | 1.37x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 39.7 | 25.2 | 1.57x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 128.9 | 48.7 | 2.65x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [adversarial]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 9859.5 | 5318.0 | 1.85x | 1.94x | 98 |
