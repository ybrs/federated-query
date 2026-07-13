# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 17:40

Tally: 96 ok | 0 wrong | 3 error   (total 99 queries, 13.0s)
Timing: ours 5.7s  duckdb 4.0s  ->  total 1.45x  geomean 1.40x  (96 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "X" does not exist (3) [ERROR]
Queries: q04, q21, q39

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.7 | 4.0 | 1.45x | 1.40x | 96 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 173.6 | 33.4 | 22.1 | 1.51x | 100 / 100 | rows and values match |
| q02 | OK | 151.3 | 89.6 | 42.0 | 2.13x | 2513 / 2513 | rows and values match |
| q03 | OK | 33.2 | 23.3 | 19.1 | 1.22x | 15 / 15 | rows and values match |
| q04 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q05 | OK | 98.0 | 54.2 | 34.2 | 1.59x | 100 / 100 | rows and values match |
| q06 | OK | 90.1 | 51.8 | 44.1 | 1.18x | 10 / 10 | rows and values match |
| q07 | OK | 41.8 | 29.3 | 72.6 | 0.40x | 100 / 100 | rows and values match |
| q08 | OK | 52.2 | 40.6 | 35.2 | 1.15x | 0 / 0 | rows and values match |
| q09 | OK | 12.3 | 12.9 | 8.3 | 1.55x | 1 / 1 | rows and values match |
| q10 | OK | 71.3 | 55.1 | 99.6 | 0.55x | 1 / 1 | rows and values match |
| q11 | OK | 81.6 | 77.3 | 52.3 | 1.48x | 4 / 4 | rows and values match |
| q12 | OK | 31.0 | 24.8 | 18.3 | 1.35x | 100 / 100 | rows and values match |
| q13 | OK | 87.0 | 68.4 | 54.8 | 1.25x | 1 / 1 | rows and values match |
| q14 | OK | 266.1 | 236.3 | 94.7 | 2.50x | 100 / 100 | rows and values match |
| q15 | OK | 46.1 | 42.3 | 25.5 | 1.65x | 33 / 33 | rows and values match |
| q16 | OK | 65.6 | 55.7 | 27.8 | 2.00x | 1 / 1 | rows and values match |
| q17 | OK | 73.2 | 65.8 | 38.5 | 1.71x | 1 / 1 | rows and values match |
| q18 | OK | 80.3 | 73.9 | 77.1 | 0.96x | 100 / 100 | rows and values match |
| q19 | OK | 35.9 | 31.4 | 35.4 | 0.89x | 10 / 10 | rows and values match |
| q20 | OK | 33.0 | 33.0 | 21.1 | 1.56x | 100 / 100 | rows and values match |
| q21 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q22 | OK | 52.7 | 41.7 | 25.5 | 1.63x | 100 / 100 | rows and values match |
| q23 | OK | 155.8 | 161.0 | 72.1 | 2.23x | 1 / 1 | rows and values match |
| q24 | OK | 60.0 | 52.6 | 15.8 | 3.33x | 0 / 0 | rows and values match |
| q25 | OK | 62.8 | 61.5 | 36.3 | 1.70x | 0 / 0 | rows and values match |
| q26 | OK | 55.2 | 51.9 | 66.8 | 0.78x | 100 / 100 | rows and values match |
| q27 | OK | 53.5 | 46.9 | 57.4 | 0.82x | 100 / 100 | rows and values match |
| q28 | OK | 27.6 | 19.0 | 10.7 | 1.76x | 1 / 1 | rows and values match |
| q29 | OK | 65.6 | 64.1 | 50.2 | 1.28x | 0 / 0 | rows and values match |
| q30 | OK | 48.9 | 47.1 | 32.2 | 1.46x | 21 / 21 | rows and values match |
| q31 | OK | 76.5 | 71.2 | 39.5 | 1.80x | 1 / 1 | rows and values match |
| q32 | OK | 14.2 | 10.7 | 22.2 | 0.48x | 1 / 1 | rows and values match |
| q33 | OK | 103.5 | 95.0 | 40.3 | 2.36x | 100 / 100 | rows and values match |
| q34 | OK | 42.4 | 32.0 | 26.8 | 1.19x | 53 / 53 | rows and values match |
| q35 | OK | 75.4 | 67.1 | 96.2 | 0.70x | 100 / 100 | rows and values match |
| q36 | OK | 36.8 | 35.6 | 27.3 | 1.30x | 100 / 100 | rows and values match |
| q37 | OK | 238.0 | 234.5 | 16.4 | 14.26x | 0 / 0 | rows and values match |
| q38 | OK | 53.6 | 51.1 | 42.0 | 1.21x | 1 / 1 | rows and values match |
| q39 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q40 | OK | 42.4 | 38.1 | 17.1 | 2.23x | 44 / 44 | rows and values match |
| q41 | OK | 7.8 | 6.7 | 7.1 | 0.95x | 0 / 0 | rows and values match |
| q42 | OK | 17.8 | 17.4 | 17.9 | 0.97x | 4 / 4 | rows and values match |
| q43 | OK | 22.8 | 18.9 | 25.1 | 0.75x | 1 / 1 | rows and values match |
| q44 | OK | 31.7 | 30.1 | 6.5 | 4.64x | 0 / 0 | rows and values match |
| q45 | OK | 54.6 | 58.7 | 34.0 | 1.73x | 24 / 24 | rows and values match |
| q46 | OK | 57.2 | 58.3 | 39.6 | 1.47x | 100 / 100 | rows and values match |
| q47 | OK | 72.1 | 62.1 | 39.8 | 1.56x | 100 / 100 | rows and values match |
| q48 | OK | 73.4 | 69.9 | 59.7 | 1.17x | 1 / 1 | rows and values match |
| q49 | OK | 95.1 | 84.4 | 33.6 | 2.51x | 2 / 2 | rows and values match |
| q50 | OK | 52.9 | 49.8 | 34.4 | 1.45x | 1 / 1 | rows and values match |
| q51 | OK | 63.9 | 56.9 | 51.6 | 1.10x | 100 / 100 | rows and values match |
| q52 | OK | 16.9 | 16.4 | 17.1 | 0.96x | 11 / 11 | rows and values match |
| q53 | OK | 32.3 | 33.6 | 23.8 | 1.41x | 100 / 100 | rows and values match |
| q54 | OK | 80.0 | 73.6 | 54.3 | 1.36x | 0 / 0 | rows and values match |
| q55 | OK | 18.5 | 15.5 | 18.2 | 0.85x | 20 / 20 | rows and values match |
| q56 | OK | 92.0 | 87.2 | 40.8 | 2.14x | 38 / 38 | rows and values match |
| q57 | OK | 92.5 | 86.6 | 48.1 | 1.80x | 100 / 100 | rows and values match |
| q58 | OK | 120.8 | 119.3 | 63.0 | 1.89x | 0 / 0 | rows and values match |
| q59 | OK | 81.9 | 79.3 | 52.2 | 1.52x | 100 / 100 | rows and values match |
| q60 | OK | 94.2 | 87.1 | 41.8 | 2.08x | 100 / 100 | rows and values match |
| q61 | OK | 53.0 | 51.9 | 37.9 | 1.37x | 1 / 1 | rows and values match |
| q62 | OK | 43.7 | 34.5 | 28.8 | 1.20x | 6 / 6 | rows and values match |
| q63 | OK | 32.8 | 32.1 | 24.4 | 1.31x | 100 / 100 | rows and values match |
| q64 | OK | 187.2 | 179.4 | 93.0 | 1.93x | 0 / 0 | rows and values match |
| q65 | OK | 43.8 | 37.3 | 27.9 | 1.34x | 0 / 0 | rows and values match |
| q66 | OK | 174.5 | 132.4 | 70.3 | 1.88x | 1 / 1 | rows and values match |
| q67 | OK | 61.4 | 57.4 | 77.1 | 0.74x | 100 / 100 | rows and values match |
| q68 | OK | 48.7 | 52.0 | 36.5 | 1.43x | 100 / 100 | rows and values match |
| q69 | OK | 65.0 | 61.8 | 81.0 | 0.76x | 71 / 71 | rows and values match |
| q70 | OK | 49.2 | 56.5 | 28.9 | 1.95x | 3 / 3 | rows and values match |
| q71 | OK | 57.4 | 53.2 | 59.7 | 0.89x | 56 / 56 | rows and values match |
| q72 | OK | 172.8 | 163.1 | 71.4 | 2.28x | 50 / 50 | rows and values match |
| q73 | OK | 32.1 | 29.7 | 33.2 | 0.89x | 0 / 0 | rows and values match |
| q74 | OK | 61.2 | 59.5 | 40.4 | 1.47x | 3 / 3 | rows and values match |
| q75 | OK | 129.4 | 109.3 | 37.1 | 2.95x | 25 / 25 | rows and values match |
| q76 | OK | 57.7 | 58.8 | 35.3 | 1.67x | 100 / 100 | rows and values match |
| q77 | OK | 55.4 | 49.8 | 35.3 | 1.41x | 10 / 10 | rows and values match |
| q78 | OK | 115.4 | 119.2 | 39.8 | 2.99x | 100 / 100 | rows and values match |
| q79 | OK | 39.6 | 33.3 | 27.9 | 1.20x | 100 / 100 | rows and values match |
| q80 | OK | 110.6 | 112.9 | 46.0 | 2.46x | 100 / 100 | rows and values match |
| q81 | OK | 44.9 | 38.5 | 29.2 | 1.32x | 34 / 34 | rows and values match |
| q82 | OK | 12.2 | 12.1 | 15.6 | 0.77x | 0 / 0 | rows and values match |
| q83 | OK | 65.8 | 72.1 | 62.9 | 1.15x | 0 / 0 | rows and values match |
| q84 | OK | 39.2 | 34.7 | 29.1 | 1.19x | 6 / 6 | rows and values match |
| q85 | OK | 129.0 | 123.5 | 78.5 | 1.57x | 0 / 0 | rows and values match |
| q86 | OK | 24.8 | 26.5 | 22.1 | 1.20x | 100 / 100 | rows and values match |
| q87 | OK | 45.0 | 50.4 | 42.4 | 1.19x | 1 / 1 | rows and values match |
| q88 | OK | 131.4 | 102.2 | 209.8 | 0.49x | 1 / 1 | rows and values match |
| q89 | OK | 34.4 | 35.2 | 33.7 | 1.04x | 100 / 100 | rows and values match |
| q90 | OK | 34.0 | 29.8 | 38.7 | 0.77x | 1 / 1 | rows and values match |
| q91 | OK | 69.2 | 69.9 | 57.4 | 1.22x | 0 / 0 | rows and values match |
| q92 | OK | 24.2 | 24.1 | 20.0 | 1.20x | 1 / 1 | rows and values match |
| q93 | OK | 29.2 | 26.3 | 4.1 | 6.35x | 0 / 0 | rows and values match |
| q94 | OK | 38.5 | 31.3 | 26.9 | 1.16x | 1 / 1 | rows and values match |
| q95 | OK | 30.3 | 32.4 | 30.0 | 1.08x | 1 / 1 | rows and values match |
| q96 | OK | 17.9 | 21.2 | 24.6 | 0.86x | 1 / 1 | rows and values match |
| q97 | OK | 35.2 | 34.1 | 28.3 | 1.21x | 1 / 1 | rows and values match |
| q98 | OK | 25.0 | 25.3 | 21.0 | 1.21x | 250 / 250 | rows and values match |
| q99 | OK | 45.7 | 46.6 | 20.5 | 2.27x | 6 / 6 | rows and values match |
