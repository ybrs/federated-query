# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-20 11:27

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 6.4s)
Timing: ours 5.7s  duckdb 4.1s  ->  total 1.41x  geomean 1.35x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.7 | 4.1 | 1.41x | 1.35x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 202.4 | - | 22.1 | 9.15x | 100 / 100 | rows and values match |
| q02 | OK | 98.8 | - | 42.0 | 2.35x | 2513 / 2513 | rows and values match |
| q03 | OK | 31.7 | - | 19.1 | 1.66x | 15 / 15 | rows and values match |
| q04 | OK | 150.5 | - | 68.5 | 2.20x | 0 / 0 | rows and values match |
| q05 | OK | 64.8 | - | 34.2 | 1.89x | 100 / 100 | rows and values match |
| q06 | OK | 74.0 | - | 44.1 | 1.68x | 10 / 10 | rows and values match |
| q07 | OK | 48.6 | - | 72.6 | 0.67x | 100 / 100 | rows and values match |
| q08 | OK | 35.7 | - | 35.2 | 1.01x | 0 / 0 | rows and values match |
| q09 | OK | 27.8 | - | 8.3 | 3.36x | 1 / 1 | rows and values match |
| q10 | OK | 57.8 | - | 99.6 | 0.58x | 1 / 1 | rows and values match |
| q11 | OK | 77.7 | - | 52.3 | 1.49x | 4 / 4 | rows and values match |
| q12 | OK | 27.9 | - | 18.3 | 1.53x | 100 / 100 | rows and values match |
| q13 | OK | 73.9 | - | 54.8 | 1.35x | 1 / 1 | rows and values match |
| q14 | OK | 160.2 | - | 94.7 | 1.69x | 100 / 100 | rows and values match |
| q15 | OK | 21.5 | - | 25.5 | 0.84x | 33 / 33 | rows and values match |
| q16 | OK | 35.5 | - | 27.8 | 1.28x | 1 / 1 | rows and values match |
| q17 | OK | 51.5 | - | 38.5 | 1.34x | 1 / 1 | rows and values match |
| q18 | OK | 57.8 | - | 77.1 | 0.75x | 100 / 100 | rows and values match |
| q19 | OK | 32.3 | - | 35.4 | 0.91x | 10 / 10 | rows and values match |
| q20 | OK | 24.2 | - | 21.1 | 1.14x | 100 / 100 | rows and values match |
| q21 | OK | 22.9 | - | 17.5 | 1.31x | 32 / 32 | rows and values match |
| q22 | OK | 27.7 | - | 25.5 | 1.09x | 100 / 100 | rows and values match |
| q23 | OK | 143.0 | - | 72.1 | 1.98x | 1 / 1 | rows and values match |
| q24 | OK | 50.1 | - | 15.8 | 3.18x | 0 / 0 | rows and values match |
| q25 | OK | 47.4 | - | 36.3 | 1.31x | 0 / 0 | rows and values match |
| q26 | OK | 29.7 | - | 66.8 | 0.45x | 100 / 100 | rows and values match |
| q27 | OK | 47.5 | - | 57.4 | 0.83x | 100 / 100 | rows and values match |
| q28 | OK | 18.3 | - | 10.7 | 1.70x | 1 / 1 | rows and values match |
| q29 | OK | 45.1 | - | 50.2 | 0.90x | 0 / 0 | rows and values match |
| q30 | OK | 38.7 | - | 32.2 | 1.20x | 21 / 21 | rows and values match |
| q31 | OK | 75.0 | - | 39.5 | 1.90x | 1 / 1 | rows and values match |
| q32 | OK | 19.0 | - | 22.2 | 0.86x | 1 / 1 | rows and values match |
| q33 | OK | 83.5 | - | 40.3 | 2.07x | 100 / 100 | rows and values match |
| q34 | OK | 37.6 | - | 26.8 | 1.40x | 53 / 53 | rows and values match |
| q35 | OK | 61.6 | - | 96.2 | 0.64x | 100 / 100 | rows and values match |
| q36 | OK | 32.5 | - | 27.3 | 1.19x | 100 / 100 | rows and values match |
| q37 | OK | 20.2 | - | 16.4 | 1.23x | 0 / 0 | rows and values match |
| q38 | OK | 44.1 | - | 42.0 | 1.05x | 1 / 1 | rows and values match |
| q39 | OK | 25.1 | - | 20.8 | 1.21x | 5 / 5 | rows and values match |
| q40 | OK | 26.5 | - | 17.1 | 1.55x | 44 / 44 | rows and values match |
| q41 | OK | 5.3 | - | 7.1 | 0.74x | 0 / 0 | rows and values match |
| q42 | OK | 17.3 | - | 17.9 | 0.96x | 4 / 4 | rows and values match |
| q43 | OK | 20.0 | - | 25.1 | 0.80x | 1 / 1 | rows and values match |
| q44 | OK | 29.4 | - | 6.5 | 4.54x | 0 / 0 | rows and values match |
| q45 | OK | 39.3 | - | 34.0 | 1.15x | 24 / 24 | rows and values match |
| q46 | OK | 53.3 | - | 39.6 | 1.34x | 100 / 100 | rows and values match |
| q47 | OK | 53.6 | - | 39.8 | 1.35x | 100 / 100 | rows and values match |
| q48 | OK | 71.6 | - | 59.7 | 1.20x | 1 / 1 | rows and values match |
| q49 | OK | 54.7 | - | 33.6 | 1.63x | 2 / 2 | rows and values match |
| q50 | OK | 205.2 | - | 34.4 | 5.97x | 1 / 1 | rows and values match |
| q51 | OK | 44.3 | - | 51.6 | 0.86x | 100 / 100 | rows and values match |
| q52 | OK | 17.6 | - | 17.1 | 1.03x | 11 / 11 | rows and values match |
| q53 | OK | 29.1 | - | 23.8 | 1.22x | 100 / 100 | rows and values match |
| q54 | OK | 62.0 | - | 54.3 | 1.14x | 0 / 0 | rows and values match |
| q55 | OK | 16.8 | - | 18.2 | 0.92x | 20 / 20 | rows and values match |
| q56 | OK | 72.3 | - | 40.8 | 1.77x | 38 / 38 | rows and values match |
| q57 | OK | 47.8 | - | 48.1 | 0.99x | 100 / 100 | rows and values match |
| q58 | OK | 113.3 | - | 63.0 | 1.80x | 0 / 0 | rows and values match |
| q59 | OK | 81.2 | - | 52.2 | 1.56x | 100 / 100 | rows and values match |
| q60 | OK | 80.2 | - | 41.8 | 1.92x | 100 / 100 | rows and values match |
| q61 | OK | 52.2 | - | 37.9 | 1.38x | 1 / 1 | rows and values match |
| q62 | OK | 38.7 | - | 28.8 | 1.34x | 6 / 6 | rows and values match |
| q63 | OK | 31.9 | - | 24.4 | 1.31x | 100 / 100 | rows and values match |
| q64 | OK | 165.0 | - | 93.0 | 1.78x | 0 / 0 | rows and values match |
| q65 | OK | 35.9 | - | 27.9 | 1.29x | 0 / 0 | rows and values match |
| q66 | OK | 133.6 | - | 70.3 | 1.90x | 1 / 1 | rows and values match |
| q67 | OK | 100.9 | - | 77.1 | 1.31x | 100 / 100 | rows and values match |
| q68 | OK | 48.1 | - | 36.5 | 1.32x | 100 / 100 | rows and values match |
| q69 | OK | 62.9 | - | 81.0 | 0.78x | 71 / 71 | rows and values match |
| q70 | OK | 49.2 | - | 28.9 | 1.70x | 3 / 3 | rows and values match |
| q71 | OK | 49.8 | - | 59.7 | 0.84x | 56 / 56 | rows and values match |
| q72 | OK | 179.9 | - | 71.4 | 2.52x | 50 / 50 | rows and values match |
| q73 | OK | 26.8 | - | 33.2 | 0.80x | 0 / 0 | rows and values match |
| q74 | OK | 62.9 | - | 40.4 | 1.56x | 3 / 3 | rows and values match |
| q75 | OK | 93.4 | - | 37.1 | 2.51x | 25 / 25 | rows and values match |
| q76 | OK | 65.9 | - | 35.3 | 1.87x | 100 / 100 | rows and values match |
| q77 | OK | 58.9 | - | 35.3 | 1.67x | 10 / 10 | rows and values match |
| q78 | OK | 91.1 | - | 39.8 | 2.29x | 100 / 100 | rows and values match |
| q79 | OK | 47.7 | - | 27.9 | 1.71x | 100 / 100 | rows and values match |
| q80 | OK | 75.6 | - | 46.0 | 1.64x | 100 / 100 | rows and values match |
| q81 | OK | 63.2 | - | 29.2 | 2.16x | 34 / 34 | rows and values match |
| q82 | OK | 14.7 | - | 15.6 | 0.94x | 0 / 0 | rows and values match |
| q83 | OK | 106.5 | - | 62.9 | 1.69x | 0 / 0 | rows and values match |
| q84 | OK | 38.3 | - | 29.1 | 1.32x | 6 / 6 | rows and values match |
| q85 | OK | 146.6 | - | 78.5 | 1.87x | 0 / 0 | rows and values match |
| q86 | OK | 26.3 | - | 22.1 | 1.19x | 100 / 100 | rows and values match |
| q87 | OK | 44.9 | - | 42.4 | 1.06x | 1 / 1 | rows and values match |
| q88 | OK | 115.9 | - | 209.8 | 0.55x | 1 / 1 | rows and values match |
| q89 | OK | 38.5 | - | 33.7 | 1.14x | 100 / 100 | rows and values match |
| q90 | OK | 36.9 | - | 38.7 | 0.95x | 1 / 1 | rows and values match |
| q91 | OK | 51.5 | - | 57.4 | 0.90x | 0 / 0 | rows and values match |
| q92 | OK | 26.6 | - | 20.0 | 1.33x | 1 / 1 | rows and values match |
| q93 | OK | 12.5 | - | 4.1 | 3.02x | 0 / 0 | rows and values match |
| q94 | OK | 34.8 | - | 26.9 | 1.29x | 1 / 1 | rows and values match |
| q95 | OK | 28.0 | - | 30.0 | 0.93x | 1 / 1 | rows and values match |
| q96 | OK | 17.4 | - | 24.6 | 0.71x | 1 / 1 | rows and values match |
| q97 | OK | 24.3 | - | 28.3 | 0.86x | 1 / 1 | rows and values match |
| q98 | OK | 26.9 | - | 21.0 | 1.28x | 250 / 250 | rows and values match |
| q99 | OK | 22.4 | - | 20.5 | 1.09x | 6 / 6 | rows and values match |
