# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 17:43

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 6.5s)
Timing: ours 5.7s  duckdb 4.1s  ->  total 1.42x  geomean 1.35x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.7 | 4.1 | 1.42x | 1.35x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 192.3 | - | 22.1 | 8.70x | 100 / 100 | rows and values match |
| q02 | OK | 107.7 | - | 42.0 | 2.56x | 2513 / 2513 | rows and values match |
| q03 | OK | 28.0 | - | 19.1 | 1.47x | 15 / 15 | rows and values match |
| q04 | OK | 150.7 | - | 68.5 | 2.20x | 0 / 0 | rows and values match |
| q05 | OK | 65.8 | - | 34.2 | 1.92x | 100 / 100 | rows and values match |
| q06 | OK | 78.1 | - | 44.1 | 1.77x | 10 / 10 | rows and values match |
| q07 | OK | 44.8 | - | 72.6 | 0.62x | 100 / 100 | rows and values match |
| q08 | OK | 33.3 | - | 35.2 | 0.94x | 0 / 0 | rows and values match |
| q09 | OK | 27.6 | - | 8.3 | 3.32x | 1 / 1 | rows and values match |
| q10 | OK | 58.9 | - | 99.6 | 0.59x | 1 / 1 | rows and values match |
| q11 | OK | 80.4 | - | 52.3 | 1.54x | 4 / 4 | rows and values match |
| q12 | OK | 28.2 | - | 18.3 | 1.54x | 100 / 100 | rows and values match |
| q13 | OK | 89.2 | - | 54.8 | 1.63x | 1 / 1 | rows and values match |
| q14 | OK | 174.7 | - | 94.7 | 1.85x | 100 / 100 | rows and values match |
| q15 | OK | 22.4 | - | 25.5 | 0.88x | 33 / 33 | rows and values match |
| q16 | OK | 35.0 | - | 27.8 | 1.26x | 1 / 1 | rows and values match |
| q17 | OK | 51.3 | - | 38.5 | 1.33x | 1 / 1 | rows and values match |
| q18 | OK | 72.0 | - | 77.1 | 0.93x | 100 / 100 | rows and values match |
| q19 | OK | 31.2 | - | 35.4 | 0.88x | 10 / 10 | rows and values match |
| q20 | OK | 25.0 | - | 21.1 | 1.18x | 100 / 100 | rows and values match |
| q21 | OK | 21.9 | - | 17.5 | 1.26x | 32 / 32 | rows and values match |
| q22 | OK | 31.7 | - | 25.5 | 1.24x | 100 / 100 | rows and values match |
| q23 | OK | 154.0 | - | 72.1 | 2.14x | 1 / 1 | rows and values match |
| q24 | OK | 55.5 | - | 15.8 | 3.51x | 0 / 0 | rows and values match |
| q25 | OK | 50.9 | - | 36.3 | 1.40x | 0 / 0 | rows and values match |
| q26 | OK | 31.5 | - | 66.8 | 0.47x | 100 / 100 | rows and values match |
| q27 | OK | 44.8 | - | 57.4 | 0.78x | 100 / 100 | rows and values match |
| q28 | OK | 28.2 | - | 10.7 | 2.63x | 1 / 1 | rows and values match |
| q29 | OK | 49.8 | - | 50.2 | 0.99x | 0 / 0 | rows and values match |
| q30 | OK | 40.3 | - | 32.2 | 1.25x | 21 / 21 | rows and values match |
| q31 | OK | 76.3 | - | 39.5 | 1.93x | 1 / 1 | rows and values match |
| q32 | OK | 19.5 | - | 22.2 | 0.88x | 1 / 1 | rows and values match |
| q33 | OK | 95.9 | - | 40.3 | 2.38x | 100 / 100 | rows and values match |
| q34 | OK | 37.8 | - | 26.8 | 1.41x | 53 / 53 | rows and values match |
| q35 | OK | 69.4 | - | 96.2 | 0.72x | 100 / 100 | rows and values match |
| q36 | OK | 38.5 | - | 27.3 | 1.41x | 100 / 100 | rows and values match |
| q37 | OK | 19.0 | - | 16.4 | 1.16x | 0 / 0 | rows and values match |
| q38 | OK | 44.4 | - | 42.0 | 1.06x | 1 / 1 | rows and values match |
| q39 | OK | 28.5 | - | 20.8 | 1.37x | 5 / 5 | rows and values match |
| q40 | OK | 26.7 | - | 17.1 | 1.56x | 44 / 44 | rows and values match |
| q41 | OK | 5.4 | - | 7.1 | 0.76x | 0 / 0 | rows and values match |
| q42 | OK | 16.1 | - | 17.9 | 0.90x | 4 / 4 | rows and values match |
| q43 | OK | 19.8 | - | 25.1 | 0.79x | 1 / 1 | rows and values match |
| q44 | OK | 31.5 | - | 6.5 | 4.86x | 0 / 0 | rows and values match |
| q45 | OK | 41.2 | - | 34.0 | 1.21x | 24 / 24 | rows and values match |
| q46 | OK | 53.9 | - | 39.6 | 1.36x | 100 / 100 | rows and values match |
| q47 | OK | 57.2 | - | 39.8 | 1.44x | 100 / 100 | rows and values match |
| q48 | OK | 82.8 | - | 59.7 | 1.39x | 1 / 1 | rows and values match |
| q49 | OK | 61.3 | - | 33.6 | 1.82x | 2 / 2 | rows and values match |
| q50 | OK | 212.2 | - | 34.4 | 6.17x | 1 / 1 | rows and values match |
| q51 | OK | 51.3 | - | 51.6 | 0.99x | 100 / 100 | rows and values match |
| q52 | OK | 15.7 | - | 17.1 | 0.92x | 11 / 11 | rows and values match |
| q53 | OK | 29.2 | - | 23.8 | 1.23x | 100 / 100 | rows and values match |
| q54 | OK | 61.1 | - | 54.3 | 1.13x | 0 / 0 | rows and values match |
| q55 | OK | 18.0 | - | 18.2 | 0.99x | 20 / 20 | rows and values match |
| q56 | OK | 75.5 | - | 40.8 | 1.85x | 38 / 38 | rows and values match |
| q57 | OK | 52.0 | - | 48.1 | 1.08x | 100 / 100 | rows and values match |
| q58 | OK | 113.5 | - | 63.0 | 1.80x | 0 / 0 | rows and values match |
| q59 | OK | 86.0 | - | 52.2 | 1.65x | 100 / 100 | rows and values match |
| q60 | OK | 84.7 | - | 41.8 | 2.02x | 100 / 100 | rows and values match |
| q61 | OK | 51.6 | - | 37.9 | 1.36x | 1 / 1 | rows and values match |
| q62 | OK | 43.9 | - | 28.8 | 1.53x | 6 / 6 | rows and values match |
| q63 | OK | 28.6 | - | 24.4 | 1.17x | 100 / 100 | rows and values match |
| q64 | OK | 179.9 | - | 93.0 | 1.94x | 0 / 0 | rows and values match |
| q65 | OK | 40.6 | - | 27.9 | 1.46x | 0 / 0 | rows and values match |
| q66 | OK | 140.0 | - | 70.3 | 1.99x | 1 / 1 | rows and values match |
| q67 | OK | 58.0 | - | 77.1 | 0.75x | 100 / 100 | rows and values match |
| q68 | OK | 49.5 | - | 36.5 | 1.36x | 100 / 100 | rows and values match |
| q69 | OK | 59.9 | - | 81.0 | 0.74x | 71 / 71 | rows and values match |
| q70 | OK | 45.1 | - | 28.9 | 1.56x | 3 / 3 | rows and values match |
| q71 | OK | 45.1 | - | 59.7 | 0.76x | 56 / 56 | rows and values match |
| q72 | OK | 144.8 | - | 71.4 | 2.03x | 50 / 50 | rows and values match |
| q73 | OK | 26.5 | - | 33.2 | 0.80x | 0 / 0 | rows and values match |
| q74 | OK | 59.5 | - | 40.4 | 1.47x | 3 / 3 | rows and values match |
| q75 | OK | 86.0 | - | 37.1 | 2.32x | 25 / 25 | rows and values match |
| q76 | OK | 60.8 | - | 35.3 | 1.72x | 100 / 100 | rows and values match |
| q77 | OK | 55.4 | - | 35.3 | 1.57x | 10 / 10 | rows and values match |
| q78 | OK | 78.5 | - | 39.8 | 1.97x | 100 / 100 | rows and values match |
| q79 | OK | 37.7 | - | 27.9 | 1.36x | 100 / 100 | rows and values match |
| q80 | OK | 74.5 | - | 46.0 | 1.62x | 100 / 100 | rows and values match |
| q81 | OK | 50.2 | - | 29.2 | 1.72x | 34 / 34 | rows and values match |
| q82 | OK | 13.7 | - | 15.6 | 0.88x | 0 / 0 | rows and values match |
| q83 | OK | 101.4 | - | 62.9 | 1.61x | 0 / 0 | rows and values match |
| q84 | OK | 32.7 | - | 29.1 | 1.12x | 6 / 6 | rows and values match |
| q85 | OK | 143.5 | - | 78.5 | 1.83x | 0 / 0 | rows and values match |
| q86 | OK | 30.1 | - | 22.1 | 1.36x | 100 / 100 | rows and values match |
| q87 | OK | 41.8 | - | 42.4 | 0.98x | 1 / 1 | rows and values match |
| q88 | OK | 128.8 | - | 209.8 | 0.61x | 1 / 1 | rows and values match |
| q89 | OK | 33.2 | - | 33.7 | 0.98x | 100 / 100 | rows and values match |
| q90 | OK | 34.2 | - | 38.7 | 0.88x | 1 / 1 | rows and values match |
| q91 | OK | 43.1 | - | 57.4 | 0.75x | 0 / 0 | rows and values match |
| q92 | OK | 22.1 | - | 20.0 | 1.11x | 1 / 1 | rows and values match |
| q93 | OK | 11.2 | - | 4.1 | 2.70x | 0 / 0 | rows and values match |
| q94 | OK | 34.2 | - | 26.9 | 1.27x | 1 / 1 | rows and values match |
| q95 | OK | 31.4 | - | 30.0 | 1.05x | 1 / 1 | rows and values match |
| q96 | OK | 15.8 | - | 24.6 | 0.64x | 1 / 1 | rows and values match |
| q97 | OK | 28.3 | - | 28.3 | 1.00x | 1 / 1 | rows and values match |
| q98 | OK | 27.5 | - | 21.0 | 1.31x | 250 / 250 | rows and values match |
| q99 | OK | 25.5 | - | 20.5 | 1.24x | 6 / 6 | rows and values match |
