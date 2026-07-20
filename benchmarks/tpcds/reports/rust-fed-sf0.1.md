# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-20 02:32

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 10.9s)
Timing: ours 4.8s  duckdb 4.1s  ->  total 1.18x  geomean 1.16x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 4.8 | 4.1 | 1.18x | 1.16x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 194.9 | 31.1 | 22.1 | 1.40x | 100 / 100 | rows and values match |
| q02 | OK | 99.9 | 65.5 | 42.0 | 1.56x | 2513 / 2513 | rows and values match |
| q03 | OK | 30.1 | 16.6 | 19.1 | 0.87x | 15 / 15 | rows and values match |
| q04 | OK | 148.6 | 124.7 | 68.5 | 1.82x | 0 / 0 | rows and values match |
| q05 | OK | 64.8 | 43.4 | 34.2 | 1.27x | 100 / 100 | rows and values match |
| q06 | OK | 73.0 | 40.0 | 44.1 | 0.91x | 10 / 10 | rows and values match |
| q07 | OK | 54.4 | 29.7 | 72.6 | 0.41x | 100 / 100 | rows and values match |
| q08 | OK | 32.9 | 26.1 | 35.2 | 0.74x | 0 / 0 | rows and values match |
| q09 | OK | 26.6 | 23.6 | 8.3 | 2.84x | 1 / 1 | rows and values match |
| q10 | OK | 56.8 | 51.6 | 99.6 | 0.52x | 1 / 1 | rows and values match |
| q11 | OK | 76.5 | 74.2 | 52.3 | 1.42x | 4 / 4 | rows and values match |
| q12 | OK | 30.2 | 25.4 | 18.3 | 1.38x | 100 / 100 | rows and values match |
| q13 | OK | 81.3 | 65.1 | 54.8 | 1.19x | 1 / 1 | rows and values match |
| q14 | OK | 156.6 | 144.1 | 94.7 | 1.52x | 100 / 100 | rows and values match |
| q15 | OK | 22.9 | 24.9 | 25.5 | 0.98x | 33 / 33 | rows and values match |
| q16 | OK | 36.6 | 27.1 | 27.8 | 0.98x | 1 / 1 | rows and values match |
| q17 | OK | 51.3 | 45.6 | 38.5 | 1.19x | 1 / 1 | rows and values match |
| q18 | OK | 63.2 | 57.7 | 77.1 | 0.75x | 100 / 100 | rows and values match |
| q19 | OK | 34.5 | 29.6 | 35.4 | 0.84x | 10 / 10 | rows and values match |
| q20 | OK | 25.6 | 25.2 | 21.1 | 1.19x | 100 / 100 | rows and values match |
| q21 | OK | 24.2 | 14.9 | 17.5 | 0.85x | 32 / 32 | rows and values match |
| q22 | OK | 31.9 | 28.6 | 25.5 | 1.12x | 100 / 100 | rows and values match |
| q23 | OK | 149.3 | 145.7 | 72.1 | 2.02x | 1 / 1 | rows and values match |
| q24 | OK | 49.4 | 35.6 | 15.8 | 2.25x | 0 / 0 | rows and values match |
| q25 | OK | 45.0 | 40.6 | 36.3 | 1.12x | 0 / 0 | rows and values match |
| q26 | OK | 33.7 | 27.8 | 66.8 | 0.42x | 100 / 100 | rows and values match |
| q27 | OK | 43.9 | 42.8 | 57.4 | 0.74x | 100 / 100 | rows and values match |
| q28 | OK | 19.7 | 16.3 | 10.7 | 1.51x | 1 / 1 | rows and values match |
| q29 | OK | 43.6 | 41.9 | 50.2 | 0.84x | 0 / 0 | rows and values match |
| q30 | OK | 37.9 | 34.8 | 32.2 | 1.08x | 21 / 21 | rows and values match |
| q31 | OK | 74.6 | 70.5 | 39.5 | 1.79x | 1 / 1 | rows and values match |
| q32 | OK | 21.0 | 21.3 | 22.2 | 0.96x | 1 / 1 | rows and values match |
| q33 | OK | 84.4 | 86.0 | 40.3 | 2.14x | 100 / 100 | rows and values match |
| q34 | OK | 37.1 | 30.3 | 26.8 | 1.13x | 53 / 53 | rows and values match |
| q35 | OK | 60.4 | 58.8 | 96.2 | 0.61x | 100 / 100 | rows and values match |
| q36 | OK | 36.9 | 42.1 | 27.3 | 1.54x | 100 / 100 | rows and values match |
| q37 | OK | 19.5 | 15.2 | 16.4 | 0.92x | 0 / 0 | rows and values match |
| q38 | OK | 40.0 | 39.3 | 42.0 | 0.94x | 1 / 1 | rows and values match |
| q39 | OK | 26.8 | 24.2 | 20.8 | 1.17x | 5 / 5 | rows and values match |
| q40 | OK | 26.9 | 24.6 | 17.1 | 1.44x | 44 / 44 | rows and values match |
| q41 | OK | 6.4 | 4.2 | 7.1 | 0.59x | 0 / 0 | rows and values match |
| q42 | OK | 17.1 | 15.3 | 17.9 | 0.85x | 4 / 4 | rows and values match |
| q43 | OK | 20.9 | 17.6 | 25.1 | 0.70x | 1 / 1 | rows and values match |
| q44 | OK | 30.3 | 28.1 | 6.5 | 4.34x | 0 / 0 | rows and values match |
| q45 | OK | 39.6 | 35.9 | 34.0 | 1.06x | 24 / 24 | rows and values match |
| q46 | OK | 47.9 | 52.1 | 39.6 | 1.32x | 100 / 100 | rows and values match |
| q47 | OK | 54.4 | 53.3 | 39.8 | 1.34x | 100 / 100 | rows and values match |
| q48 | OK | 71.5 | 72.8 | 59.7 | 1.22x | 1 / 1 | rows and values match |
| q49 | OK | 51.9 | 55.6 | 33.6 | 1.65x | 2 / 2 | rows and values match |
| q50 | OK | 197.5 | 163.8 | 34.4 | 4.77x | 1 / 1 | rows and values match |
| q51 | OK | 46.5 | 46.8 | 51.6 | 0.91x | 100 / 100 | rows and values match |
| q52 | OK | 16.6 | 15.8 | 17.1 | 0.93x | 11 / 11 | rows and values match |
| q53 | OK | 32.0 | 34.8 | 23.8 | 1.46x | 100 / 100 | rows and values match |
| q54 | OK | 56.7 | 60.6 | 54.3 | 1.12x | 0 / 0 | rows and values match |
| q55 | OK | 16.0 | 16.4 | 18.2 | 0.90x | 20 / 20 | rows and values match |
| q56 | OK | 73.7 | 73.4 | 40.8 | 1.80x | 38 / 38 | rows and values match |
| q57 | OK | 47.0 | 43.8 | 48.1 | 0.91x | 100 / 100 | rows and values match |
| q58 | OK | 106.3 | 99.4 | 63.0 | 1.58x | 0 / 0 | rows and values match |
| q59 | OK | 74.5 | 78.2 | 52.2 | 1.50x | 100 / 100 | rows and values match |
| q60 | OK | 80.8 | 76.9 | 41.8 | 1.84x | 100 / 100 | rows and values match |
| q61 | OK | 50.9 | 45.8 | 37.9 | 1.21x | 1 / 1 | rows and values match |
| q62 | OK | 37.2 | 30.9 | 28.8 | 1.07x | 6 / 6 | rows and values match |
| q63 | OK | 28.1 | 30.1 | 24.4 | 1.24x | 100 / 100 | rows and values match |
| q64 | OK | 166.2 | 156.9 | 93.0 | 1.69x | 0 / 0 | rows and values match |
| q65 | OK | 36.8 | 32.8 | 27.9 | 1.17x | 0 / 0 | rows and values match |
| q66 | OK | 129.6 | 99.3 | 70.3 | 1.41x | 1 / 1 | rows and values match |
| q67 | OK | 53.2 | 54.0 | 77.1 | 0.70x | 100 / 100 | rows and values match |
| q68 | OK | 43.4 | 43.1 | 36.5 | 1.18x | 100 / 100 | rows and values match |
| q69 | OK | 54.2 | 53.1 | 81.0 | 0.66x | 71 / 71 | rows and values match |
| q70 | OK | 39.6 | 40.7 | 28.9 | 1.41x | 3 / 3 | rows and values match |
| q71 | OK | 41.7 | 38.1 | 59.7 | 0.64x | 56 / 56 | rows and values match |
| q72 | OK | 128.5 | 130.8 | 71.4 | 1.83x | 50 / 50 | rows and values match |
| q73 | OK | 26.2 | 25.7 | 33.2 | 0.77x | 0 / 0 | rows and values match |
| q74 | OK | 60.6 | 56.8 | 40.4 | 1.41x | 3 / 3 | rows and values match |
| q75 | OK | 81.6 | 79.5 | 37.1 | 2.14x | 25 / 25 | rows and values match |
| q76 | OK | 57.1 | 56.8 | 35.3 | 1.61x | 100 / 100 | rows and values match |
| q77 | OK | 55.3 | 48.7 | 35.3 | 1.38x | 10 / 10 | rows and values match |
| q78 | OK | 70.9 | 66.1 | 39.8 | 1.66x | 100 / 100 | rows and values match |
| q79 | OK | 34.9 | 31.9 | 27.9 | 1.15x | 100 / 100 | rows and values match |
| q80 | OK | 67.3 | 64.4 | 46.0 | 1.40x | 100 / 100 | rows and values match |
| q81 | OK | 76.1 | 62.2 | 29.2 | 2.13x | 34 / 34 | rows and values match |
| q82 | OK | 17.4 | 21.1 | 15.6 | 1.35x | 0 / 0 | rows and values match |
| q83 | OK | 98.1 | 98.9 | 62.9 | 1.57x | 0 / 0 | rows and values match |
| q84 | OK | 29.4 | 24.8 | 29.1 | 0.85x | 6 / 6 | rows and values match |
| q85 | OK | 128.4 | 124.9 | 78.5 | 1.59x | 0 / 0 | rows and values match |
| q86 | OK | 24.2 | 25.8 | 22.1 | 1.16x | 100 / 100 | rows and values match |
| q87 | OK | 39.3 | 38.4 | 42.4 | 0.91x | 1 / 1 | rows and values match |
| q88 | OK | 90.8 | 78.6 | 209.8 | 0.37x | 1 / 1 | rows and values match |
| q89 | OK | 32.1 | 32.2 | 33.7 | 0.95x | 100 / 100 | rows and values match |
| q90 | OK | 30.9 | 27.6 | 38.7 | 0.71x | 1 / 1 | rows and values match |
| q91 | OK | 42.2 | 38.8 | 57.4 | 0.68x | 0 / 0 | rows and values match |
| q92 | OK | 20.7 | 22.8 | 20.0 | 1.14x | 1 / 1 | rows and values match |
| q93 | OK | 10.6 | 9.1 | 4.1 | 2.20x | 0 / 0 | rows and values match |
| q94 | OK | 30.5 | 28.5 | 26.9 | 1.06x | 1 / 1 | rows and values match |
| q95 | OK | 26.6 | 26.7 | 30.0 | 0.89x | 1 / 1 | rows and values match |
| q96 | OK | 13.8 | 14.9 | 24.6 | 0.61x | 1 / 1 | rows and values match |
| q97 | OK | 24.3 | 25.1 | 28.3 | 0.89x | 1 / 1 | rows and values match |
| q98 | OK | 24.4 | 25.6 | 21.0 | 1.22x | 250 / 250 | rows and values match |
| q99 | OK | 24.8 | 23.0 | 20.5 | 1.12x | 6 / 6 | rows and values match |
