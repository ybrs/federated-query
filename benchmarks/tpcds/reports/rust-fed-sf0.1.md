# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:56

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 17.4s)
Timing: ours 5.5s  duckdb 4.0s  ->  total 1.38x  geomean 1.31x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.5 | 4.0 | 1.38x | 1.31x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 188.6 | 36.4 | 22.1 | 1.65x | 100 / 100 | rows and values match |
| q02 | OK | 198.7 | 99.7 | 42.0 | 2.37x | 2513 / 2513 | rows and values match |
| q03 | OK | 27.1 | 17.8 | 19.1 | 0.94x | 15 / 15 | rows and values match |
| q04 | OK | 249.2 | 226.7 | 68.5 | 3.31x | 0 / 0 | rows and values match |
| q05 | OK | 57.6 | 46.9 | 34.2 | 1.37x | 100 / 100 | rows and values match |
| q06 | OK | 42.9 | 38.7 | 44.1 | 0.88x | 10 / 10 | rows and values match |
| q07 | OK | 42.1 | 33.8 | 72.6 | 0.47x | 100 / 100 | rows and values match |
| q08 | OK | 34.3 | 27.8 | 35.2 | 0.79x | 0 / 0 | rows and values match |
| q09 | OK | 27.6 | 27.5 | 8.3 | 3.32x | 1 / 1 | rows and values match |
| q10 | OK | 59.5 | 55.2 | 99.6 | 0.55x | 1 / 1 | rows and values match |
| q11 | OK | 115.0 | 124.3 | 52.3 | 2.38x | 4 / 4 | rows and values match |
| q12 | OK | 29.1 | 27.2 | 18.3 | 1.48x | 100 / 100 | rows and values match |
| q13 | OK | 88.8 | 70.9 | 54.8 | 1.29x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 22.2 | 22.8 | 25.5 | 0.89x | 33 / 33 | rows and values match |
| q16 | OK | 36.6 | 30.9 | 27.8 | 1.11x | 1 / 1 | rows and values match |
| q17 | OK | 54.8 | 50.7 | 38.5 | 1.32x | 1 / 1 | rows and values match |
| q18 | OK | 66.3 | 69.3 | 77.1 | 0.90x | 100 / 100 | rows and values match |
| q19 | OK | 34.0 | 33.0 | 35.4 | 0.93x | 10 / 10 | rows and values match |
| q20 | OK | 26.1 | 25.0 | 21.1 | 1.18x | 100 / 100 | rows and values match |
| q21 | OK | 21.5 | 15.2 | 17.5 | 0.87x | 32 / 32 | rows and values match |
| q22 | OK | 31.3 | 27.7 | 25.5 | 1.08x | 100 / 100 | rows and values match |
| q23 | OK | 208.2 | 195.0 | 72.1 | 2.71x | 1 / 1 | rows and values match |
| q24 | OK | 63.5 | 47.9 | 15.8 | 3.03x | 0 / 0 | rows and values match |
| q25 | OK | 50.0 | 47.9 | 36.3 | 1.32x | 0 / 0 | rows and values match |
| q26 | OK | 30.7 | 31.9 | 66.8 | 0.48x | 100 / 100 | rows and values match |
| q27 | OK | 58.6 | 58.6 | 57.4 | 1.02x | 100 / 100 | rows and values match |
| q28 | OK | 30.0 | 20.6 | 10.7 | 1.92x | 1 / 1 | rows and values match |
| q29 | OK | 42.8 | 46.8 | 50.2 | 0.93x | 0 / 0 | rows and values match |
| q30 | OK | 45.0 | 41.1 | 32.2 | 1.28x | 21 / 21 | rows and values match |
| q31 | OK | 87.6 | 84.6 | 39.5 | 2.14x | 1 / 1 | rows and values match |
| q32 | OK | 22.1 | 21.1 | 22.2 | 0.95x | 1 / 1 | rows and values match |
| q33 | OK | 87.4 | 90.0 | 40.3 | 2.23x | 100 / 100 | rows and values match |
| q34 | OK | 40.9 | 33.3 | 26.8 | 1.24x | 53 / 53 | rows and values match |
| q35 | OK | 68.4 | 72.7 | 96.2 | 0.76x | 100 / 100 | rows and values match |
| q36 | OK | 40.6 | 41.4 | 27.3 | 1.52x | 100 / 100 | rows and values match |
| q37 | OK | 19.2 | 15.1 | 16.4 | 0.92x | 0 / 0 | rows and values match |
| q38 | OK | 44.7 | 49.6 | 42.0 | 1.18x | 1 / 1 | rows and values match |
| q39 | OK | 30.9 | 29.5 | 20.8 | 1.42x | 5 / 5 | rows and values match |
| q40 | OK | 27.1 | 26.4 | 17.1 | 1.54x | 44 / 44 | rows and values match |
| q41 | OK | 6.3 | 5.5 | 7.1 | 0.77x | 0 / 0 | rows and values match |
| q42 | OK | 18.8 | 17.2 | 17.9 | 0.96x | 4 / 4 | rows and values match |
| q43 | OK | 20.3 | 20.6 | 25.1 | 0.82x | 1 / 1 | rows and values match |
| q44 | OK | 30.7 | 30.3 | 6.5 | 4.67x | 0 / 0 | rows and values match |
| q45 | OK | 83.9 | 85.3 | 34.0 | 2.51x | 24 / 24 | rows and values match |
| q46 | OK | 51.9 | 50.3 | 39.6 | 1.27x | 100 / 100 | rows and values match |
| q47 | OK | 92.1 | 85.5 | 39.8 | 2.15x | 100 / 100 | rows and values match |
| q48 | OK | 74.8 | 75.7 | 59.7 | 1.27x | 1 / 1 | rows and values match |
| q49 | OK | 53.5 | 55.4 | 33.6 | 1.65x | 2 / 2 | rows and values match |
| q50 | OK | 192.3 | 154.3 | 34.4 | 4.49x | 1 / 1 | rows and values match |
| q51 | OK | 48.7 | 47.7 | 51.6 | 0.93x | 100 / 100 | rows and values match |
| q52 | OK | 17.6 | 19.0 | 17.1 | 1.11x | 11 / 11 | rows and values match |
| q53 | OK | 29.3 | 30.4 | 23.8 | 1.28x | 100 / 100 | rows and values match |
| q54 | OK | 64.3 | 61.4 | 54.3 | 1.13x | 0 / 0 | rows and values match |
| q55 | OK | 15.9 | 17.6 | 18.2 | 0.97x | 20 / 20 | rows and values match |
| q56 | OK | 75.6 | 77.7 | 40.8 | 1.91x | 38 / 38 | rows and values match |
| q57 | OK | 70.8 | 76.5 | 48.1 | 1.59x | 100 / 100 | rows and values match |
| q58 | OK | 109.7 | 112.8 | 63.0 | 1.79x | 0 / 0 | rows and values match |
| q59 | OK | 98.0 | 92.6 | 52.2 | 1.77x | 100 / 100 | rows and values match |
| q60 | OK | 82.7 | 83.7 | 41.8 | 2.00x | 100 / 100 | rows and values match |
| q61 | OK | 58.3 | 51.7 | 37.9 | 1.36x | 1 / 1 | rows and values match |
| q62 | OK | 38.4 | 32.6 | 28.8 | 1.13x | 6 / 6 | rows and values match |
| q63 | OK | 30.8 | 31.6 | 24.4 | 1.30x | 100 / 100 | rows and values match |
| q64 | OK | 225.6 | 221.7 | 93.0 | 2.38x | 0 / 0 | rows and values match |
| q65 | OK | 40.6 | 39.2 | 27.9 | 1.40x | 0 / 0 | rows and values match |
| q66 | OK | 126.3 | 105.1 | 70.3 | 1.50x | 1 / 1 | rows and values match |
| q67 | OK | 60.9 | 60.7 | 77.1 | 0.79x | 100 / 100 | rows and values match |
| q68 | OK | 49.9 | 45.3 | 36.5 | 1.24x | 100 / 100 | rows and values match |
| q69 | OK | 55.1 | 57.8 | 81.0 | 0.71x | 71 / 71 | rows and values match |
| q70 | OK | 46.2 | 43.4 | 28.9 | 1.50x | 3 / 3 | rows and values match |
| q71 | OK | 44.2 | 47.4 | 59.7 | 0.79x | 56 / 56 | rows and values match |
| q72 | OK | 140.4 | 145.1 | 71.4 | 2.03x | 50 / 50 | rows and values match |
| q73 | OK | 25.9 | 27.7 | 33.2 | 0.83x | 0 / 0 | rows and values match |
| q74 | OK | 87.3 | 87.3 | 40.4 | 2.16x | 3 / 3 | rows and values match |
| q75 | OK | 119.4 | 122.5 | 37.1 | 3.30x | 25 / 25 | rows and values match |
| q76 | OK | 65.2 | 65.9 | 35.3 | 1.87x | 100 / 100 | rows and values match |
| q77 | OK | 59.7 | 52.6 | 35.3 | 1.49x | 10 / 10 | rows and values match |
| q78 | OK | 72.7 | 83.0 | 39.8 | 2.08x | 100 / 100 | rows and values match |
| q79 | OK | 44.8 | 36.1 | 27.9 | 1.30x | 100 / 100 | rows and values match |
| q80 | OK | 73.0 | 84.2 | 46.0 | 1.83x | 100 / 100 | rows and values match |
| q81 | OK | 55.0 | 55.2 | 29.2 | 1.89x | 34 / 34 | rows and values match |
| q82 | OK | 14.6 | 16.7 | 15.6 | 1.07x | 0 / 0 | rows and values match |
| q83 | OK | 98.7 | 107.4 | 62.9 | 1.71x | 0 / 0 | rows and values match |
| q84 | OK | 29.6 | 28.9 | 29.1 | 0.99x | 6 / 6 | rows and values match |
| q85 | OK | 137.2 | 125.1 | 78.5 | 1.59x | 0 / 0 | rows and values match |
| q86 | OK | 24.5 | 24.1 | 22.1 | 1.09x | 100 / 100 | rows and values match |
| q87 | OK | 42.2 | 40.8 | 42.4 | 0.96x | 1 / 1 | rows and values match |
| q88 | OK | 121.5 | 93.6 | 209.8 | 0.45x | 1 / 1 | rows and values match |
| q89 | OK | 34.0 | 39.2 | 33.7 | 1.16x | 100 / 100 | rows and values match |
| q90 | OK | 33.3 | 30.6 | 38.7 | 0.79x | 1 / 1 | rows and values match |
| q91 | OK | 47.5 | 41.1 | 57.4 | 0.72x | 0 / 0 | rows and values match |
| q92 | OK | 21.1 | 22.3 | 20.0 | 1.12x | 1 / 1 | rows and values match |
| q93 | OK | 12.2 | 10.3 | 4.1 | 2.49x | 0 / 0 | rows and values match |
| q94 | OK | 31.0 | 31.3 | 26.9 | 1.16x | 1 / 1 | rows and values match |
| q95 | OK | 27.2 | 28.8 | 30.0 | 0.96x | 1 / 1 | rows and values match |
| q96 | OK | 17.4 | 15.7 | 24.6 | 0.64x | 1 / 1 | rows and values match |
| q97 | OK | 24.9 | 27.9 | 28.3 | 0.99x | 1 / 1 | rows and values match |
| q98 | OK | 26.1 | 25.5 | 21.0 | 1.21x | 250 / 250 | rows and values match |
| q99 | OK | 22.3 | 23.7 | 20.5 | 1.15x | 6 / 6 | rows and values match |
