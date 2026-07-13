# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 02:17

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 11.5s)
Timing: ours 5.1s  duckdb 4.0s  ->  total 1.28x  geomean 1.26x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.1 | 4.0 | 1.28x | 1.26x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 206.2 | 31.7 | 22.1 | 1.44x | 100 / 100 | rows and values match |
| q02 | OK | 107.9 | 63.7 | 42.0 | 1.52x | 2513 / 2513 | rows and values match |
| q03 | OK | 29.2 | 19.8 | 19.1 | 1.04x | 15 / 15 | rows and values match |
| q04 | OK | 154.6 | 124.0 | 68.5 | 1.81x | 0 / 0 | rows and values match |
| q05 | OK | 64.7 | 45.3 | 34.2 | 1.32x | 100 / 100 | rows and values match |
| q06 | OK | 76.5 | 42.8 | 44.1 | 0.97x | 10 / 10 | rows and values match |
| q07 | OK | 52.8 | 34.8 | 72.6 | 0.48x | 100 / 100 | rows and values match |
| q08 | OK | 37.2 | 31.3 | 35.2 | 0.89x | 0 / 0 | rows and values match |
| q09 | OK | 28.6 | 24.5 | 8.3 | 2.96x | 1 / 1 | rows and values match |
| q10 | OK | 59.2 | 54.6 | 99.6 | 0.55x | 1 / 1 | rows and values match |
| q11 | OK | 81.2 | 83.9 | 52.3 | 1.60x | 4 / 4 | rows and values match |
| q12 | OK | 28.1 | 26.3 | 18.3 | 1.44x | 100 / 100 | rows and values match |
| q13 | OK | 96.9 | 68.5 | 54.8 | 1.25x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 23.8 | 25.1 | 25.5 | 0.98x | 33 / 33 | rows and values match |
| q16 | OK | 37.0 | 30.0 | 27.8 | 1.08x | 1 / 1 | rows and values match |
| q17 | OK | 51.8 | 46.4 | 38.5 | 1.21x | 1 / 1 | rows and values match |
| q18 | OK | 70.3 | 59.9 | 77.1 | 0.78x | 100 / 100 | rows and values match |
| q19 | OK | 36.0 | 30.4 | 35.4 | 0.86x | 10 / 10 | rows and values match |
| q20 | OK | 26.2 | 29.0 | 21.1 | 1.37x | 100 / 100 | rows and values match |
| q21 | OK | 24.1 | 14.4 | 17.5 | 0.83x | 32 / 32 | rows and values match |
| q22 | OK | 34.9 | 31.3 | 25.5 | 1.22x | 100 / 100 | rows and values match |
| q23 | OK | 147.8 | 148.4 | 72.1 | 2.06x | 1 / 1 | rows and values match |
| q24 | OK | 49.4 | 35.8 | 15.8 | 2.27x | 0 / 0 | rows and values match |
| q25 | OK | 49.7 | 44.5 | 36.3 | 1.22x | 0 / 0 | rows and values match |
| q26 | OK | 32.7 | 30.6 | 66.8 | 0.46x | 100 / 100 | rows and values match |
| q27 | OK | 44.6 | 48.7 | 57.4 | 0.85x | 100 / 100 | rows and values match |
| q28 | OK | 29.8 | 19.2 | 10.7 | 1.79x | 1 / 1 | rows and values match |
| q29 | OK | 42.6 | 44.1 | 50.2 | 0.88x | 0 / 0 | rows and values match |
| q30 | OK | 43.9 | 37.7 | 32.2 | 1.17x | 21 / 21 | rows and values match |
| q31 | OK | 80.3 | 73.1 | 39.5 | 1.85x | 1 / 1 | rows and values match |
| q32 | OK | 20.2 | 21.0 | 22.2 | 0.95x | 1 / 1 | rows and values match |
| q33 | OK | 115.5 | 102.8 | 40.3 | 2.55x | 100 / 100 | rows and values match |
| q34 | OK | 46.9 | 40.1 | 26.8 | 1.50x | 53 / 53 | rows and values match |
| q35 | OK | 79.6 | 78.4 | 96.2 | 0.81x | 100 / 100 | rows and values match |
| q36 | OK | 38.6 | 45.9 | 27.3 | 1.68x | 100 / 100 | rows and values match |
| q37 | OK | 30.7 | 14.4 | 16.4 | 0.88x | 0 / 0 | rows and values match |
| q38 | OK | 53.3 | 50.5 | 42.0 | 1.20x | 1 / 1 | rows and values match |
| q39 | OK | 29.3 | 30.3 | 20.8 | 1.46x | 5 / 5 | rows and values match |
| q40 | OK | 29.3 | 27.0 | 17.1 | 1.58x | 44 / 44 | rows and values match |
| q41 | OK | 5.5 | 7.0 | 7.1 | 0.99x | 0 / 0 | rows and values match |
| q42 | OK | 20.2 | 17.6 | 17.9 | 0.98x | 4 / 4 | rows and values match |
| q43 | OK | 22.2 | 23.0 | 25.1 | 0.92x | 1 / 1 | rows and values match |
| q44 | OK | 32.8 | 29.6 | 6.5 | 4.56x | 0 / 0 | rows and values match |
| q45 | OK | 38.3 | 38.8 | 34.0 | 1.14x | 24 / 24 | rows and values match |
| q46 | OK | 69.1 | 63.2 | 39.6 | 1.59x | 100 / 100 | rows and values match |
| q47 | OK | 63.0 | 58.8 | 39.8 | 1.48x | 100 / 100 | rows and values match |
| q48 | OK | 84.5 | 71.6 | 59.7 | 1.20x | 1 / 1 | rows and values match |
| q49 | OK | 52.5 | 53.6 | 33.6 | 1.59x | 2 / 2 | rows and values match |
| q50 | OK | 198.4 | 176.9 | 34.4 | 5.15x | 1 / 1 | rows and values match |
| q51 | OK | 52.1 | 51.1 | 51.6 | 0.99x | 100 / 100 | rows and values match |
| q52 | OK | 20.3 | 17.9 | 17.1 | 1.05x | 11 / 11 | rows and values match |
| q53 | OK | 32.8 | 32.5 | 23.8 | 1.37x | 100 / 100 | rows and values match |
| q54 | OK | 60.0 | 62.1 | 54.3 | 1.15x | 0 / 0 | rows and values match |
| q55 | OK | 17.0 | 20.1 | 18.2 | 1.11x | 20 / 20 | rows and values match |
| q56 | OK | 79.3 | 87.7 | 40.8 | 2.15x | 38 / 38 | rows and values match |
| q57 | OK | 47.7 | 51.5 | 48.1 | 1.07x | 100 / 100 | rows and values match |
| q58 | OK | 112.0 | 123.9 | 63.0 | 1.97x | 0 / 0 | rows and values match |
| q59 | OK | 85.7 | 73.8 | 52.2 | 1.41x | 100 / 100 | rows and values match |
| q60 | OK | 82.9 | 84.7 | 41.8 | 2.02x | 100 / 100 | rows and values match |
| q61 | OK | 53.2 | 51.0 | 37.9 | 1.35x | 1 / 1 | rows and values match |
| q62 | OK | 49.5 | 34.0 | 28.8 | 1.18x | 6 / 6 | rows and values match |
| q63 | OK | 30.8 | 31.7 | 24.4 | 1.30x | 100 / 100 | rows and values match |
| q64 | OK | 171.0 | 161.0 | 93.0 | 1.73x | 0 / 0 | rows and values match |
| q65 | OK | 43.9 | 43.4 | 27.9 | 1.55x | 0 / 0 | rows and values match |
| q66 | OK | 140.5 | 114.3 | 70.3 | 1.63x | 1 / 1 | rows and values match |
| q67 | OK | 58.9 | 65.9 | 77.1 | 0.85x | 100 / 100 | rows and values match |
| q68 | OK | 47.1 | 45.5 | 36.5 | 1.25x | 100 / 100 | rows and values match |
| q69 | OK | 57.1 | 61.0 | 81.0 | 0.75x | 71 / 71 | rows and values match |
| q70 | OK | 40.6 | 40.8 | 28.9 | 1.41x | 3 / 3 | rows and values match |
| q71 | OK | 44.5 | 41.1 | 59.7 | 0.69x | 56 / 56 | rows and values match |
| q72 | OK | 149.5 | 136.1 | 71.4 | 1.91x | 50 / 50 | rows and values match |
| q73 | OK | 28.8 | 27.5 | 33.2 | 0.83x | 0 / 0 | rows and values match |
| q74 | OK | 59.0 | 58.2 | 40.4 | 1.44x | 3 / 3 | rows and values match |
| q75 | OK | 93.5 | 87.2 | 37.1 | 2.35x | 25 / 25 | rows and values match |
| q76 | OK | 64.0 | 58.4 | 35.3 | 1.66x | 100 / 100 | rows and values match |
| q77 | OK | 58.0 | 53.0 | 35.3 | 1.50x | 10 / 10 | rows and values match |
| q78 | OK | 73.5 | 68.7 | 39.8 | 1.73x | 100 / 100 | rows and values match |
| q79 | OK | 41.2 | 35.4 | 27.9 | 1.27x | 100 / 100 | rows and values match |
| q80 | OK | 72.4 | 73.6 | 46.0 | 1.60x | 100 / 100 | rows and values match |
| q81 | OK | 51.3 | 64.7 | 29.2 | 2.21x | 34 / 34 | rows and values match |
| q82 | OK | 18.6 | 16.7 | 15.6 | 1.07x | 0 / 0 | rows and values match |
| q83 | OK | 102.7 | 102.5 | 62.9 | 1.63x | 0 / 0 | rows and values match |
| q84 | OK | 28.8 | 27.1 | 29.1 | 0.93x | 6 / 6 | rows and values match |
| q85 | OK | 132.7 | 121.4 | 78.5 | 1.55x | 0 / 0 | rows and values match |
| q86 | OK | 26.0 | 26.5 | 22.1 | 1.20x | 100 / 100 | rows and values match |
| q87 | OK | 40.0 | 43.3 | 42.4 | 1.02x | 1 / 1 | rows and values match |
| q88 | OK | 126.9 | 98.6 | 209.8 | 0.47x | 1 / 1 | rows and values match |
| q89 | OK | 32.7 | 33.6 | 33.7 | 1.00x | 100 / 100 | rows and values match |
| q90 | OK | 35.6 | 35.4 | 38.7 | 0.92x | 1 / 1 | rows and values match |
| q91 | OK | 44.1 | 45.4 | 57.4 | 0.79x | 0 / 0 | rows and values match |
| q92 | OK | 21.1 | 22.9 | 20.0 | 1.15x | 1 / 1 | rows and values match |
| q93 | OK | 13.9 | 11.9 | 4.1 | 2.87x | 0 / 0 | rows and values match |
| q94 | OK | 34.8 | 36.8 | 26.9 | 1.37x | 1 / 1 | rows and values match |
| q95 | OK | 29.1 | 27.1 | 30.0 | 0.90x | 1 / 1 | rows and values match |
| q96 | OK | 16.6 | 14.0 | 24.6 | 0.57x | 1 / 1 | rows and values match |
| q97 | OK | 26.0 | 26.1 | 28.3 | 0.92x | 1 / 1 | rows and values match |
| q98 | OK | 28.1 | 40.1 | 21.0 | 1.91x | 250 / 250 | rows and values match |
| q99 | OK | 23.1 | 21.8 | 20.5 | 1.06x | 6 / 6 | rows and values match |
