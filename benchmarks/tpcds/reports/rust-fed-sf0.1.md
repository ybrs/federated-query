# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 01:39

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 11.0s)
Timing: ours 4.9s  duckdb 4.0s  ->  total 1.23x  geomean 1.21x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 4.9 | 4.0 | 1.23x | 1.21x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 188.4 | 34.3 | 22.1 | 1.55x | 100 / 100 | rows and values match |
| q02 | OK | 102.5 | 58.6 | 42.0 | 1.39x | 2513 / 2513 | rows and values match |
| q03 | OK | 27.5 | 17.2 | 19.1 | 0.90x | 15 / 15 | rows and values match |
| q04 | OK | 143.0 | 123.5 | 68.5 | 1.80x | 0 / 0 | rows and values match |
| q05 | OK | 64.2 | 50.3 | 34.2 | 1.47x | 100 / 100 | rows and values match |
| q06 | OK | 107.4 | 42.8 | 44.1 | 0.97x | 10 / 10 | rows and values match |
| q07 | OK | 50.3 | 34.0 | 72.6 | 0.47x | 100 / 100 | rows and values match |
| q08 | OK | 38.9 | 31.2 | 35.2 | 0.89x | 0 / 0 | rows and values match |
| q09 | OK | 32.9 | 27.1 | 8.3 | 3.27x | 1 / 1 | rows and values match |
| q10 | OK | 69.5 | 57.9 | 99.6 | 0.58x | 1 / 1 | rows and values match |
| q11 | OK | 78.5 | 75.9 | 52.3 | 1.45x | 4 / 4 | rows and values match |
| q12 | OK | 28.6 | 26.6 | 18.3 | 1.45x | 100 / 100 | rows and values match |
| q13 | OK | 78.4 | 73.0 | 54.8 | 1.33x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 21.7 | 22.5 | 25.5 | 0.88x | 33 / 33 | rows and values match |
| q16 | OK | 35.3 | 29.0 | 27.8 | 1.04x | 1 / 1 | rows and values match |
| q17 | OK | 53.9 | 55.3 | 38.5 | 1.44x | 1 / 1 | rows and values match |
| q18 | OK | 63.0 | 55.8 | 77.1 | 0.72x | 100 / 100 | rows and values match |
| q19 | OK | 31.8 | 32.1 | 35.4 | 0.91x | 10 / 10 | rows and values match |
| q20 | OK | 25.3 | 26.8 | 21.1 | 1.27x | 100 / 100 | rows and values match |
| q21 | OK | 24.1 | 15.8 | 17.5 | 0.90x | 32 / 32 | rows and values match |
| q22 | OK | 30.0 | 29.8 | 25.5 | 1.17x | 100 / 100 | rows and values match |
| q23 | OK | 151.0 | 148.3 | 72.1 | 2.06x | 1 / 1 | rows and values match |
| q24 | OK | 48.7 | 37.0 | 15.8 | 2.34x | 0 / 0 | rows and values match |
| q25 | OK | 48.2 | 46.1 | 36.3 | 1.27x | 0 / 0 | rows and values match |
| q26 | OK | 32.1 | 29.7 | 66.8 | 0.44x | 100 / 100 | rows and values match |
| q27 | OK | 45.1 | 46.4 | 57.4 | 0.81x | 100 / 100 | rows and values match |
| q28 | OK | 27.9 | 19.6 | 10.7 | 1.83x | 1 / 1 | rows and values match |
| q29 | OK | 45.6 | 45.9 | 50.2 | 0.91x | 0 / 0 | rows and values match |
| q30 | OK | 41.4 | 35.9 | 32.2 | 1.12x | 21 / 21 | rows and values match |
| q31 | OK | 76.5 | 70.7 | 39.5 | 1.79x | 1 / 1 | rows and values match |
| q32 | OK | 20.9 | 20.8 | 22.2 | 0.94x | 1 / 1 | rows and values match |
| q33 | OK | 90.2 | 87.8 | 40.3 | 2.18x | 100 / 100 | rows and values match |
| q34 | OK | 41.6 | 32.0 | 26.8 | 1.19x | 53 / 53 | rows and values match |
| q35 | OK | 67.2 | 65.5 | 96.2 | 0.68x | 100 / 100 | rows and values match |
| q36 | OK | 36.8 | 35.6 | 27.3 | 1.30x | 100 / 100 | rows and values match |
| q37 | OK | 26.3 | 22.1 | 16.4 | 1.34x | 0 / 0 | rows and values match |
| q38 | OK | 51.7 | 42.6 | 42.0 | 1.01x | 1 / 1 | rows and values match |
| q39 | OK | 25.8 | 28.8 | 20.8 | 1.39x | 5 / 5 | rows and values match |
| q40 | OK | 31.3 | 26.2 | 17.1 | 1.53x | 44 / 44 | rows and values match |
| q41 | OK | 5.2 | 4.4 | 7.1 | 0.62x | 0 / 0 | rows and values match |
| q42 | OK | 17.8 | 15.9 | 17.9 | 0.89x | 4 / 4 | rows and values match |
| q43 | OK | 20.6 | 19.0 | 25.1 | 0.76x | 1 / 1 | rows and values match |
| q44 | OK | 36.5 | 29.4 | 6.5 | 4.54x | 0 / 0 | rows and values match |
| q45 | OK | 88.8 | 84.2 | 34.0 | 2.47x | 24 / 24 | rows and values match |
| q46 | OK | 53.0 | 51.0 | 39.6 | 1.29x | 100 / 100 | rows and values match |
| q47 | OK | 57.9 | 56.8 | 39.8 | 1.43x | 100 / 100 | rows and values match |
| q48 | OK | 71.7 | 66.8 | 59.7 | 1.12x | 1 / 1 | rows and values match |
| q49 | OK | 53.0 | 53.4 | 33.6 | 1.59x | 2 / 2 | rows and values match |
| q50 | OK | 193.8 | 152.6 | 34.4 | 4.44x | 1 / 1 | rows and values match |
| q51 | OK | 49.0 | 45.8 | 51.6 | 0.89x | 100 / 100 | rows and values match |
| q52 | OK | 18.5 | 16.0 | 17.1 | 0.94x | 11 / 11 | rows and values match |
| q53 | OK | 28.5 | 28.5 | 23.8 | 1.20x | 100 / 100 | rows and values match |
| q54 | OK | 58.3 | 59.1 | 54.3 | 1.09x | 0 / 0 | rows and values match |
| q55 | OK | 17.2 | 15.5 | 18.2 | 0.85x | 20 / 20 | rows and values match |
| q56 | OK | 73.5 | 74.8 | 40.8 | 1.83x | 38 / 38 | rows and values match |
| q57 | OK | 49.2 | 50.6 | 48.1 | 1.05x | 100 / 100 | rows and values match |
| q58 | OK | 107.6 | 107.2 | 63.0 | 1.70x | 0 / 0 | rows and values match |
| q59 | OK | 78.8 | 72.0 | 52.2 | 1.38x | 100 / 100 | rows and values match |
| q60 | OK | 80.8 | 102.9 | 41.8 | 2.46x | 100 / 100 | rows and values match |
| q61 | OK | 49.3 | 48.7 | 37.9 | 1.29x | 1 / 1 | rows and values match |
| q62 | OK | 38.9 | 31.9 | 28.8 | 1.11x | 6 / 6 | rows and values match |
| q63 | OK | 30.0 | 31.9 | 24.4 | 1.31x | 100 / 100 | rows and values match |
| q64 | OK | 173.0 | 159.2 | 93.0 | 1.71x | 0 / 0 | rows and values match |
| q65 | OK | 37.8 | 33.8 | 27.9 | 1.21x | 0 / 0 | rows and values match |
| q66 | OK | 118.3 | 126.9 | 70.3 | 1.81x | 1 / 1 | rows and values match |
| q67 | OK | 55.7 | 54.4 | 77.1 | 0.71x | 100 / 100 | rows and values match |
| q68 | OK | 49.1 | 45.7 | 36.5 | 1.25x | 100 / 100 | rows and values match |
| q69 | OK | 54.8 | 54.3 | 81.0 | 0.67x | 71 / 71 | rows and values match |
| q70 | OK | 44.5 | 44.7 | 28.9 | 1.54x | 3 / 3 | rows and values match |
| q71 | OK | 47.5 | 46.4 | 59.7 | 0.78x | 56 / 56 | rows and values match |
| q72 | OK | 134.3 | 131.9 | 71.4 | 1.85x | 50 / 50 | rows and values match |
| q73 | OK | 28.2 | 27.7 | 33.2 | 0.83x | 0 / 0 | rows and values match |
| q74 | OK | 56.9 | 55.4 | 40.4 | 1.37x | 3 / 3 | rows and values match |
| q75 | OK | 81.2 | 82.4 | 37.1 | 2.22x | 25 / 25 | rows and values match |
| q76 | OK | 61.7 | 55.4 | 35.3 | 1.57x | 100 / 100 | rows and values match |
| q77 | OK | 56.2 | 49.1 | 35.3 | 1.39x | 10 / 10 | rows and values match |
| q78 | OK | 71.3 | 68.3 | 39.8 | 1.72x | 100 / 100 | rows and values match |
| q79 | OK | 37.9 | 36.6 | 27.9 | 1.31x | 100 / 100 | rows and values match |
| q80 | OK | 66.5 | 67.2 | 46.0 | 1.46x | 100 / 100 | rows and values match |
| q81 | OK | 46.5 | 41.6 | 29.2 | 1.42x | 34 / 34 | rows and values match |
| q82 | OK | 14.9 | 14.6 | 15.6 | 0.94x | 0 / 0 | rows and values match |
| q83 | OK | 99.4 | 99.8 | 62.9 | 1.59x | 0 / 0 | rows and values match |
| q84 | OK | 28.8 | 26.2 | 29.1 | 0.90x | 6 / 6 | rows and values match |
| q85 | OK | 133.0 | 127.0 | 78.5 | 1.62x | 0 / 0 | rows and values match |
| q86 | OK | 25.0 | 24.0 | 22.1 | 1.08x | 100 / 100 | rows and values match |
| q87 | OK | 40.4 | 39.7 | 42.4 | 0.94x | 1 / 1 | rows and values match |
| q88 | OK | 119.1 | 94.9 | 209.8 | 0.45x | 1 / 1 | rows and values match |
| q89 | OK | 32.6 | 32.3 | 33.7 | 0.96x | 100 / 100 | rows and values match |
| q90 | OK | 34.1 | 29.0 | 38.7 | 0.75x | 1 / 1 | rows and values match |
| q91 | OK | 44.4 | 41.2 | 57.4 | 0.72x | 0 / 0 | rows and values match |
| q92 | OK | 21.3 | 21.5 | 20.0 | 1.08x | 1 / 1 | rows and values match |
| q93 | OK | 11.0 | 9.3 | 4.1 | 2.25x | 0 / 0 | rows and values match |
| q94 | OK | 32.0 | 30.1 | 26.9 | 1.12x | 1 / 1 | rows and values match |
| q95 | OK | 27.2 | 27.6 | 30.0 | 0.92x | 1 / 1 | rows and values match |
| q96 | OK | 14.6 | 15.1 | 24.6 | 0.61x | 1 / 1 | rows and values match |
| q97 | OK | 25.0 | 25.9 | 28.3 | 0.92x | 1 / 1 | rows and values match |
| q98 | OK | 26.3 | 25.6 | 21.0 | 1.22x | 250 / 250 | rows and values match |
| q99 | OK | 20.7 | 21.0 | 20.5 | 1.02x | 6 / 6 | rows and values match |
