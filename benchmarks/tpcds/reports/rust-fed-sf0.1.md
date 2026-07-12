# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:13

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 21.1s)
Timing: ours 5.6s  duckdb 1.1s  ->  total 4.92x  geomean 4.62x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.6 | 1.1 | 4.92x | 4.62x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 185.3 | 36.8 | 7.1 | 5.17x | 100 / 100 | rows and values match |
| q02 | OK | 203.2 | 96.1 | 14.5 | 6.64x | 2513 / 2513 | rows and values match |
| q03 | OK | 30.6 | 19.6 | 4.4 | 4.41x | 15 / 15 | rows and values match |
| q04 | OK | 248.4 | 218.9 | 31.5 | 6.95x | 0 / 0 | rows and values match |
| q05 | OK | 72.9 | 47.5 | 10.8 | 4.39x | 100 / 100 | rows and values match |
| q06 | OK | 43.8 | 40.3 | 10.1 | 3.98x | 10 / 10 | rows and values match |
| q07 | OK | 42.9 | 31.2 | 10.6 | 2.95x | 100 / 100 | rows and values match |
| q08 | OK | 35.1 | 29.0 | 19.5 | 1.49x | 0 / 0 | rows and values match |
| q09 | OK | 27.9 | 24.7 | 7.4 | 3.34x | 1 / 1 | rows and values match |
| q10 | OK | 62.2 | 57.1 | 14.4 | 3.98x | 1 / 1 | rows and values match |
| q11 | OK | 121.8 | 119.0 | 23.5 | 5.06x | 4 / 4 | rows and values match |
| q12 | OK | 28.5 | 27.8 | 8.9 | 3.13x | 100 / 100 | rows and values match |
| q13 | OK | 80.9 | 70.5 | 11.3 | 6.25x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 22.0 | 22.5 | 6.1 | 3.71x | 33 / 33 | rows and values match |
| q16 | OK | 36.2 | 28.5 | 7.3 | 3.90x | 1 / 1 | rows and values match |
| q17 | OK | 60.2 | 57.6 | 11.4 | 5.04x | 1 / 1 | rows and values match |
| q18 | OK | 62.0 | 59.2 | 15.7 | 3.76x | 100 / 100 | rows and values match |
| q19 | OK | 34.7 | 31.2 | 8.8 | 3.54x | 10 / 10 | rows and values match |
| q20 | OK | 25.7 | 25.1 | 7.0 | 3.58x | 100 / 100 | rows and values match |
| q21 | OK | 26.4 | 15.0 | 3.8 | 3.90x | 32 / 32 | rows and values match |
| q22 | OK | 31.8 | 28.0 | 11.4 | 2.45x | 100 / 100 | rows and values match |
| q23 | OK | 203.6 | 192.0 | 38.1 | 5.04x | 1 / 1 | rows and values match |
| q24 | OK | 57.8 | 44.9 | 3.9 | 11.39x | 0 / 0 | rows and values match |
| q25 | OK | 48.3 | 41.6 | 8.8 | 4.72x | 0 / 0 | rows and values match |
| q26 | OK | 30.2 | 29.8 | 8.9 | 3.34x | 100 / 100 | rows and values match |
| q27 | OK | 55.6 | 56.1 | 12.8 | 4.38x | 100 / 100 | rows and values match |
| q28 | OK | 28.7 | 21.0 | 10.0 | 2.10x | 1 / 1 | rows and values match |
| q29 | OK | 52.8 | 47.1 | 10.3 | 4.55x | 0 / 0 | rows and values match |
| q30 | OK | 45.3 | 41.6 | 9.4 | 4.42x | 21 / 21 | rows and values match |
| q31 | OK | 89.8 | 96.0 | 13.5 | 7.11x | 1 / 1 | rows and values match |
| q32 | OK | 21.3 | 19.8 | 2.7 | 7.34x | 1 / 1 | rows and values match |
| q33 | OK | 92.9 | 83.9 | 7.2 | 11.64x | 100 / 100 | rows and values match |
| q34 | OK | 39.5 | 30.4 | 8.2 | 3.72x | 53 / 53 | rows and values match |
| q35 | OK | 63.4 | 64.0 | 16.2 | 3.96x | 100 / 100 | rows and values match |
| q36 | OK | 40.2 | 39.8 | 14.3 | 2.78x | 100 / 100 | rows and values match |
| q37 | OK | 19.6 | 14.3 | 2.3 | 6.29x | 0 / 0 | rows and values match |
| q38 | OK | 44.9 | 46.8 | 11.1 | 4.22x | 1 / 1 | rows and values match |
| q39 | OK | 29.4 | 30.0 | 5.2 | 5.74x | 5 / 5 | rows and values match |
| q40 | OK | 27.3 | 29.0 | 6.3 | 4.58x | 44 / 44 | rows and values match |
| q41 | OK | 5.4 | 4.5 | 4.7 | 0.96x | 0 / 0 | rows and values match |
| q42 | OK | 17.9 | 16.1 | 4.0 | 4.06x | 4 / 4 | rows and values match |
| q43 | OK | 22.4 | 19.1 | 9.1 | 2.11x | 1 / 1 | rows and values match |
| q44 | OK | 35.9 | 33.3 | 3.3 | 10.18x | 0 / 0 | rows and values match |
| q45 | OK | 88.7 | 88.5 | 8.0 | 11.12x | 24 / 24 | rows and values match |
| q46 | OK | 52.7 | 52.4 | 12.8 | 4.08x | 100 / 100 | rows and values match |
| q47 | OK | 92.1 | 88.8 | 22.7 | 3.90x | 100 / 100 | rows and values match |
| q48 | OK | 84.6 | 78.1 | 9.4 | 8.29x | 1 / 1 | rows and values match |
| q49 | OK | 54.6 | 55.6 | 9.5 | 5.85x | 2 / 2 | rows and values match |
| q50 | OK | 194.9 | 178.1 | 9.5 | 18.76x | 1 / 1 | rows and values match |
| q51 | OK | 53.5 | 50.2 | 29.9 | 1.68x | 100 / 100 | rows and values match |
| q52 | OK | 19.7 | 17.9 | 4.7 | 3.78x | 11 / 11 | rows and values match |
| q53 | OK | 33.7 | 35.4 | 10.1 | 3.50x | 100 / 100 | rows and values match |
| q54 | OK | 67.9 | 63.4 | 8.9 | 7.11x | 0 / 0 | rows and values match |
| q55 | OK | 17.9 | 18.4 | 3.9 | 4.77x | 20 / 20 | rows and values match |
| q56 | OK | 85.9 | 91.5 | 10.5 | 8.70x | 38 / 38 | rows and values match |
| q57 | OK | 72.7 | 76.0 | 21.7 | 3.50x | 100 / 100 | rows and values match |
| q58 | OK | 119.7 | 114.0 | 9.6 | 11.82x | 0 / 0 | rows and values match |
| q59 | OK | 101.8 | 94.8 | 22.9 | 4.13x | 100 / 100 | rows and values match |
| q60 | OK | 84.1 | 87.2 | 11.6 | 7.53x | 100 / 100 | rows and values match |
| q61 | OK | 51.3 | 50.9 | 8.1 | 6.28x | 1 / 1 | rows and values match |
| q62 | OK | 43.7 | 33.7 | 7.6 | 4.44x | 6 / 6 | rows and values match |
| q63 | OK | 33.4 | 31.6 | 9.1 | 3.48x | 100 / 100 | rows and values match |
| q64 | OK | 234.3 | 249.3 | 19.0 | 13.15x | 0 / 0 | rows and values match |
| q65 | OK | 42.0 | 36.9 | 9.6 | 3.84x | 0 / 0 | rows and values match |
| q66 | OK | 116.2 | 108.7 | 17.3 | 6.29x | 1 / 1 | rows and values match |
| q67 | OK | 55.9 | 60.4 | 51.0 | 1.18x | 100 / 100 | rows and values match |
| q68 | OK | 48.3 | 45.4 | 12.1 | 3.74x | 100 / 100 | rows and values match |
| q69 | OK | 55.7 | 62.0 | 16.2 | 3.83x | 71 / 71 | rows and values match |
| q70 | OK | 42.4 | 46.2 | 9.1 | 5.07x | 3 / 3 | rows and values match |
| q71 | OK | 45.2 | 45.3 | 8.4 | 5.37x | 56 / 56 | rows and values match |
| q72 | OK | 164.7 | 150.5 | 18.6 | 8.10x | 50 / 50 | rows and values match |
| q73 | OK | 27.6 | 28.7 | 7.1 | 4.01x | 0 / 0 | rows and values match |
| q74 | OK | 89.2 | 85.4 | 25.2 | 3.39x | 3 / 3 | rows and values match |
| q75 | OK | 125.7 | 139.0 | 17.5 | 7.96x | 25 / 25 | rows and values match |
| q76 | OK | 66.0 | 60.4 | 12.2 | 4.94x | 100 / 100 | rows and values match |
| q77 | OK | 71.8 | 70.7 | 10.7 | 6.62x | 10 / 10 | rows and values match |
| q78 | OK | 86.3 | 112.9 | 27.9 | 4.05x | 100 / 100 | rows and values match |
| q79 | OK | 43.9 | 42.3 | 11.2 | 3.76x | 100 / 100 | rows and values match |
| q80 | OK | 73.6 | 72.5 | 14.9 | 4.88x | 100 / 100 | rows and values match |
| q81 | OK | 71.9 | 71.6 | 11.5 | 6.23x | 34 / 34 | rows and values match |
| q82 | OK | 14.9 | 14.4 | 2.4 | 6.03x | 0 / 0 | rows and values match |
| q83 | OK | 103.8 | 96.6 | 9.5 | 10.13x | 0 / 0 | rows and values match |
| q84 | OK | 31.4 | 26.4 | 6.8 | 3.88x | 6 / 6 | rows and values match |
| q85 | OK | 141.3 | 130.1 | 10.5 | 12.41x | 0 / 0 | rows and values match |
| q86 | OK | 29.5 | 26.3 | 7.9 | 3.34x | 100 / 100 | rows and values match |
| q87 | OK | 45.7 | 41.7 | 14.4 | 2.90x | 1 / 1 | rows and values match |
| q88 | OK | 126.0 | 94.9 | 21.5 | 4.42x | 1 / 1 | rows and values match |
| q89 | OK | 37.5 | 37.0 | 11.7 | 3.17x | 100 / 100 | rows and values match |
| q90 | OK | 33.1 | 27.6 | 2.9 | 9.61x | 1 / 1 | rows and values match |
| q91 | OK | 45.6 | 42.4 | 8.4 | 5.07x | 0 / 0 | rows and values match |
| q92 | OK | 21.1 | 22.6 | 4.2 | 5.38x | 1 / 1 | rows and values match |
| q93 | OK | 10.9 | 10.0 | 3.0 | 3.36x | 0 / 0 | rows and values match |
| q94 | OK | 31.6 | 31.6 | 5.3 | 5.97x | 1 / 1 | rows and values match |
| q95 | OK | 29.7 | 39.5 | 17.1 | 2.31x | 1 / 1 | rows and values match |
| q96 | OK | 15.9 | 14.4 | 3.5 | 4.15x | 1 / 1 | rows and values match |
| q97 | OK | 26.3 | 25.4 | 8.7 | 2.91x | 1 / 1 | rows and values match |
| q98 | OK | 26.2 | 32.1 | 9.6 | 3.34x | 250 / 250 | rows and values match |
| q99 | OK | 22.1 | 23.5 | 7.6 | 3.11x | 6 / 6 | rows and values match |
