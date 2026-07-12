# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:25

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 12.2s)
Timing: ours 5.4s  duckdb 4.0s  ->  total 1.36x  geomean 1.28x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 5.4 | 4.0 | 1.36x | 1.28x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 198.0 | 37.4 | 22.1 | 1.69x | 100 / 100 | rows and values match |
| q02 | OK | 231.2 | 110.0 | 42.0 | 2.62x | 2513 / 2513 | rows and values match |
| q03 | OK | 32.8 | 20.4 | 19.1 | 1.07x | 15 / 15 | rows and values match |
| q04 | OK | 255.2 | 239.6 | 68.5 | 3.50x | 0 / 0 | rows and values match |
| q05 | OK | 61.3 | 47.0 | 34.2 | 1.38x | 100 / 100 | rows and values match |
| q06 | OK | 47.7 | 40.0 | 44.1 | 0.91x | 10 / 10 | rows and values match |
| q07 | OK | 45.1 | 36.9 | 72.6 | 0.51x | 100 / 100 | rows and values match |
| q08 | OK | 35.7 | 29.4 | 35.2 | 0.84x | 0 / 0 | rows and values match |
| q09 | OK | 29.8 | 29.1 | 8.3 | 3.51x | 1 / 1 | rows and values match |
| q10 | OK | 68.2 | 58.6 | 99.6 | 0.59x | 1 / 1 | rows and values match |
| q11 | OK | 121.1 | 118.9 | 52.3 | 2.27x | 4 / 4 | rows and values match |
| q12 | OK | 29.4 | 27.2 | 18.3 | 1.48x | 100 / 100 | rows and values match |
| q13 | OK | 77.0 | 73.5 | 54.8 | 1.34x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 21.7 | 22.6 | 25.5 | 0.88x | 33 / 33 | rows and values match |
| q16 | OK | 37.5 | 31.4 | 27.8 | 1.13x | 1 / 1 | rows and values match |
| q17 | OK | 52.8 | 50.6 | 38.5 | 1.32x | 1 / 1 | rows and values match |
| q18 | OK | 63.3 | 59.4 | 77.1 | 0.77x | 100 / 100 | rows and values match |
| q19 | OK | 32.2 | 32.3 | 35.4 | 0.91x | 10 / 10 | rows and values match |
| q20 | OK | 26.9 | 26.8 | 21.1 | 1.27x | 100 / 100 | rows and values match |
| q21 | OK | 23.8 | 16.5 | 17.5 | 0.95x | 32 / 32 | rows and values match |
| q22 | OK | 31.3 | 27.7 | 25.5 | 1.08x | 100 / 100 | rows and values match |
| q23 | OK | 199.7 | 199.1 | 72.1 | 2.76x | 1 / 1 | rows and values match |
| q24 | OK | 63.5 | 46.0 | 15.8 | 2.92x | 0 / 0 | rows and values match |
| q25 | OK | 49.4 | 42.6 | 36.3 | 1.17x | 0 / 0 | rows and values match |
| q26 | OK | 31.4 | 32.8 | 66.8 | 0.49x | 100 / 100 | rows and values match |
| q27 | OK | 58.5 | 54.2 | 57.4 | 0.94x | 100 / 100 | rows and values match |
| q28 | OK | 31.2 | 21.3 | 10.7 | 1.98x | 1 / 1 | rows and values match |
| q29 | OK | 44.5 | 43.2 | 50.2 | 0.86x | 0 / 0 | rows and values match |
| q30 | OK | 42.8 | 40.8 | 32.2 | 1.27x | 21 / 21 | rows and values match |
| q31 | OK | 93.0 | 91.4 | 39.5 | 2.32x | 1 / 1 | rows and values match |
| q32 | OK | 20.2 | 19.5 | 22.2 | 0.88x | 1 / 1 | rows and values match |
| q33 | OK | 88.4 | 85.2 | 40.3 | 2.12x | 100 / 100 | rows and values match |
| q34 | OK | 40.6 | 30.3 | 26.8 | 1.13x | 53 / 53 | rows and values match |
| q35 | OK | 62.1 | 67.3 | 96.2 | 0.70x | 100 / 100 | rows and values match |
| q36 | OK | 39.3 | 38.0 | 27.3 | 1.39x | 100 / 100 | rows and values match |
| q37 | OK | 19.6 | 17.0 | 16.4 | 1.04x | 0 / 0 | rows and values match |
| q38 | OK | 56.5 | 47.7 | 42.0 | 1.13x | 1 / 1 | rows and values match |
| q39 | OK | 29.4 | 29.8 | 20.8 | 1.43x | 5 / 5 | rows and values match |
| q40 | OK | 28.4 | 24.0 | 17.1 | 1.41x | 44 / 44 | rows and values match |
| q41 | OK | 5.4 | 4.3 | 7.1 | 0.60x | 0 / 0 | rows and values match |
| q42 | OK | 17.2 | 16.2 | 17.9 | 0.91x | 4 / 4 | rows and values match |
| q43 | OK | 24.0 | 18.8 | 25.1 | 0.75x | 1 / 1 | rows and values match |
| q44 | OK | 33.1 | 28.1 | 6.5 | 4.34x | 0 / 0 | rows and values match |
| q45 | OK | 83.3 | 86.2 | 34.0 | 2.53x | 24 / 24 | rows and values match |
| q46 | OK | 52.3 | 53.0 | 39.6 | 1.34x | 100 / 100 | rows and values match |
| q47 | OK | 91.8 | 86.7 | 39.8 | 2.18x | 100 / 100 | rows and values match |
| q48 | OK | 80.4 | 74.7 | 59.7 | 1.25x | 1 / 1 | rows and values match |
| q49 | OK | 59.0 | 52.5 | 33.6 | 1.56x | 2 / 2 | rows and values match |
| q50 | OK | 206.8 | 152.7 | 34.4 | 4.44x | 1 / 1 | rows and values match |
| q51 | OK | 48.1 | 52.7 | 51.6 | 1.02x | 100 / 100 | rows and values match |
| q52 | OK | 17.1 | 18.2 | 17.1 | 1.07x | 11 / 11 | rows and values match |
| q53 | OK | 31.8 | 29.7 | 23.8 | 1.25x | 100 / 100 | rows and values match |
| q54 | OK | 66.2 | 61.2 | 54.3 | 1.13x | 0 / 0 | rows and values match |
| q55 | OK | 16.9 | 15.4 | 18.2 | 0.85x | 20 / 20 | rows and values match |
| q56 | OK | 78.9 | 87.7 | 40.8 | 2.15x | 38 / 38 | rows and values match |
| q57 | OK | 69.1 | 69.2 | 48.1 | 1.44x | 100 / 100 | rows and values match |
| q58 | OK | 112.1 | 110.0 | 63.0 | 1.75x | 0 / 0 | rows and values match |
| q59 | OK | 98.5 | 93.2 | 52.2 | 1.79x | 100 / 100 | rows and values match |
| q60 | OK | 85.6 | 82.3 | 41.8 | 1.97x | 100 / 100 | rows and values match |
| q61 | OK | 51.0 | 47.1 | 37.9 | 1.24x | 1 / 1 | rows and values match |
| q62 | OK | 38.7 | 33.2 | 28.8 | 1.15x | 6 / 6 | rows and values match |
| q63 | OK | 33.9 | 32.5 | 24.4 | 1.33x | 100 / 100 | rows and values match |
| q64 | OK | 232.8 | 213.5 | 93.0 | 2.30x | 0 / 0 | rows and values match |
| q65 | OK | 41.4 | 33.7 | 27.9 | 1.21x | 0 / 0 | rows and values match |
| q66 | OK | 131.3 | 103.9 | 70.3 | 1.48x | 1 / 1 | rows and values match |
| q67 | OK | 60.7 | 58.1 | 77.1 | 0.75x | 100 / 100 | rows and values match |
| q68 | OK | 52.5 | 48.9 | 36.5 | 1.34x | 100 / 100 | rows and values match |
| q69 | OK | 57.8 | 60.8 | 81.0 | 0.75x | 71 / 71 | rows and values match |
| q70 | OK | 40.4 | 40.5 | 28.9 | 1.40x | 3 / 3 | rows and values match |
| q71 | OK | 53.9 | 46.5 | 59.7 | 0.78x | 56 / 56 | rows and values match |
| q72 | OK | 141.8 | 146.8 | 71.4 | 2.06x | 50 / 50 | rows and values match |
| q73 | OK | 27.8 | 27.3 | 33.2 | 0.82x | 0 / 0 | rows and values match |
| q74 | OK | 81.9 | 84.6 | 40.4 | 2.09x | 3 / 3 | rows and values match |
| q75 | OK | 130.1 | 123.7 | 37.1 | 3.33x | 25 / 25 | rows and values match |
| q76 | OK | 63.5 | 62.5 | 35.3 | 1.77x | 100 / 100 | rows and values match |
| q77 | OK | 55.1 | 51.0 | 35.3 | 1.44x | 10 / 10 | rows and values match |
| q78 | OK | 67.1 | 71.5 | 39.8 | 1.80x | 100 / 100 | rows and values match |
| q79 | OK | 40.0 | 33.9 | 27.9 | 1.22x | 100 / 100 | rows and values match |
| q80 | OK | 71.4 | 72.6 | 46.0 | 1.58x | 100 / 100 | rows and values match |
| q81 | OK | 50.2 | 47.9 | 29.2 | 1.64x | 34 / 34 | rows and values match |
| q82 | OK | 15.0 | 15.5 | 15.6 | 1.00x | 0 / 0 | rows and values match |
| q83 | OK | 103.1 | 95.7 | 62.9 | 1.52x | 0 / 0 | rows and values match |
| q84 | OK | 30.6 | 25.9 | 29.1 | 0.89x | 6 / 6 | rows and values match |
| q85 | OK | 135.6 | 128.0 | 78.5 | 1.63x | 0 / 0 | rows and values match |
| q86 | OK | 26.4 | 24.3 | 22.1 | 1.10x | 100 / 100 | rows and values match |
| q87 | OK | 43.0 | 42.2 | 42.4 | 0.99x | 1 / 1 | rows and values match |
| q88 | OK | 126.2 | 90.3 | 209.8 | 0.43x | 1 / 1 | rows and values match |
| q89 | OK | 33.7 | 32.9 | 33.7 | 0.97x | 100 / 100 | rows and values match |
| q90 | OK | 43.8 | 35.0 | 38.7 | 0.90x | 1 / 1 | rows and values match |
| q91 | OK | 46.5 | 41.2 | 57.4 | 0.72x | 0 / 0 | rows and values match |
| q92 | OK | 20.8 | 21.7 | 20.0 | 1.08x | 1 / 1 | rows and values match |
| q93 | OK | 11.0 | 9.7 | 4.1 | 2.35x | 0 / 0 | rows and values match |
| q94 | OK | 32.5 | 29.3 | 26.9 | 1.09x | 1 / 1 | rows and values match |
| q95 | OK | 28.6 | 27.6 | 30.0 | 0.92x | 1 / 1 | rows and values match |
| q96 | OK | 15.6 | 16.1 | 24.6 | 0.65x | 1 / 1 | rows and values match |
| q97 | OK | 26.1 | 24.0 | 28.3 | 0.85x | 1 / 1 | rows and values match |
| q98 | OK | 25.5 | 27.5 | 21.0 | 1.31x | 250 / 250 | rows and values match |
| q99 | OK | 21.9 | 20.8 | 20.5 | 1.01x | 6 / 6 | rows and values match |
