# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 19:18

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 7.4s)
Timing: ours 6.7s  duckdb 4.1s  ->  total 1.65x  geomean 1.59x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 6.7 | 4.1 | 1.65x | 1.59x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 156.1 | - | 22.1 | 7.06x | 100 / 100 | rows and values match |
| q02 | OK | 156.1 | - | 42.0 | 3.72x | 2513 / 2513 | rows and values match |
| q03 | OK | 27.7 | - | 19.1 | 1.45x | 15 / 15 | rows and values match |
| q04 | OK | 160.9 | - | 68.5 | 2.35x | 0 / 0 | rows and values match |
| q05 | OK | 91.3 | - | 34.2 | 2.67x | 100 / 100 | rows and values match |
| q06 | OK | 100.9 | - | 44.1 | 2.29x | 10 / 10 | rows and values match |
| q07 | OK | 40.9 | - | 72.6 | 0.56x | 100 / 100 | rows and values match |
| q08 | OK | 57.5 | - | 35.2 | 1.63x | 0 / 0 | rows and values match |
| q09 | OK | 12.2 | - | 8.3 | 1.47x | 1 / 1 | rows and values match |
| q10 | OK | 67.7 | - | 99.6 | 0.68x | 1 / 1 | rows and values match |
| q11 | OK | 82.9 | - | 52.3 | 1.59x | 4 / 4 | rows and values match |
| q12 | OK | 26.7 | - | 18.3 | 1.46x | 100 / 100 | rows and values match |
| q13 | OK | 83.4 | - | 54.8 | 1.52x | 1 / 1 | rows and values match |
| q14 | OK | 261.9 | - | 94.7 | 2.77x | 100 / 100 | rows and values match |
| q15 | OK | 45.4 | - | 25.5 | 1.78x | 33 / 33 | rows and values match |
| q16 | OK | 62.9 | - | 27.8 | 2.26x | 1 / 1 | rows and values match |
| q17 | OK | 77.3 | - | 38.5 | 2.01x | 1 / 1 | rows and values match |
| q18 | OK | 79.6 | - | 77.1 | 1.03x | 100 / 100 | rows and values match |
| q19 | OK | 36.5 | - | 35.4 | 1.03x | 10 / 10 | rows and values match |
| q20 | OK | 32.7 | - | 21.1 | 1.55x | 100 / 100 | rows and values match |
| q21 | OK | 51.4 | - | 17.5 | 2.94x | 32 / 32 | rows and values match |
| q22 | OK | 52.0 | - | 25.5 | 2.04x | 100 / 100 | rows and values match |
| q23 | OK | 153.9 | - | 72.1 | 2.14x | 1 / 1 | rows and values match |
| q24 | OK | 60.7 | - | 15.8 | 3.85x | 0 / 0 | rows and values match |
| q25 | OK | 67.3 | - | 36.3 | 1.85x | 0 / 0 | rows and values match |
| q26 | OK | 57.3 | - | 66.8 | 0.86x | 100 / 100 | rows and values match |
| q27 | OK | 46.1 | - | 57.4 | 0.80x | 100 / 100 | rows and values match |
| q28 | OK | 27.6 | - | 10.7 | 2.57x | 1 / 1 | rows and values match |
| q29 | OK | 67.5 | - | 50.2 | 1.34x | 0 / 0 | rows and values match |
| q30 | OK | 50.0 | - | 32.2 | 1.55x | 21 / 21 | rows and values match |
| q31 | OK | 80.8 | - | 39.5 | 2.05x | 1 / 1 | rows and values match |
| q32 | OK | 12.7 | - | 22.2 | 0.57x | 1 / 1 | rows and values match |
| q33 | OK | 97.9 | - | 40.3 | 2.43x | 100 / 100 | rows and values match |
| q34 | OK | 40.0 | - | 26.8 | 1.49x | 53 / 53 | rows and values match |
| q35 | OK | 62.7 | - | 96.2 | 0.65x | 100 / 100 | rows and values match |
| q36 | OK | 31.0 | - | 27.3 | 1.14x | 100 / 100 | rows and values match |
| q37 | OK | 232.5 | - | 16.4 | 14.14x | 0 / 0 | rows and values match |
| q38 | OK | 49.7 | - | 42.0 | 1.18x | 1 / 1 | rows and values match |
| q39 | OK | 151.1 | - | 20.8 | 7.27x | 5 / 5 | rows and values match |
| q40 | OK | 40.0 | - | 17.1 | 2.34x | 44 / 44 | rows and values match |
| q41 | OK | 8.2 | - | 7.1 | 1.15x | 0 / 0 | rows and values match |
| q42 | OK | 18.2 | - | 17.9 | 1.02x | 4 / 4 | rows and values match |
| q43 | OK | 22.9 | - | 25.1 | 0.91x | 1 / 1 | rows and values match |
| q44 | OK | 31.5 | - | 6.5 | 4.86x | 0 / 0 | rows and values match |
| q45 | OK | 44.6 | - | 34.0 | 1.31x | 24 / 24 | rows and values match |
| q46 | OK | 54.4 | - | 39.6 | 1.37x | 100 / 100 | rows and values match |
| q47 | OK | 64.6 | - | 39.8 | 1.62x | 100 / 100 | rows and values match |
| q48 | OK | 80.7 | - | 59.7 | 1.35x | 1 / 1 | rows and values match |
| q49 | OK | 79.6 | - | 33.6 | 2.37x | 2 / 2 | rows and values match |
| q50 | OK | 44.8 | - | 34.4 | 1.30x | 1 / 1 | rows and values match |
| q51 | OK | 49.3 | - | 51.6 | 0.96x | 100 / 100 | rows and values match |
| q52 | OK | 18.6 | - | 17.1 | 1.09x | 11 / 11 | rows and values match |
| q53 | OK | 32.1 | - | 23.8 | 1.35x | 100 / 100 | rows and values match |
| q54 | OK | 77.3 | - | 54.3 | 1.43x | 0 / 0 | rows and values match |
| q55 | OK | 18.5 | - | 18.2 | 1.02x | 20 / 20 | rows and values match |
| q56 | OK | 94.1 | - | 40.8 | 2.31x | 38 / 38 | rows and values match |
| q57 | OK | 82.7 | - | 48.1 | 1.72x | 100 / 100 | rows and values match |
| q58 | OK | 125.0 | - | 63.0 | 1.98x | 0 / 0 | rows and values match |
| q59 | OK | 85.7 | - | 52.2 | 1.64x | 100 / 100 | rows and values match |
| q60 | OK | 93.2 | - | 41.8 | 2.23x | 100 / 100 | rows and values match |
| q61 | OK | 53.0 | - | 37.9 | 1.40x | 1 / 1 | rows and values match |
| q62 | OK | 42.0 | - | 28.8 | 1.46x | 6 / 6 | rows and values match |
| q63 | OK | 30.2 | - | 24.4 | 1.24x | 100 / 100 | rows and values match |
| q64 | OK | 188.4 | - | 93.0 | 2.03x | 0 / 0 | rows and values match |
| q65 | OK | 42.0 | - | 27.9 | 1.50x | 0 / 0 | rows and values match |
| q66 | OK | 142.8 | - | 70.3 | 2.03x | 1 / 1 | rows and values match |
| q67 | OK | 60.9 | - | 77.1 | 0.79x | 100 / 100 | rows and values match |
| q68 | OK | 51.5 | - | 36.5 | 1.41x | 100 / 100 | rows and values match |
| q69 | OK | 65.8 | - | 81.0 | 0.81x | 71 / 71 | rows and values match |
| q70 | OK | 50.1 | - | 28.9 | 1.73x | 3 / 3 | rows and values match |
| q71 | OK | 62.5 | - | 59.7 | 1.05x | 56 / 56 | rows and values match |
| q72 | OK | 169.8 | - | 71.4 | 2.38x | 50 / 50 | rows and values match |
| q73 | OK | 30.4 | - | 33.2 | 0.91x | 0 / 0 | rows and values match |
| q74 | OK | 61.1 | - | 40.4 | 1.51x | 3 / 3 | rows and values match |
| q75 | OK | 113.2 | - | 37.1 | 3.05x | 25 / 25 | rows and values match |
| q76 | OK | 54.9 | - | 35.3 | 1.56x | 100 / 100 | rows and values match |
| q77 | OK | 53.2 | - | 35.3 | 1.51x | 10 / 10 | rows and values match |
| q78 | OK | 126.1 | - | 39.8 | 3.17x | 100 / 100 | rows and values match |
| q79 | OK | 39.9 | - | 27.9 | 1.43x | 100 / 100 | rows and values match |
| q80 | OK | 137.3 | - | 46.0 | 2.99x | 100 / 100 | rows and values match |
| q81 | OK | 47.6 | - | 29.2 | 1.63x | 34 / 34 | rows and values match |
| q82 | OK | 13.1 | - | 15.6 | 0.84x | 0 / 0 | rows and values match |
| q83 | OK | 66.6 | - | 62.9 | 1.06x | 0 / 0 | rows and values match |
| q84 | OK | 38.3 | - | 29.1 | 1.32x | 6 / 6 | rows and values match |
| q85 | OK | 139.7 | - | 78.5 | 1.78x | 0 / 0 | rows and values match |
| q86 | OK | 28.6 | - | 22.1 | 1.29x | 100 / 100 | rows and values match |
| q87 | OK | 47.6 | - | 42.4 | 1.12x | 1 / 1 | rows and values match |
| q88 | OK | 134.5 | - | 209.8 | 0.64x | 1 / 1 | rows and values match |
| q89 | OK | 35.9 | - | 33.7 | 1.06x | 100 / 100 | rows and values match |
| q90 | OK | 37.1 | - | 38.7 | 0.96x | 1 / 1 | rows and values match |
| q91 | OK | 73.5 | - | 57.4 | 1.28x | 0 / 0 | rows and values match |
| q92 | OK | 23.4 | - | 20.0 | 1.17x | 1 / 1 | rows and values match |
| q93 | OK | 32.6 | - | 4.1 | 7.85x | 0 / 0 | rows and values match |
| q94 | OK | 39.3 | - | 26.9 | 1.46x | 1 / 1 | rows and values match |
| q95 | OK | 29.1 | - | 30.0 | 0.97x | 1 / 1 | rows and values match |
| q96 | OK | 18.2 | - | 24.6 | 0.74x | 1 / 1 | rows and values match |
| q97 | OK | 35.9 | - | 28.3 | 1.27x | 1 / 1 | rows and values match |
| q98 | OK | 23.3 | - | 21.0 | 1.11x | 250 / 250 | rows and values match |
| q99 | OK | 48.6 | - | 20.5 | 2.36x | 6 / 6 | rows and values match |
