# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 01:39

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 24.2s)
Timing: ours 11.2s  duckdb 10.8s  ->  total 1.04x  geomean 0.98x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 11.2 | 10.8 | 1.04x | 0.98x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 204.6 | 49.6 | 54.5 | 0.91x | 100 / 100 | rows and values match |
| q02 | OK | 158.4 | 109.4 | 60.5 | 1.81x | 2513 / 2513 | rows and values match |
| q03 | OK | 32.4 | 20.1 | 29.1 | 0.69x | 89 / 89 | rows and values match |
| q04 | OK | 481.0 | 392.0 | 183.9 | 2.13x | 6 / 6 | rows and values match |
| q05 | OK | 75.1 | 56.8 | 41.8 | 1.36x | 100 / 100 | rows and values match |
| q06 | OK | 192.0 | 135.8 | 92.4 | 1.47x | 46 / 46 | rows and values match |
| q07 | OK | 111.6 | 100.4 | 176.9 | 0.57x | 100 / 100 | rows and values match |
| q08 | OK | 70.7 | 67.8 | 62.2 | 1.09x | 5 / 5 | rows and values match |
| q09 | OK | 61.0 | 54.2 | 38.8 | 1.40x | 1 / 1 | rows and values match |
| q10 | OK | 126.8 | 119.6 | 334.5 | 0.36x | 6 / 6 | rows and values match |
| q11 | OK | 271.1 | 254.1 | 129.5 | 1.96x | 90 / 90 | rows and values match |
| q12 | OK | 29.9 | 28.4 | 38.6 | 0.74x | 100 / 100 | rows and values match |
| q13 | OK | 177.9 | 162.0 | 192.1 | 0.84x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 91.8 | 95.0 | 51.2 | 1.85x | 100 / 100 | rows and values match |
| q16 | OK | 67.1 | 53.2 | 34.3 | 1.55x | 1 / 1 | rows and values match |
| q17 | OK | 88.9 | 66.4 | 67.9 | 0.98x | 0 / 0 | rows and values match |
| q18 | OK | 218.6 | 208.7 | 330.5 | 0.63x | 100 / 100 | rows and values match |
| q19 | OK | 119.6 | 82.7 | 60.0 | 1.38x | 100 / 100 | rows and values match |
| q20 | OK | 31.6 | 35.4 | 37.5 | 0.94x | 100 / 100 | rows and values match |
| q21 | OK | 28.9 | 24.6 | 26.9 | 0.92x | 100 / 100 | rows and values match |
| q22 | OK | 408.5 | 408.4 | 300.6 | 1.36x | 100 / 100 | rows and values match |
| q23 | OK | 553.7 | 530.4 | 296.4 | 1.79x | 4 / 4 | rows and values match |
| q24 | OK | 179.4 | 121.8 | 96.7 | 1.26x | 1 / 1 | rows and values match |
| q25 | OK | 65.8 | 62.6 | 71.6 | 0.87x | 1 / 1 | rows and values match |
| q26 | OK | 98.7 | 96.9 | 197.1 | 0.49x | 100 / 100 | rows and values match |
| q27 | OK | 128.7 | 131.8 | 190.7 | 0.69x | 100 / 100 | rows and values match |
| q28 | OK | 59.0 | 49.1 | 40.0 | 1.23x | 1 / 1 | rows and values match |
| q29 | OK | 65.4 | 64.7 | 119.3 | 0.54x | 1 / 1 | rows and values match |
| q30 | OK | 77.2 | 71.0 | 94.4 | 0.75x | 100 / 100 | rows and values match |
| q31 | OK | 98.3 | 95.0 | 71.6 | 1.33x | 44 / 44 | rows and values match |
| q32 | OK | 22.5 | 23.0 | 27.4 | 0.84x | 1 / 1 | rows and values match |
| q33 | OK | 64.3 | 67.2 | 108.3 | 0.62x | 100 / 100 | rows and values match |
| q34 | OK | 56.8 | 48.4 | 53.5 | 0.90x | 455 / 455 | rows and values match |
| q35 | OK | 364.4 | 363.6 | 287.3 | 1.27x | 100 / 100 | rows and values match |
| q36 | OK | 50.3 | 46.0 | 47.9 | 0.96x | 100 / 100 | rows and values match |
| q37 | OK | 22.2 | 16.0 | 27.1 | 0.59x | 1 / 1 | rows and values match |
| q38 | OK | 96.1 | 95.3 | 75.4 | 1.26x | 1 / 1 | rows and values match |
| q39 | OK | 164.6 | 160.5 | 52.6 | 3.05x | 243 / 243 | rows and values match |
| q40 | OK | 38.8 | 42.5 | 29.3 | 1.45x | 100 / 100 | rows and values match |
| q41 | OK | 19.2 | 18.6 | 22.5 | 0.83x | 4 / 4 | rows and values match |
| q42 | OK | 23.6 | 21.2 | 27.3 | 0.78x | 10 / 10 | rows and values match |
| q43 | OK | 36.4 | 32.4 | 33.8 | 0.96x | 6 / 6 | rows and values match |
| q44 | OK | 62.7 | 55.8 | 31.2 | 1.79x | 10 / 10 | rows and values match |
| q45 | OK | 396.7 | 416.0 | 63.6 | 6.54x | 19 / 19 | rows and values match |
| q46 | OK | 109.2 | 90.9 | 81.5 | 1.12x | 100 / 100 | rows and values match |
| q47 | OK | 162.4 | 183.7 | 123.6 | 1.49x | 100 / 100 | rows and values match |
| q48 | OK | 148.8 | 146.7 | 210.1 | 0.70x | 1 / 1 | rows and values match |
| q49 | OK | 65.9 | 63.7 | 44.5 | 1.43x | 34 / 34 | rows and values match |
| q50 | OK | 40.5 | 34.3 | 66.2 | 0.52x | 6 / 6 | rows and values match |
| q51 | OK | 201.5 | 196.9 | 191.4 | 1.03x | 100 / 100 | rows and values match |
| q52 | OK | 23.6 | 20.5 | 28.7 | 0.71x | 100 / 100 | rows and values match |
| q53 | OK | 37.8 | 40.7 | 38.2 | 1.07x | 100 / 100 | rows and values match |
| q54 | OK | 89.9 | 81.9 | 74.4 | 1.10x | 1 / 1 | rows and values match |
| q55 | OK | 20.4 | 20.4 | 34.7 | 0.59x | 100 / 100 | rows and values match |
| q56 | OK | 76.7 | 80.6 | 109.4 | 0.74x | 100 / 100 | rows and values match |
| q57 | OK | 87.7 | 81.0 | 85.3 | 0.95x | 100 / 100 | rows and values match |
| q58 | OK | 110.3 | 107.9 | 84.3 | 1.28x | 5 / 5 | rows and values match |
| q59 | OK | 233.0 | 181.1 | 96.3 | 1.88x | 100 / 100 | rows and values match |
| q60 | OK | 67.0 | 65.7 | 127.9 | 0.51x | 100 / 100 | rows and values match |
| q61 | OK | 83.0 | 83.1 | 83.4 | 1.00x | 1 / 1 | rows and values match |
| q62 | OK | 34.9 | 25.7 | 81.2 | 0.32x | 100 / 100 | rows and values match |
| q63 | OK | 40.8 | 38.1 | 48.5 | 0.79x | 100 / 100 | rows and values match |
| q64 | OK | 368.0 | 340.8 | 419.7 | 0.81x | 2 / 2 | rows and values match |
| q65 | OK | 75.0 | 76.1 | 64.8 | 1.17x | 100 / 100 | rows and values match |
| q66 | OK | 112.2 | 81.7 | 89.1 | 0.92x | 5 / 5 | rows and values match |
| q67 | OK | 486.6 | 450.9 | 443.2 | 1.02x | 100 / 100 | rows and values match |
| q68 | OK | 115.9 | 94.2 | 78.2 | 1.20x | 100 / 100 | rows and values match |
| q69 | OK | 112.7 | 112.2 | 277.8 | 0.40x | 100 / 100 | rows and values match |
| q70 | OK | 102.7 | 101.9 | 48.2 | 2.12x | 3 / 3 | rows and values match |
| q71 | OK | 55.9 | 54.5 | 76.7 | 0.71x | 1031 / 1031 | rows and values match |
| q72 | OK | 665.9 | 627.7 | 781.4 | 0.80x | 100 / 100 | rows and values match |
| q73 | OK | 42.5 | 44.7 | 59.2 | 0.75x | 1 / 1 | rows and values match |
| q74 | OK | 153.6 | 144.0 | 86.4 | 1.67x | 92 / 92 | rows and values match |
| q75 | OK | 183.1 | 176.5 | 96.6 | 1.83x | 100 / 100 | rows and values match |
| q76 | OK | 58.7 | 53.2 | 58.3 | 0.91x | 100 / 100 | rows and values match |
| q77 | OK | 61.3 | 55.7 | 46.5 | 1.20x | 44 / 44 | rows and values match |
| q78 | OK | 342.2 | 332.0 | 139.8 | 2.38x | 100 / 100 | rows and values match |
| q79 | OK | 55.4 | 51.3 | 49.7 | 1.03x | 100 / 100 | rows and values match |
| q80 | OK | 101.0 | 94.2 | 74.8 | 1.26x | 100 / 100 | rows and values match |
| q81 | OK | 112.5 | 102.2 | 106.6 | 0.96x | 100 / 100 | rows and values match |
| q82 | OK | 18.2 | 18.7 | 47.6 | 0.39x | 2 / 2 | rows and values match |
| q83 | OK | 78.7 | 77.6 | 78.7 | 0.99x | 24 / 24 | rows and values match |
| q84 | OK | 79.9 | 76.8 | 132.0 | 0.58x | 16 / 16 | rows and values match |
| q85 | OK | 367.4 | 376.8 | 346.7 | 1.09x | 1 / 1 | rows and values match |
| q86 | OK | 38.5 | 38.3 | 31.9 | 1.20x | 100 / 100 | rows and values match |
| q87 | OK | 119.6 | 103.9 | 84.5 | 1.23x | 1 / 1 | rows and values match |
| q88 | OK | 147.6 | 113.7 | 220.8 | 0.51x | 1 / 1 | rows and values match |
| q89 | OK | 47.5 | 49.9 | 38.0 | 1.31x | 100 / 100 | rows and values match |
| q90 | OK | 34.7 | 27.8 | 42.3 | 0.66x | 1 / 1 | rows and values match |
| q91 | OK | 119.3 | 117.4 | 195.3 | 0.60x | 2 / 2 | rows and values match |
| q92 | OK | 26.7 | 25.7 | 31.3 | 0.82x | 1 / 1 | rows and values match |
| q93 | OK | 24.2 | 21.6 | 30.1 | 0.72x | 100 / 100 | rows and values match |
| q94 | OK | 65.0 | 57.0 | 35.3 | 1.61x | 1 / 1 | rows and values match |
| q95 | OK | 282.8 | 233.2 | 197.5 | 1.18x | 1 / 1 | rows and values match |
| q96 | OK | 18.2 | 17.7 | 31.9 | 0.55x | 1 / 1 | rows and values match |
| q97 | OK | 62.1 | 61.4 | 60.0 | 1.02x | 1 / 1 | rows and values match |
| q98 | OK | 43.1 | 44.6 | 47.2 | 0.95x | 2521 / 2521 | rows and values match |
| q99 | OK | 38.8 | 32.8 | 112.7 | 0.29x | 90 / 90 | rows and values match |
