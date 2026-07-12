# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 00:19

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 28.0s)
Timing: ours 13.0s  duckdb 10.8s  ->  total 1.21x  geomean 1.06x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 13.0 | 10.8 | 1.21x | 1.06x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 223.4 | 58.3 | 54.5 | 1.07x | 100 / 100 | rows and values match |
| q02 | OK | 308.8 | 181.1 | 60.5 | 2.99x | 2513 / 2513 | rows and values match |
| q03 | OK | 43.6 | 22.3 | 29.1 | 0.77x | 89 / 89 | rows and values match |
| q04 | OK | 925.9 | 810.8 | 183.9 | 4.41x | 6 / 6 | rows and values match |
| q05 | OK | 80.7 | 56.6 | 41.8 | 1.35x | 100 / 100 | rows and values match |
| q06 | OK | 173.7 | 141.9 | 92.4 | 1.54x | 46 / 46 | rows and values match |
| q07 | OK | 116.8 | 100.5 | 176.9 | 0.57x | 100 / 100 | rows and values match |
| q08 | OK | 70.7 | 65.0 | 62.2 | 1.05x | 5 / 5 | rows and values match |
| q09 | OK | 56.5 | 53.7 | 38.8 | 1.38x | 1 / 1 | rows and values match |
| q10 | OK | 132.9 | 126.0 | 334.5 | 0.38x | 6 / 6 | rows and values match |
| q11 | OK | 461.4 | 454.6 | 129.5 | 3.51x | 90 / 90 | rows and values match |
| q12 | OK | 30.0 | 28.5 | 38.6 | 0.74x | 100 / 100 | rows and values match |
| q13 | OK | 195.9 | 167.7 | 192.1 | 0.87x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 94.0 | 86.7 | 51.2 | 1.69x | 100 / 100 | rows and values match |
| q16 | OK | 70.6 | 57.1 | 34.3 | 1.66x | 1 / 1 | rows and values match |
| q17 | OK | 75.2 | 67.5 | 67.9 | 0.99x | 0 / 0 | rows and values match |
| q18 | OK | 222.8 | 214.6 | 330.5 | 0.65x | 100 / 100 | rows and values match |
| q19 | OK | 96.4 | 96.8 | 60.0 | 1.61x | 100 / 100 | rows and values match |
| q20 | OK | 26.6 | 26.3 | 37.5 | 0.70x | 100 / 100 | rows and values match |
| q21 | OK | 30.1 | 20.3 | 26.9 | 0.76x | 100 / 100 | rows and values match |
| q22 | OK | 391.1 | 425.0 | 300.6 | 1.41x | 100 / 100 | rows and values match |
| q23 | OK | 917.9 | 878.1 | 296.4 | 2.96x | 4 / 4 | rows and values match |
| q24 | OK | 227.5 | 153.8 | 96.7 | 1.59x | 1 / 1 | rows and values match |
| q25 | OK | 65.4 | 62.3 | 71.6 | 0.87x | 1 / 1 | rows and values match |
| q26 | OK | 106.5 | 103.4 | 197.1 | 0.52x | 100 / 100 | rows and values match |
| q27 | OK | 155.0 | 162.0 | 190.7 | 0.85x | 100 / 100 | rows and values match |
| q28 | OK | 60.0 | 49.5 | 40.0 | 1.24x | 1 / 1 | rows and values match |
| q29 | OK | 62.2 | 61.1 | 119.3 | 0.51x | 1 / 1 | rows and values match |
| q30 | OK | 81.5 | 80.5 | 94.4 | 0.85x | 100 / 100 | rows and values match |
| q31 | OK | 123.8 | 126.0 | 71.6 | 1.76x | 44 / 44 | rows and values match |
| q32 | OK | 30.1 | 29.3 | 27.4 | 1.07x | 1 / 1 | rows and values match |
| q33 | OK | 73.9 | 73.5 | 108.3 | 0.68x | 100 / 100 | rows and values match |
| q34 | OK | 61.7 | 53.9 | 53.5 | 1.01x | 455 / 455 | rows and values match |
| q35 | OK | 400.6 | 383.1 | 287.3 | 1.33x | 100 / 100 | rows and values match |
| q36 | OK | 71.2 | 62.7 | 47.9 | 1.31x | 100 / 100 | rows and values match |
| q37 | OK | 26.4 | 16.7 | 27.1 | 0.62x | 1 / 1 | rows and values match |
| q38 | OK | 111.3 | 108.0 | 75.4 | 1.43x | 1 / 1 | rows and values match |
| q39 | OK | 175.0 | 167.6 | 52.6 | 3.19x | 243 / 243 | rows and values match |
| q40 | OK | 40.4 | 42.7 | 29.3 | 1.46x | 100 / 100 | rows and values match |
| q41 | OK | 19.0 | 19.8 | 22.5 | 0.88x | 4 / 4 | rows and values match |
| q42 | OK | 23.5 | 22.6 | 27.3 | 0.83x | 10 / 10 | rows and values match |
| q43 | OK | 29.9 | 31.6 | 33.8 | 0.93x | 6 / 6 | rows and values match |
| q44 | OK | 61.2 | 59.5 | 31.2 | 1.91x | 10 / 10 | rows and values match |
| q45 | OK | 394.1 | 404.1 | 63.6 | 6.35x | 19 / 19 | rows and values match |
| q46 | OK | 108.9 | 96.9 | 81.5 | 1.19x | 100 / 100 | rows and values match |
| q47 | OK | 330.3 | 311.2 | 123.6 | 2.52x | 100 / 100 | rows and values match |
| q48 | OK | 152.7 | 158.4 | 210.1 | 0.75x | 1 / 1 | rows and values match |
| q49 | OK | 74.2 | 65.7 | 44.5 | 1.48x | 34 / 34 | rows and values match |
| q50 | OK | 43.5 | 33.5 | 66.2 | 0.51x | 6 / 6 | rows and values match |
| q51 | OK | 202.9 | 210.6 | 191.4 | 1.10x | 100 / 100 | rows and values match |
| q52 | OK | 21.5 | 20.5 | 28.7 | 0.71x | 100 / 100 | rows and values match |
| q53 | OK | 40.1 | 41.0 | 38.2 | 1.07x | 100 / 100 | rows and values match |
| q54 | OK | 85.4 | 91.4 | 74.4 | 1.23x | 1 / 1 | rows and values match |
| q55 | OK | 20.4 | 20.2 | 34.7 | 0.58x | 100 / 100 | rows and values match |
| q56 | OK | 75.1 | 74.8 | 109.4 | 0.68x | 100 / 100 | rows and values match |
| q57 | OK | 155.8 | 152.3 | 85.3 | 1.79x | 100 / 100 | rows and values match |
| q58 | OK | 110.5 | 111.0 | 84.3 | 1.32x | 5 / 5 | rows and values match |
| q59 | OK | 304.0 | 255.2 | 96.3 | 2.65x | 100 / 100 | rows and values match |
| q60 | OK | 63.5 | 66.0 | 127.9 | 0.52x | 100 / 100 | rows and values match |
| q61 | OK | 85.5 | 82.5 | 83.4 | 0.99x | 1 / 1 | rows and values match |
| q62 | OK | 37.8 | 26.2 | 81.2 | 0.32x | 100 / 100 | rows and values match |
| q63 | OK | 38.5 | 46.8 | 48.5 | 0.97x | 100 / 100 | rows and values match |
| q64 | OK | 414.7 | 404.2 | 419.7 | 0.96x | 2 / 2 | rows and values match |
| q65 | OK | 75.4 | 70.0 | 64.8 | 1.08x | 100 / 100 | rows and values match |
| q66 | OK | 103.6 | 80.8 | 89.1 | 0.91x | 5 / 5 | rows and values match |
| q67 | OK | 478.1 | 473.7 | 443.2 | 1.07x | 100 / 100 | rows and values match |
| q68 | OK | 125.7 | 90.4 | 78.2 | 1.16x | 100 / 100 | rows and values match |
| q69 | OK | 113.9 | 110.3 | 277.8 | 0.40x | 100 / 100 | rows and values match |
| q70 | OK | 90.8 | 95.3 | 48.2 | 1.98x | 3 / 3 | rows and values match |
| q71 | OK | 70.0 | 55.8 | 76.7 | 0.73x | 1031 / 1031 | rows and values match |
| q72 | OK | 678.0 | 634.9 | 781.4 | 0.81x | 100 / 100 | rows and values match |
| q73 | OK | 45.9 | 39.1 | 59.2 | 0.66x | 1 / 1 | rows and values match |
| q74 | OK | 255.0 | 230.3 | 86.4 | 2.67x | 92 / 92 | rows and values match |
| q75 | OK | 228.2 | 225.1 | 96.6 | 2.33x | 100 / 100 | rows and values match |
| q76 | OK | 58.6 | 48.8 | 58.3 | 0.84x | 100 / 100 | rows and values match |
| q77 | OK | 67.5 | 56.7 | 46.5 | 1.22x | 44 / 44 | rows and values match |
| q78 | OK | 350.6 | 349.5 | 139.8 | 2.50x | 100 / 100 | rows and values match |
| q79 | OK | 54.9 | 55.2 | 49.7 | 1.11x | 100 / 100 | rows and values match |
| q80 | OK | 103.8 | 99.4 | 74.8 | 1.33x | 100 / 100 | rows and values match |
| q81 | OK | 113.6 | 112.2 | 106.6 | 1.05x | 100 / 100 | rows and values match |
| q82 | OK | 18.7 | 17.9 | 47.6 | 0.38x | 2 / 2 | rows and values match |
| q83 | OK | 78.6 | 75.0 | 78.7 | 0.95x | 24 / 24 | rows and values match |
| q84 | OK | 79.3 | 80.6 | 132.0 | 0.61x | 16 / 16 | rows and values match |
| q85 | OK | 397.0 | 372.2 | 346.7 | 1.07x | 1 / 1 | rows and values match |
| q86 | OK | 51.7 | 40.9 | 31.9 | 1.28x | 100 / 100 | rows and values match |
| q87 | OK | 138.9 | 101.1 | 84.5 | 1.20x | 1 / 1 | rows and values match |
| q88 | OK | 149.6 | 115.8 | 220.8 | 0.52x | 1 / 1 | rows and values match |
| q89 | OK | 46.9 | 49.6 | 38.0 | 1.31x | 100 / 100 | rows and values match |
| q90 | OK | 37.7 | 29.0 | 42.3 | 0.69x | 1 / 1 | rows and values match |
| q91 | OK | 122.9 | 116.2 | 195.3 | 0.59x | 2 / 2 | rows and values match |
| q92 | OK | 26.5 | 26.7 | 31.3 | 0.85x | 1 / 1 | rows and values match |
| q93 | OK | 27.5 | 23.4 | 30.1 | 0.78x | 100 / 100 | rows and values match |
| q94 | OK | 59.8 | 53.6 | 35.3 | 1.52x | 1 / 1 | rows and values match |
| q95 | OK | 289.0 | 251.7 | 197.5 | 1.27x | 1 / 1 | rows and values match |
| q96 | OK | 18.6 | 18.7 | 31.9 | 0.59x | 1 / 1 | rows and values match |
| q97 | OK | 59.4 | 59.3 | 60.0 | 0.99x | 1 / 1 | rows and values match |
| q98 | OK | 40.9 | 47.8 | 47.2 | 1.01x | 2521 / 2521 | rows and values match |
| q99 | OK | 39.5 | 33.1 | 112.7 | 0.29x | 90 / 90 | rows and values match |
