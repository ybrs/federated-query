# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:26

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 54.6s)
Timing: ours 13.1s  duckdb 10.8s  ->  total 1.22x  geomean 1.07x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 13.1 | 10.8 | 1.22x | 1.07x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 207.5 | 50.7 | 54.5 | 0.93x | 100 / 100 | rows and values match |
| q02 | OK | 284.0 | 164.0 | 60.5 | 2.71x | 2513 / 2513 | rows and values match |
| q03 | OK | 33.2 | 20.1 | 29.1 | 0.69x | 89 / 89 | rows and values match |
| q04 | OK | 903.0 | 801.1 | 183.9 | 4.36x | 6 / 6 | rows and values match |
| q05 | OK | 75.1 | 61.5 | 41.8 | 1.47x | 100 / 100 | rows and values match |
| q06 | OK | 177.0 | 163.2 | 92.4 | 1.77x | 46 / 46 | rows and values match |
| q07 | OK | 117.3 | 110.5 | 176.9 | 0.62x | 100 / 100 | rows and values match |
| q08 | OK | 72.4 | 72.3 | 62.2 | 1.16x | 5 / 5 | rows and values match |
| q09 | OK | 60.9 | 56.5 | 38.8 | 1.46x | 1 / 1 | rows and values match |
| q10 | OK | 127.7 | 122.2 | 334.5 | 0.37x | 6 / 6 | rows and values match |
| q11 | OK | 474.8 | 461.5 | 129.5 | 3.56x | 90 / 90 | rows and values match |
| q12 | OK | 30.0 | 27.1 | 38.6 | 0.70x | 100 / 100 | rows and values match |
| q13 | OK | 188.6 | 174.0 | 192.1 | 0.91x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 120.3 | 75.1 | 51.2 | 1.46x | 100 / 100 | rows and values match |
| q16 | OK | 65.5 | 56.5 | 34.3 | 1.64x | 1 / 1 | rows and values match |
| q17 | OK | 69.8 | 67.5 | 67.9 | 0.99x | 0 / 0 | rows and values match |
| q18 | OK | 211.0 | 207.3 | 330.5 | 0.63x | 100 / 100 | rows and values match |
| q19 | OK | 126.7 | 78.7 | 60.0 | 1.31x | 100 / 100 | rows and values match |
| q20 | OK | 30.8 | 30.8 | 37.5 | 0.82x | 100 / 100 | rows and values match |
| q21 | OK | 30.2 | 19.1 | 26.9 | 0.71x | 100 / 100 | rows and values match |
| q22 | OK | 391.8 | 405.7 | 300.6 | 1.35x | 100 / 100 | rows and values match |
| q23 | OK | 916.2 | 859.9 | 296.4 | 2.90x | 4 / 4 | rows and values match |
| q24 | OK | 223.3 | 157.4 | 96.7 | 1.63x | 1 / 1 | rows and values match |
| q25 | OK | 81.2 | 68.3 | 71.6 | 0.96x | 1 / 1 | rows and values match |
| q26 | OK | 115.4 | 105.8 | 197.1 | 0.54x | 100 / 100 | rows and values match |
| q27 | OK | 170.3 | 163.7 | 190.7 | 0.86x | 100 / 100 | rows and values match |
| q28 | OK | 71.3 | 55.9 | 40.0 | 1.40x | 1 / 1 | rows and values match |
| q29 | OK | 70.5 | 70.5 | 119.3 | 0.59x | 1 / 1 | rows and values match |
| q30 | OK | 91.4 | 92.2 | 94.4 | 0.98x | 100 / 100 | rows and values match |
| q31 | OK | 142.0 | 122.9 | 71.6 | 1.72x | 44 / 44 | rows and values match |
| q32 | OK | 25.5 | 28.1 | 27.4 | 1.02x | 1 / 1 | rows and values match |
| q33 | OK | 70.8 | 68.7 | 108.3 | 0.63x | 100 / 100 | rows and values match |
| q34 | OK | 61.3 | 52.1 | 53.5 | 0.97x | 455 / 455 | rows and values match |
| q35 | OK | 400.2 | 368.8 | 287.3 | 1.28x | 100 / 100 | rows and values match |
| q36 | OK | 59.4 | 56.9 | 47.9 | 1.19x | 100 / 100 | rows and values match |
| q37 | OK | 24.2 | 18.3 | 27.1 | 0.68x | 1 / 1 | rows and values match |
| q38 | OK | 95.5 | 92.0 | 75.4 | 1.22x | 1 / 1 | rows and values match |
| q39 | OK | 158.5 | 151.1 | 52.6 | 2.87x | 243 / 243 | rows and values match |
| q40 | OK | 36.6 | 35.0 | 29.3 | 1.19x | 100 / 100 | rows and values match |
| q41 | OK | 17.4 | 16.3 | 22.5 | 0.72x | 4 / 4 | rows and values match |
| q42 | OK | 23.7 | 22.1 | 27.3 | 0.81x | 10 / 10 | rows and values match |
| q43 | OK | 29.3 | 26.6 | 33.8 | 0.79x | 6 / 6 | rows and values match |
| q44 | OK | 56.6 | 52.6 | 31.2 | 1.69x | 10 / 10 | rows and values match |
| q45 | OK | 385.7 | 434.5 | 63.6 | 6.83x | 19 / 19 | rows and values match |
| q46 | OK | 110.0 | 91.6 | 81.5 | 1.12x | 100 / 100 | rows and values match |
| q47 | OK | 328.6 | 322.8 | 123.6 | 2.61x | 100 / 100 | rows and values match |
| q48 | OK | 156.9 | 153.5 | 210.1 | 0.73x | 1 / 1 | rows and values match |
| q49 | OK | 68.5 | 68.0 | 44.5 | 1.53x | 34 / 34 | rows and values match |
| q50 | OK | 41.5 | 37.1 | 66.2 | 0.56x | 6 / 6 | rows and values match |
| q51 | OK | 204.5 | 200.0 | 191.4 | 1.05x | 100 / 100 | rows and values match |
| q52 | OK | 20.8 | 20.6 | 28.7 | 0.72x | 100 / 100 | rows and values match |
| q53 | OK | 49.2 | 40.0 | 38.2 | 1.05x | 100 / 100 | rows and values match |
| q54 | OK | 89.8 | 85.9 | 74.4 | 1.15x | 1 / 1 | rows and values match |
| q55 | OK | 19.5 | 21.6 | 34.7 | 0.62x | 100 / 100 | rows and values match |
| q56 | OK | 78.6 | 78.0 | 109.4 | 0.71x | 100 / 100 | rows and values match |
| q57 | OK | 154.8 | 147.7 | 85.3 | 1.73x | 100 / 100 | rows and values match |
| q58 | OK | 114.0 | 113.0 | 84.3 | 1.34x | 5 / 5 | rows and values match |
| q59 | OK | 302.3 | 258.7 | 96.3 | 2.69x | 100 / 100 | rows and values match |
| q60 | OK | 71.5 | 66.9 | 127.9 | 0.52x | 100 / 100 | rows and values match |
| q61 | OK | 93.2 | 80.8 | 83.4 | 0.97x | 1 / 1 | rows and values match |
| q62 | OK | 36.2 | 28.1 | 81.2 | 0.35x | 100 / 100 | rows and values match |
| q63 | OK | 44.5 | 42.2 | 48.5 | 0.87x | 100 / 100 | rows and values match |
| q64 | OK | 422.5 | 411.3 | 419.7 | 0.98x | 2 / 2 | rows and values match |
| q65 | OK | 81.7 | 74.9 | 64.8 | 1.16x | 100 / 100 | rows and values match |
| q66 | OK | 113.2 | 83.9 | 89.1 | 0.94x | 5 / 5 | rows and values match |
| q67 | OK | 473.9 | 492.9 | 443.2 | 1.11x | 100 / 100 | rows and values match |
| q68 | OK | 106.5 | 96.9 | 78.2 | 1.24x | 100 / 100 | rows and values match |
| q69 | OK | 115.0 | 117.5 | 277.8 | 0.42x | 100 / 100 | rows and values match |
| q70 | OK | 106.7 | 97.3 | 48.2 | 2.02x | 3 / 3 | rows and values match |
| q71 | OK | 74.1 | 65.9 | 76.7 | 0.86x | 1031 / 1031 | rows and values match |
| q72 | OK | 689.4 | 644.0 | 781.4 | 0.82x | 100 / 100 | rows and values match |
| q73 | OK | 47.1 | 42.0 | 59.2 | 0.71x | 1 / 1 | rows and values match |
| q74 | OK | 247.7 | 241.8 | 86.4 | 2.80x | 92 / 92 | rows and values match |
| q75 | OK | 235.0 | 227.0 | 96.6 | 2.35x | 100 / 100 | rows and values match |
| q76 | OK | 62.8 | 53.1 | 58.3 | 0.91x | 100 / 100 | rows and values match |
| q77 | OK | 62.9 | 60.1 | 46.5 | 1.29x | 44 / 44 | rows and values match |
| q78 | OK | 335.6 | 354.8 | 139.8 | 2.54x | 100 / 100 | rows and values match |
| q79 | OK | 67.1 | 65.4 | 49.7 | 1.31x | 100 / 100 | rows and values match |
| q80 | OK | 110.1 | 105.4 | 74.8 | 1.41x | 100 / 100 | rows and values match |
| q81 | OK | 122.4 | 111.2 | 106.6 | 1.04x | 100 / 100 | rows and values match |
| q82 | OK | 22.2 | 19.4 | 47.6 | 0.41x | 2 / 2 | rows and values match |
| q83 | OK | 88.3 | 86.8 | 78.7 | 1.10x | 24 / 24 | rows and values match |
| q84 | OK | 82.5 | 81.1 | 132.0 | 0.61x | 16 / 16 | rows and values match |
| q85 | OK | 405.9 | 369.1 | 346.7 | 1.06x | 1 / 1 | rows and values match |
| q86 | OK | 58.9 | 45.0 | 31.9 | 1.41x | 100 / 100 | rows and values match |
| q87 | OK | 117.3 | 116.3 | 84.5 | 1.38x | 1 / 1 | rows and values match |
| q88 | OK | 160.6 | 137.1 | 220.8 | 0.62x | 1 / 1 | rows and values match |
| q89 | OK | 59.5 | 50.7 | 38.0 | 1.33x | 100 / 100 | rows and values match |
| q90 | OK | 35.1 | 28.2 | 42.3 | 0.67x | 1 / 1 | rows and values match |
| q91 | OK | 126.0 | 124.9 | 195.3 | 0.64x | 2 / 2 | rows and values match |
| q92 | OK | 28.3 | 28.1 | 31.3 | 0.90x | 1 / 1 | rows and values match |
| q93 | OK | 25.7 | 23.8 | 30.1 | 0.79x | 100 / 100 | rows and values match |
| q94 | OK | 59.5 | 55.0 | 35.3 | 1.56x | 1 / 1 | rows and values match |
| q95 | OK | 259.9 | 260.6 | 197.5 | 1.32x | 1 / 1 | rows and values match |
| q96 | OK | 18.8 | 19.0 | 31.9 | 0.60x | 1 / 1 | rows and values match |
| q97 | OK | 72.4 | 63.6 | 60.0 | 1.06x | 1 / 1 | rows and values match |
| q98 | OK | 43.8 | 44.5 | 47.2 | 0.94x | 2521 / 2521 | rows and values match |
| q99 | OK | 39.0 | 39.0 | 112.7 | 0.35x | 90 / 90 | rows and values match |
