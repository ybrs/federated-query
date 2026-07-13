# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 02:18

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 23.8s)
Timing: ours 11.0s  duckdb 10.8s  ->  total 1.02x  geomean 0.97x  (98 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 11.0 | 10.8 | 1.02x | 0.97x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 207.8 | 49.1 | 54.5 | 0.90x | 100 / 100 | rows and values match |
| q02 | OK | 183.3 | 106.1 | 60.5 | 1.75x | 2513 / 2513 | rows and values match |
| q03 | OK | 46.9 | 21.3 | 29.1 | 0.73x | 89 / 89 | rows and values match |
| q04 | OK | 508.3 | 399.7 | 183.9 | 2.17x | 6 / 6 | rows and values match |
| q05 | OK | 82.6 | 57.9 | 41.8 | 1.39x | 100 / 100 | rows and values match |
| q06 | OK | 227.4 | 178.8 | 92.4 | 1.94x | 46 / 46 | rows and values match |
| q07 | OK | 115.5 | 104.0 | 176.9 | 0.59x | 100 / 100 | rows and values match |
| q08 | OK | 71.8 | 65.9 | 62.2 | 1.06x | 5 / 5 | rows and values match |
| q09 | OK | 57.6 | 58.1 | 38.8 | 1.50x | 1 / 1 | rows and values match |
| q10 | OK | 137.6 | 129.1 | 334.5 | 0.39x | 6 / 6 | rows and values match |
| q11 | OK | 286.6 | 256.9 | 129.5 | 1.98x | 90 / 90 | rows and values match |
| q12 | OK | 29.3 | 27.1 | 38.6 | 0.70x | 100 / 100 | rows and values match |
| q13 | OK | 191.3 | 164.9 | 192.1 | 0.86x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 83.0 | 80.5 | 51.2 | 1.57x | 100 / 100 | rows and values match |
| q16 | OK | 68.2 | 56.9 | 34.3 | 1.66x | 1 / 1 | rows and values match |
| q17 | OK | 88.4 | 70.3 | 67.9 | 1.04x | 0 / 0 | rows and values match |
| q18 | OK | 211.1 | 213.2 | 330.5 | 0.65x | 100 / 100 | rows and values match |
| q19 | OK | 92.3 | 75.5 | 60.0 | 1.26x | 100 / 100 | rows and values match |
| q20 | OK | 28.5 | 27.6 | 37.5 | 0.73x | 100 / 100 | rows and values match |
| q21 | OK | 30.9 | 21.0 | 26.9 | 0.78x | 100 / 100 | rows and values match |
| q22 | OK | 379.7 | 381.4 | 300.6 | 1.27x | 100 / 100 | rows and values match |
| q23 | OK | 570.5 | 555.0 | 296.4 | 1.87x | 4 / 4 | rows and values match |
| q24 | OK | 195.0 | 123.4 | 96.7 | 1.28x | 1 / 1 | rows and values match |
| q25 | OK | 69.1 | 66.2 | 71.6 | 0.92x | 1 / 1 | rows and values match |
| q26 | OK | 99.7 | 97.6 | 197.1 | 0.50x | 100 / 100 | rows and values match |
| q27 | OK | 138.3 | 154.6 | 190.7 | 0.81x | 100 / 100 | rows and values match |
| q28 | OK | 59.8 | 50.2 | 40.0 | 1.25x | 1 / 1 | rows and values match |
| q29 | OK | 60.1 | 60.2 | 119.3 | 0.50x | 1 / 1 | rows and values match |
| q30 | OK | 78.4 | 71.1 | 94.4 | 0.75x | 100 / 100 | rows and values match |
| q31 | OK | 98.0 | 95.3 | 71.6 | 1.33x | 44 / 44 | rows and values match |
| q32 | OK | 27.1 | 24.1 | 27.4 | 0.88x | 1 / 1 | rows and values match |
| q33 | OK | 63.6 | 65.9 | 108.3 | 0.61x | 100 / 100 | rows and values match |
| q34 | OK | 57.3 | 48.0 | 53.5 | 0.90x | 455 / 455 | rows and values match |
| q35 | OK | 365.8 | 352.3 | 287.3 | 1.23x | 100 / 100 | rows and values match |
| q36 | OK | 51.1 | 49.8 | 47.9 | 1.04x | 100 / 100 | rows and values match |
| q37 | OK | 24.1 | 17.3 | 27.1 | 0.64x | 1 / 1 | rows and values match |
| q38 | OK | 97.3 | 96.3 | 75.4 | 1.28x | 1 / 1 | rows and values match |
| q39 | OK | 153.9 | 150.3 | 52.6 | 2.86x | 243 / 243 | rows and values match |
| q40 | OK | 36.3 | 31.4 | 29.3 | 1.07x | 100 / 100 | rows and values match |
| q41 | OK | 19.2 | 16.3 | 22.5 | 0.73x | 4 / 4 | rows and values match |
| q42 | OK | 21.6 | 19.1 | 27.3 | 0.70x | 10 / 10 | rows and values match |
| q43 | OK | 30.1 | 27.0 | 33.8 | 0.80x | 6 / 6 | rows and values match |
| q44 | OK | 57.5 | 51.3 | 31.2 | 1.65x | 10 / 10 | rows and values match |
| q45 | OK | 121.2 | 92.0 | 63.6 | 1.45x | 19 / 19 | rows and values match |
| q46 | OK | 104.9 | 92.0 | 81.5 | 1.13x | 100 / 100 | rows and values match |
| q47 | OK | 159.2 | 152.5 | 123.6 | 1.23x | 100 / 100 | rows and values match |
| q48 | OK | 150.3 | 147.7 | 210.1 | 0.70x | 1 / 1 | rows and values match |
| q49 | OK | 70.9 | 66.2 | 44.5 | 1.49x | 34 / 34 | rows and values match |
| q50 | OK | 44.3 | 34.0 | 66.2 | 0.51x | 6 / 6 | rows and values match |
| q51 | OK | 213.4 | 207.5 | 191.4 | 1.08x | 100 / 100 | rows and values match |
| q52 | OK | 21.7 | 21.6 | 28.7 | 0.75x | 100 / 100 | rows and values match |
| q53 | OK | 39.3 | 39.7 | 38.2 | 1.04x | 100 / 100 | rows and values match |
| q54 | OK | 89.6 | 88.1 | 74.4 | 1.18x | 1 / 1 | rows and values match |
| q55 | OK | 20.1 | 19.9 | 34.7 | 0.57x | 100 / 100 | rows and values match |
| q56 | OK | 77.5 | 78.3 | 109.4 | 0.72x | 100 / 100 | rows and values match |
| q57 | OK | 84.1 | 93.9 | 85.3 | 1.10x | 100 / 100 | rows and values match |
| q58 | OK | 124.2 | 140.1 | 84.3 | 1.66x | 5 / 5 | rows and values match |
| q59 | OK | 244.6 | 196.8 | 96.3 | 2.04x | 100 / 100 | rows and values match |
| q60 | OK | 70.3 | 65.4 | 127.9 | 0.51x | 100 / 100 | rows and values match |
| q61 | OK | 83.7 | 87.7 | 83.4 | 1.05x | 1 / 1 | rows and values match |
| q62 | OK | 37.0 | 27.0 | 81.2 | 0.33x | 100 / 100 | rows and values match |
| q63 | OK | 40.2 | 39.0 | 48.5 | 0.81x | 100 / 100 | rows and values match |
| q64 | OK | 364.4 | 360.7 | 419.7 | 0.86x | 2 / 2 | rows and values match |
| q65 | OK | 75.5 | 69.9 | 64.8 | 1.08x | 100 / 100 | rows and values match |
| q66 | OK | 108.4 | 87.3 | 89.1 | 0.98x | 5 / 5 | rows and values match |
| q67 | OK | 476.0 | 461.9 | 443.2 | 1.04x | 100 / 100 | rows and values match |
| q68 | OK | 98.2 | 86.9 | 78.2 | 1.11x | 100 / 100 | rows and values match |
| q69 | OK | 122.9 | 116.4 | 277.8 | 0.42x | 100 / 100 | rows and values match |
| q70 | OK | 102.9 | 95.1 | 48.2 | 1.98x | 3 / 3 | rows and values match |
| q71 | OK | 57.8 | 57.3 | 76.7 | 0.75x | 1031 / 1031 | rows and values match |
| q72 | OK | 695.9 | 705.7 | 781.4 | 0.90x | 100 / 100 | rows and values match |
| q73 | OK | 54.0 | 45.7 | 59.2 | 0.77x | 1 / 1 | rows and values match |
| q74 | OK | 156.7 | 142.8 | 86.4 | 1.65x | 92 / 92 | rows and values match |
| q75 | OK | 183.2 | 167.5 | 96.6 | 1.73x | 100 / 100 | rows and values match |
| q76 | OK | 57.0 | 51.9 | 58.3 | 0.89x | 100 / 100 | rows and values match |
| q77 | OK | 65.5 | 56.9 | 46.5 | 1.22x | 44 / 44 | rows and values match |
| q78 | OK | 369.9 | 333.8 | 139.8 | 2.39x | 100 / 100 | rows and values match |
| q79 | OK | 54.3 | 52.5 | 49.7 | 1.05x | 100 / 100 | rows and values match |
| q80 | OK | 96.5 | 93.5 | 74.8 | 1.25x | 100 / 100 | rows and values match |
| q81 | OK | 104.5 | 101.7 | 106.6 | 0.95x | 100 / 100 | rows and values match |
| q82 | OK | 18.9 | 19.9 | 47.6 | 0.42x | 2 / 2 | rows and values match |
| q83 | OK | 78.8 | 76.3 | 78.7 | 0.97x | 24 / 24 | rows and values match |
| q84 | OK | 79.2 | 74.4 | 132.0 | 0.56x | 16 / 16 | rows and values match |
| q85 | OK | 379.1 | 363.6 | 346.7 | 1.05x | 1 / 1 | rows and values match |
| q86 | OK | 39.4 | 37.0 | 31.9 | 1.16x | 100 / 100 | rows and values match |
| q87 | OK | 98.6 | 94.3 | 84.5 | 1.12x | 1 / 1 | rows and values match |
| q88 | OK | 149.8 | 125.9 | 220.8 | 0.57x | 1 / 1 | rows and values match |
| q89 | OK | 46.7 | 46.1 | 38.0 | 1.21x | 100 / 100 | rows and values match |
| q90 | OK | 40.1 | 31.8 | 42.3 | 0.75x | 1 / 1 | rows and values match |
| q91 | OK | 126.2 | 121.3 | 195.3 | 0.62x | 2 / 2 | rows and values match |
| q92 | OK | 25.8 | 27.8 | 31.3 | 0.89x | 1 / 1 | rows and values match |
| q93 | OK | 26.0 | 22.4 | 30.1 | 0.75x | 100 / 100 | rows and values match |
| q94 | OK | 57.2 | 53.7 | 35.3 | 1.52x | 1 / 1 | rows and values match |
| q95 | OK | 224.6 | 208.6 | 197.5 | 1.06x | 1 / 1 | rows and values match |
| q96 | OK | 17.5 | 17.3 | 31.9 | 0.54x | 1 / 1 | rows and values match |
| q97 | OK | 56.8 | 57.7 | 60.0 | 0.96x | 1 / 1 | rows and values match |
| q98 | OK | 38.8 | 39.1 | 47.2 | 0.83x | 2521 / 2521 | rows and values match |
| q99 | OK | 40.3 | 36.7 | 112.7 | 0.33x | 90 / 90 | rows and values match |
