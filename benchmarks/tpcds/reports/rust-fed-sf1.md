# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 17:39

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 25.0s)
Timing: ours 11.6s  duckdb 11.1s  ->  total 1.04x  geomean 1.00x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 11.6 | 11.1 | 1.04x | 1.00x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 202.3 | 55.5 | 54.5 | 1.02x | 100 / 100 | rows and values match |
| q02 | OK | 172.4 | 107.8 | 60.5 | 1.78x | 2513 / 2513 | rows and values match |
| q03 | OK | 36.3 | 20.4 | 29.1 | 0.70x | 89 / 89 | rows and values match |
| q04 | OK | 489.4 | 411.8 | 183.9 | 2.24x | 6 / 6 | rows and values match |
| q05 | OK | 73.9 | 61.2 | 41.8 | 1.46x | 100 / 100 | rows and values match |
| q06 | OK | 213.3 | 146.6 | 92.4 | 1.59x | 46 / 46 | rows and values match |
| q07 | OK | 123.8 | 107.3 | 176.9 | 0.61x | 100 / 100 | rows and values match |
| q08 | OK | 89.1 | 74.9 | 62.2 | 1.20x | 5 / 5 | rows and values match |
| q09 | OK | 63.0 | 56.0 | 38.8 | 1.44x | 1 / 1 | rows and values match |
| q10 | OK | 129.6 | 131.2 | 334.5 | 0.39x | 6 / 6 | rows and values match |
| q11 | OK | 271.0 | 261.0 | 129.5 | 2.01x | 90 / 90 | rows and values match |
| q12 | OK | 31.9 | 28.5 | 38.6 | 0.74x | 100 / 100 | rows and values match |
| q13 | OK | 187.5 | 173.4 | 192.1 | 0.90x | 1 / 1 | rows and values match |
| q14 | OK | 294.6 | 305.1 | 356.0 | 0.86x | 100 / 100 | rows and values match |
| q15 | OK | 108.9 | 90.7 | 51.2 | 1.77x | 100 / 100 | rows and values match |
| q16 | OK | 77.5 | 70.0 | 34.3 | 2.04x | 1 / 1 | rows and values match |
| q17 | OK | 93.0 | 67.2 | 67.9 | 0.99x | 0 / 0 | rows and values match |
| q18 | OK | 227.3 | 224.5 | 330.5 | 0.68x | 100 / 100 | rows and values match |
| q19 | OK | 115.0 | 107.5 | 60.0 | 1.79x | 100 / 100 | rows and values match |
| q20 | OK | 25.3 | 26.6 | 37.5 | 0.71x | 100 / 100 | rows and values match |
| q21 | OK | 28.7 | 20.2 | 26.9 | 0.75x | 100 / 100 | rows and values match |
| q22 | OK | 431.3 | 410.0 | 300.6 | 1.36x | 100 / 100 | rows and values match |
| q23 | OK | 568.2 | 558.1 | 296.4 | 1.88x | 4 / 4 | rows and values match |
| q24 | OK | 190.7 | 126.6 | 96.7 | 1.31x | 1 / 1 | rows and values match |
| q25 | OK | 69.8 | 63.0 | 71.6 | 0.88x | 1 / 1 | rows and values match |
| q26 | OK | 96.4 | 106.3 | 197.1 | 0.54x | 100 / 100 | rows and values match |
| q27 | OK | 142.8 | 134.9 | 190.7 | 0.71x | 100 / 100 | rows and values match |
| q28 | OK | 60.2 | 50.0 | 40.0 | 1.25x | 1 / 1 | rows and values match |
| q29 | OK | 65.4 | 62.1 | 119.3 | 0.52x | 1 / 1 | rows and values match |
| q30 | OK | 76.5 | 74.5 | 94.4 | 0.79x | 100 / 100 | rows and values match |
| q31 | OK | 103.5 | 93.8 | 71.6 | 1.31x | 44 / 44 | rows and values match |
| q32 | OK | 28.8 | 26.2 | 27.4 | 0.96x | 1 / 1 | rows and values match |
| q33 | OK | 71.5 | 77.6 | 108.3 | 0.72x | 100 / 100 | rows and values match |
| q34 | OK | 63.4 | 49.7 | 53.5 | 0.93x | 455 / 455 | rows and values match |
| q35 | OK | 390.8 | 386.3 | 287.3 | 1.34x | 100 / 100 | rows and values match |
| q36 | OK | 49.1 | 50.4 | 47.9 | 1.05x | 100 / 100 | rows and values match |
| q37 | OK | 24.4 | 16.2 | 27.1 | 0.60x | 1 / 1 | rows and values match |
| q38 | OK | 95.8 | 96.8 | 75.4 | 1.28x | 1 / 1 | rows and values match |
| q39 | OK | 155.9 | 149.2 | 52.6 | 2.84x | 243 / 243 | rows and values match |
| q40 | OK | 34.4 | 31.3 | 29.3 | 1.07x | 100 / 100 | rows and values match |
| q41 | OK | 17.2 | 16.4 | 22.5 | 0.73x | 4 / 4 | rows and values match |
| q42 | OK | 20.2 | 18.8 | 27.3 | 0.69x | 10 / 10 | rows and values match |
| q43 | OK | 31.2 | 26.4 | 33.8 | 0.78x | 6 / 6 | rows and values match |
| q44 | OK | 57.4 | 56.7 | 31.2 | 1.82x | 10 / 10 | rows and values match |
| q45 | OK | 106.4 | 93.6 | 63.6 | 1.47x | 19 / 19 | rows and values match |
| q46 | OK | 105.4 | 93.6 | 81.5 | 1.15x | 100 / 100 | rows and values match |
| q47 | OK | 155.6 | 148.5 | 123.6 | 1.20x | 100 / 100 | rows and values match |
| q48 | OK | 152.4 | 156.8 | 210.1 | 0.75x | 1 / 1 | rows and values match |
| q49 | OK | 75.5 | 66.4 | 44.5 | 1.49x | 34 / 34 | rows and values match |
| q50 | OK | 42.1 | 37.1 | 66.2 | 0.56x | 6 / 6 | rows and values match |
| q51 | OK | 224.2 | 207.5 | 191.4 | 1.08x | 100 / 100 | rows and values match |
| q52 | OK | 23.5 | 21.0 | 28.7 | 0.73x | 100 / 100 | rows and values match |
| q53 | OK | 39.4 | 38.4 | 38.2 | 1.01x | 100 / 100 | rows and values match |
| q54 | OK | 92.3 | 86.6 | 74.4 | 1.16x | 1 / 1 | rows and values match |
| q55 | OK | 21.7 | 20.9 | 34.7 | 0.60x | 100 / 100 | rows and values match |
| q56 | OK | 82.1 | 83.4 | 109.4 | 0.76x | 100 / 100 | rows and values match |
| q57 | OK | 91.1 | 85.5 | 85.3 | 1.00x | 100 / 100 | rows and values match |
| q58 | OK | 113.2 | 111.0 | 84.3 | 1.32x | 5 / 5 | rows and values match |
| q59 | OK | 222.3 | 197.2 | 96.3 | 2.05x | 100 / 100 | rows and values match |
| q60 | OK | 68.4 | 71.3 | 127.9 | 0.56x | 100 / 100 | rows and values match |
| q61 | OK | 85.8 | 80.3 | 83.4 | 0.96x | 1 / 1 | rows and values match |
| q62 | OK | 35.1 | 25.2 | 81.2 | 0.31x | 100 / 100 | rows and values match |
| q63 | OK | 42.7 | 47.6 | 48.5 | 0.98x | 100 / 100 | rows and values match |
| q64 | OK | 379.1 | 362.7 | 419.7 | 0.86x | 2 / 2 | rows and values match |
| q65 | OK | 78.7 | 68.9 | 64.8 | 1.06x | 100 / 100 | rows and values match |
| q66 | OK | 98.0 | 77.5 | 89.1 | 0.87x | 5 / 5 | rows and values match |
| q67 | OK | 499.1 | 509.8 | 443.2 | 1.15x | 100 / 100 | rows and values match |
| q68 | OK | 116.5 | 95.1 | 78.2 | 1.22x | 100 / 100 | rows and values match |
| q69 | OK | 125.4 | 134.5 | 277.8 | 0.48x | 100 / 100 | rows and values match |
| q70 | OK | 107.5 | 96.2 | 48.2 | 2.00x | 3 / 3 | rows and values match |
| q71 | OK | 60.1 | 56.2 | 76.7 | 0.73x | 1031 / 1031 | rows and values match |
| q72 | OK | 661.4 | 626.7 | 781.4 | 0.80x | 100 / 100 | rows and values match |
| q73 | OK | 44.5 | 40.3 | 59.2 | 0.68x | 1 / 1 | rows and values match |
| q74 | OK | 160.8 | 150.6 | 86.4 | 1.74x | 92 / 92 | rows and values match |
| q75 | OK | 188.3 | 189.2 | 96.6 | 1.96x | 100 / 100 | rows and values match |
| q76 | OK | 62.9 | 56.0 | 58.3 | 0.96x | 100 / 100 | rows and values match |
| q77 | OK | 67.3 | 60.0 | 46.5 | 1.29x | 44 / 44 | rows and values match |
| q78 | OK | 359.8 | 328.4 | 139.8 | 2.35x | 100 / 100 | rows and values match |
| q79 | OK | 63.6 | 61.6 | 49.7 | 1.24x | 100 / 100 | rows and values match |
| q80 | OK | 110.5 | 103.8 | 74.8 | 1.39x | 100 / 100 | rows and values match |
| q81 | OK | 121.5 | 113.3 | 106.6 | 1.06x | 100 / 100 | rows and values match |
| q82 | OK | 19.7 | 20.9 | 47.6 | 0.44x | 2 / 2 | rows and values match |
| q83 | OK | 83.6 | 92.8 | 78.7 | 1.18x | 24 / 24 | rows and values match |
| q84 | OK | 88.2 | 85.3 | 132.0 | 0.65x | 16 / 16 | rows and values match |
| q85 | OK | 419.2 | 409.0 | 346.7 | 1.18x | 1 / 1 | rows and values match |
| q86 | OK | 40.1 | 43.2 | 31.9 | 1.36x | 100 / 100 | rows and values match |
| q87 | OK | 106.2 | 104.3 | 84.5 | 1.24x | 1 / 1 | rows and values match |
| q88 | OK | 158.2 | 126.6 | 220.8 | 0.57x | 1 / 1 | rows and values match |
| q89 | OK | 55.5 | 55.3 | 38.0 | 1.45x | 100 / 100 | rows and values match |
| q90 | OK | 34.6 | 32.5 | 42.3 | 0.77x | 1 / 1 | rows and values match |
| q91 | OK | 125.7 | 117.9 | 195.3 | 0.60x | 2 / 2 | rows and values match |
| q92 | OK | 27.4 | 26.9 | 31.3 | 0.86x | 1 / 1 | rows and values match |
| q93 | OK | 24.3 | 23.1 | 30.1 | 0.77x | 100 / 100 | rows and values match |
| q94 | OK | 56.3 | 53.9 | 35.3 | 1.53x | 1 / 1 | rows and values match |
| q95 | OK | 216.6 | 217.4 | 197.5 | 1.10x | 1 / 1 | rows and values match |
| q96 | OK | 18.3 | 16.4 | 31.9 | 0.51x | 1 / 1 | rows and values match |
| q97 | OK | 58.1 | 61.2 | 60.0 | 1.02x | 1 / 1 | rows and values match |
| q98 | OK | 37.6 | 41.9 | 47.2 | 0.89x | 2521 / 2521 | rows and values match |
| q99 | OK | 36.1 | 38.3 | 112.7 | 0.34x | 90 / 90 | rows and values match |
