# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 20:56

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 60.2s)
Timing: ours 29.0s  duckdb 11.1s  ->  total 2.61x  geomean 1.79x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 29.0 | 11.1 | 2.61x | 1.79x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 267.4 | 112.6 | 54.5 | 2.06x | 100 / 100 | rows and values match |
| q02 | OK | 486.6 | 375.7 | 60.5 | 6.21x | 2513 / 2513 | rows and values match |
| q03 | OK | 51.1 | 28.1 | 29.1 | 0.96x | 89 / 89 | rows and values match |
| q04 | OK | 1077.7 | 1132.6 | 183.9 | 6.16x | 6 / 6 | rows and values match |
| q05 | OK | 172.0 | 134.9 | 41.8 | 3.23x | 100 / 100 | rows and values match |
| q06 | OK | 157.2 | 126.7 | 92.4 | 1.37x | 46 / 46 | rows and values match |
| q07 | OK | 120.3 | 95.1 | 176.9 | 0.54x | 100 / 100 | rows and values match |
| q08 | OK | 193.0 | 175.7 | 62.2 | 2.82x | 5 / 5 | rows and values match |
| q09 | OK | 48.4 | 45.8 | 38.8 | 1.18x | 1 / 1 | rows and values match |
| q10 | OK | 203.9 | 170.4 | 334.5 | 0.51x | 6 / 6 | rows and values match |
| q11 | OK | 274.6 | 283.8 | 129.5 | 2.19x | 90 / 90 | rows and values match |
| q12 | OK | 32.9 | 23.5 | 38.6 | 0.61x | 100 / 100 | rows and values match |
| q13 | OK | 183.5 | 164.3 | 192.1 | 0.86x | 1 / 1 | rows and values match |
| q14 | OK | 1275.5 | 1317.5 | 356.0 | 3.70x | 100 / 100 | rows and values match |
| q15 | OK | 214.0 | 183.0 | 51.2 | 3.57x | 100 / 100 | rows and values match |
| q16 | OK | 332.7 | 324.3 | 34.3 | 9.44x | 1 / 1 | rows and values match |
| q17 | OK | 140.9 | 142.2 | 67.9 | 2.09x | 0 / 0 | rows and values match |
| q18 | OK | 415.1 | 398.1 | 330.5 | 1.20x | 100 / 100 | rows and values match |
| q19 | OK | 57.7 | 54.9 | 60.0 | 0.92x | 100 / 100 | rows and values match |
| q20 | OK | 91.6 | 93.5 | 37.5 | 2.49x | 100 / 100 | rows and values match |
| q21 | OK | 561.1 | 343.9 | 26.9 | 12.80x | 100 / 100 | rows and values match |
| q22 | OK | 1366.1 | 1353.4 | 300.6 | 4.50x | 100 / 100 | rows and values match |
| q23 | OK | 649.7 | 575.9 | 296.4 | 1.94x | 4 / 4 | rows and values match |
| q24 | OK | 545.7 | 558.9 | 96.7 | 5.78x | 1 / 1 | rows and values match |
| q25 | OK | 139.4 | 131.8 | 71.6 | 1.84x | 1 / 1 | rows and values match |
| q26 | OK | 303.4 | 292.0 | 197.1 | 1.48x | 100 / 100 | rows and values match |
| q27 | OK | 160.2 | 137.1 | 190.7 | 0.72x | 100 / 100 | rows and values match |
| q28 | OK | 50.8 | 47.9 | 40.0 | 1.20x | 1 / 1 | rows and values match |
| q29 | OK | 188.9 | 194.8 | 119.3 | 1.63x | 1 / 1 | rows and values match |
| q30 | OK | 116.8 | 109.7 | 94.4 | 1.16x | 100 / 100 | rows and values match |
| q31 | OK | 100.4 | 104.3 | 71.6 | 1.46x | 44 / 44 | rows and values match |
| q32 | OK | 125.9 | 146.2 | 27.4 | 5.33x | 1 / 1 | rows and values match |
| q33 | OK | 159.4 | 143.7 | 108.3 | 1.33x | 100 / 100 | rows and values match |
| q34 | OK | 56.1 | 49.5 | 53.5 | 0.92x | 455 / 455 | rows and values match |
| q35 | OK | 501.8 | 472.2 | 287.3 | 1.64x | 100 / 100 | rows and values match |
| q36 | OK | 53.4 | 46.6 | 47.9 | 0.97x | 100 / 100 | rows and values match |
| q37 | OK | 576.6 | 579.6 | 27.1 | 21.40x | 1 / 1 | rows and values match |
| q38 | OK | 332.0 | 318.9 | 75.4 | 4.23x | 1 / 1 | rows and values match |
| q39 | OK | 6139.5 | 6143.9 | 52.6 | 116.75x | 243 / 243 | rows and values match |
| q40 | OK | 158.1 | 162.3 | 29.3 | 5.54x | 100 / 100 | rows and values match |
| q41 | OK | 12.3 | 11.8 | 22.5 | 0.52x | 4 / 4 | rows and values match |
| q42 | OK | 21.2 | 19.9 | 27.3 | 0.73x | 10 / 10 | rows and values match |
| q43 | OK | 32.3 | 33.2 | 33.8 | 0.98x | 6 / 6 | rows and values match |
| q44 | OK | 54.4 | 60.1 | 31.2 | 1.92x | 10 / 10 | rows and values match |
| q45 | OK | 68.1 | 64.1 | 63.6 | 1.01x | 19 / 19 | rows and values match |
| q46 | OK | 114.0 | 122.4 | 81.5 | 1.50x | 100 / 100 | rows and values match |
| q47 | OK | 185.7 | 182.3 | 123.6 | 1.48x | 100 / 100 | rows and values match |
| q48 | OK | 152.0 | 137.3 | 210.1 | 0.65x | 1 / 1 | rows and values match |
| q49 | OK | 303.9 | 291.4 | 44.5 | 6.55x | 34 / 34 | rows and values match |
| q50 | OK | 184.0 | 121.6 | 66.2 | 1.84x | 6 / 6 | rows and values match |
| q51 | OK | 207.2 | 207.1 | 191.4 | 1.08x | 100 / 100 | rows and values match |
| q52 | OK | 20.5 | 20.1 | 28.7 | 0.70x | 100 / 100 | rows and values match |
| q53 | OK | 40.7 | 38.5 | 38.2 | 1.01x | 100 / 100 | rows and values match |
| q54 | OK | 200.6 | 191.0 | 74.4 | 2.57x | 1 / 1 | rows and values match |
| q55 | OK | 23.5 | 21.6 | 34.7 | 0.62x | 100 / 100 | rows and values match |
| q56 | OK | 144.3 | 144.9 | 109.4 | 1.32x | 100 / 100 | rows and values match |
| q57 | OK | 488.7 | 487.7 | 85.3 | 5.72x | 100 / 100 | rows and values match |
| q58 | OK | 195.6 | 193.6 | 84.3 | 2.30x | 5 / 5 | rows and values match |
| q59 | OK | 233.1 | 188.6 | 96.3 | 1.96x | 100 / 100 | rows and values match |
| q60 | OK | 156.8 | 153.8 | 127.9 | 1.20x | 100 / 100 | rows and values match |
| q61 | OK | 73.2 | 67.1 | 83.4 | 0.80x | 1 / 1 | rows and values match |
| q62 | OK | 35.2 | 26.4 | 81.2 | 0.33x | 100 / 100 | rows and values match |
| q63 | OK | 39.2 | 47.2 | 48.5 | 0.97x | 100 / 100 | rows and values match |
| q64 | OK | 735.5 | 697.4 | 419.7 | 1.66x | 2 / 2 | rows and values match |
| q65 | OK | 75.1 | 61.5 | 64.8 | 0.95x | 100 / 100 | rows and values match |
| q66 | OK | 270.9 | 233.3 | 89.1 | 2.62x | 5 / 5 | rows and values match |
| q67 | OK | 319.1 | 308.6 | 443.2 | 0.70x | 100 / 100 | rows and values match |
| q68 | OK | 102.6 | 101.2 | 78.2 | 1.29x | 100 / 100 | rows and values match |
| q69 | OK | 489.4 | 504.2 | 277.8 | 1.82x | 100 / 100 | rows and values match |
| q70 | OK | 82.0 | 81.4 | 48.2 | 1.69x | 3 / 3 | rows and values match |
| q71 | OK | 142.5 | 156.7 | 76.7 | 2.04x | 1031 / 1031 | rows and values match |
| q72 | OK | 1523.4 | 1481.1 | 781.4 | 1.90x | 100 / 100 | rows and values match |
| q73 | OK | 45.5 | 38.4 | 59.2 | 0.65x | 1 / 1 | rows and values match |
| q74 | OK | 166.1 | 155.7 | 86.4 | 1.80x | 92 / 92 | rows and values match |
| q75 | OK | 596.2 | 657.9 | 96.6 | 6.81x | 100 / 100 | rows and values match |
| q76 | OK | 154.0 | 130.7 | 58.3 | 2.24x | 100 / 100 | rows and values match |
| q77 | OK | 168.3 | 148.9 | 46.5 | 3.21x | 44 / 44 | rows and values match |
| q78 | OK | 811.2 | 779.6 | 139.8 | 5.58x | 100 / 100 | rows and values match |
| q79 | OK | 54.5 | 54.7 | 49.7 | 1.10x | 100 / 100 | rows and values match |
| q80 | OK | 307.1 | 287.5 | 74.8 | 3.85x | 100 / 100 | rows and values match |
| q81 | OK | 99.2 | 95.2 | 106.6 | 0.89x | 100 / 100 | rows and values match |
| q82 | OK | 322.6 | 354.2 | 47.6 | 7.44x | 2 / 2 | rows and values match |
| q83 | OK | 90.2 | 92.6 | 78.7 | 1.18x | 24 / 24 | rows and values match |
| q84 | OK | 110.1 | 103.9 | 132.0 | 0.79x | 16 / 16 | rows and values match |
| q85 | OK | 356.5 | 341.8 | 346.7 | 0.99x | 1 / 1 | rows and values match |
| q86 | OK | 42.0 | 36.0 | 31.9 | 1.13x | 100 / 100 | rows and values match |
| q87 | OK | 371.9 | 346.0 | 84.5 | 4.10x | 1 / 1 | rows and values match |
| q88 | OK | 146.2 | 117.0 | 220.8 | 0.53x | 1 / 1 | rows and values match |
| q89 | OK | 54.6 | 56.8 | 38.0 | 1.50x | 100 / 100 | rows and values match |
| q90 | OK | 39.9 | 35.3 | 42.3 | 0.83x | 1 / 1 | rows and values match |
| q91 | OK | 130.7 | 135.9 | 195.3 | 0.70x | 2 / 2 | rows and values match |
| q92 | OK | 28.6 | 27.6 | 31.3 | 0.88x | 1 / 1 | rows and values match |
| q93 | OK | 262.8 | 200.9 | 30.1 | 6.68x | 100 / 100 | rows and values match |
| q94 | OK | 70.8 | 64.7 | 35.3 | 1.83x | 1 / 1 | rows and values match |
| q95 | OK | 308.5 | 232.3 | 197.5 | 1.18x | 1 / 1 | rows and values match |
| q96 | OK | 19.8 | 17.4 | 31.9 | 0.54x | 1 / 1 | rows and values match |
| q97 | OK | 217.0 | 222.4 | 60.0 | 3.70x | 1 / 1 | rows and values match |
| q98 | OK | 36.7 | 43.7 | 47.2 | 0.93x | 2521 / 2521 | rows and values match |
| q99 | OK | 507.4 | 514.7 | 112.7 | 4.57x | 90 / 90 | rows and values match |
