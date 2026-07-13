# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 20:55

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 139.5s)
Timing: ours 65.1s  duckdb 71.4s  ->  total 0.91x  geomean 1.25x  (98 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 65.1 | 71.4 | 0.91x | 1.25x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 299.0 | 132.6 | 102.7 | 1.29x | 100 / 100 | rows and values match |
| q02 | OK | 825.5 | 595.8 | 232.1 | 2.57x | 2513 / 2513 | rows and values match |
| q03 | OK | 91.2 | 59.2 | 56.3 | 1.05x | 100 / 100 | rows and values match |
| q04 | OK | 4444.3 | 4002.3 | 1294.3 | 3.09x | 100 / 100 | rows and values match |
| q05 | OK | 215.6 | 171.0 | 152.9 | 1.12x | 100 / 100 | rows and values match |
| q06 | OK | 1850.0 | 1418.4 | 205.5 | 6.90x | 51 / 51 | rows and values match |
| q07 | OK | 261.1 | 202.6 | 240.5 | 0.84x | 100 / 100 | rows and values match |
| q08 | OK | 262.1 | 244.9 | 154.7 | 1.58x | 9 / 9 | rows and values match |
| q09 | OK | 372.7 | 376.6 | 386.6 | 0.97x | 1 / 1 | rows and values match |
| q10 | OK | 252.7 | 246.5 | 457.8 | 0.54x | 100 / 100 | rows and values match |
| q11 | OK | 2387.5 | 2034.0 | 806.7 | 2.52x | 100 / 100 | rows and values match |
| q12 | OK | 77.8 | 71.4 | 66.5 | 1.07x | 100 / 100 | rows and values match |
| q13 | OK | 356.2 | 338.2 | 340.7 | 0.99x | 1 / 1 | rows and values match |
| q14 | OK | 2250.9 | 2122.0 | 3067.1 | 0.69x | 100 / 100 | rows and values match |
| q15 | OK | 491.7 | 618.5 | 128.4 | 4.82x | 100 / 100 | rows and values match |
| q16 | OK | 219.2 | 181.3 | 48.4 | 3.75x | 1 / 1 | rows and values match |
| q17 | OK | 239.3 | 225.2 | 200.8 | 1.12x | 0 / 0 | rows and values match |
| q18 | OK | 680.4 | 676.8 | 422.6 | 1.60x | 100 / 100 | rows and values match |
| q19 | OK | 489.9 | 501.9 | 2255.7 | 0.22x | 100 / 100 | rows and values match |
| q20 | OK | 86.3 | 85.4 | 62.3 | 1.37x | 100 / 100 | rows and values match |
| q21 | OK | 91.8 | 66.8 | 57.0 | 1.17x | 100 / 100 | rows and values match |
| q22 | OK | 2716.8 | 2655.3 | 5831.2 | 0.46x | 100 / 100 | rows and values match |
| q23 | OK | 4517.4 | 4390.7 | 2941.9 | 1.49x | 100 / 100 | rows and values match |
| q24 | OK | 2029.9 | 1379.2 | 535.0 | 2.58x | 5 / 5 | rows and values match |
| q25 | OK | 201.0 | 163.1 | 142.0 | 1.15x | 2 / 2 | rows and values match |
| q26 | OK | 134.3 | 135.4 | 212.0 | 0.64x | 100 / 100 | rows and values match |
| q27 | OK | 365.0 | 291.7 | 246.8 | 1.18x | 100 / 100 | rows and values match |
| q28 | OK | 367.5 | 343.9 | 338.7 | 1.02x | 1 / 1 | rows and values match |
| q29 | OK | 197.2 | 192.4 | 205.6 | 0.94x | 8 / 8 | rows and values match |
| q30 | OK | 206.3 | 213.8 | 233.4 | 0.92x | 100 / 100 | rows and values match |
| q31 | OK | 477.7 | 419.2 | 170.0 | 2.47x | 307 / 307 | rows and values match |
| q32 | OK | 48.4 | 47.2 | 30.5 | 1.55x | 1 / 1 | rows and values match |
| q33 | OK | 211.3 | 206.2 | 173.0 | 1.19x | 100 / 100 | rows and values match |
| q34 | OK | 144.3 | 132.7 | 139.4 | 0.95x | 1560 / 1560 | rows and values match |
| q35 | OK | 670.2 | 736.4 | 506.2 | 1.45x | 100 / 100 | rows and values match |
| q36 | OK | 182.0 | 166.4 | 125.0 | 1.33x | 100 / 100 | rows and values match |
| q37 | OK | 50.0 | 44.6 | 170.7 | 0.26x | 2 / 2 | rows and values match |
| q38 | OK | 682.9 | 587.6 | 384.8 | 1.53x | 1 / 1 | rows and values match |
| q39 | OK | 1969.5 | 1798.2 | 410.9 | 4.38x | 2433 / 2433 | rows and values match |
| q40 | OK | 162.1 | 125.7 | 66.2 | 1.90x | 100 / 100 | rows and values match |
| q41 | OK | 48.8 | 52.2 | 47.2 | 1.11x | 41 / 41 | rows and values match |
| q42 | OK | 81.2 | 106.6 | 61.7 | 1.73x | 11 / 11 | rows and values match |
| q43 | OK | 115.5 | 119.6 | 100.2 | 1.19x | 18 / 18 | rows and values match |
| q44 | OK | 152.9 | 150.8 | 121.2 | 1.24x | 10 / 10 | rows and values match |
| q45 | OK | 527.8 | 520.8 | 143.6 | 3.63x | 49 / 49 | rows and values match |
| q46 | OK | 502.6 | 477.7 | 236.9 | 2.02x | 100 / 100 | rows and values match |
| q47 | OK | 1144.4 | 1055.4 | 917.2 | 1.15x | 100 / 100 | rows and values match |
| q48 | OK | 390.7 | 381.8 | 349.5 | 1.09x | 1 / 1 | rows and values match |
| q49 | OK | 161.8 | 137.9 | 96.8 | 1.42x | 46 / 46 | rows and values match |
| q50 | OK | 123.2 | 116.8 | 239.7 | 0.49x | 51 / 51 | rows and values match |
| q51 | OK | 1849.2 | 1732.7 | 1753.9 | 0.99x | 100 / 100 | rows and values match |
| q52 | OK | 67.7 | 76.5 | 66.2 | 1.16x | 100 / 100 | rows and values match |
| q53 | OK | 154.8 | 162.2 | 99.0 | 1.64x | 100 / 100 | rows and values match |
| q54 | OK | 242.9 | 228.3 | 189.4 | 1.21x | 1 / 1 | rows and values match |
| q55 | OK | 70.2 | 76.2 | 65.6 | 1.16x | 100 / 100 | rows and values match |
| q56 | OK | 201.0 | 193.9 | 176.8 | 1.10x | 100 / 100 | rows and values match |
| q57 | OK | 626.8 | 619.2 | 380.5 | 1.63x | 100 / 100 | rows and values match |
| q58 | OK | 225.0 | 183.5 | 149.5 | 1.23x | 31 / 31 | rows and values match |
| q59 | OK | 1921.3 | 1479.6 | 421.9 | 3.51x | 100 / 100 | rows and values match |
| q60 | OK | 253.0 | 224.0 | 181.9 | 1.23x | 100 / 100 | rows and values match |
| q61 | OK | 349.5 | 312.7 | 242.3 | 1.29x | 1 / 1 | rows and values match |
| q62 | OK | 71.6 | 56.7 | 468.0 | 0.12x | 100 / 100 | rows and values match |
| q63 | OK | 157.7 | 161.9 | 84.7 | 1.91x | 100 / 100 | rows and values match |
| q64 | OK | 793.5 | 786.3 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1401.9 | 1262.3 | 508.5 | 2.48x | 100 / 100 | rows and values match |
| q66 | OK | 150.2 | 117.9 | 112.2 | 1.05x | 10 / 10 | rows and values match |
| q67 | OK | 4022.5 | 3987.3 | 5919.1 | 0.67x | 100 / 100 | rows and values match |
| q68 | OK | 387.3 | 404.2 | 232.1 | 1.74x | 100 / 100 | rows and values match |
| q69 | OK | 531.3 | 508.8 | 353.4 | 1.44x | 100 / 100 | rows and values match |
| q70 | OK | 1069.9 | 1012.0 | 187.9 | 5.39x | 7 / 7 | rows and values match |
| q71 | OK | 163.2 | 170.1 | 150.2 | 1.13x | 9655 / 9655 | rows and values match |
| q72 | OK | 6716.8 | 6053.6 | 24992.5 | 0.24x | 100 / 100 | rows and values match |
| q73 | OK | 138.1 | 153.3 | 130.4 | 1.18x | 17 / 17 | rows and values match |
| q74 | OK | 1522.4 | 1208.5 | 518.7 | 2.33x | 100 / 100 | rows and values match |
| q75 | OK | 1060.1 | 1058.5 | 498.3 | 2.12x | 100 / 100 | rows and values match |
| q76 | OK | 350.1 | 310.3 | 135.3 | 2.29x | 100 / 100 | rows and values match |
| q77 | OK | 231.8 | 171.4 | 97.5 | 1.76x | 100 / 100 | rows and values match |
| q78 | OK | 4063.9 | 3729.8 | 1234.4 | 3.02x | 100 / 100 | rows and values match |
| q79 | OK | 234.9 | 234.3 | 172.9 | 1.36x | 100 / 100 | rows and values match |
| q80 | OK | 570.5 | 552.8 | 247.2 | 2.24x | 100 / 100 | rows and values match |
| q81 | OK | 424.7 | 480.7 | 226.0 | 2.13x | 100 / 100 | rows and values match |
| q82 | OK | 62.8 | 64.9 | 320.9 | 0.20x | 6 / 6 | rows and values match |
| q83 | OK | 77.0 | 79.7 | 90.2 | 0.88x | 100 / 100 | rows and values match |
| q84 | OK | 119.7 | 106.4 | 193.3 | 0.55x | 100 / 100 | rows and values match |
| q85 | OK | 534.4 | 539.4 | 526.9 | 1.02x | 15 / 15 | rows and values match |
| q86 | OK | 193.1 | 159.1 | 72.3 | 2.20x | 100 / 100 | rows and values match |
| q87 | OK | 763.0 | 756.4 | 435.1 | 1.74x | 1 / 1 | rows and values match |
| q88 | OK | 430.2 | 403.0 | 448.4 | 0.90x | 1 / 1 | rows and values match |
| q89 | OK | 343.8 | 295.8 | 123.2 | 2.40x | 100 / 100 | rows and values match |
| q90 | OK | 52.8 | 44.5 | 63.3 | 0.70x | 1 / 1 | rows and values match |
| q91 | OK | 163.6 | 173.7 | 367.7 | 0.47x | 12 / 12 | rows and values match |
| q92 | OK | 57.7 | 56.6 | 41.9 | 1.35x | 1 / 1 | rows and values match |
| q93 | OK | 107.2 | 113.7 | 297.0 | 0.38x | 100 / 100 | rows and values match |
| q94 | OK | 165.8 | 142.5 | 67.7 | 2.11x | 1 / 1 | rows and values match |
| q95 | OK | 3648.4 | 2964.5 | 956.8 | 3.10x | 1 / 1 | rows and values match |
| q96 | OK | 51.2 | 55.3 | 54.0 | 1.02x | 1 / 1 | rows and values match |
| q97 | OK | 495.9 | 425.8 | 333.0 | 1.28x | 1 / 1 | rows and values match |
| q98 | OK | 202.2 | 180.8 | 144.1 | 1.25x | 15076 / 15076 | rows and values match |
| q99 | OK | 120.3 | 84.0 | 910.8 | 0.09x | 100 / 100 | rows and values match |
