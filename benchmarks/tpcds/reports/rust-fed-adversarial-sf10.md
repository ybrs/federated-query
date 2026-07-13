# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 19:30

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 503.2s)
Timing: ours 238.6s  duckdb 71.4s  ->  total 3.34x  geomean 2.90x  (98 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 238.6 | 71.4 | 3.34x | 2.90x | 98 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 1081.2 | 964.0 | 102.7 | 9.38x | 100 / 100 | rows and values match |
| q02 | OK | 5175.7 | 4620.7 | 232.1 | 19.91x | 2513 / 2513 | rows and values match |
| q03 | OK | 114.8 | 47.9 | 56.3 | 0.85x | 100 / 100 | rows and values match |
| q04 | OK | 9401.2 | 9059.7 | 1294.3 | 7.00x | 100 / 100 | rows and values match |
| q05 | OK | 928.0 | 873.7 | 152.9 | 5.71x | 100 / 100 | rows and values match |
| q06 | OK | 1485.2 | 1227.3 | 205.5 | 5.97x | 51 / 51 | rows and values match |
| q07 | OK | 213.9 | 180.0 | 240.5 | 0.75x | 100 / 100 | rows and values match |
| q08 | OK | 560.0 | 549.9 | 154.7 | 3.55x | 9 / 9 | rows and values match |
| q09 | OK | 413.5 | 416.4 | 386.6 | 1.08x | 1 / 1 | rows and values match |
| q10 | OK | 742.9 | 717.0 | 457.8 | 1.57x | 100 / 100 | rows and values match |
| q11 | OK | 2313.2 | 1994.5 | 806.7 | 2.47x | 100 / 100 | rows and values match |
| q12 | OK | 57.7 | 47.0 | 66.5 | 0.71x | 100 / 100 | rows and values match |
| q13 | OK | 285.2 | 265.0 | 340.7 | 0.78x | 1 / 1 | rows and values match |
| q14 | OK | 12190.9 | 11757.8 | 3067.1 | 3.83x | 100 / 100 | rows and values match |
| q15 | OK | 1089.2 | 1067.0 | 128.4 | 8.31x | 100 / 100 | rows and values match |
| q16 | OK | 10322.0 | 10324.3 | 48.4 | 213.38x | 1 / 1 | rows and values match |
| q17 | OK | 1014.8 | 995.4 | 200.8 | 4.96x | 0 / 0 | rows and values match |
| q18 | OK | 2420.9 | 2846.1 | 422.6 | 6.74x | 100 / 100 | rows and values match |
| q19 | OK | 159.2 | 160.4 | 2255.7 | 0.07x | 100 / 100 | rows and values match |
| q20 | OK | 633.0 | 621.0 | 62.3 | 9.96x | 100 / 100 | rows and values match |
| q21 | OK | 6068.4 | 4393.4 | 57.0 | 77.09x | 100 / 100 | rows and values match |
| q22 | OK | 16349.2 | 15779.9 | 5831.2 | 2.71x | 100 / 100 | rows and values match |
| q23 | OK | 5408.7 | 5041.8 | 2941.9 | 1.71x | 100 / 100 | rows and values match |
| q24 | OK | 4808.9 | 4427.2 | 535.0 | 8.27x | 5 / 5 | rows and values match |
| q25 | OK | 1048.5 | 1026.2 | 142.0 | 7.22x | 2 / 2 | rows and values match |
| q26 | OK | 909.1 | 906.1 | 212.0 | 4.27x | 100 / 100 | rows and values match |
| q27 | OK | 351.6 | 294.2 | 246.8 | 1.19x | 100 / 100 | rows and values match |
| q28 | OK | 387.3 | 355.8 | 338.7 | 1.05x | 1 / 1 | rows and values match |
| q29 | OK | 1894.0 | 2215.7 | 205.6 | 10.77x | 8 / 8 | rows and values match |
| q30 | OK | 477.8 | 523.6 | 233.4 | 2.24x | 100 / 100 | rows and values match |
| q31 | OK | 199.7 | 186.6 | 170.0 | 1.10x | 307 / 307 | rows and values match |
| q32 | OK | 875.0 | 898.0 | 30.5 | 29.46x | 1 / 1 | rows and values match |
| q33 | OK | 766.9 | 758.3 | 173.0 | 4.38x | 100 / 100 | rows and values match |
| q34 | OK | 130.2 | 122.0 | 139.4 | 0.88x | 1560 / 1560 | rows and values match |
| q35 | OK | 1233.9 | 1200.0 | 506.2 | 2.37x | 100 / 100 | rows and values match |
| q36 | OK | 115.0 | 114.7 | 125.0 | 0.92x | 100 / 100 | rows and values match |
| q37 | OK | 5699.5 | 5720.9 | 170.7 | 33.51x | 2 / 2 | rows and values match |
| q38 | OK | 2197.2 | 2152.1 | 384.8 | 5.59x | 1 / 1 | rows and values match |
| q39 | OK | 74066.4 | 63188.9 | 410.9 | 153.78x | 2433 / 2433 | rows and values match |
| q40 | OK | 772.2 | 708.9 | 66.2 | 10.71x | 100 / 100 | rows and values match |
| q41 | OK | 19.2 | 20.5 | 47.2 | 0.43x | 41 / 41 | rows and values match |
| q42 | OK | 53.1 | 48.2 | 61.7 | 0.78x | 11 / 11 | rows and values match |
| q43 | OK | 95.4 | 89.5 | 100.2 | 0.89x | 18 / 18 | rows and values match |
| q44 | OK | 129.9 | 122.9 | 121.2 | 1.01x | 10 / 10 | rows and values match |
| q45 | OK | 173.7 | 164.3 | 143.6 | 1.14x | 49 / 49 | rows and values match |
| q46 | OK | 432.3 | 426.2 | 236.9 | 1.80x | 100 / 100 | rows and values match |
| q47 | OK | 1800.2 | 1686.2 | 917.2 | 1.84x | 100 / 100 | rows and values match |
| q48 | OK | 286.1 | 285.0 | 349.5 | 0.82x | 1 / 1 | rows and values match |
| q49 | OK | 2364.0 | 2328.5 | 96.8 | 24.06x | 46 / 46 | rows and values match |
| q50 | OK | 1549.6 | 1347.8 | 239.7 | 5.62x | 51 / 51 | rows and values match |
| q51 | OK | 1906.3 | 1809.6 | 1753.9 | 1.03x | 100 / 100 | rows and values match |
| q52 | OK | 66.7 | 52.8 | 66.2 | 0.80x | 100 / 100 | rows and values match |
| q53 | OK | 425.7 | 433.0 | 99.0 | 4.37x | 100 / 100 | rows and values match |
| q54 | OK | 841.0 | 743.2 | 189.4 | 3.92x | 1 / 1 | rows and values match |
| q55 | OK | 56.2 | 53.1 | 65.6 | 0.81x | 100 / 100 | rows and values match |
| q56 | OK | 732.3 | 739.2 | 176.8 | 4.18x | 100 / 100 | rows and values match |
| q57 | OK | 4676.0 | 4801.1 | 380.5 | 12.62x | 100 / 100 | rows and values match |
| q58 | OK | 869.8 | 795.6 | 149.5 | 5.32x | 31 / 31 | rows and values match |
| q59 | OK | 1757.7 | 1512.1 | 421.9 | 3.58x | 100 / 100 | rows and values match |
| q60 | OK | 812.1 | 858.8 | 181.9 | 4.72x | 100 / 100 | rows and values match |
| q61 | OK | 210.7 | 210.1 | 242.3 | 0.87x | 1 / 1 | rows and values match |
| q62 | OK | 66.4 | 51.7 | 468.0 | 0.11x | 100 / 100 | rows and values match |
| q63 | OK | 488.0 | 419.7 | 84.7 | 4.96x | 100 / 100 | rows and values match |
| q64 | OK | 3834.5 | 3689.7 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 765.9 | 584.9 | 508.5 | 1.15x | 100 / 100 | rows and values match |
| q66 | OK | 1176.1 | 1151.8 | 112.2 | 10.27x | 10 / 10 | rows and values match |
| q67 | OK | 3812.2 | 3735.7 | 5919.1 | 0.63x | 100 / 100 | rows and values match |
| q68 | OK | 394.1 | 333.7 | 232.1 | 1.44x | 100 / 100 | rows and values match |
| q69 | OK | 1100.8 | 1126.8 | 353.4 | 3.19x | 100 / 100 | rows and values match |
| q70 | OK | 658.0 | 561.5 | 187.9 | 2.99x | 7 / 7 | rows and values match |
| q71 | OK | 961.0 | 946.6 | 150.2 | 6.30x | 9655 / 9655 | rows and values match |
| q72 | OK | 14591.0 | 15096.4 | 24992.5 | 0.60x | 100 / 100 | rows and values match |
| q73 | OK | 105.8 | 98.2 | 130.4 | 0.75x | 17 / 17 | rows and values match |
| q74 | OK | 1418.9 | 1198.9 | 518.7 | 2.31x | 100 / 100 | rows and values match |
| q75 | OK | 4929.2 | 4972.0 | 498.3 | 9.98x | 100 / 100 | rows and values match |
| q76 | OK | 732.1 | 721.7 | 135.3 | 5.33x | 100 / 100 | rows and values match |
| q77 | OK | 833.8 | 793.8 | 97.5 | 8.14x | 100 / 100 | rows and values match |
| q78 | OK | 8329.7 | 8708.9 | 1234.4 | 7.05x | 100 / 100 | rows and values match |
| q79 | OK | 229.0 | 215.1 | 172.9 | 1.24x | 100 / 100 | rows and values match |
| q80 | OK | 2087.7 | 2079.8 | 247.2 | 8.41x | 100 / 100 | rows and values match |
| q81 | OK | 301.8 | 316.3 | 226.0 | 1.40x | 100 / 100 | rows and values match |
| q82 | OK | 3217.9 | 3389.0 | 320.9 | 10.56x | 6 / 6 | rows and values match |
| q83 | OK | 193.6 | 198.8 | 90.2 | 2.21x | 100 / 100 | rows and values match |
| q84 | OK | 552.8 | 533.8 | 193.3 | 2.76x | 100 / 100 | rows and values match |
| q85 | OK | 1085.9 | 1060.5 | 526.9 | 2.01x | 15 / 15 | rows and values match |
| q86 | OK | 155.2 | 132.6 | 72.3 | 1.84x | 100 / 100 | rows and values match |
| q87 | OK | 2344.1 | 2230.2 | 435.1 | 5.13x | 1 / 1 | rows and values match |
| q88 | OK | 395.1 | 370.7 | 448.4 | 0.83x | 1 / 1 | rows and values match |
| q89 | OK | 691.7 | 633.5 | 123.2 | 5.14x | 100 / 100 | rows and values match |
| q90 | OK | 55.6 | 45.3 | 63.3 | 0.72x | 1 / 1 | rows and values match |
| q91 | OK | 170.3 | 161.5 | 367.7 | 0.44x | 12 / 12 | rows and values match |
| q92 | OK | 46.5 | 41.7 | 41.9 | 1.00x | 1 / 1 | rows and values match |
| q93 | OK | 2636.5 | 2224.7 | 297.0 | 7.49x | 100 / 100 | rows and values match |
| q94 | OK | 329.7 | 320.3 | 67.7 | 4.73x | 1 / 1 | rows and values match |
| q95 | OK | 3511.9 | 2641.3 | 956.8 | 2.76x | 1 / 1 | rows and values match |
| q96 | OK | 47.1 | 48.3 | 54.0 | 0.90x | 1 / 1 | rows and values match |
| q97 | OK | 1904.0 | 2042.1 | 333.0 | 6.13x | 1 / 1 | rows and values match |
| q98 | OK | 175.0 | 159.2 | 144.1 | 1.10x | 15076 / 15076 | rows and values match |
| q99 | OK | 1889.9 | 1928.4 | 910.8 | 2.12x | 100 / 100 | rows and values match |
