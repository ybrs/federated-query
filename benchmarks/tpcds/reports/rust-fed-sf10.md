# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 19:21

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 138.0s)
Timing: ours 63.1s  duckdb 70.9s  ->  total 0.89x  geomean 1.22x  (97 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: planning budget exceeded: N.Nms > Nms budget; killed after optimize (parse N.Nms, bind N.Nms, decorrelate N.Nms, optimize N.Nms) - planning must be O(metadata); raise optimizer.planning_budget_ms only for a justified edge case (1) [ERROR]
Queries: q88

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 63.1 | 70.9 | 0.89x | 1.22x | 97 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 330.7 | 158.0 | 102.7 | 1.54x | 100 / 100 | rows and values match |
| q02 | OK | 896.6 | 601.2 | 232.1 | 2.59x | 2513 / 2513 | rows and values match |
| q03 | OK | 173.2 | 61.3 | 56.3 | 1.09x | 100 / 100 | rows and values match |
| q04 | OK | 4927.5 | 3996.5 | 1294.3 | 3.09x | 100 / 100 | rows and values match |
| q05 | OK | 267.2 | 169.7 | 152.9 | 1.11x | 100 / 100 | rows and values match |
| q06 | OK | 1817.0 | 1367.1 | 205.5 | 6.65x | 51 / 51 | rows and values match |
| q07 | OK | 319.9 | 194.3 | 240.5 | 0.81x | 100 / 100 | rows and values match |
| q08 | OK | 249.1 | 234.2 | 154.7 | 1.51x | 9 / 9 | rows and values match |
| q09 | OK | 384.5 | 357.5 | 386.6 | 0.92x | 1 / 1 | rows and values match |
| q10 | OK | 245.6 | 241.0 | 457.8 | 0.53x | 100 / 100 | rows and values match |
| q11 | OK | 2275.9 | 1999.9 | 806.7 | 2.48x | 100 / 100 | rows and values match |
| q12 | OK | 78.2 | 74.8 | 66.5 | 1.13x | 100 / 100 | rows and values match |
| q13 | OK | 387.4 | 326.0 | 340.7 | 0.96x | 1 / 1 | rows and values match |
| q14 | OK | 2316.8 | 1998.5 | 3067.1 | 0.65x | 100 / 100 | rows and values match |
| q15 | OK | 491.7 | 616.4 | 128.4 | 4.80x | 100 / 100 | rows and values match |
| q16 | OK | 193.7 | 160.5 | 48.4 | 3.32x | 1 / 1 | rows and values match |
| q17 | OK | 222.4 | 201.2 | 200.8 | 1.00x | 0 / 0 | rows and values match |
| q18 | OK | 678.3 | 702.6 | 422.6 | 1.66x | 100 / 100 | rows and values match |
| q19 | OK | 491.2 | 436.1 | 2255.7 | 0.19x | 100 / 100 | rows and values match |
| q20 | OK | 83.8 | 76.9 | 62.3 | 1.23x | 100 / 100 | rows and values match |
| q21 | OK | 77.1 | 57.5 | 57.0 | 1.01x | 100 / 100 | rows and values match |
| q22 | OK | 2889.2 | 2732.2 | 5831.2 | 0.47x | 100 / 100 | rows and values match |
| q23 | OK | 4744.6 | 4432.1 | 2941.9 | 1.51x | 100 / 100 | rows and values match |
| q24 | OK | 2011.5 | 1344.2 | 535.0 | 2.51x | 5 / 5 | rows and values match |
| q25 | OK | 164.0 | 162.1 | 142.0 | 1.14x | 2 / 2 | rows and values match |
| q26 | OK | 147.6 | 127.9 | 212.0 | 0.60x | 100 / 100 | rows and values match |
| q27 | OK | 388.9 | 330.3 | 246.8 | 1.34x | 100 / 100 | rows and values match |
| q28 | OK | 412.2 | 415.3 | 338.7 | 1.23x | 1 / 1 | rows and values match |
| q29 | OK | 198.0 | 192.2 | 205.6 | 0.93x | 8 / 8 | rows and values match |
| q30 | OK | 216.3 | 208.2 | 233.4 | 0.89x | 100 / 100 | rows and values match |
| q31 | OK | 427.5 | 424.5 | 170.0 | 2.50x | 307 / 307 | rows and values match |
| q32 | OK | 49.8 | 45.6 | 30.5 | 1.50x | 1 / 1 | rows and values match |
| q33 | OK | 227.7 | 200.7 | 173.0 | 1.16x | 100 / 100 | rows and values match |
| q34 | OK | 140.0 | 133.4 | 139.4 | 0.96x | 1560 / 1560 | rows and values match |
| q35 | OK | 762.2 | 670.2 | 506.2 | 1.32x | 100 / 100 | rows and values match |
| q36 | OK | 182.8 | 175.8 | 125.0 | 1.41x | 100 / 100 | rows and values match |
| q37 | OK | 55.0 | 45.8 | 170.7 | 0.27x | 2 / 2 | rows and values match |
| q38 | OK | 663.5 | 572.9 | 384.8 | 1.49x | 1 / 1 | rows and values match |
| q39 | OK | 1755.0 | 1618.2 | 410.9 | 3.94x | 2433 / 2433 | rows and values match |
| q40 | OK | 117.3 | 103.1 | 66.2 | 1.56x | 100 / 100 | rows and values match |
| q41 | OK | 50.2 | 47.0 | 47.2 | 1.00x | 41 / 41 | rows and values match |
| q42 | OK | 92.2 | 68.5 | 61.7 | 1.11x | 11 / 11 | rows and values match |
| q43 | OK | 128.3 | 127.7 | 100.2 | 1.27x | 18 / 18 | rows and values match |
| q44 | OK | 167.8 | 165.1 | 121.2 | 1.36x | 10 / 10 | rows and values match |
| q45 | OK | 649.5 | 478.6 | 143.6 | 3.33x | 49 / 49 | rows and values match |
| q46 | OK | 515.9 | 477.2 | 236.9 | 2.01x | 100 / 100 | rows and values match |
| q47 | OK | 1122.8 | 1015.0 | 917.2 | 1.11x | 100 / 100 | rows and values match |
| q48 | OK | 411.4 | 394.8 | 349.5 | 1.13x | 1 / 1 | rows and values match |
| q49 | OK | 150.4 | 127.4 | 96.8 | 1.32x | 46 / 46 | rows and values match |
| q50 | OK | 126.9 | 123.2 | 239.7 | 0.51x | 51 / 51 | rows and values match |
| q51 | OK | 1731.0 | 1764.2 | 1753.9 | 1.01x | 100 / 100 | rows and values match |
| q52 | OK | 66.2 | 70.0 | 66.2 | 1.06x | 100 / 100 | rows and values match |
| q53 | OK | 161.8 | 159.6 | 99.0 | 1.61x | 100 / 100 | rows and values match |
| q54 | OK | 247.0 | 246.5 | 189.4 | 1.30x | 1 / 1 | rows and values match |
| q55 | OK | 78.8 | 78.7 | 65.6 | 1.20x | 100 / 100 | rows and values match |
| q56 | OK | 197.4 | 199.7 | 176.8 | 1.13x | 100 / 100 | rows and values match |
| q57 | OK | 565.0 | 596.3 | 380.5 | 1.57x | 100 / 100 | rows and values match |
| q58 | OK | 206.6 | 179.1 | 149.5 | 1.20x | 31 / 31 | rows and values match |
| q59 | OK | 1948.1 | 1447.8 | 421.9 | 3.43x | 100 / 100 | rows and values match |
| q60 | OK | 222.6 | 218.7 | 181.9 | 1.20x | 100 / 100 | rows and values match |
| q61 | OK | 412.7 | 435.7 | 242.3 | 1.80x | 1 / 1 | rows and values match |
| q62 | OK | 107.3 | 72.0 | 468.0 | 0.15x | 100 / 100 | rows and values match |
| q63 | OK | 177.7 | 184.4 | 84.7 | 2.18x | 100 / 100 | rows and values match |
| q64 | OK | 817.7 | 904.6 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1358.4 | 1161.3 | 508.5 | 2.28x | 100 / 100 | rows and values match |
| q66 | OK | 196.2 | 127.4 | 112.2 | 1.14x | 10 / 10 | rows and values match |
| q67 | OK | 4200.9 | 3875.9 | 5919.1 | 0.65x | 100 / 100 | rows and values match |
| q68 | OK | 388.7 | 421.0 | 232.1 | 1.81x | 100 / 100 | rows and values match |
| q69 | OK | 506.8 | 493.0 | 353.4 | 1.40x | 100 / 100 | rows and values match |
| q70 | OK | 959.9 | 943.5 | 187.9 | 5.02x | 7 / 7 | rows and values match |
| q71 | OK | 178.9 | 172.5 | 150.2 | 1.15x | 9655 / 9655 | rows and values match |
| q72 | OK | 7308.1 | 5930.7 | 24992.5 | 0.24x | 100 / 100 | rows and values match |
| q73 | OK | 119.6 | 131.4 | 130.4 | 1.01x | 17 / 17 | rows and values match |
| q74 | OK | 1342.7 | 1207.4 | 518.7 | 2.33x | 100 / 100 | rows and values match |
| q75 | OK | 1097.0 | 1111.6 | 498.3 | 2.23x | 100 / 100 | rows and values match |
| q76 | OK | 332.9 | 325.5 | 135.3 | 2.41x | 100 / 100 | rows and values match |
| q77 | OK | 155.4 | 114.6 | 97.5 | 1.17x | 100 / 100 | rows and values match |
| q78 | OK | 3672.8 | 3527.3 | 1234.4 | 2.86x | 100 / 100 | rows and values match |
| q79 | OK | 243.8 | 241.4 | 172.9 | 1.40x | 100 / 100 | rows and values match |
| q80 | OK | 562.9 | 533.1 | 247.2 | 2.16x | 100 / 100 | rows and values match |
| q81 | OK | 416.3 | 397.7 | 226.0 | 1.76x | 100 / 100 | rows and values match |
| q82 | OK | 63.2 | 63.5 | 320.9 | 0.20x | 6 / 6 | rows and values match |
| q83 | OK | 75.1 | 79.3 | 90.2 | 0.88x | 100 / 100 | rows and values match |
| q84 | OK | 116.5 | 107.9 | 193.3 | 0.56x | 100 / 100 | rows and values match |
| q85 | OK | 501.2 | 497.5 | 526.9 | 0.94x | 15 / 15 | rows and values match |
| q86 | OK | 141.8 | 152.6 | 72.3 | 2.11x | 100 / 100 | rows and values match |
| q87 | OK | 645.7 | 674.8 | 435.1 | 1.55x | 1 / 1 | rows and values match |
| q88 | ERROR | - | - | - | - | - | RuntimeError: planning budget exceeded: 151.8ms > 100ms budget; killed after optimize (parse 1.3ms, bind 0.9ms, decorrelate 0.1ms, optimize 149.5ms) - planning must be O(metadata); raise optimizer.planning_budget_ms only for a justified edge case |
| q89 | OK | 359.6 | 255.9 | 123.2 | 2.08x | 100 / 100 | rows and values match |
| q90 | OK | 54.7 | 46.0 | 63.3 | 0.73x | 1 / 1 | rows and values match |
| q91 | OK | 157.3 | 156.5 | 367.7 | 0.43x | 12 / 12 | rows and values match |
| q92 | OK | 54.7 | 56.1 | 41.9 | 1.34x | 1 / 1 | rows and values match |
| q93 | OK | 105.6 | 101.0 | 297.0 | 0.34x | 100 / 100 | rows and values match |
| q94 | OK | 163.9 | 155.5 | 67.7 | 2.30x | 1 / 1 | rows and values match |
| q95 | OK | 3515.2 | 2658.3 | 956.8 | 2.78x | 1 / 1 | rows and values match |
| q96 | OK | 49.6 | 49.1 | 54.0 | 0.91x | 1 / 1 | rows and values match |
| q97 | OK | 500.6 | 450.1 | 333.0 | 1.35x | 1 / 1 | rows and values match |
| q98 | OK | 179.7 | 193.4 | 144.1 | 1.34x | 15076 / 15076 | rows and values match |
| q99 | OK | 89.8 | 70.8 | 910.8 | 0.08x | 100 / 100 | rows and values match |
