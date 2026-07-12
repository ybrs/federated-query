# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:29

Tally: 97 ok | 0 wrong | 2 error   (total 99 queries, 175.4s)
Timing: ours 81.9s  duckdb 67.8s  ->  total 1.21x  geomean 1.31x  (96 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

### RuntimeError: planning budget exceeded: N.Nms > Nms budget; killed after optimize (parse N.Nms, bind N.Nms, decorrelate N.Nms, optimize N.Nms) - planning must be O(metadata); raise optimizer.planning_budget_ms only for a justified edge case (1) [ERROR]
Queries: q88

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 81.9 | 67.8 | 1.21x | 1.31x | 96 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 304.6 | 136.6 | 102.7 | 1.33x | 100 / 100 | rows and values match |
| q02 | OK | 1202.7 | 1000.6 | 232.1 | 4.31x | 2513 / 2513 | rows and values match |
| q03 | OK | 92.0 | 64.1 | 56.3 | 1.14x | 100 / 100 | rows and values match |
| q04 | OK | 16263.9 | 15646.5 | 1294.3 | 12.09x | 100 / 100 | rows and values match |
| q05 | OK | 278.2 | 160.8 | 152.9 | 1.05x | 100 / 100 | rows and values match |
| q06 | OK | 1792.6 | 1282.5 | 205.5 | 6.24x | 51 / 51 | rows and values match |
| q07 | OK | 354.4 | 199.2 | 240.5 | 0.83x | 100 / 100 | rows and values match |
| q08 | OK | 270.7 | 237.1 | 154.7 | 1.53x | 9 / 9 | rows and values match |
| q09 | OK | 370.8 | 394.0 | 386.6 | 1.02x | 1 / 1 | rows and values match |
| q10 | OK | 265.5 | 234.4 | 457.8 | 0.51x | 100 / 100 | rows and values match |
| q11 | OK | 3386.4 | 3213.5 | 806.7 | 3.98x | 100 / 100 | rows and values match |
| q12 | OK | 76.4 | 70.4 | 66.5 | 1.06x | 100 / 100 | rows and values match |
| q13 | OK | 373.1 | 338.7 | 340.7 | 0.99x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 461.6 | 485.6 | 128.4 | 3.78x | 100 / 100 | rows and values match |
| q16 | OK | 220.1 | 172.8 | 48.4 | 3.57x | 1 / 1 | rows and values match |
| q17 | OK | 232.1 | 206.7 | 200.8 | 1.03x | 0 / 0 | rows and values match |
| q18 | OK | 567.5 | 695.3 | 422.6 | 1.65x | 100 / 100 | rows and values match |
| q19 | OK | 437.2 | 440.9 | 2255.7 | 0.20x | 100 / 100 | rows and values match |
| q20 | OK | 73.7 | 69.6 | 62.3 | 1.12x | 100 / 100 | rows and values match |
| q21 | OK | 80.2 | 55.2 | 57.0 | 0.97x | 100 / 100 | rows and values match |
| q22 | OK | 2678.5 | 2547.1 | 5831.2 | 0.44x | 100 / 100 | rows and values match |
| q23 | OK | 8390.4 | 7699.9 | 2941.9 | 2.62x | 100 / 100 | rows and values match |
| q24 | OK | 2511.4 | 1812.8 | 535.0 | 3.39x | 5 / 5 | rows and values match |
| q25 | OK | 161.8 | 151.7 | 142.0 | 1.07x | 2 / 2 | rows and values match |
| q26 | OK | 157.5 | 126.5 | 212.0 | 0.60x | 100 / 100 | rows and values match |
| q27 | OK | 405.6 | 327.9 | 246.8 | 1.33x | 100 / 100 | rows and values match |
| q28 | OK | 424.4 | 343.4 | 338.7 | 1.01x | 1 / 1 | rows and values match |
| q29 | OK | 246.7 | 179.0 | 205.6 | 0.87x | 8 / 8 | rows and values match |
| q30 | OK | 237.2 | 220.3 | 233.4 | 0.94x | 100 / 100 | rows and values match |
| q31 | OK | 548.1 | 458.4 | 170.0 | 2.70x | 307 / 307 | rows and values match |
| q32 | OK | 45.6 | 47.2 | 30.5 | 1.55x | 1 / 1 | rows and values match |
| q33 | OK | 210.1 | 174.2 | 173.0 | 1.01x | 100 / 100 | rows and values match |
| q34 | OK | 141.2 | 134.5 | 139.4 | 0.96x | 1560 / 1560 | rows and values match |
| q35 | OK | 627.3 | 625.7 | 506.2 | 1.24x | 100 / 100 | rows and values match |
| q36 | OK | 209.9 | 188.9 | 125.0 | 1.51x | 100 / 100 | rows and values match |
| q37 | OK | 54.6 | 44.1 | 170.7 | 0.26x | 2 / 2 | rows and values match |
| q38 | OK | 634.6 | 572.7 | 384.8 | 1.49x | 1 / 1 | rows and values match |
| q39 | OK | 1793.3 | 1615.2 | 410.9 | 3.93x | 2433 / 2433 | rows and values match |
| q40 | OK | 114.8 | 118.3 | 66.2 | 1.79x | 100 / 100 | rows and values match |
| q41 | OK | 47.7 | 47.1 | 47.2 | 1.00x | 41 / 41 | rows and values match |
| q42 | OK | 84.5 | 89.6 | 61.7 | 1.45x | 11 / 11 | rows and values match |
| q43 | OK | 124.6 | 125.2 | 100.2 | 1.25x | 18 / 18 | rows and values match |
| q44 | OK | 152.9 | 160.4 | 121.2 | 1.32x | 10 / 10 | rows and values match |
| q45 | OK | 2008.4 | 1942.2 | 143.6 | 13.52x | 49 / 49 | rows and values match |
| q46 | OK | 459.2 | 444.9 | 236.9 | 1.88x | 100 / 100 | rows and values match |
| q47 | OK | 2868.3 | 2556.4 | 917.2 | 2.79x | 100 / 100 | rows and values match |
| q48 | OK | 379.7 | 374.1 | 349.5 | 1.07x | 1 / 1 | rows and values match |
| q49 | OK | 141.9 | 127.0 | 96.8 | 1.31x | 46 / 46 | rows and values match |
| q50 | OK | 123.2 | 117.3 | 239.7 | 0.49x | 51 / 51 | rows and values match |
| q51 | OK | 1651.5 | 1704.3 | 1753.9 | 0.97x | 100 / 100 | rows and values match |
| q52 | OK | 76.2 | 73.5 | 66.2 | 1.11x | 100 / 100 | rows and values match |
| q53 | OK | 149.2 | 149.9 | 99.0 | 1.51x | 100 / 100 | rows and values match |
| q54 | OK | 227.1 | 216.2 | 189.4 | 1.14x | 1 / 1 | rows and values match |
| q55 | OK | 76.5 | 65.1 | 65.6 | 0.99x | 100 / 100 | rows and values match |
| q56 | OK | 162.4 | 164.4 | 176.8 | 0.93x | 100 / 100 | rows and values match |
| q57 | OK | 1276.5 | 1300.9 | 380.5 | 3.42x | 100 / 100 | rows and values match |
| q58 | OK | 176.9 | 173.3 | 149.5 | 1.16x | 31 / 31 | rows and values match |
| q59 | OK | 2555.8 | 2146.9 | 421.9 | 5.09x | 100 / 100 | rows and values match |
| q60 | OK | 227.9 | 248.3 | 181.9 | 1.36x | 100 / 100 | rows and values match |
| q61 | OK | 364.0 | 401.7 | 242.3 | 1.66x | 1 / 1 | rows and values match |
| q62 | OK | 62.4 | 54.6 | 468.0 | 0.12x | 100 / 100 | rows and values match |
| q63 | OK | 153.2 | 156.9 | 84.7 | 1.85x | 100 / 100 | rows and values match |
| q64 | OK | 847.2 | 859.6 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1275.9 | 1088.1 | 508.5 | 2.14x | 100 / 100 | rows and values match |
| q66 | OK | 151.3 | 110.7 | 112.2 | 0.99x | 10 / 10 | rows and values match |
| q67 | OK | 3755.4 | 3713.6 | 5919.1 | 0.63x | 100 / 100 | rows and values match |
| q68 | OK | 372.9 | 378.6 | 232.1 | 1.63x | 100 / 100 | rows and values match |
| q69 | OK | 523.7 | 492.0 | 353.4 | 1.39x | 100 / 100 | rows and values match |
| q70 | OK | 926.9 | 915.9 | 187.9 | 4.87x | 7 / 7 | rows and values match |
| q71 | OK | 180.0 | 163.9 | 150.2 | 1.09x | 9655 / 9655 | rows and values match |
| q72 | OK | 7339.0 | 5685.4 | 24992.5 | 0.23x | 100 / 100 | rows and values match |
| q73 | OK | 139.4 | 115.4 | 130.4 | 0.88x | 17 / 17 | rows and values match |
| q74 | OK | 1860.5 | 1743.3 | 518.7 | 3.36x | 100 / 100 | rows and values match |
| q75 | OK | 1663.9 | 1552.6 | 498.3 | 3.12x | 100 / 100 | rows and values match |
| q76 | OK | 331.6 | 302.8 | 135.3 | 2.24x | 100 / 100 | rows and values match |
| q77 | OK | 146.0 | 118.5 | 97.5 | 1.22x | 100 / 100 | rows and values match |
| q78 | OK | 3578.1 | 3395.6 | 1234.4 | 2.75x | 100 / 100 | rows and values match |
| q79 | OK | 237.1 | 224.3 | 172.9 | 1.30x | 100 / 100 | rows and values match |
| q80 | OK | 535.2 | 523.6 | 247.2 | 2.12x | 100 / 100 | rows and values match |
| q81 | OK | 461.3 | 447.1 | 226.0 | 1.98x | 100 / 100 | rows and values match |
| q82 | OK | 64.1 | 62.5 | 320.9 | 0.19x | 6 / 6 | rows and values match |
| q83 | OK | 77.5 | 78.3 | 90.2 | 0.87x | 100 / 100 | rows and values match |
| q84 | OK | 116.8 | 107.2 | 193.3 | 0.55x | 100 / 100 | rows and values match |
| q85 | OK | 475.8 | 495.4 | 526.9 | 0.94x | 15 / 15 | rows and values match |
| q86 | OK | 162.7 | 139.8 | 72.3 | 1.93x | 100 / 100 | rows and values match |
| q87 | OK | 603.0 | 616.5 | 435.1 | 1.42x | 1 / 1 | rows and values match |
| q88 | ERROR | - | - | - | - | - | RuntimeError: planning budget exceeded: 133.6ms > 100ms budget; killed after optimize (parse 0.8ms, bind 0.5ms, decorrelate 0.1ms, optimize 132.2ms) - planning must be O(metadata); raise optimizer.planning_budget_ms only for a justified edge case |
| q89 | OK | 352.2 | 284.8 | 123.2 | 2.31x | 100 / 100 | rows and values match |
| q90 | OK | 77.3 | 69.2 | 63.3 | 1.09x | 1 / 1 | rows and values match |
| q91 | OK | 182.8 | 164.8 | 367.7 | 0.45x | 12 / 12 | rows and values match |
| q92 | OK | 62.8 | 65.4 | 41.9 | 1.56x | 1 / 1 | rows and values match |
| q93 | OK | 132.5 | 134.8 | 297.0 | 0.45x | 100 / 100 | rows and values match |
| q94 | OK | 184.6 | 179.4 | 67.7 | 2.65x | 1 / 1 | rows and values match |
| q95 | OK | 3494.7 | 2507.4 | 956.8 | 2.62x | 1 / 1 | rows and values match |
| q96 | OK | 46.8 | 45.7 | 54.0 | 0.85x | 1 / 1 | rows and values match |
| q97 | OK | 467.5 | 432.7 | 333.0 | 1.30x | 1 / 1 | rows and values match |
| q98 | OK | 167.7 | 179.1 | 144.1 | 1.24x | 15076 / 15076 | rows and values match |
| q99 | OK | 86.6 | 70.6 | 910.8 | 0.08x | 100 / 100 | rows and values match |
