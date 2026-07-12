# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 01:42

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 133.7s)
Timing: ours 62.5s  duckdb 68.3s  ->  total 0.91x  geomean 1.23x  (97 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 62.5 | 68.3 | 0.91x | 1.23x | 97 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 301.3 | 138.8 | 102.7 | 1.35x | 100 / 100 | rows and values match |
| q02 | OK | 853.7 | 594.9 | 232.1 | 2.56x | 2513 / 2513 | rows and values match |
| q03 | OK | 99.0 | 61.1 | 56.3 | 1.08x | 100 / 100 | rows and values match |
| q04 | OK | 4363.2 | 3710.3 | 1294.3 | 2.87x | 100 / 100 | rows and values match |
| q05 | OK | 205.1 | 167.8 | 152.9 | 1.10x | 100 / 100 | rows and values match |
| q06 | OK | 1707.2 | 1365.8 | 205.5 | 6.65x | 51 / 51 | rows and values match |
| q07 | OK | 235.0 | 195.3 | 240.5 | 0.81x | 100 / 100 | rows and values match |
| q08 | OK | 247.6 | 224.6 | 154.7 | 1.45x | 9 / 9 | rows and values match |
| q09 | OK | 357.2 | 351.5 | 386.6 | 0.91x | 1 / 1 | rows and values match |
| q10 | OK | 235.9 | 226.9 | 457.8 | 0.50x | 100 / 100 | rows and values match |
| q11 | OK | 2208.7 | 2017.1 | 806.7 | 2.50x | 100 / 100 | rows and values match |
| q12 | OK | 77.6 | 72.5 | 66.5 | 1.09x | 100 / 100 | rows and values match |
| q13 | OK | 345.0 | 321.0 | 340.7 | 0.94x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 449.1 | 429.8 | 128.4 | 3.35x | 100 / 100 | rows and values match |
| q16 | OK | 190.1 | 166.3 | 48.4 | 3.44x | 1 / 1 | rows and values match |
| q17 | OK | 215.8 | 211.6 | 200.8 | 1.05x | 0 / 0 | rows and values match |
| q18 | OK | 523.8 | 663.4 | 422.6 | 1.57x | 100 / 100 | rows and values match |
| q19 | OK | 416.1 | 406.5 | 2255.7 | 0.18x | 100 / 100 | rows and values match |
| q20 | OK | 73.3 | 70.2 | 62.3 | 1.13x | 100 / 100 | rows and values match |
| q21 | OK | 70.8 | 53.9 | 57.0 | 0.95x | 100 / 100 | rows and values match |
| q22 | OK | 2815.7 | 2577.1 | 5831.2 | 0.44x | 100 / 100 | rows and values match |
| q23 | OK | 4619.6 | 4277.7 | 2941.9 | 1.45x | 100 / 100 | rows and values match |
| q24 | OK | 1963.1 | 1275.2 | 535.0 | 2.38x | 5 / 5 | rows and values match |
| q25 | OK | 167.2 | 164.5 | 142.0 | 1.16x | 2 / 2 | rows and values match |
| q26 | OK | 146.8 | 144.0 | 212.0 | 0.68x | 100 / 100 | rows and values match |
| q27 | OK | 348.1 | 281.2 | 246.8 | 1.14x | 100 / 100 | rows and values match |
| q28 | OK | 359.2 | 375.1 | 338.7 | 1.11x | 1 / 1 | rows and values match |
| q29 | OK | 193.5 | 183.0 | 205.6 | 0.89x | 8 / 8 | rows and values match |
| q30 | OK | 206.9 | 200.9 | 233.4 | 0.86x | 100 / 100 | rows and values match |
| q31 | OK | 420.2 | 417.6 | 170.0 | 2.46x | 307 / 307 | rows and values match |
| q32 | OK | 47.0 | 45.7 | 30.5 | 1.50x | 1 / 1 | rows and values match |
| q33 | OK | 197.2 | 178.3 | 173.0 | 1.03x | 100 / 100 | rows and values match |
| q34 | OK | 140.8 | 131.3 | 139.4 | 0.94x | 1560 / 1560 | rows and values match |
| q35 | OK | 639.8 | 756.2 | 506.2 | 1.49x | 100 / 100 | rows and values match |
| q36 | OK | 182.3 | 170.0 | 125.0 | 1.36x | 100 / 100 | rows and values match |
| q37 | OK | 52.8 | 44.8 | 170.7 | 0.26x | 2 / 2 | rows and values match |
| q38 | OK | 653.6 | 606.3 | 384.8 | 1.58x | 1 / 1 | rows and values match |
| q39 | OK | 1725.6 | 1672.1 | 410.9 | 4.07x | 2433 / 2433 | rows and values match |
| q40 | OK | 116.5 | 99.7 | 66.2 | 1.51x | 100 / 100 | rows and values match |
| q41 | OK | 48.8 | 47.3 | 47.2 | 1.00x | 41 / 41 | rows and values match |
| q42 | OK | 100.2 | 86.2 | 61.7 | 1.40x | 11 / 11 | rows and values match |
| q43 | OK | 139.6 | 124.1 | 100.2 | 1.24x | 18 / 18 | rows and values match |
| q44 | OK | 157.6 | 152.3 | 121.2 | 1.26x | 10 / 10 | rows and values match |
| q45 | OK | 1976.5 | 1941.9 | 143.6 | 13.52x | 49 / 49 | rows and values match |
| q46 | OK | 506.1 | 492.5 | 236.9 | 2.08x | 100 / 100 | rows and values match |
| q47 | OK | 1116.2 | 1093.4 | 917.2 | 1.19x | 100 / 100 | rows and values match |
| q48 | OK | 425.2 | 428.9 | 349.5 | 1.23x | 1 / 1 | rows and values match |
| q49 | OK | 150.8 | 137.1 | 96.8 | 1.42x | 46 / 46 | rows and values match |
| q50 | OK | 126.6 | 118.1 | 239.7 | 0.49x | 51 / 51 | rows and values match |
| q51 | OK | 1679.3 | 1715.9 | 1753.9 | 0.98x | 100 / 100 | rows and values match |
| q52 | OK | 70.3 | 71.7 | 66.2 | 1.08x | 100 / 100 | rows and values match |
| q53 | OK | 153.3 | 168.9 | 99.0 | 1.71x | 100 / 100 | rows and values match |
| q54 | OK | 226.7 | 221.0 | 189.4 | 1.17x | 1 / 1 | rows and values match |
| q55 | OK | 65.7 | 66.4 | 65.6 | 1.01x | 100 / 100 | rows and values match |
| q56 | OK | 166.5 | 163.0 | 176.8 | 0.92x | 100 / 100 | rows and values match |
| q57 | OK | 570.2 | 674.8 | 380.5 | 1.77x | 100 / 100 | rows and values match |
| q58 | OK | 272.0 | 224.7 | 149.5 | 1.50x | 31 / 31 | rows and values match |
| q59 | OK | 1972.2 | 1414.4 | 421.9 | 3.35x | 100 / 100 | rows and values match |
| q60 | OK | 233.7 | 206.2 | 181.9 | 1.13x | 100 / 100 | rows and values match |
| q61 | OK | 329.5 | 305.9 | 242.3 | 1.26x | 1 / 1 | rows and values match |
| q62 | OK | 71.0 | 57.5 | 468.0 | 0.12x | 100 / 100 | rows and values match |
| q63 | OK | 150.4 | 150.5 | 84.7 | 1.78x | 100 / 100 | rows and values match |
| q64 | OK | 757.7 | 717.6 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1280.6 | 1129.8 | 508.5 | 2.22x | 100 / 100 | rows and values match |
| q66 | OK | 140.3 | 123.5 | 112.2 | 1.10x | 10 / 10 | rows and values match |
| q67 | OK | 3865.6 | 4159.7 | 5919.1 | 0.70x | 100 / 100 | rows and values match |
| q68 | OK | 364.9 | 364.9 | 232.1 | 1.57x | 100 / 100 | rows and values match |
| q69 | OK | 480.2 | 482.3 | 353.4 | 1.37x | 100 / 100 | rows and values match |
| q70 | OK | 946.7 | 990.5 | 187.9 | 5.27x | 7 / 7 | rows and values match |
| q71 | OK | 165.1 | 155.5 | 150.2 | 1.04x | 9655 / 9655 | rows and values match |
| q72 | OK | 7247.4 | 5958.6 | 24992.5 | 0.24x | 100 / 100 | rows and values match |
| q73 | OK | 141.8 | 144.5 | 130.4 | 1.11x | 17 / 17 | rows and values match |
| q74 | OK | 1462.7 | 1152.8 | 518.7 | 2.22x | 100 / 100 | rows and values match |
| q75 | OK | 1034.5 | 1068.2 | 498.3 | 2.14x | 100 / 100 | rows and values match |
| q76 | OK | 357.4 | 320.9 | 135.3 | 2.37x | 100 / 100 | rows and values match |
| q77 | OK | 177.4 | 146.6 | 97.5 | 1.50x | 100 / 100 | rows and values match |
| q78 | OK | 3751.9 | 3586.2 | 1234.4 | 2.91x | 100 / 100 | rows and values match |
| q79 | OK | 238.6 | 236.1 | 172.9 | 1.37x | 100 / 100 | rows and values match |
| q80 | OK | 547.9 | 558.8 | 247.2 | 2.26x | 100 / 100 | rows and values match |
| q81 | OK | 417.8 | 450.2 | 226.0 | 1.99x | 100 / 100 | rows and values match |
| q82 | OK | 74.0 | 71.2 | 320.9 | 0.22x | 6 / 6 | rows and values match |
| q83 | OK | 90.4 | 95.9 | 90.2 | 1.06x | 100 / 100 | rows and values match |
| q84 | OK | 122.5 | 110.8 | 193.3 | 0.57x | 100 / 100 | rows and values match |
| q85 | OK | 474.8 | 500.8 | 526.9 | 0.95x | 15 / 15 | rows and values match |
| q86 | OK | 143.7 | 156.1 | 72.3 | 2.16x | 100 / 100 | rows and values match |
| q87 | OK | 665.9 | 762.8 | 435.1 | 1.75x | 1 / 1 | rows and values match |
| q88 | OK | 443.4 | 416.2 | 448.4 | 0.93x | 1 / 1 | rows and values match |
| q89 | OK | 344.4 | 257.1 | 123.2 | 2.09x | 100 / 100 | rows and values match |
| q90 | OK | 53.2 | 47.1 | 63.3 | 0.74x | 1 / 1 | rows and values match |
| q91 | OK | 158.5 | 148.8 | 367.7 | 0.40x | 12 / 12 | rows and values match |
| q92 | OK | 54.1 | 53.2 | 41.9 | 1.27x | 1 / 1 | rows and values match |
| q93 | OK | 105.8 | 104.0 | 297.0 | 0.35x | 100 / 100 | rows and values match |
| q94 | OK | 149.6 | 134.1 | 67.7 | 1.98x | 1 / 1 | rows and values match |
| q95 | OK | 3457.0 | 2688.1 | 956.8 | 2.81x | 1 / 1 | rows and values match |
| q96 | OK | 53.0 | 74.5 | 54.0 | 1.38x | 1 / 1 | rows and values match |
| q97 | OK | 481.8 | 424.5 | 333.0 | 1.27x | 1 / 1 | rows and values match |
| q98 | OK | 171.1 | 179.7 | 144.1 | 1.25x | 15076 / 15076 | rows and values match |
| q99 | OK | 78.0 | 90.4 | 910.8 | 0.10x | 100 / 100 | rows and values match |
