# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 00:36

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 175.1s)
Timing: ours 83.1s  duckdb 68.3s  ->  total 1.22x  geomean 1.31x  (97 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 83.1 | 68.3 | 1.22x | 1.31x | 97 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 302.6 | 135.6 | 102.7 | 1.32x | 100 / 100 | rows and values match |
| q02 | OK | 1314.9 | 907.8 | 232.1 | 3.91x | 2513 / 2513 | rows and values match |
| q03 | OK | 89.2 | 57.1 | 56.3 | 1.01x | 100 / 100 | rows and values match |
| q04 | OK | 16084.6 | 15621.2 | 1294.3 | 12.07x | 100 / 100 | rows and values match |
| q05 | OK | 215.2 | 185.8 | 152.9 | 1.22x | 100 / 100 | rows and values match |
| q06 | OK | 1912.9 | 1302.1 | 205.5 | 6.34x | 51 / 51 | rows and values match |
| q07 | OK | 252.1 | 209.3 | 240.5 | 0.87x | 100 / 100 | rows and values match |
| q08 | OK | 249.5 | 241.4 | 154.7 | 1.56x | 9 / 9 | rows and values match |
| q09 | OK | 374.6 | 370.7 | 386.6 | 0.96x | 1 / 1 | rows and values match |
| q10 | OK | 241.7 | 236.0 | 457.8 | 0.52x | 100 / 100 | rows and values match |
| q11 | OK | 3399.0 | 3029.7 | 806.7 | 3.76x | 100 / 100 | rows and values match |
| q12 | OK | 72.3 | 71.3 | 66.5 | 1.07x | 100 / 100 | rows and values match |
| q13 | OK | 358.9 | 346.0 | 340.7 | 1.02x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 456.2 | 468.2 | 128.4 | 3.65x | 100 / 100 | rows and values match |
| q16 | OK | 198.4 | 162.1 | 48.4 | 3.35x | 1 / 1 | rows and values match |
| q17 | OK | 229.8 | 222.2 | 200.8 | 1.11x | 0 / 0 | rows and values match |
| q18 | OK | 518.8 | 519.4 | 422.6 | 1.23x | 100 / 100 | rows and values match |
| q19 | OK | 466.0 | 445.2 | 2255.7 | 0.20x | 100 / 100 | rows and values match |
| q20 | OK | 72.2 | 68.9 | 62.3 | 1.11x | 100 / 100 | rows and values match |
| q21 | OK | 75.2 | 52.7 | 57.0 | 0.92x | 100 / 100 | rows and values match |
| q22 | OK | 2764.5 | 2666.5 | 5831.2 | 0.46x | 100 / 100 | rows and values match |
| q23 | OK | 7761.5 | 7732.8 | 2941.9 | 2.63x | 100 / 100 | rows and values match |
| q24 | OK | 2555.5 | 1854.4 | 535.0 | 3.47x | 5 / 5 | rows and values match |
| q25 | OK | 179.0 | 156.9 | 142.0 | 1.10x | 2 / 2 | rows and values match |
| q26 | OK | 152.0 | 135.9 | 212.0 | 0.64x | 100 / 100 | rows and values match |
| q27 | OK | 395.8 | 338.7 | 246.8 | 1.37x | 100 / 100 | rows and values match |
| q28 | OK | 378.3 | 348.4 | 338.7 | 1.03x | 1 / 1 | rows and values match |
| q29 | OK | 200.1 | 189.6 | 205.6 | 0.92x | 8 / 8 | rows and values match |
| q30 | OK | 231.0 | 216.6 | 233.4 | 0.93x | 100 / 100 | rows and values match |
| q31 | OK | 537.7 | 446.1 | 170.0 | 2.62x | 307 / 307 | rows and values match |
| q32 | OK | 49.1 | 50.4 | 30.5 | 1.65x | 1 / 1 | rows and values match |
| q33 | OK | 216.5 | 205.7 | 173.0 | 1.19x | 100 / 100 | rows and values match |
| q34 | OK | 145.5 | 129.7 | 139.4 | 0.93x | 1560 / 1560 | rows and values match |
| q35 | OK | 649.8 | 697.1 | 506.2 | 1.38x | 100 / 100 | rows and values match |
| q36 | OK | 212.9 | 205.3 | 125.0 | 1.64x | 100 / 100 | rows and values match |
| q37 | OK | 53.1 | 45.5 | 170.7 | 0.27x | 2 / 2 | rows and values match |
| q38 | OK | 655.1 | 584.9 | 384.8 | 1.52x | 1 / 1 | rows and values match |
| q39 | OK | 1732.4 | 1650.1 | 410.9 | 4.02x | 2433 / 2433 | rows and values match |
| q40 | OK | 112.2 | 102.9 | 66.2 | 1.56x | 100 / 100 | rows and values match |
| q41 | OK | 50.2 | 51.5 | 47.2 | 1.09x | 41 / 41 | rows and values match |
| q42 | OK | 71.7 | 79.2 | 61.7 | 1.28x | 11 / 11 | rows and values match |
| q43 | OK | 136.8 | 127.0 | 100.2 | 1.27x | 18 / 18 | rows and values match |
| q44 | OK | 156.5 | 163.8 | 121.2 | 1.35x | 10 / 10 | rows and values match |
| q45 | OK | 1950.2 | 1959.9 | 143.6 | 13.65x | 49 / 49 | rows and values match |
| q46 | OK | 456.2 | 437.6 | 236.9 | 1.85x | 100 / 100 | rows and values match |
| q47 | OK | 2659.9 | 2622.7 | 917.2 | 2.86x | 100 / 100 | rows and values match |
| q48 | OK | 408.2 | 401.2 | 349.5 | 1.15x | 1 / 1 | rows and values match |
| q49 | OK | 136.7 | 127.3 | 96.8 | 1.32x | 46 / 46 | rows and values match |
| q50 | OK | 120.8 | 115.3 | 239.7 | 0.48x | 51 / 51 | rows and values match |
| q51 | OK | 1704.9 | 1701.8 | 1753.9 | 0.97x | 100 / 100 | rows and values match |
| q52 | OK | 67.4 | 68.1 | 66.2 | 1.03x | 100 / 100 | rows and values match |
| q53 | OK | 153.9 | 151.3 | 99.0 | 1.53x | 100 / 100 | rows and values match |
| q54 | OK | 218.7 | 217.7 | 189.4 | 1.15x | 1 / 1 | rows and values match |
| q55 | OK | 69.9 | 70.8 | 65.6 | 1.08x | 100 / 100 | rows and values match |
| q56 | OK | 163.3 | 172.0 | 176.8 | 0.97x | 100 / 100 | rows and values match |
| q57 | OK | 1278.5 | 1253.6 | 380.5 | 3.29x | 100 / 100 | rows and values match |
| q58 | OK | 177.6 | 188.2 | 149.5 | 1.26x | 31 / 31 | rows and values match |
| q59 | OK | 2557.4 | 2196.6 | 421.9 | 5.21x | 100 / 100 | rows and values match |
| q60 | OK | 217.3 | 202.4 | 181.9 | 1.11x | 100 / 100 | rows and values match |
| q61 | OK | 320.5 | 342.6 | 242.3 | 1.41x | 1 / 1 | rows and values match |
| q62 | OK | 67.5 | 58.4 | 468.0 | 0.12x | 100 / 100 | rows and values match |
| q63 | OK | 151.1 | 152.5 | 84.7 | 1.80x | 100 / 100 | rows and values match |
| q64 | OK | 824.4 | 776.9 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1278.9 | 1122.8 | 508.5 | 2.21x | 100 / 100 | rows and values match |
| q66 | OK | 197.6 | 124.3 | 112.2 | 1.11x | 10 / 10 | rows and values match |
| q67 | OK | 3801.9 | 4188.2 | 5919.1 | 0.71x | 100 / 100 | rows and values match |
| q68 | OK | 362.6 | 362.7 | 232.1 | 1.56x | 100 / 100 | rows and values match |
| q69 | OK | 488.2 | 481.1 | 353.4 | 1.36x | 100 / 100 | rows and values match |
| q70 | OK | 987.8 | 987.7 | 187.9 | 5.26x | 7 / 7 | rows and values match |
| q71 | OK | 168.6 | 162.8 | 150.2 | 1.08x | 9655 / 9655 | rows and values match |
| q72 | OK | 6392.4 | 5668.3 | 24992.5 | 0.23x | 100 / 100 | rows and values match |
| q73 | OK | 119.1 | 119.0 | 130.4 | 0.91x | 17 / 17 | rows and values match |
| q74 | OK | 2022.8 | 1708.4 | 518.7 | 3.29x | 100 / 100 | rows and values match |
| q75 | OK | 1553.6 | 1616.0 | 498.3 | 3.24x | 100 / 100 | rows and values match |
| q76 | OK | 316.3 | 317.8 | 135.3 | 2.35x | 100 / 100 | rows and values match |
| q77 | OK | 153.7 | 131.7 | 97.5 | 1.35x | 100 / 100 | rows and values match |
| q78 | OK | 3585.5 | 3426.9 | 1234.4 | 2.78x | 100 / 100 | rows and values match |
| q79 | OK | 250.8 | 247.7 | 172.9 | 1.43x | 100 / 100 | rows and values match |
| q80 | OK | 561.5 | 560.3 | 247.2 | 2.27x | 100 / 100 | rows and values match |
| q81 | OK | 512.2 | 504.7 | 226.0 | 2.23x | 100 / 100 | rows and values match |
| q82 | OK | 70.9 | 77.6 | 320.9 | 0.24x | 6 / 6 | rows and values match |
| q83 | OK | 101.3 | 96.5 | 90.2 | 1.07x | 100 / 100 | rows and values match |
| q84 | OK | 127.8 | 122.1 | 193.3 | 0.63x | 100 / 100 | rows and values match |
| q85 | OK | 590.7 | 505.1 | 526.9 | 0.96x | 15 / 15 | rows and values match |
| q86 | OK | 176.6 | 153.1 | 72.3 | 2.12x | 100 / 100 | rows and values match |
| q87 | OK | 735.4 | 737.1 | 435.1 | 1.69x | 1 / 1 | rows and values match |
| q88 | OK | 463.0 | 407.2 | 448.4 | 0.91x | 1 / 1 | rows and values match |
| q89 | OK | 352.6 | 254.5 | 123.2 | 2.07x | 100 / 100 | rows and values match |
| q90 | OK | 51.1 | 46.5 | 63.3 | 0.73x | 1 / 1 | rows and values match |
| q91 | OK | 156.9 | 149.5 | 367.7 | 0.41x | 12 / 12 | rows and values match |
| q92 | OK | 54.4 | 54.7 | 41.9 | 1.31x | 1 / 1 | rows and values match |
| q93 | OK | 105.4 | 103.3 | 297.0 | 0.35x | 100 / 100 | rows and values match |
| q94 | OK | 158.4 | 143.6 | 67.7 | 2.12x | 1 / 1 | rows and values match |
| q95 | OK | 3431.7 | 2508.5 | 956.8 | 2.62x | 1 / 1 | rows and values match |
| q96 | OK | 45.9 | 52.2 | 54.0 | 0.97x | 1 / 1 | rows and values match |
| q97 | OK | 474.4 | 417.5 | 333.0 | 1.25x | 1 / 1 | rows and values match |
| q98 | OK | 170.2 | 174.7 | 144.1 | 1.21x | 15076 / 15076 | rows and values match |
| q99 | OK | 87.2 | 74.2 | 910.8 | 0.08x | 100 / 100 | rows and values match |
