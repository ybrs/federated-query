# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf10.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-13 02:20

Tally: 98 ok | 0 wrong | 1 error   (total 99 queries, 129.9s)
Timing: ours 60.6s  duckdb 68.3s  ->  total 0.89x  geomean 1.21x  (97 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 60.6 | 68.3 | 0.89x | 1.21x | 97 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 322.4 | 147.8 | 102.7 | 1.44x | 100 / 100 | rows and values match |
| q02 | OK | 847.6 | 585.4 | 232.1 | 2.52x | 2513 / 2513 | rows and values match |
| q03 | OK | 88.7 | 59.3 | 56.3 | 1.05x | 100 / 100 | rows and values match |
| q04 | OK | 4245.9 | 3869.6 | 1294.3 | 2.99x | 100 / 100 | rows and values match |
| q05 | OK | 239.8 | 186.1 | 152.9 | 1.22x | 100 / 100 | rows and values match |
| q06 | OK | 1710.7 | 1408.9 | 205.5 | 6.85x | 51 / 51 | rows and values match |
| q07 | OK | 235.1 | 195.6 | 240.5 | 0.81x | 100 / 100 | rows and values match |
| q08 | OK | 248.3 | 238.6 | 154.7 | 1.54x | 9 / 9 | rows and values match |
| q09 | OK | 383.7 | 365.8 | 386.6 | 0.95x | 1 / 1 | rows and values match |
| q10 | OK | 234.7 | 223.3 | 457.8 | 0.49x | 100 / 100 | rows and values match |
| q11 | OK | 2188.9 | 2029.8 | 806.7 | 2.52x | 100 / 100 | rows and values match |
| q12 | OK | 104.2 | 97.6 | 66.5 | 1.47x | 100 / 100 | rows and values match |
| q13 | OK | 360.1 | 322.7 | 340.7 | 0.95x | 1 / 1 | rows and values match |
| q14 | ERROR | - | - | - | - | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 457.3 | 423.2 | 128.4 | 3.30x | 100 / 100 | rows and values match |
| q16 | OK | 216.1 | 167.8 | 48.4 | 3.47x | 1 / 1 | rows and values match |
| q17 | OK | 242.3 | 200.0 | 200.8 | 1.00x | 0 / 0 | rows and values match |
| q18 | OK | 586.4 | 708.2 | 422.6 | 1.68x | 100 / 100 | rows and values match |
| q19 | OK | 414.7 | 582.8 | 2255.7 | 0.26x | 100 / 100 | rows and values match |
| q20 | OK | 96.9 | 68.7 | 62.3 | 1.10x | 100 / 100 | rows and values match |
| q21 | OK | 68.5 | 55.0 | 57.0 | 0.97x | 100 / 100 | rows and values match |
| q22 | OK | 2839.7 | 2620.9 | 5831.2 | 0.45x | 100 / 100 | rows and values match |
| q23 | OK | 4361.3 | 4207.4 | 2941.9 | 1.43x | 100 / 100 | rows and values match |
| q24 | OK | 2011.7 | 1264.2 | 535.0 | 2.36x | 5 / 5 | rows and values match |
| q25 | OK | 166.5 | 156.1 | 142.0 | 1.10x | 2 / 2 | rows and values match |
| q26 | OK | 136.0 | 128.1 | 212.0 | 0.60x | 100 / 100 | rows and values match |
| q27 | OK | 404.4 | 306.6 | 246.8 | 1.24x | 100 / 100 | rows and values match |
| q28 | OK | 374.5 | 339.6 | 338.7 | 1.00x | 1 / 1 | rows and values match |
| q29 | OK | 194.5 | 188.6 | 205.6 | 0.92x | 8 / 8 | rows and values match |
| q30 | OK | 227.5 | 204.7 | 233.4 | 0.88x | 100 / 100 | rows and values match |
| q31 | OK | 426.3 | 416.3 | 170.0 | 2.45x | 307 / 307 | rows and values match |
| q32 | OK | 47.1 | 48.8 | 30.5 | 1.60x | 1 / 1 | rows and values match |
| q33 | OK | 210.6 | 204.8 | 173.0 | 1.18x | 100 / 100 | rows and values match |
| q34 | OK | 150.4 | 136.1 | 139.4 | 0.98x | 1560 / 1560 | rows and values match |
| q35 | OK | 648.3 | 704.9 | 506.2 | 1.39x | 100 / 100 | rows and values match |
| q36 | OK | 173.2 | 167.2 | 125.0 | 1.34x | 100 / 100 | rows and values match |
| q37 | OK | 51.2 | 46.5 | 170.7 | 0.27x | 2 / 2 | rows and values match |
| q38 | OK | 666.4 | 563.5 | 384.8 | 1.46x | 1 / 1 | rows and values match |
| q39 | OK | 1709.6 | 1700.6 | 410.9 | 4.14x | 2433 / 2433 | rows and values match |
| q40 | OK | 109.0 | 97.9 | 66.2 | 1.48x | 100 / 100 | rows and values match |
| q41 | OK | 46.9 | 46.2 | 47.2 | 0.98x | 41 / 41 | rows and values match |
| q42 | OK | 86.8 | 77.2 | 61.7 | 1.25x | 11 / 11 | rows and values match |
| q43 | OK | 125.6 | 118.4 | 100.2 | 1.18x | 18 / 18 | rows and values match |
| q44 | OK | 160.3 | 172.3 | 121.2 | 1.42x | 10 / 10 | rows and values match |
| q45 | OK | 481.9 | 465.5 | 143.6 | 3.24x | 49 / 49 | rows and values match |
| q46 | OK | 463.9 | 455.0 | 236.9 | 1.92x | 100 / 100 | rows and values match |
| q47 | OK | 1050.8 | 1012.5 | 917.2 | 1.10x | 100 / 100 | rows and values match |
| q48 | OK | 386.8 | 379.8 | 349.5 | 1.09x | 1 / 1 | rows and values match |
| q49 | OK | 134.9 | 126.4 | 96.8 | 1.31x | 46 / 46 | rows and values match |
| q50 | OK | 124.8 | 119.1 | 239.7 | 0.50x | 51 / 51 | rows and values match |
| q51 | OK | 1834.7 | 1757.1 | 1753.9 | 1.00x | 100 / 100 | rows and values match |
| q52 | OK | 67.4 | 73.3 | 66.2 | 1.11x | 100 / 100 | rows and values match |
| q53 | OK | 162.3 | 157.3 | 99.0 | 1.59x | 100 / 100 | rows and values match |
| q54 | OK | 238.5 | 225.5 | 189.4 | 1.19x | 1 / 1 | rows and values match |
| q55 | OK | 74.8 | 71.4 | 65.6 | 1.09x | 100 / 100 | rows and values match |
| q56 | OK | 172.1 | 170.0 | 176.8 | 0.96x | 100 / 100 | rows and values match |
| q57 | OK | 521.4 | 602.6 | 380.5 | 1.58x | 100 / 100 | rows and values match |
| q58 | OK | 207.2 | 177.6 | 149.5 | 1.19x | 31 / 31 | rows and values match |
| q59 | OK | 1885.1 | 1413.2 | 421.9 | 3.35x | 100 / 100 | rows and values match |
| q60 | OK | 239.3 | 250.1 | 181.9 | 1.37x | 100 / 100 | rows and values match |
| q61 | OK | 394.3 | 373.1 | 242.3 | 1.54x | 1 / 1 | rows and values match |
| q62 | OK | 75.3 | 62.8 | 468.0 | 0.13x | 100 / 100 | rows and values match |
| q63 | OK | 166.0 | 164.8 | 84.7 | 1.95x | 100 / 100 | rows and values match |
| q64 | OK | 759.4 | 733.2 | - | - | 28 / 28 | rows and values match |
| q65 | OK | 1316.8 | 1269.9 | 508.5 | 2.50x | 100 / 100 | rows and values match |
| q66 | OK | 146.9 | 112.5 | 112.2 | 1.00x | 10 / 10 | rows and values match |
| q67 | OK | 3900.5 | 3768.0 | 5919.1 | 0.64x | 100 / 100 | rows and values match |
| q68 | OK | 356.2 | 371.9 | 232.1 | 1.60x | 100 / 100 | rows and values match |
| q69 | OK | 479.7 | 480.7 | 353.4 | 1.36x | 100 / 100 | rows and values match |
| q70 | OK | 914.1 | 922.1 | 187.9 | 4.91x | 7 / 7 | rows and values match |
| q71 | OK | 204.2 | 168.3 | 150.2 | 1.12x | 9655 / 9655 | rows and values match |
| q72 | OK | 6742.9 | 5778.9 | 24992.5 | 0.23x | 100 / 100 | rows and values match |
| q73 | OK | 121.0 | 116.8 | 130.4 | 0.90x | 17 / 17 | rows and values match |
| q74 | OK | 1342.1 | 1215.0 | 518.7 | 2.34x | 100 / 100 | rows and values match |
| q75 | OK | 1058.0 | 1082.1 | 498.3 | 2.17x | 100 / 100 | rows and values match |
| q76 | OK | 379.2 | 319.9 | 135.3 | 2.36x | 100 / 100 | rows and values match |
| q77 | OK | 135.9 | 107.3 | 97.5 | 1.10x | 100 / 100 | rows and values match |
| q78 | OK | 3734.8 | 3432.8 | 1234.4 | 2.78x | 100 / 100 | rows and values match |
| q79 | OK | 242.5 | 235.6 | 172.9 | 1.36x | 100 / 100 | rows and values match |
| q80 | OK | 586.7 | 572.3 | 247.2 | 2.32x | 100 / 100 | rows and values match |
| q81 | OK | 446.2 | 500.4 | 226.0 | 2.21x | 100 / 100 | rows and values match |
| q82 | OK | 69.1 | 68.9 | 320.9 | 0.21x | 6 / 6 | rows and values match |
| q83 | OK | 89.2 | 101.8 | 90.2 | 1.13x | 100 / 100 | rows and values match |
| q84 | OK | 127.6 | 121.0 | 193.3 | 0.63x | 100 / 100 | rows and values match |
| q85 | OK | 529.5 | 487.8 | 526.9 | 0.93x | 15 / 15 | rows and values match |
| q86 | OK | 173.7 | 158.5 | 72.3 | 2.19x | 100 / 100 | rows and values match |
| q87 | OK | 751.3 | 725.2 | 435.1 | 1.67x | 1 / 1 | rows and values match |
| q88 | OK | 446.1 | 411.1 | 448.4 | 0.92x | 1 / 1 | rows and values match |
| q89 | OK | 334.5 | 258.7 | 123.2 | 2.10x | 100 / 100 | rows and values match |
| q90 | OK | 57.0 | 44.2 | 63.3 | 0.70x | 1 / 1 | rows and values match |
| q91 | OK | 153.7 | 149.9 | 367.7 | 0.41x | 12 / 12 | rows and values match |
| q92 | OK | 54.9 | 57.2 | 41.9 | 1.37x | 1 / 1 | rows and values match |
| q93 | OK | 107.0 | 105.1 | 297.0 | 0.35x | 100 / 100 | rows and values match |
| q94 | OK | 162.8 | 138.7 | 67.7 | 2.05x | 1 / 1 | rows and values match |
| q95 | OK | 3492.8 | 2627.1 | 956.8 | 2.75x | 1 / 1 | rows and values match |
| q96 | OK | 48.6 | 45.3 | 54.0 | 0.84x | 1 / 1 | rows and values match |
| q97 | OK | 476.5 | 421.4 | 333.0 | 1.27x | 1 / 1 | rows and values match |
| q98 | OK | 175.3 | 198.6 | 144.1 | 1.38x | 15076 / 15076 | rows and values match |
| q99 | OK | 108.7 | 76.3 | 910.8 | 0.08x | 100 / 100 | rows and values match |
