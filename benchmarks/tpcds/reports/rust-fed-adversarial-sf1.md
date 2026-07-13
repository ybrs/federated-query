# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 17:38

Tally: 96 ok | 0 wrong | 3 error   (total 99 queries, 45.9s)
Timing: ours 21.9s  duckdb 10.9s  ->  total 2.02x  geomean 1.65x  (96 OK queries measured)

## Non-OK queries grouped by reason

### RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "X" does not exist (3) [ERROR]
Queries: q04, q21, q39

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 21.9 | 10.9 | 2.02x | 1.65x | 96 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 282.2 | 115.8 | 54.5 | 2.12x | 100 / 100 | rows and values match |
| q02 | OK | 487.3 | 380.9 | 60.5 | 6.29x | 2513 / 2513 | rows and values match |
| q03 | OK | 73.6 | 26.1 | 29.1 | 0.90x | 89 / 89 | rows and values match |
| q04 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q05 | OK | 154.9 | 152.7 | 41.8 | 3.66x | 100 / 100 | rows and values match |
| q06 | OK | 157.1 | 132.5 | 92.4 | 1.43x | 46 / 46 | rows and values match |
| q07 | OK | 122.7 | 128.1 | 176.9 | 0.72x | 100 / 100 | rows and values match |
| q08 | OK | 207.1 | 198.0 | 62.2 | 3.18x | 5 / 5 | rows and values match |
| q09 | OK | 43.9 | 42.7 | 38.8 | 1.10x | 1 / 1 | rows and values match |
| q10 | OK | 184.6 | 160.0 | 334.5 | 0.48x | 6 / 6 | rows and values match |
| q11 | OK | 279.3 | 287.5 | 129.5 | 2.22x | 90 / 90 | rows and values match |
| q12 | OK | 30.5 | 24.0 | 38.6 | 0.62x | 100 / 100 | rows and values match |
| q13 | OK | 177.7 | 165.3 | 192.1 | 0.86x | 1 / 1 | rows and values match |
| q14 | OK | 1273.3 | 1392.1 | 356.0 | 3.91x | 100 / 100 | rows and values match |
| q15 | OK | 209.3 | 190.7 | 51.2 | 3.72x | 100 / 100 | rows and values match |
| q16 | OK | 315.5 | 310.3 | 34.3 | 9.03x | 1 / 1 | rows and values match |
| q17 | OK | 155.7 | 150.4 | 67.9 | 2.22x | 0 / 0 | rows and values match |
| q18 | OK | 480.6 | 432.6 | 330.5 | 1.31x | 100 / 100 | rows and values match |
| q19 | OK | 68.5 | 52.6 | 60.0 | 0.88x | 100 / 100 | rows and values match |
| q20 | OK | 116.5 | 93.1 | 37.5 | 2.48x | 100 / 100 | rows and values match |
| q21 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q22 | OK | 1517.8 | 1362.3 | 300.6 | 4.53x | 100 / 100 | rows and values match |
| q23 | OK | 671.0 | 609.4 | 296.4 | 2.06x | 4 / 4 | rows and values match |
| q24 | OK | 523.5 | 488.7 | 96.7 | 5.05x | 1 / 1 | rows and values match |
| q25 | OK | 134.5 | 137.1 | 71.6 | 1.92x | 1 / 1 | rows and values match |
| q26 | OK | 327.2 | 315.2 | 197.1 | 1.60x | 100 / 100 | rows and values match |
| q27 | OK | 155.8 | 151.7 | 190.7 | 0.80x | 100 / 100 | rows and values match |
| q28 | OK | 59.9 | 52.6 | 40.0 | 1.31x | 1 / 1 | rows and values match |
| q29 | OK | 184.5 | 184.2 | 119.3 | 1.54x | 1 / 1 | rows and values match |
| q30 | OK | 118.4 | 106.9 | 94.4 | 1.13x | 100 / 100 | rows and values match |
| q31 | OK | 91.1 | 88.5 | 71.6 | 1.24x | 44 / 44 | rows and values match |
| q32 | OK | 124.8 | 119.3 | 27.4 | 4.35x | 1 / 1 | rows and values match |
| q33 | OK | 149.3 | 138.9 | 108.3 | 1.28x | 100 / 100 | rows and values match |
| q34 | OK | 55.8 | 44.3 | 53.5 | 0.83x | 455 / 455 | rows and values match |
| q35 | OK | 477.4 | 471.9 | 287.3 | 1.64x | 100 / 100 | rows and values match |
| q36 | OK | 45.8 | 45.0 | 47.9 | 0.94x | 100 / 100 | rows and values match |
| q37 | OK | 593.0 | 566.0 | 27.1 | 20.89x | 1 / 1 | rows and values match |
| q38 | OK | 342.9 | 344.4 | 75.4 | 4.57x | 1 / 1 | rows and values match |
| q39 | ERROR | - | - | - | - | - | RuntimeError: execution error: adbc execute: InvalidArguments: Failed to prepare query: ERROR:  type "double" does not exist |
| q40 | OK | 176.9 | 157.5 | 29.3 | 5.37x | 100 / 100 | rows and values match |
| q41 | OK | 13.2 | 10.2 | 22.5 | 0.45x | 4 / 4 | rows and values match |
| q42 | OK | 21.6 | 20.5 | 27.3 | 0.75x | 10 / 10 | rows and values match |
| q43 | OK | 31.0 | 30.6 | 33.8 | 0.91x | 6 / 6 | rows and values match |
| q44 | OK | 50.9 | 48.0 | 31.2 | 1.54x | 10 / 10 | rows and values match |
| q45 | OK | 63.2 | 60.9 | 63.6 | 0.96x | 19 / 19 | rows and values match |
| q46 | OK | 118.5 | 112.4 | 81.5 | 1.38x | 100 / 100 | rows and values match |
| q47 | OK | 174.3 | 166.9 | 123.6 | 1.35x | 100 / 100 | rows and values match |
| q48 | OK | 138.8 | 137.4 | 210.1 | 0.65x | 1 / 1 | rows and values match |
| q49 | OK | 312.7 | 289.4 | 44.5 | 6.50x | 34 / 34 | rows and values match |
| q50 | OK | 183.3 | 119.1 | 66.2 | 1.80x | 6 / 6 | rows and values match |
| q51 | OK | 204.9 | 212.3 | 191.4 | 1.11x | 100 / 100 | rows and values match |
| q52 | OK | 25.4 | 20.0 | 28.7 | 0.70x | 100 / 100 | rows and values match |
| q53 | OK | 41.1 | 36.5 | 38.2 | 0.96x | 100 / 100 | rows and values match |
| q54 | OK | 200.9 | 189.4 | 74.4 | 2.55x | 1 / 1 | rows and values match |
| q55 | OK | 23.6 | 19.1 | 34.7 | 0.55x | 100 / 100 | rows and values match |
| q56 | OK | 152.0 | 146.9 | 109.4 | 1.34x | 100 / 100 | rows and values match |
| q57 | OK | 482.7 | 454.4 | 85.3 | 5.33x | 100 / 100 | rows and values match |
| q58 | OK | 185.4 | 193.8 | 84.3 | 2.30x | 5 / 5 | rows and values match |
| q59 | OK | 200.8 | 186.1 | 96.3 | 1.93x | 100 / 100 | rows and values match |
| q60 | OK | 151.0 | 148.4 | 127.9 | 1.16x | 100 / 100 | rows and values match |
| q61 | OK | 77.3 | 77.8 | 83.4 | 0.93x | 1 / 1 | rows and values match |
| q62 | OK | 38.0 | 27.4 | 81.2 | 0.34x | 100 / 100 | rows and values match |
| q63 | OK | 44.1 | 45.2 | 48.5 | 0.93x | 100 / 100 | rows and values match |
| q64 | OK | 764.8 | 698.0 | 419.7 | 1.66x | 2 / 2 | rows and values match |
| q65 | OK | 77.6 | 61.6 | 64.8 | 0.95x | 100 / 100 | rows and values match |
| q66 | OK | 243.8 | 222.5 | 89.1 | 2.50x | 5 / 5 | rows and values match |
| q67 | OK | 327.1 | 319.6 | 443.2 | 0.72x | 100 / 100 | rows and values match |
| q68 | OK | 99.6 | 113.2 | 78.2 | 1.45x | 100 / 100 | rows and values match |
| q69 | OK | 505.0 | 479.0 | 277.8 | 1.72x | 100 / 100 | rows and values match |
| q70 | OK | 80.8 | 77.9 | 48.2 | 1.62x | 3 / 3 | rows and values match |
| q71 | OK | 149.2 | 136.0 | 76.7 | 1.77x | 1031 / 1031 | rows and values match |
| q72 | OK | 1425.1 | 1705.6 | 781.4 | 2.18x | 100 / 100 | rows and values match |
| q73 | OK | 53.0 | 43.4 | 59.2 | 0.73x | 1 / 1 | rows and values match |
| q74 | OK | 186.7 | 320.2 | 86.4 | 3.71x | 92 / 92 | rows and values match |
| q75 | OK | 709.4 | 781.8 | 96.6 | 8.09x | 100 / 100 | rows and values match |
| q76 | OK | 141.8 | 125.1 | 58.3 | 2.14x | 100 / 100 | rows and values match |
| q77 | OK | 184.7 | 156.5 | 46.5 | 3.37x | 44 / 44 | rows and values match |
| q78 | OK | 837.6 | 764.7 | 139.8 | 5.47x | 100 / 100 | rows and values match |
| q79 | OK | 58.3 | 53.6 | 49.7 | 1.08x | 100 / 100 | rows and values match |
| q80 | OK | 289.6 | 288.8 | 74.8 | 3.86x | 100 / 100 | rows and values match |
| q81 | OK | 85.9 | 86.5 | 106.6 | 0.81x | 100 / 100 | rows and values match |
| q82 | OK | 303.4 | 305.5 | 47.6 | 6.42x | 2 / 2 | rows and values match |
| q83 | OK | 85.3 | 84.8 | 78.7 | 1.08x | 24 / 24 | rows and values match |
| q84 | OK | 100.8 | 90.8 | 132.0 | 0.69x | 16 / 16 | rows and values match |
| q85 | OK | 327.4 | 312.1 | 346.7 | 0.90x | 1 / 1 | rows and values match |
| q86 | OK | 33.7 | 33.6 | 31.9 | 1.05x | 100 / 100 | rows and values match |
| q87 | OK | 320.2 | 312.2 | 84.5 | 3.70x | 1 / 1 | rows and values match |
| q88 | OK | 158.5 | 120.4 | 220.8 | 0.55x | 1 / 1 | rows and values match |
| q89 | OK | 48.8 | 52.4 | 38.0 | 1.38x | 100 / 100 | rows and values match |
| q90 | OK | 39.9 | 27.4 | 42.3 | 0.65x | 1 / 1 | rows and values match |
| q91 | OK | 135.6 | 133.5 | 195.3 | 0.68x | 2 / 2 | rows and values match |
| q92 | OK | 28.3 | 26.8 | 31.3 | 0.86x | 1 / 1 | rows and values match |
| q93 | OK | 268.4 | 196.0 | 30.1 | 6.51x | 100 / 100 | rows and values match |
| q94 | OK | 72.5 | 66.9 | 35.3 | 1.89x | 1 / 1 | rows and values match |
| q95 | OK | 305.6 | 224.2 | 197.5 | 1.14x | 1 / 1 | rows and values match |
| q96 | OK | 21.4 | 17.4 | 31.9 | 0.55x | 1 / 1 | rows and values match |
| q97 | OK | 198.6 | 208.8 | 60.0 | 3.48x | 1 / 1 | rows and values match |
| q98 | OK | 37.7 | 45.1 | 47.2 | 0.96x | 2521 / 2521 | rows and values match |
| q99 | OK | 619.2 | 651.2 | 112.7 | 5.78x | 90 / 90 | rows and values match |
