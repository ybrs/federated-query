# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: adversarial (sales facts split from their matching returns facts, dimensions alternated (the retired Python-engine harness's adversarial assignment)).
Truth: cached pure-DuckDB references in references_sf1.duckdb.
Baseline: cached oracle_timings from the same references file, measured once by save-refs over the pg-dims split.
Generated: 2026-07-13 19:19

Tally: 99 ok | 0 wrong | 0 error   (total 99 queries, 30.1s)
Timing: ours 29.3s  duckdb 11.1s  ->  total 2.63x  geomean 1.87x  (99 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 29.3 | 11.1 | 2.63x | 1.87x | 99 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | OK | 267.5 | - | 54.5 | 4.90x | 100 / 100 | rows and values match |
| q02 | OK | 460.0 | - | 60.5 | 7.60x | 2513 / 2513 | rows and values match |
| q03 | OK | 39.9 | - | 29.1 | 1.37x | 89 / 89 | rows and values match |
| q04 | OK | 1084.7 | - | 183.9 | 5.90x | 6 / 6 | rows and values match |
| q05 | OK | 162.6 | - | 41.8 | 3.89x | 100 / 100 | rows and values match |
| q06 | OK | 145.2 | - | 92.4 | 1.57x | 46 / 46 | rows and values match |
| q07 | OK | 106.4 | - | 176.9 | 0.60x | 100 / 100 | rows and values match |
| q08 | OK | 198.0 | - | 62.2 | 3.18x | 5 / 5 | rows and values match |
| q09 | OK | 46.3 | - | 38.8 | 1.19x | 1 / 1 | rows and values match |
| q10 | OK | 215.8 | - | 334.5 | 0.65x | 6 / 6 | rows and values match |
| q11 | OK | 273.2 | - | 129.5 | 2.11x | 90 / 90 | rows and values match |
| q12 | OK | 26.3 | - | 38.6 | 0.68x | 100 / 100 | rows and values match |
| q13 | OK | 185.4 | - | 192.1 | 0.97x | 1 / 1 | rows and values match |
| q14 | OK | 1245.6 | - | 356.0 | 3.50x | 100 / 100 | rows and values match |
| q15 | OK | 187.3 | - | 51.2 | 3.66x | 100 / 100 | rows and values match |
| q16 | OK | 303.7 | - | 34.3 | 8.84x | 1 / 1 | rows and values match |
| q17 | OK | 163.7 | - | 67.9 | 2.41x | 0 / 0 | rows and values match |
| q18 | OK | 482.1 | - | 330.5 | 1.46x | 100 / 100 | rows and values match |
| q19 | OK | 77.2 | - | 60.0 | 1.29x | 100 / 100 | rows and values match |
| q20 | OK | 99.9 | - | 37.5 | 2.66x | 100 / 100 | rows and values match |
| q21 | OK | 364.4 | - | 26.9 | 13.57x | 100 / 100 | rows and values match |
| q22 | OK | 1373.8 | - | 300.6 | 4.57x | 100 / 100 | rows and values match |
| q23 | OK | 646.4 | - | 296.4 | 2.18x | 4 / 4 | rows and values match |
| q24 | OK | 499.2 | - | 96.7 | 5.16x | 1 / 1 | rows and values match |
| q25 | OK | 126.7 | - | 71.6 | 1.77x | 1 / 1 | rows and values match |
| q26 | OK | 276.2 | - | 197.1 | 1.40x | 100 / 100 | rows and values match |
| q27 | OK | 131.1 | - | 190.7 | 0.69x | 100 / 100 | rows and values match |
| q28 | OK | 63.5 | - | 40.0 | 1.59x | 1 / 1 | rows and values match |
| q29 | OK | 179.2 | - | 119.3 | 1.50x | 1 / 1 | rows and values match |
| q30 | OK | 110.8 | - | 94.4 | 1.17x | 100 / 100 | rows and values match |
| q31 | OK | 87.1 | - | 71.6 | 1.22x | 44 / 44 | rows and values match |
| q32 | OK | 121.5 | - | 27.4 | 4.43x | 1 / 1 | rows and values match |
| q33 | OK | 137.5 | - | 108.3 | 1.27x | 100 / 100 | rows and values match |
| q34 | OK | 54.5 | - | 53.5 | 1.02x | 455 / 455 | rows and values match |
| q35 | OK | 484.2 | - | 287.3 | 1.69x | 100 / 100 | rows and values match |
| q36 | OK | 51.9 | - | 47.9 | 1.08x | 100 / 100 | rows and values match |
| q37 | OK | 538.3 | - | 27.1 | 19.87x | 1 / 1 | rows and values match |
| q38 | OK | 313.0 | - | 75.4 | 4.15x | 1 / 1 | rows and values match |
| q39 | OK | 5825.0 | - | 52.6 | 110.69x | 243 / 243 | rows and values match |
| q40 | OK | 163.1 | - | 29.3 | 5.57x | 100 / 100 | rows and values match |
| q41 | OK | 12.1 | - | 22.5 | 0.54x | 4 / 4 | rows and values match |
| q42 | OK | 21.1 | - | 27.3 | 0.77x | 10 / 10 | rows and values match |
| q43 | OK | 32.7 | - | 33.8 | 0.97x | 6 / 6 | rows and values match |
| q44 | OK | 55.7 | - | 31.2 | 1.79x | 10 / 10 | rows and values match |
| q45 | OK | 66.7 | - | 63.6 | 1.05x | 19 / 19 | rows and values match |
| q46 | OK | 118.3 | - | 81.5 | 1.45x | 100 / 100 | rows and values match |
| q47 | OK | 177.9 | - | 123.6 | 1.44x | 100 / 100 | rows and values match |
| q48 | OK | 144.0 | - | 210.1 | 0.69x | 1 / 1 | rows and values match |
| q49 | OK | 291.7 | - | 44.5 | 6.55x | 34 / 34 | rows and values match |
| q50 | OK | 182.7 | - | 66.2 | 2.76x | 6 / 6 | rows and values match |
| q51 | OK | 202.7 | - | 191.4 | 1.06x | 100 / 100 | rows and values match |
| q52 | OK | 23.2 | - | 28.7 | 0.81x | 100 / 100 | rows and values match |
| q53 | OK | 44.0 | - | 38.2 | 1.15x | 100 / 100 | rows and values match |
| q54 | OK | 192.5 | - | 74.4 | 2.59x | 1 / 1 | rows and values match |
| q55 | OK | 20.6 | - | 34.7 | 0.59x | 100 / 100 | rows and values match |
| q56 | OK | 142.7 | - | 109.4 | 1.31x | 100 / 100 | rows and values match |
| q57 | OK | 519.7 | - | 85.3 | 6.09x | 100 / 100 | rows and values match |
| q58 | OK | 188.1 | - | 84.3 | 2.23x | 5 / 5 | rows and values match |
| q59 | OK | 202.7 | - | 96.3 | 2.10x | 100 / 100 | rows and values match |
| q60 | OK | 145.0 | - | 127.9 | 1.13x | 100 / 100 | rows and values match |
| q61 | OK | 72.1 | - | 83.4 | 0.87x | 1 / 1 | rows and values match |
| q62 | OK | 34.2 | - | 81.2 | 0.42x | 100 / 100 | rows and values match |
| q63 | OK | 38.3 | - | 48.5 | 0.79x | 100 / 100 | rows and values match |
| q64 | OK | 691.7 | - | 419.7 | 1.65x | 2 / 2 | rows and values match |
| q65 | OK | 71.4 | - | 64.8 | 1.10x | 100 / 100 | rows and values match |
| q66 | OK | 231.7 | - | 89.1 | 2.60x | 5 / 5 | rows and values match |
| q67 | OK | 326.6 | - | 443.2 | 0.74x | 100 / 100 | rows and values match |
| q68 | OK | 97.4 | - | 78.2 | 1.24x | 100 / 100 | rows and values match |
| q69 | OK | 493.2 | - | 277.8 | 1.78x | 100 / 100 | rows and values match |
| q70 | OK | 79.5 | - | 48.2 | 1.65x | 3 / 3 | rows and values match |
| q71 | OK | 150.6 | - | 76.7 | 1.96x | 1031 / 1031 | rows and values match |
| q72 | OK | 1428.9 | - | 781.4 | 1.83x | 100 / 100 | rows and values match |
| q73 | OK | 65.5 | - | 59.2 | 1.11x | 1 / 1 | rows and values match |
| q74 | OK | 171.4 | - | 86.4 | 1.98x | 92 / 92 | rows and values match |
| q75 | OK | 590.2 | - | 96.6 | 6.11x | 100 / 100 | rows and values match |
| q76 | OK | 118.4 | - | 58.3 | 2.03x | 100 / 100 | rows and values match |
| q77 | OK | 153.5 | - | 46.5 | 3.30x | 44 / 44 | rows and values match |
| q78 | OK | 760.3 | - | 139.8 | 5.44x | 100 / 100 | rows and values match |
| q79 | OK | 54.5 | - | 49.7 | 1.10x | 100 / 100 | rows and values match |
| q80 | OK | 301.7 | - | 74.8 | 4.04x | 100 / 100 | rows and values match |
| q81 | OK | 93.4 | - | 106.6 | 0.88x | 100 / 100 | rows and values match |
| q82 | OK | 360.6 | - | 47.6 | 7.58x | 2 / 2 | rows and values match |
| q83 | OK | 80.7 | - | 78.7 | 1.03x | 24 / 24 | rows and values match |
| q84 | OK | 94.9 | - | 132.0 | 0.72x | 16 / 16 | rows and values match |
| q85 | OK | 345.9 | - | 346.7 | 1.00x | 1 / 1 | rows and values match |
| q86 | OK | 33.7 | - | 31.9 | 1.06x | 100 / 100 | rows and values match |
| q87 | OK | 327.9 | - | 84.5 | 3.88x | 1 / 1 | rows and values match |
| q88 | OK | 154.0 | - | 220.8 | 0.70x | 1 / 1 | rows and values match |
| q89 | OK | 57.4 | - | 38.0 | 1.51x | 100 / 100 | rows and values match |
| q90 | OK | 38.1 | - | 42.3 | 0.90x | 1 / 1 | rows and values match |
| q91 | OK | 138.3 | - | 195.3 | 0.71x | 2 / 2 | rows and values match |
| q92 | OK | 26.9 | - | 31.3 | 0.86x | 1 / 1 | rows and values match |
| q93 | OK | 257.8 | - | 30.1 | 8.57x | 100 / 100 | rows and values match |
| q94 | OK | 76.7 | - | 35.3 | 2.17x | 1 / 1 | rows and values match |
| q95 | OK | 299.1 | - | 197.5 | 1.51x | 1 / 1 | rows and values match |
| q96 | OK | 18.8 | - | 31.9 | 0.59x | 1 / 1 | rows and values match |
| q97 | OK | 189.0 | - | 60.0 | 3.15x | 1 / 1 | rows and values match |
| q98 | OK | 36.4 | - | 47.2 | 0.77x | 2521 / 2521 | rows and values match |
| q99 | OK | 644.6 | - | 112.7 | 5.72x | 90 / 90 | rows and values match |
