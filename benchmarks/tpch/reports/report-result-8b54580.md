# TPC-H benchmark report

Commit: `8b54580` - tests(M7): join-reordering e2e differential suite + SF1 experiment gate
Generated: 2026-07-05 15:09
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.
Scale factors: SF0.1 (600K lineitem), SF1 (6M lineitem).

## Methodology

Timing is the median of three warm runs per query; correctness is a differential row-by-row check against DuckDB on the same data (numbers rounded to 2 decimals). Each query runs in its own subprocess with an RSS cap, so a blow-up is a KILLED row, not a lost run. Ratio is ours/DuckDB (higher = we are slower).

- **single** - Single source (pure engine, Parquet).
- **fedparquet** - Federated, 2 Parquet sources (DuckDB monolithic - unfair); DuckDB reads all files as one, so this overstates our cost.
- **fedpgduck** - Federated, PostgreSQL + DuckDB (both federate - fair); the DuckDB oracle federates the same split via its postgres connector. This is the honest federated number.

## Summary

Totals cover only queries that produced a measurement; TIMEOUT / KILLED / ERROR queries are excluded from the ms totals but counted as not-correct. `measured` is how many of the 22 timed cleanly.

| Cell | SF | Correct | Ours (ms) | DuckDB (ms) | Ratio | Measured |
| --- | --- | --- | --- | --- | --- | --- |
| single | 0.1 | 22/22 | 842.0 | 341.4 | 2.47x | 22 |
| single | 1 | 22/22 | 7649.1 | 1360.6 | 5.62x | 22 |
| fedparquet | 0.1 | 22/22 | 1668.0 | 329.6 | 5.06x | 22 |
| fedparquet | 1 | 22/22 | 13210.7 | 1330.2 | 9.93x | 22 |
| fedpgduck | 0.1 | 22/22 | 3200.2 | 283.5 | 11.29x | 22 |
| fedpgduck | 1 | 22/22 | 24799.8 | 1067.0 | 23.24x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.3 | 16.0 | 1.83x | yes | 4 / 4 |
| q02 | 24.5 | 12.6 | 1.94x | yes | 44 / 44 |
| q03 | 22.9 | 14.4 | 1.59x | yes | 10 / 10 |
| q04 | 14.4 | 13.9 | 1.04x | yes | 5 / 5 |
| q05 | 24.0 | 13.6 | 1.77x | yes | 5 / 5 |
| q06 | 12.2 | 5.8 | 2.09x | yes | 1 / 1 |
| q07 | 114.6 | 16.7 | 6.84x | yes | 4 / 4 |
| q08 | 30.3 | 17.7 | 1.71x | yes | 2 / 2 |
| q09 | 71.3 | 24.7 | 2.88x | yes | 175 / 175 |
| q10 | 30.5 | 19.8 | 1.54x | yes | 20 / 20 |
| q11 | 35.8 | 18.4 | 1.94x | yes | 2541 / 2541 |
| q12 | 20.1 | 9.9 | 2.03x | yes | 2 / 2 |
| q13 | 25.2 | 28.6 | 0.88x | yes | 37 / 37 |
| q14 | 14.8 | 9.0 | 1.65x | yes | 1 / 1 |
| q15 | 66.9 | 7.3 | 9.19x | yes | 1 / 1 |
| q16 | 32.4 | 16.1 | 2.01x | yes | 2762 / 2762 |
| q17 | 23.2 | 12.2 | 1.90x | yes | 1 / 1 |
| q18 | 151.2 | 19.3 | 7.82x | yes | 5 / 5 |
| q19 | 23.4 | 11.8 | 1.99x | yes | 1 / 1 |
| q20 | 26.5 | 13.4 | 1.97x | yes | 9 / 9 |
| q21 | 29.9 | 30.0 | 1.00x | yes | 47 / 47 |
| q22 | 18.6 | 10.2 | 1.82x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.0 | 65.2 | 3.30x | yes | 4 / 4 |
| q02 | 37.1 | 23.6 | 1.57x | yes | 100 / 100 |
| q03 | 87.7 | 55.2 | 1.59x | yes | 10 / 10 |
| q04 | 38.6 | 32.5 | 1.19x | yes | 5 / 5 |
| q05 | 146.5 | 51.6 | 2.84x | yes | 5 / 5 |
| q06 | 43.2 | 22.3 | 1.93x | yes | 1 / 1 |
| q07 | 740.4 | 59.9 | 12.35x | yes | 4 / 4 |
| q08 | 92.5 | 63.9 | 1.45x | yes | 2 / 2 |
| q09 | 447.6 | 134.2 | 3.33x | yes | 175 / 175 |
| q10 | 128.0 | 93.9 | 1.36x | yes | 20 / 20 |
| q11 | 36.0 | 21.7 | 1.66x | yes | 1048 / 1048 |
| q12 | 62.5 | 33.3 | 1.88x | yes | 2 / 2 |
| q13 | 81.4 | 95.2 | 0.86x | yes | 42 / 42 |
| q14 | 56.9 | 43.1 | 1.32x | yes | 1 / 1 |
| q15 | 3135.7 | 29.8 | 105.34x | yes | 1 / 1 |
| q16 | 168.2 | 63.2 | 2.66x | yes | 18314 / 18314 |
| q17 | 286.3 | 48.8 | 5.86x | yes | 1 / 1 |
| q18 | 1448.4 | 99.9 | 14.50x | yes | 57 / 57 |
| q19 | 83.2 | 63.0 | 1.32x | yes | 1 / 1 |
| q20 | 137.0 | 49.4 | 2.77x | yes | 186 / 186 |
| q21 | 129.3 | 167.6 | 0.77x | yes | 100 / 100 |
| q22 | 47.6 | 43.2 | 1.10x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.0 | 15.3 | 2.03x | yes | 4 / 4 |
| q02 | 60.5 | 12.8 | 4.74x | yes | 44 / 44 |
| q03 | 94.3 | 13.9 | 6.79x | yes | 10 / 10 |
| q04 | 15.3 | 10.6 | 1.44x | yes | 5 / 5 |
| q05 | 139.0 | 15.8 | 8.81x | yes | 5 / 5 |
| q06 | 12.3 | 5.9 | 2.09x | yes | 1 / 1 |
| q07 | 120.0 | 16.6 | 7.24x | yes | 4 / 4 |
| q08 | 118.7 | 18.5 | 6.41x | yes | 2 / 2 |
| q09 | 305.5 | 24.8 | 12.33x | yes | 175 / 175 |
| q10 | 87.7 | 19.8 | 4.44x | yes | 20 / 20 |
| q11 | 62.0 | 18.2 | 3.40x | yes | 2541 / 2541 |
| q12 | 18.7 | 10.1 | 1.84x | yes | 2 / 2 |
| q13 | 41.9 | 25.6 | 1.63x | yes | 37 / 37 |
| q14 | 15.3 | 8.1 | 1.89x | yes | 1 / 1 |
| q15 | 63.9 | 6.8 | 9.44x | yes | 1 / 1 |
| q16 | 62.6 | 13.4 | 4.65x | yes | 2762 / 2762 |
| q17 | 30.1 | 12.3 | 2.44x | yes | 1 / 1 |
| q18 | 191.4 | 19.2 | 9.96x | yes | 5 / 5 |
| q19 | 23.3 | 11.0 | 2.12x | yes | 1 / 1 |
| q20 | 28.3 | 11.8 | 2.39x | yes | 9 / 9 |
| q21 | 114.1 | 30.1 | 3.79x | yes | 47 / 47 |
| q22 | 32.3 | 9.0 | 3.59x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.3 | 65.4 | 3.29x | yes | 4 / 4 |
| q02 | 140.3 | 24.8 | 5.67x | yes | 100 / 100 |
| q03 | 667.8 | 53.5 | 12.48x | yes | 10 / 10 |
| q04 | 36.5 | 32.9 | 1.11x | yes | 5 / 5 |
| q05 | 1149.0 | 49.8 | 23.07x | yes | 5 / 5 |
| q06 | 39.5 | 22.0 | 1.79x | yes | 1 / 1 |
| q07 | 701.3 | 54.8 | 12.79x | yes | 4 / 4 |
| q08 | 442.4 | 65.4 | 6.76x | yes | 2 / 2 |
| q09 | 2122.4 | 131.7 | 16.11x | yes | 175 / 175 |
| q10 | 554.0 | 90.6 | 6.11x | yes | 20 / 20 |
| q11 | 114.5 | 18.9 | 6.05x | yes | 1048 / 1048 |
| q12 | 60.9 | 30.9 | 1.97x | yes | 2 / 2 |
| q13 | 311.2 | 91.5 | 3.40x | yes | 42 / 42 |
| q14 | 54.6 | 41.6 | 1.31x | yes | 1 / 1 |
| q15 | 3288.3 | 32.4 | 101.43x | yes | 1 / 1 |
| q16 | 253.5 | 60.2 | 4.21x | yes | 18314 / 18314 |
| q17 | 265.1 | 49.7 | 5.33x | yes | 1 / 1 |
| q18 | 1675.8 | 101.4 | 16.53x | yes | 57 / 57 |
| q19 | 98.4 | 61.3 | 1.60x | yes | 1 / 1 |
| q20 | 111.1 | 47.4 | 2.35x | yes | 186 / 186 |
| q21 | 807.4 | 167.7 | 4.81x | yes | 100 / 100 |
| q22 | 101.5 | 36.2 | 2.81x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.7 | 7.7 | 3.09x | yes | 4 / 4 |
| q02 | 98.6 | 11.9 | 8.29x | yes | 44 / 44 |
| q03 | 160.5 | 10.1 | 15.93x | yes | 10 / 10 |
| q04 | 25.4 | 7.5 | 3.37x | yes | 5 / 5 |
| q05 | 307.0 | 16.7 | 18.40x | yes | 5 / 5 |
| q06 | 15.9 | 2.0 | 7.97x | yes | 1 / 1 |
| q07 | 200.0 | 13.7 | 14.55x | yes | 4 / 4 |
| q08 | 152.8 | 16.5 | 9.25x | yes | 2 / 2 |
| q09 | 588.4 | 24.5 | 24.00x | yes | 175 / 175 |
| q10 | 136.1 | 19.4 | 7.03x | yes | 20 / 20 |
| q11 | 84.4 | 11.3 | 7.46x | yes | 2541 / 2541 |
| q12 | 22.2 | 5.7 | 3.92x | yes | 2 / 2 |
| q13 | 113.0 | 21.5 | 5.27x | yes | 37 / 37 |
| q14 | 31.6 | 9.2 | 3.45x | yes | 1 / 1 |
| q15 | 131.1 | 4.9 | 26.87x | yes | 1 / 1 |
| q16 | 58.4 | 13.2 | 4.42x | yes | 2762 / 2762 |
| q17 | 54.4 | 9.0 | 6.02x | yes | 1 / 1 |
| q18 | 437.5 | 16.8 | 26.03x | yes | 5 / 5 |
| q19 | 39.5 | 13.8 | 2.86x | yes | 1 / 1 |
| q20 | 57.6 | 11.1 | 5.19x | yes | 9 / 9 |
| q21 | 379.1 | 20.2 | 18.77x | yes | 47 / 47 |
| q22 | 82.9 | 16.8 | 4.93x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.9 | 26.3 | 1.89x | yes | 4 / 4 |
| q02 | 269.5 | 39.4 | 6.84x | yes | 100 / 100 |
| q03 | 1406.6 | 33.2 | 42.32x | yes | 10 / 10 |
| q04 | 44.4 | 22.2 | 2.00x | yes | 5 / 5 |
| q05 | 2572.9 | 45.5 | 56.50x | yes | 5 / 5 |
| q06 | 29.3 | 6.9 | 4.24x | yes | 1 / 1 |
| q07 | 1221.5 | 43.9 | 27.80x | yes | 4 / 4 |
| q08 | 530.0 | 53.4 | 9.92x | yes | 2 / 2 |
| q09 | 4621.5 | 89.5 | 51.63x | yes | 175 / 175 |
| q10 | 923.7 | 66.9 | 13.81x | yes | 20 / 20 |
| q11 | 174.2 | 15.9 | 10.97x | yes | 1048 / 1048 |
| q12 | 42.9 | 16.7 | 2.57x | yes | 2 / 2 |
| q13 | 784.6 | 104.0 | 7.55x | yes | 42 / 42 |
| q14 | 102.5 | 41.3 | 2.48x | yes | 1 / 1 |
| q15 | 3562.2 | 20.3 | 175.09x | yes | 1 / 1 |
| q16 | 325.8 | 54.3 | 6.00x | yes | 18314 / 18314 |
| q17 | 391.7 | 26.5 | 14.76x | yes | 1 / 1 |
| q18 | 3772.2 | 85.3 | 44.23x | yes | 57 / 57 |
| q19 | 129.4 | 61.3 | 2.11x | yes | 1 / 1 |
| q20 | 129.3 | 33.6 | 3.84x | yes | 186 / 186 |
| q21 | 3288.0 | 124.2 | 26.48x | yes | 100 / 100 |
| q22 | 427.8 | 56.1 | 7.63x | yes | 7 / 7 |

