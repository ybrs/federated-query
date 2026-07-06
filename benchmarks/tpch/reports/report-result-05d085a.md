# TPC-H benchmark report

Commit: `05d085a` - optimizer(cost): cap the composite-key NDV denominator at min(rows)
Generated: 2026-07-06 10:24
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
| single | 0.1 | 22/22 | 592.3 | 332.4 | 1.78x | 22 |
| single | 1 | 22/22 | 2311.2 | 1358.2 | 1.70x | 22 |
| fedparquet | 0.1 | 22/22 | 798.2 | 325.6 | 2.45x | 22 |
| fedparquet | 1 | 22/22 | 2668.4 | 1340.5 | 1.99x | 22 |
| fedpgduck | 0.1 | 22/22 | 1220.2 | 287.7 | 4.24x | 22 |
| fedpgduck | 1 | 22/22 | 3134.2 | 1053.2 | 2.98x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.9 | 15.5 | 1.94x | yes | 4 / 4 |
| q02 | 25.3 | 12.4 | 2.04x | yes | 44 / 44 |
| q03 | 22.7 | 13.8 | 1.64x | yes | 10 / 10 |
| q04 | 13.9 | 11.6 | 1.20x | yes | 5 / 5 |
| q05 | 23.8 | 15.3 | 1.55x | yes | 5 / 5 |
| q06 | 12.2 | 6.2 | 1.95x | yes | 1 / 1 |
| q07 | 30.6 | 15.8 | 1.94x | yes | 4 / 4 |
| q08 | 30.7 | 17.8 | 1.72x | yes | 2 / 2 |
| q09 | 36.3 | 25.7 | 1.41x | yes | 175 / 175 |
| q10 | 29.1 | 19.4 | 1.50x | yes | 20 / 20 |
| q11 | 33.3 | 19.1 | 1.74x | yes | 2541 / 2541 |
| q12 | 18.3 | 9.3 | 1.97x | yes | 2 / 2 |
| q13 | 24.6 | 24.9 | 0.99x | yes | 37 / 37 |
| q14 | 14.8 | 8.6 | 1.72x | yes | 1 / 1 |
| q15 | 35.5 | 7.4 | 4.82x | yes | 1 / 1 |
| q16 | 31.3 | 13.6 | 2.30x | yes | 2762 / 2762 |
| q17 | 23.7 | 11.9 | 2.00x | yes | 1 / 1 |
| q18 | 34.5 | 19.7 | 1.75x | yes | 5 / 5 |
| q19 | 30.9 | 12.4 | 2.49x | yes | 1 / 1 |
| q20 | 25.8 | 13.2 | 1.96x | yes | 9 / 9 |
| q21 | 46.3 | 29.9 | 1.55x | yes | 47 / 47 |
| q22 | 19.0 | 9.1 | 2.09x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 228.2 | 65.5 | 3.48x | yes | 4 / 4 |
| q02 | 37.3 | 23.8 | 1.57x | yes | 100 / 100 |
| q03 | 84.5 | 50.6 | 1.67x | yes | 10 / 10 |
| q04 | 42.4 | 37.2 | 1.14x | yes | 5 / 5 |
| q05 | 99.5 | 50.8 | 1.96x | yes | 5 / 5 |
| q06 | 41.3 | 21.7 | 1.91x | yes | 1 / 1 |
| q07 | 102.8 | 57.2 | 1.80x | yes | 4 / 4 |
| q08 | 88.6 | 64.4 | 1.38x | yes | 2 / 2 |
| q09 | 152.2 | 134.8 | 1.13x | yes | 175 / 175 |
| q10 | 130.9 | 93.8 | 1.40x | yes | 20 / 20 |
| q11 | 36.0 | 18.6 | 1.94x | yes | 1048 / 1048 |
| q12 | 62.7 | 34.0 | 1.84x | yes | 2 / 2 |
| q13 | 81.3 | 100.5 | 0.81x | yes | 42 / 42 |
| q14 | 51.4 | 45.1 | 1.14x | yes | 1 / 1 |
| q15 | 85.5 | 29.8 | 2.87x | yes | 1 / 1 |
| q16 | 156.2 | 63.9 | 2.44x | yes | 18314 / 18314 |
| q17 | 273.2 | 49.4 | 5.54x | yes | 1 / 1 |
| q18 | 138.9 | 103.5 | 1.34x | yes | 57 / 57 |
| q19 | 89.6 | 63.1 | 1.42x | yes | 1 / 1 |
| q20 | 138.2 | 49.5 | 2.79x | yes | 186 / 186 |
| q21 | 140.6 | 165.0 | 0.85x | yes | 100 / 100 |
| q22 | 49.8 | 36.0 | 1.38x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 22.8 | 14.9 | 1.52x | yes | 4 / 4 |
| q02 | 49.2 | 12.2 | 4.05x | yes | 44 / 44 |
| q03 | 27.1 | 13.1 | 2.06x | yes | 10 / 10 |
| q04 | 13.8 | 12.6 | 1.10x | yes | 5 / 5 |
| q05 | 53.3 | 14.7 | 3.61x | yes | 5 / 5 |
| q06 | 12.4 | 5.5 | 2.24x | yes | 1 / 1 |
| q07 | 64.7 | 16.3 | 3.98x | yes | 4 / 4 |
| q08 | 68.5 | 16.2 | 4.22x | yes | 2 / 2 |
| q09 | 79.2 | 24.0 | 3.30x | yes | 175 / 175 |
| q10 | 40.7 | 19.8 | 2.06x | yes | 20 / 20 |
| q11 | 41.7 | 17.9 | 2.33x | yes | 2541 / 2541 |
| q12 | 19.0 | 9.9 | 1.91x | yes | 2 / 2 |
| q13 | 23.3 | 24.9 | 0.94x | yes | 37 / 37 |
| q14 | 15.0 | 9.0 | 1.66x | yes | 1 / 1 |
| q15 | 35.3 | 7.4 | 4.79x | yes | 1 / 1 |
| q16 | 37.2 | 13.5 | 2.75x | yes | 2762 / 2762 |
| q17 | 24.2 | 12.1 | 2.00x | yes | 1 / 1 |
| q18 | 40.4 | 19.1 | 2.11x | yes | 5 / 5 |
| q19 | 26.6 | 10.0 | 2.65x | yes | 1 / 1 |
| q20 | 36.4 | 12.7 | 2.86x | yes | 9 / 9 |
| q21 | 46.7 | 30.1 | 1.55x | yes | 47 / 47 |
| q22 | 20.6 | 9.6 | 2.16x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 212.8 | 63.5 | 3.35x | yes | 4 / 4 |
| q02 | 93.6 | 24.8 | 3.77x | yes | 100 / 100 |
| q03 | 109.5 | 47.8 | 2.29x | yes | 10 / 10 |
| q04 | 38.4 | 32.6 | 1.18x | yes | 5 / 5 |
| q05 | 188.3 | 51.2 | 3.68x | yes | 5 / 5 |
| q06 | 42.0 | 21.7 | 1.93x | yes | 1 / 1 |
| q07 | 123.2 | 55.7 | 2.21x | yes | 4 / 4 |
| q08 | 144.4 | 63.7 | 2.27x | yes | 2 / 2 |
| q09 | 361.3 | 135.0 | 2.68x | yes | 175 / 175 |
| q10 | 172.9 | 96.9 | 1.78x | yes | 20 / 20 |
| q11 | 54.1 | 18.5 | 2.92x | yes | 1048 / 1048 |
| q12 | 61.6 | 31.5 | 1.95x | yes | 2 / 2 |
| q13 | 125.0 | 92.4 | 1.35x | yes | 42 / 42 |
| q14 | 53.1 | 41.5 | 1.28x | yes | 1 / 1 |
| q15 | 84.2 | 28.9 | 2.91x | yes | 1 / 1 |
| q16 | 188.4 | 62.3 | 3.02x | yes | 18314 / 18314 |
| q17 | 74.6 | 49.4 | 1.51x | yes | 1 / 1 |
| q18 | 142.2 | 103.1 | 1.38x | yes | 57 / 57 |
| q19 | 95.3 | 65.9 | 1.45x | yes | 1 / 1 |
| q20 | 111.4 | 49.6 | 2.25x | yes | 186 / 186 |
| q21 | 139.6 | 166.4 | 0.84x | yes | 100 / 100 |
| q22 | 52.7 | 37.8 | 1.39x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.8 | 5.7 | 4.16x | yes | 4 / 4 |
| q02 | 78.4 | 11.4 | 6.87x | yes | 44 / 44 |
| q03 | 46.2 | 11.3 | 4.10x | yes | 10 / 10 |
| q04 | 25.4 | 8.7 | 2.91x | yes | 5 / 5 |
| q05 | 65.0 | 12.9 | 5.04x | yes | 5 / 5 |
| q06 | 16.1 | 2.2 | 7.17x | yes | 1 / 1 |
| q07 | 85.9 | 17.5 | 4.91x | yes | 4 / 4 |
| q08 | 82.6 | 15.9 | 5.21x | yes | 2 / 2 |
| q09 | 112.9 | 22.6 | 4.99x | yes | 175 / 175 |
| q10 | 69.2 | 23.1 | 3.00x | yes | 20 / 20 |
| q11 | 57.4 | 11.1 | 5.19x | yes | 2541 / 2541 |
| q12 | 25.1 | 6.3 | 3.96x | yes | 2 / 2 |
| q13 | 48.6 | 21.5 | 2.26x | yes | 37 / 37 |
| q14 | 43.6 | 12.4 | 3.52x | yes | 1 / 1 |
| q15 | 47.4 | 4.8 | 9.96x | yes | 1 / 1 |
| q16 | 42.8 | 13.5 | 3.17x | yes | 2762 / 2762 |
| q17 | 45.7 | 11.0 | 4.17x | yes | 1 / 1 |
| q18 | 70.9 | 16.9 | 4.20x | yes | 5 / 5 |
| q19 | 48.6 | 13.2 | 3.69x | yes | 1 / 1 |
| q20 | 63.6 | 11.7 | 5.45x | yes | 9 / 9 |
| q21 | 77.2 | 18.6 | 4.16x | yes | 47 / 47 |
| q22 | 44.0 | 15.7 | 2.80x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 48.0 | 25.2 | 1.90x | yes | 4 / 4 |
| q02 | 134.4 | 30.4 | 4.42x | yes | 100 / 100 |
| q03 | 118.5 | 32.7 | 3.62x | yes | 10 / 10 |
| q04 | 44.5 | 21.6 | 2.05x | yes | 5 / 5 |
| q05 | 204.5 | 42.4 | 4.83x | yes | 5 / 5 |
| q06 | 27.6 | 7.2 | 3.84x | yes | 1 / 1 |
| q07 | 307.1 | 44.9 | 6.84x | yes | 4 / 4 |
| q08 | 180.8 | 49.5 | 3.65x | yes | 2 / 2 |
| q09 | 308.8 | 92.2 | 3.35x | yes | 175 / 175 |
| q10 | 283.0 | 74.8 | 3.78x | yes | 20 / 20 |
| q11 | 72.3 | 15.3 | 4.71x | yes | 1048 / 1048 |
| q12 | 42.4 | 15.8 | 2.69x | yes | 2 / 2 |
| q13 | 280.3 | 99.1 | 2.83x | yes | 42 / 42 |
| q14 | 102.7 | 37.3 | 2.75x | yes | 1 / 1 |
| q15 | 83.4 | 18.0 | 4.64x | yes | 1 / 1 |
| q16 | 113.8 | 56.0 | 2.03x | yes | 18314 / 18314 |
| q17 | 72.0 | 25.8 | 2.79x | yes | 1 / 1 |
| q18 | 203.7 | 87.3 | 2.33x | yes | 57 / 57 |
| q19 | 97.0 | 62.7 | 1.55x | yes | 1 / 1 |
| q20 | 130.9 | 33.9 | 3.87x | yes | 186 / 186 |
| q21 | 174.6 | 120.8 | 1.45x | yes | 100 / 100 |
| q22 | 103.9 | 60.4 | 1.72x | yes | 7 / 7 |

