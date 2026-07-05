# TPC-H benchmark report

Commit: `f1e4921` - optimizer(cost): occupancy estimate for SEMI/ANTI matched rows (q21)
Generated: 2026-07-06 00:52
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
| single | 0.1 | 22/22 | 626.5 | 331.5 | 1.89x | 22 |
| single | 1 | 22/22 | 2705.1 | 1357.8 | 1.99x | 22 |
| fedparquet | 0.1 | 22/22 | 877.9 | 331.5 | 2.65x | 22 |
| fedparquet | 1 | 22/22 | 3546.2 | 1361.8 | 2.60x | 22 |
| fedpgduck | 0.1 | 22/22 | 1272.6 | 284.9 | 4.47x | 22 |
| fedpgduck | 1 | 22/22 | 3432.4 | 1053.8 | 3.26x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 27.3 | 14.5 | 1.89x | yes | 4 / 4 |
| q02 | 27.1 | 13.5 | 2.01x | yes | 44 / 44 |
| q03 | 23.0 | 13.7 | 1.67x | yes | 10 / 10 |
| q04 | 14.2 | 10.5 | 1.35x | yes | 5 / 5 |
| q05 | 22.7 | 16.1 | 1.40x | yes | 5 / 5 |
| q06 | 10.7 | 5.9 | 1.82x | yes | 1 / 1 |
| q07 | 33.1 | 16.8 | 1.97x | yes | 4 / 4 |
| q08 | 31.0 | 18.8 | 1.65x | yes | 2 / 2 |
| q09 | 69.0 | 25.6 | 2.69x | yes | 175 / 175 |
| q10 | 30.5 | 19.8 | 1.54x | yes | 20 / 20 |
| q11 | 35.2 | 16.9 | 2.09x | yes | 2541 / 2541 |
| q12 | 19.1 | 9.8 | 1.95x | yes | 2 / 2 |
| q13 | 25.6 | 24.9 | 1.03x | yes | 37 / 37 |
| q14 | 14.2 | 7.2 | 1.97x | yes | 1 / 1 |
| q15 | 35.8 | 6.9 | 5.23x | yes | 1 / 1 |
| q16 | 32.6 | 13.6 | 2.40x | yes | 2762 / 2762 |
| q17 | 21.7 | 12.3 | 1.77x | yes | 1 / 1 |
| q18 | 31.6 | 19.6 | 1.61x | yes | 5 / 5 |
| q19 | 30.2 | 13.3 | 2.28x | yes | 1 / 1 |
| q20 | 25.5 | 12.9 | 1.98x | yes | 9 / 9 |
| q21 | 48.8 | 30.0 | 1.63x | yes | 47 / 47 |
| q22 | 17.5 | 8.9 | 1.95x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 221.9 | 63.4 | 3.50x | yes | 4 / 4 |
| q02 | 36.7 | 24.6 | 1.49x | yes | 100 / 100 |
| q03 | 89.5 | 49.6 | 1.80x | yes | 10 / 10 |
| q04 | 42.2 | 40.7 | 1.04x | yes | 5 / 5 |
| q05 | 140.6 | 49.7 | 2.83x | yes | 5 / 5 |
| q06 | 40.2 | 21.7 | 1.85x | yes | 1 / 1 |
| q07 | 101.0 | 55.3 | 1.83x | yes | 4 / 4 |
| q08 | 88.5 | 63.8 | 1.39x | yes | 2 / 2 |
| q09 | 438.9 | 135.0 | 3.25x | yes | 175 / 175 |
| q10 | 125.5 | 96.9 | 1.30x | yes | 20 / 20 |
| q11 | 36.8 | 19.5 | 1.89x | yes | 1048 / 1048 |
| q12 | 64.2 | 32.5 | 1.97x | yes | 2 / 2 |
| q13 | 81.9 | 93.8 | 0.87x | yes | 42 / 42 |
| q14 | 53.0 | 42.1 | 1.26x | yes | 1 / 1 |
| q15 | 86.8 | 28.7 | 3.02x | yes | 1 / 1 |
| q16 | 214.3 | 68.2 | 3.14x | yes | 18314 / 18314 |
| q17 | 275.8 | 47.9 | 5.76x | yes | 1 / 1 |
| q18 | 142.4 | 102.2 | 1.39x | yes | 57 / 57 |
| q19 | 92.1 | 62.1 | 1.48x | yes | 1 / 1 |
| q20 | 132.0 | 50.0 | 2.64x | yes | 186 / 186 |
| q21 | 152.2 | 172.5 | 0.88x | yes | 100 / 100 |
| q22 | 48.7 | 37.4 | 1.30x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 28.6 | 16.0 | 1.79x | yes | 4 / 4 |
| q02 | 59.4 | 12.6 | 4.72x | yes | 44 / 44 |
| q03 | 28.2 | 13.7 | 2.06x | yes | 10 / 10 |
| q04 | 14.3 | 12.4 | 1.16x | yes | 5 / 5 |
| q05 | 51.0 | 14.1 | 3.62x | yes | 5 / 5 |
| q06 | 11.3 | 6.1 | 1.86x | yes | 1 / 1 |
| q07 | 61.7 | 15.3 | 4.03x | yes | 4 / 4 |
| q08 | 66.5 | 17.4 | 3.83x | yes | 2 / 2 |
| q09 | 129.1 | 25.4 | 5.08x | yes | 175 / 175 |
| q10 | 40.8 | 18.6 | 2.19x | yes | 20 / 20 |
| q11 | 59.6 | 17.7 | 3.37x | yes | 2541 / 2541 |
| q12 | 20.0 | 10.8 | 1.85x | yes | 2 / 2 |
| q13 | 22.8 | 28.7 | 0.79x | yes | 37 / 37 |
| q14 | 14.4 | 8.5 | 1.69x | yes | 1 / 1 |
| q15 | 36.8 | 6.6 | 5.55x | yes | 1 / 1 |
| q16 | 37.4 | 13.6 | 2.74x | yes | 2762 / 2762 |
| q17 | 30.1 | 12.9 | 2.34x | yes | 1 / 1 |
| q18 | 40.3 | 18.4 | 2.20x | yes | 5 / 5 |
| q19 | 27.4 | 11.8 | 2.32x | yes | 1 / 1 |
| q20 | 33.8 | 11.4 | 2.97x | yes | 9 / 9 |
| q21 | 45.4 | 30.4 | 1.49x | yes | 47 / 47 |
| q22 | 19.2 | 9.2 | 2.08x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 222.7 | 66.8 | 3.33x | yes | 4 / 4 |
| q02 | 84.5 | 25.6 | 3.30x | yes | 100 / 100 |
| q03 | 108.5 | 49.1 | 2.21x | yes | 10 / 10 |
| q04 | 37.0 | 34.6 | 1.07x | yes | 5 / 5 |
| q05 | 185.8 | 51.2 | 3.63x | yes | 5 / 5 |
| q06 | 41.7 | 21.7 | 1.92x | yes | 1 / 1 |
| q07 | 129.5 | 55.2 | 2.35x | yes | 4 / 4 |
| q08 | 145.9 | 67.6 | 2.16x | yes | 2 / 2 |
| q09 | 997.7 | 136.9 | 7.29x | yes | 175 / 175 |
| q10 | 166.4 | 95.1 | 1.75x | yes | 20 / 20 |
| q11 | 67.7 | 20.5 | 3.30x | yes | 1048 / 1048 |
| q12 | 66.2 | 31.0 | 2.13x | yes | 2 / 2 |
| q13 | 123.2 | 95.7 | 1.29x | yes | 42 / 42 |
| q14 | 54.1 | 43.2 | 1.25x | yes | 1 / 1 |
| q15 | 88.5 | 32.0 | 2.76x | yes | 1 / 1 |
| q16 | 218.5 | 62.9 | 3.47x | yes | 18314 / 18314 |
| q17 | 272.1 | 50.5 | 5.39x | yes | 1 / 1 |
| q18 | 141.6 | 101.8 | 1.39x | yes | 57 / 57 |
| q19 | 91.7 | 63.1 | 1.45x | yes | 1 / 1 |
| q20 | 110.6 | 49.4 | 2.24x | yes | 186 / 186 |
| q21 | 141.6 | 170.5 | 0.83x | yes | 100 / 100 |
| q22 | 50.9 | 37.3 | 1.36x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.3 | 7.5 | 3.11x | yes | 4 / 4 |
| q02 | 82.4 | 11.9 | 6.90x | yes | 44 / 44 |
| q03 | 47.2 | 9.9 | 4.75x | yes | 10 / 10 |
| q04 | 29.7 | 8.7 | 3.42x | yes | 5 / 5 |
| q05 | 61.4 | 12.4 | 4.96x | yes | 5 / 5 |
| q06 | 17.0 | 2.9 | 5.96x | yes | 1 / 1 |
| q07 | 84.9 | 13.5 | 6.30x | yes | 4 / 4 |
| q08 | 85.6 | 22.4 | 3.83x | yes | 2 / 2 |
| q09 | 150.8 | 22.6 | 6.67x | yes | 175 / 175 |
| q10 | 67.9 | 20.3 | 3.34x | yes | 20 / 20 |
| q11 | 67.4 | 11.6 | 5.81x | yes | 2541 / 2541 |
| q12 | 23.1 | 5.7 | 4.09x | yes | 2 / 2 |
| q13 | 46.9 | 21.7 | 2.16x | yes | 37 / 37 |
| q14 | 45.4 | 8.1 | 5.59x | yes | 1 / 1 |
| q15 | 47.2 | 4.7 | 10.03x | yes | 1 / 1 |
| q16 | 44.2 | 13.4 | 3.29x | yes | 2762 / 2762 |
| q17 | 48.5 | 9.3 | 5.24x | yes | 1 / 1 |
| q18 | 71.1 | 17.7 | 4.02x | yes | 5 / 5 |
| q19 | 46.9 | 13.7 | 3.43x | yes | 1 / 1 |
| q20 | 60.3 | 11.1 | 5.45x | yes | 9 / 9 |
| q21 | 77.4 | 19.9 | 3.90x | yes | 47 / 47 |
| q22 | 43.8 | 16.1 | 2.73x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 52.3 | 27.4 | 1.91x | yes | 4 / 4 |
| q02 | 162.4 | 31.3 | 5.19x | yes | 100 / 100 |
| q03 | 124.3 | 33.0 | 3.77x | yes | 10 / 10 |
| q04 | 45.3 | 22.4 | 2.02x | yes | 5 / 5 |
| q05 | 196.0 | 45.7 | 4.28x | yes | 5 / 5 |
| q06 | 27.4 | 6.9 | 3.97x | yes | 1 / 1 |
| q07 | 286.2 | 43.0 | 6.66x | yes | 4 / 4 |
| q08 | 172.9 | 51.6 | 3.35x | yes | 2 / 2 |
| q09 | 433.4 | 91.0 | 4.76x | yes | 175 / 175 |
| q10 | 269.1 | 67.5 | 3.98x | yes | 20 / 20 |
| q11 | 142.7 | 15.2 | 9.41x | yes | 1048 / 1048 |
| q12 | 44.3 | 16.8 | 2.64x | yes | 2 / 2 |
| q13 | 300.4 | 103.5 | 2.90x | yes | 42 / 42 |
| q14 | 102.3 | 38.7 | 2.64x | yes | 1 / 1 |
| q15 | 76.6 | 16.6 | 4.63x | yes | 1 / 1 |
| q16 | 116.4 | 57.8 | 2.01x | yes | 18314 / 18314 |
| q17 | 173.7 | 25.8 | 6.72x | yes | 1 / 1 |
| q18 | 200.9 | 87.9 | 2.28x | yes | 57 / 57 |
| q19 | 93.9 | 59.1 | 1.59x | yes | 1 / 1 |
| q20 | 131.4 | 34.0 | 3.87x | yes | 186 / 186 |
| q21 | 172.2 | 121.6 | 1.42x | yes | 100 / 100 |
| q22 | 108.2 | 56.9 | 1.90x | yes | 7 / 7 |

