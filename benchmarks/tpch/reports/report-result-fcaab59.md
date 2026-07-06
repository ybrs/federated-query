# TPC-H benchmark report

Commit: `fcaab59` - optimizer: one shared cost model everywhere; gate on semi-join selectivity  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 15:51
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
| single | 0.1 | 21/22 | 623.7 | 350.7 | 1.78x | 22 |
| single | 1 | 21/22 | 2611.2 | 1401.1 | 1.86x | 22 |
| fedparquet | 0.1 | 22/22 | 851.3 | 347.6 | 2.45x | 22 |
| fedparquet | 1 | 22/22 | 3161.6 | 1371.1 | 2.31x | 22 |
| fedpgduck | 0.1 | 22/22 | 927.3 | 285.6 | 3.25x | 22 |
| fedpgduck | 1 | 22/22 | 2134.0 | 1090.4 | 1.96x | 22 |

## Issues

| Cell | SF | Query | Status | Detail |
| --- | --- | --- | --- | --- |
| single | 0.1 | q13 | MISMATCH | MISMATCH |
| single | 1 | q13 | MISMATCH | MISMATCH |

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 30.5 | 15.4 | 1.98x | yes | 4 / 4 |
| q02 | 25.0 | 14.4 | 1.74x | yes | 44 / 44 |
| q03 | 21.6 | 13.6 | 1.59x | yes | 10 / 10 |
| q04 | 16.2 | 12.8 | 1.27x | yes | 5 / 5 |
| q05 | 27.0 | 23.7 | 1.14x | yes | 5 / 5 |
| q06 | 12.4 | 5.9 | 2.09x | yes | 1 / 1 |
| q07 | 31.8 | 15.1 | 2.10x | yes | 4 / 4 |
| q08 | 31.3 | 18.5 | 1.69x | yes | 2 / 2 |
| q09 | 32.9 | 27.7 | 1.19x | yes | 175 / 175 |
| q10 | 29.7 | 20.0 | 1.48x | yes | 20 / 20 |
| q11 | 33.7 | 17.3 | 1.94x | yes | 2541 / 2541 |
| q12 | 20.8 | 10.3 | 2.02x | yes | 2 / 2 |
| q13 | 26.4 | 26.1 | 1.01x | MISMATCH | 36 / 37 |
| q14 | 14.3 | 8.6 | 1.67x | yes | 1 / 1 |
| q15 | 38.6 | 6.9 | 5.58x | yes | 1 / 1 |
| q16 | 33.9 | 15.1 | 2.25x | yes | 2762 / 2762 |
| q17 | 22.2 | 11.5 | 1.93x | yes | 1 / 1 |
| q18 | 28.9 | 19.1 | 1.51x | yes | 5 / 5 |
| q19 | 29.5 | 12.4 | 2.38x | yes | 1 / 1 |
| q20 | 25.7 | 13.4 | 1.91x | yes | 9 / 9 |
| q21 | 67.9 | 30.2 | 2.25x | yes | 47 / 47 |
| q22 | 23.6 | 12.7 | 1.85x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 229.1 | 64.4 | 3.56x | yes | 4 / 4 |
| q02 | 36.6 | 24.5 | 1.50x | yes | 100 / 100 |
| q03 | 87.2 | 53.1 | 1.64x | yes | 10 / 10 |
| q04 | 38.8 | 35.8 | 1.08x | yes | 5 / 5 |
| q05 | 104.7 | 53.4 | 1.96x | yes | 5 / 5 |
| q06 | 43.6 | 22.6 | 1.93x | yes | 1 / 1 |
| q07 | 106.9 | 57.5 | 1.86x | yes | 4 / 4 |
| q08 | 91.3 | 66.3 | 1.38x | yes | 2 / 2 |
| q09 | 154.3 | 141.5 | 1.09x | yes | 175 / 175 |
| q10 | 130.3 | 109.9 | 1.19x | yes | 20 / 20 |
| q11 | 35.9 | 18.7 | 1.92x | yes | 1048 / 1048 |
| q12 | 61.7 | 32.1 | 1.92x | yes | 2 / 2 |
| q13 | 83.5 | 97.2 | 0.86x | MISMATCH | 41 / 42 |
| q14 | 51.9 | 45.3 | 1.15x | yes | 1 / 1 |
| q15 | 87.0 | 30.6 | 2.84x | yes | 1 / 1 |
| q16 | 162.2 | 65.1 | 2.49x | yes | 18314 / 18314 |
| q17 | 282.5 | 49.0 | 5.77x | yes | 1 / 1 |
| q18 | 140.4 | 109.4 | 1.28x | yes | 57 / 57 |
| q19 | 103.5 | 69.9 | 1.48x | yes | 1 / 1 |
| q20 | 131.3 | 47.4 | 2.77x | yes | 186 / 186 |
| q21 | 399.4 | 170.5 | 2.34x | yes | 100 / 100 |
| q22 | 49.1 | 36.9 | 1.33x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 27.7 | 15.8 | 1.75x | yes | 4 / 4 |
| q02 | 50.2 | 12.8 | 3.91x | yes | 44 / 44 |
| q03 | 31.4 | 17.4 | 1.81x | yes | 10 / 10 |
| q04 | 14.7 | 11.6 | 1.27x | yes | 5 / 5 |
| q05 | 41.2 | 14.1 | 2.92x | yes | 5 / 5 |
| q06 | 11.1 | 6.1 | 1.83x | yes | 1 / 1 |
| q07 | 70.0 | 16.4 | 4.26x | yes | 4 / 4 |
| q08 | 77.1 | 18.2 | 4.23x | yes | 2 / 2 |
| q09 | 80.5 | 25.7 | 3.13x | yes | 175 / 175 |
| q10 | 41.5 | 21.3 | 1.95x | yes | 20 / 20 |
| q11 | 41.8 | 19.3 | 2.17x | yes | 2541 / 2541 |
| q12 | 18.6 | 10.5 | 1.78x | yes | 2 / 2 |
| q13 | 28.8 | 24.7 | 1.17x | yes | 37 / 37 |
| q14 | 16.4 | 9.1 | 1.81x | yes | 1 / 1 |
| q15 | 37.1 | 6.7 | 5.54x | yes | 1 / 1 |
| q16 | 40.0 | 13.3 | 3.02x | yes | 2762 / 2762 |
| q17 | 27.1 | 11.9 | 2.28x | yes | 1 / 1 |
| q18 | 37.2 | 19.6 | 1.90x | yes | 5 / 5 |
| q19 | 29.6 | 13.2 | 2.24x | yes | 1 / 1 |
| q20 | 38.5 | 15.0 | 2.57x | yes | 9 / 9 |
| q21 | 69.1 | 34.2 | 2.02x | yes | 47 / 47 |
| q22 | 21.9 | 10.9 | 2.02x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 237.4 | 65.3 | 3.63x | yes | 4 / 4 |
| q02 | 97.6 | 25.7 | 3.80x | yes | 100 / 100 |
| q03 | 114.1 | 51.8 | 2.20x | yes | 10 / 10 |
| q04 | 39.3 | 33.8 | 1.16x | yes | 5 / 5 |
| q05 | 195.4 | 51.0 | 3.83x | yes | 5 / 5 |
| q06 | 41.0 | 22.1 | 1.86x | yes | 1 / 1 |
| q07 | 134.8 | 53.9 | 2.50x | yes | 4 / 4 |
| q08 | 287.7 | 65.6 | 4.38x | yes | 2 / 2 |
| q09 | 369.4 | 136.9 | 2.70x | yes | 175 / 175 |
| q10 | 169.4 | 95.1 | 1.78x | yes | 20 / 20 |
| q11 | 54.1 | 18.5 | 2.93x | yes | 1048 / 1048 |
| q12 | 65.3 | 33.5 | 1.95x | yes | 2 / 2 |
| q13 | 134.7 | 102.7 | 1.31x | yes | 42 / 42 |
| q14 | 58.2 | 45.8 | 1.27x | yes | 1 / 1 |
| q15 | 98.8 | 32.8 | 3.01x | yes | 1 / 1 |
| q16 | 174.0 | 61.3 | 2.84x | yes | 18314 / 18314 |
| q17 | 77.0 | 49.9 | 1.54x | yes | 1 / 1 |
| q18 | 163.4 | 103.2 | 1.58x | yes | 57 / 57 |
| q19 | 94.2 | 66.3 | 1.42x | yes | 1 / 1 |
| q20 | 114.6 | 48.4 | 2.37x | yes | 186 / 186 |
| q21 | 390.6 | 170.5 | 2.29x | yes | 100 / 100 |
| q22 | 50.7 | 37.1 | 1.37x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.8 | 6.7 | 2.21x | yes | 4 / 4 |
| q02 | 61.5 | 13.0 | 4.75x | yes | 44 / 44 |
| q03 | 40.6 | 9.3 | 4.36x | yes | 10 / 10 |
| q04 | 18.3 | 8.3 | 2.22x | yes | 5 / 5 |
| q05 | 51.3 | 11.7 | 4.37x | yes | 5 / 5 |
| q06 | 6.7 | 2.0 | 3.39x | yes | 1 / 1 |
| q07 | 68.5 | 13.9 | 4.92x | yes | 4 / 4 |
| q08 | 74.8 | 16.5 | 4.53x | yes | 2 / 2 |
| q09 | 76.1 | 25.0 | 3.04x | yes | 175 / 175 |
| q10 | 47.0 | 19.8 | 2.37x | yes | 20 / 20 |
| q11 | 39.6 | 12.3 | 3.22x | yes | 2541 / 2541 |
| q12 | 15.9 | 7.0 | 2.28x | yes | 2 / 2 |
| q13 | 42.9 | 23.4 | 1.83x | yes | 37 / 37 |
| q14 | 22.8 | 9.1 | 2.51x | yes | 1 / 1 |
| q15 | 29.1 | 4.8 | 6.05x | yes | 1 / 1 |
| q16 | 33.9 | 14.7 | 2.32x | yes | 2762 / 2762 |
| q17 | 28.3 | 10.7 | 2.65x | yes | 1 / 1 |
| q18 | 59.4 | 16.4 | 3.62x | yes | 5 / 5 |
| q19 | 35.2 | 13.4 | 2.62x | yes | 1 / 1 |
| q20 | 47.8 | 11.2 | 4.26x | yes | 9 / 9 |
| q21 | 69.8 | 20.1 | 3.48x | yes | 47 / 47 |
| q22 | 42.8 | 16.3 | 2.63x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.7 | 26.1 | 1.25x | yes | 4 / 4 |
| q02 | 106.3 | 34.8 | 3.06x | yes | 100 / 100 |
| q03 | 74.2 | 36.4 | 2.04x | yes | 10 / 10 |
| q04 | 29.9 | 22.3 | 1.34x | yes | 5 / 5 |
| q05 | 138.5 | 42.5 | 3.26x | yes | 5 / 5 |
| q06 | 11.6 | 7.4 | 1.57x | yes | 1 / 1 |
| q07 | 153.3 | 41.1 | 3.73x | yes | 4 / 4 |
| q08 | 149.9 | 54.5 | 2.75x | yes | 2 / 2 |
| q09 | 239.4 | 99.9 | 2.40x | yes | 175 / 175 |
| q10 | 141.2 | 73.8 | 1.91x | yes | 20 / 20 |
| q11 | 55.2 | 17.3 | 3.19x | yes | 1048 / 1048 |
| q12 | 24.9 | 17.3 | 1.44x | yes | 2 / 2 |
| q13 | 191.3 | 99.4 | 1.92x | yes | 42 / 42 |
| q14 | 52.3 | 41.4 | 1.26x | yes | 1 / 1 |
| q15 | 47.5 | 17.8 | 2.67x | yes | 1 / 1 |
| q16 | 98.1 | 54.5 | 1.80x | yes | 18314 / 18314 |
| q17 | 50.1 | 27.7 | 1.81x | yes | 1 / 1 |
| q18 | 134.0 | 91.0 | 1.47x | yes | 57 / 57 |
| q19 | 66.8 | 64.1 | 1.04x | yes | 1 / 1 |
| q20 | 90.4 | 33.6 | 2.69x | yes | 186 / 186 |
| q21 | 157.0 | 134.1 | 1.17x | yes | 100 / 100 |
| q22 | 89.5 | 53.4 | 1.68x | yes | 7 / 7 |

