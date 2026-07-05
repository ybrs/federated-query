# TPC-H benchmark report

Commit: `1b5c9da` - optimizer: semi-join pushdown through inner joins (q18 7.3x -> 2.3x)
Generated: 2026-07-05 23:54
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
| single | 0.1 | 22/22 | 674.3 | 338.0 | 1.99x | 22 |
| single | 1 | 22/22 | 2884.5 | 1340.1 | 2.15x | 22 |
| fedparquet | 0.1 | 22/22 | 895.6 | 325.4 | 2.75x | 22 |
| fedparquet | 1 | 22/22 | 3621.9 | 1343.3 | 2.70x | 22 |
| fedpgduck | 0.1 | 22/22 | 1367.3 | 297.0 | 4.60x | 22 |
| fedpgduck | 1 | 22/22 | 3819.8 | 1057.6 | 3.61x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.8 | 16.7 | 1.78x | yes | 4 / 4 |
| q02 | 25.9 | 13.0 | 2.00x | yes | 44 / 44 |
| q03 | 23.3 | 14.4 | 1.62x | yes | 10 / 10 |
| q04 | 14.2 | 10.1 | 1.41x | yes | 5 / 5 |
| q05 | 24.9 | 16.1 | 1.55x | yes | 5 / 5 |
| q06 | 13.0 | 5.7 | 2.29x | yes | 1 / 1 |
| q07 | 32.3 | 19.3 | 1.67x | yes | 4 / 4 |
| q08 | 31.0 | 18.7 | 1.66x | yes | 2 / 2 |
| q09 | 76.1 | 26.9 | 2.83x | yes | 175 / 175 |
| q10 | 28.3 | 19.9 | 1.42x | yes | 20 / 20 |
| q11 | 33.6 | 17.3 | 1.95x | yes | 2541 / 2541 |
| q12 | 18.4 | 10.3 | 1.79x | yes | 2 / 2 |
| q13 | 24.9 | 26.8 | 0.93x | yes | 37 / 37 |
| q14 | 14.1 | 7.8 | 1.82x | yes | 1 / 1 |
| q15 | 40.1 | 7.4 | 5.39x | yes | 1 / 1 |
| q16 | 33.6 | 14.1 | 2.38x | yes | 2762 / 2762 |
| q17 | 23.1 | 12.3 | 1.87x | yes | 1 / 1 |
| q18 | 31.2 | 17.6 | 1.77x | yes | 5 / 5 |
| q19 | 31.9 | 11.5 | 2.76x | yes | 1 / 1 |
| q20 | 28.4 | 13.3 | 2.13x | yes | 9 / 9 |
| q21 | 76.7 | 30.4 | 2.52x | yes | 47 / 47 |
| q22 | 19.6 | 8.5 | 2.29x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 213.7 | 62.0 | 3.45x | yes | 4 / 4 |
| q02 | 36.5 | 24.4 | 1.50x | yes | 100 / 100 |
| q03 | 86.1 | 48.6 | 1.77x | yes | 10 / 10 |
| q04 | 39.4 | 34.4 | 1.14x | yes | 5 / 5 |
| q05 | 145.7 | 51.0 | 2.86x | yes | 5 / 5 |
| q06 | 43.7 | 22.6 | 1.93x | yes | 1 / 1 |
| q07 | 101.9 | 54.5 | 1.87x | yes | 4 / 4 |
| q08 | 90.3 | 61.8 | 1.46x | yes | 2 / 2 |
| q09 | 440.3 | 134.3 | 3.28x | yes | 175 / 175 |
| q10 | 127.2 | 100.9 | 1.26x | yes | 20 / 20 |
| q11 | 35.4 | 18.8 | 1.88x | yes | 1048 / 1048 |
| q12 | 66.3 | 31.5 | 2.11x | yes | 2 / 2 |
| q13 | 77.9 | 91.9 | 0.85x | yes | 42 / 42 |
| q14 | 51.8 | 42.0 | 1.23x | yes | 1 / 1 |
| q15 | 86.3 | 29.7 | 2.91x | yes | 1 / 1 |
| q16 | 160.1 | 62.6 | 2.56x | yes | 18314 / 18314 |
| q17 | 273.6 | 47.9 | 5.71x | yes | 1 / 1 |
| q18 | 144.1 | 104.6 | 1.38x | yes | 57 / 57 |
| q19 | 92.6 | 65.4 | 1.42x | yes | 1 / 1 |
| q20 | 133.0 | 50.7 | 2.62x | yes | 186 / 186 |
| q21 | 390.0 | 164.4 | 2.37x | yes | 100 / 100 |
| q22 | 48.6 | 36.0 | 1.35x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.1 | 15.4 | 1.89x | yes | 4 / 4 |
| q02 | 58.5 | 11.7 | 5.00x | yes | 44 / 44 |
| q03 | 28.2 | 13.8 | 2.04x | yes | 10 / 10 |
| q04 | 14.7 | 11.3 | 1.30x | yes | 5 / 5 |
| q05 | 50.1 | 14.0 | 3.59x | yes | 5 / 5 |
| q06 | 12.8 | 5.5 | 2.32x | yes | 1 / 1 |
| q07 | 65.3 | 17.2 | 3.80x | yes | 4 / 4 |
| q08 | 65.7 | 19.3 | 3.40x | yes | 2 / 2 |
| q09 | 124.4 | 24.5 | 5.08x | yes | 175 / 175 |
| q10 | 40.2 | 19.5 | 2.06x | yes | 20 / 20 |
| q11 | 61.7 | 16.8 | 3.67x | yes | 2541 / 2541 |
| q12 | 19.5 | 9.8 | 2.00x | yes | 2 / 2 |
| q13 | 23.7 | 26.1 | 0.91x | yes | 37 / 37 |
| q14 | 16.5 | 9.3 | 1.78x | yes | 1 / 1 |
| q15 | 35.5 | 7.0 | 5.10x | yes | 1 / 1 |
| q16 | 39.3 | 13.2 | 2.98x | yes | 2762 / 2762 |
| q17 | 27.6 | 10.7 | 2.58x | yes | 1 / 1 |
| q18 | 31.7 | 18.3 | 1.73x | yes | 5 / 5 |
| q19 | 27.9 | 9.8 | 2.85x | yes | 1 / 1 |
| q20 | 37.6 | 13.6 | 2.77x | yes | 9 / 9 |
| q21 | 66.8 | 29.9 | 2.24x | yes | 47 / 47 |
| q22 | 18.8 | 8.9 | 2.11x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 214.0 | 66.3 | 3.23x | yes | 4 / 4 |
| q02 | 85.4 | 23.2 | 3.67x | yes | 100 / 100 |
| q03 | 109.3 | 49.2 | 2.22x | yes | 10 / 10 |
| q04 | 37.9 | 33.8 | 1.12x | yes | 5 / 5 |
| q05 | 180.5 | 50.3 | 3.59x | yes | 5 / 5 |
| q06 | 42.5 | 24.1 | 1.77x | yes | 1 / 1 |
| q07 | 130.3 | 55.7 | 2.34x | yes | 4 / 4 |
| q08 | 146.1 | 64.5 | 2.27x | yes | 2 / 2 |
| q09 | 987.4 | 135.8 | 7.27x | yes | 175 / 175 |
| q10 | 163.8 | 88.4 | 1.85x | yes | 20 / 20 |
| q11 | 53.7 | 19.4 | 2.76x | yes | 1048 / 1048 |
| q12 | 62.7 | 31.1 | 2.02x | yes | 2 / 2 |
| q13 | 122.3 | 90.2 | 1.36x | yes | 42 / 42 |
| q14 | 57.7 | 45.2 | 1.28x | yes | 1 / 1 |
| q15 | 83.0 | 30.3 | 2.74x | yes | 1 / 1 |
| q16 | 179.1 | 64.1 | 2.79x | yes | 18314 / 18314 |
| q17 | 267.0 | 49.7 | 5.37x | yes | 1 / 1 |
| q18 | 140.7 | 103.3 | 1.36x | yes | 57 / 57 |
| q19 | 91.7 | 64.0 | 1.43x | yes | 1 / 1 |
| q20 | 112.9 | 47.4 | 2.38x | yes | 186 / 186 |
| q21 | 304.6 | 171.1 | 1.78x | yes | 100 / 100 |
| q22 | 49.4 | 36.2 | 1.36x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.2 | 6.6 | 3.52x | yes | 4 / 4 |
| q02 | 92.3 | 14.7 | 6.28x | yes | 44 / 44 |
| q03 | 47.1 | 9.9 | 4.75x | yes | 10 / 10 |
| q04 | 24.5 | 8.8 | 2.79x | yes | 5 / 5 |
| q05 | 69.1 | 12.5 | 5.54x | yes | 5 / 5 |
| q06 | 15.6 | 2.0 | 7.66x | yes | 1 / 1 |
| q07 | 84.9 | 19.1 | 4.45x | yes | 4 / 4 |
| q08 | 78.9 | 16.9 | 4.68x | yes | 2 / 2 |
| q09 | 154.9 | 30.3 | 5.11x | yes | 175 / 175 |
| q10 | 68.0 | 19.0 | 3.58x | yes | 20 / 20 |
| q11 | 72.2 | 12.4 | 5.80x | yes | 2541 / 2541 |
| q12 | 22.7 | 6.1 | 3.74x | yes | 2 / 2 |
| q13 | 50.0 | 22.1 | 2.26x | yes | 37 / 37 |
| q14 | 41.6 | 8.8 | 4.75x | yes | 1 / 1 |
| q15 | 47.5 | 4.4 | 10.71x | yes | 1 / 1 |
| q16 | 42.5 | 13.6 | 3.13x | yes | 2762 / 2762 |
| q17 | 50.2 | 9.2 | 5.47x | yes | 1 / 1 |
| q18 | 61.3 | 17.6 | 3.49x | yes | 5 / 5 |
| q19 | 47.5 | 13.5 | 3.51x | yes | 1 / 1 |
| q20 | 60.8 | 11.4 | 5.35x | yes | 9 / 9 |
| q21 | 165.7 | 18.2 | 9.10x | yes | 47 / 47 |
| q22 | 47.1 | 20.1 | 2.35x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 47.7 | 26.3 | 1.82x | yes | 4 / 4 |
| q02 | 159.0 | 31.0 | 5.13x | yes | 100 / 100 |
| q03 | 120.6 | 35.0 | 3.45x | yes | 10 / 10 |
| q04 | 51.6 | 22.0 | 2.34x | yes | 5 / 5 |
| q05 | 194.3 | 44.9 | 4.33x | yes | 5 / 5 |
| q06 | 26.8 | 7.7 | 3.49x | yes | 1 / 1 |
| q07 | 282.8 | 47.1 | 6.00x | yes | 4 / 4 |
| q08 | 164.3 | 51.9 | 3.17x | yes | 2 / 2 |
| q09 | 437.2 | 94.5 | 4.62x | yes | 175 / 175 |
| q10 | 278.7 | 79.0 | 3.53x | yes | 20 / 20 |
| q11 | 141.7 | 14.7 | 9.65x | yes | 1048 / 1048 |
| q12 | 42.0 | 16.8 | 2.50x | yes | 2 / 2 |
| q13 | 282.6 | 94.3 | 3.00x | yes | 42 / 42 |
| q14 | 101.6 | 36.1 | 2.81x | yes | 1 / 1 |
| q15 | 77.0 | 16.8 | 4.58x | yes | 1 / 1 |
| q16 | 115.9 | 52.9 | 2.19x | yes | 18314 / 18314 |
| q17 | 167.9 | 27.6 | 6.09x | yes | 1 / 1 |
| q18 | 199.3 | 87.6 | 2.28x | yes | 57 / 57 |
| q19 | 88.5 | 59.2 | 1.50x | yes | 1 / 1 |
| q20 | 131.4 | 33.4 | 3.94x | yes | 186 / 186 |
| q21 | 590.6 | 120.5 | 4.90x | yes | 100 / 100 |
| q22 | 118.2 | 58.4 | 2.02x | yes | 7 / 7 |

