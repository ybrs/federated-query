# TPC-H benchmark report

Commit: `62bba5f` - executor(B3): key injection into pushed remote queries
Generated: 2026-07-05 21:18
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
| single | 0.1 | 22/22 | 608.1 | 326.6 | 1.86x | 22 |
| single | 1 | 22/22 | 2794.7 | 1359.5 | 2.06x | 22 |
| fedparquet | 0.1 | 22/22 | 913.8 | 330.4 | 2.77x | 22 |
| fedparquet | 1 | 22/22 | 4225.9 | 1338.9 | 3.16x | 22 |
| fedpgduck | 0.1 | 22/22 | 1392.1 | 287.3 | 4.85x | 22 |
| fedpgduck | 1 | 22/22 | 4785.8 | 1079.9 | 4.43x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.3 | 15.2 | 2.06x | yes | 4 / 4 |
| q02 | 24.1 | 12.4 | 1.94x | yes | 44 / 44 |
| q03 | 20.3 | 13.1 | 1.55x | yes | 10 / 10 |
| q04 | 14.4 | 10.5 | 1.38x | yes | 5 / 5 |
| q05 | 23.4 | 14.4 | 1.63x | yes | 5 / 5 |
| q06 | 11.8 | 5.5 | 2.15x | yes | 1 / 1 |
| q07 | 28.2 | 16.3 | 1.73x | yes | 4 / 4 |
| q08 | 30.5 | 18.9 | 1.62x | yes | 2 / 2 |
| q09 | 72.5 | 24.1 | 3.01x | yes | 175 / 175 |
| q10 | 29.3 | 18.8 | 1.56x | yes | 20 / 20 |
| q11 | 33.0 | 18.9 | 1.75x | yes | 2541 / 2541 |
| q12 | 18.6 | 9.8 | 1.89x | yes | 2 / 2 |
| q13 | 24.7 | 26.4 | 0.93x | yes | 37 / 37 |
| q14 | 15.6 | 7.4 | 2.10x | yes | 1 / 1 |
| q15 | 35.7 | 7.0 | 5.13x | yes | 1 / 1 |
| q16 | 32.3 | 13.9 | 2.31x | yes | 2762 / 2762 |
| q17 | 22.7 | 10.5 | 2.16x | yes | 1 / 1 |
| q18 | 41.0 | 18.5 | 2.22x | yes | 5 / 5 |
| q19 | 20.5 | 12.9 | 1.59x | yes | 1 / 1 |
| q20 | 27.4 | 12.9 | 2.12x | yes | 9 / 9 |
| q21 | 31.9 | 29.9 | 1.07x | yes | 47 / 47 |
| q22 | 18.9 | 9.3 | 2.03x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.3 | 63.2 | 3.40x | yes | 4 / 4 |
| q02 | 39.3 | 23.5 | 1.67x | yes | 100 / 100 |
| q03 | 83.7 | 49.8 | 1.68x | yes | 10 / 10 |
| q04 | 36.7 | 36.2 | 1.02x | yes | 5 / 5 |
| q05 | 150.4 | 51.0 | 2.95x | yes | 5 / 5 |
| q06 | 40.4 | 21.7 | 1.86x | yes | 1 / 1 |
| q07 | 98.9 | 53.7 | 1.84x | yes | 4 / 4 |
| q08 | 92.6 | 62.0 | 1.49x | yes | 2 / 2 |
| q09 | 438.3 | 147.8 | 2.97x | yes | 175 / 175 |
| q10 | 126.1 | 96.0 | 1.31x | yes | 20 / 20 |
| q11 | 35.6 | 19.9 | 1.79x | yes | 1048 / 1048 |
| q12 | 61.7 | 33.6 | 1.84x | yes | 2 / 2 |
| q13 | 80.8 | 94.6 | 0.85x | yes | 42 / 42 |
| q14 | 55.0 | 43.6 | 1.26x | yes | 1 / 1 |
| q15 | 84.4 | 28.6 | 2.96x | yes | 1 / 1 |
| q16 | 155.6 | 61.4 | 2.53x | yes | 18314 / 18314 |
| q17 | 271.5 | 47.8 | 5.69x | yes | 1 / 1 |
| q18 | 339.0 | 101.4 | 3.34x | yes | 57 / 57 |
| q19 | 82.0 | 67.8 | 1.21x | yes | 1 / 1 |
| q20 | 130.4 | 47.2 | 2.76x | yes | 186 / 186 |
| q21 | 126.8 | 167.6 | 0.76x | yes | 100 / 100 |
| q22 | 50.1 | 41.2 | 1.22x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.7 | 15.2 | 1.56x | yes | 4 / 4 |
| q02 | 61.0 | 12.1 | 5.06x | yes | 44 / 44 |
| q03 | 28.0 | 14.7 | 1.91x | yes | 10 / 10 |
| q04 | 13.7 | 11.5 | 1.18x | yes | 5 / 5 |
| q05 | 46.1 | 15.0 | 3.07x | yes | 5 / 5 |
| q06 | 12.1 | 5.8 | 2.09x | yes | 1 / 1 |
| q07 | 71.8 | 15.1 | 4.75x | yes | 4 / 4 |
| q08 | 66.7 | 17.8 | 3.75x | yes | 2 / 2 |
| q09 | 126.3 | 23.8 | 5.31x | yes | 175 / 175 |
| q10 | 42.0 | 19.4 | 2.16x | yes | 20 / 20 |
| q11 | 62.3 | 17.6 | 3.54x | yes | 2541 / 2541 |
| q12 | 21.8 | 10.4 | 2.10x | yes | 2 / 2 |
| q13 | 22.9 | 27.0 | 0.85x | yes | 37 / 37 |
| q14 | 15.7 | 7.9 | 1.98x | yes | 1 / 1 |
| q15 | 36.1 | 7.1 | 5.10x | yes | 1 / 1 |
| q16 | 42.3 | 13.9 | 3.05x | yes | 2762 / 2762 |
| q17 | 30.8 | 11.8 | 2.62x | yes | 1 / 1 |
| q18 | 57.1 | 19.7 | 2.90x | yes | 5 / 5 |
| q19 | 24.6 | 12.6 | 1.95x | yes | 1 / 1 |
| q20 | 27.8 | 13.3 | 2.10x | yes | 9 / 9 |
| q21 | 62.2 | 29.6 | 2.10x | yes | 47 / 47 |
| q22 | 18.6 | 9.2 | 2.03x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 216.7 | 65.6 | 3.30x | yes | 4 / 4 |
| q02 | 83.4 | 24.8 | 3.37x | yes | 100 / 100 |
| q03 | 107.2 | 47.9 | 2.24x | yes | 10 / 10 |
| q04 | 35.7 | 34.6 | 1.03x | yes | 5 / 5 |
| q05 | 196.6 | 54.6 | 3.60x | yes | 5 / 5 |
| q06 | 40.8 | 22.0 | 1.86x | yes | 1 / 1 |
| q07 | 350.5 | 57.5 | 6.10x | yes | 4 / 4 |
| q08 | 143.7 | 64.1 | 2.24x | yes | 2 / 2 |
| q09 | 986.8 | 141.0 | 7.00x | yes | 175 / 175 |
| q10 | 163.2 | 91.4 | 1.78x | yes | 20 / 20 |
| q11 | 59.1 | 19.5 | 3.03x | yes | 1048 / 1048 |
| q12 | 61.7 | 31.9 | 1.94x | yes | 2 / 2 |
| q13 | 125.4 | 94.4 | 1.33x | yes | 42 / 42 |
| q14 | 58.9 | 41.4 | 1.42x | yes | 1 / 1 |
| q15 | 82.5 | 29.7 | 2.78x | yes | 1 / 1 |
| q16 | 169.5 | 61.0 | 2.78x | yes | 18314 / 18314 |
| q17 | 265.2 | 48.6 | 5.46x | yes | 1 / 1 |
| q18 | 605.8 | 103.4 | 5.86x | yes | 57 / 57 |
| q19 | 97.2 | 61.9 | 1.57x | yes | 1 / 1 |
| q20 | 114.9 | 45.4 | 2.53x | yes | 186 / 186 |
| q21 | 206.0 | 160.8 | 1.28x | yes | 100 / 100 |
| q22 | 55.1 | 37.5 | 1.47x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.7 | 6.6 | 3.60x | yes | 4 / 4 |
| q02 | 81.3 | 11.7 | 6.97x | yes | 44 / 44 |
| q03 | 48.4 | 10.3 | 4.70x | yes | 10 / 10 |
| q04 | 24.9 | 6.6 | 3.76x | yes | 5 / 5 |
| q05 | 63.0 | 13.1 | 4.81x | yes | 5 / 5 |
| q06 | 15.5 | 1.9 | 8.25x | yes | 1 / 1 |
| q07 | 91.2 | 13.5 | 6.75x | yes | 4 / 4 |
| q08 | 83.5 | 15.4 | 5.42x | yes | 2 / 2 |
| q09 | 154.3 | 24.3 | 6.35x | yes | 175 / 175 |
| q10 | 71.4 | 21.4 | 3.35x | yes | 20 / 20 |
| q11 | 68.4 | 11.5 | 5.96x | yes | 2541 / 2541 |
| q12 | 22.5 | 6.0 | 3.79x | yes | 2 / 2 |
| q13 | 48.7 | 22.2 | 2.19x | yes | 37 / 37 |
| q14 | 43.6 | 8.7 | 5.00x | yes | 1 / 1 |
| q15 | 47.4 | 4.7 | 10.18x | yes | 1 / 1 |
| q16 | 55.2 | 13.9 | 3.98x | yes | 2762 / 2762 |
| q17 | 61.6 | 9.6 | 6.40x | yes | 1 / 1 |
| q18 | 106.1 | 16.8 | 6.30x | yes | 5 / 5 |
| q19 | 40.7 | 21.4 | 1.90x | yes | 1 / 1 |
| q20 | 60.8 | 12.2 | 4.99x | yes | 9 / 9 |
| q21 | 133.3 | 19.0 | 7.01x | yes | 47 / 47 |
| q22 | 46.8 | 16.6 | 2.82x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.3 | 26.1 | 1.89x | yes | 4 / 4 |
| q02 | 155.6 | 32.1 | 4.85x | yes | 100 / 100 |
| q03 | 129.4 | 36.7 | 3.53x | yes | 10 / 10 |
| q04 | 47.7 | 23.3 | 2.04x | yes | 5 / 5 |
| q05 | 199.3 | 41.1 | 4.85x | yes | 5 / 5 |
| q06 | 28.7 | 7.4 | 3.86x | yes | 1 / 1 |
| q07 | 449.2 | 44.9 | 10.00x | yes | 4 / 4 |
| q08 | 185.6 | 60.4 | 3.07x | yes | 2 / 2 |
| q09 | 433.2 | 91.8 | 4.72x | yes | 175 / 175 |
| q10 | 266.6 | 73.3 | 3.64x | yes | 20 / 20 |
| q11 | 142.7 | 16.3 | 8.76x | yes | 1048 / 1048 |
| q12 | 41.0 | 16.9 | 2.43x | yes | 2 / 2 |
| q13 | 287.0 | 94.9 | 3.02x | yes | 42 / 42 |
| q14 | 102.4 | 39.9 | 2.57x | yes | 1 / 1 |
| q15 | 76.1 | 21.1 | 3.61x | yes | 1 / 1 |
| q16 | 133.3 | 54.3 | 2.46x | yes | 18314 / 18314 |
| q17 | 393.5 | 26.2 | 15.05x | yes | 1 / 1 |
| q18 | 816.4 | 88.3 | 9.24x | yes | 57 / 57 |
| q19 | 128.2 | 63.2 | 2.03x | yes | 1 / 1 |
| q20 | 128.8 | 32.7 | 3.93x | yes | 186 / 186 |
| q21 | 476.8 | 119.2 | 4.00x | yes | 100 / 100 |
| q22 | 115.0 | 70.0 | 1.64x | yes | 7 / 7 |

