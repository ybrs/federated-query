# TPC-H benchmark report

Commit: `636b403` - docs: rewrite architecture for the Rust engine; profiled per-fetch overhead  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 13:26
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
| single | 0.1 | 22/22 | 632.8 | 390.4 | 1.62x | 22 |
| single | 1 | 22/22 | 2437.2 | 1403.7 | 1.74x | 22 |
| fedparquet | 0.1 | 22/22 | 839.8 | 346.2 | 2.43x | 22 |
| fedparquet | 1 | 22/22 | 2782.5 | 1380.2 | 2.02x | 22 |
| fedpgduck | 0.1 | 22/22 | 935.3 | 291.7 | 3.21x | 22 |
| fedpgduck | 1 | 22/22 | 2303.4 | 1087.5 | 2.12x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 30.1 | 16.6 | 1.81x | yes | 4 / 4 |
| q02 | 25.4 | 12.4 | 2.05x | yes | 44 / 44 |
| q03 | 22.2 | 14.1 | 1.57x | yes | 10 / 10 |
| q04 | 14.4 | 12.0 | 1.20x | yes | 5 / 5 |
| q05 | 22.9 | 15.9 | 1.44x | yes | 5 / 5 |
| q06 | 12.6 | 5.9 | 2.13x | yes | 1 / 1 |
| q07 | 31.2 | 16.9 | 1.85x | yes | 4 / 4 |
| q08 | 31.2 | 19.2 | 1.62x | yes | 2 / 2 |
| q09 | 37.5 | 26.2 | 1.43x | yes | 175 / 175 |
| q10 | 33.3 | 49.2 | 0.68x | yes | 20 / 20 |
| q11 | 37.2 | 19.6 | 1.89x | yes | 2541 / 2541 |
| q12 | 23.1 | 12.2 | 1.89x | yes | 2 / 2 |
| q13 | 26.2 | 28.7 | 0.91x | yes | 37 / 37 |
| q14 | 15.4 | 8.6 | 1.79x | yes | 1 / 1 |
| q15 | 38.3 | 6.9 | 5.54x | yes | 1 / 1 |
| q16 | 37.0 | 19.0 | 1.95x | yes | 2762 / 2762 |
| q17 | 24.3 | 13.4 | 1.81x | yes | 1 / 1 |
| q18 | 38.2 | 23.0 | 1.66x | yes | 5 / 5 |
| q19 | 32.9 | 13.1 | 2.51x | yes | 1 / 1 |
| q20 | 28.1 | 13.3 | 2.11x | yes | 9 / 9 |
| q21 | 47.7 | 31.2 | 1.53x | yes | 47 / 47 |
| q22 | 23.8 | 13.0 | 1.83x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 231.8 | 69.4 | 3.34x | yes | 4 / 4 |
| q02 | 40.9 | 27.3 | 1.50x | yes | 100 / 100 |
| q03 | 97.1 | 54.7 | 1.78x | yes | 10 / 10 |
| q04 | 38.3 | 35.9 | 1.07x | yes | 5 / 5 |
| q05 | 101.9 | 55.5 | 1.84x | yes | 5 / 5 |
| q06 | 41.5 | 21.6 | 1.92x | yes | 1 / 1 |
| q07 | 102.5 | 58.5 | 1.75x | yes | 4 / 4 |
| q08 | 94.2 | 66.3 | 1.42x | yes | 2 / 2 |
| q09 | 164.7 | 141.5 | 1.16x | yes | 175 / 175 |
| q10 | 136.6 | 93.5 | 1.46x | yes | 20 / 20 |
| q11 | 38.9 | 18.7 | 2.08x | yes | 1048 / 1048 |
| q12 | 64.4 | 32.9 | 1.96x | yes | 2 / 2 |
| q13 | 85.8 | 97.6 | 0.88x | yes | 42 / 42 |
| q14 | 56.3 | 43.4 | 1.30x | yes | 1 / 1 |
| q15 | 90.1 | 33.9 | 2.65x | yes | 1 / 1 |
| q16 | 167.3 | 64.1 | 2.61x | yes | 18314 / 18314 |
| q17 | 292.6 | 49.4 | 5.92x | yes | 1 / 1 |
| q18 | 148.0 | 106.9 | 1.38x | yes | 57 / 57 |
| q19 | 98.4 | 76.0 | 1.29x | yes | 1 / 1 |
| q20 | 145.3 | 48.9 | 2.97x | yes | 186 / 186 |
| q21 | 147.2 | 167.5 | 0.88x | yes | 100 / 100 |
| q22 | 53.4 | 39.9 | 1.34x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.0 | 15.6 | 1.85x | yes | 4 / 4 |
| q02 | 53.0 | 12.5 | 4.25x | yes | 44 / 44 |
| q03 | 31.6 | 14.0 | 2.26x | yes | 10 / 10 |
| q04 | 15.5 | 11.6 | 1.34x | yes | 5 / 5 |
| q05 | 39.1 | 16.5 | 2.36x | yes | 5 / 5 |
| q06 | 12.5 | 5.9 | 2.11x | yes | 1 / 1 |
| q07 | 51.7 | 17.1 | 3.02x | yes | 4 / 4 |
| q08 | 67.6 | 16.5 | 4.09x | yes | 2 / 2 |
| q09 | 89.8 | 26.2 | 3.43x | yes | 175 / 175 |
| q10 | 40.3 | 20.8 | 1.94x | yes | 20 / 20 |
| q11 | 49.8 | 19.6 | 2.54x | yes | 2541 / 2541 |
| q12 | 21.2 | 10.2 | 2.09x | yes | 2 / 2 |
| q13 | 25.0 | 32.3 | 0.77x | yes | 37 / 37 |
| q14 | 15.4 | 9.1 | 1.69x | yes | 1 / 1 |
| q15 | 41.8 | 7.0 | 5.97x | yes | 1 / 1 |
| q16 | 42.2 | 13.8 | 3.06x | yes | 2762 / 2762 |
| q17 | 26.7 | 10.5 | 2.54x | yes | 1 / 1 |
| q18 | 39.2 | 18.6 | 2.11x | yes | 5 / 5 |
| q19 | 33.0 | 12.9 | 2.56x | yes | 1 / 1 |
| q20 | 39.6 | 14.4 | 2.76x | yes | 9 / 9 |
| q21 | 57.4 | 32.1 | 1.79x | yes | 47 / 47 |
| q22 | 18.4 | 9.0 | 2.05x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 219.4 | 65.1 | 3.37x | yes | 4 / 4 |
| q02 | 96.1 | 27.6 | 3.48x | yes | 100 / 100 |
| q03 | 115.6 | 50.8 | 2.28x | yes | 10 / 10 |
| q04 | 40.0 | 36.8 | 1.09x | yes | 5 / 5 |
| q05 | 195.6 | 52.8 | 3.70x | yes | 5 / 5 |
| q06 | 39.8 | 22.3 | 1.78x | yes | 1 / 1 |
| q07 | 129.1 | 57.6 | 2.24x | yes | 4 / 4 |
| q08 | 153.8 | 65.8 | 2.34x | yes | 2 / 2 |
| q09 | 379.0 | 138.4 | 2.74x | yes | 175 / 175 |
| q10 | 166.1 | 93.3 | 1.78x | yes | 20 / 20 |
| q11 | 57.8 | 20.3 | 2.85x | yes | 1048 / 1048 |
| q12 | 64.8 | 34.0 | 1.91x | yes | 2 / 2 |
| q13 | 135.0 | 93.7 | 1.44x | yes | 42 / 42 |
| q14 | 61.3 | 43.8 | 1.40x | yes | 1 / 1 |
| q15 | 88.5 | 30.8 | 2.87x | yes | 1 / 1 |
| q16 | 195.5 | 64.5 | 3.03x | yes | 18314 / 18314 |
| q17 | 75.6 | 51.8 | 1.46x | yes | 1 / 1 |
| q18 | 155.6 | 111.6 | 1.39x | yes | 57 / 57 |
| q19 | 99.3 | 63.6 | 1.56x | yes | 1 / 1 |
| q20 | 112.5 | 50.4 | 2.23x | yes | 186 / 186 |
| q21 | 150.2 | 169.2 | 0.89x | yes | 100 / 100 |
| q22 | 51.7 | 35.9 | 1.44x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.3 | 6.5 | 2.20x | yes | 4 / 4 |
| q02 | 59.1 | 12.7 | 4.66x | yes | 44 / 44 |
| q03 | 35.9 | 9.7 | 3.70x | yes | 10 / 10 |
| q04 | 16.7 | 7.8 | 2.13x | yes | 5 / 5 |
| q05 | 54.6 | 12.6 | 4.32x | yes | 5 / 5 |
| q06 | 7.3 | 2.0 | 3.68x | yes | 1 / 1 |
| q07 | 65.9 | 14.0 | 4.71x | yes | 4 / 4 |
| q08 | 73.5 | 17.0 | 4.32x | yes | 2 / 2 |
| q09 | 80.0 | 24.8 | 3.22x | yes | 175 / 175 |
| q10 | 65.9 | 21.2 | 3.11x | yes | 20 / 20 |
| q11 | 43.4 | 14.1 | 3.08x | yes | 2541 / 2541 |
| q12 | 14.3 | 6.0 | 2.40x | yes | 2 / 2 |
| q13 | 42.2 | 22.7 | 1.86x | yes | 37 / 37 |
| q14 | 26.8 | 12.6 | 2.12x | yes | 1 / 1 |
| q15 | 30.5 | 5.2 | 5.84x | yes | 1 / 1 |
| q16 | 35.2 | 14.6 | 2.41x | yes | 2762 / 2762 |
| q17 | 26.9 | 8.6 | 3.13x | yes | 1 / 1 |
| q18 | 60.0 | 16.8 | 3.58x | yes | 5 / 5 |
| q19 | 36.6 | 13.9 | 2.64x | yes | 1 / 1 |
| q20 | 44.3 | 11.3 | 3.93x | yes | 9 / 9 |
| q21 | 63.1 | 19.4 | 3.26x | yes | 47 / 47 |
| q22 | 38.9 | 18.4 | 2.12x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.7 | 25.3 | 1.29x | yes | 4 / 4 |
| q02 | 114.1 | 32.5 | 3.50x | yes | 100 / 100 |
| q03 | 107.5 | 34.8 | 3.09x | yes | 10 / 10 |
| q04 | 30.1 | 24.6 | 1.22x | yes | 5 / 5 |
| q05 | 143.1 | 44.8 | 3.20x | yes | 5 / 5 |
| q06 | 12.0 | 7.3 | 1.65x | yes | 1 / 1 |
| q07 | 210.7 | 46.4 | 4.54x | yes | 4 / 4 |
| q08 | 160.8 | 55.2 | 2.91x | yes | 2 / 2 |
| q09 | 236.6 | 93.0 | 2.54x | yes | 175 / 175 |
| q10 | 135.5 | 73.9 | 1.83x | yes | 20 / 20 |
| q11 | 51.9 | 18.0 | 2.88x | yes | 1048 / 1048 |
| q12 | 24.7 | 16.8 | 1.47x | yes | 2 / 2 |
| q13 | 221.4 | 95.2 | 2.33x | yes | 42 / 42 |
| q14 | 82.1 | 39.8 | 2.06x | yes | 1 / 1 |
| q15 | 47.0 | 17.3 | 2.71x | yes | 1 / 1 |
| q16 | 103.5 | 54.1 | 1.91x | yes | 18314 / 18314 |
| q17 | 49.7 | 28.2 | 1.76x | yes | 1 / 1 |
| q18 | 138.5 | 92.4 | 1.50x | yes | 57 / 57 |
| q19 | 72.8 | 65.4 | 1.11x | yes | 1 / 1 |
| q20 | 91.9 | 36.3 | 2.53x | yes | 186 / 186 |
| q21 | 133.6 | 125.1 | 1.07x | yes | 100 / 100 |
| q22 | 103.2 | 61.1 | 1.69x | yes | 7 / 7 |

