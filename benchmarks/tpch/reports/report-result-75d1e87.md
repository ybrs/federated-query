# TPC-H benchmark report

Commit: `75d1e87` - pushdown: a nullable-side scan filter rides the JOIN ON in remote SQL  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 16:00
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
| single | 0.1 | 22/22 | 626.9 | 343.9 | 1.82x | 22 |
| single | 1 | 22/22 | 2572.5 | 1350.0 | 1.91x | 22 |
| fedparquet | 0.1 | 22/22 | 825.7 | 330.5 | 2.50x | 22 |
| fedparquet | 1 | 22/22 | 3075.4 | 1352.8 | 2.27x | 22 |
| fedpgduck | 0.1 | 22/22 | 885.4 | 279.8 | 3.16x | 22 |
| fedpgduck | 1 | 22/22 | 2133.2 | 1079.1 | 1.98x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.7 | 16.2 | 1.96x | yes | 4 / 4 |
| q02 | 26.3 | 16.4 | 1.60x | yes | 44 / 44 |
| q03 | 21.4 | 15.1 | 1.42x | yes | 10 / 10 |
| q04 | 14.1 | 12.2 | 1.15x | yes | 5 / 5 |
| q05 | 24.8 | 14.6 | 1.70x | yes | 5 / 5 |
| q06 | 12.1 | 6.1 | 2.00x | yes | 1 / 1 |
| q07 | 28.6 | 16.1 | 1.77x | yes | 4 / 4 |
| q08 | 29.3 | 17.9 | 1.63x | yes | 2 / 2 |
| q09 | 39.0 | 26.8 | 1.46x | yes | 175 / 175 |
| q10 | 34.8 | 20.5 | 1.70x | yes | 20 / 20 |
| q11 | 36.8 | 19.6 | 1.88x | yes | 2541 / 2541 |
| q12 | 20.9 | 10.2 | 2.05x | yes | 2 / 2 |
| q13 | 25.7 | 26.2 | 0.98x | yes | 37 / 37 |
| q14 | 14.3 | 8.1 | 1.77x | yes | 1 / 1 |
| q15 | 35.3 | 7.1 | 4.95x | yes | 1 / 1 |
| q16 | 33.5 | 15.8 | 2.12x | yes | 2762 / 2762 |
| q17 | 23.6 | 12.4 | 1.91x | yes | 1 / 1 |
| q18 | 30.4 | 20.6 | 1.47x | yes | 5 / 5 |
| q19 | 30.4 | 10.0 | 3.04x | yes | 1 / 1 |
| q20 | 27.0 | 13.7 | 1.97x | yes | 9 / 9 |
| q21 | 66.7 | 29.3 | 2.28x | yes | 47 / 47 |
| q22 | 20.0 | 9.0 | 2.21x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 217.5 | 66.3 | 3.28x | yes | 4 / 4 |
| q02 | 39.2 | 24.1 | 1.63x | yes | 100 / 100 |
| q03 | 88.9 | 52.2 | 1.70x | yes | 10 / 10 |
| q04 | 39.2 | 34.4 | 1.14x | yes | 5 / 5 |
| q05 | 100.4 | 51.4 | 1.95x | yes | 5 / 5 |
| q06 | 40.9 | 23.6 | 1.74x | yes | 1 / 1 |
| q07 | 102.9 | 56.6 | 1.82x | yes | 4 / 4 |
| q08 | 91.6 | 66.8 | 1.37x | yes | 2 / 2 |
| q09 | 156.2 | 139.3 | 1.12x | yes | 175 / 175 |
| q10 | 128.8 | 89.8 | 1.43x | yes | 20 / 20 |
| q11 | 36.1 | 18.1 | 2.00x | yes | 1048 / 1048 |
| q12 | 62.1 | 32.8 | 1.89x | yes | 2 / 2 |
| q13 | 80.4 | 91.6 | 0.88x | yes | 42 / 42 |
| q14 | 52.0 | 40.2 | 1.29x | yes | 1 / 1 |
| q15 | 100.6 | 31.9 | 3.15x | yes | 1 / 1 |
| q16 | 155.0 | 61.6 | 2.52x | yes | 18314 / 18314 |
| q17 | 275.7 | 50.0 | 5.52x | yes | 1 / 1 |
| q18 | 142.3 | 104.6 | 1.36x | yes | 57 / 57 |
| q19 | 94.7 | 62.6 | 1.51x | yes | 1 / 1 |
| q20 | 130.9 | 51.5 | 2.54x | yes | 186 / 186 |
| q21 | 386.9 | 164.3 | 2.35x | yes | 100 / 100 |
| q22 | 50.2 | 36.6 | 1.37x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 27.2 | 16.2 | 1.68x | yes | 4 / 4 |
| q02 | 51.6 | 13.2 | 3.90x | yes | 44 / 44 |
| q03 | 32.0 | 14.2 | 2.25x | yes | 10 / 10 |
| q04 | 15.1 | 11.6 | 1.30x | yes | 5 / 5 |
| q05 | 37.7 | 14.5 | 2.61x | yes | 5 / 5 |
| q06 | 12.3 | 5.6 | 2.20x | yes | 1 / 1 |
| q07 | 67.8 | 15.9 | 4.26x | yes | 4 / 4 |
| q08 | 70.0 | 18.0 | 3.89x | yes | 2 / 2 |
| q09 | 75.8 | 26.6 | 2.86x | yes | 175 / 175 |
| q10 | 40.9 | 19.8 | 2.06x | yes | 20 / 20 |
| q11 | 44.1 | 19.2 | 2.30x | yes | 2541 / 2541 |
| q12 | 18.7 | 9.6 | 1.94x | yes | 2 / 2 |
| q13 | 27.9 | 26.0 | 1.07x | yes | 37 / 37 |
| q14 | 15.7 | 8.2 | 1.92x | yes | 1 / 1 |
| q15 | 35.7 | 6.9 | 5.16x | yes | 1 / 1 |
| q16 | 38.7 | 13.3 | 2.92x | yes | 2762 / 2762 |
| q17 | 25.4 | 10.6 | 2.40x | yes | 1 / 1 |
| q18 | 38.1 | 17.8 | 2.14x | yes | 5 / 5 |
| q19 | 27.7 | 11.4 | 2.44x | yes | 1 / 1 |
| q20 | 38.4 | 12.8 | 3.00x | yes | 9 / 9 |
| q21 | 66.1 | 30.6 | 2.16x | yes | 47 / 47 |
| q22 | 18.7 | 8.6 | 2.18x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.0 | 64.1 | 3.35x | yes | 4 / 4 |
| q02 | 94.9 | 24.1 | 3.93x | yes | 100 / 100 |
| q03 | 111.5 | 50.8 | 2.19x | yes | 10 / 10 |
| q04 | 38.0 | 32.5 | 1.17x | yes | 5 / 5 |
| q05 | 186.1 | 52.0 | 3.58x | yes | 5 / 5 |
| q06 | 42.4 | 22.2 | 1.91x | yes | 1 / 1 |
| q07 | 135.5 | 57.8 | 2.34x | yes | 4 / 4 |
| q08 | 282.0 | 62.7 | 4.50x | yes | 2 / 2 |
| q09 | 380.5 | 142.3 | 2.67x | yes | 175 / 175 |
| q10 | 173.5 | 89.5 | 1.94x | yes | 20 / 20 |
| q11 | 55.2 | 19.7 | 2.81x | yes | 1048 / 1048 |
| q12 | 61.1 | 31.7 | 1.92x | yes | 2 / 2 |
| q13 | 128.4 | 97.4 | 1.32x | yes | 42 / 42 |
| q14 | 54.3 | 40.5 | 1.34x | yes | 1 / 1 |
| q15 | 82.6 | 28.9 | 2.86x | yes | 1 / 1 |
| q16 | 166.5 | 63.1 | 2.64x | yes | 18314 / 18314 |
| q17 | 75.7 | 49.4 | 1.53x | yes | 1 / 1 |
| q18 | 137.2 | 99.6 | 1.38x | yes | 57 / 57 |
| q19 | 93.1 | 63.6 | 1.46x | yes | 1 / 1 |
| q20 | 125.0 | 48.0 | 2.60x | yes | 186 / 186 |
| q21 | 388.1 | 176.0 | 2.21x | yes | 100 / 100 |
| q22 | 48.9 | 36.8 | 1.33x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.0 | 5.9 | 2.35x | yes | 4 / 4 |
| q02 | 59.8 | 12.4 | 4.81x | yes | 44 / 44 |
| q03 | 33.5 | 9.2 | 3.62x | yes | 10 / 10 |
| q04 | 16.4 | 8.6 | 1.90x | yes | 5 / 5 |
| q05 | 52.2 | 12.0 | 4.34x | yes | 5 / 5 |
| q06 | 7.0 | 2.0 | 3.49x | yes | 1 / 1 |
| q07 | 64.1 | 15.0 | 4.29x | yes | 4 / 4 |
| q08 | 72.6 | 16.1 | 4.50x | yes | 2 / 2 |
| q09 | 76.0 | 23.8 | 3.18x | yes | 175 / 175 |
| q10 | 47.2 | 19.3 | 2.45x | yes | 20 / 20 |
| q11 | 38.8 | 11.3 | 3.45x | yes | 2541 / 2541 |
| q12 | 15.1 | 6.8 | 2.22x | yes | 2 / 2 |
| q13 | 40.3 | 22.7 | 1.77x | yes | 37 / 37 |
| q14 | 22.7 | 8.9 | 2.54x | yes | 1 / 1 |
| q15 | 32.1 | 4.6 | 7.03x | yes | 1 / 1 |
| q16 | 33.7 | 13.5 | 2.50x | yes | 2762 / 2762 |
| q17 | 28.1 | 9.1 | 3.08x | yes | 1 / 1 |
| q18 | 55.3 | 16.8 | 3.29x | yes | 5 / 5 |
| q19 | 34.9 | 13.2 | 2.64x | yes | 1 / 1 |
| q20 | 42.0 | 11.5 | 3.65x | yes | 9 / 9 |
| q21 | 64.4 | 20.7 | 3.11x | yes | 47 / 47 |
| q22 | 35.3 | 16.2 | 2.18x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.5 | 27.2 | 1.20x | yes | 4 / 4 |
| q02 | 105.7 | 31.6 | 3.35x | yes | 100 / 100 |
| q03 | 69.4 | 35.3 | 1.97x | yes | 10 / 10 |
| q04 | 28.9 | 21.8 | 1.32x | yes | 5 / 5 |
| q05 | 142.1 | 43.7 | 3.25x | yes | 5 / 5 |
| q06 | 11.4 | 7.6 | 1.50x | yes | 1 / 1 |
| q07 | 149.3 | 43.5 | 3.44x | yes | 4 / 4 |
| q08 | 179.7 | 56.2 | 3.20x | yes | 2 / 2 |
| q09 | 241.6 | 108.6 | 2.22x | yes | 175 / 175 |
| q10 | 145.4 | 76.0 | 1.91x | yes | 20 / 20 |
| q11 | 51.9 | 15.8 | 3.28x | yes | 1048 / 1048 |
| q12 | 25.5 | 17.9 | 1.43x | yes | 2 / 2 |
| q13 | 191.3 | 97.7 | 1.96x | yes | 42 / 42 |
| q14 | 52.1 | 50.0 | 1.04x | yes | 1 / 1 |
| q15 | 46.8 | 16.0 | 2.92x | yes | 1 / 1 |
| q16 | 95.9 | 54.4 | 1.76x | yes | 18314 / 18314 |
| q17 | 47.2 | 27.6 | 1.71x | yes | 1 / 1 |
| q18 | 132.7 | 84.4 | 1.57x | yes | 57 / 57 |
| q19 | 64.5 | 58.4 | 1.10x | yes | 1 / 1 |
| q20 | 91.1 | 33.7 | 2.70x | yes | 186 / 186 |
| q21 | 140.1 | 117.3 | 1.19x | yes | 100 / 100 |
| q22 | 88.0 | 54.3 | 1.62x | yes | 7 / 7 |

