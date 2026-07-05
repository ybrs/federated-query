# TPC-H benchmark report

Commit: `37c413d` - optimizer: derive pushable single-relation predicates from ORs (q07)
Generated: 2026-07-05 23:15
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
| single | 0.1 | 22/22 | 622.9 | 336.9 | 1.85x | 22 |
| single | 1 | 22/22 | 2777.0 | 1352.0 | 2.05x | 22 |
| fedparquet | 0.1 | 22/22 | 915.8 | 333.5 | 2.75x | 22 |
| fedparquet | 1 | 22/22 | 4060.4 | 1344.9 | 3.02x | 22 |
| fedpgduck | 0.1 | 22/22 | 1388.3 | 275.8 | 5.03x | 22 |
| fedpgduck | 1 | 22/22 | 4146.7 | 1030.3 | 4.02x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 28.2 | 14.9 | 1.90x | yes | 4 / 4 |
| q02 | 24.6 | 13.5 | 1.82x | yes | 44 / 44 |
| q03 | 21.8 | 15.0 | 1.46x | yes | 10 / 10 |
| q04 | 13.9 | 11.9 | 1.17x | yes | 5 / 5 |
| q05 | 21.9 | 15.2 | 1.44x | yes | 5 / 5 |
| q06 | 11.1 | 6.1 | 1.82x | yes | 1 / 1 |
| q07 | 32.0 | 16.0 | 2.00x | yes | 4 / 4 |
| q08 | 31.3 | 18.2 | 1.72x | yes | 2 / 2 |
| q09 | 68.8 | 26.7 | 2.58x | yes | 175 / 175 |
| q10 | 31.8 | 20.2 | 1.57x | yes | 20 / 20 |
| q11 | 35.0 | 17.8 | 1.96x | yes | 2541 / 2541 |
| q12 | 18.9 | 10.5 | 1.80x | yes | 2 / 2 |
| q13 | 25.1 | 25.3 | 0.99x | yes | 37 / 37 |
| q14 | 16.6 | 7.9 | 2.10x | yes | 1 / 1 |
| q15 | 36.2 | 7.4 | 4.88x | yes | 1 / 1 |
| q16 | 36.4 | 13.4 | 2.71x | yes | 2762 / 2762 |
| q17 | 21.6 | 12.7 | 1.70x | yes | 1 / 1 |
| q18 | 41.1 | 19.0 | 2.16x | yes | 5 / 5 |
| q19 | 29.0 | 11.5 | 2.53x | yes | 1 / 1 |
| q20 | 27.2 | 12.9 | 2.11x | yes | 9 / 9 |
| q21 | 31.0 | 31.3 | 0.99x | yes | 47 / 47 |
| q22 | 19.4 | 9.3 | 2.08x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 220.4 | 63.1 | 3.49x | yes | 4 / 4 |
| q02 | 37.5 | 26.1 | 1.44x | yes | 100 / 100 |
| q03 | 87.3 | 49.0 | 1.78x | yes | 10 / 10 |
| q04 | 38.6 | 34.2 | 1.13x | yes | 5 / 5 |
| q05 | 142.5 | 50.2 | 2.84x | yes | 5 / 5 |
| q06 | 41.1 | 21.9 | 1.88x | yes | 1 / 1 |
| q07 | 98.5 | 59.9 | 1.64x | yes | 4 / 4 |
| q08 | 90.0 | 65.3 | 1.38x | yes | 2 / 2 |
| q09 | 437.9 | 137.7 | 3.18x | yes | 175 / 175 |
| q10 | 126.4 | 91.9 | 1.38x | yes | 20 / 20 |
| q11 | 35.4 | 18.3 | 1.93x | yes | 1048 / 1048 |
| q12 | 63.6 | 31.6 | 2.01x | yes | 2 / 2 |
| q13 | 78.7 | 99.7 | 0.79x | yes | 42 / 42 |
| q14 | 54.0 | 43.6 | 1.24x | yes | 1 / 1 |
| q15 | 84.9 | 30.0 | 2.83x | yes | 1 / 1 |
| q16 | 156.7 | 60.1 | 2.61x | yes | 18314 / 18314 |
| q17 | 274.5 | 49.0 | 5.61x | yes | 1 / 1 |
| q18 | 305.7 | 100.9 | 3.03x | yes | 57 / 57 |
| q19 | 88.9 | 65.2 | 1.36x | yes | 1 / 1 |
| q20 | 133.0 | 49.3 | 2.70x | yes | 186 / 186 |
| q21 | 129.4 | 169.0 | 0.77x | yes | 100 / 100 |
| q22 | 52.0 | 35.9 | 1.45x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.0 | 15.5 | 1.87x | yes | 4 / 4 |
| q02 | 61.1 | 12.4 | 4.93x | yes | 44 / 44 |
| q03 | 26.6 | 12.7 | 2.10x | yes | 10 / 10 |
| q04 | 14.4 | 12.8 | 1.13x | yes | 5 / 5 |
| q05 | 48.8 | 14.7 | 3.32x | yes | 5 / 5 |
| q06 | 12.5 | 5.7 | 2.20x | yes | 1 / 1 |
| q07 | 61.2 | 16.0 | 3.84x | yes | 4 / 4 |
| q08 | 65.3 | 17.1 | 3.82x | yes | 2 / 2 |
| q09 | 132.0 | 24.0 | 5.51x | yes | 175 / 175 |
| q10 | 41.3 | 19.5 | 2.12x | yes | 20 / 20 |
| q11 | 62.6 | 18.9 | 3.31x | yes | 2541 / 2541 |
| q12 | 19.0 | 11.0 | 1.73x | yes | 2 / 2 |
| q13 | 23.5 | 27.6 | 0.85x | yes | 37 / 37 |
| q14 | 14.5 | 7.6 | 1.91x | yes | 1 / 1 |
| q15 | 36.4 | 6.7 | 5.41x | yes | 1 / 1 |
| q16 | 39.4 | 13.6 | 2.90x | yes | 2762 / 2762 |
| q17 | 26.4 | 11.8 | 2.24x | yes | 1 / 1 |
| q18 | 53.9 | 19.9 | 2.71x | yes | 5 / 5 |
| q19 | 29.0 | 12.1 | 2.38x | yes | 1 / 1 |
| q20 | 36.0 | 13.0 | 2.78x | yes | 9 / 9 |
| q21 | 63.9 | 31.3 | 2.04x | yes | 47 / 47 |
| q22 | 19.0 | 9.8 | 1.94x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 218.4 | 65.0 | 3.36x | yes | 4 / 4 |
| q02 | 84.8 | 24.8 | 3.42x | yes | 100 / 100 |
| q03 | 107.7 | 48.8 | 2.21x | yes | 10 / 10 |
| q04 | 37.0 | 33.8 | 1.10x | yes | 5 / 5 |
| q05 | 186.6 | 50.7 | 3.68x | yes | 5 / 5 |
| q06 | 42.1 | 22.4 | 1.88x | yes | 1 / 1 |
| q07 | 133.3 | 54.2 | 2.46x | yes | 4 / 4 |
| q08 | 164.4 | 66.9 | 2.46x | yes | 2 / 2 |
| q09 | 1041.3 | 139.4 | 7.47x | yes | 175 / 175 |
| q10 | 184.7 | 110.5 | 1.67x | yes | 20 / 20 |
| q11 | 55.3 | 19.3 | 2.87x | yes | 1048 / 1048 |
| q12 | 63.0 | 33.7 | 1.87x | yes | 2 / 2 |
| q13 | 123.3 | 90.3 | 1.36x | yes | 42 / 42 |
| q14 | 54.2 | 41.1 | 1.32x | yes | 1 / 1 |
| q15 | 86.1 | 28.7 | 3.00x | yes | 1 / 1 |
| q16 | 175.5 | 61.0 | 2.88x | yes | 18314 / 18314 |
| q17 | 274.8 | 49.2 | 5.59x | yes | 1 / 1 |
| q18 | 578.2 | 99.9 | 5.79x | yes | 57 / 57 |
| q19 | 90.9 | 61.0 | 1.49x | yes | 1 / 1 |
| q20 | 110.9 | 48.1 | 2.31x | yes | 186 / 186 |
| q21 | 200.0 | 160.9 | 1.24x | yes | 100 / 100 |
| q22 | 47.7 | 35.1 | 1.36x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.1 | 6.4 | 3.62x | yes | 4 / 4 |
| q02 | 81.2 | 12.0 | 6.78x | yes | 44 / 44 |
| q03 | 48.6 | 9.4 | 5.17x | yes | 10 / 10 |
| q04 | 26.1 | 8.3 | 3.14x | yes | 5 / 5 |
| q05 | 61.8 | 12.4 | 4.97x | yes | 5 / 5 |
| q06 | 15.7 | 2.1 | 7.54x | yes | 1 / 1 |
| q07 | 87.6 | 13.7 | 6.40x | yes | 4 / 4 |
| q08 | 84.1 | 16.6 | 5.07x | yes | 2 / 2 |
| q09 | 161.3 | 22.3 | 7.22x | yes | 175 / 175 |
| q10 | 67.7 | 19.0 | 3.56x | yes | 20 / 20 |
| q11 | 73.3 | 10.8 | 6.78x | yes | 2541 / 2541 |
| q12 | 23.1 | 6.0 | 3.85x | yes | 2 / 2 |
| q13 | 49.6 | 24.4 | 2.03x | yes | 37 / 37 |
| q14 | 42.9 | 8.5 | 5.05x | yes | 1 / 1 |
| q15 | 49.4 | 4.2 | 11.65x | yes | 1 / 1 |
| q16 | 57.9 | 13.3 | 4.35x | yes | 2762 / 2762 |
| q17 | 48.9 | 9.0 | 5.46x | yes | 1 / 1 |
| q18 | 103.3 | 16.6 | 6.21x | yes | 5 / 5 |
| q19 | 48.9 | 13.2 | 3.69x | yes | 1 / 1 |
| q20 | 57.3 | 11.5 | 5.00x | yes | 9 / 9 |
| q21 | 132.9 | 19.7 | 6.74x | yes | 47 / 47 |
| q22 | 43.7 | 16.3 | 2.68x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 47.5 | 24.6 | 1.93x | yes | 4 / 4 |
| q02 | 157.1 | 30.8 | 5.10x | yes | 100 / 100 |
| q03 | 121.7 | 32.2 | 3.78x | yes | 10 / 10 |
| q04 | 45.4 | 21.9 | 2.07x | yes | 5 / 5 |
| q05 | 194.0 | 44.4 | 4.37x | yes | 5 / 5 |
| q06 | 27.9 | 6.7 | 4.16x | yes | 1 / 1 |
| q07 | 282.2 | 44.9 | 6.28x | yes | 4 / 4 |
| q08 | 164.5 | 48.5 | 3.39x | yes | 2 / 2 |
| q09 | 425.6 | 89.5 | 4.75x | yes | 175 / 175 |
| q10 | 264.0 | 65.2 | 4.05x | yes | 20 / 20 |
| q11 | 142.3 | 14.8 | 9.62x | yes | 1048 / 1048 |
| q12 | 42.5 | 16.3 | 2.60x | yes | 2 / 2 |
| q13 | 297.7 | 92.1 | 3.23x | yes | 42 / 42 |
| q14 | 106.6 | 38.1 | 2.80x | yes | 1 / 1 |
| q15 | 75.8 | 16.5 | 4.59x | yes | 1 / 1 |
| q16 | 134.7 | 52.2 | 2.58x | yes | 18314 / 18314 |
| q17 | 177.0 | 29.3 | 6.04x | yes | 1 / 1 |
| q18 | 641.9 | 87.6 | 7.33x | yes | 57 / 57 |
| q19 | 91.9 | 61.5 | 1.49x | yes | 1 / 1 |
| q20 | 138.8 | 34.5 | 4.02x | yes | 186 / 186 |
| q21 | 465.1 | 119.7 | 3.88x | yes | 100 / 100 |
| q22 | 102.7 | 58.9 | 1.75x | yes | 7 / 7 |

