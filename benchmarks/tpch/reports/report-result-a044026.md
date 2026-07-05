# TPC-H benchmark report

Commit: `a044026` - optimizer: pushdown rules descend CTEs; CTE-aware side classification (q15)
Generated: 2026-07-05 17:58
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
| single | 0.1 | 22/22 | 628.3 | 335.9 | 1.87x | 22 |
| single | 1 | 22/22 | 2902.5 | 1362.9 | 2.13x | 22 |
| fedparquet | 0.1 | 22/22 | 873.3 | 333.3 | 2.62x | 22 |
| fedparquet | 1 | 22/22 | 4667.4 | 1336.2 | 3.49x | 22 |
| fedpgduck | 0.1 | 22/22 | 1708.2 | 292.5 | 5.84x | 22 |
| fedpgduck | 1 | 22/22 | 9126.2 | 1061.8 | 8.60x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.6 | 14.8 | 2.00x | yes | 4 / 4 |
| q02 | 25.7 | 12.9 | 1.99x | yes | 44 / 44 |
| q03 | 21.6 | 13.7 | 1.57x | yes | 10 / 10 |
| q04 | 14.8 | 11.2 | 1.32x | yes | 5 / 5 |
| q05 | 21.8 | 15.3 | 1.43x | yes | 5 / 5 |
| q06 | 12.7 | 5.5 | 2.30x | yes | 1 / 1 |
| q07 | 29.4 | 16.3 | 1.81x | yes | 4 / 4 |
| q08 | 29.7 | 18.5 | 1.60x | yes | 2 / 2 |
| q09 | 67.6 | 24.9 | 2.71x | yes | 175 / 175 |
| q10 | 32.6 | 19.5 | 1.67x | yes | 20 / 20 |
| q11 | 33.5 | 19.1 | 1.75x | yes | 2541 / 2541 |
| q12 | 19.3 | 10.3 | 1.88x | yes | 2 / 2 |
| q13 | 25.7 | 26.2 | 0.98x | yes | 37 / 37 |
| q14 | 17.2 | 8.6 | 2.00x | yes | 1 / 1 |
| q15 | 28.0 | 7.5 | 3.76x | yes | 1 / 1 |
| q16 | 32.3 | 13.9 | 2.32x | yes | 2762 / 2762 |
| q17 | 23.2 | 12.0 | 1.93x | yes | 1 / 1 |
| q18 | 46.3 | 19.6 | 2.37x | yes | 5 / 5 |
| q19 | 25.5 | 12.0 | 2.13x | yes | 1 / 1 |
| q20 | 27.0 | 13.5 | 2.01x | yes | 9 / 9 |
| q21 | 32.8 | 30.9 | 1.06x | yes | 47 / 47 |
| q22 | 32.0 | 9.6 | 3.32x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 234.9 | 67.4 | 3.49x | yes | 4 / 4 |
| q02 | 37.2 | 24.7 | 1.51x | yes | 100 / 100 |
| q03 | 86.3 | 48.8 | 1.77x | yes | 10 / 10 |
| q04 | 37.3 | 33.1 | 1.13x | yes | 5 / 5 |
| q05 | 142.2 | 50.0 | 2.84x | yes | 5 / 5 |
| q06 | 41.6 | 22.5 | 1.85x | yes | 1 / 1 |
| q07 | 99.8 | 59.2 | 1.68x | yes | 4 / 4 |
| q08 | 90.9 | 63.7 | 1.43x | yes | 2 / 2 |
| q09 | 443.1 | 131.4 | 3.37x | yes | 175 / 175 |
| q10 | 127.6 | 91.3 | 1.40x | yes | 20 / 20 |
| q11 | 39.9 | 21.8 | 1.83x | yes | 1048 / 1048 |
| q12 | 63.8 | 32.0 | 1.99x | yes | 2 / 2 |
| q13 | 81.3 | 92.3 | 0.88x | yes | 42 / 42 |
| q14 | 52.9 | 41.1 | 1.29x | yes | 1 / 1 |
| q15 | 82.0 | 29.4 | 2.79x | yes | 1 / 1 |
| q16 | 158.8 | 59.7 | 2.66x | yes | 18314 / 18314 |
| q17 | 273.7 | 50.2 | 5.45x | yes | 1 / 1 |
| q18 | 351.0 | 102.6 | 3.42x | yes | 57 / 57 |
| q19 | 81.9 | 62.2 | 1.32x | yes | 1 / 1 |
| q20 | 130.6 | 48.1 | 2.72x | yes | 186 / 186 |
| q21 | 145.6 | 195.8 | 0.74x | yes | 100 / 100 |
| q22 | 100.2 | 35.8 | 2.80x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.9 | 15.4 | 1.94x | yes | 4 / 4 |
| q02 | 41.0 | 13.0 | 3.16x | yes | 44 / 44 |
| q03 | 30.1 | 13.6 | 2.21x | yes | 10 / 10 |
| q04 | 14.6 | 10.4 | 1.40x | yes | 5 / 5 |
| q05 | 43.5 | 16.1 | 2.70x | yes | 5 / 5 |
| q06 | 12.6 | 5.8 | 2.18x | yes | 1 / 1 |
| q07 | 56.7 | 17.2 | 3.29x | yes | 4 / 4 |
| q08 | 46.4 | 17.8 | 2.60x | yes | 2 / 2 |
| q09 | 86.8 | 25.0 | 3.47x | yes | 175 / 175 |
| q10 | 43.9 | 21.3 | 2.06x | yes | 20 / 20 |
| q11 | 36.9 | 18.2 | 2.03x | yes | 2541 / 2541 |
| q12 | 19.7 | 10.0 | 1.98x | yes | 2 / 2 |
| q13 | 42.6 | 26.7 | 1.59x | yes | 37 / 37 |
| q14 | 14.7 | 7.4 | 1.98x | yes | 1 / 1 |
| q15 | 31.7 | 7.6 | 4.16x | yes | 1 / 1 |
| q16 | 37.7 | 13.3 | 2.83x | yes | 2762 / 2762 |
| q17 | 31.3 | 11.2 | 2.79x | yes | 1 / 1 |
| q18 | 51.4 | 19.1 | 2.70x | yes | 5 / 5 |
| q19 | 23.8 | 11.9 | 2.00x | yes | 1 / 1 |
| q20 | 30.8 | 13.4 | 2.30x | yes | 9 / 9 |
| q21 | 115.7 | 28.8 | 4.02x | yes | 47 / 47 |
| q22 | 31.5 | 10.0 | 3.14x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 214.8 | 63.7 | 3.37x | yes | 4 / 4 |
| q02 | 76.4 | 26.0 | 2.94x | yes | 100 / 100 |
| q03 | 111.7 | 49.5 | 2.25x | yes | 10 / 10 |
| q04 | 37.9 | 31.9 | 1.19x | yes | 5 / 5 |
| q05 | 183.5 | 50.4 | 3.64x | yes | 5 / 5 |
| q06 | 42.3 | 21.9 | 1.93x | yes | 1 / 1 |
| q07 | 340.5 | 54.6 | 6.24x | yes | 4 / 4 |
| q08 | 120.0 | 65.0 | 1.84x | yes | 2 / 2 |
| q09 | 713.7 | 140.5 | 5.08x | yes | 175 / 175 |
| q10 | 174.7 | 89.7 | 1.95x | yes | 20 / 20 |
| q11 | 46.1 | 19.5 | 2.37x | yes | 1048 / 1048 |
| q12 | 62.0 | 31.1 | 1.99x | yes | 2 / 2 |
| q13 | 312.9 | 89.4 | 3.50x | yes | 42 / 42 |
| q14 | 55.4 | 44.0 | 1.26x | yes | 1 / 1 |
| q15 | 84.6 | 29.3 | 2.89x | yes | 1 / 1 |
| q16 | 166.4 | 61.3 | 2.71x | yes | 18314 / 18314 |
| q17 | 270.2 | 49.9 | 5.41x | yes | 1 / 1 |
| q18 | 555.9 | 102.7 | 5.41x | yes | 57 / 57 |
| q19 | 98.5 | 61.7 | 1.60x | yes | 1 / 1 |
| q20 | 108.1 | 46.1 | 2.34x | yes | 186 / 186 |
| q21 | 791.4 | 169.0 | 4.68x | yes | 100 / 100 |
| q22 | 100.3 | 38.7 | 2.59x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.6 | 6.1 | 3.90x | yes | 4 / 4 |
| q02 | 74.3 | 11.5 | 6.48x | yes | 44 / 44 |
| q03 | 63.6 | 10.7 | 5.96x | yes | 10 / 10 |
| q04 | 24.4 | 8.3 | 2.94x | yes | 5 / 5 |
| q05 | 93.4 | 14.3 | 6.54x | yes | 5 / 5 |
| q06 | 15.6 | 2.1 | 7.44x | yes | 1 / 1 |
| q07 | 88.4 | 14.4 | 6.15x | yes | 4 / 4 |
| q08 | 82.7 | 15.1 | 5.47x | yes | 2 / 2 |
| q09 | 126.0 | 24.3 | 5.19x | yes | 175 / 175 |
| q10 | 77.8 | 21.8 | 3.57x | yes | 20 / 20 |
| q11 | 55.3 | 11.1 | 5.00x | yes | 2541 / 2541 |
| q12 | 22.7 | 5.9 | 3.83x | yes | 2 / 2 |
| q13 | 112.0 | 21.5 | 5.20x | yes | 37 / 37 |
| q14 | 32.9 | 13.5 | 2.43x | yes | 1 / 1 |
| q15 | 42.5 | 4.7 | 9.05x | yes | 1 / 1 |
| q16 | 49.7 | 14.6 | 3.42x | yes | 2762 / 2762 |
| q17 | 64.9 | 9.0 | 7.17x | yes | 1 / 1 |
| q18 | 104.5 | 16.8 | 6.23x | yes | 5 / 5 |
| q19 | 39.8 | 13.4 | 2.97x | yes | 1 / 1 |
| q20 | 54.6 | 14.0 | 3.91x | yes | 9 / 9 |
| q21 | 375.9 | 20.0 | 18.84x | yes | 47 / 47 |
| q22 | 83.6 | 19.5 | 4.28x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 48.3 | 24.1 | 2.01x | yes | 4 / 4 |
| q02 | 136.2 | 30.0 | 4.55x | yes | 100 / 100 |
| q03 | 247.7 | 34.3 | 7.23x | yes | 10 / 10 |
| q04 | 45.5 | 22.6 | 2.01x | yes | 5 / 5 |
| q05 | 427.9 | 45.8 | 9.34x | yes | 5 / 5 |
| q06 | 26.9 | 6.9 | 3.92x | yes | 1 / 1 |
| q07 | 473.5 | 51.1 | 9.26x | yes | 4 / 4 |
| q08 | 162.6 | 51.6 | 3.15x | yes | 2 / 2 |
| q09 | 955.9 | 90.9 | 10.52x | yes | 175 / 175 |
| q10 | 369.4 | 73.2 | 5.04x | yes | 20 / 20 |
| q11 | 106.0 | 15.2 | 6.98x | yes | 1048 / 1048 |
| q12 | 41.3 | 16.4 | 2.51x | yes | 2 / 2 |
| q13 | 774.3 | 99.2 | 7.81x | yes | 42 / 42 |
| q14 | 101.8 | 38.7 | 2.63x | yes | 1 / 1 |
| q15 | 76.7 | 16.5 | 4.65x | yes | 1 / 1 |
| q16 | 133.9 | 57.9 | 2.31x | yes | 18314 / 18314 |
| q17 | 394.2 | 26.2 | 15.03x | yes | 1 / 1 |
| q18 | 768.5 | 87.5 | 8.79x | yes | 57 / 57 |
| q19 | 127.8 | 59.3 | 2.16x | yes | 1 / 1 |
| q20 | 124.1 | 32.3 | 3.84x | yes | 186 / 186 |
| q21 | 3161.3 | 118.9 | 26.60x | yes | 100 / 100 |
| q22 | 422.4 | 63.3 | 6.67x | yes | 7 / 7 |

