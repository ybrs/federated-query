# TPC-H benchmark report

Commit: `6dcc2ca` - optimizer: COUNT(*) must not disable column pruning (fixes q21)
Generated: 2026-07-05 18:35
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
| single | 0.1 | 22/22 | 607.4 | 332.6 | 1.83x | 22 |
| single | 1 | 22/22 | 2832.3 | 1343.7 | 2.11x | 22 |
| fedparquet | 0.1 | 22/22 | 765.0 | 320.0 | 2.39x | 22 |
| fedparquet | 1 | 22/22 | 3773.8 | 1339.0 | 2.82x | 22 |
| fedpgduck | 0.1 | 22/22 | 1338.1 | 282.1 | 4.74x | 22 |
| fedpgduck | 1 | 22/22 | 5594.6 | 1096.0 | 5.10x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.9 | 14.9 | 2.00x | yes | 4 / 4 |
| q02 | 24.9 | 13.4 | 1.85x | yes | 44 / 44 |
| q03 | 20.8 | 15.2 | 1.37x | yes | 10 / 10 |
| q04 | 14.6 | 11.1 | 1.32x | yes | 5 / 5 |
| q05 | 24.4 | 14.8 | 1.65x | yes | 5 / 5 |
| q06 | 12.3 | 6.1 | 2.01x | yes | 1 / 1 |
| q07 | 28.0 | 14.2 | 1.98x | yes | 4 / 4 |
| q08 | 28.2 | 18.7 | 1.50x | yes | 2 / 2 |
| q09 | 73.3 | 27.8 | 2.64x | yes | 175 / 175 |
| q10 | 28.1 | 17.4 | 1.62x | yes | 20 / 20 |
| q11 | 35.0 | 17.4 | 2.02x | yes | 2541 / 2541 |
| q12 | 19.0 | 10.5 | 1.81x | yes | 2 / 2 |
| q13 | 24.6 | 25.6 | 0.96x | yes | 37 / 37 |
| q14 | 15.5 | 7.3 | 2.13x | yes | 1 / 1 |
| q15 | 28.5 | 7.9 | 3.59x | yes | 1 / 1 |
| q16 | 32.8 | 14.3 | 2.30x | yes | 2762 / 2762 |
| q17 | 23.5 | 12.4 | 1.90x | yes | 1 / 1 |
| q18 | 44.3 | 20.4 | 2.17x | yes | 5 / 5 |
| q19 | 21.9 | 11.5 | 1.90x | yes | 1 / 1 |
| q20 | 27.8 | 14.1 | 1.97x | yes | 9 / 9 |
| q21 | 30.6 | 28.5 | 1.07x | yes | 47 / 47 |
| q22 | 19.2 | 9.1 | 2.10x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 218.8 | 63.3 | 3.45x | yes | 4 / 4 |
| q02 | 38.8 | 24.3 | 1.60x | yes | 100 / 100 |
| q03 | 85.1 | 50.4 | 1.69x | yes | 10 / 10 |
| q04 | 42.0 | 35.4 | 1.19x | yes | 5 / 5 |
| q05 | 152.3 | 54.4 | 2.80x | yes | 5 / 5 |
| q06 | 40.7 | 21.6 | 1.89x | yes | 1 / 1 |
| q07 | 107.8 | 61.2 | 1.76x | yes | 4 / 4 |
| q08 | 92.1 | 63.8 | 1.44x | yes | 2 / 2 |
| q09 | 438.5 | 139.2 | 3.15x | yes | 175 / 175 |
| q10 | 124.1 | 90.2 | 1.38x | yes | 20 / 20 |
| q11 | 36.1 | 19.1 | 1.89x | yes | 1048 / 1048 |
| q12 | 65.7 | 33.0 | 1.99x | yes | 2 / 2 |
| q13 | 83.1 | 89.5 | 0.93x | yes | 42 / 42 |
| q14 | 51.7 | 41.8 | 1.24x | yes | 1 / 1 |
| q15 | 85.4 | 29.2 | 2.93x | yes | 1 / 1 |
| q16 | 162.1 | 60.9 | 2.66x | yes | 18314 / 18314 |
| q17 | 278.2 | 49.5 | 5.62x | yes | 1 / 1 |
| q18 | 338.8 | 100.9 | 3.36x | yes | 57 / 57 |
| q19 | 81.8 | 64.4 | 1.27x | yes | 1 / 1 |
| q20 | 131.2 | 46.8 | 2.81x | yes | 186 / 186 |
| q21 | 129.1 | 167.6 | 0.77x | yes | 100 / 100 |
| q22 | 48.7 | 37.1 | 1.31x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.4 | 15.2 | 1.54x | yes | 4 / 4 |
| q02 | 40.3 | 12.0 | 3.37x | yes | 44 / 44 |
| q03 | 30.2 | 13.7 | 2.20x | yes | 10 / 10 |
| q04 | 14.6 | 10.1 | 1.45x | yes | 5 / 5 |
| q05 | 45.7 | 14.2 | 3.21x | yes | 5 / 5 |
| q06 | 12.3 | 5.5 | 2.22x | yes | 1 / 1 |
| q07 | 54.6 | 15.6 | 3.49x | yes | 4 / 4 |
| q08 | 49.9 | 16.7 | 2.99x | yes | 2 / 2 |
| q09 | 88.6 | 25.6 | 3.46x | yes | 175 / 175 |
| q10 | 44.0 | 20.9 | 2.10x | yes | 20 / 20 |
| q11 | 36.3 | 16.8 | 2.16x | yes | 2541 / 2541 |
| q12 | 20.3 | 8.5 | 2.40x | yes | 2 / 2 |
| q13 | 23.0 | 25.1 | 0.92x | yes | 37 / 37 |
| q14 | 15.2 | 8.0 | 1.91x | yes | 1 / 1 |
| q15 | 26.4 | 6.9 | 3.81x | yes | 1 / 1 |
| q16 | 40.9 | 13.7 | 2.98x | yes | 2762 / 2762 |
| q17 | 31.1 | 11.5 | 2.71x | yes | 1 / 1 |
| q18 | 50.3 | 18.6 | 2.70x | yes | 5 / 5 |
| q19 | 24.1 | 11.2 | 2.15x | yes | 1 / 1 |
| q20 | 27.5 | 11.6 | 2.37x | yes | 9 / 9 |
| q21 | 46.5 | 29.6 | 1.57x | yes | 47 / 47 |
| q22 | 19.8 | 9.0 | 2.19x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.2 | 66.3 | 3.25x | yes | 4 / 4 |
| q02 | 82.6 | 26.3 | 3.14x | yes | 100 / 100 |
| q03 | 111.7 | 48.5 | 2.30x | yes | 10 / 10 |
| q04 | 36.0 | 32.7 | 1.10x | yes | 5 / 5 |
| q05 | 184.5 | 50.3 | 3.67x | yes | 5 / 5 |
| q06 | 39.3 | 21.3 | 1.84x | yes | 1 / 1 |
| q07 | 347.1 | 54.6 | 6.36x | yes | 4 / 4 |
| q08 | 121.6 | 65.8 | 1.85x | yes | 2 / 2 |
| q09 | 674.0 | 132.4 | 5.09x | yes | 175 / 175 |
| q10 | 172.3 | 104.2 | 1.65x | yes | 20 / 20 |
| q11 | 48.0 | 18.4 | 2.62x | yes | 1048 / 1048 |
| q12 | 62.2 | 31.7 | 1.96x | yes | 2 / 2 |
| q13 | 117.3 | 88.9 | 1.32x | yes | 42 / 42 |
| q14 | 55.6 | 40.9 | 1.36x | yes | 1 / 1 |
| q15 | 83.7 | 29.5 | 2.83x | yes | 1 / 1 |
| q16 | 166.0 | 59.4 | 2.80x | yes | 18314 / 18314 |
| q17 | 271.6 | 48.8 | 5.57x | yes | 1 / 1 |
| q18 | 546.4 | 100.0 | 5.46x | yes | 57 / 57 |
| q19 | 104.8 | 68.0 | 1.54x | yes | 1 / 1 |
| q20 | 109.3 | 47.5 | 2.30x | yes | 186 / 186 |
| q21 | 174.6 | 166.4 | 1.05x | yes | 100 / 100 |
| q22 | 50.0 | 37.0 | 1.35x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.8 | 6.5 | 3.64x | yes | 4 / 4 |
| q02 | 72.4 | 11.4 | 6.36x | yes | 44 / 44 |
| q03 | 61.9 | 9.0 | 6.91x | yes | 10 / 10 |
| q04 | 25.5 | 7.0 | 3.65x | yes | 5 / 5 |
| q05 | 93.0 | 14.9 | 6.24x | yes | 5 / 5 |
| q06 | 15.4 | 2.2 | 7.14x | yes | 1 / 1 |
| q07 | 89.1 | 13.8 | 6.46x | yes | 4 / 4 |
| q08 | 78.8 | 15.7 | 5.02x | yes | 2 / 2 |
| q09 | 120.6 | 25.8 | 4.68x | yes | 175 / 175 |
| q10 | 91.2 | 20.1 | 4.53x | yes | 20 / 20 |
| q11 | 53.5 | 11.1 | 4.82x | yes | 2541 / 2541 |
| q12 | 23.3 | 6.1 | 3.80x | yes | 2 / 2 |
| q13 | 50.7 | 21.9 | 2.32x | yes | 37 / 37 |
| q14 | 34.6 | 10.0 | 3.46x | yes | 1 / 1 |
| q15 | 45.6 | 5.0 | 9.05x | yes | 1 / 1 |
| q16 | 48.3 | 14.0 | 3.46x | yes | 2762 / 2762 |
| q17 | 55.1 | 9.1 | 6.07x | yes | 1 / 1 |
| q18 | 99.7 | 16.2 | 6.17x | yes | 5 / 5 |
| q19 | 45.2 | 13.6 | 3.32x | yes | 1 / 1 |
| q20 | 55.5 | 10.6 | 5.23x | yes | 9 / 9 |
| q21 | 108.2 | 20.5 | 5.27x | yes | 47 / 47 |
| q22 | 46.5 | 17.7 | 2.63x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.1 | 26.5 | 1.85x | yes | 4 / 4 |
| q02 | 140.0 | 32.8 | 4.27x | yes | 100 / 100 |
| q03 | 245.8 | 35.3 | 6.97x | yes | 10 / 10 |
| q04 | 45.5 | 22.4 | 2.03x | yes | 5 / 5 |
| q05 | 439.2 | 45.7 | 9.61x | yes | 5 / 5 |
| q06 | 26.7 | 7.7 | 3.46x | yes | 1 / 1 |
| q07 | 478.6 | 43.0 | 11.12x | yes | 4 / 4 |
| q08 | 160.5 | 54.8 | 2.93x | yes | 2 / 2 |
| q09 | 950.5 | 93.0 | 10.22x | yes | 175 / 175 |
| q10 | 377.7 | 74.2 | 5.09x | yes | 20 / 20 |
| q11 | 109.1 | 14.8 | 7.39x | yes | 1048 / 1048 |
| q12 | 42.2 | 16.2 | 2.60x | yes | 2 / 2 |
| q13 | 288.3 | 95.1 | 3.03x | yes | 42 / 42 |
| q14 | 103.5 | 49.4 | 2.09x | yes | 1 / 1 |
| q15 | 82.8 | 17.7 | 4.68x | yes | 1 / 1 |
| q16 | 136.7 | 53.3 | 2.56x | yes | 18314 / 18314 |
| q17 | 395.2 | 27.1 | 14.57x | yes | 1 / 1 |
| q18 | 782.7 | 90.7 | 8.63x | yes | 57 / 57 |
| q19 | 136.3 | 71.4 | 1.91x | yes | 1 / 1 |
| q20 | 123.4 | 31.5 | 3.92x | yes | 186 / 186 |
| q21 | 365.9 | 138.2 | 2.65x | yes | 100 / 100 |
| q22 | 114.6 | 55.0 | 2.08x | yes | 7 / 7 |

