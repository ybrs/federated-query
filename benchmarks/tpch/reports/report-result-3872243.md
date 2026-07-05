# TPC-H benchmark report

Commit: `3872243` - benchmarks: reproducible TPC-H report harness (bench.py) + parquet export
Generated: 2026-07-05 11:59
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
| single | 0.1 | 22/22 | 1483.6 | 343.9 | 4.31x | 22 |
| single | 1 | 21/22 | 16664.9 | 1211.3 | 13.76x | 21 |
| fedparquet | 0.1 | 22/22 | 2190.0 | 338.3 | 6.47x | 22 |
| fedparquet | 1 | 21/22 | 22420.7 | 1205.8 | 18.59x | 21 |
| fedpgduck | 0.1 | 22/22 | 3723.6 | 310.1 | 12.01x | 22 |
| fedpgduck | 1 | 22/22 | 47483.5 | 1231.9 | 38.55x | 22 |

## Issues

| Cell | SF | Query | Status | Detail |
| --- | --- | --- | --- | --- |
| single | 1 | q09 | KILLED | KILLED |
| fedparquet | 1 | q09 | KILLED | KILLED |

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 30.9 | 15.8 | 1.96x | yes | 4 / 4 |
| q02 | 131.8 | 12.5 | 10.58x | yes | 44 / 44 |
| q03 | 20.5 | 12.6 | 1.63x | yes | 10 / 10 |
| q04 | 16.1 | 14.0 | 1.15x | yes | 5 / 5 |
| q05 | 21.8 | 15.2 | 1.43x | yes | 5 / 5 |
| q06 | 13.9 | 6.1 | 2.27x | yes | 1 / 1 |
| q07 | 39.0 | 19.5 | 1.99x | yes | 4 / 4 |
| q08 | 293.7 | 17.0 | 17.25x | yes | 2 / 2 |
| q09 | 417.5 | 28.2 | 14.80x | yes | 175 / 175 |
| q10 | 29.0 | 20.5 | 1.42x | yes | 20 / 20 |
| q11 | 35.4 | 19.5 | 1.82x | yes | 2541 / 2541 |
| q12 | 19.9 | 11.4 | 1.74x | yes | 2 / 2 |
| q13 | 25.4 | 26.1 | 0.97x | yes | 37 / 37 |
| q14 | 16.8 | 9.1 | 1.85x | yes | 1 / 1 |
| q15 | 66.4 | 7.2 | 9.18x | yes | 1 / 1 |
| q16 | 33.3 | 13.5 | 2.46x | yes | 2762 / 2762 |
| q17 | 23.6 | 13.3 | 1.78x | yes | 1 / 1 |
| q18 | 145.7 | 17.5 | 8.33x | yes | 5 / 5 |
| q19 | 23.1 | 12.4 | 1.87x | yes | 1 / 1 |
| q20 | 28.2 | 14.5 | 1.95x | yes | 9 / 9 |
| q21 | 32.9 | 29.2 | 1.12x | yes | 47 / 47 |
| q22 | 18.9 | 8.9 | 2.12x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 216.8 | 65.4 | 3.31x | yes | 4 / 4 |
| q02 | 3857.5 | 24.3 | 159.07x | yes | 100 / 100 |
| q03 | 87.1 | 50.1 | 1.74x | yes | 10 / 10 |
| q04 | 39.1 | 34.1 | 1.14x | yes | 5 / 5 |
| q05 | 145.8 | 51.3 | 2.84x | yes | 5 / 5 |
| q06 | 40.5 | 21.9 | 1.85x | yes | 1 / 1 |
| q07 | 193.3 | 55.6 | 3.47x | yes | 4 / 4 |
| q08 | 6253.4 | 64.6 | 96.76x | yes | 2 / 2 |
| q09 | - | - | - | KILLED | - |
| q10 | 128.1 | 91.0 | 1.41x | yes | 20 / 20 |
| q11 | 36.0 | 18.5 | 1.95x | yes | 1048 / 1048 |
| q12 | 64.3 | 32.5 | 1.98x | yes | 2 / 2 |
| q13 | 80.0 | 97.2 | 0.82x | yes | 42 / 42 |
| q14 | 56.8 | 41.7 | 1.36x | yes | 1 / 1 |
| q15 | 3140.7 | 29.7 | 105.75x | yes | 1 / 1 |
| q16 | 160.2 | 63.0 | 2.54x | yes | 18314 / 18314 |
| q17 | 280.5 | 48.5 | 5.78x | yes | 1 / 1 |
| q18 | 1429.0 | 101.6 | 14.06x | yes | 57 / 57 |
| q19 | 83.1 | 60.8 | 1.37x | yes | 1 / 1 |
| q20 | 144.8 | 53.2 | 2.72x | yes | 186 / 186 |
| q21 | 185.6 | 167.4 | 1.11x | yes | 100 / 100 |
| q22 | 42.3 | 38.7 | 1.09x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.7 | 15.5 | 1.92x | yes | 4 / 4 |
| q02 | 164.7 | 11.9 | 13.82x | yes | 44 / 44 |
| q03 | 90.8 | 13.5 | 6.73x | yes | 10 / 10 |
| q04 | 14.4 | 10.2 | 1.40x | yes | 5 / 5 |
| q05 | 139.2 | 17.6 | 7.89x | yes | 5 / 5 |
| q06 | 11.5 | 5.7 | 2.02x | yes | 1 / 1 |
| q07 | 233.8 | 15.1 | 15.45x | yes | 4 / 4 |
| q08 | 292.8 | 18.1 | 16.21x | yes | 2 / 2 |
| q09 | 366.2 | 25.6 | 14.31x | yes | 175 / 175 |
| q10 | 90.9 | 20.6 | 4.41x | yes | 20 / 20 |
| q11 | 80.4 | 17.0 | 4.72x | yes | 2541 / 2541 |
| q12 | 19.2 | 10.1 | 1.90x | yes | 2 / 2 |
| q13 | 41.4 | 26.7 | 1.55x | yes | 37 / 37 |
| q14 | 15.8 | 9.2 | 1.71x | yes | 1 / 1 |
| q15 | 62.7 | 7.0 | 8.94x | yes | 1 / 1 |
| q16 | 63.8 | 15.0 | 4.24x | yes | 2762 / 2762 |
| q17 | 29.4 | 12.7 | 2.31x | yes | 1 / 1 |
| q18 | 189.3 | 19.3 | 9.82x | yes | 5 / 5 |
| q19 | 25.5 | 12.6 | 2.02x | yes | 1 / 1 |
| q20 | 28.4 | 13.9 | 2.05x | yes | 9 / 9 |
| q21 | 168.7 | 31.4 | 5.37x | yes | 47 / 47 |
| q22 | 31.7 | 9.4 | 3.36x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.0 | 63.9 | 3.36x | yes | 4 / 4 |
| q02 | 3932.9 | 24.4 | 161.28x | yes | 100 / 100 |
| q03 | 676.7 | 48.8 | 13.88x | yes | 10 / 10 |
| q04 | 37.6 | 34.8 | 1.08x | yes | 5 / 5 |
| q05 | 1138.9 | 51.5 | 22.11x | yes | 5 / 5 |
| q06 | 41.6 | 21.8 | 1.91x | yes | 1 / 1 |
| q07 | 2092.1 | 53.9 | 38.83x | yes | 4 / 4 |
| q08 | 6051.0 | 62.0 | 97.57x | yes | 2 / 2 |
| q09 | - | - | - | KILLED | - |
| q10 | 560.9 | 91.8 | 6.11x | yes | 20 / 20 |
| q11 | 140.4 | 19.5 | 7.20x | yes | 1048 / 1048 |
| q12 | 64.9 | 31.9 | 2.03x | yes | 2 / 2 |
| q13 | 322.9 | 94.8 | 3.41x | yes | 42 / 42 |
| q14 | 57.2 | 43.8 | 1.30x | yes | 1 / 1 |
| q15 | 3237.4 | 29.8 | 108.80x | yes | 1 / 1 |
| q16 | 258.5 | 62.5 | 4.13x | yes | 18314 / 18314 |
| q17 | 275.8 | 50.3 | 5.49x | yes | 1 / 1 |
| q18 | 1710.8 | 102.8 | 16.64x | yes | 57 / 57 |
| q19 | 104.7 | 69.1 | 1.51x | yes | 1 / 1 |
| q20 | 114.3 | 48.5 | 2.36x | yes | 186 / 186 |
| q21 | 1283.2 | 163.1 | 7.87x | yes | 100 / 100 |
| q22 | 103.8 | 36.7 | 2.83x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.8 | 6.6 | 3.59x | yes | 4 / 4 |
| q02 | 122.7 | 11.6 | 10.54x | yes | 44 / 44 |
| q03 | 156.4 | 9.9 | 15.74x | yes | 10 / 10 |
| q04 | 24.9 | 6.8 | 3.67x | yes | 5 / 5 |
| q05 | 301.8 | 13.9 | 21.76x | yes | 5 / 5 |
| q06 | 16.1 | 2.1 | 7.72x | yes | 1 / 1 |
| q07 | 351.1 | 18.8 | 18.70x | yes | 4 / 4 |
| q08 | 367.4 | 18.3 | 20.08x | yes | 2 / 2 |
| q09 | 641.2 | 22.3 | 28.74x | yes | 175 / 175 |
| q10 | 123.0 | 19.8 | 6.22x | yes | 20 / 20 |
| q11 | 100.9 | 11.4 | 8.84x | yes | 2541 / 2541 |
| q12 | 22.0 | 5.8 | 3.81x | yes | 2 / 2 |
| q13 | 117.5 | 21.5 | 5.46x | yes | 37 / 37 |
| q14 | 32.8 | 8.8 | 3.72x | yes | 1 / 1 |
| q15 | 129.4 | 4.5 | 28.75x | yes | 1 / 1 |
| q16 | 62.0 | 13.4 | 4.64x | yes | 2762 / 2762 |
| q17 | 57.0 | 9.1 | 6.29x | yes | 1 / 1 |
| q18 | 429.5 | 27.0 | 15.92x | yes | 5 / 5 |
| q19 | 41.8 | 14.4 | 2.91x | yes | 1 / 1 |
| q20 | 54.8 | 11.3 | 4.83x | yes | 9 / 9 |
| q21 | 462.9 | 36.9 | 12.55x | yes | 47 / 47 |
| q22 | 84.5 | 16.0 | 5.29x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.3 | 25.4 | 1.94x | yes | 4 / 4 |
| q02 | 1134.5 | 39.9 | 28.42x | yes | 100 / 100 |
| q03 | 1410.1 | 37.1 | 37.96x | yes | 10 / 10 |
| q04 | 45.8 | 21.9 | 2.10x | yes | 5 / 5 |
| q05 | 2580.7 | 59.2 | 43.58x | yes | 5 / 5 |
| q06 | 26.8 | 7.2 | 3.72x | yes | 1 / 1 |
| q07 | 2852.3 | 41.3 | 69.05x | yes | 4 / 4 |
| q08 | 5094.4 | 59.6 | 85.51x | yes | 2 / 2 |
| q09 | 19441.2 | 117.5 | 165.39x | yes | 175 / 175 |
| q10 | 773.9 | 72.4 | 10.69x | yes | 20 / 20 |
| q11 | 326.4 | 14.5 | 22.48x | yes | 1048 / 1048 |
| q12 | 42.3 | 17.8 | 2.37x | yes | 2 / 2 |
| q13 | 767.6 | 92.4 | 8.30x | yes | 42 / 42 |
| q14 | 101.3 | 40.4 | 2.51x | yes | 1 / 1 |
| q15 | 3542.7 | 16.8 | 210.90x | yes | 1 / 1 |
| q16 | 320.9 | 54.7 | 5.87x | yes | 18314 / 18314 |
| q17 | 393.2 | 26.3 | 14.95x | yes | 1 / 1 |
| q18 | 3749.8 | 90.0 | 41.68x | yes | 57 / 57 |
| q19 | 142.6 | 69.8 | 2.04x | yes | 1 / 1 |
| q20 | 127.4 | 35.3 | 3.61x | yes | 186 / 186 |
| q21 | 4114.9 | 235.7 | 17.46x | yes | 100 / 100 |
| q22 | 445.3 | 56.5 | 7.88x | yes | 7 / 7 |

