# TPC-H benchmark report

Commit: `39a1c8f` - tests(A3): pin the same-source island collapse (q05 pattern)
Generated: 2026-07-05 20:47
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
| single | 0.1 | 22/22 | 596.3 | 327.3 | 1.82x | 22 |
| single | 1 | 22/22 | 2783.1 | 1316.8 | 2.11x | 22 |
| fedparquet | 0.1 | 22/22 | 768.0 | 329.8 | 2.33x | 22 |
| fedparquet | 1 | 22/22 | 4168.0 | 1331.9 | 3.13x | 22 |
| fedpgduck | 0.1 | 22/22 | 1310.9 | 293.9 | 4.46x | 22 |
| fedpgduck | 1 | 22/22 | 5209.3 | 1039.5 | 5.01x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.3 | 15.2 | 1.92x | yes | 4 / 4 |
| q02 | 24.8 | 12.6 | 1.98x | yes | 44 / 44 |
| q03 | 19.4 | 13.7 | 1.42x | yes | 10 / 10 |
| q04 | 15.7 | 11.6 | 1.36x | yes | 5 / 5 |
| q05 | 23.9 | 15.5 | 1.54x | yes | 5 / 5 |
| q06 | 12.2 | 5.5 | 2.24x | yes | 1 / 1 |
| q07 | 28.6 | 16.3 | 1.76x | yes | 4 / 4 |
| q08 | 29.3 | 17.4 | 1.69x | yes | 2 / 2 |
| q09 | 66.6 | 24.7 | 2.70x | yes | 175 / 175 |
| q10 | 30.3 | 17.5 | 1.73x | yes | 20 / 20 |
| q11 | 33.4 | 18.2 | 1.83x | yes | 2541 / 2541 |
| q12 | 20.5 | 10.0 | 2.06x | yes | 2 / 2 |
| q13 | 25.4 | 27.4 | 0.93x | yes | 37 / 37 |
| q14 | 15.0 | 7.5 | 2.01x | yes | 1 / 1 |
| q15 | 26.4 | 6.7 | 3.95x | yes | 1 / 1 |
| q16 | 32.7 | 13.2 | 2.47x | yes | 2762 / 2762 |
| q17 | 22.1 | 11.4 | 1.93x | yes | 1 / 1 |
| q18 | 40.4 | 17.5 | 2.31x | yes | 5 / 5 |
| q19 | 23.1 | 12.3 | 1.88x | yes | 1 / 1 |
| q20 | 26.6 | 13.3 | 1.99x | yes | 9 / 9 |
| q21 | 30.8 | 30.2 | 1.02x | yes | 47 / 47 |
| q22 | 20.0 | 9.9 | 2.01x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 211.0 | 64.8 | 3.26x | yes | 4 / 4 |
| q02 | 37.0 | 24.4 | 1.52x | yes | 100 / 100 |
| q03 | 91.5 | 51.7 | 1.77x | yes | 10 / 10 |
| q04 | 36.7 | 37.5 | 0.98x | yes | 5 / 5 |
| q05 | 137.6 | 50.6 | 2.72x | yes | 5 / 5 |
| q06 | 42.5 | 21.6 | 1.97x | yes | 1 / 1 |
| q07 | 97.6 | 55.2 | 1.77x | yes | 4 / 4 |
| q08 | 91.6 | 62.6 | 1.46x | yes | 2 / 2 |
| q09 | 438.1 | 131.1 | 3.34x | yes | 175 / 175 |
| q10 | 122.9 | 88.3 | 1.39x | yes | 20 / 20 |
| q11 | 36.5 | 19.5 | 1.87x | yes | 1048 / 1048 |
| q12 | 60.8 | 31.4 | 1.94x | yes | 2 / 2 |
| q13 | 78.0 | 86.6 | 0.90x | yes | 42 / 42 |
| q14 | 53.0 | 40.9 | 1.29x | yes | 1 / 1 |
| q15 | 85.7 | 28.3 | 3.03x | yes | 1 / 1 |
| q16 | 154.7 | 60.0 | 2.58x | yes | 18314 / 18314 |
| q17 | 273.2 | 49.1 | 5.56x | yes | 1 / 1 |
| q18 | 346.5 | 100.2 | 3.46x | yes | 57 / 57 |
| q19 | 81.6 | 62.9 | 1.30x | yes | 1 / 1 |
| q20 | 129.4 | 46.9 | 2.76x | yes | 186 / 186 |
| q21 | 125.4 | 164.2 | 0.76x | yes | 100 / 100 |
| q22 | 51.5 | 39.0 | 1.32x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 28.8 | 15.4 | 1.87x | yes | 4 / 4 |
| q02 | 42.5 | 12.3 | 3.45x | yes | 44 / 44 |
| q03 | 27.0 | 13.8 | 1.96x | yes | 10 / 10 |
| q04 | 14.2 | 12.3 | 1.15x | yes | 5 / 5 |
| q05 | 31.8 | 14.2 | 2.24x | yes | 5 / 5 |
| q06 | 13.8 | 5.8 | 2.37x | yes | 1 / 1 |
| q07 | 53.7 | 15.3 | 3.51x | yes | 4 / 4 |
| q08 | 42.2 | 17.0 | 2.48x | yes | 2 / 2 |
| q09 | 110.0 | 22.6 | 4.86x | yes | 175 / 175 |
| q10 | 37.7 | 18.5 | 2.03x | yes | 20 / 20 |
| q11 | 36.4 | 17.2 | 2.11x | yes | 2541 / 2541 |
| q12 | 18.3 | 9.8 | 1.86x | yes | 2 / 2 |
| q13 | 23.3 | 24.7 | 0.94x | yes | 37 / 37 |
| q14 | 16.0 | 8.5 | 1.88x | yes | 1 / 1 |
| q15 | 25.6 | 7.2 | 3.57x | yes | 1 / 1 |
| q16 | 41.8 | 13.7 | 3.06x | yes | 2762 / 2762 |
| q17 | 32.2 | 13.1 | 2.47x | yes | 1 / 1 |
| q18 | 54.0 | 19.4 | 2.79x | yes | 5 / 5 |
| q19 | 24.8 | 12.6 | 1.96x | yes | 1 / 1 |
| q20 | 26.9 | 14.0 | 1.92x | yes | 9 / 9 |
| q21 | 47.7 | 32.4 | 1.47x | yes | 47 / 47 |
| q22 | 19.3 | 10.0 | 1.93x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 214.0 | 62.4 | 3.43x | yes | 4 / 4 |
| q02 | 77.6 | 23.1 | 3.36x | yes | 100 / 100 |
| q03 | 104.5 | 49.0 | 2.13x | yes | 10 / 10 |
| q04 | 37.2 | 34.7 | 1.07x | yes | 5 / 5 |
| q05 | 179.8 | 50.1 | 3.59x | yes | 5 / 5 |
| q06 | 41.7 | 21.8 | 1.91x | yes | 1 / 1 |
| q07 | 320.9 | 57.8 | 5.55x | yes | 4 / 4 |
| q08 | 263.1 | 62.9 | 4.18x | yes | 2 / 2 |
| q09 | 973.3 | 133.3 | 7.30x | yes | 175 / 175 |
| q10 | 155.6 | 92.2 | 1.69x | yes | 20 / 20 |
| q11 | 48.3 | 19.2 | 2.51x | yes | 1048 / 1048 |
| q12 | 60.9 | 32.0 | 1.91x | yes | 2 / 2 |
| q13 | 125.1 | 91.6 | 1.37x | yes | 42 / 42 |
| q14 | 54.1 | 44.4 | 1.22x | yes | 1 / 1 |
| q15 | 82.4 | 29.1 | 2.83x | yes | 1 / 1 |
| q16 | 195.0 | 65.7 | 2.97x | yes | 18314 / 18314 |
| q17 | 264.9 | 49.6 | 5.34x | yes | 1 / 1 |
| q18 | 539.7 | 99.9 | 5.40x | yes | 57 / 57 |
| q19 | 94.0 | 65.3 | 1.44x | yes | 1 / 1 |
| q20 | 111.8 | 45.4 | 2.46x | yes | 186 / 186 |
| q21 | 173.2 | 166.1 | 1.04x | yes | 100 / 100 |
| q22 | 50.9 | 36.3 | 1.40x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.0 | 6.4 | 3.57x | yes | 4 / 4 |
| q02 | 73.5 | 11.6 | 6.31x | yes | 44 / 44 |
| q03 | 43.3 | 10.9 | 3.98x | yes | 10 / 10 |
| q04 | 27.3 | 8.1 | 3.38x | yes | 5 / 5 |
| q05 | 60.1 | 17.8 | 3.38x | yes | 5 / 5 |
| q06 | 15.0 | 1.9 | 7.74x | yes | 1 / 1 |
| q07 | 80.2 | 14.0 | 5.71x | yes | 4 / 4 |
| q08 | 77.4 | 16.9 | 4.58x | yes | 2 / 2 |
| q09 | 145.9 | 23.9 | 6.10x | yes | 175 / 175 |
| q10 | 67.8 | 19.0 | 3.56x | yes | 20 / 20 |
| q11 | 60.8 | 11.3 | 5.39x | yes | 2541 / 2541 |
| q12 | 22.7 | 6.1 | 3.74x | yes | 2 / 2 |
| q13 | 48.2 | 22.9 | 2.10x | yes | 37 / 37 |
| q14 | 32.2 | 9.1 | 3.55x | yes | 1 / 1 |
| q15 | 43.7 | 5.2 | 8.34x | yes | 1 / 1 |
| q16 | 44.9 | 13.3 | 3.38x | yes | 2762 / 2762 |
| q17 | 55.9 | 10.6 | 5.29x | yes | 1 / 1 |
| q18 | 113.2 | 17.5 | 6.47x | yes | 5 / 5 |
| q19 | 48.3 | 17.9 | 2.70x | yes | 1 / 1 |
| q20 | 57.3 | 11.2 | 5.11x | yes | 9 / 9 |
| q21 | 104.4 | 19.4 | 5.37x | yes | 47 / 47 |
| q22 | 65.7 | 18.8 | 3.49x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 48.8 | 24.1 | 2.03x | yes | 4 / 4 |
| q02 | 135.6 | 31.7 | 4.28x | yes | 100 / 100 |
| q03 | 123.0 | 31.1 | 3.96x | yes | 10 / 10 |
| q04 | 47.0 | 21.1 | 2.23x | yes | 5 / 5 |
| q05 | 194.7 | 40.5 | 4.81x | yes | 5 / 5 |
| q06 | 26.0 | 7.2 | 3.63x | yes | 1 / 1 |
| q07 | 415.5 | 44.3 | 9.39x | yes | 4 / 4 |
| q08 | 294.1 | 48.8 | 6.03x | yes | 2 / 2 |
| q09 | 995.9 | 92.3 | 10.79x | yes | 175 / 175 |
| q10 | 269.6 | 83.6 | 3.23x | yes | 20 / 20 |
| q11 | 108.4 | 15.0 | 7.23x | yes | 1048 / 1048 |
| q12 | 43.3 | 16.4 | 2.64x | yes | 2 / 2 |
| q13 | 285.1 | 89.9 | 3.17x | yes | 42 / 42 |
| q14 | 101.5 | 39.6 | 2.56x | yes | 1 / 1 |
| q15 | 77.6 | 16.3 | 4.75x | yes | 1 / 1 |
| q16 | 133.5 | 52.4 | 2.55x | yes | 18314 / 18314 |
| q17 | 387.3 | 25.5 | 15.21x | yes | 1 / 1 |
| q18 | 774.4 | 86.8 | 8.92x | yes | 57 / 57 |
| q19 | 132.7 | 62.2 | 2.13x | yes | 1 / 1 |
| q20 | 124.0 | 31.5 | 3.94x | yes | 186 / 186 |
| q21 | 375.9 | 121.1 | 3.10x | yes | 100 / 100 |
| q22 | 115.1 | 58.1 | 1.98x | yes | 7 / 7 |

