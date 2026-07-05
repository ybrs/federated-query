# TPC-H benchmark report

Commit: `979969a` - benchmarks: shape-fix gate (report-result-37c413d) - 4.02x fair federated
Generated: 2026-07-05 23:27
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
| single | 0.1 | 22/22 | 617.0 | 337.0 | 1.83x | 22 |
| single | 1 | 22/22 | 2786.5 | 1349.3 | 2.07x | 22 |
| fedparquet | 0.1 | 22/22 | 909.3 | 335.8 | 2.71x | 22 |
| fedparquet | 1 | 22/22 | 3970.7 | 1348.1 | 2.95x | 22 |
| fedpgduck | 0.1 | 22/22 | 1400.9 | 293.4 | 4.77x | 22 |
| fedpgduck | 1 | 22/22 | 4224.9 | 1060.2 | 3.99x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.9 | 15.4 | 1.95x | yes | 4 / 4 |
| q02 | 25.6 | 12.5 | 2.06x | yes | 44 / 44 |
| q03 | 21.1 | 12.5 | 1.69x | yes | 10 / 10 |
| q04 | 15.6 | 12.1 | 1.28x | yes | 5 / 5 |
| q05 | 22.3 | 16.3 | 1.37x | yes | 5 / 5 |
| q06 | 11.1 | 6.1 | 1.83x | yes | 1 / 1 |
| q07 | 33.2 | 16.1 | 2.07x | yes | 4 / 4 |
| q08 | 27.9 | 19.7 | 1.41x | yes | 2 / 2 |
| q09 | 68.7 | 26.8 | 2.56x | yes | 175 / 175 |
| q10 | 29.9 | 19.0 | 1.58x | yes | 20 / 20 |
| q11 | 33.1 | 17.6 | 1.88x | yes | 2541 / 2541 |
| q12 | 19.3 | 9.7 | 2.00x | yes | 2 / 2 |
| q13 | 25.6 | 29.5 | 0.87x | yes | 37 / 37 |
| q14 | 14.7 | 8.8 | 1.67x | yes | 1 / 1 |
| q15 | 39.1 | 7.2 | 5.47x | yes | 1 / 1 |
| q16 | 32.4 | 13.7 | 2.37x | yes | 2762 / 2762 |
| q17 | 22.7 | 12.5 | 1.82x | yes | 1 / 1 |
| q18 | 38.3 | 18.7 | 2.04x | yes | 5 / 5 |
| q19 | 29.4 | 11.4 | 2.58x | yes | 1 / 1 |
| q20 | 25.8 | 13.0 | 1.99x | yes | 9 / 9 |
| q21 | 32.2 | 29.6 | 1.09x | yes | 47 / 47 |
| q22 | 19.0 | 9.0 | 2.11x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 221.5 | 66.9 | 3.31x | yes | 4 / 4 |
| q02 | 38.4 | 24.4 | 1.58x | yes | 100 / 100 |
| q03 | 84.8 | 49.2 | 1.72x | yes | 10 / 10 |
| q04 | 38.1 | 33.4 | 1.14x | yes | 5 / 5 |
| q05 | 141.7 | 51.2 | 2.77x | yes | 5 / 5 |
| q06 | 40.8 | 22.4 | 1.83x | yes | 1 / 1 |
| q07 | 102.6 | 53.6 | 1.91x | yes | 4 / 4 |
| q08 | 91.8 | 65.5 | 1.40x | yes | 2 / 2 |
| q09 | 437.2 | 133.5 | 3.27x | yes | 175 / 175 |
| q10 | 128.5 | 98.9 | 1.30x | yes | 20 / 20 |
| q11 | 36.9 | 21.5 | 1.71x | yes | 1048 / 1048 |
| q12 | 64.9 | 32.3 | 2.01x | yes | 2 / 2 |
| q13 | 79.8 | 92.5 | 0.86x | yes | 42 / 42 |
| q14 | 53.2 | 42.3 | 1.26x | yes | 1 / 1 |
| q15 | 85.3 | 29.1 | 2.93x | yes | 1 / 1 |
| q16 | 163.7 | 67.3 | 2.43x | yes | 18314 / 18314 |
| q17 | 271.9 | 50.6 | 5.37x | yes | 1 / 1 |
| q18 | 306.2 | 101.0 | 3.03x | yes | 57 / 57 |
| q19 | 88.6 | 61.5 | 1.44x | yes | 1 / 1 |
| q20 | 133.6 | 49.9 | 2.68x | yes | 186 / 186 |
| q21 | 127.2 | 166.1 | 0.77x | yes | 100 / 100 |
| q22 | 49.8 | 36.1 | 1.38x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.0 | 15.6 | 1.86x | yes | 4 / 4 |
| q02 | 58.7 | 13.5 | 4.35x | yes | 44 / 44 |
| q03 | 26.2 | 13.9 | 1.88x | yes | 10 / 10 |
| q04 | 14.0 | 12.6 | 1.11x | yes | 5 / 5 |
| q05 | 49.6 | 14.4 | 3.45x | yes | 5 / 5 |
| q06 | 11.1 | 6.0 | 1.85x | yes | 1 / 1 |
| q07 | 63.5 | 16.6 | 3.82x | yes | 4 / 4 |
| q08 | 65.3 | 17.8 | 3.67x | yes | 2 / 2 |
| q09 | 124.7 | 24.7 | 5.04x | yes | 175 / 175 |
| q10 | 40.8 | 19.6 | 2.09x | yes | 20 / 20 |
| q11 | 58.4 | 17.3 | 3.38x | yes | 2541 / 2541 |
| q12 | 20.3 | 10.0 | 2.04x | yes | 2 / 2 |
| q13 | 23.3 | 28.5 | 0.82x | yes | 37 / 37 |
| q14 | 14.6 | 8.6 | 1.71x | yes | 1 / 1 |
| q15 | 35.8 | 6.8 | 5.27x | yes | 1 / 1 |
| q16 | 47.3 | 14.3 | 3.31x | yes | 2762 / 2762 |
| q17 | 27.1 | 12.1 | 2.24x | yes | 1 / 1 |
| q18 | 54.3 | 18.8 | 2.88x | yes | 5 / 5 |
| q19 | 25.2 | 12.0 | 2.10x | yes | 1 / 1 |
| q20 | 36.2 | 13.1 | 2.76x | yes | 9 / 9 |
| q21 | 63.9 | 31.0 | 2.06x | yes | 47 / 47 |
| q22 | 20.0 | 8.6 | 2.31x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 221.2 | 65.6 | 3.37x | yes | 4 / 4 |
| q02 | 84.2 | 25.7 | 3.28x | yes | 100 / 100 |
| q03 | 110.5 | 49.6 | 2.23x | yes | 10 / 10 |
| q04 | 39.2 | 35.2 | 1.11x | yes | 5 / 5 |
| q05 | 183.4 | 50.4 | 3.64x | yes | 5 / 5 |
| q06 | 42.7 | 22.5 | 1.90x | yes | 1 / 1 |
| q07 | 142.2 | 57.4 | 2.48x | yes | 4 / 4 |
| q08 | 143.8 | 63.1 | 2.28x | yes | 2 / 2 |
| q09 | 996.8 | 130.9 | 7.62x | yes | 175 / 175 |
| q10 | 160.4 | 92.6 | 1.73x | yes | 20 / 20 |
| q11 | 55.0 | 19.2 | 2.86x | yes | 1048 / 1048 |
| q12 | 60.6 | 32.3 | 1.88x | yes | 2 / 2 |
| q13 | 121.9 | 100.2 | 1.22x | yes | 42 / 42 |
| q14 | 56.5 | 40.3 | 1.40x | yes | 1 / 1 |
| q15 | 91.8 | 32.3 | 2.85x | yes | 1 / 1 |
| q16 | 166.4 | 61.3 | 2.71x | yes | 18314 / 18314 |
| q17 | 269.3 | 49.2 | 5.48x | yes | 1 / 1 |
| q18 | 590.2 | 102.3 | 5.77x | yes | 57 / 57 |
| q19 | 87.2 | 64.5 | 1.35x | yes | 1 / 1 |
| q20 | 110.7 | 47.7 | 2.32x | yes | 186 / 186 |
| q21 | 187.4 | 168.8 | 1.11x | yes | 100 / 100 |
| q22 | 49.1 | 37.0 | 1.33x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.7 | 6.6 | 3.58x | yes | 4 / 4 |
| q02 | 92.4 | 14.6 | 6.33x | yes | 44 / 44 |
| q03 | 49.5 | 11.0 | 4.51x | yes | 10 / 10 |
| q04 | 27.5 | 9.2 | 3.00x | yes | 5 / 5 |
| q05 | 65.1 | 14.4 | 4.51x | yes | 5 / 5 |
| q06 | 15.6 | 2.0 | 7.83x | yes | 1 / 1 |
| q07 | 85.0 | 17.8 | 4.77x | yes | 4 / 4 |
| q08 | 82.8 | 15.8 | 5.24x | yes | 2 / 2 |
| q09 | 158.1 | 23.5 | 6.73x | yes | 175 / 175 |
| q10 | 74.6 | 19.6 | 3.81x | yes | 20 / 20 |
| q11 | 67.4 | 11.8 | 5.71x | yes | 2541 / 2541 |
| q12 | 22.9 | 6.0 | 3.82x | yes | 2 / 2 |
| q13 | 53.8 | 22.7 | 2.37x | yes | 37 / 37 |
| q14 | 44.6 | 9.8 | 4.57x | yes | 1 / 1 |
| q15 | 48.4 | 4.6 | 10.50x | yes | 1 / 1 |
| q16 | 51.1 | 13.6 | 3.76x | yes | 2762 / 2762 |
| q17 | 50.6 | 10.2 | 4.94x | yes | 1 / 1 |
| q18 | 101.9 | 17.0 | 6.01x | yes | 5 / 5 |
| q19 | 47.8 | 14.0 | 3.42x | yes | 1 / 1 |
| q20 | 60.1 | 11.5 | 5.24x | yes | 9 / 9 |
| q21 | 133.3 | 20.0 | 6.66x | yes | 47 / 47 |
| q22 | 44.6 | 17.7 | 2.52x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.6 | 25.0 | 1.98x | yes | 4 / 4 |
| q02 | 171.6 | 36.2 | 4.74x | yes | 100 / 100 |
| q03 | 141.4 | 37.6 | 3.76x | yes | 10 / 10 |
| q04 | 45.0 | 22.1 | 2.04x | yes | 5 / 5 |
| q05 | 198.2 | 42.7 | 4.64x | yes | 5 / 5 |
| q06 | 27.7 | 7.2 | 3.83x | yes | 1 / 1 |
| q07 | 283.1 | 41.5 | 6.82x | yes | 4 / 4 |
| q08 | 170.8 | 52.0 | 3.28x | yes | 2 / 2 |
| q09 | 440.1 | 90.7 | 4.85x | yes | 175 / 175 |
| q10 | 296.6 | 72.9 | 4.07x | yes | 20 / 20 |
| q11 | 139.1 | 17.7 | 7.86x | yes | 1048 / 1048 |
| q12 | 42.8 | 17.2 | 2.49x | yes | 2 / 2 |
| q13 | 285.5 | 95.8 | 2.98x | yes | 42 / 42 |
| q14 | 120.0 | 40.6 | 2.95x | yes | 1 / 1 |
| q15 | 76.5 | 16.8 | 4.55x | yes | 1 / 1 |
| q16 | 136.4 | 53.4 | 2.56x | yes | 18314 / 18314 |
| q17 | 169.4 | 26.9 | 6.30x | yes | 1 / 1 |
| q18 | 637.1 | 87.7 | 7.26x | yes | 57 / 57 |
| q19 | 89.4 | 59.4 | 1.51x | yes | 1 / 1 |
| q20 | 134.8 | 32.6 | 4.13x | yes | 186 / 186 |
| q21 | 465.3 | 121.9 | 3.82x | yes | 100 / 100 |
| q22 | 104.4 | 62.1 | 1.68x | yes | 7 / 7 |

