# TPC-H benchmark report

Commit: `c0e8d43` - executor: reduce LEFT-join aggregate subquery probes (q17 6.6x -> 2.8x)
Generated: 2026-07-06 02:35
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
| single | 0.1 | 22/22 | 628.9 | 339.6 | 1.85x | 22 |
| single | 1 | 22/22 | 2686.7 | 1363.3 | 1.97x | 22 |
| fedparquet | 0.1 | 22/22 | 907.4 | 331.5 | 2.74x | 22 |
| fedparquet | 1 | 22/22 | 3321.5 | 1344.3 | 2.47x | 22 |
| fedpgduck | 0.1 | 22/22 | 1295.0 | 293.8 | 4.41x | 22 |
| fedpgduck | 1 | 22/22 | 3392.3 | 1073.9 | 3.16x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.5 | 16.2 | 1.82x | yes | 4 / 4 |
| q02 | 25.5 | 14.1 | 1.80x | yes | 44 / 44 |
| q03 | 21.8 | 14.5 | 1.51x | yes | 10 / 10 |
| q04 | 14.7 | 12.0 | 1.23x | yes | 5 / 5 |
| q05 | 23.0 | 14.1 | 1.63x | yes | 5 / 5 |
| q06 | 12.5 | 5.8 | 2.17x | yes | 1 / 1 |
| q07 | 32.2 | 15.8 | 2.04x | yes | 4 / 4 |
| q08 | 30.1 | 18.0 | 1.67x | yes | 2 / 2 |
| q09 | 67.4 | 25.5 | 2.64x | yes | 175 / 175 |
| q10 | 31.1 | 22.2 | 1.40x | yes | 20 / 20 |
| q11 | 34.5 | 19.0 | 1.82x | yes | 2541 / 2541 |
| q12 | 20.2 | 9.2 | 2.19x | yes | 2 / 2 |
| q13 | 26.0 | 25.9 | 1.00x | yes | 37 / 37 |
| q14 | 14.7 | 8.4 | 1.75x | yes | 1 / 1 |
| q15 | 36.3 | 7.2 | 5.06x | yes | 1 / 1 |
| q16 | 32.8 | 13.8 | 2.37x | yes | 2762 / 2762 |
| q17 | 24.6 | 12.1 | 2.03x | yes | 1 / 1 |
| q18 | 31.6 | 20.0 | 1.58x | yes | 5 / 5 |
| q19 | 28.9 | 12.3 | 2.35x | yes | 1 / 1 |
| q20 | 27.2 | 13.0 | 2.08x | yes | 9 / 9 |
| q21 | 45.4 | 30.7 | 1.48x | yes | 47 / 47 |
| q22 | 19.0 | 9.7 | 1.96x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 217.3 | 65.5 | 3.32x | yes | 4 / 4 |
| q02 | 37.9 | 23.7 | 1.60x | yes | 100 / 100 |
| q03 | 88.8 | 50.1 | 1.77x | yes | 10 / 10 |
| q04 | 38.0 | 35.6 | 1.07x | yes | 5 / 5 |
| q05 | 144.9 | 54.1 | 2.68x | yes | 5 / 5 |
| q06 | 42.9 | 22.6 | 1.90x | yes | 1 / 1 |
| q07 | 107.6 | 55.4 | 1.94x | yes | 4 / 4 |
| q08 | 102.7 | 69.7 | 1.48x | yes | 2 / 2 |
| q09 | 439.0 | 134.8 | 3.26x | yes | 175 / 175 |
| q10 | 138.9 | 100.0 | 1.39x | yes | 20 / 20 |
| q11 | 35.5 | 18.6 | 1.91x | yes | 1048 / 1048 |
| q12 | 62.9 | 31.9 | 1.97x | yes | 2 / 2 |
| q13 | 80.2 | 98.9 | 0.81x | yes | 42 / 42 |
| q14 | 52.5 | 41.1 | 1.28x | yes | 1 / 1 |
| q15 | 85.8 | 30.1 | 2.85x | yes | 1 / 1 |
| q16 | 173.0 | 62.8 | 2.75x | yes | 18314 / 18314 |
| q17 | 276.5 | 53.7 | 5.15x | yes | 1 / 1 |
| q18 | 147.5 | 101.3 | 1.46x | yes | 57 / 57 |
| q19 | 94.9 | 60.7 | 1.56x | yes | 1 / 1 |
| q20 | 127.5 | 47.9 | 2.66x | yes | 186 / 186 |
| q21 | 142.0 | 168.6 | 0.84x | yes | 100 / 100 |
| q22 | 50.3 | 36.1 | 1.39x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.9 | 14.7 | 2.04x | yes | 4 / 4 |
| q02 | 60.6 | 12.7 | 4.78x | yes | 44 / 44 |
| q03 | 30.1 | 13.6 | 2.21x | yes | 10 / 10 |
| q04 | 15.0 | 11.9 | 1.26x | yes | 5 / 5 |
| q05 | 47.8 | 14.7 | 3.24x | yes | 5 / 5 |
| q06 | 12.7 | 6.5 | 1.97x | yes | 1 / 1 |
| q07 | 62.6 | 15.2 | 4.12x | yes | 4 / 4 |
| q08 | 72.8 | 17.3 | 4.20x | yes | 2 / 2 |
| q09 | 140.6 | 25.7 | 5.46x | yes | 175 / 175 |
| q10 | 45.5 | 23.0 | 1.98x | yes | 20 / 20 |
| q11 | 64.1 | 18.3 | 3.49x | yes | 2541 / 2541 |
| q12 | 19.2 | 10.2 | 1.88x | yes | 2 / 2 |
| q13 | 23.1 | 26.9 | 0.86x | yes | 37 / 37 |
| q14 | 16.1 | 8.3 | 1.94x | yes | 1 / 1 |
| q15 | 36.4 | 6.6 | 5.51x | yes | 1 / 1 |
| q16 | 37.3 | 13.5 | 2.76x | yes | 2762 / 2762 |
| q17 | 24.8 | 10.5 | 2.36x | yes | 1 / 1 |
| q18 | 38.7 | 17.0 | 2.28x | yes | 5 / 5 |
| q19 | 29.0 | 13.0 | 2.23x | yes | 1 / 1 |
| q20 | 35.4 | 13.2 | 2.68x | yes | 9 / 9 |
| q21 | 46.4 | 29.7 | 1.56x | yes | 47 / 47 |
| q22 | 19.3 | 8.8 | 2.19x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 220.7 | 67.6 | 3.26x | yes | 4 / 4 |
| q02 | 84.8 | 25.8 | 3.29x | yes | 100 / 100 |
| q03 | 108.2 | 47.5 | 2.28x | yes | 10 / 10 |
| q04 | 39.2 | 34.3 | 1.14x | yes | 5 / 5 |
| q05 | 184.8 | 50.5 | 3.66x | yes | 5 / 5 |
| q06 | 42.1 | 22.3 | 1.89x | yes | 1 / 1 |
| q07 | 133.9 | 55.7 | 2.40x | yes | 4 / 4 |
| q08 | 146.0 | 65.1 | 2.24x | yes | 2 / 2 |
| q09 | 1002.8 | 136.5 | 7.35x | yes | 175 / 175 |
| q10 | 158.4 | 89.3 | 1.77x | yes | 20 / 20 |
| q11 | 53.5 | 19.0 | 2.81x | yes | 1048 / 1048 |
| q12 | 62.4 | 33.4 | 1.87x | yes | 2 / 2 |
| q13 | 128.2 | 97.6 | 1.31x | yes | 42 / 42 |
| q14 | 54.4 | 40.7 | 1.34x | yes | 1 / 1 |
| q15 | 87.4 | 28.8 | 3.03x | yes | 1 / 1 |
| q16 | 194.5 | 64.9 | 3.00x | yes | 18314 / 18314 |
| q17 | 81.0 | 48.6 | 1.67x | yes | 1 / 1 |
| q18 | 140.9 | 99.5 | 1.42x | yes | 57 / 57 |
| q19 | 91.6 | 64.2 | 1.43x | yes | 1 / 1 |
| q20 | 114.1 | 50.4 | 2.26x | yes | 186 / 186 |
| q21 | 141.7 | 166.5 | 0.85x | yes | 100 / 100 |
| q22 | 50.9 | 36.0 | 1.41x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.4 | 6.1 | 3.87x | yes | 4 / 4 |
| q02 | 80.6 | 11.5 | 6.99x | yes | 44 / 44 |
| q03 | 47.0 | 9.7 | 4.86x | yes | 10 / 10 |
| q04 | 25.6 | 8.3 | 3.10x | yes | 5 / 5 |
| q05 | 68.2 | 13.2 | 5.16x | yes | 5 / 5 |
| q06 | 16.7 | 2.0 | 8.42x | yes | 1 / 1 |
| q07 | 88.1 | 17.9 | 4.92x | yes | 4 / 4 |
| q08 | 83.3 | 16.7 | 4.97x | yes | 2 / 2 |
| q09 | 158.8 | 25.0 | 6.36x | yes | 175 / 175 |
| q10 | 71.9 | 21.1 | 3.41x | yes | 20 / 20 |
| q11 | 69.1 | 11.3 | 6.09x | yes | 2541 / 2541 |
| q12 | 24.0 | 5.6 | 4.28x | yes | 2 / 2 |
| q13 | 51.8 | 27.9 | 1.85x | yes | 37 / 37 |
| q14 | 42.7 | 9.3 | 4.59x | yes | 1 / 1 |
| q15 | 45.9 | 4.4 | 10.47x | yes | 1 / 1 |
| q16 | 47.5 | 16.3 | 2.92x | yes | 2762 / 2762 |
| q17 | 44.2 | 8.9 | 4.96x | yes | 1 / 1 |
| q18 | 75.5 | 17.2 | 4.39x | yes | 5 / 5 |
| q19 | 47.8 | 13.8 | 3.47x | yes | 1 / 1 |
| q20 | 59.6 | 11.3 | 5.30x | yes | 9 / 9 |
| q21 | 77.7 | 20.1 | 3.87x | yes | 47 / 47 |
| q22 | 45.5 | 16.3 | 2.79x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.2 | 26.0 | 1.90x | yes | 4 / 4 |
| q02 | 158.7 | 32.8 | 4.84x | yes | 100 / 100 |
| q03 | 137.4 | 37.6 | 3.65x | yes | 10 / 10 |
| q04 | 47.4 | 21.9 | 2.16x | yes | 5 / 5 |
| q05 | 195.1 | 42.1 | 4.63x | yes | 5 / 5 |
| q06 | 28.8 | 6.9 | 4.18x | yes | 1 / 1 |
| q07 | 289.1 | 43.8 | 6.61x | yes | 4 / 4 |
| q08 | 170.5 | 50.1 | 3.40x | yes | 2 / 2 |
| q09 | 466.9 | 91.8 | 5.09x | yes | 175 / 175 |
| q10 | 263.5 | 72.3 | 3.65x | yes | 20 / 20 |
| q11 | 142.1 | 15.4 | 9.20x | yes | 1048 / 1048 |
| q12 | 41.9 | 16.8 | 2.50x | yes | 2 / 2 |
| q13 | 282.2 | 98.8 | 2.86x | yes | 42 / 42 |
| q14 | 107.6 | 40.2 | 2.68x | yes | 1 / 1 |
| q15 | 75.6 | 16.9 | 4.46x | yes | 1 / 1 |
| q16 | 116.7 | 52.5 | 2.22x | yes | 18314 / 18314 |
| q17 | 72.8 | 27.2 | 2.68x | yes | 1 / 1 |
| q18 | 238.2 | 94.7 | 2.51x | yes | 57 / 57 |
| q19 | 89.7 | 60.8 | 1.47x | yes | 1 / 1 |
| q20 | 131.0 | 33.3 | 3.93x | yes | 186 / 186 |
| q21 | 176.7 | 138.8 | 1.27x | yes | 100 / 100 |
| q22 | 111.2 | 53.1 | 2.09x | yes | 7 / 7 |

