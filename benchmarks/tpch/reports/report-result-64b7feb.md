# TPC-H benchmark report

Commit: `64b7feb` - optimizer(RC-M2): size a remote by its max base scan, not join output
Generated: 2026-07-06 04:52
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
| single | 0.1 | 22/22 | 629.1 | 335.0 | 1.88x | 22 |
| single | 1 | 22/22 | 2633.8 | 1345.3 | 1.96x | 22 |
| fedparquet | 0.1 | 22/22 | 857.1 | 341.8 | 2.51x | 22 |
| fedparquet | 1 | 22/22 | 3334.9 | 1353.4 | 2.46x | 22 |
| fedpgduck | 0.1 | 22/22 | 1274.8 | 294.4 | 4.33x | 22 |
| fedpgduck | 1 | 22/22 | 3280.0 | 1063.6 | 3.08x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.4 | 16.4 | 1.92x | yes | 4 / 4 |
| q02 | 26.3 | 11.8 | 2.23x | yes | 44 / 44 |
| q03 | 20.2 | 13.9 | 1.46x | yes | 10 / 10 |
| q04 | 14.6 | 11.9 | 1.22x | yes | 5 / 5 |
| q05 | 22.5 | 15.5 | 1.45x | yes | 5 / 5 |
| q06 | 11.0 | 5.6 | 1.94x | yes | 1 / 1 |
| q07 | 29.9 | 16.5 | 1.81x | yes | 4 / 4 |
| q08 | 27.3 | 18.9 | 1.44x | yes | 2 / 2 |
| q09 | 69.8 | 26.4 | 2.64x | yes | 175 / 175 |
| q10 | 31.3 | 20.2 | 1.55x | yes | 20 / 20 |
| q11 | 34.8 | 17.8 | 1.96x | yes | 2541 / 2541 |
| q12 | 20.6 | 10.3 | 2.00x | yes | 2 / 2 |
| q13 | 24.7 | 25.1 | 0.98x | yes | 37 / 37 |
| q14 | 14.7 | 8.6 | 1.70x | yes | 1 / 1 |
| q15 | 37.5 | 7.3 | 5.17x | yes | 1 / 1 |
| q16 | 36.9 | 13.8 | 2.67x | yes | 2762 / 2762 |
| q17 | 22.4 | 10.9 | 2.06x | yes | 1 / 1 |
| q18 | 30.8 | 19.6 | 1.57x | yes | 5 / 5 |
| q19 | 29.2 | 12.0 | 2.43x | yes | 1 / 1 |
| q20 | 26.8 | 11.8 | 2.28x | yes | 9 / 9 |
| q21 | 46.2 | 31.3 | 1.48x | yes | 47 / 47 |
| q22 | 20.1 | 9.2 | 2.17x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 217.3 | 64.9 | 3.35x | yes | 4 / 4 |
| q02 | 38.3 | 25.0 | 1.53x | yes | 100 / 100 |
| q03 | 85.9 | 48.3 | 1.78x | yes | 10 / 10 |
| q04 | 39.1 | 35.6 | 1.10x | yes | 5 / 5 |
| q05 | 149.4 | 52.7 | 2.83x | yes | 5 / 5 |
| q06 | 40.4 | 23.9 | 1.69x | yes | 1 / 1 |
| q07 | 105.4 | 55.8 | 1.89x | yes | 4 / 4 |
| q08 | 88.2 | 65.6 | 1.34x | yes | 2 / 2 |
| q09 | 442.0 | 136.4 | 3.24x | yes | 175 / 175 |
| q10 | 125.8 | 97.1 | 1.30x | yes | 20 / 20 |
| q11 | 35.2 | 18.0 | 1.95x | yes | 1048 / 1048 |
| q12 | 63.7 | 32.5 | 1.96x | yes | 2 / 2 |
| q13 | 79.3 | 90.3 | 0.88x | yes | 42 / 42 |
| q14 | 51.2 | 41.2 | 1.24x | yes | 1 / 1 |
| q15 | 85.8 | 29.5 | 2.91x | yes | 1 / 1 |
| q16 | 153.9 | 63.8 | 2.41x | yes | 18314 / 18314 |
| q17 | 277.7 | 50.4 | 5.51x | yes | 1 / 1 |
| q18 | 141.0 | 103.1 | 1.37x | yes | 57 / 57 |
| q19 | 90.3 | 61.3 | 1.47x | yes | 1 / 1 |
| q20 | 133.0 | 48.8 | 2.72x | yes | 186 / 186 |
| q21 | 142.9 | 164.2 | 0.87x | yes | 100 / 100 |
| q22 | 48.0 | 36.9 | 1.30x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.0 | 15.6 | 1.47x | yes | 4 / 4 |
| q02 | 50.5 | 15.9 | 3.17x | yes | 44 / 44 |
| q03 | 27.4 | 16.1 | 1.70x | yes | 10 / 10 |
| q04 | 14.2 | 11.5 | 1.24x | yes | 5 / 5 |
| q05 | 53.5 | 17.5 | 3.05x | yes | 5 / 5 |
| q06 | 12.2 | 5.8 | 2.10x | yes | 1 / 1 |
| q07 | 62.7 | 16.3 | 3.85x | yes | 4 / 4 |
| q08 | 66.7 | 19.7 | 3.39x | yes | 2 / 2 |
| q09 | 129.8 | 25.8 | 5.04x | yes | 175 / 175 |
| q10 | 42.3 | 18.3 | 2.30x | yes | 20 / 20 |
| q11 | 48.3 | 20.2 | 2.39x | yes | 2541 / 2541 |
| q12 | 21.9 | 10.7 | 2.05x | yes | 2 / 2 |
| q13 | 23.5 | 24.6 | 0.95x | yes | 37 / 37 |
| q14 | 14.5 | 8.1 | 1.80x | yes | 1 / 1 |
| q15 | 36.0 | 6.7 | 5.38x | yes | 1 / 1 |
| q16 | 38.2 | 13.4 | 2.85x | yes | 2762 / 2762 |
| q17 | 25.9 | 12.0 | 2.15x | yes | 1 / 1 |
| q18 | 38.9 | 18.4 | 2.12x | yes | 5 / 5 |
| q19 | 26.7 | 13.0 | 2.06x | yes | 1 / 1 |
| q20 | 36.0 | 13.4 | 2.68x | yes | 9 / 9 |
| q21 | 46.5 | 29.3 | 1.59x | yes | 47 / 47 |
| q22 | 18.6 | 9.5 | 1.96x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 229.4 | 68.2 | 3.36x | yes | 4 / 4 |
| q02 | 97.1 | 24.6 | 3.95x | yes | 100 / 100 |
| q03 | 110.2 | 48.7 | 2.26x | yes | 10 / 10 |
| q04 | 37.7 | 36.0 | 1.05x | yes | 5 / 5 |
| q05 | 190.6 | 52.6 | 3.62x | yes | 5 / 5 |
| q06 | 40.8 | 21.9 | 1.86x | yes | 1 / 1 |
| q07 | 126.2 | 55.6 | 2.27x | yes | 4 / 4 |
| q08 | 147.2 | 65.2 | 2.26x | yes | 2 / 2 |
| q09 | 1006.0 | 139.8 | 7.20x | yes | 175 / 175 |
| q10 | 165.6 | 91.0 | 1.82x | yes | 20 / 20 |
| q11 | 55.6 | 18.7 | 2.97x | yes | 1048 / 1048 |
| q12 | 63.9 | 32.1 | 1.99x | yes | 2 / 2 |
| q13 | 128.2 | 97.6 | 1.31x | yes | 42 / 42 |
| q14 | 58.6 | 44.9 | 1.31x | yes | 1 / 1 |
| q15 | 83.7 | 29.3 | 2.86x | yes | 1 / 1 |
| q16 | 169.4 | 61.3 | 2.76x | yes | 18314 / 18314 |
| q17 | 76.6 | 49.2 | 1.56x | yes | 1 / 1 |
| q18 | 145.2 | 102.8 | 1.41x | yes | 57 / 57 |
| q19 | 100.0 | 61.0 | 1.64x | yes | 1 / 1 |
| q20 | 113.5 | 47.8 | 2.37x | yes | 186 / 186 |
| q21 | 141.6 | 167.0 | 0.85x | yes | 100 / 100 |
| q22 | 47.9 | 37.9 | 1.27x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.5 | 6.5 | 3.61x | yes | 4 / 4 |
| q02 | 77.3 | 12.0 | 6.46x | yes | 44 / 44 |
| q03 | 44.8 | 9.0 | 4.97x | yes | 10 / 10 |
| q04 | 23.9 | 7.6 | 3.15x | yes | 5 / 5 |
| q05 | 62.8 | 12.7 | 4.95x | yes | 5 / 5 |
| q06 | 16.2 | 2.5 | 6.42x | yes | 1 / 1 |
| q07 | 86.7 | 13.8 | 6.30x | yes | 4 / 4 |
| q08 | 82.4 | 15.5 | 5.31x | yes | 2 / 2 |
| q09 | 161.0 | 25.8 | 6.25x | yes | 175 / 175 |
| q10 | 72.0 | 22.5 | 3.20x | yes | 20 / 20 |
| q11 | 64.6 | 16.3 | 3.95x | yes | 2541 / 2541 |
| q12 | 23.5 | 5.9 | 4.01x | yes | 2 / 2 |
| q13 | 51.5 | 21.8 | 2.36x | yes | 37 / 37 |
| q14 | 47.4 | 9.7 | 4.90x | yes | 1 / 1 |
| q15 | 46.9 | 4.9 | 9.65x | yes | 1 / 1 |
| q16 | 43.9 | 13.4 | 3.27x | yes | 2762 / 2762 |
| q17 | 44.1 | 9.0 | 4.92x | yes | 1 / 1 |
| q18 | 69.9 | 21.5 | 3.25x | yes | 5 / 5 |
| q19 | 44.6 | 14.3 | 3.11x | yes | 1 / 1 |
| q20 | 66.2 | 13.3 | 4.97x | yes | 9 / 9 |
| q21 | 78.2 | 20.2 | 3.87x | yes | 47 / 47 |
| q22 | 43.6 | 16.3 | 2.67x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 49.0 | 24.5 | 2.00x | yes | 4 / 4 |
| q02 | 132.5 | 31.8 | 4.16x | yes | 100 / 100 |
| q03 | 124.3 | 32.7 | 3.80x | yes | 10 / 10 |
| q04 | 50.0 | 22.1 | 2.27x | yes | 5 / 5 |
| q05 | 203.0 | 44.6 | 4.56x | yes | 5 / 5 |
| q06 | 31.0 | 8.0 | 3.89x | yes | 1 / 1 |
| q07 | 288.8 | 44.0 | 6.57x | yes | 4 / 4 |
| q08 | 172.7 | 54.0 | 3.20x | yes | 2 / 2 |
| q09 | 424.3 | 93.3 | 4.55x | yes | 175 / 175 |
| q10 | 264.1 | 65.1 | 4.05x | yes | 20 / 20 |
| q11 | 75.4 | 16.6 | 4.54x | yes | 1048 / 1048 |
| q12 | 44.0 | 16.4 | 2.68x | yes | 2 / 2 |
| q13 | 288.0 | 92.5 | 3.11x | yes | 42 / 42 |
| q14 | 131.2 | 40.3 | 3.26x | yes | 1 / 1 |
| q15 | 80.4 | 19.3 | 4.16x | yes | 1 / 1 |
| q16 | 115.0 | 56.9 | 2.02x | yes | 18314 / 18314 |
| q17 | 72.8 | 25.9 | 2.81x | yes | 1 / 1 |
| q18 | 225.6 | 99.2 | 2.27x | yes | 57 / 57 |
| q19 | 92.1 | 64.0 | 1.44x | yes | 1 / 1 |
| q20 | 133.4 | 36.3 | 3.67x | yes | 186 / 186 |
| q21 | 174.6 | 122.0 | 1.43x | yes | 100 / 100 |
| q22 | 107.7 | 54.1 | 1.99x | yes | 7 / 7 |

