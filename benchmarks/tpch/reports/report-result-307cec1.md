# TPC-H benchmark report

Commit: `307cec1` - tests(PP-M5): pin scan narrowness for ORDER BY and derived-table queries
Generated: 2026-07-05 16:47
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
| single | 0.1 | 22/22 | 640.0 | 336.2 | 1.90x | 22 |
| single | 1 | 22/22 | 5379.4 | 1353.4 | 3.97x | 22 |
| fedparquet | 0.1 | 22/22 | 875.2 | 322.7 | 2.71x | 22 |
| fedparquet | 1 | 22/22 | 7201.1 | 1326.6 | 5.43x | 22 |
| fedpgduck | 0.1 | 22/22 | 1764.5 | 279.6 | 6.31x | 22 |
| fedpgduck | 1 | 22/22 | 11922.3 | 1049.7 | 11.36x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Single source (pure engine, Parquet) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 30.3 | 15.7 | 1.92x | yes | 4 / 4 |
| q02 | 24.3 | 13.9 | 1.75x | yes | 44 / 44 |
| q03 | 22.4 | 14.6 | 1.53x | yes | 10 / 10 |
| q04 | 13.7 | 10.1 | 1.35x | yes | 5 / 5 |
| q05 | 23.4 | 13.4 | 1.74x | yes | 5 / 5 |
| q06 | 11.6 | 5.6 | 2.06x | yes | 1 / 1 |
| q07 | 30.3 | 16.5 | 1.84x | yes | 4 / 4 |
| q08 | 30.2 | 17.6 | 1.71x | yes | 2 / 2 |
| q09 | 70.1 | 26.1 | 2.69x | yes | 175 / 175 |
| q10 | 33.8 | 20.8 | 1.62x | yes | 20 / 20 |
| q11 | 32.8 | 18.5 | 1.78x | yes | 2541 / 2541 |
| q12 | 20.7 | 10.2 | 2.03x | yes | 2 / 2 |
| q13 | 24.4 | 25.5 | 0.96x | yes | 37 / 37 |
| q14 | 14.7 | 8.1 | 1.81x | yes | 1 / 1 |
| q15 | 56.2 | 7.1 | 7.95x | yes | 1 / 1 |
| q16 | 33.6 | 14.9 | 2.26x | yes | 2762 / 2762 |
| q17 | 24.2 | 11.8 | 2.04x | yes | 1 / 1 |
| q18 | 45.8 | 19.4 | 2.36x | yes | 5 / 5 |
| q19 | 22.3 | 12.6 | 1.76x | yes | 1 / 1 |
| q20 | 26.8 | 14.9 | 1.79x | yes | 9 / 9 |
| q21 | 30.9 | 29.2 | 1.06x | yes | 47 / 47 |
| q22 | 18.0 | 9.8 | 1.84x | yes | 7 / 7 |

### Single source (pure engine, Parquet) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 213.1 | 65.1 | 3.27x | yes | 4 / 4 |
| q02 | 37.6 | 24.9 | 1.51x | yes | 100 / 100 |
| q03 | 87.2 | 50.8 | 1.72x | yes | 10 / 10 |
| q04 | 38.5 | 34.6 | 1.11x | yes | 5 / 5 |
| q05 | 156.7 | 57.6 | 2.72x | yes | 5 / 5 |
| q06 | 41.7 | 23.3 | 1.79x | yes | 1 / 1 |
| q07 | 96.5 | 57.4 | 1.68x | yes | 4 / 4 |
| q08 | 93.4 | 64.2 | 1.46x | yes | 2 / 2 |
| q09 | 436.7 | 131.5 | 3.32x | yes | 175 / 175 |
| q10 | 121.2 | 95.7 | 1.27x | yes | 20 / 20 |
| q11 | 37.2 | 20.2 | 1.84x | yes | 1048 / 1048 |
| q12 | 58.4 | 31.4 | 1.86x | yes | 2 / 2 |
| q13 | 81.5 | 88.1 | 0.92x | yes | 42 / 42 |
| q14 | 55.1 | 47.7 | 1.15x | yes | 1 / 1 |
| q15 | 2667.7 | 29.6 | 90.08x | yes | 1 / 1 |
| q16 | 155.8 | 60.2 | 2.59x | yes | 18314 / 18314 |
| q17 | 275.3 | 49.1 | 5.61x | yes | 1 / 1 |
| q18 | 339.7 | 100.6 | 3.38x | yes | 57 / 57 |
| q19 | 82.8 | 62.1 | 1.33x | yes | 1 / 1 |
| q20 | 133.2 | 49.3 | 2.70x | yes | 186 / 186 |
| q21 | 127.3 | 170.7 | 0.75x | yes | 100 / 100 |
| q22 | 43.0 | 39.2 | 1.10x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 22.4 | 15.8 | 1.42x | yes | 4 / 4 |
| q02 | 40.3 | 12.2 | 3.30x | yes | 44 / 44 |
| q03 | 28.8 | 12.5 | 2.31x | yes | 10 / 10 |
| q04 | 14.3 | 10.8 | 1.32x | yes | 5 / 5 |
| q05 | 42.0 | 12.7 | 3.30x | yes | 5 / 5 |
| q06 | 12.8 | 6.3 | 2.02x | yes | 1 / 1 |
| q07 | 54.5 | 15.1 | 3.60x | yes | 4 / 4 |
| q08 | 45.9 | 17.4 | 2.64x | yes | 2 / 2 |
| q09 | 87.9 | 24.7 | 3.56x | yes | 175 / 175 |
| q10 | 42.8 | 18.4 | 2.32x | yes | 20 / 20 |
| q11 | 36.2 | 17.9 | 2.02x | yes | 2541 / 2541 |
| q12 | 17.6 | 9.8 | 1.79x | yes | 2 / 2 |
| q13 | 42.4 | 25.4 | 1.67x | yes | 37 / 37 |
| q14 | 15.5 | 8.9 | 1.74x | yes | 1 / 1 |
| q15 | 55.3 | 7.0 | 7.94x | yes | 1 / 1 |
| q16 | 38.4 | 13.1 | 2.93x | yes | 2762 / 2762 |
| q17 | 31.3 | 12.1 | 2.58x | yes | 1 / 1 |
| q18 | 51.8 | 19.0 | 2.72x | yes | 5 / 5 |
| q19 | 24.1 | 11.4 | 2.10x | yes | 1 / 1 |
| q20 | 26.5 | 13.8 | 1.92x | yes | 9 / 9 |
| q21 | 113.6 | 28.4 | 4.00x | yes | 47 / 47 |
| q22 | 30.9 | 9.8 | 3.17x | yes | 7 / 7 |

### Federated, 2 Parquet sources (DuckDB monolithic - unfair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 215.4 | 64.2 | 3.36x | yes | 4 / 4 |
| q02 | 75.4 | 23.0 | 3.28x | yes | 100 / 100 |
| q03 | 113.6 | 48.6 | 2.34x | yes | 10 / 10 |
| q04 | 37.8 | 33.5 | 1.13x | yes | 5 / 5 |
| q05 | 196.6 | 55.2 | 3.56x | yes | 5 / 5 |
| q06 | 40.9 | 22.0 | 1.86x | yes | 1 / 1 |
| q07 | 345.0 | 54.6 | 6.32x | yes | 4 / 4 |
| q08 | 120.3 | 63.3 | 1.90x | yes | 2 / 2 |
| q09 | 682.3 | 136.0 | 5.02x | yes | 175 / 175 |
| q10 | 166.1 | 87.6 | 1.90x | yes | 20 / 20 |
| q11 | 46.4 | 18.4 | 2.52x | yes | 1048 / 1048 |
| q12 | 62.3 | 33.5 | 1.86x | yes | 2 / 2 |
| q13 | 310.1 | 90.7 | 3.42x | yes | 42 / 42 |
| q14 | 52.7 | 40.6 | 1.30x | yes | 1 / 1 |
| q15 | 2624.3 | 28.9 | 90.70x | yes | 1 / 1 |
| q16 | 176.1 | 61.0 | 2.89x | yes | 18314 / 18314 |
| q17 | 274.9 | 49.2 | 5.58x | yes | 1 / 1 |
| q18 | 558.6 | 107.4 | 5.20x | yes | 57 / 57 |
| q19 | 98.3 | 59.9 | 1.64x | yes | 1 / 1 |
| q20 | 109.1 | 46.9 | 2.33x | yes | 186 / 186 |
| q21 | 791.7 | 165.9 | 4.77x | yes | 100 / 100 |
| q22 | 103.2 | 36.4 | 2.84x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.0 | 6.4 | 3.57x | yes | 4 / 4 |
| q02 | 71.2 | 11.6 | 6.17x | yes | 44 / 44 |
| q03 | 63.2 | 9.5 | 6.68x | yes | 10 / 10 |
| q04 | 25.4 | 7.7 | 3.28x | yes | 5 / 5 |
| q05 | 83.6 | 13.0 | 6.44x | yes | 5 / 5 |
| q06 | 15.4 | 2.1 | 7.22x | yes | 1 / 1 |
| q07 | 90.3 | 13.8 | 6.55x | yes | 4 / 4 |
| q08 | 78.0 | 15.9 | 4.91x | yes | 2 / 2 |
| q09 | 124.7 | 23.8 | 5.25x | yes | 175 / 175 |
| q10 | 82.6 | 18.9 | 4.38x | yes | 20 / 20 |
| q11 | 56.9 | 11.7 | 4.85x | yes | 2541 / 2541 |
| q12 | 23.0 | 6.0 | 3.83x | yes | 2 / 2 |
| q13 | 110.0 | 21.2 | 5.19x | yes | 37 / 37 |
| q14 | 31.5 | 8.6 | 3.65x | yes | 1 / 1 |
| q15 | 119.0 | 6.8 | 17.51x | yes | 1 / 1 |
| q16 | 42.4 | 14.2 | 3.00x | yes | 2762 / 2762 |
| q17 | 56.0 | 9.1 | 6.13x | yes | 1 / 1 |
| q18 | 103.8 | 15.9 | 6.52x | yes | 5 / 5 |
| q19 | 41.4 | 13.3 | 3.12x | yes | 1 / 1 |
| q20 | 55.8 | 11.4 | 4.88x | yes | 9 / 9 |
| q21 | 378.6 | 21.4 | 17.69x | yes | 47 / 47 |
| q22 | 88.8 | 17.3 | 5.13x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 48.7 | 25.2 | 1.93x | yes | 4 / 4 |
| q02 | 140.8 | 31.4 | 4.49x | yes | 100 / 100 |
| q03 | 249.5 | 32.6 | 7.65x | yes | 10 / 10 |
| q04 | 45.1 | 22.9 | 1.97x | yes | 5 / 5 |
| q05 | 427.4 | 44.0 | 9.70x | yes | 5 / 5 |
| q06 | 27.2 | 7.3 | 3.71x | yes | 1 / 1 |
| q07 | 475.8 | 43.9 | 10.83x | yes | 4 / 4 |
| q08 | 158.0 | 50.2 | 3.15x | yes | 2 / 2 |
| q09 | 961.9 | 100.0 | 9.62x | yes | 175 / 175 |
| q10 | 373.6 | 66.4 | 5.63x | yes | 20 / 20 |
| q11 | 106.9 | 17.7 | 6.05x | yes | 1048 / 1048 |
| q12 | 41.9 | 17.4 | 2.41x | yes | 2 / 2 |
| q13 | 760.9 | 92.1 | 8.26x | yes | 42 / 42 |
| q14 | 101.6 | 38.6 | 2.63x | yes | 1 / 1 |
| q15 | 2823.6 | 16.8 | 168.52x | yes | 1 / 1 |
| q16 | 131.6 | 55.1 | 2.39x | yes | 18314 / 18314 |
| q17 | 391.3 | 26.2 | 14.92x | yes | 1 / 1 |
| q18 | 771.2 | 88.5 | 8.72x | yes | 57 / 57 |
| q19 | 129.4 | 62.6 | 2.07x | yes | 1 / 1 |
| q20 | 123.9 | 33.4 | 3.71x | yes | 186 / 186 |
| q21 | 3201.1 | 120.3 | 26.62x | yes | 100 / 100 |
| q22 | 430.8 | 57.1 | 7.54x | yes | 7 / 7 |

