# TPC-H benchmark report

Commit: `3006554` - benchmarks: SEMI/ANTI-estimate gate (report-result-f1e4921) - 3.26x fair
Generated: 2026-07-06 00:56
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
| fedpgduck | 0.1 | 22/22 | 1339.2 | 298.2 | 4.49x | 22 |
| fedpgduck | 1 | 22/22 | 3417.1 | 1057.9 | 3.23x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 23.5 | 6.7 | 3.49x | yes | 4 / 4 |
| q02 | 91.1 | 12.2 | 7.44x | yes | 44 / 44 |
| q03 | 50.0 | 10.8 | 4.61x | yes | 10 / 10 |
| q04 | 25.5 | 8.8 | 2.89x | yes | 5 / 5 |
| q05 | 75.3 | 13.1 | 5.76x | yes | 5 / 5 |
| q06 | 25.6 | 2.7 | 9.61x | yes | 1 / 1 |
| q07 | 90.0 | 13.9 | 6.46x | yes | 4 / 4 |
| q08 | 90.3 | 18.1 | 5.00x | yes | 2 / 2 |
| q09 | 157.6 | 23.9 | 6.59x | yes | 175 / 175 |
| q10 | 69.7 | 29.3 | 2.38x | yes | 20 / 20 |
| q11 | 66.9 | 13.6 | 4.93x | yes | 2541 / 2541 |
| q12 | 23.3 | 6.0 | 3.91x | yes | 2 / 2 |
| q13 | 52.9 | 26.0 | 2.03x | yes | 37 / 37 |
| q14 | 45.4 | 8.9 | 5.11x | yes | 1 / 1 |
| q15 | 48.2 | 4.6 | 10.52x | yes | 1 / 1 |
| q16 | 43.3 | 13.5 | 3.22x | yes | 2762 / 2762 |
| q17 | 49.9 | 9.5 | 5.24x | yes | 1 / 1 |
| q18 | 72.8 | 16.5 | 4.42x | yes | 5 / 5 |
| q19 | 48.2 | 13.4 | 3.59x | yes | 1 / 1 |
| q20 | 61.4 | 11.5 | 5.33x | yes | 9 / 9 |
| q21 | 81.4 | 19.3 | 4.22x | yes | 47 / 47 |
| q22 | 46.6 | 15.9 | 2.94x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 48.5 | 25.7 | 1.89x | yes | 4 / 4 |
| q02 | 156.5 | 32.6 | 4.80x | yes | 100 / 100 |
| q03 | 125.6 | 34.8 | 3.61x | yes | 10 / 10 |
| q04 | 46.7 | 22.0 | 2.13x | yes | 5 / 5 |
| q05 | 192.9 | 40.1 | 4.81x | yes | 5 / 5 |
| q06 | 28.9 | 7.5 | 3.88x | yes | 1 / 1 |
| q07 | 293.8 | 49.6 | 5.93x | yes | 4 / 4 |
| q08 | 171.3 | 49.6 | 3.45x | yes | 2 / 2 |
| q09 | 437.6 | 90.8 | 4.82x | yes | 175 / 175 |
| q10 | 260.5 | 66.2 | 3.94x | yes | 20 / 20 |
| q11 | 139.7 | 20.2 | 6.90x | yes | 1048 / 1048 |
| q12 | 41.8 | 17.3 | 2.42x | yes | 2 / 2 |
| q13 | 293.8 | 99.1 | 2.96x | yes | 42 / 42 |
| q14 | 102.9 | 40.1 | 2.57x | yes | 1 / 1 |
| q15 | 77.9 | 20.9 | 3.72x | yes | 1 / 1 |
| q16 | 117.1 | 54.4 | 2.15x | yes | 18314 / 18314 |
| q17 | 170.9 | 25.8 | 6.62x | yes | 1 / 1 |
| q18 | 206.4 | 86.6 | 2.38x | yes | 57 / 57 |
| q19 | 90.4 | 58.8 | 1.54x | yes | 1 / 1 |
| q20 | 132.5 | 33.9 | 3.91x | yes | 186 / 186 |
| q21 | 174.4 | 129.8 | 1.34x | yes | 100 / 100 |
| q22 | 107.2 | 52.3 | 2.05x | yes | 7 / 7 |

