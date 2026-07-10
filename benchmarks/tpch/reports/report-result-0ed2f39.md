# TPC-H benchmark report

Commit: `0ed2f39` - plan: parallel reads (cross-step scheduler; investigation + phased design)
Generated: 2026-07-10 03:49
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.
Scale factors: SF1 (6M lineitem).

## Methodology

Timing is the median of three warm runs per query; correctness is a differential row-by-row check against DuckDB on the same data (numbers rounded to 2 decimals). Each query runs in its own subprocess with an RSS cap, so a blow-up is a KILLED row, not a lost run. Ratio is ours/DuckDB (higher = we are slower).

- **single** - Single source (pure engine, Parquet).
- **fedparquet** - Federated, 2 Parquet sources (DuckDB monolithic - unfair); DuckDB reads all files as one, so this overstates our cost.
- **fedpgduck** - Federated, PostgreSQL + DuckDB (both federate - fair); the DuckDB oracle federates the same split via its postgres connector. This is the honest federated number.

## Summary

Totals cover only queries that produced a measurement; TIMEOUT / KILLED / ERROR queries are excluded from the ms totals but counted as not-correct. `measured` is how many of the 22 timed cleanly.

| Cell | SF | Correct | Ours (ms) | DuckDB (ms) | Ratio | Measured |
| --- | --- | --- | --- | --- | --- | --- |
| fedpgduck | 1 | 22/22 | 1701.2 | 1095.0 | 1.55x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 35.7 | 25.8 | 1.38x | yes | 4 / 4 |
| q02 | 85.2 | 31.4 | 2.71x | yes | 100 / 100 |
| q03 | 61.1 | 34.5 | 1.77x | yes | 10 / 10 |
| q04 | 29.9 | 23.1 | 1.29x | yes | 5 / 5 |
| q05 | 70.7 | 44.9 | 1.57x | yes | 5 / 5 |
| q06 | 12.2 | 7.0 | 1.74x | yes | 1 / 1 |
| q07 | 98.9 | 47.5 | 2.08x | yes | 4 / 4 |
| q08 | 92.8 | 52.7 | 1.76x | yes | 2 / 2 |
| q09 | 130.4 | 91.3 | 1.43x | yes | 175 / 175 |
| q10 | 129.3 | 71.8 | 1.80x | yes | 20 / 20 |
| q11 | 42.8 | 20.7 | 2.07x | yes | 1048 / 1048 |
| q12 | 26.5 | 17.8 | 1.49x | yes | 2 / 2 |
| q13 | 132.8 | 98.4 | 1.35x | yes | 42 / 42 |
| q14 | 53.8 | 40.9 | 1.31x | yes | 1 / 1 |
| q15 | 35.3 | 16.9 | 2.09x | yes | 1 / 1 |
| q16 | 100.0 | 56.0 | 1.79x | yes | 18314 / 18314 |
| q17 | 50.7 | 26.7 | 1.90x | yes | 1 / 1 |
| q18 | 127.8 | 89.8 | 1.42x | yes | 57 / 57 |
| q19 | 85.1 | 65.5 | 1.30x | yes | 1 / 1 |
| q20 | 73.6 | 33.1 | 2.23x | yes | 186 / 186 |
| q21 | 151.4 | 138.6 | 1.09x | yes | 100 / 100 |
| q22 | 75.0 | 60.5 | 1.24x | yes | 7 / 7 |

