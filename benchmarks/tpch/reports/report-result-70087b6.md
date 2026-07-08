# TPC-H benchmark report

Commit: `70087b6` - decorrelation: a scalar-equality residual tightens its LEFT join to an INNER equi join  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-08 02:36
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
| fedpgduck | 1 | 22/22 | 1893.6 | 1071.7 | 1.77x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 34.9 | 27.2 | 1.28x | yes | 4 / 4 |
| q02 | 89.8 | 34.1 | 2.63x | yes | 100 / 100 |
| q03 | 71.7 | 34.4 | 2.08x | yes | 10 / 10 |
| q04 | 30.6 | 23.2 | 1.32x | yes | 5 / 5 |
| q05 | 124.8 | 44.4 | 2.81x | yes | 5 / 5 |
| q06 | 11.8 | 7.0 | 1.69x | yes | 1 / 1 |
| q07 | 98.0 | 42.0 | 2.33x | yes | 4 / 4 |
| q08 | 157.2 | 52.9 | 2.97x | yes | 2 / 2 |
| q09 | 210.2 | 105.0 | 2.00x | yes | 175 / 175 |
| q10 | 126.3 | 73.6 | 1.72x | yes | 20 / 20 |
| q11 | 44.0 | 15.0 | 2.92x | yes | 1048 / 1048 |
| q12 | 27.5 | 20.5 | 1.34x | yes | 2 / 2 |
| q13 | 133.6 | 95.0 | 1.41x | yes | 42 / 42 |
| q14 | 50.4 | 37.5 | 1.34x | yes | 1 / 1 |
| q15 | 30.6 | 17.0 | 1.80x | yes | 1 / 1 |
| q16 | 95.4 | 54.6 | 1.75x | yes | 18314 / 18314 |
| q17 | 50.9 | 26.7 | 1.91x | yes | 1 / 1 |
| q18 | 134.4 | 91.8 | 1.47x | yes | 57 / 57 |
| q19 | 66.2 | 60.5 | 1.09x | yes | 1 / 1 |
| q20 | 83.9 | 31.4 | 2.67x | yes | 186 / 186 |
| q21 | 137.3 | 121.8 | 1.13x | yes | 100 / 100 |
| q22 | 84.2 | 56.1 | 1.50x | yes | 7 / 7 |

