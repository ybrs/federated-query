# TPC-H benchmark report

Commit: `522267b` - rules: predicate pushdown descends through the cardinality guard  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-08 01:45
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
| fedpgduck | 1 | 22/22 | 1870.6 | 1065.6 | 1.76x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.8 | 25.0 | 1.31x | yes | 4 / 4 |
| q02 | 88.9 | 33.0 | 2.69x | yes | 100 / 100 |
| q03 | 68.9 | 36.4 | 1.89x | yes | 10 / 10 |
| q04 | 30.5 | 23.9 | 1.27x | yes | 5 / 5 |
| q05 | 113.2 | 42.7 | 2.65x | yes | 5 / 5 |
| q06 | 11.8 | 7.2 | 1.65x | yes | 1 / 1 |
| q07 | 96.4 | 47.4 | 2.04x | yes | 4 / 4 |
| q08 | 166.9 | 51.4 | 3.24x | yes | 2 / 2 |
| q09 | 198.4 | 94.4 | 2.10x | yes | 175 / 175 |
| q10 | 122.5 | 74.8 | 1.64x | yes | 20 / 20 |
| q11 | 42.7 | 16.1 | 2.65x | yes | 1048 / 1048 |
| q12 | 24.8 | 16.8 | 1.48x | yes | 2 / 2 |
| q13 | 137.7 | 96.4 | 1.43x | yes | 42 / 42 |
| q14 | 49.7 | 39.5 | 1.26x | yes | 1 / 1 |
| q15 | 34.2 | 17.0 | 2.01x | yes | 1 / 1 |
| q16 | 98.8 | 55.9 | 1.77x | yes | 18314 / 18314 |
| q17 | 51.6 | 27.4 | 1.88x | yes | 1 / 1 |
| q18 | 131.5 | 86.6 | 1.52x | yes | 57 / 57 |
| q19 | 66.3 | 58.8 | 1.13x | yes | 1 / 1 |
| q20 | 81.3 | 33.6 | 2.42x | yes | 186 / 186 |
| q21 | 136.2 | 121.2 | 1.12x | yes | 100 / 100 |
| q22 | 85.6 | 60.1 | 1.42x | yes | 7 / 7 |

