# TPC-H benchmark report

Commit: `6ce46d3` - cost model: delete the legacy untracked estimator family (user-approved)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 02:42
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
| fedpgduck | 1 | 22/22 | 1750.0 | 1070.9 | 1.63x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 33.0 | 26.6 | 1.24x | yes | 4 / 4 |
| q02 | 98.2 | 35.8 | 2.74x | yes | 100 / 100 |
| q03 | 56.4 | 36.1 | 1.56x | yes | 10 / 10 |
| q04 | 31.6 | 22.0 | 1.44x | yes | 5 / 5 |
| q05 | 70.9 | 44.8 | 1.58x | yes | 5 / 5 |
| q06 | 11.8 | 7.1 | 1.65x | yes | 1 / 1 |
| q07 | 95.2 | 46.1 | 2.06x | yes | 4 / 4 |
| q08 | 94.4 | 53.1 | 1.78x | yes | 2 / 2 |
| q09 | 143.1 | 93.6 | 1.53x | yes | 175 / 175 |
| q10 | 125.9 | 70.6 | 1.78x | yes | 20 / 20 |
| q11 | 42.0 | 15.5 | 2.72x | yes | 1048 / 1048 |
| q12 | 24.2 | 16.5 | 1.47x | yes | 2 / 2 |
| q13 | 136.8 | 107.4 | 1.27x | yes | 42 / 42 |
| q14 | 51.8 | 38.3 | 1.35x | yes | 1 / 1 |
| q15 | 33.7 | 16.8 | 2.01x | yes | 1 / 1 |
| q16 | 101.4 | 54.4 | 1.86x | yes | 18314 / 18314 |
| q17 | 55.9 | 27.0 | 2.07x | yes | 1 / 1 |
| q18 | 139.3 | 88.1 | 1.58x | yes | 57 / 57 |
| q19 | 85.1 | 58.9 | 1.45x | yes | 1 / 1 |
| q20 | 87.3 | 36.3 | 2.41x | yes | 186 / 186 |
| q21 | 145.1 | 119.8 | 1.21x | yes | 100 / 100 |
| q22 | 86.9 | 56.0 | 1.55x | yes | 7 / 7 |

