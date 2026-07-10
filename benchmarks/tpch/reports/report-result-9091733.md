# TPC-H benchmark report

Commit: `9091733` - docs: eager aggregation Phase A landed + gate findings  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 16:48
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
| fedpgduck | 1 | 22/22 | 1417.9 | 1087.4 | 1.30x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.5 | 24.5 | 1.20x | yes | 4 / 4 |
| q02 | 61.2 | 35.6 | 1.72x | yes | 100 / 100 |
| q03 | 69.2 | 36.1 | 1.92x | yes | 10 / 10 |
| q04 | 26.2 | 22.8 | 1.15x | yes | 5 / 5 |
| q05 | 51.0 | 42.6 | 1.20x | yes | 5 / 5 |
| q06 | 10.7 | 6.9 | 1.55x | yes | 1 / 1 |
| q07 | 84.0 | 47.1 | 1.78x | yes | 4 / 4 |
| q08 | 73.0 | 61.6 | 1.19x | yes | 2 / 2 |
| q09 | 112.9 | 91.0 | 1.24x | yes | 175 / 175 |
| q10 | 84.9 | 72.7 | 1.17x | yes | 20 / 20 |
| q11 | 46.6 | 15.7 | 2.98x | yes | 1048 / 1048 |
| q12 | 20.9 | 17.0 | 1.23x | yes | 2 / 2 |
| q13 | 125.4 | 97.9 | 1.28x | yes | 42 / 42 |
| q14 | 43.8 | 38.4 | 1.14x | yes | 1 / 1 |
| q15 | 24.7 | 17.0 | 1.45x | yes | 1 / 1 |
| q16 | 87.3 | 58.6 | 1.49x | yes | 18314 / 18314 |
| q17 | 42.3 | 27.7 | 1.53x | yes | 1 / 1 |
| q18 | 115.6 | 88.5 | 1.31x | yes | 57 / 57 |
| q19 | 60.1 | 60.8 | 0.99x | yes | 1 / 1 |
| q20 | 60.5 | 36.6 | 1.65x | yes | 186 / 186 |
| q21 | 128.8 | 135.2 | 0.95x | yes | 100 / 100 |
| q22 | 59.5 | 53.1 | 1.12x | yes | 7 / 7 |

