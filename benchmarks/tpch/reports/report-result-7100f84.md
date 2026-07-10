# TPC-H benchmark report

Commit: `7100f84` - eager-agg tests: the fixture's customer exceeds the ship budget (the rescue gate)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 17:04
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
| fedpgduck | 1 | 22/22 | 1396.5 | 1085.4 | 1.29x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 29.4 | 24.6 | 1.20x | yes | 4 / 4 |
| q02 | 59.5 | 33.1 | 1.80x | yes | 100 / 100 |
| q03 | 71.6 | 33.5 | 2.14x | yes | 10 / 10 |
| q04 | 26.6 | 21.9 | 1.21x | yes | 5 / 5 |
| q05 | 50.0 | 41.3 | 1.21x | yes | 5 / 5 |
| q06 | 10.4 | 7.2 | 1.44x | yes | 1 / 1 |
| q07 | 73.6 | 43.6 | 1.69x | yes | 4 / 4 |
| q08 | 66.5 | 53.7 | 1.24x | yes | 2 / 2 |
| q09 | 105.7 | 94.2 | 1.12x | yes | 175 / 175 |
| q10 | 108.7 | 86.3 | 1.26x | yes | 20 / 20 |
| q11 | 28.8 | 15.1 | 1.91x | yes | 1048 / 1048 |
| q12 | 20.9 | 17.2 | 1.21x | yes | 2 / 2 |
| q13 | 128.6 | 102.2 | 1.26x | yes | 42 / 42 |
| q14 | 51.3 | 44.5 | 1.15x | yes | 1 / 1 |
| q15 | 24.3 | 17.4 | 1.39x | yes | 1 / 1 |
| q16 | 86.8 | 60.7 | 1.43x | yes | 18314 / 18314 |
| q17 | 44.7 | 25.3 | 1.77x | yes | 1 / 1 |
| q18 | 115.3 | 86.4 | 1.33x | yes | 57 / 57 |
| q19 | 59.8 | 63.4 | 0.94x | yes | 1 / 1 |
| q20 | 59.8 | 34.2 | 1.75x | yes | 186 / 186 |
| q21 | 113.2 | 125.4 | 0.90x | yes | 100 / 100 |
| q22 | 61.2 | 54.2 | 1.13x | yes | 7 / 7 |

