# TPC-H benchmark report

Commit: `5f123d0` - docs: the CBO completion round - 1.98x fair, all decisions cost-based  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 17:20
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
| fedpgduck | 1 | 22/22 | 2201.1 | 1115.3 | 1.97x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 33.2 | 26.4 | 1.26x | yes | 4 / 4 |
| q02 | 118.2 | 35.7 | 3.31x | yes | 100 / 100 |
| q03 | 73.9 | 35.1 | 2.11x | yes | 10 / 10 |
| q04 | 31.4 | 24.9 | 1.26x | yes | 5 / 5 |
| q05 | 142.6 | 50.0 | 2.85x | yes | 5 / 5 |
| q06 | 11.8 | 7.1 | 1.66x | yes | 1 / 1 |
| q07 | 159.5 | 50.3 | 3.17x | yes | 4 / 4 |
| q08 | 178.6 | 56.4 | 3.17x | yes | 2 / 2 |
| q09 | 248.2 | 108.0 | 2.30x | yes | 175 / 175 |
| q10 | 142.2 | 73.9 | 1.92x | yes | 20 / 20 |
| q11 | 52.1 | 15.9 | 3.29x | yes | 1048 / 1048 |
| q12 | 23.9 | 17.4 | 1.38x | yes | 2 / 2 |
| q13 | 181.4 | 98.3 | 1.84x | yes | 42 / 42 |
| q14 | 54.5 | 39.5 | 1.38x | yes | 1 / 1 |
| q15 | 49.8 | 17.4 | 2.86x | yes | 1 / 1 |
| q16 | 108.4 | 64.4 | 1.68x | yes | 18314 / 18314 |
| q17 | 51.6 | 28.3 | 1.82x | yes | 1 / 1 |
| q18 | 133.5 | 87.9 | 1.52x | yes | 57 / 57 |
| q19 | 66.5 | 60.3 | 1.10x | yes | 1 / 1 |
| q20 | 93.3 | 33.1 | 2.81x | yes | 186 / 186 |
| q21 | 143.7 | 126.6 | 1.13x | yes | 100 / 100 |
| q22 | 102.9 | 58.4 | 1.76x | yes | 7 / 7 |

