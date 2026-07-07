# TPC-H benchmark report

Commit: `9a28f39` - handoff: Phase 1 of disjunctive decorrelation done - PASS 98 | MISMATCH 1 | ERROR 0  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 15:41
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
| fedpgduck | 1 | 22/22 | 2145.2 | 1124.8 | 1.91x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 35.1 | 26.8 | 1.31x | yes | 4 / 4 |
| q02 | 99.7 | 31.7 | 3.15x | yes | 100 / 100 |
| q03 | 73.5 | 34.9 | 2.11x | yes | 10 / 10 |
| q04 | 30.2 | 22.9 | 1.32x | yes | 5 / 5 |
| q05 | 146.3 | 46.9 | 3.12x | yes | 5 / 5 |
| q06 | 12.5 | 7.7 | 1.62x | yes | 1 / 1 |
| q07 | 113.8 | 43.1 | 2.64x | yes | 4 / 4 |
| q08 | 196.3 | 54.4 | 3.61x | yes | 2 / 2 |
| q09 | 234.0 | 98.5 | 2.38x | yes | 175 / 175 |
| q10 | 159.4 | 72.0 | 2.21x | yes | 20 / 20 |
| q11 | 47.8 | 16.5 | 2.90x | yes | 1048 / 1048 |
| q12 | 25.9 | 17.9 | 1.44x | yes | 2 / 2 |
| q13 | 178.4 | 94.7 | 1.88x | yes | 42 / 42 |
| q14 | 50.9 | 46.8 | 1.09x | yes | 1 / 1 |
| q15 | 46.3 | 21.3 | 2.17x | yes | 1 / 1 |
| q16 | 103.3 | 58.7 | 1.76x | yes | 18314 / 18314 |
| q17 | 51.1 | 32.4 | 1.58x | yes | 1 / 1 |
| q18 | 135.3 | 95.5 | 1.42x | yes | 57 / 57 |
| q19 | 67.2 | 60.4 | 1.11x | yes | 1 / 1 |
| q20 | 92.0 | 35.5 | 2.59x | yes | 186 / 186 |
| q21 | 145.6 | 140.9 | 1.03x | yes | 100 / 100 |
| q22 | 100.6 | 65.3 | 1.54x | yes | 7 / 7 |

