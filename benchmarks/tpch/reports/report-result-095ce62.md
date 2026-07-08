# TPC-H benchmark report

Commit: `095ce62` - rust_ir: the most selective reduction wins a shared base, not the innermost  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-08 00:29
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
| fedpgduck | 1 | 22/22 | 1843.3 | 1064.6 | 1.73x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.7 | 26.0 | 1.22x | yes | 4 / 4 |
| q02 | 89.8 | 32.7 | 2.74x | yes | 100 / 100 |
| q03 | 67.9 | 33.5 | 2.03x | yes | 10 / 10 |
| q04 | 28.7 | 22.2 | 1.30x | yes | 5 / 5 |
| q05 | 110.7 | 44.7 | 2.47x | yes | 5 / 5 |
| q06 | 11.9 | 7.5 | 1.59x | yes | 1 / 1 |
| q07 | 103.2 | 56.0 | 1.84x | yes | 4 / 4 |
| q08 | 144.6 | 49.7 | 2.91x | yes | 2 / 2 |
| q09 | 195.0 | 94.2 | 2.07x | yes | 175 / 175 |
| q10 | 122.2 | 71.2 | 1.72x | yes | 20 / 20 |
| q11 | 43.7 | 15.2 | 2.88x | yes | 1048 / 1048 |
| q12 | 23.3 | 16.8 | 1.39x | yes | 2 / 2 |
| q13 | 131.6 | 92.0 | 1.43x | yes | 42 / 42 |
| q14 | 49.4 | 39.7 | 1.25x | yes | 1 / 1 |
| q15 | 33.0 | 16.9 | 1.96x | yes | 1 / 1 |
| q16 | 98.2 | 55.6 | 1.77x | yes | 18314 / 18314 |
| q17 | 52.8 | 28.7 | 1.84x | yes | 1 / 1 |
| q18 | 127.1 | 85.2 | 1.49x | yes | 57 / 57 |
| q19 | 66.9 | 71.5 | 0.94x | yes | 1 / 1 |
| q20 | 82.0 | 34.4 | 2.38x | yes | 186 / 186 |
| q21 | 140.2 | 120.1 | 1.17x | yes | 100 / 100 |
| q22 | 89.5 | 50.9 | 1.76x | yes | 7 / 7 |

