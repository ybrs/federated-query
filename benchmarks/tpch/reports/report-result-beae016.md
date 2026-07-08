# TPC-H benchmark report

Commit: `beae016` - rust_ir: collect build keys from the ORIGINATING base relation when traceable  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 19:27
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
| fedpgduck | 1 | 22/22 | 1913.6 | 1050.5 | 1.82x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.6 | 25.8 | 1.26x | yes | 4 / 4 |
| q02 | 91.1 | 33.0 | 2.76x | yes | 100 / 100 |
| q03 | 64.9 | 31.6 | 2.05x | yes | 10 / 10 |
| q04 | 30.4 | 25.3 | 1.20x | yes | 5 / 5 |
| q05 | 137.4 | 41.8 | 3.29x | yes | 5 / 5 |
| q06 | 11.6 | 7.2 | 1.62x | yes | 1 / 1 |
| q07 | 100.1 | 44.3 | 2.26x | yes | 4 / 4 |
| q08 | 145.2 | 50.4 | 2.88x | yes | 2 / 2 |
| q09 | 202.2 | 88.4 | 2.29x | yes | 175 / 175 |
| q10 | 139.0 | 73.0 | 1.90x | yes | 20 / 20 |
| q11 | 45.0 | 15.3 | 2.94x | yes | 1048 / 1048 |
| q12 | 24.8 | 15.9 | 1.56x | yes | 2 / 2 |
| q13 | 133.2 | 94.1 | 1.41x | yes | 42 / 42 |
| q14 | 50.2 | 39.7 | 1.27x | yes | 1 / 1 |
| q15 | 35.2 | 19.5 | 1.80x | yes | 1 / 1 |
| q16 | 98.2 | 55.2 | 1.78x | yes | 18314 / 18314 |
| q17 | 51.1 | 27.0 | 1.89x | yes | 1 / 1 |
| q18 | 132.6 | 88.8 | 1.49x | yes | 57 / 57 |
| q19 | 65.8 | 60.4 | 1.09x | yes | 1 / 1 |
| q20 | 86.8 | 34.3 | 2.53x | yes | 186 / 186 |
| q21 | 138.9 | 122.6 | 1.13x | yes | 100 / 100 |
| q22 | 97.5 | 56.8 | 1.71x | yes | 7 / 7 |

