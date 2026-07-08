# TPC-H benchmark report

Commit: `7c0a420` - rust_ir: union-branch injection + composite probe urgency from traced bases  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 23:19
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
| fedpgduck | 1 | 22/22 | 1872.2 | 1087.2 | 1.72x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.6 | 25.4 | 1.28x | yes | 4 / 4 |
| q02 | 90.5 | 32.8 | 2.76x | yes | 100 / 100 |
| q03 | 74.4 | 35.5 | 2.10x | yes | 10 / 10 |
| q04 | 30.2 | 22.3 | 1.36x | yes | 5 / 5 |
| q05 | 115.0 | 43.9 | 2.62x | yes | 5 / 5 |
| q06 | 11.8 | 7.0 | 1.67x | yes | 1 / 1 |
| q07 | 100.6 | 46.8 | 2.15x | yes | 4 / 4 |
| q08 | 149.9 | 52.8 | 2.84x | yes | 2 / 2 |
| q09 | 195.3 | 96.0 | 2.03x | yes | 175 / 175 |
| q10 | 120.6 | 74.7 | 1.61x | yes | 20 / 20 |
| q11 | 51.4 | 14.8 | 3.47x | yes | 1048 / 1048 |
| q12 | 23.7 | 16.9 | 1.41x | yes | 2 / 2 |
| q13 | 131.4 | 97.1 | 1.35x | yes | 42 / 42 |
| q14 | 53.5 | 42.4 | 1.26x | yes | 1 / 1 |
| q15 | 35.0 | 16.6 | 2.11x | yes | 1 / 1 |
| q16 | 95.3 | 53.5 | 1.78x | yes | 18314 / 18314 |
| q17 | 56.2 | 27.1 | 2.07x | yes | 1 / 1 |
| q18 | 132.1 | 86.6 | 1.53x | yes | 57 / 57 |
| q19 | 65.8 | 61.0 | 1.08x | yes | 1 / 1 |
| q20 | 82.0 | 32.4 | 2.53x | yes | 186 / 186 |
| q21 | 134.5 | 120.9 | 1.11x | yes | 100 / 100 |
| q22 | 90.7 | 80.8 | 1.12x | yes | 7 / 7 |

