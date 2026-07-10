# TPC-H benchmark report

Commit: `7846a15` - benchmark: cold-sources harness (learning off vs on with no remote stats)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 01:52
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
| fedpgduck | 1 | 22/22 | 1748.6 | 1081.2 | 1.62x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 34.8 | 26.1 | 1.33x | yes | 4 / 4 |
| q02 | 89.4 | 32.0 | 2.79x | yes | 100 / 100 |
| q03 | 57.8 | 34.5 | 1.67x | yes | 10 / 10 |
| q04 | 30.9 | 23.0 | 1.35x | yes | 5 / 5 |
| q05 | 68.7 | 42.4 | 1.62x | yes | 5 / 5 |
| q06 | 12.1 | 7.3 | 1.66x | yes | 1 / 1 |
| q07 | 97.0 | 48.6 | 2.00x | yes | 4 / 4 |
| q08 | 94.9 | 53.5 | 1.78x | yes | 2 / 2 |
| q09 | 128.9 | 93.4 | 1.38x | yes | 175 / 175 |
| q10 | 128.3 | 67.7 | 1.90x | yes | 20 / 20 |
| q11 | 44.0 | 16.1 | 2.73x | yes | 1048 / 1048 |
| q12 | 25.5 | 17.7 | 1.44x | yes | 2 / 2 |
| q13 | 135.9 | 93.4 | 1.45x | yes | 42 / 42 |
| q14 | 56.0 | 53.1 | 1.06x | yes | 1 / 1 |
| q15 | 38.3 | 18.9 | 2.02x | yes | 1 / 1 |
| q16 | 108.9 | 63.1 | 1.73x | yes | 18314 / 18314 |
| q17 | 51.4 | 28.2 | 1.82x | yes | 1 / 1 |
| q18 | 134.1 | 87.3 | 1.54x | yes | 57 / 57 |
| q19 | 86.9 | 63.3 | 1.37x | yes | 1 / 1 |
| q20 | 86.8 | 33.9 | 2.56x | yes | 186 / 186 |
| q21 | 144.4 | 120.4 | 1.20x | yes | 100 / 100 |
| q22 | 93.5 | 57.3 | 1.63x | yes | 7 / 7 |

