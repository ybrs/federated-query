# TPC-H benchmark report

Commit: `01531ae` - handoff: parallel reads Phase A landed; Phase B pointers  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 04:39
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
| fedpgduck | 1 | 22/22 | 1683.8 | 1078.7 | 1.56x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 34.0 | 25.7 | 1.32x | yes | 4 / 4 |
| q02 | 82.4 | 30.9 | 2.66x | yes | 100 / 100 |
| q03 | 59.0 | 32.6 | 1.81x | yes | 10 / 10 |
| q04 | 30.3 | 22.4 | 1.35x | yes | 5 / 5 |
| q05 | 70.7 | 46.7 | 1.51x | yes | 5 / 5 |
| q06 | 12.0 | 7.1 | 1.70x | yes | 1 / 1 |
| q07 | 93.1 | 43.9 | 2.12x | yes | 4 / 4 |
| q08 | 85.7 | 52.2 | 1.64x | yes | 2 / 2 |
| q09 | 125.9 | 98.5 | 1.28x | yes | 175 / 175 |
| q10 | 124.5 | 74.0 | 1.68x | yes | 20 / 20 |
| q11 | 42.5 | 15.7 | 2.71x | yes | 1048 / 1048 |
| q12 | 23.8 | 16.4 | 1.45x | yes | 2 / 2 |
| q13 | 132.8 | 107.2 | 1.24x | yes | 42 / 42 |
| q14 | 54.8 | 40.4 | 1.35x | yes | 1 / 1 |
| q15 | 34.3 | 17.5 | 1.96x | yes | 1 / 1 |
| q16 | 99.4 | 55.7 | 1.78x | yes | 18314 / 18314 |
| q17 | 50.9 | 26.1 | 1.95x | yes | 1 / 1 |
| q18 | 129.6 | 89.0 | 1.46x | yes | 57 / 57 |
| q19 | 86.6 | 62.6 | 1.38x | yes | 1 / 1 |
| q20 | 79.6 | 35.9 | 2.22x | yes | 186 / 186 |
| q21 | 160.3 | 123.1 | 1.30x | yes | 100 / 100 |
| q22 | 71.5 | 55.0 | 1.30x | yes | 7 / 7 |

