# TPC-H benchmark report

Commit: `77ac78e` - rust_ir: multi-injection - runner-up candidates ride as extra IN lists  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-08 08:31
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
| fedpgduck | 1 | 22/22 | 1958.2 | 1116.5 | 1.75x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.3 | 24.5 | 1.32x | yes | 4 / 4 |
| q02 | 103.2 | 33.4 | 3.09x | yes | 100 / 100 |
| q03 | 87.2 | 39.3 | 2.22x | yes | 10 / 10 |
| q04 | 32.3 | 22.7 | 1.42x | yes | 5 / 5 |
| q05 | 118.5 | 42.9 | 2.76x | yes | 5 / 5 |
| q06 | 12.0 | 7.6 | 1.57x | yes | 1 / 1 |
| q07 | 99.4 | 46.0 | 2.16x | yes | 4 / 4 |
| q08 | 161.6 | 53.1 | 3.04x | yes | 2 / 2 |
| q09 | 205.6 | 102.8 | 2.00x | yes | 175 / 175 |
| q10 | 123.6 | 74.3 | 1.66x | yes | 20 / 20 |
| q11 | 35.7 | 18.1 | 1.97x | yes | 1048 / 1048 |
| q12 | 24.6 | 17.0 | 1.45x | yes | 2 / 2 |
| q13 | 140.2 | 101.6 | 1.38x | yes | 42 / 42 |
| q14 | 52.8 | 40.7 | 1.30x | yes | 1 / 1 |
| q15 | 32.0 | 17.5 | 1.83x | yes | 1 / 1 |
| q16 | 100.2 | 54.1 | 1.85x | yes | 18314 / 18314 |
| q17 | 54.8 | 28.0 | 1.96x | yes | 1 / 1 |
| q18 | 140.7 | 92.7 | 1.52x | yes | 57 / 57 |
| q19 | 68.8 | 60.4 | 1.14x | yes | 1 / 1 |
| q20 | 87.8 | 35.2 | 2.50x | yes | 186 / 186 |
| q21 | 143.1 | 145.6 | 0.98x | yes | 100 / 100 |
| q22 | 101.7 | 58.9 | 1.73x | yes | 7 / 7 |

