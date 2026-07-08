# TPC-H benchmark report

Commit: `b5240d1` - rust_ir: inject build keys through composite probes (fact-injection plan phase A)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 23:12
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
| fedpgduck | 1 | 22/22 | 1850.2 | 1047.6 | 1.77x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.9 | 26.9 | 1.22x | yes | 4 / 4 |
| q02 | 88.6 | 31.7 | 2.79x | yes | 100 / 100 |
| q03 | 68.5 | 36.3 | 1.89x | yes | 10 / 10 |
| q04 | 33.3 | 22.8 | 1.46x | yes | 5 / 5 |
| q05 | 110.9 | 45.9 | 2.42x | yes | 5 / 5 |
| q06 | 11.5 | 7.4 | 1.54x | yes | 1 / 1 |
| q07 | 93.3 | 42.2 | 2.21x | yes | 4 / 4 |
| q08 | 151.4 | 55.3 | 2.74x | yes | 2 / 2 |
| q09 | 194.2 | 92.4 | 2.10x | yes | 175 / 175 |
| q10 | 121.0 | 72.7 | 1.66x | yes | 20 / 20 |
| q11 | 43.4 | 16.0 | 2.71x | yes | 1048 / 1048 |
| q12 | 25.3 | 15.8 | 1.60x | yes | 2 / 2 |
| q13 | 129.6 | 88.9 | 1.46x | yes | 42 / 42 |
| q14 | 52.9 | 39.6 | 1.34x | yes | 1 / 1 |
| q15 | 33.7 | 18.9 | 1.79x | yes | 1 / 1 |
| q16 | 104.3 | 56.9 | 1.83x | yes | 18314 / 18314 |
| q17 | 50.7 | 27.0 | 1.88x | yes | 1 / 1 |
| q18 | 133.3 | 84.9 | 1.57x | yes | 57 / 57 |
| q19 | 66.2 | 59.2 | 1.12x | yes | 1 / 1 |
| q20 | 85.3 | 32.4 | 2.63x | yes | 186 / 186 |
| q21 | 136.4 | 119.7 | 1.14x | yes | 100 / 100 |
| q22 | 83.4 | 54.6 | 1.53x | yes | 7 / 7 |

