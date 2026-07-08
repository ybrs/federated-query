# TPC-H benchmark report

Commit: `3f0efc4` - rules: transitive constant propagation at joins + condition push under residual filters  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-08 04:00
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
| fedpgduck | 1 | 22/22 | 1871.9 | 1063.3 | 1.76x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.9 | 24.4 | 1.35x | yes | 4 / 4 |
| q02 | 95.5 | 32.9 | 2.90x | yes | 100 / 100 |
| q03 | 70.0 | 36.3 | 1.93x | yes | 10 / 10 |
| q04 | 31.1 | 22.2 | 1.40x | yes | 5 / 5 |
| q05 | 110.2 | 42.8 | 2.57x | yes | 5 / 5 |
| q06 | 11.6 | 7.1 | 1.63x | yes | 1 / 1 |
| q07 | 94.4 | 52.3 | 1.80x | yes | 4 / 4 |
| q08 | 143.1 | 53.5 | 2.68x | yes | 2 / 2 |
| q09 | 200.8 | 99.2 | 2.02x | yes | 175 / 175 |
| q10 | 127.5 | 69.8 | 1.83x | yes | 20 / 20 |
| q11 | 42.3 | 15.0 | 2.82x | yes | 1048 / 1048 |
| q12 | 24.4 | 16.7 | 1.46x | yes | 2 / 2 |
| q13 | 142.3 | 100.0 | 1.42x | yes | 42 / 42 |
| q14 | 49.4 | 38.8 | 1.27x | yes | 1 / 1 |
| q15 | 34.5 | 17.3 | 1.99x | yes | 1 / 1 |
| q16 | 99.3 | 53.2 | 1.86x | yes | 18314 / 18314 |
| q17 | 52.1 | 26.9 | 1.94x | yes | 1 / 1 |
| q18 | 130.9 | 87.0 | 1.51x | yes | 57 / 57 |
| q19 | 70.6 | 64.0 | 1.10x | yes | 1 / 1 |
| q20 | 85.3 | 34.1 | 2.50x | yes | 186 / 186 |
| q21 | 140.5 | 113.6 | 1.24x | yes | 100 / 100 |
| q22 | 83.0 | 56.1 | 1.48x | yes | 7 / 7 |

