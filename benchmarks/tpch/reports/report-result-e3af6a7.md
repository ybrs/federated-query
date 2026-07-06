# TPC-H benchmark report

Commit: `e3af6a7` - executor: place the key filter INSIDE island SQL, on its owning relation  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 15:43
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.
Scale factors: SF0.1 (600K lineitem), SF1 (6M lineitem).

## Methodology

Timing is the median of three warm runs per query; correctness is a differential row-by-row check against DuckDB on the same data (numbers rounded to 2 decimals). Each query runs in its own subprocess with an RSS cap, so a blow-up is a KILLED row, not a lost run. Ratio is ours/DuckDB (higher = we are slower).

- **single** - Single source (pure engine, Parquet).
- **fedparquet** - Federated, 2 Parquet sources (DuckDB monolithic - unfair); DuckDB reads all files as one, so this overstates our cost.
- **fedpgduck** - Federated, PostgreSQL + DuckDB (both federate - fair); the DuckDB oracle federates the same split via its postgres connector. This is the honest federated number.

## Summary

Totals cover only queries that produced a measurement; TIMEOUT / KILLED / ERROR queries are excluded from the ms totals but counted as not-correct. `measured` is how many of the 22 timed cleanly.

| Cell | SF | Correct | Ours (ms) | DuckDB (ms) | Ratio | Measured |
| --- | --- | --- | --- | --- | --- | --- |
| fedpgduck | 0.1 | 22/22 | 965.4 | 295.8 | 3.26x | 22 |
| fedpgduck | 1 | 22/22 | 2163.2 | 1122.2 | 1.93x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 15.5 | 6.9 | 2.24x | yes | 4 / 4 |
| q02 | 61.3 | 11.8 | 5.18x | yes | 44 / 44 |
| q03 | 39.3 | 10.3 | 3.82x | yes | 10 / 10 |
| q04 | 16.6 | 6.7 | 2.46x | yes | 5 / 5 |
| q05 | 54.1 | 12.9 | 4.19x | yes | 5 / 5 |
| q06 | 8.3 | 2.5 | 3.35x | yes | 1 / 1 |
| q07 | 73.8 | 15.3 | 4.81x | yes | 4 / 4 |
| q08 | 85.0 | 16.7 | 5.08x | yes | 2 / 2 |
| q09 | 81.2 | 24.3 | 3.34x | yes | 175 / 175 |
| q10 | 47.4 | 18.9 | 2.50x | yes | 20 / 20 |
| q11 | 39.5 | 11.9 | 3.32x | yes | 2541 / 2541 |
| q12 | 14.1 | 6.1 | 2.34x | yes | 2 / 2 |
| q13 | 46.6 | 23.4 | 1.99x | yes | 37 / 37 |
| q14 | 25.0 | 9.6 | 2.61x | yes | 1 / 1 |
| q15 | 33.4 | 4.6 | 7.32x | yes | 1 / 1 |
| q16 | 35.4 | 16.1 | 2.19x | yes | 2762 / 2762 |
| q17 | 29.7 | 9.5 | 3.14x | yes | 1 / 1 |
| q18 | 56.9 | 16.7 | 3.41x | yes | 5 / 5 |
| q19 | 42.6 | 22.6 | 1.88x | yes | 1 / 1 |
| q20 | 50.7 | 11.6 | 4.36x | yes | 9 / 9 |
| q21 | 69.4 | 20.9 | 3.32x | yes | 47 / 47 |
| q22 | 39.6 | 16.4 | 2.42x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 33.9 | 26.5 | 1.28x | yes | 4 / 4 |
| q02 | 107.8 | 32.3 | 3.33x | yes | 100 / 100 |
| q03 | 73.7 | 32.8 | 2.25x | yes | 10 / 10 |
| q04 | 31.1 | 23.0 | 1.35x | yes | 5 / 5 |
| q05 | 145.1 | 46.8 | 3.10x | yes | 5 / 5 |
| q06 | 12.7 | 7.7 | 1.66x | yes | 1 / 1 |
| q07 | 159.7 | 48.1 | 3.32x | yes | 4 / 4 |
| q08 | 157.8 | 52.2 | 3.02x | yes | 2 / 2 |
| q09 | 236.2 | 100.4 | 2.35x | yes | 175 / 175 |
| q10 | 139.4 | 76.3 | 1.83x | yes | 20 / 20 |
| q11 | 53.6 | 15.2 | 3.51x | yes | 1048 / 1048 |
| q12 | 27.2 | 18.8 | 1.44x | yes | 2 / 2 |
| q13 | 184.4 | 101.0 | 1.83x | yes | 42 / 42 |
| q14 | 53.6 | 39.8 | 1.35x | yes | 1 / 1 |
| q15 | 49.3 | 19.3 | 2.56x | yes | 1 / 1 |
| q16 | 107.3 | 64.2 | 1.67x | yes | 18314 / 18314 |
| q17 | 50.4 | 26.8 | 1.88x | yes | 1 / 1 |
| q18 | 137.0 | 92.8 | 1.48x | yes | 57 / 57 |
| q19 | 67.4 | 80.3 | 0.84x | yes | 1 / 1 |
| q20 | 93.5 | 34.8 | 2.68x | yes | 186 / 186 |
| q21 | 142.0 | 124.9 | 1.14x | yes | 100 / 100 |
| q22 | 100.0 | 58.2 | 1.72x | yes | 7 / 7 |

