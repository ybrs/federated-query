# TPC-H benchmark report

Commit: `e10d67b` - merge: cap the composite-key NDV denominator (q09 -290ms single, fair 3.08x -> 2.98x)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 11:49
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
| fedpgduck | 0.1 | 22/22 | 944.1 | 288.8 | 3.27x | 22 |
| fedpgduck | 1 | 22/22 | 2483.2 | 1097.6 | 2.26x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.8 | 6.5 | 2.28x | yes | 4 / 4 |
| q02 | 61.8 | 11.9 | 5.18x | yes | 44 / 44 |
| q03 | 39.2 | 11.2 | 3.48x | yes | 10 / 10 |
| q04 | 15.7 | 8.0 | 1.95x | yes | 5 / 5 |
| q05 | 57.1 | 12.2 | 4.68x | yes | 5 / 5 |
| q06 | 7.4 | 2.2 | 3.35x | yes | 1 / 1 |
| q07 | 72.4 | 15.5 | 4.67x | yes | 4 / 4 |
| q08 | 74.7 | 15.6 | 4.79x | yes | 2 / 2 |
| q09 | 81.9 | 25.7 | 3.18x | yes | 175 / 175 |
| q10 | 57.8 | 22.0 | 2.63x | yes | 20 / 20 |
| q11 | 40.5 | 11.5 | 3.51x | yes | 2541 / 2541 |
| q12 | 15.2 | 5.9 | 2.59x | yes | 2 / 2 |
| q13 | 40.4 | 22.2 | 1.82x | yes | 37 / 37 |
| q14 | 26.5 | 9.4 | 2.83x | yes | 1 / 1 |
| q15 | 31.1 | 4.6 | 6.77x | yes | 1 / 1 |
| q16 | 35.1 | 13.4 | 2.63x | yes | 2762 / 2762 |
| q17 | 27.5 | 8.9 | 3.09x | yes | 1 / 1 |
| q18 | 65.4 | 17.9 | 3.65x | yes | 5 / 5 |
| q19 | 36.8 | 14.0 | 2.63x | yes | 1 / 1 |
| q20 | 43.8 | 12.0 | 3.65x | yes | 9 / 9 |
| q21 | 62.9 | 20.7 | 3.03x | yes | 47 / 47 |
| q22 | 36.1 | 17.4 | 2.07x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.6 | 24.8 | 1.32x | yes | 4 / 4 |
| q02 | 113.5 | 30.4 | 3.74x | yes | 100 / 100 |
| q03 | 107.3 | 33.1 | 3.25x | yes | 10 / 10 |
| q04 | 31.6 | 22.8 | 1.38x | yes | 5 / 5 |
| q05 | 169.2 | 45.4 | 3.73x | yes | 5 / 5 |
| q06 | 12.9 | 7.5 | 1.73x | yes | 1 / 1 |
| q07 | 221.7 | 43.3 | 5.12x | yes | 4 / 4 |
| q08 | 163.7 | 56.1 | 2.92x | yes | 2 / 2 |
| q09 | 245.7 | 97.4 | 2.52x | yes | 175 / 175 |
| q10 | 265.3 | 80.5 | 3.30x | yes | 20 / 20 |
| q11 | 52.4 | 15.2 | 3.44x | yes | 1048 / 1048 |
| q12 | 26.0 | 17.8 | 1.46x | yes | 2 / 2 |
| q13 | 218.0 | 100.7 | 2.16x | yes | 42 / 42 |
| q14 | 81.3 | 38.5 | 2.11x | yes | 1 / 1 |
| q15 | 48.7 | 16.9 | 2.89x | yes | 1 / 1 |
| q16 | 98.8 | 53.9 | 1.83x | yes | 18314 / 18314 |
| q17 | 48.9 | 27.3 | 1.79x | yes | 1 / 1 |
| q18 | 159.4 | 91.0 | 1.75x | yes | 57 / 57 |
| q19 | 70.7 | 62.2 | 1.14x | yes | 1 / 1 |
| q20 | 93.2 | 34.5 | 2.70x | yes | 186 / 186 |
| q21 | 134.1 | 139.0 | 0.96x | yes | 100 / 100 |
| q22 | 88.3 | 59.6 | 1.48x | yes | 7 / 7 |

