# TPC-H benchmark report

Commit: `aef5e6d` - optimizer: remotes carry their real output estimate; orientation and the reduction gate consume it  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 15:26
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
| fedpgduck | 0.1 | 22/22 | 964.0 | 299.5 | 3.22x | 22 |
| fedpgduck | 1 | 22/22 | 2286.4 | 1135.1 | 2.01x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 15.4 | 6.5 | 2.36x | yes | 4 / 4 |
| q02 | 66.6 | 14.7 | 4.52x | yes | 44 / 44 |
| q03 | 41.9 | 13.4 | 3.12x | yes | 10 / 10 |
| q04 | 20.2 | 9.2 | 2.20x | yes | 5 / 5 |
| q05 | 54.9 | 12.6 | 4.38x | yes | 5 / 5 |
| q06 | 7.4 | 2.2 | 3.28x | yes | 1 / 1 |
| q07 | 66.1 | 13.8 | 4.78x | yes | 4 / 4 |
| q08 | 82.7 | 17.2 | 4.81x | yes | 2 / 2 |
| q09 | 83.4 | 24.4 | 3.42x | yes | 175 / 175 |
| q10 | 53.2 | 21.2 | 2.51x | yes | 20 / 20 |
| q11 | 42.8 | 11.4 | 3.76x | yes | 2541 / 2541 |
| q12 | 14.2 | 5.8 | 2.47x | yes | 2 / 2 |
| q13 | 49.1 | 29.2 | 1.68x | yes | 37 / 37 |
| q14 | 27.2 | 8.9 | 3.07x | yes | 1 / 1 |
| q15 | 33.3 | 4.7 | 7.07x | yes | 1 / 1 |
| q16 | 34.2 | 15.4 | 2.22x | yes | 2762 / 2762 |
| q17 | 29.3 | 9.3 | 3.16x | yes | 1 / 1 |
| q18 | 64.4 | 16.0 | 4.03x | yes | 5 / 5 |
| q19 | 36.6 | 13.7 | 2.67x | yes | 1 / 1 |
| q20 | 42.6 | 13.4 | 3.18x | yes | 9 / 9 |
| q21 | 61.8 | 19.9 | 3.10x | yes | 47 / 47 |
| q22 | 36.6 | 16.7 | 2.19x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 31.9 | 26.3 | 1.21x | yes | 4 / 4 |
| q02 | 109.0 | 32.5 | 3.36x | yes | 100 / 100 |
| q03 | 112.1 | 34.5 | 3.25x | yes | 10 / 10 |
| q04 | 29.9 | 21.6 | 1.38x | yes | 5 / 5 |
| q05 | 146.1 | 46.4 | 3.15x | yes | 5 / 5 |
| q06 | 12.2 | 7.4 | 1.64x | yes | 1 / 1 |
| q07 | 156.5 | 45.9 | 3.41x | yes | 4 / 4 |
| q08 | 166.4 | 52.1 | 3.19x | yes | 2 / 2 |
| q09 | 239.6 | 105.8 | 2.26x | yes | 175 / 175 |
| q10 | 146.4 | 73.7 | 1.99x | yes | 20 / 20 |
| q11 | 53.3 | 15.4 | 3.45x | yes | 1048 / 1048 |
| q12 | 25.8 | 17.9 | 1.44x | yes | 2 / 2 |
| q13 | 214.0 | 104.8 | 2.04x | yes | 42 / 42 |
| q14 | 99.5 | 49.5 | 2.01x | yes | 1 / 1 |
| q15 | 48.8 | 20.8 | 2.34x | yes | 1 / 1 |
| q16 | 102.4 | 61.5 | 1.67x | yes | 18314 / 18314 |
| q17 | 49.7 | 26.6 | 1.87x | yes | 1 / 1 |
| q18 | 134.7 | 93.7 | 1.44x | yes | 57 / 57 |
| q19 | 71.2 | 62.3 | 1.14x | yes | 1 / 1 |
| q20 | 93.8 | 34.8 | 2.70x | yes | 186 / 186 |
| q21 | 153.9 | 143.0 | 1.08x | yes | 100 / 100 |
| q22 | 89.3 | 58.5 | 1.53x | yes | 7 / 7 |

