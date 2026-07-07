# TPC-H benchmark report

Commit: `9e9c44a` - optimizer: keep ORDER BY sort above a filter, not below it (q59)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 19:38
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
| fedpgduck | 0.1 | 22/22 | 931.6 | 291.2 | 3.20x | 22 |
| fedpgduck | 1 | 22/22 | 2143.1 | 1088.3 | 1.97x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.1 | 6.5 | 2.17x | yes | 4 / 4 |
| q02 | 65.2 | 13.3 | 4.90x | yes | 44 / 44 |
| q03 | 40.3 | 10.2 | 3.96x | yes | 10 / 10 |
| q04 | 16.1 | 8.2 | 1.95x | yes | 5 / 5 |
| q05 | 59.5 | 12.0 | 4.95x | yes | 5 / 5 |
| q06 | 7.7 | 2.2 | 3.45x | yes | 1 / 1 |
| q07 | 67.8 | 14.1 | 4.81x | yes | 4 / 4 |
| q08 | 77.8 | 16.4 | 4.75x | yes | 2 / 2 |
| q09 | 80.3 | 24.7 | 3.25x | yes | 175 / 175 |
| q10 | 49.5 | 19.9 | 2.49x | yes | 20 / 20 |
| q11 | 41.1 | 11.4 | 3.61x | yes | 2541 / 2541 |
| q12 | 15.9 | 5.8 | 2.77x | yes | 2 / 2 |
| q13 | 43.1 | 23.7 | 1.82x | yes | 37 / 37 |
| q14 | 22.6 | 10.6 | 2.14x | yes | 1 / 1 |
| q15 | 29.4 | 5.0 | 5.93x | yes | 1 / 1 |
| q16 | 34.3 | 13.7 | 2.51x | yes | 2762 / 2762 |
| q17 | 32.1 | 9.4 | 3.40x | yes | 1 / 1 |
| q18 | 50.8 | 17.7 | 2.88x | yes | 5 / 5 |
| q19 | 34.8 | 15.2 | 2.29x | yes | 1 / 1 |
| q20 | 45.1 | 12.2 | 3.70x | yes | 9 / 9 |
| q21 | 66.8 | 20.5 | 3.25x | yes | 47 / 47 |
| q22 | 37.2 | 18.4 | 2.02x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 34.7 | 26.0 | 1.34x | yes | 4 / 4 |
| q02 | 109.7 | 32.1 | 3.41x | yes | 100 / 100 |
| q03 | 76.4 | 33.7 | 2.27x | yes | 10 / 10 |
| q04 | 31.3 | 21.9 | 1.43x | yes | 5 / 5 |
| q05 | 143.1 | 42.5 | 3.37x | yes | 5 / 5 |
| q06 | 11.5 | 7.0 | 1.65x | yes | 1 / 1 |
| q07 | 112.8 | 46.8 | 2.41x | yes | 4 / 4 |
| q08 | 154.8 | 59.6 | 2.60x | yes | 2 / 2 |
| q09 | 233.9 | 95.3 | 2.45x | yes | 175 / 175 |
| q10 | 158.5 | 71.9 | 2.21x | yes | 20 / 20 |
| q11 | 52.4 | 14.9 | 3.51x | yes | 1048 / 1048 |
| q12 | 23.9 | 17.7 | 1.35x | yes | 2 / 2 |
| q13 | 197.7 | 98.5 | 2.01x | yes | 42 / 42 |
| q14 | 52.4 | 45.2 | 1.16x | yes | 1 / 1 |
| q15 | 48.8 | 17.3 | 2.82x | yes | 1 / 1 |
| q16 | 102.8 | 56.9 | 1.81x | yes | 18314 / 18314 |
| q17 | 53.2 | 27.8 | 1.91x | yes | 1 / 1 |
| q18 | 143.8 | 89.1 | 1.61x | yes | 57 / 57 |
| q19 | 66.9 | 62.2 | 1.07x | yes | 1 / 1 |
| q20 | 90.0 | 33.6 | 2.68x | yes | 186 / 186 |
| q21 | 149.5 | 127.4 | 1.17x | yes | 100 / 100 |
| q22 | 95.0 | 60.9 | 1.56x | yes | 7 / 7 |

