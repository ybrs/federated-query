# TPC-H benchmark report

Commit: `88e7e2a` - optimizer: parallel pg source scans + cost-based reduction usefulness gate  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 13:43
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
| fedpgduck | 0.1 | 22/22 | 936.8 | 289.7 | 3.23x | 22 |
| fedpgduck | 1 | 22/22 | 2291.6 | 1126.6 | 2.03x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 15.3 | 6.9 | 2.20x | yes | 4 / 4 |
| q02 | 62.3 | 11.3 | 5.51x | yes | 44 / 44 |
| q03 | 39.0 | 10.0 | 3.88x | yes | 10 / 10 |
| q04 | 16.4 | 8.4 | 1.94x | yes | 5 / 5 |
| q05 | 54.0 | 12.4 | 4.34x | yes | 5 / 5 |
| q06 | 7.4 | 2.1 | 3.57x | yes | 1 / 1 |
| q07 | 67.2 | 13.9 | 4.84x | yes | 4 / 4 |
| q08 | 73.6 | 17.5 | 4.22x | yes | 2 / 2 |
| q09 | 78.5 | 24.0 | 3.28x | yes | 175 / 175 |
| q10 | 54.7 | 19.7 | 2.77x | yes | 20 / 20 |
| q11 | 39.9 | 12.2 | 3.27x | yes | 2541 / 2541 |
| q12 | 14.6 | 7.2 | 2.03x | yes | 2 / 2 |
| q13 | 40.5 | 23.0 | 1.76x | yes | 37 / 37 |
| q14 | 27.0 | 10.1 | 2.67x | yes | 1 / 1 |
| q15 | 31.4 | 4.8 | 6.60x | yes | 1 / 1 |
| q16 | 35.0 | 15.6 | 2.25x | yes | 2762 / 2762 |
| q17 | 32.1 | 9.5 | 3.39x | yes | 1 / 1 |
| q18 | 64.1 | 18.8 | 3.42x | yes | 5 / 5 |
| q19 | 37.6 | 14.0 | 2.68x | yes | 1 / 1 |
| q20 | 44.3 | 11.1 | 4.00x | yes | 9 / 9 |
| q21 | 65.5 | 21.7 | 3.02x | yes | 47 / 47 |
| q22 | 36.2 | 15.6 | 2.32x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 32.4 | 24.9 | 1.30x | yes | 4 / 4 |
| q02 | 113.0 | 32.2 | 3.51x | yes | 100 / 100 |
| q03 | 108.3 | 34.2 | 3.16x | yes | 10 / 10 |
| q04 | 30.0 | 21.2 | 1.42x | yes | 5 / 5 |
| q05 | 142.7 | 45.4 | 3.14x | yes | 5 / 5 |
| q06 | 11.6 | 7.0 | 1.64x | yes | 1 / 1 |
| q07 | 153.4 | 47.4 | 3.24x | yes | 4 / 4 |
| q08 | 157.1 | 55.2 | 2.85x | yes | 2 / 2 |
| q09 | 254.8 | 107.4 | 2.37x | yes | 175 / 175 |
| q10 | 131.0 | 75.5 | 1.74x | yes | 20 / 20 |
| q11 | 52.3 | 15.6 | 3.35x | yes | 1048 / 1048 |
| q12 | 24.9 | 16.8 | 1.48x | yes | 2 / 2 |
| q13 | 222.4 | 91.9 | 2.42x | yes | 42 / 42 |
| q14 | 81.8 | 41.7 | 1.96x | yes | 1 / 1 |
| q15 | 50.5 | 18.4 | 2.75x | yes | 1 / 1 |
| q16 | 117.5 | 57.3 | 2.05x | yes | 18314 / 18314 |
| q17 | 53.4 | 30.5 | 1.75x | yes | 1 / 1 |
| q18 | 135.8 | 92.3 | 1.47x | yes | 57 / 57 |
| q19 | 70.1 | 66.3 | 1.06x | yes | 1 / 1 |
| q20 | 96.4 | 35.9 | 2.68x | yes | 186 / 186 |
| q21 | 158.0 | 151.0 | 1.05x | yes | 100 / 100 |
| q22 | 94.1 | 58.5 | 1.61x | yes | 7 / 7 |

