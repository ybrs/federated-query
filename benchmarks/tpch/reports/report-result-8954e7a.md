# TPC-H benchmark report

Commit: `8954e7a` - optimizer: push single-sided condition conjuncts below outer/existential joins  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 15:33
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
| fedpgduck | 0.1 | 22/22 | 996.5 | 298.3 | 3.34x | 22 |
| fedpgduck | 1 | 22/22 | 2260.8 | 1109.4 | 2.04x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.0 | 6.2 | 2.27x | yes | 4 / 4 |
| q02 | 64.6 | 12.7 | 5.09x | yes | 44 / 44 |
| q03 | 37.2 | 10.6 | 3.51x | yes | 10 / 10 |
| q04 | 16.7 | 8.8 | 1.89x | yes | 5 / 5 |
| q05 | 58.9 | 16.0 | 3.68x | yes | 5 / 5 |
| q06 | 8.5 | 2.4 | 3.49x | yes | 1 / 1 |
| q07 | 80.2 | 16.8 | 4.76x | yes | 4 / 4 |
| q08 | 86.6 | 18.1 | 4.78x | yes | 2 / 2 |
| q09 | 83.9 | 25.8 | 3.25x | yes | 175 / 175 |
| q10 | 51.1 | 22.0 | 2.33x | yes | 20 / 20 |
| q11 | 41.7 | 11.1 | 3.77x | yes | 2541 / 2541 |
| q12 | 15.9 | 6.0 | 2.63x | yes | 2 / 2 |
| q13 | 49.7 | 23.1 | 2.15x | yes | 37 / 37 |
| q14 | 27.9 | 9.3 | 3.01x | yes | 1 / 1 |
| q15 | 33.5 | 5.3 | 6.37x | yes | 1 / 1 |
| q16 | 37.2 | 13.1 | 2.83x | yes | 2762 / 2762 |
| q17 | 27.2 | 9.5 | 2.86x | yes | 1 / 1 |
| q18 | 66.7 | 17.4 | 3.84x | yes | 5 / 5 |
| q19 | 37.8 | 13.2 | 2.86x | yes | 1 / 1 |
| q20 | 45.5 | 11.4 | 3.99x | yes | 9 / 9 |
| q21 | 68.5 | 21.8 | 3.14x | yes | 47 / 47 |
| q22 | 43.1 | 17.6 | 2.44x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 33.6 | 27.0 | 1.25x | yes | 4 / 4 |
| q02 | 117.6 | 32.5 | 3.62x | yes | 100 / 100 |
| q03 | 70.9 | 32.4 | 2.19x | yes | 10 / 10 |
| q04 | 31.4 | 23.2 | 1.36x | yes | 5 / 5 |
| q05 | 141.5 | 46.3 | 3.06x | yes | 5 / 5 |
| q06 | 11.8 | 7.4 | 1.60x | yes | 1 / 1 |
| q07 | 160.7 | 45.2 | 3.56x | yes | 4 / 4 |
| q08 | 155.8 | 55.5 | 2.81x | yes | 2 / 2 |
| q09 | 247.4 | 94.4 | 2.62x | yes | 175 / 175 |
| q10 | 142.8 | 77.3 | 1.85x | yes | 20 / 20 |
| q11 | 53.3 | 15.3 | 3.47x | yes | 1048 / 1048 |
| q12 | 25.4 | 19.9 | 1.28x | yes | 2 / 2 |
| q13 | 218.7 | 104.3 | 2.10x | yes | 42 / 42 |
| q14 | 83.0 | 41.8 | 1.99x | yes | 1 / 1 |
| q15 | 49.9 | 17.4 | 2.86x | yes | 1 / 1 |
| q16 | 113.3 | 62.9 | 1.80x | yes | 18314 / 18314 |
| q17 | 50.7 | 27.4 | 1.85x | yes | 1 / 1 |
| q18 | 145.3 | 94.4 | 1.54x | yes | 57 / 57 |
| q19 | 72.6 | 61.9 | 1.17x | yes | 1 / 1 |
| q20 | 94.3 | 34.5 | 2.73x | yes | 186 / 186 |
| q21 | 147.6 | 126.2 | 1.17x | yes | 100 / 100 |
| q22 | 93.0 | 62.2 | 1.50x | yes | 7 / 7 |

