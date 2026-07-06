# TPC-H benchmark report

Commit: `618e58d` - cost: temporal range interpolation + interval pairing for conjunctions  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 15:20
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
| fedpgduck | 0.1 | 22/22 | 952.3 | 308.6 | 3.09x | 22 |
| fedpgduck | 1 | 22/22 | 2292.4 | 1105.1 | 2.07x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF0.1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 14.3 | 6.6 | 2.18x | yes | 4 / 4 |
| q02 | 66.1 | 13.2 | 5.00x | yes | 44 / 44 |
| q03 | 37.0 | 11.2 | 3.30x | yes | 10 / 10 |
| q04 | 17.1 | 8.3 | 2.05x | yes | 5 / 5 |
| q05 | 59.7 | 17.9 | 3.33x | yes | 5 / 5 |
| q06 | 8.5 | 2.2 | 3.95x | yes | 1 / 1 |
| q07 | 71.8 | 13.8 | 5.19x | yes | 4 / 4 |
| q08 | 78.9 | 16.3 | 4.84x | yes | 2 / 2 |
| q09 | 80.3 | 26.9 | 2.98x | yes | 175 / 175 |
| q10 | 52.9 | 29.7 | 1.78x | yes | 20 / 20 |
| q11 | 40.1 | 11.2 | 3.59x | yes | 2541 / 2541 |
| q12 | 15.5 | 6.1 | 2.51x | yes | 2 / 2 |
| q13 | 40.8 | 24.0 | 1.70x | yes | 37 / 37 |
| q14 | 28.3 | 9.0 | 3.15x | yes | 1 / 1 |
| q15 | 31.3 | 5.0 | 6.27x | yes | 1 / 1 |
| q16 | 34.8 | 14.9 | 2.34x | yes | 2762 / 2762 |
| q17 | 26.9 | 10.7 | 2.52x | yes | 1 / 1 |
| q18 | 61.4 | 18.0 | 3.40x | yes | 5 / 5 |
| q19 | 40.0 | 15.0 | 2.67x | yes | 1 / 1 |
| q20 | 44.3 | 11.3 | 3.92x | yes | 9 / 9 |
| q21 | 63.9 | 20.4 | 3.13x | yes | 47 / 47 |
| q22 | 38.4 | 16.7 | 2.30x | yes | 7 / 7 |

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF1

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 33.1 | 25.6 | 1.29x | yes | 4 / 4 |
| q02 | 112.4 | 32.8 | 3.42x | yes | 100 / 100 |
| q03 | 108.8 | 35.6 | 3.05x | yes | 10 / 10 |
| q04 | 29.8 | 21.5 | 1.39x | yes | 5 / 5 |
| q05 | 141.8 | 44.1 | 3.22x | yes | 5 / 5 |
| q06 | 11.7 | 7.4 | 1.58x | yes | 1 / 1 |
| q07 | 157.8 | 47.7 | 3.31x | yes | 4 / 4 |
| q08 | 162.9 | 57.9 | 2.82x | yes | 2 / 2 |
| q09 | 244.0 | 93.8 | 2.60x | yes | 175 / 175 |
| q10 | 154.8 | 76.6 | 2.02x | yes | 20 / 20 |
| q11 | 63.6 | 17.2 | 3.69x | yes | 1048 / 1048 |
| q12 | 32.4 | 20.1 | 1.61x | yes | 2 / 2 |
| q13 | 217.3 | 97.5 | 2.23x | yes | 42 / 42 |
| q14 | 81.3 | 43.0 | 1.89x | yes | 1 / 1 |
| q15 | 49.9 | 16.7 | 2.98x | yes | 1 / 1 |
| q16 | 105.4 | 56.5 | 1.87x | yes | 18314 / 18314 |
| q17 | 52.2 | 26.8 | 1.95x | yes | 1 / 1 |
| q18 | 135.8 | 95.4 | 1.42x | yes | 57 / 57 |
| q19 | 71.8 | 65.1 | 1.10x | yes | 1 / 1 |
| q20 | 95.3 | 33.4 | 2.85x | yes | 186 / 186 |
| q21 | 138.1 | 126.0 | 1.10x | yes | 100 / 100 |
| q22 | 92.2 | 64.2 | 1.44x | yes | 7 / 7 |

