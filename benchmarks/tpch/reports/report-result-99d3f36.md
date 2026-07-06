# TPC-H benchmark report

Commit: `99d3f36` - executor: push a reduction's key filter inside an aggregate-subquery base  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-06 18:25
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.
Scale factors: SF10 (? lineitem).

## Methodology

Timing is the median of three warm runs per query; correctness is a differential row-by-row check against DuckDB on the same data (numbers rounded to 2 decimals). Each query runs in its own subprocess with an RSS cap, so a blow-up is a KILLED row, not a lost run. Ratio is ours/DuckDB (higher = we are slower).

- **single** - Single source (pure engine, Parquet).
- **fedparquet** - Federated, 2 Parquet sources (DuckDB monolithic - unfair); DuckDB reads all files as one, so this overstates our cost.
- **fedpgduck** - Federated, PostgreSQL + DuckDB (both federate - fair); the DuckDB oracle federates the same split via its postgres connector. This is the honest federated number.

## Summary

Totals cover only queries that produced a measurement; TIMEOUT / KILLED / ERROR queries are excluded from the ms totals but counted as not-correct. `measured` is how many of the 22 timed cleanly.

| Cell | SF | Correct | Ours (ms) | DuckDB (ms) | Ratio | Measured |
| --- | --- | --- | --- | --- | --- | --- |
| fedpgduck | 10 | 22/22 | 16160.2 | 8363.9 | 1.93x | 22 |

## Issues

No failures: every query matched DuckDB in every cell.

## Per-query detail

### Federated, PostgreSQL + DuckDB (both federate - fair) - SF10

| Query | Ours (ms) | DuckDB (ms) | Ratio | Correct | Rows o/d |
| --- | --- | --- | --- | --- | --- |
| q01 | 220.2 | 221.5 | 0.99x | yes | 4 / 4 |
| q02 | 446.5 | 135.4 | 3.30x | yes | 100 / 100 |
| q03 | 317.5 | 320.5 | 0.99x | yes | 10 / 10 |
| q04 | 207.3 | 204.7 | 1.01x | yes | 5 / 5 |
| q05 | 1434.8 | 333.7 | 4.30x | yes | 5 / 5 |
| q06 | 58.5 | 60.2 | 0.97x | yes | 1 / 1 |
| q07 | 727.7 | 269.3 | 2.70x | yes | 4 / 4 |
| q08 | 1036.4 | 360.8 | 2.87x | yes | 2 / 2 |
| q09 | 2650.6 | 1167.6 | 2.27x | yes | 175 / 175 |
| q10 | 1147.3 | 525.7 | 2.18x | yes | 20 / 20 |
| q11 | 148.5 | 63.2 | 2.35x | yes | 0 / 0 |
| q12 | 152.9 | 142.6 | 1.07x | yes | 2 / 2 |
| q13 | 2439.5 | 1108.5 | 2.20x | yes | 45 / 45 |
| q14 | 377.5 | 270.8 | 1.39x | yes | 1 / 1 |
| q15 | 262.1 | 137.3 | 1.91x | yes | 1 / 1 |
| q16 | 738.3 | 287.5 | 2.57x | yes | 27840 / 27840 |
| q17 | 291.2 | 244.3 | 1.19x | yes | 1 / 1 |
| q18 | 1138.7 | 731.7 | 1.56x | yes | 100 / 100 |
| q19 | 355.1 | 426.8 | 0.83x | yes | 1 / 1 |
| q20 | 605.1 | 190.5 | 3.18x | yes | 1804 / 1804 |
| q21 | 869.5 | 799.3 | 1.09x | yes | 100 / 100 |
| q22 | 535.1 | 362.0 | 1.48x | yes | 7 / 7 |

