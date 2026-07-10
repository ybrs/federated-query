# TPC-DS federated benchmark report

Commit: `cc407f9` - eager-agg plan: drop the uniqueness gate - duplication commutes with the final merge  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 16:35
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 120.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 3 | PASS 3 | MISMATCH 0 | ERROR 0 | cross-source 3

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q04 | 130.1 | 68.6 | 1.90x | PASS | cross | 0 / 0 | rows and values match |
| q11 | 80.5 | 50.6 | 1.59x | PASS | cross | 4 / 4 | rows and values match |
| q74 | 62.9 | 44.0 | 1.43x | PASS | cross | 3 / 3 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 273.6 | 163.2 | 1.68x | 1.63x | 3 |
