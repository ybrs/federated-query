# TPC-DS federated benchmark report

Commit: `e90c9bd` - handoff: TPC-DS SF1 - PASS 99 | 0 | 0 holds, geomean 1.86x; gaps COMPRESS at scale; q64 schema-reconcile fix; real perf backlog is q39/q05/q78/q04  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 17:39
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 10, PostgreSQL + DuckDB split, per-query timeout 300.0s, memory cap 40000 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 3 | PASS 3 | MISMATCH 0 | ERROR 0 | cross-source 3

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q18 | 633.9 | 466.2 | 1.36x | PASS | cross | 100 / 100 | rows and values match |
| q65 | 1704.0 | 541.8 | 3.15x | PASS | cross | 100 / 100 | rows and values match |
| q71 | 214.7 | 155.3 | 1.38x | PASS | cross | 9655 / 9655 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 2552.7 | 1163.2 | 2.19x | 1.81x | 3 |
