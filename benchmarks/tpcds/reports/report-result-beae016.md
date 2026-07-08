# TPC-DS federated benchmark report

Commit: `beae016` - rust_ir: collect build keys from the ORIGINATING base relation when traceable  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 19:31
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 10, PostgreSQL + DuckDB split, per-query timeout 900.0s, memory cap 40000 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 1 | PASS 1 | MISMATCH 0 | ERROR 0 | cross-source 1

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q78 | 13720.3 | 1259.9 | 10.89x | PASS | cross | 100 / 100 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 13720.3 | 1259.9 | 10.89x | 10.89x | 1 |
