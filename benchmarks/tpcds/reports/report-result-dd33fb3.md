# TPC-DS federated benchmark report

Commit: `dd33fb3` - plan: fragment fusion via lazy bindings - materialize only at pipeline breakers  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 19:13
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 10, PostgreSQL + DuckDB split, per-query timeout 600.0s, memory cap 40000 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 4 | PASS 2 | MISMATCH 0 | ERROR 2 | cross-source 4

### Failure clusters

### Memory limit (killed) (1)
Queries: q64

- Killed: worker exited with code 137 (likely memory limit)

### Other (1)
Queries: q78

- RuntimeError: Resources exhausted: Failed to allocate additional 129.0 MB for HashJoinInput[2] with 3.9 GB already allocated for this reservation - 51.5 MB remain available for the total memory pool: fair(pool_size: 32.0 GB)

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q23 | 6745.2 | 2866.5 | 2.35x | PASS | cross | 100 / 100 | rows and values match |
| q64 | - | - | - | ERROR | cross | - | Killed: worker exited with code 137 (likely memory limit) |
| q67 | 3761.5 | 5968.9 | 0.63x | PASS | cross | 100 / 100 | rows and values match |
| q78 | - | - | - | ERROR | cross | - | RuntimeError: Resources exhausted: Failed to allocate additional 129.0 MB for HashJoinInput[2] with 3.9 GB already allocated for this reservation - 51.5 MB remain available for the total memory pool: fair(pool_size: 32.0 GB) |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 10506.7 | 8835.4 | 1.19x | 1.22x | 2 |
