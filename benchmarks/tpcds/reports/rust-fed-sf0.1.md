# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Baseline: pure DuckDB over the fact+dim file (every table local).
Generated: 2026-07-12 21:44

Tally: 2 ok | 0 wrong | 0 error   (total 2 queries, 0.2s)
Timing: ours 0.2s  duckdb 0.1s  ->  total 2.71x  geomean 2.58x  (2 OK queries measured)

## Non-OK queries grouped by reason

## Timing summary (OK queries with a DuckDB baseline)

| Ours (s) | DuckDB (s) | Total ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 0.2 | 0.1 | 2.71x | 2.58x | 2 |

## Per-query matrix

| Query | Status | Cold ms | Warm ms | DuckDB ms | Ratio | Rows engine/truth | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q05 | OK | 111.8 | - | 34.2 | 3.27x | 100 / 100 | rows and values match |
| q70 | OK | 59.0 | - | 28.9 | 2.04x | 3 / 3 | rows and values match |
