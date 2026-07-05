# Join-order experiment (fedpgduck, SF1)

Commit: `70c8961`  Generated: 2026-07-05 16:03

Each offender run two ways through the fair PostgreSQL + DuckDB engine: as written (user FROM order) and with joins hand-reordered into a good left-deep order. Median of three warm runs; both variants checked against the DuckDB oracle. Ratio is ours/DuckDB.

| Query | Orig (ms) | Reordered (ms) | Speedup | DuckDB (ms) | Orig/Duck | Reord/Duck | Correct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q05 | 2621.6 | 2769.8 | 0.9x | 48.4 | 54.13x | 57.19x | yes |
| q07 | 2688.6 | 2704.3 | 1.0x | 42.9 | 62.66x | 63.02x | yes |
| q08 | 546.1 | 533.6 | 1.0x | 50.2 | 10.88x | 10.63x | yes |
| q09 | 4551.9 | 4571.7 | 1.0x | 93.0 | 48.93x | 49.14x | yes |

