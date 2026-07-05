# Join-order experiment (fedpgduck, SF1)

Commit: `4018914`  Generated: 2026-07-05 12:19

Each offender run two ways through the fair PostgreSQL + DuckDB engine: as written (user FROM order) and with joins hand-reordered into a good left-deep order. Median of three warm runs; both variants checked against the DuckDB oracle. Ratio is ours/DuckDB.

| Query | Orig (ms) | Reordered (ms) | Speedup | DuckDB (ms) | Orig/Duck | Reord/Duck | Correct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q05 | 2762.6 | 2556.2 | 1.1x | 58.6 | 47.15x | 43.63x | yes |
| q07 | 2867.0 | 2634.6 | 1.1x | 38.7 | 74.14x | 68.13x | yes |
| q08 | 5129.6 | 548.4 | 9.4x | 62.3 | 82.33x | 8.80x | yes |
| q09 | 19438.6 | 3338.3 | 5.8x | 108.0 | 180.03x | 30.92x | yes |

