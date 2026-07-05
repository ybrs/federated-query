# Join-order experiment (fedpgduck, SF1)

Commit: `12609d3`  Generated: 2026-07-05 14:56

Each offender run two ways through the fair PostgreSQL + DuckDB engine: as written (user FROM order) and with joins hand-reordered into a good left-deep order. Median of three warm runs; both variants checked against the DuckDB oracle. Ratio is ours/DuckDB.

| Query | Orig (ms) | Reordered (ms) | Speedup | DuckDB (ms) | Orig/Duck | Reord/Duck | Correct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q05 | 2622.5 | 2749.5 | 1.0x | 51.0 | 51.46x | 53.95x | yes |
| q07 | 1247.3 | 1246.2 | 1.0x | 41.1 | 30.36x | 30.33x | yes |
| q08 | 531.3 | 568.4 | 0.9x | 55.7 | 9.53x | 10.20x | yes |
| q09 | 4805.5 | 4843.5 | 1.0x | 99.2 | 48.43x | 48.82x | yes |

