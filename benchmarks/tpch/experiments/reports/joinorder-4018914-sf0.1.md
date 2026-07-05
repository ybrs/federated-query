# Join-order experiment (fedpgduck, SF0.1)

Commit: `4018914`  Generated: 2026-07-05 12:20

Each offender run two ways through the fair PostgreSQL + DuckDB engine: as written (user FROM order) and with joins hand-reordered into a good left-deep order. Median of three warm runs; both variants checked against the DuckDB oracle. Ratio is ours/DuckDB.

| Query | Orig (ms) | Reordered (ms) | Speedup | DuckDB (ms) | Orig/Duck | Reord/Duck | Correct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q05 | 318.0 | 322.8 | 1.0x | 15.2 | 20.91x | 21.23x | yes |
| q07 | 346.2 | 327.2 | 1.1x | 13.6 | 25.43x | 24.03x | yes |
| q08 | 357.7 | 150.3 | 2.4x | 22.1 | 16.20x | 6.81x | yes |
| q09 | 664.0 | 380.6 | 1.7x | 23.8 | 27.89x | 15.99x | yes |

