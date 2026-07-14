# Synthetic event-analytics benchmark report (medium)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~100000000 events over ~500000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_medium.duckdb.
Build: CREATE 200851ms, REFRESH 0ms (scan + global (entity, timestamp, tiebreak) sort + chunk write).
Warm runs per analysis: 2. Generated: 2026-07-14 02:56.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 6612.4 | 6658.3 | 38804.6 | 0.17x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 6085.7 | 6174.6 | 1759.7 | 3.51x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 8866.5 | 8886.2 | 342.4 | 25.95x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 9007.7 | 8931.3 | 2003.0 | 4.46x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 6963.5 | 6963.8 | 20061.1 | 0.35x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 6730.3 | 6897.8 | 21778.3 | 0.32x | AGREE | 20 | - |

Analysis wall total: 333.7s.
