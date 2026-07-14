# Synthetic event-analytics benchmark report (small)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~1000000 events over ~50000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_small.duckdb.
Build: CREATE 697ms, REFRESH 0ms (scan + global (entity, timestamp, tiebreak) sort + chunk write).
Warm runs per analysis: 3. Generated: 2026-07-14 02:40.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 69.7 | 69.1 | 39.8 | 1.74x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 63.1 | 63.3 | 13.2 | 4.81x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 97.5 | 100.9 | 8.2 | 12.34x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 100.6 | 100.5 | 23.0 | 4.36x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 106.8 | 105.4 | 224.4 | 0.47x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 89.1 | 89.4 | 240.3 | 0.37x | AGREE | 20 | - |

Analysis wall total: 2.8s.
