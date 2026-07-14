# Synthetic event-analytics benchmark report (small)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~1000000 events over ~50000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_small.duckdb.
Build: CREATE 614ms, REFRESH 0ms (scan + global (entity, timestamp, tiebreak) sort + chunk write + derived sidecars).
Derived sidecars on disk: bitmaps 551.3 KB, rowindex 1.4 MB, segment 46.8 KB. Peak run RSS 434 MB.
Warm runs per analysis: 1. Generated: 2026-07-14 21:38.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 63.6 | 63.1 | 39.8 | 1.59x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 46.6 | 47.9 | 13.2 | 3.64x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 0.3 | 0.2 | 8.2 | 0.02x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 0.2 | 0.2 | 23.0 | 0.01x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 112.0 | 110.5 | 224.4 | 0.49x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 93.4 | 93.9 | 240.3 | 0.39x | AGREE | 20 | - |

Analysis wall total: 1.3s.
