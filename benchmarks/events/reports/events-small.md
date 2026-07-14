# Synthetic event-analytics benchmark report (small)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~1000000 events over ~50000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_small.duckdb.
Build: CREATE 884ms, REFRESH 0ms (scan + global (entity, timestamp, tiebreak) sort + chunk write + derived sidecars).
Derived sidecars on disk: bitmaps 551.3 KB, rowindex 1.4 MB, segment 46.8 KB. Peak run RSS 492 MB.
Warm runs per analysis: 3. Generated: 2026-07-14 11:41.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 66.1 | 65.6 | 39.8 | 1.65x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 47.2 | 46.3 | 13.2 | 3.52x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 0.3 | 0.2 | 8.2 | 0.02x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 0.2 | 0.2 | 23.0 | 0.01x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 111.2 | 105.2 | 224.4 | 0.47x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 89.6 | 89.3 | 240.3 | 0.37x | AGREE | 20 | - |

Analysis wall total: 2.1s.
