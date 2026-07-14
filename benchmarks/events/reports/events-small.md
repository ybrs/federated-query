# Synthetic event-analytics benchmark report (small)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~1000000 events over ~50000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_small.duckdb.
Build: CREATE 810ms, REFRESH 0ms (scan + global (entity, timestamp, tiebreak) sort + chunk write + derived sidecars).
Derived sidecars on disk: bitmaps 551.3 KB, segment 46.8 KB. Peak run RSS 492 MB.
Warm runs per analysis: 3. Generated: 2026-07-14 05:52.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 71.8 | 69.5 | 39.8 | 1.75x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 56.4 | 52.8 | 13.2 | 4.01x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 0.3 | 0.2 | 8.2 | 0.02x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 0.2 | 0.2 | 23.0 | 0.01x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 112.0 | 103.3 | 224.4 | 0.46x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 89.9 | 91.8 | 240.3 | 0.38x | AGREE | 20 | - |

Analysis wall total: 2.1s.
