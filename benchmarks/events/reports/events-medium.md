# Synthetic event-analytics benchmark report (medium)

Engine: fedq-py (Rust), driven via fedq.Runtime over an EVENT VIEW.
Dataset: ~100000000 events over ~500000 entities, 20 event types, sessionized across 30 days.
Baseline: pure-DuckDB SQL over the same file, cached once by save-refs in references_medium.duckdb.
Build: CREATE 68024ms, REFRESH 33ms (scan + global (entity, timestamp, tiebreak) sort + chunk write + derived sidecars).
Derived sidecars on disk: bitmaps 5.1 MB, rowindex 137.8 MB, segment 46.8 KB. Peak run RSS 26830 MB.
Warm runs per analysis: 1. Generated: 2026-07-14 21:23.

Ratio is engine (warm when measured, else cold) / DuckDB baseline; below 1.0x the engine is faster. Match cross-checks the engine result against the baseline signature (AGREE = identical counts).

| Analysis | Cold ms | Warm ms | DuckDB ms | Ratio | Match | Rows | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| FUNNEL common (page_view->view_item->add_to_cart, 7d) | 6365.9 | 6209.2 | 38804.6 | 0.16x | AGREE | 3 | - |
| FUNNEL selective (signup->begin_checkout->purchase, 7d) | 4611.7 | 4608.1 | 1759.7 | 2.62x | AGREE | 3 | - |
| SEGMENT MEASURE EVENTS BY DAY | 0.3 | 0.2 | 342.4 | 0.00x | AGREE | 31 | - |
| SEGMENT MEASURE ENTITIES BY DAY | 0.2 | 0.2 | 2003.0 | 0.00x | AGREE | 31 | - |
| PATHS MAX DEPTH 5 TOP 20 | 7277.9 | 7317.0 | 20061.1 | 0.36x | AGREE | 20 | - |
| PATHS STARTING AT 'page_view' MAX DEPTH 5 TOP 20 | 6973.8 | 6871.0 | 21778.3 | 0.32x | AGREE | 20 | - |

Analysis wall total: 118.3s.
