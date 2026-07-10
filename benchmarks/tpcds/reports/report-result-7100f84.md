# TPC-DS federated benchmark report

Commit: `7100f84` - eager-agg tests: the fixture's customer exceeds the ship budget (the rescue gate)  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-10 17:01
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 120.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: adversarial

[adversarial] Total 99 | PASS 99 | MISMATCH 0 | ERROR 0 | cross-source 95

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 35.0 | 31.7 | 1.10x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 82.9 | 54.7 | 1.52x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 18.1 | 25.5 | 0.71x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 146.4 | 94.6 | 1.55x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 120.4 | 75.0 | 1.61x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 51.1 | 52.5 | 0.97x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 33.4 | 49.8 | 0.67x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 60.7 | 29.9 | 2.03x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 25.7 | 7.9 | 3.23x | PASS | single | 1 / 1 | rows and values match |
| q10 | 71.6 | 107.3 | 0.67x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 83.7 | 53.2 | 1.57x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 31.6 | 21.1 | 1.50x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 60.0 | 52.1 | 1.15x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 201.8 | 108.9 | 1.85x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 42.8 | 41.5 | 1.03x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 97.0 | 70.1 | 1.38x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 55.7 | 65.6 | 0.85x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 75.3 | 139.3 | 0.54x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 22.4 | 32.7 | 0.69x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 41.8 | 39.9 | 1.05x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 46.6 | 75.4 | 0.62x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 42.7 | 80.6 | 0.53x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 191.2 | 112.2 | 1.70x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 37.6 | 15.9 | 2.36x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 50.5 | 70.8 | 0.71x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 53.1 | 109.6 | 0.48x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 46.5 | 54.4 | 0.85x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 31.4 | 10.6 | 2.95x | PASS | single | 1 / 1 | rows and values match |
| q29 | 126.6 | 63.8 | 1.98x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 47.6 | 30.9 | 1.54x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 51.7 | 33.9 | 1.52x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 15.3 | 49.7 | 0.31x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 67.5 | 57.1 | 1.18x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 32.7 | 34.7 | 0.94x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 75.3 | 104.4 | 0.72x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 34.3 | 23.4 | 1.47x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 231.3 | 89.8 | 2.58x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 50.1 | 51.7 | 0.97x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 143.9 | 84.3 | 1.71x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 41.7 | 41.3 | 1.01x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 20.8 | 4.6 | 4.56x | PASS | single | 0 / 0 | rows and values match |
| q42 | 16.9 | 15.9 | 1.06x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 24.2 | 32.3 | 0.75x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 31.8 | 2.8 | 11.22x | PASS | single | 0 / 0 | rows and values match |
| q45 | 56.6 | 28.7 | 1.97x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 38.3 | 28.3 | 1.35x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 57.9 | 41.2 | 1.41x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 55.9 | 45.8 | 1.22x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 85.4 | 78.3 | 1.09x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 45.6 | 47.4 | 0.96x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 60.1 | 48.1 | 1.25x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 18.6 | 16.7 | 1.11x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 38.2 | 23.1 | 1.66x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 87.1 | 55.8 | 1.56x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 16.6 | 16.3 | 1.02x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 69.0 | 55.5 | 1.24x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 80.1 | 58.1 | 1.38x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 140.2 | 84.0 | 1.67x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 73.6 | 78.8 | 0.93x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 69.8 | 53.8 | 1.30x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 37.7 | 33.6 | 1.12x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 39.6 | 24.2 | 1.64x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 36.5 | 32.5 | 1.12x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 129.0 | 97.0 | 1.33x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 39.2 | 26.3 | 1.49x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 149.4 | 110.5 | 1.35x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 58.5 | 71.8 | 0.82x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 42.7 | 25.1 | 1.70x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 71.4 | 99.3 | 0.72x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 45.5 | 30.1 | 1.51x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 72.0 | 83.2 | 0.87x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 130.8 | 151.2 | 0.87x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 29.5 | 29.8 | 0.99x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 67.6 | 44.6 | 1.52x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 163.9 | 77.3 | 2.12x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 55.6 | 47.9 | 1.16x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 58.5 | 72.9 | 0.80x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 110.9 | 75.0 | 1.48x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 33.2 | 27.6 | 1.20x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 117.0 | 81.2 | 1.44x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 39.0 | 27.7 | 1.41x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 18.9 | 69.2 | 0.27x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 76.0 | 72.8 | 1.04x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 42.1 | 42.5 | 0.99x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 78.2 | - | - | PASS | cross | 0 / 0 | rows and values match |
| q86 | 29.9 | 21.4 | 1.39x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 51.3 | 55.2 | 0.93x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 89.4 | 225.8 | 0.40x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 42.9 | 32.1 | 1.34x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 31.6 | 39.2 | 0.81x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 50.0 | 58.6 | 0.85x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 23.2 | 20.8 | 1.11x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 28.1 | 11.8 | 2.37x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 34.3 | 16.6 | 2.06x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 31.2 | 34.3 | 0.91x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 14.4 | 23.1 | 0.62x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 37.4 | 42.7 | 0.88x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 26.6 | 20.0 | 1.33x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 71.4 | 39.2 | 1.82x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [adversarial]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 6087.7 | 5257.6 | 1.16x | 1.19x | 98 |
