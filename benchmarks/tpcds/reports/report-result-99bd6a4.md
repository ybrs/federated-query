# TPC-DS federated benchmark report

Commit: `99bd6a4` - tpcds harness: a PASS with no oracle timing must not crash the summary  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 20:33
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 10, PostgreSQL + DuckDB split, per-query timeout 300.0s, memory cap 40000 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 99 | MISMATCH 0 | ERROR 0 | cross-source 97

### Failure clusters

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 147.8 | 112.7 | 1.31x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 762.9 | 239.2 | 3.19x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 87.0 | 57.9 | 1.50x | PASS | cross | 100 / 100 | rows and values match |
| q04 | 15385.3 | 1302.7 | 11.81x | PASS | cross | 100 / 100 | rows and values match |
| q05 | 11009.4 | 161.1 | 68.36x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 3441.8 | 184.8 | 18.63x | PASS | cross | 51 / 51 | rows and values match |
| q07 | 262.4 | 224.6 | 1.17x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 300.1 | 144.3 | 2.08x | PASS | cross | 9 / 9 | rows and values match |
| q09 | 406.6 | 374.0 | 1.09x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 314.0 | 440.2 | 0.71x | PASS | cross | 100 / 100 | rows and values match |
| q11 | 5811.1 | 823.0 | 7.06x | PASS | cross | 100 / 100 | rows and values match |
| q12 | 68.6 | 57.5 | 1.19x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 349.2 | 333.8 | 1.05x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 6067.8 | 2987.8 | 2.03x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 162.3 | 127.0 | 1.28x | PASS | cross | 100 / 100 | rows and values match |
| q16 | 324.1 | 51.7 | 6.27x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 871.7 | 200.7 | 4.34x | PASS | cross | 0 / 0 | rows and values match |
| q18 | 571.5 | 415.1 | 1.38x | PASS | cross | 100 / 100 | rows and values match |
| q19 | 251.3 | 2251.5 | 0.11x | PASS | cross | 100 / 100 | rows and values match |
| q20 | 63.5 | 62.4 | 1.02x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 630.5 | 56.9 | 11.08x | PASS | cross | 100 / 100 | rows and values match |
| q22 | 2503.6 | 5890.4 | 0.43x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 6529.0 | 2730.7 | 2.39x | PASS | cross | 100 / 100 | rows and values match |
| q24 | 622.1 | 502.2 | 1.24x | PASS | cross | 5 / 5 | rows and values match |
| q25 | 717.0 | 129.1 | 5.56x | PASS | cross | 2 / 2 | rows and values match |
| q26 | 195.6 | 190.6 | 1.03x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 263.6 | 226.4 | 1.16x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 360.5 | 332.4 | 1.08x | PASS | single | 1 / 1 | rows and values match |
| q29 | 196.1 | 212.1 | 0.92x | PASS | cross | 8 / 8 | rows and values match |
| q30 | 215.0 | 249.6 | 0.86x | PASS | cross | 100 / 100 | rows and values match |
| q31 | 2859.6 | 171.1 | 16.71x | PASS | cross | 307 / 307 | rows and values match |
| q32 | 93.1 | 32.0 | 2.91x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 483.1 | 162.3 | 2.98x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 338.8 | 136.2 | 2.49x | PASS | cross | 1560 / 1560 | rows and values match |
| q35 | 709.5 | 498.1 | 1.42x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 622.9 | 119.6 | 5.21x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 203.0 | 179.3 | 1.13x | PASS | cross | 2 / 2 | rows and values match |
| q38 | 1632.4 | 366.0 | 4.46x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 14193.8 | 421.7 | 33.66x | PASS | cross | 2433 / 2433 | rows and values match |
| q40 | 295.5 | 66.0 | 4.47x | PASS | cross | 100 / 100 | rows and values match |
| q41 | 80.9 | 58.5 | 1.38x | PASS | single | 41 / 41 | rows and values match |
| q42 | 117.8 | 62.5 | 1.89x | PASS | cross | 11 / 11 | rows and values match |
| q43 | 253.7 | 111.9 | 2.27x | PASS | cross | 18 / 18 | rows and values match |
| q44 | 2027.0 | 124.0 | 16.35x | PASS | cross | 10 / 10 | rows and values match |
| q45 | 381.8 | 141.6 | 2.70x | PASS | cross | 49 / 49 | rows and values match |
| q46 | 1015.5 | 245.5 | 4.14x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 1427.4 | 979.5 | 1.46x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 378.8 | 327.1 | 1.16x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 222.0 | 98.3 | 2.26x | PASS | cross | 46 / 46 | rows and values match |
| q50 | 160.6 | 235.3 | 0.68x | PASS | cross | 51 / 51 | rows and values match |
| q51 | 1752.8 | 1590.0 | 1.10x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 122.4 | 63.9 | 1.91x | PASS | cross | 100 / 100 | rows and values match |
| q53 | 208.5 | 91.2 | 2.29x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 946.2 | 194.7 | 4.86x | PASS | cross | 1 / 1 | rows and values match |
| q55 | 117.1 | 73.6 | 1.59x | PASS | cross | 100 / 100 | rows and values match |
| q56 | 446.9 | 156.4 | 2.86x | PASS | cross | 100 / 100 | rows and values match |
| q57 | 727.0 | 368.8 | 1.97x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 2569.0 | 141.2 | 18.19x | PASS | cross | 31 / 31 | rows and values match |
| q59 | 1338.4 | 406.1 | 3.30x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 511.1 | 172.1 | 2.97x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 465.0 | 252.9 | 1.84x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 307.8 | 444.3 | 0.69x | PASS | cross | 100 / 100 | rows and values match |
| q63 | 200.8 | 83.3 | 2.41x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 13823.3 | - | - | PASS | cross | 28 / 28 | rows and values match |
| q65 | 1341.7 | 496.0 | 2.71x | PASS | cross | 100 / 100 | rows and values match |
| q66 | 589.9 | 120.6 | 4.89x | PASS | cross | 10 / 10 | rows and values match |
| q67 | 3899.0 | 6117.9 | 0.64x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 1094.3 | 224.9 | 4.87x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 535.2 | 344.1 | 1.56x | PASS | cross | 100 / 100 | rows and values match |
| q70 | 3697.0 | 187.9 | 19.67x | PASS | cross | 7 / 7 | rows and values match |
| q71 | 184.4 | 147.9 | 1.25x | PASS | cross | 9655 / 9655 | rows and values match |
| q72 | 11718.8 | 24218.9 | 0.48x | PASS | cross | 100 / 100 | rows and values match |
| q73 | 330.1 | 143.9 | 2.29x | PASS | cross | 17 / 17 | rows and values match |
| q74 | 2682.2 | 540.0 | 4.97x | PASS | cross | 100 / 100 | rows and values match |
| q75 | 2592.2 | 507.0 | 5.11x | PASS | cross | 100 / 100 | rows and values match |
| q76 | 264.2 | 139.3 | 1.90x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 235.1 | 97.1 | 2.42x | PASS | cross | 100 / 100 | rows and values match |
| q78 | 10918.0 | 1276.5 | 8.55x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 5772.7 | 177.5 | 32.53x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 532.7 | 242.3 | 2.20x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 341.1 | 274.0 | 1.24x | PASS | cross | 100 / 100 | rows and values match |
| q82 | 233.3 | 319.8 | 0.73x | PASS | cross | 6 / 6 | rows and values match |
| q83 | 389.4 | 91.6 | 4.25x | PASS | cross | 100 / 100 | rows and values match |
| q84 | 137.1 | 195.0 | 0.70x | PASS | cross | 100 / 100 | rows and values match |
| q85 | 324.9 | 525.1 | 0.62x | PASS | cross | 15 / 15 | rows and values match |
| q86 | 143.6 | 71.2 | 2.02x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 1633.5 | 427.7 | 3.82x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 851.9 | 496.7 | 1.72x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 299.8 | 133.1 | 2.25x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 90.7 | 60.4 | 1.50x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 187.8 | 389.4 | 0.48x | PASS | cross | 12 / 12 | rows and values match |
| q92 | 84.4 | 42.4 | 1.99x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 115.1 | 307.1 | 0.37x | PASS | cross | 100 / 100 | rows and values match |
| q94 | 206.8 | 68.1 | 3.04x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 5700.7 | 956.1 | 5.96x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 86.7 | 78.0 | 1.11x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 1171.0 | 332.1 | 3.53x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 98.6 | 146.5 | 0.67x | PASS | cross | 15076 / 15076 | rows and values match |
| q99 | 668.6 | 907.5 | 0.74x | PASS | cross | 100 / 100 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 155254.3 | 70451.0 | 2.20x | 2.28x | 98 |
