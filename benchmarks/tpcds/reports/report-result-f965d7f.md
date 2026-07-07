# TPC-DS federated benchmark report

Commit: `f965d7f` - plan: disjunctive decorrelation (q10/q35/q45) - pushdown-through-union enabler, domain-union SEMI, mark joins  (working tree DIRTY - results not from a clean commit)
Generated: 2026-07-07 15:27
Host: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz (12 cores)
Engine: fedqrs (Rust / DataFusion) - the only execution path.
Oracle: DuckDB 1.5.3.

Scale factor 0.1, PostgreSQL + DuckDB split, per-query timeout 60.0s, memory cap 12288 MB. Each query's engine and DuckDB oracle (with PostgreSQL attached) run together in one isolated child process; timings are steady-state (one warm-up run discarded).

Correctness compares fedq's federated result against PURE DuckDB over the same file (every table read locally), the canonical answer - so a MISMATCH is a real engine bug, not a federation quirk of the DuckDB postgres scanner (which dropped rows on q59 and drifts on avg-of-decimal on q18). The federated DuckDB oracle is used only for the timing baseline. Rows are compared in order, values rounded to 2 decimals.

## Placement: pg-dims

[pg-dims] Total 99 | PASS 98 | MISMATCH 1 | ERROR 0 | cross-source 97

### Failure clusters

### Wrong result: row values (1)
Queries: q18

- row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00')

### Per-query matrix

| Query | Ours (ms) | DuckDB (ms) | Ratio | Status | Span | Rows engine/oracle | Detail |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q01 | 59.2 | 20.0 | 2.96x | PASS | cross | 100 / 100 | rows and values match |
| q02 | 2430.8 | 44.8 | 54.28x | PASS | cross | 2513 / 2513 | rows and values match |
| q03 | 32.8 | 19.4 | 1.69x | PASS | cross | 15 / 15 | rows and values match |
| q04 | 1491.5 | 70.1 | 21.27x | PASS | cross | 0 / 0 | rows and values match |
| q05 | 162.7 | 33.8 | 4.81x | PASS | cross | 100 / 100 | rows and values match |
| q06 | 121.4 | 40.6 | 2.99x | PASS | cross | 10 / 10 | rows and values match |
| q07 | 68.9 | 64.8 | 1.06x | PASS | cross | 100 / 100 | rows and values match |
| q08 | 82.0 | 28.1 | 2.92x | PASS | cross | 0 / 0 | rows and values match |
| q09 | 120.3 | 9.8 | 12.25x | PASS | cross | 1 / 1 | rows and values match |
| q10 | 517.1 | 91.3 | 5.66x | PASS | cross | 1 / 1 | rows and values match |
| q11 | 544.2 | 48.9 | 11.14x | PASS | cross | 4 / 4 | rows and values match |
| q12 | 38.2 | 21.1 | 1.81x | PASS | cross | 100 / 100 | rows and values match |
| q13 | 95.4 | 58.9 | 1.62x | PASS | cross | 1 / 1 | rows and values match |
| q14 | 748.2 | 86.8 | 8.62x | PASS | cross | 100 / 100 | rows and values match |
| q15 | 39.5 | 31.4 | 1.26x | PASS | cross | 33 / 33 | rows and values match |
| q16 | 61.5 | 24.2 | 2.54x | PASS | cross | 1 / 1 | rows and values match |
| q17 | 128.9 | 35.1 | 3.68x | PASS | cross | 1 / 1 | rows and values match |
| q18 | 109.8 | 75.4 | 1.46x | MISMATCH | cross | 100 / 100 | row 7 differs: engine=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.98', '836.96', '78.89', '-1022.42', '1945.50', '4.00') oracle=('AAAAAAAAAGEAAAAA', 'NULL', 'NULL', 'NULL', '91.00', '206.99', '836.96', '78.89', '-1022.41', '1945.50', '4.00') |
| q19 | 63.5 | 35.8 | 1.77x | PASS | cross | 10 / 10 | rows and values match |
| q20 | 39.1 | 24.8 | 1.58x | PASS | cross | 100 / 100 | rows and values match |
| q21 | 56.9 | 16.2 | 3.51x | PASS | cross | 32 / 32 | rows and values match |
| q22 | 46.1 | 29.9 | 1.54x | PASS | cross | 100 / 100 | rows and values match |
| q23 | 676.6 | 77.1 | 8.78x | PASS | cross | 1 / 1 | rows and values match |
| q24 | 163.3 | 15.8 | 10.32x | PASS | cross | 0 / 0 | rows and values match |
| q25 | 87.1 | 36.1 | 2.41x | PASS | cross | 0 / 0 | rows and values match |
| q26 | 59.5 | 74.5 | 0.80x | PASS | cross | 100 / 100 | rows and values match |
| q27 | 231.1 | 59.2 | 3.91x | PASS | cross | 100 / 100 | rows and values match |
| q28 | 108.7 | 10.3 | 10.56x | PASS | single | 1 / 1 | rows and values match |
| q29 | 87.0 | 47.6 | 1.83x | PASS | cross | 0 / 0 | rows and values match |
| q30 | 93.2 | 30.7 | 3.03x | PASS | cross | 21 / 21 | rows and values match |
| q31 | 297.5 | 37.9 | 7.85x | PASS | cross | 1 / 1 | rows and values match |
| q32 | 38.0 | 24.7 | 1.54x | PASS | cross | 1 / 1 | rows and values match |
| q33 | 240.6 | 38.4 | 6.26x | PASS | cross | 100 / 100 | rows and values match |
| q34 | 72.9 | 38.1 | 1.91x | PASS | cross | 53 / 53 | rows and values match |
| q35 | 572.1 | 76.4 | 7.49x | PASS | cross | 100 / 100 | rows and values match |
| q36 | 145.0 | 23.1 | 6.28x | PASS | cross | 100 / 100 | rows and values match |
| q37 | 25.5 | 19.6 | 1.31x | PASS | cross | 0 / 0 | rows and values match |
| q38 | 97.7 | 40.8 | 2.39x | PASS | cross | 1 / 1 | rows and values match |
| q39 | 218.0 | 29.2 | 7.45x | PASS | cross | 5 / 5 | rows and values match |
| q40 | 54.8 | 17.1 | 3.21x | PASS | cross | 44 / 44 | rows and values match |
| q41 | 37.3 | 6.9 | 5.44x | PASS | single | 0 / 0 | rows and values match |
| q42 | 36.3 | 18.7 | 1.94x | PASS | cross | 4 / 4 | rows and values match |
| q43 | 46.5 | 29.0 | 1.60x | PASS | cross | 1 / 1 | rows and values match |
| q44 | 103.2 | 5.1 | 20.24x | PASS | cross | 0 / 0 | rows and values match |
| q45 | 123.5 | 31.0 | 3.99x | PASS | cross | 24 / 24 | rows and values match |
| q46 | 112.3 | 36.9 | 3.04x | PASS | cross | 100 / 100 | rows and values match |
| q47 | 621.6 | 45.0 | 13.81x | PASS | cross | 100 / 100 | rows and values match |
| q48 | 97.6 | 63.1 | 1.55x | PASS | cross | 1 / 1 | rows and values match |
| q49 | 147.3 | 33.6 | 4.38x | PASS | cross | 2 / 2 | rows and values match |
| q50 | 71.2 | 35.4 | 2.01x | PASS | cross | 1 / 1 | rows and values match |
| q51 | 133.9 | 45.5 | 2.94x | PASS | cross | 100 / 100 | rows and values match |
| q52 | 36.4 | 19.7 | 1.85x | PASS | cross | 11 / 11 | rows and values match |
| q53 | 64.8 | 25.5 | 2.54x | PASS | cross | 100 / 100 | rows and values match |
| q54 | 170.6 | 54.0 | 3.16x | PASS | cross | 0 / 0 | rows and values match |
| q55 | 35.5 | 18.3 | 1.94x | PASS | cross | 20 / 20 | rows and values match |
| q56 | 153.9 | 42.3 | 3.64x | PASS | cross | 38 / 38 | rows and values match |
| q57 | 444.7 | 40.7 | 10.91x | PASS | cross | 100 / 100 | rows and values match |
| q58 | 235.4 | 64.2 | 3.67x | PASS | cross | 0 / 0 | rows and values match |
| q59 | 4069.8 | 75.7 | 53.78x | PASS | cross | 100 / 100 | rows and values match |
| q60 | 146.7 | 38.2 | 3.84x | PASS | cross | 100 / 100 | rows and values match |
| q61 | 127.9 | 41.0 | 3.12x | PASS | cross | 1 / 1 | rows and values match |
| q62 | 72.4 | 32.7 | 2.22x | PASS | cross | 6 / 6 | rows and values match |
| q63 | 60.4 | 27.7 | 2.18x | PASS | cross | 100 / 100 | rows and values match |
| q64 | 3400.0 | 101.7 | 33.44x | PASS | cross | 0 / 0 | rows and values match |
| q65 | 74.5 | 26.0 | 2.86x | PASS | cross | 0 / 0 | rows and values match |
| q66 | 1034.8 | 72.8 | 14.21x | PASS | cross | 1 / 1 | rows and values match |
| q67 | 229.4 | 65.0 | 3.53x | PASS | cross | 100 / 100 | rows and values match |
| q68 | 104.1 | 31.4 | 3.31x | PASS | cross | 100 / 100 | rows and values match |
| q69 | 127.8 | 91.9 | 1.39x | PASS | cross | 71 / 71 | rows and values match |
| q70 | 75.8 | 32.6 | 2.33x | PASS | cross | 3 / 3 | rows and values match |
| q71 | 94.1 | 77.7 | 1.21x | PASS | cross | 56 / 56 | rows and values match |
| q72 | 176.9 | 69.9 | 2.53x | PASS | cross | 50 / 50 | rows and values match |
| q73 | 88.9 | 33.5 | 2.65x | PASS | cross | 0 / 0 | rows and values match |
| q74 | 360.8 | 40.6 | 8.88x | PASS | cross | 3 / 3 | rows and values match |
| q75 | 668.3 | 37.7 | 17.72x | PASS | cross | 25 / 25 | rows and values match |
| q76 | 86.3 | 33.6 | 2.57x | PASS | cross | 100 / 100 | rows and values match |
| q77 | 157.0 | 32.8 | 4.78x | PASS | cross | 10 / 10 | rows and values match |
| q78 | 195.7 | 39.7 | 4.93x | PASS | cross | 100 / 100 | rows and values match |
| q79 | 91.6 | 27.4 | 3.34x | PASS | cross | 100 / 100 | rows and values match |
| q80 | 225.0 | 44.5 | 5.06x | PASS | cross | 100 / 100 | rows and values match |
| q81 | 97.3 | 30.0 | 3.24x | PASS | cross | 34 / 34 | rows and values match |
| q82 | 27.1 | 13.9 | 1.94x | PASS | cross | 0 / 0 | rows and values match |
| q83 | 170.8 | 65.6 | 2.60x | PASS | cross | 0 / 0 | rows and values match |
| q84 | 39.9 | 30.4 | 1.32x | PASS | cross | 6 / 6 | rows and values match |
| q85 | 148.5 | 78.2 | 1.90x | PASS | cross | 0 / 0 | rows and values match |
| q86 | 42.9 | 19.4 | 2.21x | PASS | cross | 100 / 100 | rows and values match |
| q87 | 93.8 | 42.3 | 2.21x | PASS | cross | 1 / 1 | rows and values match |
| q88 | 376.9 | 194.0 | 1.94x | PASS | cross | 1 / 1 | rows and values match |
| q89 | 76.4 | 32.3 | 2.37x | PASS | cross | 100 / 100 | rows and values match |
| q90 | 97.5 | 38.7 | 2.52x | PASS | cross | 1 / 1 | rows and values match |
| q91 | 63.9 | 50.6 | 1.26x | PASS | cross | 0 / 0 | rows and values match |
| q92 | 42.0 | 20.8 | 2.02x | PASS | cross | 1 / 1 | rows and values match |
| q93 | 22.0 | 4.3 | 5.17x | PASS | cross | 0 / 0 | rows and values match |
| q94 | 54.9 | 24.6 | 2.24x | PASS | cross | 1 / 1 | rows and values match |
| q95 | 55.2 | 28.2 | 1.96x | PASS | cross | 1 / 1 | rows and values match |
| q96 | 38.3 | 24.4 | 1.57x | PASS | cross | 1 / 1 | rows and values match |
| q97 | 55.2 | 26.7 | 2.07x | PASS | cross | 1 / 1 | rows and values match |
| q98 | 37.7 | 20.9 | 1.81x | PASS | cross | 250 / 250 | rows and values match |
| q99 | 69.0 | 21.0 | 3.29x | PASS | cross | 6 / 6 | rows and values match |


### Timing summary (PASS only): engine vs DuckDB-over-Postgres [pg-dims]

| Ours (ms) | DuckDB (ms) | Ratio | Geomean | Measured |
| --- | --- | --- | --- | --- |
| 26165.9 | 3987.4 | 6.56x | 3.47x | 98 |
