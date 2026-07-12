# TPC-DS federated benchmark report (Rust engine)

Engine: fedq-py (Rust / DataFusion), driven via pyo3 fedq.Runtime.
Placement: pg-dims (dimensions on PostgreSQL, facts on DuckDB).
Truth: cached pure-DuckDB references in references_sf0.1.duckdb.
Generated: 2026-07-12 19:42

Tally: 92 ok | 0 wrong | 7 error   (total 99 queries, 6.5s)

## Non-OK queries grouped by reason

### RuntimeError: execution error: Error during planning: function 'X' not supported (4) [ERROR]
Queries: q05, q18, q77, q80

### RuntimeError: execution error: window over GROUPING() (two-stage split) is not yet supported by step-building (2) [ERROR]
Queries: q70, q86

### RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression (1) [ERROR]
Queries: q14

## Per-query matrix

| Query | Status | ms | Rows engine/truth | Message |
| --- | --- | --- | --- | --- |
| q01 | OK | 189.2 | 100 / 100 | rows and values match |
| q02 | OK | 197.2 | 2513 / 2513 | rows and values match |
| q03 | OK | 28.2 | 15 / 15 | rows and values match |
| q04 | OK | 259.9 | 0 / 0 | rows and values match |
| q05 | ERROR | 42.9 | - | RuntimeError: execution error: Error during planning: function 'rollup' not supported |
| q06 | OK | 46.4 | 10 / 10 | rows and values match |
| q07 | OK | 40.1 | 100 / 100 | rows and values match |
| q08 | OK | 34.2 | 0 / 0 | rows and values match |
| q09 | OK | 28.2 | 1 / 1 | rows and values match |
| q10 | OK | 61.8 | 1 / 1 | rows and values match |
| q11 | OK | 122.0 | 4 / 4 | rows and values match |
| q12 | OK | 32.1 | 100 / 100 | rows and values match |
| q13 | OK | 84.1 | 1 / 1 | rows and values match |
| q14 | ERROR | 0.6 | - | RuntimeError: parse error: unsupported SQL: set operation used as a scalar expression |
| q15 | OK | 22.1 | 33 / 33 | rows and values match |
| q16 | OK | 35.3 | 1 / 1 | rows and values match |
| q17 | OK | 49.9 | 1 / 1 | rows and values match |
| q18 | ERROR | 47.6 | - | RuntimeError: execution error: Error during planning: function 'rollup' not supported |
| q19 | OK | 32.1 | 10 / 10 | rows and values match |
| q20 | OK | 25.7 | 100 / 100 | rows and values match |
| q21 | OK | 20.9 | 32 / 32 | rows and values match |
| q22 | OK | 26.7 | 100 / 100 | rows and values match |
| q23 | OK | 199.0 | 1 / 1 | rows and values match |
| q24 | OK | 56.8 | 0 / 0 | rows and values match |
| q25 | OK | 48.4 | 0 / 0 | rows and values match |
| q26 | OK | 35.1 | 100 / 100 | rows and values match |
| q27 | OK | 58.0 | 100 / 100 | rows and values match |
| q28 | OK | 28.4 | 1 / 1 | rows and values match |
| q29 | OK | 42.6 | 0 / 0 | rows and values match |
| q30 | OK | 45.2 | 21 / 21 | rows and values match |
| q31 | OK | 88.1 | 1 / 1 | rows and values match |
| q32 | OK | 21.8 | 1 / 1 | rows and values match |
| q33 | OK | 108.6 | 100 / 100 | rows and values match |
| q34 | OK | 42.4 | 53 / 53 | rows and values match |
| q35 | OK | 73.0 | 100 / 100 | rows and values match |
| q36 | OK | 39.6 | 100 / 100 | rows and values match |
| q37 | OK | 19.2 | 0 / 0 | rows and values match |
| q38 | OK | 44.7 | 1 / 1 | rows and values match |
| q39 | OK | 28.6 | 5 / 5 | rows and values match |
| q40 | OK | 27.9 | 44 / 44 | rows and values match |
| q41 | OK | 5.2 | 0 / 0 | rows and values match |
| q42 | OK | 17.4 | 4 / 4 | rows and values match |
| q43 | OK | 20.2 | 1 / 1 | rows and values match |
| q44 | OK | 32.2 | 0 / 0 | rows and values match |
| q45 | OK | 83.1 | 24 / 24 | rows and values match |
| q46 | OK | 50.4 | 100 / 100 | rows and values match |
| q47 | OK | 86.5 | 100 / 100 | rows and values match |
| q48 | OK | 72.6 | 1 / 1 | rows and values match |
| q49 | OK | 53.0 | 2 / 2 | rows and values match |
| q50 | OK | 205.0 | 1 / 1 | rows and values match |
| q51 | OK | 54.4 | 100 / 100 | rows and values match |
| q52 | OK | 19.1 | 11 / 11 | rows and values match |
| q53 | OK | 37.0 | 100 / 100 | rows and values match |
| q54 | OK | 60.3 | 0 / 0 | rows and values match |
| q55 | OK | 16.1 | 20 / 20 | rows and values match |
| q56 | OK | 76.2 | 38 / 38 | rows and values match |
| q57 | OK | 70.8 | 100 / 100 | rows and values match |
| q58 | OK | 111.6 | 0 / 0 | rows and values match |
| q59 | OK | 93.9 | 100 / 100 | rows and values match |
| q60 | OK | 83.2 | 100 / 100 | rows and values match |
| q61 | OK | 50.0 | 1 / 1 | rows and values match |
| q62 | OK | 41.0 | 6 / 6 | rows and values match |
| q63 | OK | 31.3 | 100 / 100 | rows and values match |
| q64 | OK | 227.4 | 0 / 0 | rows and values match |
| q65 | OK | 39.3 | 0 / 0 | rows and values match |
| q66 | OK | 124.3 | 1 / 1 | rows and values match |
| q67 | OK | 80.4 | 100 / 100 | rows and values match |
| q68 | OK | 48.3 | 100 / 100 | rows and values match |
| q69 | OK | 54.4 | 71 / 71 | rows and values match |
| q70 | ERROR | 6.5 | - | RuntimeError: execution error: window over GROUPING() (two-stage split) is not yet supported by step-building |
| q71 | OK | 46.9 | 56 / 56 | rows and values match |
| q72 | OK | 139.7 | 50 / 50 | rows and values match |
| q73 | OK | 32.5 | 0 / 0 | rows and values match |
| q74 | OK | 79.0 | 3 / 3 | rows and values match |
| q75 | OK | 128.9 | 25 / 25 | rows and values match |
| q76 | OK | 65.2 | 100 / 100 | rows and values match |
| q77 | ERROR | 38.6 | - | RuntimeError: execution error: Error during planning: function 'rollup' not supported |
| q78 | OK | 75.3 | 100 / 100 | rows and values match |
| q79 | OK | 38.7 | 100 / 100 | rows and values match |
| q80 | ERROR | 49.4 | - | RuntimeError: execution error: Error during planning: function 'rollup' not supported |
| q81 | OK | 56.5 | 34 / 34 | rows and values match |
| q82 | OK | 14.0 | 0 / 0 | rows and values match |
| q83 | OK | 102.0 | 0 / 0 | rows and values match |
| q84 | OK | 28.9 | 6 / 6 | rows and values match |
| q85 | OK | 127.7 | 0 / 0 | rows and values match |
| q86 | ERROR | 2.3 | - | RuntimeError: execution error: window over GROUPING() (two-stage split) is not yet supported by step-building |
| q87 | OK | 42.5 | 1 / 1 | rows and values match |
| q88 | OK | 130.7 | 1 / 1 | rows and values match |
| q89 | OK | 35.0 | 100 / 100 | rows and values match |
| q90 | OK | 37.5 | 1 / 1 | rows and values match |
| q91 | OK | 46.4 | 0 / 0 | rows and values match |
| q92 | OK | 22.4 | 1 / 1 | rows and values match |
| q93 | OK | 10.3 | 0 / 0 | rows and values match |
| q94 | OK | 31.3 | 1 / 1 | rows and values match |
| q95 | OK | 28.3 | 1 / 1 | rows and values match |
| q96 | OK | 15.0 | 1 / 1 | rows and values match |
| q97 | OK | 27.7 | 1 / 1 | rows and values match |
| q98 | OK | 35.7 | 250 / 250 | rows and values match |
| q99 | OK | 23.1 | 6 / 6 | rows and values match |
