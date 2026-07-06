# TPC-DS benchmark

This benchmark checks how many of the 99 standard TPC-DS queries the federated
query engine supports and whether it returns correct results. Performance is not
measured here; the goal is query coverage and correctness.

TPC-DS is much larger and nastier than TPC-H: 99 queries instead of 22, 24 base
tables instead of 8, and heavy use of CTEs, nested subqueries, date logic,
casts, `ROLLUP` / `GROUPING SETS`, and set operations. A query the engine cannot
yet handle (an unsupported feature or a real limitation) shows up as an `ERROR`
row with the raising exception; nothing is skipped or hidden.

## What it does

1. `generate.py` uses the DuckDB `tpcds` extension to build a native DuckDB
   database file (`data/tpcds_sf<SF>.duckdb`) at a chosen scale factor and dumps
   the 99 TPC-DS queries (as DuckDB emits them, with template parameters already
   substituted) to `queries/q01.sql` .. `q99.sql`.
2. `run.py` registers that database file as a single DuckDB source named
   `tpcds`, then for each query:
   - qualifies the 24 TPC-DS base tables to `tpcds.main.<table>` and transpiles
     the query to the engine's dialect, then runs it through the full engine
     pipeline;
   - runs the original query directly in DuckDB as the correctness oracle;
   - compares the two result sets row by row.
3. `run_federated.py` splits the 24 tables across a PostgreSQL source and a
   DuckDB source (a `placement`) to exercise the engine's cross-source paths,
   using DuckDB's `postgres` connector reading the same split as the oracle.

## Running

```bash
cd benchmarks/tpcds
./run.sh                     # scale factor 1 (default; ~300 MB DuckDB file)
./run.sh 0.1                 # a smaller scale factor
./run.sh 1 --only 1,6        # run a subset of queries
./run.sh 1 --timeout 300     # raise the per-query wall-clock limit
./run.sh 1 --report tpcds-report.md  # also write a clustered markdown report
```

`--report <path>` writes a markdown report with the summary, failures grouped
into clusters (by reason), and the full per-query matrix including engine/oracle
row counts.

`run.sh` defaults to the `venv-fedq` interpreter; override with
`PYTHON=/path/to/python ./run.sh`.

Each engine run happens in an isolated child process bounded by `--timeout`
seconds (default 120) and `--memory-limit` MB of address space (default 12288).
A query that exceeds either becomes an `ERROR` row (`Timeout` / `Killed`)
instead of stalling or OOM-ing the whole run. Raise the limits for large scale
factors.

### Federated (cross-source) run

```bash
# Postgres must be up (see the repo Makefile: make pg-start).
/workspace/venv-fedq/bin/python load_postgres.py --scale-factor 1
/workspace/venv-fedq/bin/python run_federated.py --scale-factor 1 \
    --report tpcds-federated-report.md
```

`load_postgres.py` copies all 24 tables into PostgreSQL (byte-identical to the
DuckDB file) so a placement only decides which source each table is READ from.
`run_federated.py` then runs the query set under each `placement` (`pg-dims`
puts dimensions on PostgreSQL and facts on DuckDB; `adversarial` splits sales
facts from their returns and alternates dimensions to force cross-source joins).

Like the single-source runner, each engine run happens in an isolated child
process bounded by `--timeout` seconds (default 120) and `--memory-limit` MB
(default 12288); the DuckDB oracle runs in the parent. A cross-source query that
hangs or explodes intermediate results becomes a clean `ERROR` row (`Timeout` /
`Killed`) instead of stalling or OOM-ing the whole run. `--report <path>` writes
a markdown report with a per-placement summary, failure clusters, and the full
per-query matrix (including the `single`/`cross` span of each query).

## Output

One line per query plus a summary:

- `PASS` - engine ran the query and the result set matches DuckDB.
- `MISMATCH` - engine ran the query but returned different rows (the reason
  names the row-count or an example differing row).
- `ERROR` - engine could not run the query (the reason is the exception).

## Correctness comparison

Results are compared by column position (the engines may name columns
differently), row by row in order: row i of the engine output must equal row i
of DuckDB's output. Numbers are rounded to `--decimals` (default 2, matching
TPC-DS monetary values) and `CHAR` padding is stripped, so only genuine value
differences count. Row order is part of correctness; when the rows match as a
set but not in order, the report flags it as an ordering difference rather than
a wrong value. Note that many TPC-DS queries end in `ORDER BY ... LIMIT n` over
columns with ties, so a differing tie-break is a legitimate ordering difference
rather than a wrong result.

### DuckDB postgres-scanner filter pushdown (disabled in the oracle)

The federated oracle sets `pg_experimental_filter_pushdown=false`. DuckDB's
postgres extension can push a WHERE filter down to Postgres incorrectly and the
scan then returns zero rows - on q59 the oracle returned 0 rows where both pure
DuckDB and this engine return 100 (verified: the two data copies are identical;
`EXPLAIN ANALYZE` shows a scan collapsing to 0; turning the pushdown off returns
the correct 100). Since the oracle is the correctness reference, we disable the
buggy (and, per its name, experimental) pushdown so the oracle is correct. The
feature is new, so DuckDB will likely fix it; revisit then.

One side effect: without pushdown the oracle reads unfiltered dimension rows and
filters locally, so it is a bit slower than DuckDB's real fast path. That makes
the engine-vs-DuckDB timing ratio somewhat favorable to us for now. Acceptable
for outlier-hunting; treat the timing headline as "vs correct DuckDB", not "vs
DuckDB at its fastest".

## Files

- `generate.py` - build the DuckDB database and query files.
- `qualify.py` - rewrite bare TPC-DS table names to `source.schema.table`.
- `compare.py` - row-by-row result comparison with numeric tolerance.
- `run.py` - run queries through the engine and the DuckDB oracle, report.
- `run.sh` - end-to-end orchestration (generate if needed, then run).
- `load_postgres.py` - copy the 24 tables into PostgreSQL for the federated run.
- `run_federated.py` - run the query set split across PostgreSQL and DuckDB.
- `data/` - generated database file (git-ignored).
- `queries/` - generated TPC-DS query files.
