# Status: join ordering + projection pushdown live; next is q15/q21

State of the work. Facts only: what exists, what passes, what is measured,
what is not done. Previous phase (Rust cutover, N-K decorrelation, merge-engine
removal) is in git history before commit `4018914`.

## Branches

```
merge-engine-datafusion
  `- nk-decorrelation
       `- remove-duckdb-merge          Rust-only execution, N-K decorrelation
            `- feature/cost-based-optimizer   <-- HEAD (this document)
```

Test suite: **1120 passed, 3 skipped, 39 xfailed, 0 failed**
(`POSTGRES_DB=duckpoc python -m pytest -q`). `make lint` green.

## What was built on this branch

### 1. Reproducible benchmark harness (committed before the optimizer)
- `benchmarks/tpch/bench.py`: one entry point, three methodologies
  (`single` Parquet / `fedparquet` 2-Parquet / `fedpgduck` fair PG+DuckDB)
  x SF0.1 + SF1, per-query subprocess with RSS cap + timeout, differential
  correctness vs DuckDB, report written to
  `benchmarks/tpch/reports/report-result-<commit>.md`.
- Data prep is fully scripted (`generate.py`, `export_parquet.py`,
  `load_postgres.py` - now runs ANALYZE). See `benchmarks/tpch/README.md`.
- Baseline (pre-optimizer) report: `report-result-3872243.md`.

### 2. Cost-based join-ordering optimizer (M0-M7, ON by default)
- **Statistics** (`optimizer/statistics.py`, datasources): fetched from the
  SOURCES' CATALOGS at optimize time, lazily per column (join keys + filter
  columns only), session-cached, absence cached. PG: schema-qualified
  `pg_class.reltuples` (+`pg_namespace`; -1 = honest None) + `pg_stats`
  (n_distinct decode, null_frac, avg_width, histogram min/max). DuckDB /
  Parquet: `duckdb_tables()` row count + ONE `approx_count_distinct/min/max/
  count` scan for requested columns. Sources never fabricate a statistic.
- **Estimates** (`optimizer/cost.py`, `estimate_defaults.py`):
  `CostModel.estimate(plan) -> CardinalityEstimate` covers every logical node
  type; joins use `card(L) x card(R) / prod max(ndv(l_key), ndv(r_key))` with
  NDVs clamped per side. Every missing statistic falls back to a NAMED default
  and is recorded in `defaults_used` provenance. `join_tree_cost` (C_out) is
  the documented seam for a future locality/network cost term.
- **Ordering** (`optimizer/join_graph.py`, `join_ordering.py`): region
  extraction (INNER/CROSS joins + absorbed Filters; boundaries: outer/semi/
  anti/NATURAL/USING joins, laterals, guards, derived tables - recursed into
  separately), left-deep Selinger DP over connected subsets per component
  (<= `max_join_reorder_size`, default 10; GOO greedy above), connected
  extensions only = intra-component cross products structurally impossible,
  components CROSSed smallest-first. Deterministic, idempotent.
- **The rule** (`JoinOrderingRule`): re-emits in PredicatePushdown's normal
  form; a predicate-conservation guard RAISES if any conjunct is not placed
  exactly once. Runs right after PredicatePushdown via `optimizer/factory.py`
  (`build_optimizer` - the single assembly point; `enable_join_reordering`,
  `enable_predicate_pushdown`, `enable_projection_pushdown` are real flags
  now). `FedQRuntime` takes the whole `Config`.
- **EXPLAIN**: joins print `rows=<estimate>` and `stats=defaulted[...]`
  naming every estimate built on a default instead of a real statistic.
- Tests: unit (stats layer, NDV formula, extraction, enumerator with stub
  stats incl. the q05 cyclic trap, rule shape/idempotence/guard), e2e
  differential (reordering on vs off vs DuckDB oracle,
  `tests/e2e_pushdown/test_join_reordering.py`), plus the committed
  experiment gate.

## Measured (committed reports)

`benchmarks/tpch/reports/report-result-8b54580.md` vs baseline
`report-result-3872243.md` (ratio = ours/DuckDB, lower is better):

| Cell       | SF  | Before                | After           |
| ---------- | --- | --------------------- | --------------- |
| single     | 0.1 | 4.31x                 | **2.47x**       |
| single     | 1   | 13.76x, q09 OOM-KILLED | **5.62x, 22/22** |
| fedparquet | 0.1 | 6.47x                 | 5.06x           |
| fedparquet | 1   | 18.59x, q09 OOM-KILLED | **9.93x, 22/22** |
| fedpgduck  | 0.1 | 12.01x                | 11.29x          |
| fedpgduck  | 1   | 38.55x                | **23.24x**      |

- **All 132 cells correct; zero KILLED/ERROR anywhere.** q09 went from
  OOM (>12GB) to 448ms (single SF1).
- Join-order pathologies eliminated: single SF1 q02 3857 -> 37ms,
  q08 6253 -> 92ms, q09 KILLED -> 448ms.
- Order-independence confirmed (`experiments/reports/joinorder-12609d3-sf1.md`):
  original vs hand-reordered CONVERGE; q07 fedpgduck the DP beat the hand
  order (1247ms vs 2635ms).

## Projection pushdown: FIXED (PP-M1..M6)

Column pruning was a NO-OP on every TPC-H query: the gate
(_has_explicit_projection) did not see through Sort, and every query has
ORDER BY at the root. Fixed end to end:
- gate sees through Sort/GroupedLimit/SingleRowGuard, CTE.child, set-op
  branches;
- collection is qualified (table.col) and annotation-driven
  (direct_expressions - total by construction, unclassifiable fields raise),
  covering Scan.order_by_keys (fixpoint iteration 2), subquery-hidden probe
  values, and correlated refs across scopes;
- scans match by alias AND physical name; pushed-aggregate/DISTINCT scans and
  star refs are never pruned; unreferenced relations keep everything;
- the prune walker is total and fail-loud; derived tables / CTE plans /
  set-op branches prune inside only when their root pins an explicit schema.
Measured widths (fedpgduck q03): customer 8 -> 2, orders 9 -> 4, lineitem
16 -> 4 columns; q15's CTE interior narrows too. Zero e2e fallout; the q07
history (equi-only connectivity fix and its data-movement trade-off) is in
the git log at 063aada.

## Measured after both phases (report-result-307cec1.md)

| Cell       | SF  | Pre-optimizer | Join ordering | + Projection pushdown |
| ---------- | --- | ------------- | ------------- | --------------------- |
| single     | 0.1 | 4.31x         | 2.47x         | **1.90x**             |
| single     | 1   | 13.8x, q09 OOM | 5.62x        | **3.97x**             |
| fedparquet | 0.1 | 6.47x         | 5.06x         | **2.71x**             |
| fedparquet | 1   | 18.6x, q09 OOM | 9.93x        | **5.43x**             |
| fedpgduck  | 0.1 | 12.01x        | 11.29x        | **6.31x**             |
| fedpgduck  | 1   | 38.55x        | 23.24x        | **11.36x**            |

All 132 cells correct everywhere. fedpgduck SF1 big movers vs the
join-ordering report: q03 1407 -> 250ms, q05 2573 -> 427ms, q07 2689 ->
476ms, q09 4622 -> 962ms, q18 3772 -> 771ms.

## Known gaps / next work (in priority order from the data)

1. **q15 (168x fedpgduck, ~105x single at SF1, 2.8s).** View + window shape;
   untouched by both optimizer phases. The single biggest outlier left.
2. **q21 (26.6x fedpgduck SF1, 3.2s).** lineitem multi-self-join with
   EXISTS/NOT EXISTS; barely moved by either phase - decorrelation shape +
   data movement.
3. **q17 (14.9x fedpgduck SF1)** - correlated aggregate subquery shape.
4. **Locality cost term** at the `join_tree_cost` seam: transfer cost =
   rows x width per source boundary; pruning made width meaningful, and the
   q07 pair-first episode (git log 063aada) showed order choice can trade
   local work against movement.
5. **fedqrs missing operators** (unchanged): `PhysicalSingleRowGuard`,
   `PhysicalUnion`, empty-condition semi-join - 18 xfails in
   `tests/e2e_decorrelation/`.
6. `enable_decorrelation` config flag is still unwired (the Decorrelator is
   not an optimizer rule).

## How to run

```
# suite + lint
POSTGRES_DB=duckpoc python -m pytest -q
make lint

# full benchmark matrix -> reports/report-result-<commit>.md
python benchmarks/tpch/bench.py

# join-order experiment (original vs hand-reordered through the same engine)
python benchmarks/tpch/experiments/joinorder_experiment.py --scale 1
```

Data prep for both scale factors: `benchmarks/tpch/README.md`. PostgreSQL:
`make pg-start`; SF0.1 lives in `duckpoc`, SF1 in `duckpoc_sf1` (ANALYZEd).
