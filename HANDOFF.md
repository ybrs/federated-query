# Status: cost-based join ordering live; next is estimation precision

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

Test suite: **1096 passed, 3 skipped, 39 xfailed, 0 failed**
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

## Known gaps / next work (in priority order from the data)

1. **q07 regression (single SF1: 193 -> 740ms, 12.4x).** The
   FRANCE/GERMANY OR predicate spans the two nation atoms; it is a
   cross-side residual conjunct estimated with `stats=None`, so its
   equalities get `DEFAULT_EQ_SELECTIVITY` instead of 1/ndv(n_name) - the
   misestimate steers the DP into a worse order than FROM order. Fix:
   resolve column stats for residual-conjunct selectivity (the resolver
   already exists for join keys). THE CURRENT TASK.
2. **q15 (105x single, 175x fedpgduck at SF1).** View + window shape;
   unchanged by join ordering (never was a join-order problem). Next big
   single-query win.
3. **fedpgduck residuals** (q03 42x, q05 57x, q09 52x, q18 44x at SF1):
   data movement - projection pushdown does not prune fat scans
   (q03 lineitem reads 16 columns, needs 4), and the cost model has no
   locality term yet (the `join_tree_cost` seam). Semi-join reduction works
   for some tables but not the fact table.
4. **q09 estimation gap**: engine picks a ~4.8s order; best known hand order
   is 3.3s (fedpgduck SF1). More estimation precision (LIKE selectivity is a
   default) would close it.
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
