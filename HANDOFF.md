# Handoff: Rust engine cutover, N-K decorrelation, DuckDB-merge removal

State of the work done in this session. Facts only: what exists, what passes,
what is measured, what is not done.

## Branches

```
merge-engine-datafusion            base
  └─ nk-decorrelation              (12 commits) Decimal128, q11, Rust cutover, N-K M1-M6, window schema
       └─ remove-duckdb-merge      (6 commits)  merge-engine removal + committed benchmarks   <-- HEAD
```

Current branch: **remove-duckdb-merge**. Test suite: **1019 passed, 3 skipped, 39 xfailed, 0 failed**
(`POSTGRES_DB=duckpoc python -m pytest -q`). There is one execution path now: the Rust engine (fedqrs).

## What was built

### 1. Rust engine is the only execution path
- `execute_to_table` runs every plan through the Rust engine (`execute_via_rust`). `UnsupportedIR`
  propagates (fail loud); there is no fallback and no `FEDQ_ENGINE` switch.
- EXPLAIN renders directly (`PhysicalExplain`), no engine.

### 2. DuckDB merge engine: fully removed
- `federated_query/executor/merge_engine.py` deleted. Zero references to `merge_engine`/`MergeEngine`/
  `FEDQ_ENGINE`/`_execute_merge`/`set_merge_engine` in `federated_query/`.
- Physical nodes lost their Python merge-execution methods; `schema()` methods that ran SQL through
  DuckDB now compute types directly (`PhysicalWindow` in particular: `ROW_NUMBER -> Int64` + passthrough).
- The DuckDB **data source** (`datasources/duckdb.py`, `parquet.py`) is untouched and still used.
- `PhysicalCTEMergeQuery.schema()` / `PhysicalLateralJoin.schema()` raise `NotImplementedError` (they are
  never reached on the Rust path; proven by the green suite with no engine attached).

### 3. Decimal exactness (Decimal128)
- Postgres `numeric`/`decimal` arrive over ADBC as opaque strings; now parsed to `Decimal128(38, scale)`
  with scale derived from the data, instead of a lossy `Float64` cast. TPC-H q02/q10/q22 now match DuckDB
  to the cent. (`fedqrs/src/connectors.rs`.)

### 4. q11 and correlated-subquery output resolution
- `PhysicalHashAggregate.column_aliases()` returns OUTPUT columns (was an input passthrough); the
  aggregate's own type inference uses `input.column_aliases()`. Pushdown exposes computed remote-query
  outputs. q11 runs on Rust.

### 5. Neumann-Kemper general decorrelation (M1-M6) - `optimizer/decorrelation.py`
Every correlated shape and user LATERAL now unnests to regular algebra (distinct domain + join +
aggregate/top-k-window + join-back) and runs cross-source on Rust. `tests/test_nk_decorrelation.py`
(15 differential-vs-DuckDB tests). Covered:
- non-equi correlated scalar aggregate (+ count-bug, + outer-ref-in-aggregate-value)
- non-equi correlated top-k (`ORDER BY ... LIMIT`) via `ROW_NUMBER()`
- user `LATERAL`: top-k, aggregate, multi-row (set), and comma-lateral bodies
Plan doc: `nk-decorrel-plan.md`.

## Benchmarks (committed, reproducible)

Data: `/workspace/tpch_parquet` (SF 0.1), `/workspace/tpch_parquet_sf1` (SF 1, 6M lineitem),
Postgres `duckpoc` (SF 0.1, all TPC-H tables), DuckDB files under `benchmarks/tpch/data/`.

### `benchmarks/tpch/run_tpch.py` - Rust vs DuckDB over the SAME Parquet
- `--mode single` (all 8 tables one source = pure engine) / `--mode federated` (2 parquet sources).
- Each query runs in a subprocess with a hard RSS cap (`--memlimit-gb`, default 12); over-cap queries
  are KILLED and reported. `slower` column = ours / duckdb.
- **Caveat: DuckDB here reads all files monolithically - it does NOT federate.** So the federated
  numbers from this script overstate our disadvantage; use `run_federated.py` for the fair federated one.
- Measured: SF0.1 single ~4.3x (median query ~1.9x); SF1 single 21x total. The total is dominated by
  5 join-ordering-broken queries (q02/q08/q09/q15/q18) that scale super-linearly (q09: 365ms -> 12s at
  10x data; DuckDB q09 scales 5.5x). The other 17 stay ~2x.

### `benchmarks/tpch/run_federated.py` - the FAIR federated comparison
- Facts in Postgres, dims in DuckDB; **DuckDB federates the SAME split by attaching Postgres via its
  `postgres` connector** (both engines cross the source boundary).
- `--placements pg-dims,adversarial`, `--scale-factor`, `--only`. Reports correctness + timing + ratio.
- Measured (SF0.1, pg-dims, 18/22 cross-source): **22/22 PASS, ours 3758ms vs duckdb 306ms = 12.27x
  slower.** Worst: q09 32.6x, q15 28.6x, q18 25.3x, q08 22x, q05 21x. Best ~3x (q01/q04/q19).

## Known gaps (not done / measured-broken)

- **fedqrs missing operators** (compiled Rust, `/workspace/fedqrs`) - 3 plan shapes have no Rust
  operator and are `xfail`ed in `tests/e2e_decorrelation/` (18 xfails): `PhysicalSingleRowGuard`
  (scalar cardinality), `PhysicalUnion` (union-distinct), empty-condition semi-join (uncorrelated EXISTS).
  Scalar-cardinality violation now surfaces as a loud `UnsupportedIR` (2 tests updated to expect it).
- **No cost-based join ordering.** FROM-order joins produce cross-products; q02/q08/q09/q15/q18 blow up
  super-linearly and q09 OOMs (>12GB) at SF1. Present in single-source too, not just federated.
- **Projection pushdown does not prune join-feeding scans.** Verified on q03: the lineitem scan reads all
  16 columns when 4 are needed; orders reads 9 (needs 4); customer reads 8 (needs 1). Filter pushdown
  DOES work; dynamic-filter / semi-join reduction works for some tables (orders) but not the fact table
  (lineitem full-scanned).
- **One lost test:** `test_cross_source_semi_join_reduction` was deleted (it asserted a runtime injected
  filter only observable via the removed merge proxy). Correctness still covered by
  `test_cross_datasource_subquery_fallback`.

## How to run

```
# unit + integration suite (Rust engine only)
POSTGRES_DB=duckpoc python -m pytest -q

# pure-engine and 2-parquet-source (DuckDB monolithic reference)
python benchmarks/tpch/run_tpch.py --mode single    --data /workspace/tpch_parquet
python benchmarks/tpch/run_tpch.py --mode federated --data /workspace/tpch_parquet_sf1 --memlimit-gb 12

# FAIR federated: both engines cross PG<->DuckDB
POSTGRES_DB=duckpoc python benchmarks/tpch/run_federated.py --scale-factor 0.1 --placements pg-dims
```
