# Handoff: TPC-DS federated coverage + correctness

State of the work, facts only. Earlier phases (Rust cutover, N-K decorrelation,
cost-based optimizer, TPC-H fair benchmark) are in git history and in the
auto-memory; this document is the CURRENT TPC-DS federated push.

Test suite: **1240 passed, 3 skipped, 32 xfailed** (`POSTGRES_DB=duckpoc
/workspace/venv-fedq/bin/python -m pytest -q`).

---

## OPEN ISSUES (investigate next)

### q18: avg(decimal) differs by 0.01 cross-source - SUSPECTED TYPE CONVERSION

The one remaining MISMATCH. `q18` computes `avg(cast(<col> AS decimal(12,2)))`
under `GROUP BY ROLLUP(...)`; a ROLLUP subtotal row differs from pure DuckDB by
0.01 in the last decimal (e.g. agg2 `206.98` vs `206.99`, agg5 `-1022.42` vs
`-1022.41`).

NOT floating-point summation order: for float64 that effect is ~1e-10 on these
magnitudes, far below 0.01. The size points to a **precision/type-conversion
bug** - a decimal likely narrowed to float32 (or a scale truncated) somewhere in
the CROSS-SOURCE aggregation path. Evidence gathered:
- Single-source the engine matches DuckDB bit-for-bit: `avg(cast(cs_list_price
  AS decimal(12,2)))` over `catalog_sales` = `100.99702512344902` on both.
- The 0.01 gap appears only cross-source (fact on DuckDB, dims on Postgres,
  avg computed at the coordinator / DataFusion).
- Engine avg output type is `double`; DuckDB's `avg(cast(... as decimal))` is
  also `DOUBLE`. Result is deterministic run-to-run.
- Reproduce: `run_federated.py --scale-factor 0.1 --placements pg-dims --only 18`
  (correctness is vs pure DuckDB, so this is a real engine-vs-DuckDB diff).

Where to look: the type carried for the fetched decimal columns across the
Postgres/ADBC boundary and into the DataFusion aggregate (float32 vs float64 vs
decimal); the `cast(... AS decimal(12,2))` rendering in the pushed SQL vs at the
coordinator. Deferred by request.

---

## Current status (pg-dims, SF0.1) - VERIFIED full 99-query tally 2026-07-07

`PASS 91 | MISMATCH 1 (q18) | ERROR 7`, geomean 3.41x vs the federated
DuckDB timing oracle over the 91 passing queries. Full runs are SAFE (see the
MEMORY section below); the tally is from a complete 99-query run. Suite:
1251 passed, 3 skipped, 26 xfailed.

Remaining ERROR clusters (by cause):
- **ResourcesExhausted - 3**: q10/q35/q45 build a large cartesian product and
  now fail CLEANLY from the DataFusion 32GB pool itself ("Failed to allocate
  ... fedq_collect ... fair(pool_size: 32.0 GB)") - no watchdog kill needed.
  Their join condition is a DISJUNCTION with NO common conjunct (q45:
  `(ca_zip IN ...) OR (i_item_id IN subquery)`), so factoring cannot lift a
  hash key out; passing them needs disjunctive-subquery decorrelation.
- UnsupportedSQLError - 2 (q39 simple CASE, q70 window in WHERE).
- RuntimeError - 2 (q23 type_coercion; q86 window combined with
  GROUPING()/ROLLUP, a DataFusion physical-planner limitation).

## Scalar-subquery cardinality guard - DONE 2026-07-07 (q06/q14/q44/q54/q58)

fedqrs commit c33e9c8 + federated-query commit 8fdf1ff. The former
`PhysicalSingleRowGuard UnsupportedIR` cluster is gone. It took the guard
PLUS two upstream correctness bugs its runtime check exposed:

- **`single_row_guard` fragment** (fedqrs ir.rs/engine.rs; emitter in
  rust_ir.py): keyless = at most one row in TOTAL, keyed = at most one row
  per distinct key tuple. Violation = "Scalar subquery produced more than one
  row" (probe: `SELECT 1 FROM in_0 [GROUP BY keys] HAVING count(*) > 1`);
  otherwise the input passes through unchanged.
- **Subquery DISTINCT was silently dropped** (decorrelation.py
  `_peel_values_top`): peeling `SELECT DISTINCT v` into bare value
  expressions lost the flag, so q06's single-valued subquery fed 31 rows to
  the guard. An uncorrelated DISTINCT projection now stays whole as the value
  relation (`_peel_distinct_projection`); a correlated one fails fast.
- **Stacked-projection SELECT overwrite** (single_source_pushdown.py
  `_absorb_projection`): the inner projection's `_set_select` clobbered the
  outer rename (q54: `SELECT DISTINCT d_month_seq+1` lost its rename to
  `__subq_0_v0`). A stack now collapses only as a bijective pure-column
  rename (inner exprs under OUTER aliases, DISTINCT kept); anything else
  declines to the merge engine; a second SELECT list is refused outright.
- Also fixed alongside: cross-source `SELECT DISTINCT` silently returned
  duplicates (project fragment now carries `distinct`), and
  `IS [NOT] DISTINCT FROM` is in the IR operator vocabulary.

## MEMORY - DONE 2026-07-07 (fedqrs commit 5f62deb); full runs are safe

It took THREE pieces, and the third was the actual root cause of the server
OOMs:

1. **Shared 32GB pool** (hardcoded by request): one `RuntimeEnv` with
   `FairSpillPool(32GB)` + default `DiskManager` behind EVERY SessionContext -
   `engine.rs` `runtime_env()` / `memory_capped_context()` (collect_distinct,
   run_fragment) and `connectors.rs` parquet_ctx. Tracked allocations fail
   with ResourcesExhausted; sort + grouped aggregate spill to disk; joins do
   not spill (DataFusion gap vs DuckDB, which spills everything).
2. **Pool-tracked accumulation**: the pool alone did NOT catch q45 - operators
   only account their WORKING memory, and the exploding cross-join OUTPUT
   accumulated untracked. `collect_batches`/`collect_distinct` now stream via
   `collect_tracked()` (engine.rs), charging every accumulated batch to the
   pool via `MemoryConsumer.try_grow`. NOTE: `project_dataframe` (the collect
   path of project/hash-join/nested-loop fragments) initially kept a plain
   `collect()` and still evaded the pool; it now goes through
   `collect_tracked` too (commit c33e9c8), which is why q10/q35/q45 die from
   the pool rather than the watchdog.
3. **GIL release in `execute_ir`** (lib.rs): the old entry held the GIL for
   the entire native run, so the harness RSS-watchdog THREAD was frozen during
   every Rust blowup and could never fire in flight - THIS is how full runs
   OOMed the box despite the watchdog. `engine::execute` now returns
   `(SchemaRef, Vec<RecordBatch>)` and runs under `py.allow_threads`; the
   Arrow FFI export (raw pointers, not Send) is built back under the GIL.
   Verified: q45 dies at the 12GB watchdog with a clean exit-137 ERROR.

Environment facts (still true, for reference): no cgroup cap without root
(cgroup v2 mounted read-only, root cgroup `0::/`, no delegation; `cgexec` and
`systemd-run` missing). `prlimit`/`ulimit -v` cap VIRTUAL address space
(RLIMIT_AS), which the Rust engine over-reserves - the reason the harness uses
an RSS watchdog, not RLIMIT_AS.

## CROSS join + disjunction factoring (this round)

- **CROSS join emit** (`rust_ir` `_nested_loop_kind`): a CROSS join is a
  PhysicalNestedLoopJoin with join_type CROSS, no keys, no condition; the engine
  expresses a cartesian product as an INNER nested-loop join with an absent
  condition (run_nested_loop_join reads None as join_on with no predicates), so
  CROSS maps to inner. No Rust change. Fixed q08/q28/q88.
- **Factor common conjuncts out of EACH conjunct's OR** (`optimizer/rules.py`
  `_factor_filter_predicate`): it factored the WHOLE predicate, but a WHERE is a
  top-level AND (split_disjuncts saw one branch, did nothing). A disjunction is a
  CONJUNCT, so factor each conjunct. Lifts an equi-join repeated in every OR
  branch (q13) to a top-level conjunct -> hash key instead of a cartesian
  product. Fixed q13. Does NOT help q45/q48 (their OR has no common conjunct).

## Window functions - DONE (this round, +12)

The TPC-DS window family (sum(sum(x)) OVER (...) with GROUP BY) lands the
WindowExpr INSIDE PhysicalHashAggregate.aggregates - the SELECT list is fused
onto the aggregate, so the structured aggregate fragment could not express the
window ("expression WindowExpr not supported in IR"). Fix: a window over grouped
aggregates is valid SQL, so PhysicalHashAggregate.has_window_output() +
_aggregate_sql() render SELECT <outputs> FROM in_0 [GROUP BY <keys>] (columns
lowered via MergeResolver, like PhysicalWindow), and rust_ir emits it as a
raw_sql fragment. DataFusion evaluates it. Fixed q12/q20/q36/q47/q49/q51/q53/
q57/q63/q67/q89/q98. q86 remains (window + GROUPING()).

Set operations (UNION/INTERSECT/EXCEPT) are DONE - emitted via the raw_sql
escape hatch in `rust_ir` (`_emit_union` / `_emit_set_operation`); no Rust change.

## RuntimeError cluster - fully fixed (was 15, now 0)

All were column-naming / schema-consistency bugs:
- **ORDER BY sum(x) re-applied over the aggregate** (`binder.py`
  `_match_aggregate_output`): a bound sort key equal to an aggregate output
  expression now references that output column, not a FunctionCall recomputing
  sum over the aggregate's own output (which lacks raw x). Fixed q42/q85/q92/q96.
- **ORDER BY substring(x) re-applied over the projection** (`binder.py`
  `_match_projection_output`): same idea for a computed projection output; a
  plain-column key is left qualified so single-source ORDER BY still pushes
  down. Fixed q79.
- **PhysicalProjection.column_aliases() leaked the input's columns**
  (`physical.py`): it returned `self.input.column_aliases()` even when the
  projection drops/renames columns, so a sort ABOVE a projection resolved a
  passthrough self-join-renamed column (right_customer_id) to a name the
  4-column projection had dropped. Now it returns the projection's OUTPUT
  contract; its own expressions are typed against self.input.column_aliases()
  directly. Fixed q04/q11/q74/q84.
- **Binding schema must match its executed batches** (`fedqrs engine.rs`
  `collect_batches`): run_aggregate/raw_sql/sort/filter/limit set the binding
  schema from the LOGICAL df.schema(), but the batches from execution disagree -
  SUM widens decimal precision (Decimal(17,2)->Decimal(27,2); q16/q94/q95), and
  a UNION's branches disagree on column nullability so the batches disagree with
  EACH OTHER (q77). collect_batches now takes executed types from the first
  batch, widens every field to nullable, and re-schemas every batch to that one
  schema. Fixed q16/q94/q95/q77.

Debugged with a temporary FEDQRS_SCHEMA per-fragment schema trace in the Rust
engine (added then removed): it proved joins produced the right columns and
localized each bug to alias resolution or schema declaration, not execution.

## Benchmark setup (important)

- **Dedicated Postgres database per scale**: `pg_database_name(sf)` ->
  `tpcds_sf01` / `tpcds_sf1` / `tpcds_sf10` (generate.py). TPC-DS no longer
  shares `duckpoc` with TPC-H (they collided on the `customer` table; TPC-H's
  8-column customer shadowed TPC-DS's, breaking c_customer_sk binding). Reload
  after `make pg-start`: `load_postgres.py --scale-factor 0.1` (fills
  `tpcds_sf01`; the DB itself is created once via psycopg2 CREATE DATABASE).
- **Correctness vs pure DuckDB, timing vs the federated oracle** (split refs).
  The federated DuckDB oracle (Postgres attached) has scanner quirks that are
  NOT our bugs - q59 filter-pushdown dropped all rows (worked around with
  `SET pg_experimental_filter_pushdown=false`), q18 avg drift - so it is used
  ONLY for the timing baseline; correctness compares the engine against pure
  DuckDB over the same file (the canonical single-source answer).
- Each query runs in one isolated child (engine + both references together),
  memory-capped by an **RSS watchdog** (not RLIMIT_AS, which the Rust engine's
  virtual reservations tripped falsely - q72).

## Engine fixes made this session (all with tests, all committed)

- **Self-join output-name collision** (`physical.py _right_output_name`): a
  left-deep self-join of a CTE (q31 ss1..ss3) produced two `right_<name>`
  columns; now suffixed `_1, _2` until unique.
- **Positional ORDER BY** (`binder.py`): `ORDER BY <n>` resolves to the n-th
  output, not left as an integer literal (q62 vanished cross-source).
- **NULLIF / ROUND at the coordinator** (`expression_evaluator.py`): handlers
  added; the dialect's safe-division rewrite `a/b -> a/NULLIF(b,0)` no longer
  errors. NULLIF kept (not rewritten to CASE, which double-evaluates a).
- **Join read-set over-collection** (`parser.py _columns_for_join_table`): a
  join input's read-set now over-collects EVERY referenced column (the binder
  drops what a table lacks); an unqualified aggregate measure was being lost
  from the fact scan (q03 "No field named ss_ext_sales_price"). The parser does
  NOT attribute columns to tables - that is the binder's job once refs are
  qualified.
- **Reduction gate** (`rust_ir.py _probe_base_resolvable`): a join whose probe
  has no single injectable base emits a normal join instead of crashing on a
  None base (q96 AttributeError).
- **Case-insensitive table qualifiers** (`binder.py _resolve_qualified`): a
  `catalog` reference resolves against a `CATALOG` alias (q49).

## How to run

```
make pg-start                                        # Postgres in a container
cd benchmarks/tpcds
python generate.py --scale-factor 0.1                # DuckDB file (once)
python load_postgres.py --scale-factor 0.1           # into tpcds_sf01 (once)
PGUSER=postgres python run_federated.py --scale-factor 0.1 \
    --placements pg-dims --timeout 30 --report out.md
```

`run.py` is the single-source (pure DuckDB) variant. venv:
`/workspace/venv-fedq`. Engine (Rust) rebuild:
`PATH="$HOME/.cargo/bin:$PATH" VIRTUAL_ENV=/workspace/venv-fedq \
/workspace/venv-fedq/bin/maturin develop --release -m /workspace/fedqrs/Cargo.toml`.
