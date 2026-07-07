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

## Current status (pg-dims, SF0.1)

~`PASS 85 | MISMATCH 1 (q18) | ERROR ~14`. Last CLEAN full tally was PASS 81;
the CROSS-join round then added q08/q13/q28/q88 (verified per-query). A full
99-query tally was NOT re-run afterward ON PURPOSE - the remaining CROSS-join
queries materialize huge cartesian products and OOMed the box on repeated full
runs (see MEMORY section - the engine has no memory cap yet). Re-run only small
`--only` subsets until the DataFusion memory pool below is wired.

Remaining ERROR clusters (by cause) - RuntimeError and window clusters GONE:
- **CROSS-join OOM/timeout - ~4**: q45/q48/q10/q35 build a large cartesian
  product. Their join condition is a DISJUNCTION with NO common conjunct
  (q45: `(ca_zip IN ...) OR (i_item_id IN subquery)`), so factoring cannot lift
  a hash key out; needs the memory pool (to fail cleanly) and/or disjunctive-
  subquery decorrelation. q23 is a separate type_coercion.
- **UnsupportedIR**: `PhysicalSingleRowGuard` (scalar-subquery guard, ~3) and a
  few other physical nodes.
- UnsupportedSQLError - 2 (simple CASE, window in WHERE - q70).
- RuntimeError - 1 (q86: window combined with GROUPING()/ROLLUP, a DataFusion
  physical-planner limitation).

## MEMORY - cap every run; DataFusion has NO limit today (DO THIS FIRST)

The engine creates `SessionContext::new()` per fragment with NO runtime config,
so DataFusion has NO memory limit. A CROSS-join query (q45/q48/q10/q35) that
materializes a huge cartesian product allocates faster than the harness RSS
watchdog polls, so it can OOM the whole server. DO NOT run the full 99-query
tally until this is fixed; use `--only <subset>`.

Environment facts (checked 2026-07-07):
- No cgroup cap possible without root: cgroup v2 is mounted but /sys/fs/cgroup
  is root-owned and READ-ONLY, we are in the root cgroup `0::/` with no
  delegation (mkdir child cgroup = "Read-only file system"). `cgexec` and
  `systemd-run` are both MISSING. `prlimit`/`ulimit -v` exist but cap VIRTUAL
  address space per-process (RLIMIT_AS), which the Rust engine over-reserves -
  the reason the harness uses an RSS watchdog, not RLIMIT_AS.

Proper fix (in-engine, no root): configure a DataFusion `RuntimeEnv`:
- `RuntimeEnvBuilder::new().with_memory_pool(Arc::new(FairSpillPool::new(LIMIT)))`
  + default `DiskManager`, then `SessionContext::new_with_config_rt(cfg, rt)` for
  EVERY context (engine.rs run_fragment ~417/439). Make LIMIT an env var
  (e.g. FEDQRS_MEMORY_LIMIT, default ~a safe fraction of RAM).
- Effect: memory-aware operators reserve from the pool and fail with
  ResourcesExhausted instead of OOMing. DataFusion SPILLS sort + grouped
  aggregate to disk (may let some big queries succeed); it does NOT spill hash
  or nested-loop/CROSS joins (they error) - a real gap vs DuckDB, which spills
  everything. So cross-join queries fail cleanly (still ERROR, but no OOM).
- Caveat: the pool is COOPERATIVE accounting (tracks operator working memory,
  not accumulated bindings), so it is not a hard kernel cap - but it covers the
  join blowups that bit us.

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
