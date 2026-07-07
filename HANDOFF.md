# Handoff: TPC-DS federated coverage + correctness

State of the work, facts only. Earlier phases (Rust cutover, N-K decorrelation,
cost-based optimizer, TPC-H fair benchmark) are in git history and in the
auto-memory; this document is the CURRENT TPC-DS federated push.

Test suite: **1240 passed, 3 skipped, 32 xfailed** (`POSTGRES_DB=duckpoc
/workspace/venv-fedq/bin/python -m pytest -q`).

---

## OPEN ISSUES

None. q18 is RESOLVED 2026-07-07 (commit 05b1dcd) and was never an engine
bug: the raw values are engine `206.98499999999999` vs oracle `206.985` -
diff 1.4e-14, pure float64 summation ORDER (a distributed plan and a single
engine sum in different orders) sitting exactly on a cent rounding boundary.
The comparator's fixed-decimal rounding AMPLIFIED the last-bit difference
into a visible cent (the earlier "not summation order, that effect is
~1e-10, far below 0.01" analysis missed that the COMPARISON does the
amplifying). Fix: compare.py matches cells whose rounded forms agree OR that
sit within a TIGHT relative tolerance (1e-9); real value bugs differ by far
more and still fail (pinned by tests/test_tpcds_compare.py).

---

## Current status (pg-dims, SF0.1) - VERIFIED full 99-query tally 2026-07-07

`PASS 99 | MISMATCH 0 | ERROR 0` - EVERY TPC-DS query passes federated.
Geomean 2.80x, totals 13.9s vs 4.2s. Suite: 1272 passed, 3 skipped,
25 xfailed. Reports are commit-named under `benchmarks/tpcds/reports/`.

TPC-H regression check 2026-07-07 (report-result-9a28f39.md): fedpgduck SF1
22/22 correct, 2145ms vs 1125ms = 1.91x - no regression vs the previous
report's 1.97x (5f123d0); the TPC-DS rounds did not open a gap.

## Perf round 1 - schema memoization DONE 2026-07-07 (commit 534b246)

Outlier diagnosis found TWO structural costs; the first is fixed:
- **build_ir schema recomputation (FIXED)**: schema()/column_aliases()
  recursed per parent with no caching - exponential in tree depth. q59 spent
  4572ms of a 4580ms run re-deriving schemas (engine execution: ~300ms).
  Fix: PhysicalPlanNode.__init_subclass__ wraps every subclass's
  schema()/column_aliases() in a per-instance once-cache; model_copy starts
  the copy with an empty cache. Invariant documented on the base class:
  structural fields never mutate after construction. Effect: geomean 3.44x
  -> 2.80x, totals 27.1s -> 13.9s; q59 3989 -> 158ms, q02 2552 -> 135ms,
  q64 3384 -> 398ms, q66 1057 -> 224ms.
- **CTE re-emission (NEXT)**: a multi-referenced CTE re-emits and re-executes
  its whole body per reference (rust_ir _emit_cte_scan documents it). q04
  emits year_total 18 TIMES (customer scanned 18x, 82 merge fragments) - now
  the top outlier at 1210ms/18.2x; also q74/q11/q75/q14/q31/q47/q57. Fix:
  emit each CTE body once into a binding (emitter cache by producer
  identity) + make Rust binding reads non-consuming (batches are Arc-cheap
  to clone; take_materialized currently REMOVES the binding).
- Remaining after that: q09 (108ms/13x, 15 scalar subqueries), q44
  (67ms/13x), q10/q35 (disjunctive plan phases 2/3).

## Disjunctive decorrelation - Phase 1 DONE 2026-07-07 (commit 1767f60)

Plan: `disjunctive-decorrelation-plan.md`. Diagnosis: the OR-of-subqueries
rewrite (SEMI/ANTI union split) was already correct; predicate pushdown
treated Union/SetOperation as OPAQUE, stranding the comma-join equalities
above the union, so each branch planned as a conditionless cross join
(q10/q35/q45's memory blowups).

- `rules.py _push_filter_into_set_operation`: distributes the conjuncts
  every branch can evaluate into EVERY branch (a deterministic predicate
  commutes with any set operation applied per branch); conjuncts a branch
  cannot evaluate (the rewrite's flag columns) stay above. Union also joined
  the `_push_down` recursion arm (was missing - a walker-descent stop).
- `pushdown.py available_columns`: Union/SetOperation arm = the INTERSECTION
  of branch columns. Without it a union exposed no columns, which also
  blocked the SEMI-join left-side descent above q10/q35's nested unions.
- q10 517ms / q35 572ms / q45 124ms at SF0.1. The union split still
  replicates the input per SEMI/ANTI pair; plan phases 2 (common-key OR of
  existentials -> one SEMI over a domain union) and 3 (DataFusion LeftMark
  joins) are now PERF refinements, to be judged against these numbers.

## Error round 2026-07-07 (commit b484808): q23/q39/q70/q86 fixed

- **HAVING / ORDER BY aggregate hoist** (binder `_hoist_aggregate_calls`): an
  aggregate or GROUPING() call in HAVING or ORDER BY matching no SELECT
  output cannot be recomputed above the aggregate; it becomes a hidden
  aggregate output (`__agg_N`) read by name, with a restore projection above
  (q23's `HAVING ... max(tpcds_cmax)`, q86's ORDER BY grouping-CASE).
  Single-source is unaffected (split_where_having substitutes back).
- **Two-stage grouping-window split** (physical.py
  `split_window_aggregate_sqls` + rust_ir): DataFusion cannot plan GROUPING()
  inside a window expression; the GROUP BY stage materializes every window
  operand as an output and the window stage runs over those columns
  (q70/q86 `rank() OVER (PARTITION BY grouping(a)+grouping(b), ...)`).
- **Window-rejection scoping** (parser `_reject_window_in_clause`): prunes at
  nested SELECT boundaries, so a ranked derived table inside a WHERE
  IN-subquery is legal (q70); a window used directly in the clause still
  fails fast (pinned by the safety sweep).
- **Simple CASE** (parser `_convert_case_expression`): lowers to the searched
  form with `operand = value` per branch (identical NULL semantics); a
  function-bearing operand fails fast (duplication would re-evaluate a
  volatile call per branch). q39.

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
