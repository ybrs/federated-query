# Handoff: TPC-DS federated coverage + correctness

State of the work, facts only. Earlier phases (Rust cutover, N-K decorrelation,
cost-based optimizer, TPC-H fair benchmark) are in git history and in the
auto-memory; this document is the CURRENT TPC-DS federated push.

Test suite: **1276 passed, 3 skipped, 25 xfailed** (`POSTGRES_DB=duckpoc
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
Geomean 2.39x, totals 10.5s vs 4.4s. Suite: 1276 passed, 3 skipped,
25 xfailed. Reports are commit-named under `benchmarks/tpcds/reports/`.

Disjunctive decorrelation is COMPLETE (disjunctive-decorrelation-plan.md):
phase 2 (commit e68149e) rewrites a same-key OR of positive existentials as
ONE SEMI join over a UNION ALL of the subquery key domains - q10 522 ->
143ms (1.49x), q35 567 -> 136ms (1.72x); taken from both the top-level-OR
and OR-conjunct entries; mixed/negative/multi-key shapes keep the flag
path. Phase 3 (LeftMark) declined by its decision gate (q45 ~120ms/3.1x on
the flag path).

TPC-H regression check 2026-07-07 (report-result-9a28f39.md): fedpgduck SF1
22/22 correct, 2145ms vs 1125ms = 1.91x - no regression vs the previous
report's 1.97x (5f123d0); the TPC-DS rounds did not open a gap.

## TPC-DS at SF1 (10x data) - 2026-07-07, PASS 99 | 0 | 0 holds

Infra: `tpcds_sf1` Postgres database created + loaded (store_sales 2.88M);
`benchmarks/tpcds/data/tpcds_sf1.duckdb` already existed. Full tally at SF1
(report-result-7983eee.md): `PASS 99 | MISMATCH 0 | ERROR 0`, geomean
1.86x, totals 23.4s vs 12.2s (1.91x) - matching TPC-H's fair-cell ratio.

THE SCALING ANSWER: the gaps do NOT grow with data - they COMPRESS.
Geomean 2.39x (SF0.1) -> 1.86x (SF1); 70/99 queries have a BETTER relative
ratio at SF1 (fixed per-query Python/orchestration overhead amortizes; the
data path scales). The SF0.1 "scalar-subquery island family" mostly
DISSOLVED at scale (q09 14.1x -> 3.7x, q28 8.5x -> 2.4x, q44 15.5x -> 6.4x)
- their small-scale ratios were fixed-overhead artifacts.

Queries that genuinely scale WORSE (the real perf backlog now):
q39 2.7x -> 11.3x (776ms), q05 4.5x -> 9.3x (424ms), q78 4.6x -> 7.9x
(1082ms), q04 4.2x -> 6.7x (1451ms), q79 2.3x -> 5.9x, q06 2.6x -> 5.3x,
q11 2.8x -> 5.3x (785ms). Diagnose these at SF1 before optimizing anything.

## TPC-DS at SF10 (100x) - 2026-07-07: PASS 95 | MISMATCH 0 | ERROR 4

Infra: tpcds_sf10 pg db loaded (store_sales 28.8M), tpcds_sf10.duckdb
(3.2GB) generated. Run with `--timeout 300 --memory-limit 40000` (the
default 12GB child cap would kill legitimate SF10 queries). Report:
report-result-1c4f848.md. Geomean 2.28x over the 95 measured; totals 118s
vs 55s (2.14x) - NO global blowup (q72 runs at 0.39x, 2.5x FASTER than
DuckDB). Suite: 1278 passed.

THE TWO REAL SF10 FINDINGS:
1. **Memory wall - 4 ERRORs**: q23/q67/q78 exhaust the 32GB pool in
   fedq_collect; q64 hits the 40GB RSS watchdog. The engine fully
   MATERIALIZES every fragment result and every source read as an Arrow
   binding; DuckDB streams and spills. Fixing means spillable/streamed
   bindings or aggressive pre-reduction - an architecture item, not a tweak.
2. **A super-linear family** whose ratio grows at every scale step
   (SF0.1 -> SF1 -> SF10): q05 4.5->9.3->24.3x, q39 2.7->11.3->19.2x,
   q06 2.6->5.3->19.8x, q31 3.3->3.8->19.5x, q58 ->16.9x, q79 ->15.3x,
   q44 ->13.2x, q70 ->12.2x. 67/95 queries got relatively worse SF1->SF10.
   Suspects (UNDIAGNOSED - profile at SF10 first): cross-source fact
   transfer volume, injections degrading to full reads, per-fragment
   materialization latency. This family is the top perf backlog; the
   SF0.1-era "island family" analysis is superseded.

Comparator hardening from SF10 (commit 1c4f848): order-only differences
with identical multisets are a MATCH - q18's data-NULL detail rows tie with
ROLLUP subtotal rows on every sort column (full-precision sets verified
IDENTICAL), q65/q71 tie on their whole key lists. Ties are legitimately
unordered; a mis-sort under ORDER BY..LIMIT still changes the surviving
set and fails.

Three MISMATCH-free scales now: SF0.1 99/0/0, SF1 99/0/0, SF10 95/0/4.

SF1 also exposed a REAL bug, fixed (fedqrs 83ca5cb): two pg read paths
could return a binding whose declared schema disagreed with its executed
batches ("Mismatch between schema and batches") - ADBC drops the NUMERIC
typmod on temp-join results, and fetch_parallel took the FIRST ctid
partition's schema while each partition normalizes NUMERIC scales from its
own rows (q64: an empty first partition said Decimal128(38,0), row-bearing
ones shipped (38,2); the temp-table path only engages above the 2000-key
inline-IN cap, so SF0.1 never hit it). reconcile_executed now re-derives
disagreeing decimal columns across ALL batches at both assemblies and fails
loudly on any non-decimal disagreement; MemTable registration failures now
name the binding and the exact field diff.

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
- **CTE re-emission (FIXED, commit 2b0d639 + fedqrs 4526e0d)**: a
  multi-referenced CTE re-emitted and re-executed its whole body per
  reference. _emit_cte_scan now caches the body binding by producer identity
  (the planner already shared ONE PhysicalCTE across references); the engine
  pre-counts each binding's reads and CLONES a shared binding Arc-shallow
  until its LAST consumer takes it (memory released where single-use
  released it). Effect: geomean 2.80x -> 2.48x, totals 13.9s -> 11.0s;
  q04 1210 -> 289ms (body ran 18x, now once), q14 711 -> 372ms, q11 532 ->
  171ms, q75 302 -> 157ms, q74 366 -> 123ms, q47 198 -> 106ms.
- Remaining outliers, all small absolute: q44 82ms/15.5x, q09 122ms/14.1x,
  q28 84ms/8.5x (many scalar subqueries over one table - the island-breaking
  family, the next candidate), then q93/q41/q78 at 4.6-6.2x under 200ms.

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
   with ResourcesExhausted. Spill support VERIFIED in the DF54 sources:
   sort, grouped aggregate, NESTED-LOOP join (block-spill fallback), and
   sort-merge join (buffered side) all spill via the default OsTmpDirectory
   DiskManager; HASH JOIN builds and CROSS joins do NOT (they error). The
   engine's own binding accumulation (fedq_collect) is accounted but has NO
   spill path - that is where the SF10 memory wall lives.
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
