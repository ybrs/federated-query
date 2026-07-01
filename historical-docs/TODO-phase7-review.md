
> HISTORICAL SNAPSHOT. This is a running review log from the Phase 7/8 work; the
> pass/fail counts scattered through it (766/19, 743/48, "remaining 95", "125
> failing", etc.) are stale snapshots from different points in time and do NOT
> agree with each other by design. Current status is 809 passing / 0 failed /

> The local row-at-a-time Python operators now run vectorized in a reused
> in-memory **DuckDB coordinator** (`executor/merge_engine.py`). Migrated
> `execute()`s (all gated, row-loop fallback when no engine attached):
> LEFT/RIGHT/FULL/SEMI/ANTI stream left + materialize right), PhysicalHashAggregate
> (column aggregates, output cast to `schema()`), PhysicalSort (per-key NULLS
> FIRST/LAST), PhysicalUnion DISTINCT + PhysicalSetOperation INTERSECT/EXCEPT[ALL].
> **Deliberately NOT migrated:** PhysicalGroupedLimit (order-dependent) and
> PhysicalNestedLoopJoin (null-aware decorrelation condition over the joined
> No change to parser/binder/decorrelation/planner/pushdown. **Full design:
> `plan-physical-merge-engine.md`.** Remaining complement: **G9 v2.1**.

---

Findings from an adversarial review of the Phase 7 decorrelation work plus an
audit of the pre-existing engine. Every item marked VERIFIED was reproduced
against the live engine and cross-checked with PostgreSQL 17; repro scripts
for section A live under `/tmp/fedq_repro/`. Items marked READ are from code
inspection.

---

## STATUS (updated 2026-06-19, branch `phase8`)

766 passed / 19 failed (from 645/125 baseline); decorrelation suite 128 green.
(Total test count dropped earlier because the constant-folding feature and its
**Done: G4 set ops, G1 join breadth, G2 computed projections, G9 cross-source
dynamic filtering (first version), P1 projection pruning, P2 ADBC connector,
run vectorized in DuckDB), G6 date/time (EXTRACT, DATE_TRUNC, INTERVAL
arithmetic, CURRENT_DATE, AGE now parse, bind and push down), and G7 aggregate
FILTER + NATURAL/USING joins (single-source push).** Edge-case tests were
corrected to match real engine behavior (COALESCE operand count vs sqlglot's
this/expressions split; pushed user aliases; multi-statement injection is
rejected, not parsed; constant arithmetic is pushed unevaluated, not folded).
**Removed fedq-side constant folding / expression simplification
(`ExpressionSimplificationRule` + its rewriters): it baked Python float
semantics into pushed SQL and folded `x - x = 0`-style predicates to TRUE
(a NULL-correctness bug). Sources fold pushed exprs; the DuckDB merge engine
folds local ones; the Arrow evaluator handles pure-local `SELECT 1+2`.**
**Phase 1 (subqueries): decorrelation capability matrix inventoried
(`doc/decorrelation-capabilities.md`) and the six fail-fast gaps pinned with
tests. Phase 2a: single-source SEMI/ANTI joins now push as one correlated
re-correlating decorrelation's output and handling self-correlation aliasing.
Phase 2b: single-source scalar subqueries (correlated + uncorrelated, COUNT)
push as one query via inlined `(SELECT ...)` subqueries, with
execution-correctness tests vs the raw source; guard-wrapped (not-provably-
single-row) scalars stay local for the typed CardinalityViolationError.**
Remaining failures are **G3 CTEs (10); derived tables (Phase 2c); scalar-in-
HAVING (needs HAVING-clause pushdown); the nested-aggregate-via-subquery test;
and the decorrelation shape gaps (Sort/Filter/UNION body, deferred to the
dependent-join rewrite)**.
Verified
correctness/silent-fail items are checked off. PARTIAL = some sub-cases remain
(noted inline); DEFERRED = moderate/decision/perf/architectural, none are
silent corruption (they raise clean errors or are documented deviations).
(the bulk of the remaining 95 failures).

## 0. DO FIRST (agreed)

- [x] **0.1. Native EXPLAIN parsing via a custom sqlglot dialect.** Prototype
  VERIFIED (all forms parse with zero warnings, real ASTs).
  Add `federated_query/parser/dialect.py` with `FedQPostgres(Postgres)`:
  - `Tokenizer.KEYWORDS`: map `"EXPLAIN" -> TokenType.DESCRIBE` (the same
    mechanism sqlglot's MySQL dialect uses natively).
  - `Parser._parse_describe`: consume the optional Postgres-style
    `(FORMAT JSON, ...)` option list, then `_parse_statement()`; return
    `exp.Describe(this=<parsed stmt>)` with `format` set from the options.
  Then in `parser.py`: use the dialect for all parsing; handle
  `exp.Describe` directly in `ast_to_logical_plan` (build `Explain` from the
  already-parsed inner statement); DELETE the Command-fallback string
  machinery (`_is_explain_command`, `_extract_explain_parts`,
  "Falling back to parsing as a 'Command'" warning noise on every EXPLAIN,
  removes the string re-parse round trip, and closes the multi-statement
  `Block` fragility (B5) on the EXPLAIN path. Check `query_preprocessor` and
  any other `sqlglot.parse_one` callers use the same dialect.

- [x] **0.2. Kill the operator-map silent fails (E1) + the evaluator gaps
  behind the "simple" string tests.** VERIFIED by executing the queries:
  `name || 'x'` silently returns `[False, False]`; `ILIKE 'JACK%'` silently
  returns empty; `CONCAT(...)` and `CAST(...)` raise "Unsupported function".
  Fix package:
  - `parser.py _map_binary_op`: REMOVE the `.get(..., BinaryOpType.EQ)`
    (enum exists, never mapped); add ILIKE support. Same fail-fast for
    `_map_unary_op` (unknown silently becomes NOT today).
  - `expression_evaluator.py`: implement CONCAT and `||` (e.g.
    `pc.binary_join_element_wise` with NULL propagation); ILIKE via
    `pc.match_like(..., ignore_case=True)`.
  - `parser.py`: convert `exp.Cast` properly (keep the target type; emit a
    real cast node or typed expression) instead of dropping it through
    (root cause of G5).
  - Pushdown SQL for `||`/CONCAT/ILIKE follows from `to_sql()` once the

---


- [x] **A1. Self-join correlation misclassified.** VERIFIED.
  `decorrelation.py` `_collect_inner_aliases` (and `CorrelationAnalyzer._collect_tables`)
  add a Scan's `table_name` to the inner alias set even when the scan is
  aliased. SQL: `FROM emp e2` hides the name `emp`, so `emp.id` inside the
  subquery is an OUTER reference. Engine silently evaluates
  `e2.manager_id = e2.id` instead.
  Repro: `SELECT id FROM emp WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.manager_id = emp.id)`
  Fix: alias replaces table name in the inner set (only add `table_name` when no alias).

  `_apply_conjunct` routes NOT-wrapped subquery conjuncts through the
  flag-union path, which collapses UNKNOWN to FALSE; `NOT FALSE = TRUE` keeps
  rows SQL would drop. (`NOT IN`/`NOT EXISTS` are safe only because the parser
  folds negation into the node.)
  keeps the NULL row, PostgreSQL drops it.
  Fix: in WHERE context, rewrite `NOT(node)` by negating the node itself
  (swap SEMI/ANTI + null-aware condition), never via flags. Also fix the
  module docstring claim "WHERE-context rewrites are exact" (currently false).

- [x] **A3. EXISTS over a correlated GLOBAL aggregate returns wrong rows.** VERIFIED.
  A global aggregate yields one row even for empty input, so
  `EXISTS (SELECT COUNT(*) FROM i WHERE i.k = o.k)` is TRUE for every outer
  row. Key-widening turns it into a grouped aggregate where empty groups
  Fix: EXISTS whose subquery top is a global Aggregate is constant-TRUE per

- [x] **A4. OR-expansion distinct union merges legitimate duplicate source rows.** VERIFIED.
  `_expand_or` uses `Union(distinct=True)`; keyless tables lose row
  multiplicity. Real engines dedup by row identity, not value.
  engine 2 rows, PostgreSQL 3.
  Fix options: tag rows with a synthetic row-id before the union and dedup on
  it, or rewrite via flag-OR (`flag1 OR flag2` filter) once A2's flag
  semantics are NULL-correct.

- [x] **A5. `COUNT(DISTINCT ...)` (and SUM/AVG DISTINCT) ignored by local hash aggregate.** VERIFIED.
  `physical.py` accumulators never consult `FunctionCall.distinct`.
  (Pre-existing operator bug, but Phase 7 made it reachable.)

- [x] **A6. Hash-join key orientation by bare column name.** VERIFIED.
  `physical_planner.py` `_orient_key_pair` + `_extract_key_values` ignore
  `ColumnRef.table`. When both inputs expose both column names and the ON
  equality is written right-side-first, the join silently computes
  `left.b = right.a`.
  wrong rows.
  Fix: orient using table qualifiers/aliases, not bare names; make
  `_extract_key_values` qualifier-aware.

- [x] **A7. (PARTIAL) SingleRowGuard false positives.** VERIFIED.
  The guard checks ALL inner groups before the join:
  (a) duplicate inner keys that no outer row matches raise;
  (b) two inner rows with NULL keys raise, though NULL keys can never match.
  PostgreSQL succeeds in both cases.
  Fix minimum: skip NULL-containing keys in the guard. Full fix: enforce
  cardinality per matched outer row (post-join check or guard keyed by the
  semi-filtered key set).
  unmatched non-NULL duplicate keys still over-raise (needs the semi-filter
  or join-level single-match assertion).


- [x] **B1. CASE evaluates all branches eagerly.** VERIFIED.
  `expression_evaluator.py` `_eval_case` evaluates every branch over the full
  SQL must not evaluate the unguarded branch. Fix: evaluate branches under
  their condition mask (or mask inputs before kernel calls).

  `_rewrite_sort_with_subqueries` threads the scalar join above the SELECT
  projection where the correlation column is already projected away.
  Fix: plant the join below the projection (widen projection, prune after).

  Pulled HAVING conjuncts carrying aggregate calls expose `i.v` above the
  Aggregate where it does not exist. Fix: hoist the aggregate call first and
  pull the rewritten predicate, or raise a clean DecorrelationError.

- [x] **B4. (PARTIAL) Common shapes still rejected (DecorrelationError on valid SQL).** VERIFIED.
    no-op; strip it instead of failing on `Limit(Projection(...))`.
    `_peel_values_top` lacks a Sort case; GroupedLimit already preserves sort
    order beneath it, so peeling Sort+Limit together is implementable.
    can't distinguish keys it added itself from original GROUP BY keys.
    validation then raises "survived decorrelation".
    (dependent join); keep, but track here.
  EXISTS (rewritten onto a clean one-row placeholder Values). REMAINING:
  correlated scalar ORDER BY+LIMIT (latest-row-per-key), two correlation
  equalities over a global aggregate, and skip-level correlation.

- [x] **B5. Multi-statement input silently drops trailing statements.** VERIFIED.
  `parser.py` `_parse_one` returns `Block.expressions[0]` and discards the
  rest. `SELECT ...; SELECT ...` runs only the first. Fix: raise on
  multi-statement input.


- [x] **C1. Lenient scan-column filtering weakened bind-time typo detection in
  join queries.** VERIFIED. Single-table typos still raise at expression
  binding, but in join queries the multi-table fallback (`binder.py`
  `_bind_column_ref_multi_table` `return col_ref` on miss) lets unbound refs
  through to execution (KeyError / remote UndefinedColumn instead of
  BindingError). Fix the pre-existing fallback to raise; then lenient scan
  filtering is safe.
- [ ] **C2. (DEFERRED) Derived-table scope leak (pre-existing helpers).** `_contains_join`
  /`_extract_tables_from_tree` recurse through `SubqueryScan.input`, binding
  outer expressions against tables INSIDE the derived table. Accepts SQL
  PostgreSQL rejects. READ/partially VERIFIED.
- [ ] **C3. (DEFERRED) PG type mapping silent fallbacks.** Unknown OIDs (uuid, json,
  precision for money-grade decimals (documented but worth a decision). READ.
  correlation with name overlap would bind silently to the left, not error. READ.
- [x] **C5. `PhysicalGroupedLimit.column_aliases` defined twice** (second
  shadows first). READ.
  merges across types; unhashable nested types raise. READ.
- [ ] **C7. (DEFERRED) `_create_empty_batch` builds null-typed arrays** mismatching the
  declared schema. READ.
- [x] **C8. Generator expressions in `_execute_semi_anti` key tuples**
  (comprehension-style, against repo rule). READ.


  one PostgreSQL query PER OUTER ROW for NOT IN / null-aware / non-equi
  semi-anti joins. Fix: materialize the right side once (like the inner/outer
  NLJ path already does). VERIFIED (5 PG queries for a 4-row NOT IN).
- [ ] **D2. (DEFERRED) Flag-union executes the outer input and subquery twice per flag**;
  per disjunct. Future: mark-join operator.
- [ ] **D3. (DEFERRED) Row-at-a-time loops** in PhysicalUnion/SingleRowGuard/GroupedLimit;
  hash join yields single-row batches. Vectorize later.


- [x] **E1. (DONE via 0.2) `parser.py` `_map_binary_op`: unknown operators silently become `=`.**
  (`BinaryOpType.CONCAT` exists but is never mapped). Worst silent-fail in
  2 in remote SQL ('02' would silently mismatch). VERIFIED.
  SQL; injection vector. VERIFIED.
- [x] **E4. OFFSET without LIMIT silently discarded** (`_build_limit_clause`);
  `LimitPushdownRule` zeroes offsets it didn't push (wrong pagination).
  VERIFIED both.
  never dedupes; `_propagate_distinct` no-ops over joins (duplicate rows
  VERIFIED); scan-level DISTINCT applies to the scanned column set, not the
  projected one (VERIFIED).
- [x] **E6. Local aggregates silently return None** for unknown functions
  (STDDEV) and non-column args (`SUM(price * quantity)`). VERIFIED. Duplicate
  `_find_group_by_index` definition shadows the correct one and defaults to
  group key 0 (READ).
- [x] **E7. DuckDB `get_query_schema` types every column `pa.string()`**
  (ArrowInvalid schema mismatch VERIFIED); empty results carry fake schemas.
  `_normalize_count_distinct` renders `COUNT(region)` while the executed SQL
  has DISTINCT. VERIFIED.
- [ ] **E10. (DEFERRED) `query_preprocessor.after_execution`** silently skips renaming on
  count mismatch; `_resolve_source_for_column` returns None on ambiguity. READ.
- [x] **E11. `postgresql.connect()` wraps psycopg2 errors in ConnectionError**


  RecursionError. VERIFIED.
  filters merged into `scan.filters`; `_build_query` routes the whole
- [x] **F3. `PhysicalRemoteJoin._build_source`** emits per-side filters
  referencing the join alias INSIDE the derived table
  any remote join with a side filter. VERIFIED.
  local DESC sorts diverge from PostgreSQL null ordering. READ/VERIFIED.
- [ ] **F6. (DEFERRED) AggregatePushdownRule merges into scans that may carry
  limit/distinct** without guards; no capability check. READ.
- [x] **F7. PredicatePushdown `_extract_column_refs` returns empty set for
- [ ] **F8. (DEFERRED) Physical nodes are mutable and mutated** (`_plan_sort`,
  decide a policy. READ.

## G. Missing features (the 125 failing e2e_pushdown tests, categorized)

These tests assert PUSHDOWN SHAPES (the remote SQL sent to the datasource),
execution. All were failing before Phase 7.

  Replaced the narrow binary `_is_remote_join_candidate` path with a unified
  single-source SQL generator: `federated_query/optimizer/single_source_pushdown.py`
  (`SingleSourcePushdown`) renders any same-source subtree
  (projection / aggregate / left-deep join-tree / filtered scans) into ONE flat
  remote `SELECT`, surfaced as a new `PhysicalRemoteQuery` node. Invoked at the
  top of `PhysicalPlanner._plan_node`, so it fires for the maximal pushable
  subtree and leaves cross-source boundaries to local operators. Now pushes:
  multi-table (N-way) same-source joins, FULL OUTER, self-joins, non-equi/OR/
  computed join conditions, and top-level WHERE (WHERE pushes; HAVING stays
  local, matching the established aggregate convention). Several tests that
  enshrined the old 2-table-only / split-query / FULL-not-pushed behavior were
  updated to assert the improved single-query pushdown.
  REMAINING: NATURAL JOIN and `USING (...)` (3 tests in test_advanced_join_types)
  natural/using fields through parser+binder+generator; deferred (niche, and
  test_join_with_using_single_column even uses a column absent from one table,
  so it is shape-only). NOTE: the planner no longer constructs
  `PhysicalRemoteJoin` (the generator handles every single-source join), but it
  pushdown (query accelerator with locally-cached tables: push only the remote
  portion of a join). See `selective-pushdown.md` and the class comment.
  computed, non-aggregate projection (`UPPER(x)`, `a || b`, `price*quantity`,
  CASE/CAST) into the remote `SELECT` for single-table queries too (gated on a
  computed expression so plain bare-column scans keep the existing path). Also
  fixed two enabling bugs: `ColumnRef.to_sql` now quotes reserved-word
  and `Parser._convert_function_call` now collects every argument via the
  function's `arg_types` (NULLIF/typed funcs were dropping their 2nd arg).
  Test-helper `find_alias_expression`/`find_in_select` now unwrap parens
  (BinaryOp.to_sql parenthesizes for precedence).
  previously silently dropped). Needs parser+binder+planner support.
  parse to a binary `SetOperation` logical node (kind + distinct), bound with a
  branch-arity check. The pushdown rules (predicate/aggregate/order-by) recurse
  into branches so each collapses to a single scan; the planner emits one
  left-associative set ops reproduce the original nesting) when both branches
  share a source, folding a trailing ORDER BY/LIMIT into an outer
  `SELECT * FROM (...)`. Cross-source falls back to local `PhysicalUnion`
  (UNION) or `PhysicalSetOperation` (INTERSECT/EXCEPT, multiset semantics
  `"from_"`; cross-source fixture used nonexistent datasource names).
  NOTE: a UNION used as an IN-subquery *body* (test_subqueries
  subquery work, not set-op pushdown.
  missing pushdown.
  and `Interval` expression nodes (with visitor hooks) plus parser converters
  for `EXTRACT(field FROM src)`, `DATE_TRUNC` (Postgres normalises it to its
  `TimestampTrunc` alias; the parser re-materialises the canonical call and the
  `FedQPostgres` dialect now builds `exp.DateTrunc` directly), and `INTERVAL`
  literals; binder resolves the inner column of EXTRACT. AGE/CURRENT_DATE
  operand was blocking them. All push down via `to_sql`; 0 regressions.
  (2026-06-17).** FILTER: parser rewrites `agg(x) FILTER (WHERE p)` into
  aggregate node intact, reuses `CaseExpr` (no new node/binder/renderer work).
  NATURAL/USING: `Join` now carries `natural`/`using`; parser captures them
  pushdown preserve them, the single-source renderer emits `NATURAL JOIN` /
  `natural` flag. Cross-source NATURAL/USING (expand to common-column
  equi-join) deferred. The 6th aggregate test (nested-aggregate-via-subquery)
  correctly and return right answers but execute as a *local* join (2+ remote
  scans) while the test wants ONE pushed remote query; that bucket is a pushdown
  extension of G1 (push decorrelated SEMI/ANTI/scalar joins). ~4 are genuine
  decorrelation coverage gaps (subquery body topped by Filter/Sort; `SetOperation`
  body) and ~2 are binding gaps.
  **Strategic direction (per owner):** the goal is a *subquery-free* logical
  planner only ever sees a flat join/aggregate/set-op plan (DuckDB-style;
  Neumann & Kemper general dependent join). We then push the resulting joins
  like any other (G1). Physical subquery planning stays only as a last-resort
  fallback for genuinely un-decorrelatable cases. This is a large rewrite
  deferred. Full gap inventory, code/test references, and sequencing live in

  VERSION DONE (2026-06-12).** G9a + G9b shipped; `tests/e2e_pushdown/
  test_dynamic_filtering.py` verifies it end-to-end (probe carries
  `WHERE key IN (build keys)`; comma + explicit join; results unchanged).
  G9a: `PredicatePushdownRule._push_filter_below_join` folds a cross-side
  `ColumnRef = ColumnRef` equality into an INNER join's ON condition
  (`_is_equi_predicate`/`_merge_join_condition`) so comma/cross joins become
  hash joins (one same-source comma-join test updated: it now pushes as one
  query). G9b: `PhysicalHashJoin._maybe_reduce_probe` runs after the build
  hash table is built and calls `probe.apply_dynamic_filter(keys, values)`;
  `PhysicalScan` implements it (mutates `self.filters` with an `InList`; SQL is
  rebuilt at execute time). Guards: INNER-only (outer joins must keep
  non-matching probe rows), single-column keys, `_DYNAMIC_FILTER_MAX_KEYS`=2000
  cap with an info-level log on fallback, NULL keys already excluded (not
  **Build-side selection (heuristic) DONE (2026-06-12):** `_plan_join` now picks
  literal` filter (likely small) is built, so the *other* (big) side is the one
  reduced, regardless of FROM order. INNER-only (SEMI/ANTI/outer keep build_side
  "right" for correctness). Two supporting fixes: `PhysicalHashJoin` now emits
  matched rows in stable left-then-right order via `_join_matched_rows` (output
  no longer depends on which side is built), and `_mark_dynamic_filter` /
  EXPLAIN prefetch resolve the probe from `build_side`. EXPLAIN also shows the
  real build values (`IN (101, ...)`, capped at 5) by reading a few rows from
  the build side. NOTE: a stray method insertion briefly orphaned the
  non-equi-join fallback `return` in `_plan_join` (54 decorrelation tests went
  red with `NoneType.execute`); restored.
  Also fixed (duplicate-column family, found via real queries): the
  PostgreSQL connector decodes results positionally (duplicate result column
  single-source generator emits UNIQUE column aliases + a `(table,col)->name`
  alias map and expands bare `SELECT *` joins explicitly; `PhysicalHashJoin`
  key extraction resolves qualified keys through that map ("Field id exists 2
  times in schema").

  - [ ] **v2.1 Inject into `PhysicalRemoteQuery` probes.** Today only a *bare*
    `PhysicalScan` probe receives the IN filter (`apply_dynamic_filter` is a
    `PhysicalScan`-only override; the base returns False). When the probe is a
    pushed subtree (e.g. a same-source join already rendered as one remote
    pushdown is NOT reduced. Implement `PhysicalRemoteQuery.apply_dynamic_filter`
    (AND an `IN` into its AST / wrap it). Highest-value v2 item.
    IN list (`_literal_for_value` currently does int/float/str/bool only;
    others fall back to a full fetch).
  - [ ] **v2.4 See filters inside pushed subqueries for build-side choice.**
    `_choose_build_side`/`_has_selective_filter` only inspect
    `PhysicalScan.filters`, so a selective filter buried in a
    `PhysicalRemoteQuery` is invisible and that side can't be chosen as build.
    a hard-coded constant.
    outer-join cases where the probe is the inner side can also reduce.

  `selective-pushdown.md`):**
    list is the wrong tool; pick a merge / streamed / partitioned join instead
    of materializing keys (v1 just falls back to a full probe fetch).
    literal-equality heuristic with cardinality/stats to choose the build side
    and to decide whether dynamic filtering even beats the alternative.
    can push `BETWEEN min AND max` instead of an IN; v1 only helps equi joins.
  Original problem statement:
  When a join spans
  two data sources it cannot be pushed to one engine, so today BOTH sides are
  over the network. A real federated engine constrains the probe side with the
  build side's join keys (Trino calls this *dynamic filtering*; the literature,
  *semi-join reduction* / *sideways information passing*). This is the single
  most important optimization for cross-source usability and is currently
  absent (grep: no dynamic-filter/reducer/IN-injection infra anywhere).
  Observed:
  ```
  SELECT count(*) FROM postgres_prod.public.catalog C,
                       local_duckdb.main.catalog_access A
  -- local_duckdb:  SELECT "catalog_id" FROM catalog_access       (WHOLE TABLE)
  ```
  Wanted: `local_duckdb: SELECT "catalog_id" FROM catalog_access WHERE
  "catalog_id" IN (<keys from the postgres side>)`.
  Two coupled parts:
  - **G9a (prerequisite).** A comma/cross join with a two-sided equi-predicate
    in WHERE is left as `Filter(a.x=b.y)` over a condition-less `Join`
    (`PredicatePushdownRule._push_filter_below_join`, rules.py:383, only pushes
    single-side predicates), so it plans to a `PhysicalNestedLoopJoin`
    (Cartesian product) + filter instead of a hash join. Promote a two-sided
    equality into the join condition so it becomes a `PhysicalHashJoin` with
  - **G9b.** Execute the build side first, collect its distinct join-key
    SQL at runtime (a dynamic filter). Needs executor build-before-probe
    ordering and a `PhysicalScan` that accepts a runtime-injected predicate.
    Guards: build-side cardinality threshold (fall back to full fetch when the
    column keys only, probe side is a remote scan, NULL-key handling, and an
  Sequencing decision (2026-06-11): tackle **G1 + G2 first** (same-source join
  pushdown is foundational and shares the equi-key machinery G9a needs), then
  G9.

## P. Data-path performance (measured 2026-06-13)

Found while profiling a real cross-source query
wire to find 1 match. Two concrete, measured levers (microbench on 11,041 rows,
in the bundled `perf_files` test table; Rust harness in `/workspace/fedqrs`):

  Root cause was localized: projection pushdown *already* prunes the logical
  `_expand_star_select` ignored `scan.columns` and enumerated the whole catalog
  table. Fixed `_scan_output_columns` to emit each scan's pruned column list
  (falling back to the catalog only for a true `*`). Result for the 3-table
  repro: pushed query went from 8 wide columns (incl. `name`/`amount`/text) to
  semantics change, no regressions; regression test in
  `tests/e2e_decorrelation/test_duplicate_columns.py`.

  `driver: adbc` on a postgres datasource: `execute_query`/`get_query_schema`
  fetch through `adbc_driver_postgresql` (Arrow straight off the wire, no
  per-cell Python objects); metadata/stats stay on psycopg2. It's a true
  transaction holding locks. Correctness test:
  `tests/e2e_decorrelation/test_adbc_connector.py` (byte-identical to psycopg2,
  incl. nulls + schema).
  MEASURED (Python, 11,041 rows): non-uuid wide **psycopg2 20.6 ms vs adbc
  than the tiny fetch saves). So ADBC is a big win for wide/non-uuid results and
  universal default. The deeper fix (let uuid stay native instead of forcing
  string) is a larger engine change.
  Cross-language benchmark harness lives in `/workspace/fedqrs` (rust-postgres,
  per-query cost makes it slower on small results in both Python and Rust.

  high priority).** Every column type must be respected, not flattened to a
  lowest-common-denominator. Today the engine coerces aggressively and
  `_map_type` does substring matching (E8). Wanted: a real type system where a
  uuid is a uuid, a decimal is a decimal, an int32 is an int32, etc., preserved
  from source metadata through the catalog, the Arrow runtime types, comparisons,
  and results. Spans: catalog `_map_type`; psycopg2 `_OID_TO_ARROW` +
  `_column_values`; the ADBC `_normalize_table` (which only exists to *match*
  cross-source comparison/join-key compatibility (a pg uuid and a duckdb uuid
  must compare as uuids). NOTE: per the P-section profiling, this is a
  CORRECTNESS/cleanliness fix, not the cause of the slow cross-source query
  (that's P4/G9 v2.1).

  Chose **DuckDB as the local execution engine** (`executor/merge_engine.py`
  `MergeEngine`, reused per `Executor`, warmed at CLI startup). Migrated operator
  `execute()`s (gated, row-loop fallback): PhysicalHashJoin ALL shapes (INNER
  keeps the G9 build-materialize+probe-stream path; LEFT/RIGHT/FULL/SEMI/ANTI
  stream the left + materialize the right; DuckDB NULL/anti semantics verified ==
  row loop), PhysicalHashAggregate (column aggregates; output cast to `schema()`),
  single-placement pyarrow path), PhysicalUnion DISTINCT + PhysicalSetOperation
  INTERSECT/EXCEPT[ALL] (also fixes C6 type-coercion). NOT migrated: PhysicalGroupedLimit
  (order-dependent) and PhysicalNestedLoopJoin (null-aware condition over joined
  psycopg2 reads autocommit (no teardown lock-hang); non-INNER drains right side
  first (else nested joins exhaust the pool). 743 pass / 48 fail; decorrelation
  122/122. Tests: `test_merge_engine_streaming.py`, `test_merge_set_ops.py`. The
  original profiling/analysis below is retained for context.

  ORIGINAL PROBLEM STATEMENT (kept):
  PROFILED on the 3-table cross-source `count(*)`
  (total 24.5 ms): pg fetch of 11,041 rows = 5.1 ms (~20%), **local processing
  = 19.5 ms (~80%)**. `PhysicalHashJoin` builds a Python dict keyed by per-row
  `.as_py()` tuples, probes row-by-row, and `_join_rows` allocates a fresh
  intermediate row. Same shape in `PhysicalUnion`/`PhysicalSetOperation`/
  `PhysicalGroupedLimit`/aggregates. Paths to evaluate (see the discussion in
  the design notes):
    Arrow's vectorized C++ kernels. Incremental, low risk; must verify SQL
    semantics (NULL-aware anti-joins, the `right_` dup-name convention, the
    dynamic-filter hook).
    tables in a coordinator in-memory DuckDB (zero-copy) and run the post-
    pushdown plan there; vectorized joins/aggregates/sorts/window with correct
    SQL semantics for free (we already depend on DuckDB). Bigger architectural
    shift; the custom semi/anti + guards + dynamic-filter logic must integrate.
  Note: **G9 v2.1** (push the probe-side keys into the *pushed* remote query)
  sidesteps this for the common case by making the remote return ~1 row instead

## H. Test-suite bugs (cheap wins, ~28 of the 125)

- [x] **H1.** Orders fixture lacks `created_at` (7 failures: data_type_edge_cases,
  date_time_functions).
- [x] **H2.** Tests reference `P.status` but the products fixture has no
  `status` column (test_where_not_exists, test_exists_with_complex_predicates).
- [x] **H3.** Stale expectations: tests expect remote SQL WITHOUT user aliases;
  engine now correctly emits `AS "cnt"` (6+ in single_table_aggregations,
  null_handling).
- [x] **H4.** `FedQRuntime` has no `.explain()` API used by 2 tests.
- [x] **H5.** `test_query_on_empty_table_behavior` asserts literal `1 = 0`
  survives; engine correctly constant-folds to FALSE.
  (sqlglot represents FULL JOIN as side=FULL, kind=OUTER).
  now work (DID NOT RAISE).
- [x] **H8.** Tests query fixture columns that were never created
  (`"select"`, `"order id"`).
  DONE: added a reserved-word `"select"` column to the orders fixture and
  taught the preprocessor to quote reserved-word identifiers during star
  expansion (re-parse check, so `name`/`order` are not over-quoted); the
  four stale "remote-query shape" tests (spaces/wide/duplicate/aggregate)
  were corrected to assert the engine's internal-name + local-rename
  behaviour and the final result columns.

## Suggested order of attack

   fail-fast + CONCAT/`||`/ILIKE/CAST support.
   OFFSET): small fixes, large blast radius.
   joins and many pushdown tests.
   ~51 tests).
