# Phase 7 Review — TODO

> ## ★ Physical Merge Engine (DuckDB local execution) — **DONE (2026-06-15)**
> The local row-at-a-time Python operators now run vectorized in a reused
> in-memory **DuckDB coordinator** (`executor/merge_engine.py`). Migrated
> `execute()`s (all gated, row-loop fallback when no engine attached):
> PhysicalHashJoin (ALL shapes — INNER keeps G9 build-materialize+probe-stream;
> LEFT/RIGHT/FULL/SEMI/ANTI stream left + materialize right), PhysicalHashAggregate
> (column aggregates, output cast to `schema()`), PhysicalSort (per-key NULLS
> FIRST/LAST), PhysicalUnion DISTINCT + PhysicalSetOperation INTERSECT/EXCEPT[ALL].
> **Deliberately NOT migrated:** PhysicalGroupedLimit (order-dependent) and
> PhysicalNestedLoopJoin (null-aware decorrelation condition over the joined
> schema — side-resolving renderer too risky) — both documented in code.
> No change to parser/binder/decorrelation/planner/pushdown. **Full design:
> `plan-physical-merge-engine.md`.** Remaining complement: **G9 v2.1**.

---

Findings from an adversarial review of the Phase 7 decorrelation work plus an
audit of the pre-existing engine. Every item marked VERIFIED was reproduced
against the live engine and cross-checked with PostgreSQL 17; repro scripts
for section A live under `/tmp/fedq_repro/`. Items marked READ are from code
inspection.

---

## STATUS (updated 2026-06-18, branch `phase8`)

739 passed / 31 failed (from 645/125 baseline); decorrelation suite green.
(Total test count dropped because the constant-folding feature and its two
dedicated test files were removed — see below.)
**Done: G4 set ops, G1 join breadth, G2 computed projections, G9 cross-source
dynamic filtering (first version), P1 projection pruning, P2 ADBC connector,
the Physical Merge Engine (P4 — all local operators except GroupedLimit/NLJ now
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
Remaining failures are **G3 CTEs (10), subqueries/G8 (20, deferred — see
`decorrelation-gaps.md`), and the nested-aggregate-via-subquery test
(derived-table-in-FROM pushdown)** — all the nested-structure pushdown work.
Verified
correctness/silent-fail items are checked off. PARTIAL = some sub-cases remain
(noted inline); DEFERRED = moderate/decision/perf/architectural, none are
silent corruption (they raise clean errors or are documented deviations).
Repro harness: `/tmp/fedq_repro/` (POSTGRES_DB=test_db). G1–G8 = Phase 8 work
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
  `_split_option_clause`, `_parse_explain_options` string scanning — keep the
  option-word → `ExplainFormat` mapping). Side effects: kills the
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
    default — raise on unknown operators; map `DPipe` → `BinaryOpType.CONCAT`
    (enum exists, never mapped); add ILIKE support. Same fail-fast for
    `_map_unary_op` (unknown silently becomes NOT today).
  - `expression_evaluator.py`: implement CONCAT and `||` (e.g.
    `pc.binary_join_element_wise` with NULL propagation); ILIKE via
    `pc.match_like(..., ignore_case=True)`.
  - `parser.py`: convert `exp.Cast` properly (keep the target type; emit a
    real cast node or typed expression) instead of dropping it through
    `_convert_function_call` — fixes the invalid `CAST(quantity)` remote SQL
    (root cause of G5).
  - Pushdown SQL for `||`/CONCAT/ILIKE follows from `to_sql()` once the
    operators exist — re-check the string-function pushdown tests after.

---

## A. Phase 7 work — verified correctness bugs (fix first)

- [x] **A1. Self-join correlation misclassified.** VERIFIED.
  `decorrelation.py` `_collect_inner_aliases` (and `CorrelationAnalyzer._collect_tables`)
  add a Scan's `table_name` to the inner alias set even when the scan is
  aliased. SQL: `FROM emp e2` hides the name `emp`, so `emp.id` inside the
  subquery is an OUTER reference. Engine silently evaluates
  `e2.manager_id = e2.id` instead.
  Repro: `SELECT id FROM emp WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e2.manager_id = emp.id)`
  → engine `[]`, PostgreSQL `[(1,)]`.
  Fix: alias replaces table name in the inner set (only add `table_name` when no alias).

- [x] **A2. `NOT (<subquery predicate>)` in WHERE collapses UNKNOWN→FALSE.** VERIFIED.
  `_apply_conjunct` routes NOT-wrapped subquery conjuncts through the
  flag-union path, which collapses UNKNOWN to FALSE; `NOT FALSE = TRUE` keeps
  rows SQL would drop. (`NOT IN`/`NOT EXISTS` are safe only because the parser
  folds negation into the node.)
  Repro: `WHERE NOT (o.x = ANY (SELECT v FROM vals))` with NULL `o.x` → engine
  keeps the NULL row, PostgreSQL drops it.
  Fix: in WHERE context, rewrite `NOT(node)` by negating the node itself
  (swap SEMI/ANTI + null-aware condition), never via flags. Also fix the
  module docstring claim "WHERE-context rewrites are exact" (currently false).

- [x] **A3. EXISTS over a correlated GLOBAL aggregate returns wrong rows.** VERIFIED.
  A global aggregate yields one row even for empty input, so
  `EXISTS (SELECT COUNT(*) FROM i WHERE i.k = o.k)` is TRUE for every outer
  row. Key-widening turns it into a grouped aggregate where empty groups
  vanish → SEMI join misses non-matching outer rows.
  Fix: EXISTS whose subquery top is a global Aggregate is constant-TRUE per
  outer row (no grouping needed) — or decorrelate via the scalar LEFT-join path.

- [x] **A4. OR-expansion distinct union merges legitimate duplicate source rows.** VERIFIED.
  `_expand_or` uses `Union(distinct=True)`; keyless tables lose row
  multiplicity. Real engines dedup by row identity, not value.
  Repro: table with duplicate `(1,1)` rows; `WHERE a IN (...) OR a = 2` →
  engine 2 rows, PostgreSQL 3.
  Fix options: tag rows with a synthetic row-id before the union and dedup on
  it, or rewrite via flag-OR (`flag1 OR flag2` filter) once A2's flag
  semantics are NULL-correct.

- [x] **A5. `COUNT(DISTINCT ...)` (and SUM/AVG DISTINCT) ignored by local hash aggregate.** VERIFIED.
  `physical.py` accumulators never consult `FunctionCall.distinct`.
  Decorrelated subquery aggregates run locally → silently counts duplicates.
  (Pre-existing operator bug, but Phase 7 made it reachable.)

- [x] **A6. Hash-join key orientation by bare column name.** VERIFIED.
  `physical_planner.py` `_orient_key_pair` + `_extract_key_values` ignore
  `ColumnRef.table`. When both inputs expose both column names and the ON
  equality is written right-side-first, the join silently computes
  `left.b = right.a`.
  Repro: cross-datasource `JOIN ... ON d.b = p.a` with shared column names →
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
  STATUS: minimum done (case b — NULL keys skipped). REMAINING: case (a),
  unmatched non-NULL duplicate keys still over-raise (needs the semi-filter
  or join-level single-match assertion).

## B. Phase 7 work — errors raised on valid SQL (robustness)

- [x] **B1. CASE evaluates all branches eagerly.** VERIFIED.
  `expression_evaluator.py` `_eval_case` evaluates every branch over the full
  batch; `CASE WHEN id <> 1 THEN 10/(id-1) ELSE 0 END` → divide-by-zero where
  SQL must not evaluate the unguarded branch. Fix: evaluate branches under
  their condition mask (or mask inputs before kernel calls).

- [x] **B2. ORDER BY a correlated scalar subquery → runtime KeyError.** VERIFIED.
  `_rewrite_sort_with_subqueries` threads the scalar join above the SELECT
  projection where the correlation column is already projected away.
  Fix: plant the join below the projection (widen projection, prune after).

- [x] **B3. Correlated HAVING (`HAVING SUM(i.v) > o.x`) → obscure KeyError.** VERIFIED.
  Pulled HAVING conjuncts carrying aggregate calls expose `i.v` above the
  Aggregate where it does not exist. Fix: hoist the aggregate call first and
  pull the rewritten predicate, or raise a clean DecorrelationError.

- [x] **B4. (PARTIAL) Common shapes still rejected (DecorrelationError on valid SQL).** VERIFIED.
  - `EXISTS (... WHERE corr LIMIT 1)` — LIMIT inside EXISTS is a semantic
    no-op; strip it instead of failing on `Limit(Projection(...))`.
  - Correlated scalar with `ORDER BY ... LIMIT 1` (latest-row-per-key idiom) —
    `_peel_values_top` lacks a Sort case; GroupedLimit already preserves sort
    order beneath it, so peeling Sort+Limit together is implementable.
  - Two correlation equalities over a global aggregate — `_add_group_key`
    can't distinguish keys it added itself from original GROUP BY keys.
  - FROM-less `SELECT EXISTS (SELECT ...)` — Values rows are never rewritten;
    validation then raises "survived decorrelation".
  - Skip-level correlation (two levels up) — documented future work
    (dependent join); keep, but track here.
  STATUS: DONE — EXISTS-with-LIMIT/Sort/Projection peel; FROM-less SELECT
  EXISTS (rewritten onto a clean one-row placeholder Values). REMAINING:
  correlated scalar ORDER BY+LIMIT (latest-row-per-key), two correlation
  equalities over a global aggregate, and skip-level correlation.

- [x] **B5. Multi-statement input silently drops trailing statements.** VERIFIED.
  `parser.py` `_parse_one` returns `Block.expressions[0]` and discards the
  rest. `SELECT ...; SELECT ...` runs only the first. Fix: raise on
  multi-statement input.

## C. Phase 7 work — silent-fail / style items

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
  arrays, interval...) → `pa.string()`; NUMERIC→float64 silently loses
  precision for money-grade decimals (documented but worth a decision). READ.
- [ ] **C4. (DEFERRED) `_rewrite_inner_join_condition` comment overpromises** — right-side
  correlation with name overlap would bind silently to the left, not error. READ.
- [x] **C5. `PhysicalGroupedLimit.column_aliases` defined twice** (second
  shadows first). READ.
- [ ] **C6. (DEFERRED) PhysicalUnion distinct dedup uses `as_py()` tuples** — `1 == 1.0`
  merges across types; unhashable nested types raise. READ.
- [ ] **C7. (DEFERRED) `_create_empty_batch` builds null-typed arrays** mismatching the
  declared schema. READ.
- [x] **C8. Generator expressions in `_execute_semi_anti` key tuples**
  (comprehension-style, against repo rule). READ.

## D. Phase 7 work — performance landmines (flagged, not bugs)

- [ ] **D1. (DEFERRED) Nested-loop SEMI/ANTI re-executes the right side per left row** —
  one PostgreSQL query PER OUTER ROW for NOT IN / null-aware / non-equi
  semi-anti joins. Fix: materialize the right side once (like the inner/outer
  NLJ path already does). VERIFIED (5 PG queries for a 4-row NOT IN).
- [ ] **D2. (DEFERRED) Flag-union executes the outer input and subquery twice per flag**;
  N flags in one projection → 2^N branches. OR-expansion re-executes input
  per disjunct. Future: mark-join operator.
- [ ] **D3. (DEFERRED) Row-at-a-time loops** in PhysicalUnion/SingleRowGuard/GroupedLimit;
  hash join yields single-row batches. Vectorize later.

## E. Pre-existing engine — silent-fail violations (rule #1)

- [x] **E1. (DONE via 0.2) `parser.py` `_map_binary_op`: unknown operators silently become `=`.**
  VERIFIED: `ILIKE` → silently empty results; `||` → boolean garbage
  (`BinaryOpType.CONCAT` exists but is never mapped). Worst silent-fail in
  the codebase. Same pattern in `_map_unary_op` (unknown → NOT).
- [x] **E2. `_convert_literal` ignores `lit.is_string`** — `'2'` becomes integer
  2 in remote SQL ('02' would silently mismatch). VERIFIED.
- [x] **E3. No string escaping in `Literal.to_sql`** — `O''Brien` breaks remote
  SQL; injection vector. VERIFIED.
- [x] **E4. OFFSET without LIMIT silently discarded** (`_build_limit_clause`);
  `LimitPushdownRule` zeroes offsets it didn't push (wrong pagination).
  VERIFIED both.
- [x] **E5. DISTINCT is decorative locally** — `PhysicalProjection.distinct`
  never dedupes; `_propagate_distinct` no-ops over joins (duplicate rows
  VERIFIED); scan-level DISTINCT applies to the scanned column set, not the
  projected one (VERIFIED).
- [x] **E6. Local aggregates silently return None** for unknown functions
  (STDDEV) and non-column args (`SUM(price * quantity)`). VERIFIED. Duplicate
  `_find_group_by_index` definition shadows the correct one and defaults to
  group key 0 (READ).
- [x] **E7. DuckDB `get_query_schema` types every column `pa.string()`**
  (same stub fixed for PG in Phase 7) → FULL OUTER join crashes
  (ArrowInvalid schema mismatch VERIFIED); empty results carry fake schemas.
- [x] **E8. `catalog._map_type` substring matching** mis-maps DATETIME→DATE,
  POINT→INTEGER; default VARCHAR fall-through. READ.
- [x] **E9. EXPLAIN misrepresents COUNT(DISTINCT)** —
  `_normalize_count_distinct` renders `COUNT(region)` while the executed SQL
  has DISTINCT. VERIFIED.
- [ ] **E10. (DEFERRED) `query_preprocessor.after_execution`** silently skips renaming on
  count mismatch; `_resolve_source_for_column` returns None on ambiguity. READ.
- [x] **E11. `postgresql.connect()` wraps psycopg2 errors in ConnectionError**
  — explicit CLAUDE.md anti-pattern; `get_table_statistics` catches → None. READ.

## F. Pre-existing engine — correctness risks

- [x] **F1. PredicatePushdownRule infinite recursion** — AND-split ↔
  `_merge_filters` ping-pong on conjunctive HAVING over non-pushable input →
  RecursionError. VERIFIED.
- [x] **F2. WHERE+HAVING merged into one scan predicate** — post-aggregate
  filters merged into `scan.filters`; `_build_query` routes the whole
  conjunction to WHERE or HAVING wholesale → invalid remote SQL. VERIFIED.
- [x] **F3. `PhysicalRemoteJoin._build_source`** emits per-side filters
  referencing the join alias INSIDE the derived table
  (`(SELECT * FROM products WHERE p.category=...) AS p`) → Binder Error on
  any remote join with a side filter. VERIFIED.
- [x] **F4. PhysicalSort drops NULLS FIRST/LAST** — node has no nulls field;
  local DESC sorts diverge from PostgreSQL null ordering. READ/VERIFIED.
- [x] **F5. MIN/MAX coerce through float()** — `MIN(status)` on strings crashes
  locally; SUM(int)→float64 contradicts `FunctionCall.get_type` BIGINT. VERIFIED.
- [ ] **F6. (DEFERRED) AggregatePushdownRule merges into scans that may carry
  limit/distinct** without guards; no capability check. READ.
- [x] **F7. PredicatePushdown `_extract_column_refs` returns empty set for
  CaseExpr/InList/Between/subqueries** → vacuously-true pushability. READ.
- [ ] **F8. (DEFERRED) Physical nodes are mutable and mutated** (`_plan_sort`,
  `_propagate_distinct`, schema caching) while logical nodes are frozen —
  decide a policy. READ.

## G. Missing features (the 125 failing e2e_pushdown tests, categorized)

These tests assert PUSHDOWN SHAPES (the remote SQL sent to the datasource),
not result correctness — many produce correct results today via local
execution. All were failing before Phase 7.

- [x] **G1. Join pushdown breadth — DONE except NATURAL/USING (3 tests).**
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
  — the logical `Join` node has only `condition`, so these need new
  natural/using fields through parser+binder+generator; deferred (niche, and
  test_join_with_using_single_column even uses a column absent from one table,
  so it is shape-only). NOTE: the planner no longer constructs
  `PhysicalRemoteJoin` (the generator handles every single-source join), but it
  is **intentionally retained** — not deleted — for future selective/partial
  pushdown (query accelerator with locally-cached tables: push only the remote
  portion of a join). See `selective-pushdown.md` and the class comment.
- [x] **G2. Computed projection pushdown — DONE.** The same generator pushes a
  computed, non-aggregate projection (`UPPER(x)`, `a || b`, `price*quantity`,
  CASE/CAST) into the remote `SELECT` for single-table queries too (gated on a
  computed expression so plain bare-column scans keep the existing path). Also
  fixed two enabling bugs: `ColumnRef.to_sql` now quotes reserved-word
  identifiers (memoized re-parse check — `"select"` was emitting invalid SQL),
  and `Parser._convert_function_call` now collects every argument via the
  function's `arg_types` (NULLIF/typed funcs were dropping their 2nd arg).
  Test-helper `find_alias_expression`/`find_in_select` now unwrap parens
  (BinaryOp.to_sql parenthesizes for precedence).
- [ ] **G3. CTEs (10 tests)** — parser raises (fail-fast added in Phase 7;
  previously silently dropped). Needs parser+binder+planner support.
- [x] **G4. Set operations (10 tests)** — DONE. UNION/INTERSECT/EXCEPT now
  parse to a binary `SetOperation` logical node (kind + distinct), bound with a
  branch-arity check. The pushdown rules (predicate/aggregate/order-by) recurse
  into branches so each collapses to a single scan; the planner emits one
  `PhysicalRemoteSetOp` (remote SQL built by combining branch ASTs — sqlglot's
  left-associative set ops reproduce the original nesting) when both branches
  share a source, folding a trailing ORDER BY/LIMIT into an outer
  `SELECT * FROM (...)`. Cross-source falls back to local `PhysicalUnion`
  (UNION) or `PhysicalSetOperation` (INTERSECT/EXCEPT, multiset semantics
  verified vs PostgreSQL). Two test-suite bugs fixed (`args.get("from")` →
  `"from_"`; cross-source fixture used nonexistent datasource names).
  NOTE: a UNION used as an IN-subquery *body* (test_subqueries
  `test_subquery_with_union`) still raises a clean BindingError — that is G8
  subquery work, not set-op pushdown.
- [ ] **G5. CAST broken (8 tests)** — parser drops the target type via generic
  function conversion → emits invalid `CAST(quantity)`. Real bug, not just
  missing pushdown.
- [x] **G6. Date/time gaps (7 tests) — DONE (2026-06-17).** Added `Extract`
  and `Interval` expression nodes (with visitor hooks) plus parser converters
  for `EXTRACT(field FROM src)`, `DATE_TRUNC` (Postgres normalises it to its
  `TimestampTrunc` alias; the parser re-materialises the canonical call and the
  `FedQPostgres` dialect now builds `exp.DateTrunc` directly), and `INTERVAL`
  literals; binder resolves the inner column of EXTRACT. AGE/CURRENT_DATE
  already flowed through the generic function-call path — only the `INTERVAL`
  operand was blocking them. All push down via `to_sql`; 0 regressions.
- [x] **G7. Aggregate FILTER clause + NATURAL/USING joins (5 tests) — DONE
  (2026-06-17).** FILTER: parser rewrites `agg(x) FILTER (WHERE p)` into
  `agg(CASE WHEN p THEN x END)` (`THEN 1` for `COUNT(*)`) — portable, keeps the
  aggregate node intact, reuses `CaseExpr` (no new node/binder/renderer work).
  NATURAL/USING: `Join` now carries `natural`/`using`; parser captures them
  (previously silently dropped → unconditioned cross join), decorrelation and
  pushdown preserve them, the single-source renderer emits `NATURAL JOIN` /
  `JOIN … USING (…)`, and `parse_query` marks NATURAL joins with an explicit
  `natural` flag. Cross-source NATURAL/USING (expand to common-column
  equi-join) deferred. The 6th aggregate test (nested-aggregate-via-subquery)
  is derived-table-in-FROM pushdown — folded into the nested-structure work.
- [ ] **G8. Subqueries (20 `test_subqueries` tests) — DEFERRED (big task).**
  Re-triaged: ~14 of these are NOT decorrelation failures — they decorrelate
  correctly and return right answers but execute as a *local* join (2+ remote
  scans) while the test wants ONE pushed remote query; that bucket is a pushdown
  extension of G1 (push decorrelated SEMI/ANTI/scalar joins). ~4 are genuine
  decorrelation coverage gaps (subquery body topped by Filter/Sort; `SetOperation`
  body) and ~2 are binding gaps.
  **Strategic direction (per owner):** the goal is a *subquery-free* logical
  plan — fully unnest every subquery during logical planning so the physical
  planner only ever sees a flat join/aggregate/set-op plan (DuckDB-style;
  Neumann & Kemper general dependent join). We then push the resulting joins
  like any other (G1). Physical subquery planning stays only as a last-resort
  fallback for genuinely un-decorrelatable cases. This is a large rewrite
  (pattern-based → general dependent-join decorrelation), so G8 is intentionally
  deferred. Full gap inventory, code/test references, and sequencing live in
  **`decorrelation-gaps.md`** — start there before picking this up.

- [x] **G9. Cross-source dynamic filtering / semi-join reduction — FIRST
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
  indexed in the hash table), empty build ⇒ no probe.
  **Build-side selection (heuristic) DONE (2026-06-12):** `_plan_join` now picks
  the build via `_choose_build_side` — the side whose scan has a `column =
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
  names no longer collapse — "Arrays were not all the same length"); the
  single-source generator emits UNIQUE column aliases + a `(table,col)->name`
  alias map and expands bare `SELECT *` joins explicitly; `PhysicalHashJoin`
  key extraction resolves qualified keys through that map ("Field id exists 2
  times in schema").

  **G9 v2 — incremental coverage (no cost model required):**
  - [ ] **v2.1 Inject into `PhysicalRemoteQuery` probes.** Today only a *bare*
    `PhysicalScan` probe receives the IN filter (`apply_dynamic_filter` is a
    `PhysicalScan`-only override; the base returns False). When the probe is a
    pushed subtree (e.g. a same-source join already rendered as one remote
    SELECT) it gets nothing — so a cross-source join whose big side is itself a
    pushdown is NOT reduced. Implement `PhysicalRemoteQuery.apply_dynamic_filter`
    (AND an `IN` into its AST / wrap it). Highest-value v2 item.
  - [ ] **v2.2 Composite keys** — `(a, b) IN ((..),(..))`; v1 is single-column.
  - [ ] **v2.3 More literal types** — date / timestamp / decimal / uuid in the
    IN list (`_literal_for_value` currently does int/float/str/bool only;
    others fall back to a full fetch).
  - [ ] **v2.4 See filters inside pushed subqueries for build-side choice.**
    `_choose_build_side`/`_has_selective_filter` only inspect
    `PhysicalScan.filters`, so a selective filter buried in a
    `PhysicalRemoteQuery` is invisible and that side can't be chosen as build.
  - [ ] **v2.5 Config-drive the threshold** — `_DYNAMIC_FILTER_MAX_KEYS=2000` is
    a hard-coded constant.
  - [ ] **v2.6 Safe outer cases** — v1 is INNER-only; SEMI/ANTI and the
    outer-join cases where the probe is the inner side can also reduce.

  **G9 v3 — optimization phase (needs the cost model / new operators; see
  `selective-pushdown.md`):**
  - [ ] **v3.1 Large build side → don't use IN.** Above some cardinality an IN
    list is the wrong tool; pick a merge / streamed / partitioned join instead
    of materializing keys (v1 just falls back to a full probe fetch).
  - [ ] **v3.2 Full cost-based build-side selection** — replace the
    literal-equality heuristic with cardinality/stats to choose the build side
    and to decide whether dynamic filtering even beats the alternative.
  - [ ] **v3.3 Range (min/max) pushdown for non-equi joins** — band/range joins
    can push `BETWEEN min AND max` instead of an IN; v1 only helps equi joins.
  Original problem statement:
  When a join spans
  two data sources it cannot be pushed to one engine, so today BOTH sides are
  fetched in full and joined locally — the probe side ships its entire table
  over the network. A real federated engine constrains the probe side with the
  build side's join keys (Trino calls this *dynamic filtering*; the literature,
  *semi-join reduction* / *sideways information passing*). This is the single
  most important optimization for cross-source usability and is currently
  absent (grep: no dynamic-filter/reducer/IN-injection infra anywhere).
  Observed:
  ```
  SELECT count(*) FROM postgres_prod.public.catalog C,
                       local_duckdb.main.catalog_access A
   WHERE A.catalog_id = C.id AND C.id = '661c…';
  -- postgres_prod: SELECT "id" FROM catalog WHERE id = '661c…'   (1 row)
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
    real keys — also the prerequisite for G9b (the reducer needs the keys).
  - **G9b.** Execute the build side first, collect its distinct join-key
    values, inject `WHERE probe_key IN (…)` into the probe-side scan's remote
    SQL at runtime (a dynamic filter). Needs executor build-before-probe
    ordering and a `PhysicalScan` that accepts a runtime-injected predicate.
    Guards: build-side cardinality threshold (fall back to full fetch when the
    key set is too large — `log` the fallback, never silently truncate), plain
    column keys only, probe side is a remote scan, NULL-key handling, and an
    empty key set ⇒ probe yields nothing.
  Sequencing decision (2026-06-11): tackle **G1 + G2 first** (same-source join
  pushdown is foundational and shares the equi-key machinery G9a needs), then
  G9.

## P. Data-path performance (measured 2026-06-13)

Found while profiling a real cross-source query
(`count(*)` over `catalog_tables ⨝ catalog_files ⨝ file_access`, ~87 ms): the
same-source `T ⨝ F` returns ~11,041 rows and we pull all of them across the
wire to find 1 match. Two concrete, measured levers (microbench on 11,041 rows,
in the bundled `perf_files` test table; Rust harness in `/workspace/fedqrs`):

- [x] **P1. Projection pruning into pushed sub-queries — DONE (2026-06-13).**
  Root cause was localized: projection pushdown *already* prunes the logical
  scans (e.g. `users → [id]`, `orders → [id, user_id]`), but the generator's
  `_expand_star_select` ignored `scan.columns` and enumerated the whole catalog
  table. Fixed `_scan_output_columns` to emit each scan's pruned column list
  (falling back to the catalog only for a true `*`). Result for the 3-table
  repro: pushed query went from 8 wide columns (incl. `name`/`amount`/text) to
  the 3 join-key columns; for the real query that's all-13 → just `F.id`. No
  semantics change, no regressions; regression test in
  `tests/e2e_decorrelation/test_duplicate_columns.py`.

- [x] **P2. Faster PostgreSQL connector (ADBC) — DONE (2026-06-13).** Opt-in via
  `driver: adbc` on a postgres datasource: `execute_query`/`get_query_schema`
  fetch through `adbc_driver_postgresql` (Arrow straight off the wire, no
  per-cell Python objects); metadata/stats stay on psycopg2. It's a true
  drop-in — `_normalize_table` aligns ADBC's Arrow types with the psycopg2
  path's: uuid (opaque binary) → canonical string (**vectorized** numpy
  hex/dash, null-safe — a per-row loop made uuid columns *slower* than
  psycopg2), numeric (opaque string) → float64, every integer width → int64,
  real → float64. `autocommit=True` so read fetches don't sit idle-in-
  transaction holding locks. Correctness test:
  `tests/e2e_decorrelation/test_adbc_connector.py` (byte-identical to psycopg2,
  incl. nulls + schema).
  MEASURED (Python, 11,041 rows): non-uuid wide **psycopg2 20.6 ms vs adbc
  4.3 ms (~4.8×)**; mixed wide 29 vs 22 ms; **uuid-only narrow 4.4 vs 9.5 ms —
  psycopg2 wins** (the uuid→string normalization the engine requires costs more
  than the tiny fetch saves). So ADBC is a big win for wide/non-uuid results and
  a loss for narrow-uuid-only fetches — it's per-datasource opt-in, not a
  universal default. The deeper fix (let uuid stay native instead of forcing
  string) is a larger engine change.
  Cross-language benchmark harness lives in `/workspace/fedqrs` (rust-postgres,
  ADBC, connectorx). connectorx was measured but **not** adopted — high fixed
  per-query cost makes it slower on small results in both Python and Rust.

- [ ] **P3. FIX COLUMN TYPES — native type fidelity end-to-end (owner-flagged,
  high priority).** Every column type must be respected, not flattened to a
  lowest-common-denominator. Today the engine coerces aggressively and
  lossily: **uuid → string** (an incidental psycopg2 wart nobody chose),
  **numeric/decimal → float64** (loses precision — unacceptable for money),
  **all integer widths → int64**, **real → double**, and the catalog
  `_map_type` does substring matching (E8). Wanted: a real type system where a
  uuid is a uuid, a decimal is a decimal, an int32 is an int32, etc., preserved
  from source metadata through the catalog, the Arrow runtime types, comparisons,
  and results. Spans: catalog `_map_type`; psycopg2 `_OID_TO_ARROW` +
  `_column_values`; the ADBC `_normalize_table` (which only exists to *match*
  the psycopg2 lossiness — it should instead preserve native types); and
  cross-source comparison/join-key compatibility (a pg uuid and a duckdb uuid
  must compare as uuids). NOTE: per the P-section profiling, this is a
  CORRECTNESS/cleanliness fix, not the cause of the slow cross-source query
  (that's P4/G9 v2.1).

- [x] **P4. Vectorize local execution (was D3) — DONE (2026-06-15) via option B.**
  Chose **DuckDB as the local execution engine** (`executor/merge_engine.py`
  `MergeEngine`, reused per `Executor`, warmed at CLI startup). Migrated operator
  `execute()`s (gated, row-loop fallback): PhysicalHashJoin ALL shapes (INNER
  keeps the G9 build-materialize+probe-stream path; LEFT/RIGHT/FULL/SEMI/ANTI
  stream the left + materialize the right; DuckDB NULL/anti semantics verified ==
  row loop), PhysicalHashAggregate (column aggregates; output cast to `schema()`),
  PhysicalSort (per-key NULLS FIRST/LAST — strictly more correct than the old
  single-placement pyarrow path), PhysicalUnion DISTINCT + PhysicalSetOperation
  INTERSECT/EXCEPT[ALL] (also fixes C6 type-coercion). NOT migrated: PhysicalGroupedLimit
  (order-dependent) and PhysicalNestedLoopJoin (null-aware condition over joined
  schema — risk > reward). Probe is STREAMED not materialized (regression-tested);
  psycopg2 reads autocommit (no teardown lock-hang); non-INNER drains right side
  first (else nested joins exhaust the pool). 743 pass / 48 fail; decorrelation
  122/122. Tests: `test_merge_engine_streaming.py`, `test_merge_set_ops.py`. The
  original profiling/analysis below is retained for context.

  ORIGINAL PROBLEM STATEMENT (kept):
  PROFILED on the 3-table cross-source `count(*)`
  (total 24.5 ms): pg fetch of 11,041 rows = 5.1 ms (~20%), **local processing
  = 19.5 ms (~80%)**. `PhysicalHashJoin` builds a Python dict keyed by per-row
  `.as_py()` tuples, probes row-by-row, and `_join_rows` allocates a fresh
  one-row `RecordBatch` per matched row — pure row-at-a-time Python over every
  intermediate row. Same shape in `PhysicalUnion`/`PhysicalSetOperation`/
  `PhysicalGroupedLimit`/aggregates. Paths to evaluate (see the discussion in
  the design notes):
  - **A. `pyarrow.Table.join` / pyarrow.compute** — replace each operator with
    Arrow's vectorized C++ kernels. Incremental, low risk; must verify SQL
    semantics (NULL-aware anti-joins, the `right_` dup-name convention, the
    dynamic-filter hook).
  - **B. DuckDB as the local execution engine** — register the fetched Arrow
    tables in a coordinator in-memory DuckDB (zero-copy) and run the post-
    pushdown plan there; vectorized joins/aggregates/sorts/window with correct
    SQL semantics for free (we already depend on DuckDB). Bigger architectural
    shift; the custom semi/anti + guards + dynamic-filter logic must integrate.
  - **C. Hand-vectorize** with pyarrow primitives — most work, least benefit.
  Note: **G9 v2.1** (push the probe-side keys into the *pushed* remote query)
  sidesteps this for the common case by making the remote return ~1 row instead
  of 11k — so v2.1 + P4 together kill this query's cost; either alone helps.

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
- [x] **H6.** `test_normalize_join_kind_full_join` — bug in the test helper
  (sqlglot represents FULL JOIN as side=FULL, kind=OUTER).
- [x] **H7.** `test_scalar_subquery_not_supported` — stale: scalar subqueries
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

1. **0.1 + 0.2** — agreed first: native EXPLAIN dialect; operator-map
   fail-fast + CONCAT/`||`/ILIKE/CAST support.
2. **A1–A7** — Phase 7 correctness (repros saved in /tmp/fedq_repro/).
3. **E2–E4** — remaining parser silent-fails (literal typing, escaping,
   OFFSET): small fixes, large blast radius.
4. **H1–H8** — test bugs; shrinks the red count to ~97 and makes CI meaningful.
5. **E7 + F3** — DuckDB typed schemas + remote-join SQL bug; unlocks outer
   joins and many pushdown tests.
6. **B1–B5, F1–F2** — robustness.
7. **G1/G2** — the real Phase 8 feature work (join + projection pushdown,
   ~51 tests).
