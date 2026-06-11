# Phase 7 Review — TODO

Findings from an adversarial review of the Phase 7 decorrelation work plus an
audit of the pre-existing engine. Every item marked VERIFIED was reproduced
against the live engine and cross-checked with PostgreSQL 17; repro scripts
for section A live under `/tmp/fedq_repro/`. Items marked READ are from code
inspection.

---

## STATUS (updated 2026-06-11, branch `parsing_explain`)

689 passed / 85 failed (from 645/125 baseline); decorrelation suite green;
**G4 set operations done** (10 e2e_pushdown tests green, no regressions);
remaining failures are dominated by **G1–G3, G5–G8** feature work. Verified
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

- [ ] **G1. Join pushdown too narrow (~37 tests)** — `_is_remote_join_candidate`
  only pushes Scan⨝Scan INNER/LEFT/RIGHT equi-joins. Needed: multi-table
  same-source joins, SEMI/ANTI (decorrelated subqueries → single remote
  query), non-equi conditions, FULL OUTER, computed keys. Phase 8 work;
  depends on F3.
- [ ] **G2. Computed projection pushdown (~14 tests)** — `SELECT UPPER(x) AS y`
  fetches base columns and computes locally; tests assert the alias appears
  in remote SQL. Results are correct; needs a projection-expression pushdown
  rule + capability checks.
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
- [ ] **G6. Date/time gaps (~6 tests)** — EXTRACT (exp.Var), INTERVAL, AGE,
  CURRENT_DATE pushdown.
- [ ] **G7. Aggregate FILTER clause (2 tests)**.
- [ ] **G8. IN-subquery bodies ending in Filter/Sort (3 tests)** — overlaps B4.

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
