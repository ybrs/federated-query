# Silent-drop audit (full pipeline) - 2026-06-26

Question asked: with the Pydantic/no-dataclass migration done, how do silent drops
STILL happen? Answer: the migration killed exactly ONE silent-drop mechanism
(reconstructing a node by re-listing fields, where the rebuild now goes through
`model_copy`). It did not touch three other mechanisms that were always present.
This audit (5 parallel agents over parser, preprocessor, binder, optimizer,
physical rendering) found ~26 distinct silent drops across all of them.

A "silent drop" here = input SQL semantics the engine recognizes or partially
reads, then discards WITHOUT either honoring it or failing loud. Symptom: the
query RUNS and returns a WRONG answer with NO error.

VERIFIED: I personally reproduced the items tagged [verified] below (ran the
engine, showed the dropped semantics). The rest are agent-found with a stated
repro and not yet independently re-run.

---

## How the migration MISSED Class A, and how we make it impossible to recur

Honest account of the gap (no excuses):

- The migration changed two things: the TYPE definitions (`@dataclass` ->
  `StateModel`) and the copy-with-change IDIOM (the `replace`/`dataclasses.replace`
  /`with_children` call sites -> `model_copy`). I found those by grepping for the
  idiom.
- A raw rebuild like `Join(left=a, right=b, join_type=t, condition=c)` is NOT that
  idiom. It is a plain constructor call. Under dataclasses it dropped fields the
  exact same way, but it never matched the `replace`/`dataclasses.replace` grep, so
  it was never on the migration's radar. It is indistinguishable from legitimate
  fresh-node construction unless you READ each call and ask: "is the first arg an
  existing node of this same type, and are any of that node's fields omitted?"
- I leaned on "suite green" as the field-completeness safety net. That was the
  shortcut. The suite is green because it does not exercise the dropped fields
  (NATURAL/USING-with-pushdown, grouping-sets-in-correlated-subquery, TABLESAMPLE-
  folded-into-a-join, DISTINCT-over-ORDER-BY-subquery). Green meant "no COVERED
  drop," not "no drop." A test suite can never prove a negative like this.

The structural stop (this is "how we stop that"): vigilance does not scale, so we
remove the ability to make the mistake quietly.

1. LINT (mechanical, blocking): an AST test that scans the transform modules
   (`parser/binder.py`, `optimizer/*.py`, anything that rewrites a plan) and FAILS
   on any direct constructor call of a logical/expression `StateModel` node class,
   UNLESS the call carries an explicit `# new-node: <reason>` justification comment.
   - Rationale: in transform code, "make a changed copy" must go through
     `model_copy(update=...)` (copies every field, drop-proof). The only valid raw
     constructor in a transform is a genuinely NEW node (e.g. decorrelation
     synthesizing a SEMI join), and those must be justified out loud.
   - Effect: the lint ENUMERATES every Class A site for us. We fix each (model_copy,
     or add a justified `# new-node:`), lint goes green, and the class is closed for
     all CURRENT and FUTURE fields - a new field added to `Join` can never again be
     silently dropped by a rebuild, because there are no unjustified rebuilds left.
   - This is the true completion of the dataclass migration. We build the lint
     FIRST; its failure list becomes the fix worklist (no judgement call about
     "did I find them all" - the lint did).

2. COVERAGE for the dropped fields: a semantics-preservation test matrix (one case
   per field/feature dropped below) so a regression fails a test, not a user query.

Sequence: build the lint -> fix every site it flags (Class A) -> then Class B/C/D.

---

## Why `extra="forbid"` and `model_copy` did NOT prevent these

- `extra="forbid"` rejects an UNKNOWN kwarg at construction. These bugs OMIT a
  KNOWN field (so it takes its default) or never read an input arg at all - both
  are perfectly "valid" to Pydantic. extra-forbid cannot see an omission.
- `model_copy(update=...)` copies every field, but only protects the sites that
  actually call it. A hand-written `Join(left=..., right=..., join_type=...,
  condition=...)` rebuild bypasses it entirely.
- The migration's guarantee was narrow: copy-with-change via `model_copy`/
  `with_children` never drops a field. It never converted the binder's and
  optimizer's hand-rolled raw-constructor rebuilds, nor the parser's partial-read
  converters, nor the logical->physical->SQL lowering. Those are the surviving
  mechanisms.

---

## The four root-cause classes

- CLASS A - raw-constructor rebuild re-lists a SUBSET of fields. The omitted
  field silently takes its default. This is the EXACT dataclass-era failure mode;
  it survives wherever transform code calls `Node(...)` instead of
  `node.model_copy(update=...)`. Fix: use `model_copy`.
- CLASS B - partial-read converter with no consume-or-reject guard. A converter
  reads a hand-picked subset of a sqlglot node's args and ignores the rest. Fix:
  after consuming, assert nothing semantically significant remains, else raise
  (the "consume-or-reject" discipline).
- CLASS C - cross-representation lowering (logical -> physical -> SQL) that
  hand-threads fields and misses some. Fix: thread the field; add per-field
  rendering tests.
- CLASS D - incomplete binder dispatch: a recursion handles some expression node
  types and `return`s the rest unbound, skipping existence/type validation. Fix:
  delegate leaves to the full `_bind_expression` dispatch; never return unbound.

A recurring THEME cutting across A and C: the `Join.natural` / `Join.using`
fields are dropped in THREE independent places (A1, A2, C1). NATURAL/USING is
systematically under-propagated and is the single most dangerous column here
(it turns a join into a Cartesian product, silently).

---

## CLASS A - raw-constructor rebuild drops a field (fix: model_copy)

- [ ] A1 [verified] HIGH/CRITICAL - `PredicatePushdownRule._push_filter_below_join`
  rebuilds `Join` at `rules.py:307,319,330,344,351` listing only
  `left,right,join_type,condition` - drops `natural` and `using`. A NATURAL/USING
  join with a WHERE above it becomes `natural=False, condition=None` = a Cartesian
  product (verified: `natural=True` in, `natural=False, condition=None` out).
  Fix: `join.model_copy(update={"left": ..., "right": ...})` in all five sites.

- [ ] A2 [verified] HIGH - `SubqueryPlanBinder._bind_join` at `binder.py:1574`
  rebuilds `Join` listing `left,right,join_type,condition` - drops `natural`,
  `using`. Any NATURAL/USING join inside a correlated EXISTS/IN/scalar/LATERAL
  subquery loses its equi-match; downstream `_join_is_pushable` then sees no
  predicate and the join degenerates to a cross product. Fix: `model_copy`.

- [ ] A3 HIGH - `decorrelation.py:1603` `_widen_projection_for_keys` rebuilds the
  user's `Projection` as `Projection(input, expressions, aliases)` - drops
  `distinct` and `distinct_on`. `SELECT DISTINCT ... ORDER BY (<correlated
  subquery>)` loses DISTINCT (duplicate rows). Fix: `projection.model_copy(...)`.

- [ ] A4 MED - `decorrelation.py:613,808,887` rebuild `Aggregate` listing
  `input,group_by,aggregates,output_names` - drop `grouping_sets`. A correlated
  subquery with GROUP BY ROLLUP/CUBE/GROUPING SETS that is HAVING-hoisted or
  key-widened collapses to a single-level GROUP BY. Fix: `model_copy`.

- [ ] A5 MED - `physical_planner.py:911` `_scan_with_columns` rebuilds
  `PhysicalScan` to fold a bare projection into the scan, omitting `sample`
  (verified by the agent: `TABLESAMPLE` becomes None). A sampled scan whose
  projection collapses scans the FULL table. Fix: `scan.model_copy(update=
  {"columns": columns})`.

---

## CLASS B - partial-read converter, no consume-or-reject

### B.statement / clause (parser.py)

- [ ] B1 [verified] HIGH - DISTINCT / DISTINCT ON over an aggregate dropped.
  `_build_select_clause` (`parser.py:1082-1086`) returns the Aggregate (or
  Filter-over-Aggregate) BEFORE the `distinct`/`distinct_on` read at 1098-1106.
  `SELECT DISTINCT count(*) FROM t GROUP BY a` loses DISTINCT.
  Fix: honor distinct over the aggregate result, or fail-fast.

- [ ] B2 [verified] HIGH - FROM-less SELECT drops LIMIT / OFFSET / DISTINCT.
  `_build_values_select` (`parser.py:310-327`) rejects only
  where/group/having/order/joins, never reads limit/offset/distinct.
  `SELECT 1 LIMIT 0` returns 1 row; `SELECT 1 OFFSET 5` returns 1 row. Fix:
  consume limit/offset (and reject distinct on) or fail-fast.

- [ ] B3 [verified] HIGH - WITH dropped on a top-level set operation.
  `_convert_set_operation` (`parser.py:144-160`) never reads the `with_` arg
  sqlglot attaches to a leading-WITH union. `WITH c AS (...) SELECT ... FROM c
  UNION SELECT ... FROM c` silently treats `c` as a catalog table (verified:
  produces `Scan table=c`, not `CTERef`); masked crash if no such table, wrong
  table if one exists. Fix: establish CTE scope on the set-op path, or fail-fast.

- [ ] B4 HIGH - `UNION [ALL] BY NAME` degraded to positional UNION.
  `_convert_set_operation` never reads `by_name`. Branches with different column
  orders get unioned by position. Fix: fail-fast on `by_name` (or implement
  name alignment).

- [ ] B-systemic - the set-op path has NO consume-or-reject guard at all (unlike
  the plain-Select path's `_reject_unknown_select_args`). B3/B4 are instances.

### B.expression (parser.py)

- [ ] B5 [verified] CRITICAL - multi-argument aggregates drop every arg after the
  first. `_extract_function_args` (`parser.py:1704-1729`) reads only `func.this`,
  never iterates `arg_types`. `covar_pop(a,b)` becomes `COVAR_POP(a)`;
  `regr_slope(y,x)` becomes `REGR_SLOPE(y)`; `approx_quantile(x,0.5)` becomes
  `APPROX_QUANTILE(x)` (all verified). A different/invalid function is pushed.
  Fix: gather args by iterating `arg_types` like the scalar `_collect_function_args`
  already does, or fail-fast on extra populated slots.

- [ ] B6 [verified] CRITICAL - `string_agg`/`group_concat` separator dropped
  (special case of B5). `string_agg(x, ';')` becomes `STRING_AGG(x)` (verified) -
  invalid in Postgres (mandatory delimiter). Fix: same as B5.

- [ ] B7 [verified] HIGH - `TRIM(LEADING/TRAILING/BOTH ... FROM ...)` position
  keyword dropped. `_append_function_arg` (`parser.py:1804-1812`) silently skips
  the `position` slot (a bare string, not an Expression). `trim(LEADING 'x' FROM
  col)` becomes `TRIM(col, 'x')` = both-sides trim (verified). COLLATE is dropped
  the same way. Fix: handle `exp.Trim` explicitly; fail-fast on a non-Expression,
  non-list arg slot rather than skipping.

- [ ] B8 [verified] HIGH - `TRY_CAST`/`SAFE_CAST` silently becomes strict `CAST`.
  `_convert_cast` (`parser.py:1484-1488`) reads `this`+`to`, never the `safe` flag.
  `TRY_CAST(x AS INT)` becomes `CAST(x AS INT)` (verified) - NULL-on-failure turns
  into error-on-failure. Fix: read `safe`, render TRY_CAST per dialect, or fail-fast.

- [ ] B9 [verified] HIGH - `x IN UNNEST(array)` becomes empty `IN ()`.
  `_convert_in_expression` (`parser.py:1292-1307`) reads only this/query/expressions;
  with `unnest` populated and `expressions` empty it builds `InList(options=[])`.
  `x IN UNNEST(arr)` becomes `(x IN ())` (verified) = always false/NULL. The
  `field` form drops the same way. Fix: handle unnest/field, or fail-fast.

- [ ] B10 [verified] MED - `BETWEEN SYMMETRIC` drops the SYMMETRIC flag.
  `_convert_between_expression` (`parser.py:1309-1314`) reads this/low/high only.
  `col BETWEEN SYMMETRIC 5 AND 1` becomes `(col BETWEEN 5 AND 1)` (verified) =
  always false, when SYMMETRIC means swap-the-bounds. Fix: honor or fail-fast.

(Checked and SAFE - already loud, not findings: aggregate `ORDER BY` inside agg,
`IGNORE/RESPECT NULLS`, `LIKE ... ESCAPE`, window `EXCLUDE`, byte/hex string
literals - all raise. `count(DISTINCT a,b)` honored.)

### B.preprocessor (query_preprocessor.py)

- [ ] B11 HIGH - chained named windows drop the inherited PARTITION BY.
  `_inline_named_windows` (`query_preprocessor.py:290-302`) resolves each window
  reference independently in document order and merges only one level, so
  `WINDOW w1 AS (PARTITION BY active), w2 AS (w1 ORDER BY id)` referenced via
  `OVER w2` loses `PARTITION BY active` (wrong row numbers). Fix: resolve the
  base-window chain to a fixed point, or fail-fast when a window's base ref is set.

- [ ] B12 HIGH - `SELECT * EXCLUDE/REPLACE` ignored on the PIVOT path.
  `_is_star_select` (`query_preprocessor.py:224-227`) treats any Star as bare and
  does not inspect `except_`/`replace`; `_apply_pivot_rewrite` rebuilds the
  projection from catalog columns. `SELECT * EXCLUDE (amount) ... PIVOT(...)`
  keeps `amount`. Fix: reject a star carrying except_/replace on the pivot path,
  or thread them through.

- [ ] B13 MED - user GROUP BY overwritten by the PIVOT rewrite.
  `_apply_pivot_rewrite` (`query_preprocessor.py:242-246`) unconditionally
  `select.set("group", ...)`, clobbering an existing GROUP BY. Fix: fail-fast when
  a GROUP BY already exists alongside PIVOT.

- [ ] B14 MED (masked error) - `EXCLUDE (nonexistent)` silently no-ops.
  `_excluded_column_names`/`_expand_wildcard` (`query_preprocessor.py:389-394,
  366-369`) only `continue` on a match; a name matching nothing is never
  validated. A typo silently keeps the column. Fix: assert every excluded name
  exists; raise `StarExpansionError`.

- [ ] B15 MED (masked error) - `REPLACE (expr AS nonexistent)` silently discarded.
  `_replacement_expressions`/`_append_wildcard_column` key by column name; a
  REPLACE aliased to a non-existent column is never looked up or emitted. Fix:
  validate REPLACE keys against target columns; raise on a miss.

(Verified by the agent as NOW CORRECT/loud, not findings: QUALIFY rewrite,
EXCLUDE/REPLACE on the normal star path, TABLESAMPLE on bad relations, PIVOT on a
joined table - all honored or rejected.)

---

## CLASS C - cross-representation lowering misses fields

- [ ] C1 HIGH - physical planner never honors NATURAL/USING for cross-source
  joins. `physical_planner.py:512-555,771-791` lowers a join purely from
  `condition`; with `condition is None` it builds `PhysicalNestedLoopJoin(
  condition=None)` = an unconditional Cartesian product. The name-match semantics
  is lost. Fix: expand NATURAL/USING into an explicit ON equality before physical
  planning (and ideally before the optimizer, which also ignores them - see A1).

- [ ] C2 HIGH - single-source pushdown drops a folded scan's `sample`, `offset`,
  `distinct`. `_scan_ref` (`single_source_pushdown.py:870-882`) never renders
  `sample`; `_absorb_scan_modifiers` (`584-602`) adopts limit but not offset, and
  distinct is never read off the scan. A sampled join side loses TABLESAMPLE; a
  paginated scan loses OFFSET. Fix: render sample in `_scan_ref`; adopt offset
  (and handle distinct) alongside limit.

- [ ] C3 MED - the PhysicalWindow path drops projection `distinct`/`distinct_on`.
  `_plan_projection` (`physical_planner.py:331-348`) returns `PhysicalWindow`
  early, before applying distinct. `SELECT DISTINCT rank() OVER (...) ...` over a
  cross-source input loses DISTINCT. Fix: carry distinct onto PhysicalWindow and
  render `SELECT DISTINCT`, or wrap in a distinct operator.

---

## CLASS D - incomplete binder dispatch (returns unbound, skips validation)

- [ ] D1 HIGH - HAVING does not bind/validate aggregate (function) arguments.
  `_bind_having_expression` (`binder.py:816-844`) recurses only into
  ColumnRef/BinaryOp/UnaryOp/subquery and `return expr` for everything else, so a
  `FunctionCall` in HAVING keeps `data_type=None` and its column refs are never
  existence-checked. `HAVING SUM(nonexistent_col) > 10` is accepted by the binder
  (only the source might later reject it; a pushed subtree can run). Fix: delegate
  non-ColumnRef leaves to the full `_bind_expression` dispatch.

- [ ] D2 HIGH - JOIN ON does not recurse into Cast/FunctionCall/CaseExpr/Extract/
  Between/InList/Window. `_bind_join_condition` (`binder.py:1045-1066`) handles
  ColumnRef/Literal/BinaryOp/UnaryOp/subquery and `return condition` for the rest,
  leaving nested columns unbound and unchecked. `ON a.x = LENGTH(b.does_not_exist)`
  passes silently. Fix: route non-trivial operands through
  `_bind_expression_multi_table`.

- [ ] D3 HIGH - alias-qualified column binds to the WRONG relation in a multi-table
  join. `_extract_tables` (`binder.py:1013-1022`) keys a scan by `table_name`
  while `_add_scan_to_scope` (`881`) keys by `alias or table_name`. In
  `_bind_column_ref_multi_table` (`1068`) an alias-qualified lookup misses and the
  fallback returns the FIRST table that merely contains the column name,
  re-tagged with the requested alias. With `a(val INT)` aliased `x` and `b(val
  VARCHAR)` aliased `y`, `y.val` resolves to a's INTEGER `val`; `y.only_a` (a
  column only in a) resolves silently instead of failing. Fix: key
  `_extract_tables` by `alias or table_name` (consistent with the rest of the
  binder), so the per-table existence/type check runs.

---

## Systemic remediation (the real fixes, in priority order)

1. CLASS A is the highest-value, lowest-risk: replace every raw same-type node
   rebuild in the binder/optimizer/decorrelation with `model_copy(update=...)`.
   Add a guard test (AST lint) that FAILS on a raw constructor call for a
   StateModel subclass inside transform modules (binder/optimizer/decorrelation),
   forcing `model_copy`. That structurally closes the entire class, including
   future fields. This is the true completion of the migration.

2. CLASS B needs a CONSUME-OR-REJECT discipline. Replace the global
   `SUPPORTED_SELECT_ARGS` allow-list (which only proves a name is handled
   SOMEWHERE) with per-converter assertions: after each converter (plain select,
   FROM-less, aggregate select-clause, set operation, and EACH `_convert_*`
   expression helper) consumes the args it handles, assert no other populated,
   semantically-significant arg remains on that node - else raise
   `UnsupportedSQLError`. This converts every "supported but unconsumed on this
   shape/node" from a wrong answer into a loud error. The aggregate-arg extractor
   (B5/B6) should iterate `arg_types` like the scalar path already does.

3. CLASS D: make `_bind_join_condition` and `_bind_having_expression` delegate
   their leaf/operand cases to the full `_bind_expression` / multi-table dispatch
   instead of `return expr`. A binder must bind every operand and FAIL on an
   unresolved reference. Also fix the `_extract_tables` keying (D3).

4. CLASS C: thread `natural`/`using` into physical planning (or pre-expand them to
   ON equalities), render `sample`/`offset`/`distinct` in the pushdown builders,
   carry distinct onto PhysicalWindow. Back these with per-field rendering tests
   (build node -> assert the pushed SQL contains the clause).

5. A cross-cutting regression net: a semantics-preservation test that, for a
   matrix of SQL features (each modifier above), runs the engine and asserts the
   pushed/executed SQL preserves the feature - so any future silent drop fails a
   test instead of returning wrong rows.

---

## Verification status

Personally reproduced (ran the engine, observed the drop): A1, A2(structural),
B1, B2, B3, B5, B6, B7, B8, B9, B10. The remaining items (A3, A4, A5, B4,
B11-B15, C1-C3, D1-D3) are agent-found with stated repros and should be confirmed
when each is fixed (the fix's regression test is the confirmation).
