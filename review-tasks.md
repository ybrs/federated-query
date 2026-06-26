# Review tasks (cleanup / tidy) - 2026-06-26

Source: 5 parallel read-only review agents over the whole project (plan layer,
optimizer, parser/processor/catalog/config, executor/datasources, cross-cutting
docs/tests). HIGH-severity items were verified directly against the code; items
only reported by an agent are marked [reported] until confirmed.

We will go through these together. Nothing here is done yet. Dead-code removals
and file deletions are gated by the no-delete-without-approval rule - listed as
candidates, not actions.

Status legend: [verified] confirmed by direct check; [reported] from an agent,
needs confirming. Severity: HIGH / MED / LOW.

---

## 1. Correctness bugs

- [x] B1 (DONE 2026-06-26, fail-fast) `SELECT DISTINCT` over GROUP BY is silently dropped.
  `parser.py:1082` `_build_select_clause` returns the aggregate (or filter-over-
  aggregate) before reading `distinct`; `Aggregate` has no distinct field. So
  `SELECT DISTINCT count(*) FROM t GROUP BY a` can return duplicates with no
  error. Fix: fail-fast with `UnsupportedSQLError` when `select.args.get("distinct")`
  is set on the aggregate path (or implement it). Decide: fail-fast vs implement.

- [x] B2 (DONE 2026-06-26, fail-fast) FROM-less SELECT drops LIMIT/OFFSET/DISTINCT.
  `parser.py:310` `_build_values_select` rejects only where/group/having/order/
  joins, so `SELECT 1 LIMIT 0` returns a row, `SELECT DISTINCT 1` ignores DISTINCT.
  Fix: add limit/offset/distinct to the rejected-clause list (fail-fast) or handle.

- [ ] B3 (HIGH, verified) `OrderByPushdownRule._merge_filters` is an AttributeError.
  `rules.py:1363` calls `self._merge_filters(...)`, but `OrderByPushdownRule`
  (920-1402) has no such method (`hasattr` = False; it lives on PredicatePushdown
  @160 and AggregatePushdown @1454). Path: HAVING filter over an aggregate-bearing
  scan after order-by pushdown. Fix: merge the two predicate expressions directly
  with `BinaryOp(op=AND, ...)` (None-safe), not the Filter-typed method. Add a
  regression test (ORDER BY + HAVING over a pushed aggregate scan).

- [ ] B4 (MED, verified) Three aggregate-pushdown tests are inverted / false-green.
  `tests/test_functional_pushdown.py:365+` `test_count_aggregate`,
  `test_sum_aggregate`, `test_group_by_aggregate` are framed "EXPECTED TO FAIL /
  not implemented" and `pytest.skip(... implemented (unexpected))`. Aggregate
  pushdown IS implemented, so they skip instead of asserting it. Fix: rewrite to
  positively assert COUNT/SUM/GROUP BY appear in the pushed SQL.

- [x] B5 (DONE 2026-06-26, from the B1/B2 sweep) Set-operation builder silently
  dropped clauses. `_convert_set_operation` consumed only this/expression/distinct/
  order/limit/offset, so `UNION ALL BY NAME` (by_name) and a WITH binding a top-
  level UNION (with_, drops the CTE) were ignored. Fix: allowlist guard
  (SUPPORTED_SET_OP_ARGS) fails fast; same guard added to per-JOIN args
  (SUPPORTED_JOIN_ARGS) and the FROM-less path. Shared helper
  `_reject_unsupported_args`; `UnsupportedSQLError` now subclasses `ValueError`.

- [x] B6 (DONE 2026-06-26, expression-converter sweep) Expression builders dropped
  semantically-meaningful modifiers. Verified silent-wrongs now fail fast via
  per-node allowlists: simple CASE (operand under `this` was ignored -> wrong
  branch), TRY_CAST / safe cast (`safe`), BETWEEN SYMMETRIC (`symmetric`), plus
  precautionary guards on IN (`field`/UNNEST) and window args. STRING_AGG
  separator was silently dropped (false-green: the rendering test only passed
  because ',' equals DuckDB's default) - now IMPLEMENTED as a real second arg.
  Already-loud (not silent) and left as-is: array_agg(ORDER BY) and LIKE ESCAPE.
  Implement-candidates now loud: simple CASE, TRY_CAST, BETWEEN SYMMETRIC.

---

## 1b. Deep silent-fail sweep (2026-06-26) - whole engine

Goal: no lie-shipping silent fails (wrong/incomplete result with no error). Found
via 5 parallel subsystem audits + 1 completeness-critic pass, each finding
adversarially verified before fixing. 916 tests pass (was 898). One audit finding
(LIMIT-below-DISTINCT for single-source) was a FALSE POSITIVE caught by an e2e
test; the real bug was the cross-source variant, fixed narrowly.

Fixed (all verified to ship a wrong/incomplete result before the fix):
- [x] Binder: compound predicates (IN/BETWEEN/CASE/...) in a join's ON/WHERE were
  returned UNBOUND, so an invalid column inside them was never validated. Now
  routed through the full multi-table binder; dispatchers raise on unknown type.
- [x] Binder: a column with an unknown table qualifier (`x.col`) was silently
  rebound to another table by name-scan. Now alias-aware keys + raise (closes M10).
- [x] Optimizer: LIMIT pushed below DISTINCT over a non-Scan (cross-source) child
  capped rows before dedup. Now kept above; single-source push preserved.
- [x] Optimizer: correlation analyzer + rewrite did not descend into CAST/EXTRACT/
  window, so a correlated subquery with an outer ref inside a CAST was treated as
  UNCORRELATED (executed once). Now descends in both detection and substitution.
- [x] Optimizer: column-pruning (`_extract_columns`, `_predicate_children`) omitted
  columns inside CASE/CAST/EXTRACT/TUPLE, pruning still-needed columns. Now covered.
- [x] Executor/datasources: unknown PostgreSQL type OID / ClickHouse type / ADBC
  opaque type silently became string/VARCHAR (mistyped values). Now raise (uuid
  OID mapped explicitly to match the ADBC path). Unary evaluator raises on unknown.
- [x] Physical: local GROUP BY emitted NULL for a non-column key / a non-aggregate-
  non-group SELECT expr (e.g. `SUM(x)*2`). Now raises (Literal returns its value).
- [x] Physical: `_scan_with_columns` / `_plan_remote_join_aggregate` rebuilt nodes by
  re-listing fields, dropping sample/grouping_sets/distinct/order_by. Now model_copy.
- [x] Physical: select-list and join-key zips length-guarded (no silent truncation).
- [x] Processor: star-expansion column rename silently shipped internal names on a
  count mismatch. Now best-effort rename by internal name; internal names never leak.
- [x] Config: an unknown/typo'd top-level section was silently ignored. Now raises.

Enforcement (lint): `tests/test_expression_walker_exhaustiveness.py` plants sentinel
columns in every child slot of every compound Expression type and asserts each
recursive collector (decorrelation `_expression_column_refs`, ProjectionPushdown
`_extract_columns`, PredicatePushdown `_extract_column_refs`) surfaces them all. A
new Expression subclass fails the test until classified leaf/subquery/compound. This
lint caught a further gap the manual audits missed: TupleExpression/WindowExpr were
dropped by PredicatePushdown column extraction (`(a,b) IN (...)` -> vacuous pushdown).

Left as NOT lie-shipping (validated late but caught LOUDLY downstream, documented):
- Binder HAVING compound predicate + column-ref-with-None-table: an invalid column
  is rejected by the source / expression evaluator at execution, not at bind.
- Parser HAVING aggregate-rewrite around CAST/CASE; window-operand alias resolution
  pass-through: render-correct or fail loud; narrow shapes.

---

## 2. Rule violations (ASCII / lint / stale docs)

- [ ] R1 (HIGH, verified) Non-ASCII glyph in EXPLAIN runtime output.
  `physical.py:4200` `_remote_join_detail` returns `f"...sources={left}<bowtie>{right}"`
  using U+2A1D. This prints a non-ASCII byte in EXPLAIN. Fix: use `JOIN`.

- [ ] R2 (MED, verified) ~300 non-ASCII chars in `.py` docstrings/comments and
  `.md` docs: em-dash (~323), arrow U+2192 (~211), box-drawing (~115), ellipsis
  (~40), en-dash (~26), down-arrow (~21), multiply sign (~11), curly quotes (~12),
  <= />= (~10), section sign (~7), plus a few subscripts. Worst: `physical.py`
  (22 lines), `single_source_pushdown.py`/`postgresql.py` (6 each),
  `decorrelation.py` (4); README/plan.md diagrams use box-drawing + arrows.
  Fix: sweep replace em-dash -> ` - `, arrows -> `->`, ellipsis -> `...`,
  multiply -> `x`, `<=`/`>=` spelled out, curly -> straight, drop/redraw box-art.
  Keep genuine non-ASCII test FIXTURE content (e.g.
  `tests/e2e_pushdown/test_string_edge_cases.py:40`) - that is the allowed exception.

- [ ] R3 (HIGH, verified) `no_dataclasses` lint does not cover `tests/`, and a
  live `@dataclass` slipped through. `tests/test_no_dataclasses.py` globs only the
  package root; `tests/e2e_pushdown/conftest.py:5,34` has `from dataclasses import
  dataclass` + `@dataclass class QueryEnvironment`. Fix: convert `QueryEnvironment`
  to `StateModel`; extend the lint to also walk `tests/`.

- [ ] R4 (MED, verified) `PhysicalRemoteJoin` docstring lies about reachability.
  `physical.py:1785-1800` claims "the planner no longer constructs this node
  (0 hits)" / "do not delete without sign-off". It IS constructed at
  `physical_planner.py:426` and `:757`. Fix: rewrite the docstring to describe
  when the planner builds it; drop the historical/0-hits cruft.

- [ ] R5 (MED, verified) README/plan.md advertise "Immutable Plans".
  `README.md:319` and `plan.md:354-357` state plans are immutable for thread
  safety - contradicts mutable-by-default and the mutable `StateModel`. Fix:
  rewrite to "mutable Pydantic StateModel nodes; transforms use model_copy".

- [ ] R6 (MED, verified) `tasks.md` Phase 9 status is self-contradictory.
  Roadmap table row says "Not Started"; Phase 9 section says "COMPLETE"; the top
  heading still reads "In progress: migrate all state types off @dataclass" while
  its body is all checked/COMPLETE; "Phases 0-8 ... 809 passing (2026-06-25)" line
  is stale. Fix: set table row to Complete, retitle the migration heading to
  Completed, refresh the status line.

- [ ] R7 (MED, reported) `doc/limit-pushdown.md:409,421,527` show
  `@dataclass(frozen=True)` / `@dataclass` code samples - contradict the
  no-dataclass + mutable rules. Fix: rewrite samples as `StateModel` or mark
  clearly as superseded.

---

## 3. Mechanical tidy (safe)

- [ ] M1 (HIGH, verified) Duplicate method silently shadows the first.
  `physical.py` `PhysicalNestedLoopJoin._left_row_batch` defined twice, identical
  (2284 and 2332). Delete the duplicate (2332).

- [ ] M2 (MED, verified) `while index < len(...)` loops violate the for-loop rule.
  `physical.py:729` (`_format_columns`) and `physical.py:1919`
  (`_build_aggregate_select_clause`). Convert to `for index, ... in enumerate(...)`.

- [ ] M3 (MED, verified) Unreachable code after `return`.
  `physical_planner.py:617-622` - orphaned `PhysicalNestedLoopJoin(...)` after the
  return at 615 (the live one is in `_plan_nested_loop`). Delete 617-622.

- [ ] M4 (MED, reported) Dead and broken `_propagate_sort_metadata`.
  `rules.py:986-993` - only self-recursive caller; also discards the copied scan
  it builds (silent no-op). Delete.

- [ ] M5 (LOW, reported) Duplicated conjunct-splitter.
  `physical_planner.py:75 _split_and` duplicates `decorrelation.py:190
  _split_conjuncts` (already imported by single_source_pushdown). Reuse the latter,
  drop `_split_and`.

- [ ] M6 (LOW, reported) Stray f-string with no placeholder.
  `postgresql.py:227` `f"SELECT reltuples..."` (the `%s` is a driver param). Drop `f`.

- [ ] M7 (LOW, reported) Mutable `{}` field default vs the codebase pattern.
  `physical.py:1816` `_column_alias_map: Dict[...] = {}`; two classes down
  `PhysicalRemoteQuery` uses `Field(default_factory=dict)`. Make it consistent.

- [ ] M8 (LOW, reported) Defensive `getattr` cruft on always-present private attrs.
  `physical.py:99` `getattr(self, "_merge_engine", None)` and `:252`
  `getattr(self, "_cached", None)` - both declared with `= None`, so the fallback
  never fires. Use plain `self._merge_engine` / `self._cached`.

- [ ] M9 (LOW, reported) Trim leftover "as a dataclass field" comments.
  `expressions.py:597,618,643,697` - the four `subquery: Any` comments say "...never
  validated as a dataclass field either"; drop the dataclass clause.

- [ ] M10 (LOW, reported) `_extract_tables` keys a Scan by table name, ignoring
  alias. `binder.py:1017-1022` vs the alias-aware `_add_scan_to_scope` (binder.py
  :890). Reconcile to the alias-aware key.

- [ ] M11 (LOW, reported) `datetime.utcnow()` deprecation. `utils/logging.py:23`
  (only relevant if the structured formatter survives D5). Use
  `datetime.now(timezone.utc)`.

- [ ] M12 (LOW, reported) Stale `catalog.py:52` `# TODO: Proper type mapping` over
  a fairly complete mapping. Remove or make specific.

---

## 4. Dead-code removal candidates (NEED APPROVAL per no-delete rule)

Evidence noted; action is "delete pending your OK". Decide each.

- [ ] D1 (MED, reported) `executor/operators.py` - whole file dead. `OperatorExecutor`
  + 4 methods all `raise NotImplementedError`; referenced only by its own re-export
  in `executor/__init__.py`. Execution goes through MergeEngine + ExpressionEvaluator.
  Remove file + the `__init__.py` export. (Includes 4 stale TODOs and M7-style unused
  imports.)

- [ ] D2 (MED, reported) Visitor layer - `LogicalPlanVisitor` (`logical.py:614`),
  `ExpressionVisitor` (`expressions.py:516`), and every `accept()` on logical nodes
  and expressions. Zero subclasses, `.accept(` never called. Also internally
  inconsistent (two visit methods non-abstract -> silent no-op hazard). Big surface.
  Decide: delete, or keep/wire-up as the traversal API.

- [ ] D3 (LOW, reported) `Gather` node - `physical.py:4081`. Exported in `__all__`;
  never constructed; execute/schema/estimated_cost all `NotImplementedError`. Remove
  or keep as planned operator.

- [ ] D4 (MED, reported) `DataSource._mark_natural_joins` - `base.py:180`. Sets a
  `natural` arg on the sqlglot AST that nothing reads (sqlglot renders NATURAL from
  its own method). Remove and simplify `parse_query` to a plain parse.

- [ ] D5 (MED, reported) Structured-logging machinery in `utils/logging.py`:
  `StructuredFormatter`, `LoggerAdapter`, `get_contextual_logger` (and the JSON
  branch) - zero external references. `StandardFormatter`/`setup_logging`/`get_logger`
  stay. Remove the unused trio.

- [ ] D6 (MED, reported) `PostgreSQLDataSource._extract_column_names` - `postgresql.py
  :503`. Never called (live path uses `_build_arrow_fields`/`_build_column_arrays`).
  Remove.

- [ ] D7 (MED, verified) `parser._filter_columns_for_table` - `parser.py:593`.
  Orphaned; superseded by `_columns_for_join_table`. Remove.

- [ ] D8 (MED, reported) `Parser.parse_ast` - `parser.py:100`. No caller in package
  or tests. Remove or confirm intended public API.

- [ ] D9 (MED, reported) `QueryContext.metadata` + `set_metadata`/`get_metadata` -
  `query_context.py:23,30-36`. Zero callers; speculative extension point. Remove.

- [ ] D10 (MED, reported) `parse_to_logical_plan(catalog, query_executor)` -
  `parser.py:1899`. Both params accepted and ignored (body is `return self.parse(sql)`);
  callers pass them. Drop the params (update caller) or document why.

- [ ] D11 (LOW, reported) `query_executor` param threaded but unused -
  `RuleBasedOptimizer.optimize` (`rules.py:1536`) and `PhysicalPlanner.plan`
  (`physical_planner.py:110`). Caller passes `self`; nothing reads it. Wire to a
  real use or drop.

- [ ] D12 (LOW, reported) `cost.py` internal issues (Phase 11 scaffold, fix when
  wired, NOT delete): unused `Union` import (line 13); `_estimate_limit_cost`
  (333-338) returns only `cpu_cost`, dropping `input_cost` that every sibling adds
  - likely an estimation bug.

---

## 5. Repo hygiene (deletes NEED APPROVAL)

- [ ] H1 (MED, reported) Tracked clutter: 100MB `postgres-server.log`, `analytics.db`,
  `.coverage`, `federated_query.egg-info/`, root `__pycache__/`. Untrack +
  `.gitignore`. The 100MB log especially must not be tracked.

- [ ] H2 (LOW, reported) Orphan root script `test_cli_interactive.py` - print-based
  subprocess demo, not pytest; collection hazard under a bare `pytest`. Move under
  tests/ as a real test or delete.

- [ ] H3 (LOW, reported) `example/test_federated_join.py`, `example/test_join_fixes.py`
  - `__main__` demos misnamed `test_*`; same collection hazard. Rename (drop `test_`)
  or move out of collection.

- [ ] H4 (LOW, reported) Duplicate `build-test-scripts/download_postgresql.sh` (older
  wget copy) vs `scripts/download_postgresql.sh` (curl, idempotent). Drift risk.
  Keep one.

- [ ] H5 (LOW, reported) Superseded planning docs at root (redirect stubs:
  `TODO-next.md`, `pushdown-status.md`, `decorrelation-gaps.md`; historical:
  `TODO-phase7-review.md`, `decorrelation-plan.md`, `decorrelation-tasks.md`,
  `PHASE_3_COMPLETION.md`, `plan-physical-merge-engine.md`, `todo-current-state.md`).
  Archive into a doc/history folder. Candidates only.

- [ ] H6 (LOW, reported) No pytest config (`testpaths = tests`). Adding it fixes the
  H2/H3 collection hazard and future-proofs the lint coverage gap.

---

## Non-findings (verified clean - do not "fix")

- Decorrelation fail-fast (`DecorrelationError` on unsupported shapes) is by design,
  not a silent skip.
- `JoinReorderingRule` raising `NotImplementedError` and being unregistered is the
  documented Phase 11 placeholder (tasks.md).
- `cost.py` / `statistics.py` unwired from the pipeline = intentional Phase 11
  scaffolding with its own 615-line test; not cruft to delete (but see D12).
- catalog back-ref private attrs + read-only properties + `@model_validator` are
  correct; the model_copy caveat is honestly documented.
- No `@dataclass` / `dataclasses` / `replace` bridge / `frozen` / positional-construct
  artifacts remain in `federated_query/` (migration is clean).
- Connection/cursor lifecycle in executor/datasources closes on error paths; no
  silent swallows (the two broad excepts - CLI top-level, ADBC best-effort option
  set - are both legitimate).
