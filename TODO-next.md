# TODO-next — handoff / restart notes

State of the subquery-pushdown work on branch `phase8`. The "No Subqueries in the
Physical Plan" rule is now enforced: decorrelation produces a flat join plan and
single-source pushdown renders it as **SQL joins**, never re-correlated
`EXISTS`/`IN`/scalar `(SELECT …)`.

## REVIEW TASK — push joins to the merge engine, not Python

We keep finding cross-source joins executed in Python row loops instead of being
handed to the DuckDB merge engine. Principle: **local joins run set-based in the
merge engine; we should not hand-roll join/group/sort logic in Python.** Audit
every physical operator for Python-side processing that DuckDB should own:
- `PhysicalNestedLoopJoin` — DONE (now routes through the merge engine; the
  Python loop is only a no-engine fallback). It used to re-execute its inner
  side once per outer row (a remote query per row, cross-source).
- Re-check `PhysicalHashJoin` row-loop fallbacks, `PhysicalGroupedLimit`
  (per-key limit done in Python), `PhysicalSingleRowGuard`, set-operation /
  union dedup, and any operator with an `_execute_*` Python path.
- For each: confirm it materializes inputs once and pushes the operation to the
  merge engine (or document why a Python path is genuinely required).

## Current test state

- Full suite: **766 passed / 15 xfailed / 0 failed**
  (`POSTGRES_DB=duckpoc /workspace/venv-fedq/bin/python -m pytest tests/ -q`;
  needs `make pg-start`).
- `tests/e2e_decorrelation/`: green.
- `tests/e2e_pushdown/test_subqueries.py`: rewritten to assert the join shape +
  a global no-subquery-expression invariant (`_assert_no_subquery_expressions`).

## What was done (the rule violation from Phase 2a/2b is fixed)

- Stripped the EXISTS/scalar re-correlation renderer and the whole
  `_inline_scalars` pre-pass from `optimizer/single_source_pushdown.py`
  (789 → ~567 lines).
- `_absorb_join` now renders **every** join type as SQL. `_JOIN_KEYWORDS` gained
  `SEMI JOIN`/`ANTI JOIN`. The right side renders via `_render_relation`:
  a plain base scan → table ref; anything projected or aggregate-bearing →
  `_render_derived` → `(SELECT …) AS subq_N` (an independent sub-render).
- Decorrelated EXISTS/IN/ANY → `SEMI JOIN`; NOT EXISTS/NOT IN/ALL → `ANTI JOIN`;
  scalar → `LEFT JOIN` to a keyed aggregate. Verified vs raw DuckDB and Postgres
  (sqlglot rewrites `ANTI JOIN` → `WHERE NOT EXISTS` for the Postgres dialect on
  re-render — valid SQL, derived relation preserved).
- Aggregate sub-relations: the optimizer folds `Aggregate→Scan` into a
  `Scan(aggregates=…)`. `_absorb_aggregate_scan` renders such a scan, and
  `_absorb_aggregate_projection` resolves a projection that selects/renames an
  aggregate's outputs (`AVG(amount)` → `__subq_0_v0`) to the real expression.
- `has_derived_columns`: a `*` projection over a subtree whose LEFT/INNER join
  pulls in a derived relation can't be expanded from base scans alone, so that
  push declines and runs locally (keeps synthetic columns intact). SEMI/ANTI
  derived joins contribute no columns, so `SELECT *` over them still pushes.

## Remaining work (all xfailed with reasons, not failing)

- **Cluster B — derived tables / FROM-subqueries**: CLOSED. `single_source`
  renders a `SubqueryScan` as `(SELECT ...) AS alias` keeping the user alias:
  `_render_subquery_scan` (JOIN/LATERAL right side) + `_absorb_subquery_scan_base`
  (FROM base) + `has_subquery_relation` so a derived-table FROM pushes even
  without a join. Multi-source derived-table joins already worked via the merge
  engine (the planner unwraps `SubqueryScan` and pushes each single-source piece
  as an Arrow stream); cluster B was only the single-source one-query push.
- **Cluster A — decorrelation gaps**: 3 of 4 now CLOSED.
  - DONE `test_scalar_subquery_in_having` — already worked; xfail removed.
  - DONE `test_subquery_with_order_by_limit` — `_peel_values_top` keeps `Sort`
    wrappers (uncorrelated only); single_source renders a scan's folded
    `order_by_keys` (the optimizer folds the Sort into the Scan).
  - DONE `test_subquery_with_group_by` — `_peel_values_top` keeps `Filter`
    (HAVING) wrappers; single_source `_split_scan_filter` routes a folded
    aggregate-scan filter into WHERE vs HAVING and substitutes decorrelation's
    hoisted aggregate aliases (`__subq_0_h1`) back to the real expression.
  - The `Sort`/`Filter` peel is gated to **uncorrelated** subqueries
    (`prepare_values`: `allow_wrappers = not self._is_correlated(plan)`).
  - DONE correlated scalar `ORDER BY … LIMIT n` (pick-one/dedup): decorrelates to
    a LEFT join + order-aware `GroupedLimit` (sort within each correlation key,
    take n). Added `order_by_*` to `GroupedLimit` (logical + physical + planner),
    `prepare_scalar` captures the Sort, `_assemble`/`_apply_pending_limit` build
    the ordered per-key limit, and the `SingleRowGuard` is skipped for LIMIT 1.
    Equi-correlation only; non-equi needs a general dependent join. Follow-up:
    semi-join reduction / dynamic filter pushdown so the inner scan stays narrow.
  - DONE non-equi correlated scalar (aggregate/LIMIT) → LATERAL (dependent) join.
    New `LateralJoin` logical node; `decorrelation._join_scalar` falls back to it
    when pattern-flattening fails (`_needs_lateral`); `single_source_pushdown`
    renders `LEFT JOIN LATERAL (...) ON TRUE` for same-source push.
  - DONE user-written LATERAL (parse + bind lateral scope + render).
  - DONE cross-source LATERAL: `PhysicalLateralJoin` runs it in the merge-engine
    DuckDB (materialize left + right base as Arrow, register, decorrelate). The
    base is reduced to the left's correlation domain — `=`→`IN`, `<`/`>`→range
    bound (`_derive_domain_filter`), a sound superset. `render_correlated_sql`
    maps base scans → register names. Single base relation only; multi-base fails
    fast. Tests: `test_cross_source_lateral.py`. Follow-up: the same domain /
    dynamic-filter mechanism is what cross-source subquery fallback (cluster D)
    and skip-level correlation will reuse.
  - DONE `test_subquery_with_union` — UNION/INTERSECT/EXCEPT subquery body. Three
    layers: `SubqueryPlanBinder._bind_set_operation` binds both branches;
    decorrelation `_peel_values_top` keeps a `SetOperation` value relation intact
    (uncorrelated); single_source `_absorb_set_operation_base` renders it as a
    derived `(... UNION ...) AS u` — gated by `context.in_derived` so a top-level
    set op is still left to the planner's bare-UNION path. Cross-source works via
    the merge engine (the planner's `_plan_set_operation`). Tested same- and
    cross-source.
- **Cluster C — CTEs** (whole `test_ctes.py` xfailed): parser hard-rejects
  `WITH` (`parser.py`: "WITH clauses (CTEs) are not supported yet"). A CTE is a
  named relation, not a correlated subquery — separate feature, deferred.
- **Cluster D — cross-source** (`test_cross_datasource_subquery_fallback`):
  needs the cross-source materialization policy; the `multi_source_env` fixture
  also lacks the tables the test names.

## Architecture quick map

- Pipeline (`processor/query_executor.py::_plan_pipeline`): preprocess → parse →
  bind → **decorrelate** → optimize → physical plan.
- Decorrelation: `optimizer/decorrelation.py` (pattern-based; emits SEMI/ANTI/
  LEFT joins; fail-fast `DecorrelationError` on unsupported shapes).
- Single-source pushdown: `optimizer/single_source_pushdown.py`, invoked by
  `optimizer/physical_planner.py::_plan_node` via `try_build` (top-down).
- Remote SQL is built as a string then re-parsed via `datasource.parse_query`
  for the EXPLAIN document; on execution it is re-rendered to the source dialect.

## Coding rules (CLAUDE.md / AGENTS.md) — enforce

No list comprehensions; no bare except; ≤20 lines & cyclomatic ≤4 per function
(guard-heavy dispatch methods match the local idiom); comment every function;
no "pointless" verbose code; `black` clean; fail fast, don't wrap exceptions;
no compat cruft (delete, don't shim).
