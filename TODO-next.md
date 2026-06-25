# TODO-next — handoff / restart notes

State of the subquery-pushdown work on branch `phase8`. The "No Subqueries in the
Physical Plan" rule is now enforced: decorrelation produces a flat join plan and
single-source pushdown renders it as **SQL joins**, never re-correlated
`EXISTS`/`IN`/scalar `(SELECT …)`.

## REVIEW TASK — push joins to the merge engine, not Python — DONE

Audited every physical operator (full per-operator table was produced). Result:
local set-based work runs in the DuckDB merge engine; Python paths remain only as
a no-engine fallback or for genuinely un-renderable expressions.
- `PhysicalNestedLoopJoin`, `PhysicalHashJoin`, `PhysicalHashAggregate`,
  `PhysicalSort`, `PhysicalUnion`, `PhysicalSetOperation` — merge engine when
  attached; Python only when no engine / un-renderable expression (sound).
- `PhysicalGroupedLimit` — **was the last violation** (per-key limit entirely in
  Python). Now pushed as `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY …)
  WHERE rn <= limit`, with a synthetic input-order index as the final ORDER BY
  tiebreaker so the merge path returns identical rows *and order* to the Python
  streaming path. Parity tests in `test_physical_decorrelation_operators.py`.
- `PhysicalSingleRowGuard` — a pure cardinality guard, no set-based work; N/A.
- `PhysicalRemoteJoin` — pushes the whole join to the source as SQL; N/A.

## Current test state

- Full suite: **809 passed / 0 xfailed / 0 failed**
- Coverage (numpy 2.4 + coverage crash on C-ext reimport): use the pre-import
  shim `/tmp/covrun.py` (imports duckdb/numpy/pyarrow before `coverage.start()`)
  with the `/workspace/venv-cov` venv (numpy 2.3.5). Plain pytest-cov dies on
  `import duckdb` under instrumentation.
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
- **Cluster C — CTEs** (`test_ctes.py`): CLOSED for single-source. A `WITH`
  parses into `CTE(name, cte_plan, child, recursive, column_names)` nodes with a
  dedicated `CTERef` leaf for each name reference (parser tracks in-scope CTE
  names). The binder registers each name as a synthetic relation (recursive:
  before binding its own body, requires an explicit column list) so a `CTERef`
  resolves like a table without a catalog lookup. `single_source_pushdown`
  renders the whole query as a pushed `WITH [RECURSIVE] name [(cols)] AS (body)
  child` statement — `_absorb_cte` collects each body, `CTERef` renders as the
  bare name, `_should_push` fires on `has_cte`, and `_resolve_datasource`
  defaults a pure-computation CTE (recursive counter, no scan) to the sole
  source. Verified vs DuckDB (simple/agg/union/join/multi-ref/nested/recursive).
  **Cross-source CTEs: CLOSED.** `_plan_cte` now branches by strategy:
  - **Non-recursive** → materialize-once producer/consumer. `_plan_cross_source_cte`
    plans the body to a `PhysicalCTE` (memoizes the body to one Arrow table),
    registers `name → producer` in `self._cte_producers`, plans the child, and
    each `CTERef` becomes a `PhysicalCTEScan` over the shared producer
    (`_plan_cte_ref`). The child's CTE-vs-other-source joins run in the merge
    engine. So a CTE referenced N times executes its body once.
  - **Recursive** → `_plan_recursive_cte_merge` runs the whole `WITH RECURSIVE`
    in the merge engine: `_collect_base_scans` gathers every base `Scan`,
    materializes each to Arrow under a generated name, and
    `render_correlated_sql(cte, scan_names)` renders the full WITH with those
    names; `PhysicalCTEMergeQuery` registers them and DuckDB computes the
    fixpoint locally.
  - Enablers: `single_source._claim_source` skips the single-datasource check in
    merge-render mode (`scan_names` set) so a multi-source WITH renders;
    `_cte_defined`/`visible_ctes` guard makes a child referencing a materialized
    (external) CTE decline to push so it plans structurally and reads the
    producer instead. New ops: `PhysicalCTE`, `PhysicalCTEScan`,
    `PhysicalCTEMergeQuery`. Tests: `test_cross_source_ctes.py` (4, vs combined
    DuckDB): body-single-source/child-joins-other, body-cross-source,
    multi-ref-materialize-once (asserts one shared producer), recursive
    cross-source (asserts `PhysicalCTEMergeQuery` with 2 registered sources),
    cross-source explicit-column-list (relabel), and a recursive hierarchy
    traversal over a real `employees(id, manager_id, name)` self-join in one
    source joined to `bonuses` in a second (local `hierarchy_env` fixture).
  - Edge coverage in `test_ctes.py`: non-recursive explicit column list
    (`WITH t(oid, p) AS …`) and a constant FROM-less body (`WITH x AS
    (SELECT 1 AS n) …`, exercises the sole-source default).
  - Coverage-driven follow-ups (`test_cross_source_ctes.py`): producer
    materialize-once + `execute()`/`repr`, `_plan_cte_ref` in-scope invariant,
    constant CTE in a multi-source catalog (no sole-source default → producer
    path), recursive-unrenderable fail-fast, and `PhysicalCTEMergeQuery.schema()`.
  - **Bug fixed while covering `schema()`:** `PhysicalCTEMergeQuery.schema()` AND
    the pre-existing `PhysicalLateralJoin.schema()` used a `… LIMIT 0` probe and
    read the schema off the *first batch* — but `LIMIT 0` yields **zero**
    batches, so both returned an empty schema. Added `MergeEngine.schema(sql,
    inputs)` which reads `to_arrow_reader().schema` (available before any batch);
    both operators now use it. Latent because neither `schema()` is called when
    the operator is the plan root (execution builds the schema from real
    batches); it only bites when the operator is nested under another.
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
