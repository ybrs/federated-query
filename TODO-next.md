# TODO-next — handoff / restart notes

State of the subquery-pushdown work on branch `phase8`. The "No Subqueries in the
Physical Plan" rule is now enforced: decorrelation produces a flat join plan and
single-source pushdown renders it as **SQL joins**, never re-correlated
`EXISTS`/`IN`/scalar `(SELECT …)`.

## Current test state

- Full suite: **763 passed / 17 xfailed / 1 xpassed / 0 failed**
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

- **Cluster B — derived tables / FROM-subqueries** (`SubqueryScan` rendering):
  `test_subqueries.py::test_subquery_in_from_derived_table`,
  `::test_derived_table_with_join`,
  `test_aggregate_advanced.py::test_nested_aggregate_via_subquery`.
  `single_source_pushdown` has no `SubqueryScan` handler; needs a FROM/JOIN
  derived-table render that preserves the `SubqueryScan` alias for outer column
  resolution. Target shape: a relation subquery (`exp.Subquery` in FROM/JOIN) —
  allowed by the invariant.
- **Cluster A — decorrelation gaps** (subquery *body* doesn't decorrelate yet):
  `test_subquery_with_group_by` (Filter/HAVING top — `_peel_values_top`),
  `test_subquery_with_order_by_limit` (Sort/Limit top),
  `test_subquery_with_union` (SetOperation body rejected by the binder),
  `test_scalar_subquery_in_having` (xpasses now — HAVING scalar; revisit).
  These fail *before* rendering, in `optimizer/decorrelation.py` /
  `parser/binder.py`.
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
