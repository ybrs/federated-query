# Decorrelation Gaps & the "No Subqueries in the Physical Plan" Goal

## North star

The logical-planning phase should **fully unnest every subquery** so that the
physical planner never sees a subquery expression. After binding +
decorrelation we want a **flat relational plan** — scans, joins (incl.
semi/anti), aggregates, set operations — exactly like the shape DuckDB feeds its
executor. Two payoffs:

1. **Pushdown becomes uniform.** A decorrelated `EXISTS`/`IN`/scalar subquery is
   just a join; pushing it to a source is then the *same* problem as ordinary
   join pushdown (the G1 single-source generator), including SEMI/ANTI. Most of
   the G8 "subquery pushdown shape" failures dissolve into "decorrelate fully,
   then push the resulting join."
2. **The physical layer stays simple.** No subquery operators, no per-row
   re-execution, no correlated-scan special cases.

Reference approach: Neumann & Kemper, *"Unnesting Arbitrary Queries"* (2015) —
the general **dependent join** ("magic" decorrelation) that unnests *any*
correlated subquery, with algebraic rules to push the dependent join down until
the correlation disappears. Our current engine does **pattern-based**
decorrelation (a fast path per recognized shape); the end state keeps those fast
paths as optimizations but adds a general dependent-join fallback so nothing is
left un-unnested.

**Physical subquery planning is a last resort.** Ideally never reached. We add
it only if a construct is provably un-decorrelatable (none of the gaps below
are — they are all unnestable with a general dependent join; today they just hit
pattern-coverage limits and fail fast).

## Current state (pattern-based decorrelation, Phases 7–8)

All decorrelation e2e tests are green (full suite **809 passed / 0 xfailed /
0 failed**). Covered today:

- `EXISTS`/`NOT EXISTS` → SEMI/ANTI; `IN`/`NOT IN` (incl. tuple IN) with exact
  three-valued NULL semantics; `ANY`/`SOME`/`ALL`.
- Correlated and uncorrelated scalar subqueries → LEFT join to a keyed aggregate
  (+ COALESCE for COUNT, runtime cardinality guard).
- Correlated `ORDER BY … LIMIT n` (pick-one / latest-per-key) → LEFT join +
  order-aware `GroupedLimit` (now pushed to the merge engine as a
  `ROW_NUMBER() OVER (PARTITION BY …)` window).
- **Value/scalar output shapes** — body topped by `Filter` (HAVING) or `Sort`
  (ORDER BY/LIMIT) now peel (uncorrelated). *(gap A — CLOSED)*
- **Set-operation subquery body** (`UNION`/`INTERSECT`/`EXCEPT`) binds and
  decorrelates. *(gap B — CLOSED)*
- **Non-equality correlation through an aggregate/limit** → `LateralJoin`
  (dependent join); same-source pushes `LEFT JOIN LATERAL`, cross-source runs in
  the merge engine with domain reduction. User-written `LATERAL` also supported.
- **CTEs** (`WITH`, incl. `RECURSIVE`) — single-source pushed as one remote
  `WITH`; cross-source via materialize-once producer / merge-engine recursion.

Unsupported patterns still **fail fast** with `DecorrelationError` (never
silently wrong) — each has a test in
`tests/e2e_decorrelation/test_error_cases.py`.

## Remaining gaps (all fail-fast)

These are the patterns pattern-based decorrelation still gives up on. Each
raises `DecorrelationError`; none can return a wrong answer. All are unnestable
in principle via a general dependent join (the Phase 9 plan below). Tests live
in `tests/e2e_decorrelation/test_error_cases.py`.

| Gap | Behavior | Test |
|-----|----------|------|
| **Skip-level correlation** (subquery references a relation 2+ levels up) | fail-fast | `test_skip_level_correlation` |
| **Subquery in `GROUP BY` / aggregate-argument position** | fail-fast | `test_subquery_in_group_by` |
| **`OFFSET` in a correlated subquery** | fail-fast | `test_offset_in_correlated_subquery` |
| **Multi-column scalar / quantified subquery** | fail-fast | `test_quantified_comparison_multi_column_subquery` |
| **`SELECT *` value subquery** | fail-fast | `test_select_star_value_subquery` |
| **Subquery in a non-INNER join `ON`; multi-row `VALUES` subquery; two correlation equalities over a global aggregate** | fail-fast | guarded in `decorrelation.py`; no dedicated test yet |

## Phase 9 — general dependent-join decorrelation (the big rewrite)

The end state moves from pattern-based decorrelation (fast paths per recognized
shape) to a **general dependent join** (Neumann & Kemper, *"Unnesting Arbitrary
Queries"*, 2015): emit a dependent join, then push it down with algebraic rules
until the correlation disappears. The pattern fast-paths stay as optimizations;
the general fallback removes every "unsupported shape" fail-fast above and
guarantees a subquery-free physical plan unconditionally.

Scope for Phase 9:

1. **General dependent join** subsuming the remaining gaps (skip-level,
   two-equality global aggregate, non-INNER `ON`, multi-column / `SELECT *` /
   multi-row value subqueries, `OFFSET`, GROUP-BY-position).
2. **Cross-source correlated-subquery fallback** (the old "cluster D"): a
   correlated subquery that cannot decorrelate same-source falls back to the
   cross-source dependent-join path, reusing the existing LATERAL / CTE
   materialize-and-register + domain-reduction machinery (`PhysicalLateralJoin`,
   `PhysicalCTE*`). No test/fixture exists yet.

This is a **large, deliberately-deferred** task. Phase 8 (pushdown breadth,
decorrelation coverage, CTEs) is closed; the items here are the next phase, not
blockers.
