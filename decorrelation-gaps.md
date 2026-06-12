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

## Current state (pattern-based decorrelation, Phase 7)

Covered and green (116/116 decorrelation e2e tests): `EXISTS`/`NOT EXISTS` →
SEMI/ANTI; `IN`/`NOT IN` (incl. tuple IN) with exact three-valued NULL
semantics; `ANY`/`SOME`/`ALL`; correlated and uncorrelated scalar subqueries →
LEFT join to a keyed aggregate (+ COALESCE for COUNT, runtime cardinality guard,
per-key limit for correlated LIMIT); boolean subqueries in SELECT lists;
OR-of-subqueries via union expansion; nested subqueries innermost-first; derived
tables; INNER-join-condition subqueries. Unsupported patterns **fail fast** with
`DecorrelationError` (never silently wrong).

## The gaps (grounded in code + failing tests)

Each item is a place where pattern-based decorrelation gives up today. All are
unnestable in principle via a general dependent join.

### A. Value/scalar subquery output shapes — `_peel_values_top`
`federated_query/optimizer/decorrelation.py:635` only peels a subquery body that
is topped by `Limit`, `Projection`, `Values`, or `Aggregate`. Other tops raise
`DecorrelationError: Unsupported subquery output shape: <Node>`:
- **Body topped by `Filter`** — `test_subqueries::test_subquery_with_group_by`.
- **Body topped by `Sort`** (ORDER BY / LIMIT inside the subquery) —
  `test_subqueries::test_subquery_with_order_by_limit`. The
  "latest-row-per-key" idiom (`ORDER BY ... LIMIT 1`) needs a Sort+Limit peel
  combined with the existing `GroupedLimit`.

### B. Set-operation subquery body — subquery binder
`federated_query/parser/binder.py:1231` raises
`Unsupported plan node in subquery: SetOperation` for a `UNION`/`INTERSECT`/
`EXCEPT` used as a subquery body — `test_subqueries::test_subquery_with_union`.
The subquery binder + IN/EXISTS rewrite must accept a `SetOperation` body.

### C. Skip-level correlation (the canonical dependent-join case)
A subquery that references a relation **two or more levels up**. Needs real
dependent-join machinery; documented unsupported in `decorrelation-tasks.md`.

### D. Correlation through aggregates / limits
- **Non-equality** correlation pushed through an aggregate or limit.
- **Two correlation equalities over a global (ungrouped) aggregate** — the
  key-widening rewrite can't tell its own added keys from original GROUP BY keys.
- **`OFFSET` in a correlated subquery** — `decorrelation.py:519`.

### E. Subqueries in disallowed positions
- In `GROUP BY` or aggregate arguments.
- In a **non-INNER** join `ON` condition.
- (Both documented unsupported in `decorrelation-tasks.md`.)

### F. Value-subquery shape restrictions
- **Multi-column** scalar subquery — `decorrelation.py:527` /`:595`.
- **`SELECT *`** value subquery — `decorrelation.py:657`.
- **Multi-row `VALUES`** subquery — `decorrelation.py:666`.

### G. Binding gap to investigate
`test_subqueries::test_exists_with_complex_predicates` fails with
`BindingError: Column 'quantity' not found in table 'P'`. Unlike the others this
is a *binding* failure, not a clean decorrelation fail-fast — likely a
scope/resolution bug (or a stale fixture reference). Triage separately.

## Relationship to G8

The 20 red `test_subqueries` tests split three ways:
- **~14** decorrelate correctly and return right answers, but execute as a
  **local** join (2+ remote scans) while the test wants **one** pushed remote
  query — a *pushdown* gap (push decorrelated SEMI/ANTI/scalar joins), a natural
  extension of G1, **not** a decorrelation gap.
- **~4** are real decorrelation coverage gaps above (A, B, D).
- **~2** are binding gaps (B, G).

So "finish G8" = (1) extend the single-source generator to push decorrelated
semi/anti/scalar joins, and (2) close the decorrelation/binding gaps in this
doc. The strategic version of (2) is to move from pattern-based to general
dependent-join decorrelation — a **large** task, deferred deliberately.

## Suggested sequencing (when picked up)

1. Cheap coverage wins first: A (Filter/Sort peel) and B (SetOperation body).
2. Fix the G-bucket binding error.
3. Generalize to a dependent join (C/D/E) — the big rewrite that removes the
   "unsupported shape" failures wholesale and guarantees a subquery-free
   physical plan.
4. Pushdown of decorrelated semi/anti/scalar joins (the G8 shape bucket) — folds
   into the G1 generator once those joins are in scope.
