# Decorrelation Capability Matrix

Ground-truth inventory of what the pattern-based decorrelation engine can and
cannot do, verified empirically (parse → bind → decorrelate) on 2026-06-19.
Every supported pattern is exercised by `tests/e2e_decorrelation/`; every
unsupported pattern **fails fast with a clear error** (never a silent wrong
answer) and is pinned by a test in `test_error_cases.py`.

## Supported — decorrelates to a flat relational plan

| Pattern | Decorrelated form | Tested in |
|---|---|---|
| `EXISTS` (correlated/uncorrelated) | SEMI join | `test_exists.py` |
| `NOT EXISTS` | ANTI join | `test_exists.py` |
| `IN` / correlated `IN` / tuple `IN` | SEMI join (3-valued NULL aware) | `test_in.py` |
| `NOT IN` | ANTI join (NULL aware) | `test_in.py` |
| `ANY` / `SOME` | SEMI join | `test_quantified_comparisons.py` |
| `ALL` | ANTI join | `test_quantified_comparisons.py` |
| Scalar subquery — uncorrelated, correlated, in SELECT, in WHERE, in HAVING | LEFT join to keyed aggregate (+ COALESCE for COUNT, + cardinality guard) | `test_scalar_subqueries.py` |
| Subquery body topped by Aggregate / Projection / Limit / Values | peeled | `test_scalar_subqueries.py` |
| Derived table (`FROM (SELECT ...)`) | inlined SubqueryScan | `test_nested_subqueries.py` |
| Nested subqueries (innermost-first), immediate-parent chains | recursive SEMI/ANTI/LEFT | `test_nested_subqueries.py` |
| OR-of-subqueries | per-disjunct flags unioned | `test_in.py` / exists |

## Unsupported — fail-fast (pinned by `TestUnsupportedSubqueryShapes`)

| Pattern | Error | Gap (decorrelation-gaps.md) |
|---|---|---|
| Subquery body topped by `Sort` (`ORDER BY ... LIMIT`) | `DecorrelationError: Unsupported subquery output shape: Sort` | A |
| Subquery body is a `UNION`/`INTERSECT`/`EXCEPT` | `BindingError: Unsupported plan node in subquery: SetOperation` | B |
| `OFFSET` in a correlated subquery | `DecorrelationError: OFFSET in a correlated subquery ...` | D |
| Subquery in a `GROUP BY` position | `DecorrelationError: Subqueries in GROUP BY ...` | E |
| Skip-level correlation (references a relation 2+ levels up) | `DecorrelationError: ... unsupported position` | C |
| Multi-column / `SELECT *` scalar subquery | `DecorrelationError: ... exactly one column` | F |
| Multi-row scalar subquery (runtime) | `CardinalityViolationError: more than one row` | F |
| Window function in subquery | `ValueError: Window ...` | — |
| `LATERAL`, recursive `WITH` | `ValueError` | — |
| Negated / multi-column quantified operator | `DecorrelationError` | — |

## Not a decorrelation gap — the pushdown gap

A correlated `EXISTS`/`IN` on a **single source** decorrelates correctly to a
SEMI/ANTI join, but that join does **not** currently push to the source as one
remote query: `single_source_pushdown._JOIN_KEYWORDS` covers
INNER/LEFT/RIGHT/FULL only, so a SEMI/ANTI join falls out of pushdown and runs
in the DuckDB merge engine over two remote scans. That is the next phase —
extending the single-source generator to push SEMI/ANTI/scalar joins (rendering
them as `EXISTS`/`IN` for the Postgres dialect) — and is independent of the
decorrelation gaps above.

## The strategic fix

Closing gaps A–F one by one is the tactical path. The strategic version is to
move from pattern-based decorrelation to a general **dependent join** (Neumann &
Kemper 2015), which unnests *any* correlated subquery and removes the
"unsupported shape/position" failures wholesale. Large; deliberately deferred.
