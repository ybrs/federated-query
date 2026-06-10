# Decorrelation Execution Plan

This document tracks the step-by-step implementation plan for the decorrelation engine. Each task is scoped to keep functions short (\<=20 LOC) and cyclomatic complexity \<=4, and to fail fast on unsupported patterns.

## 0) Foundations (already started)
- Add subquery expression nodes (`SubqueryExpression`, `ExistsExpression`, `InSubquery`, `QuantifiedComparison`, `Quantifier`).
- Parser builds these nodes from sqlglot AST.
- Decorrelator inserted after binding, before rule optimizer.
- Introduce `Logical CTE` wrapper for hoisting.
- Physical joins support SEMI/ANTI output semantics (hash and nested-loop).
- CorrelationAnalyzer implemented to detect outer references.

## 1) Correlation Analysis
- Implement a `CorrelationAnalyzer` that walks expressions to:
  - Identify outer references vs local references for each subquery.
  - Extract correlation predicates (ColumnRef pairs) and classify correlated vs uncorrelated subqueries.
  - Detect unsupported constructs early (multiple columns in scalar subquery, window functions, recursive CTEs).
- Output metadata: `is_correlated`, `correlation_keys`, `nullable_flags` for subquery outputs.

## 2) Utility Builders
- Null-safe comparison helper: build `IS NOT DISTINCT FROM`-style expressions for equality.
- Join condition builder: combine correlation predicates with comparison predicates.
- Aggregate builder for scalar rewrites: group by correlation keys, produce value + row_count/null flags.
- CTE hoister with deterministic naming (`cte_subq_<n>`), reuse identical uncorrelated subqueries.

## 3) EXISTS / NOT EXISTS
- Correlated EXISTS → SEMI join on correlation predicates.
- Correlated NOT EXISTS → ANTI join on correlation predicates.
- Uncorrelated: rewrite to one-row existence check (Limit 1) and constant filter; hoist as CTE.
- Validation: ensure no subquery expressions remain; raise `DecorrelationError` on failure.

## 4) IN / NOT IN
- Correlated IN → SEMI join using null-safe equality on value vs subquery projection.
- Correlated NOT IN → left join + aggregate null/match flags or ANTI join with null guard.
- Uncorrelated variants hoisted as CTEs (optional DISTINCT for IN).
- Tuple IN (multi-column) support using composite null-safe comparisons.

## 5) Quantified Comparisons (ANY/SOME/ALL)
- ANY/SOME: treat like IN with comparison operator; SEMI join on predicate.
- ALL: ANTI join looking for violations + null guard aggregate when subquery can emit NULL.
- <> ALL / = ANY mapped to NOT IN / IN semantics, preserving NULL rules.

## 6) Scalar Subqueries
- Uncorrelated scalar: hoist as CTE, CROSS join once.
- Correlated scalar: LEFT join to aggregated subplan keyed by correlation columns; enforce single-row via row_count guard; raise `DecorrelationError` if violation.
- Scalars in WHERE/HAVING/ON, SELECT list, ORDER BY, CASE.

## 7) Nested & Derived Contexts
- Recursive pass: decorrelate innermost first, iterate to fixed point.
- Handle subqueries inside derived tables, join conditions, and nested subqueries.
- Prevent infinite loops with rewrite counter; raise if exceeded.

## 8) Physical Layer Support
- Extend physical joins to honor SEMI/ANTI semantics (emit only left rows for SEMI, left rows with no match for ANTI).
- Ensure executor handles null-safe comparisons for IN/ANY and null guards for NOT IN/ALL.

## 9) Validation & Errors
- After rewrite, verify no subquery expressions remain.
- Ensure correlation predicates covered in join conditions.
- Fail fast with `DecorrelationError` for unsupported patterns (multi-column scalar, windowed subqueries, recursive CTEs, unsupported quantified operators).

## 10) Testing & Rollout
- Run decorrelation e2e suite in `tests/e2e_decorrelation/`.
- Add targeted unit tests for analysis helpers and builders.
- Iterate per pattern until tests converge.

---
### Current status — Phase 7 IMPLEMENTED

All four rewrite families are implemented and the full
`tests/e2e_decorrelation` suite (116 tests) passes:

- **EXISTS / NOT EXISTS** → SEMI/ANTI joins; uncorrelated probes a `LIMIT 1`
  subplan with no condition.
- **IN / NOT IN** → SEMI join with plain SQL equality / ANTI join with a
  NULL-aware match (`x = v OR x IS NULL OR v IS NULL`), giving exact
  three-valued WHERE semantics. Tuple IN supported.
- **ANY / SOME / ALL** → SEMI join on the comparison / ANTI join over
  violations with NULL guards; `LIKE ALL/ANY` supported.
- **Scalar subqueries** → LEFT join to an aggregate keyed by the correlation
  columns; `COUNT` wrapped in `COALESCE(.., 0)`; non-aggregated scalars get a
  runtime `SingleRowGuard` (raises `CardinalityViolationError`, matching real
  engines); correlated `LIMIT n` becomes a per-key `GroupedLimit`.
- **Boolean subqueries in SELECT lists** → SEMI/ANTI branch pair unioned with
  literal flags (UNION ALL partitions rows exactly once).
- **OR containing subqueries** → distinct-union expansion of the disjuncts.
- **HAVING aggregates** referenced only in the predicate are hoisted into the
  aggregate's outputs.
- Nested subqueries decorrelate innermost-first; derived tables
  (`SubqueryScan`) and subqueries in INNER join conditions are handled.
- Binder performs scoped subquery binding (inner-first resolution, outer
  fallback, fail-fast on unknown names).

Deliberate design deviations from the original sketch:
- Uncorrelated subqueries are **inlined** as join inputs instead of hoisted
  to CTEs — the execution layer has no CTE support, and `_plan_cte` now
  fails fast instead of silently dropping the CTE subplan. CTE reuse is a
  future optimization.
- Boolean flag columns collapse UNKNOWN to FALSE (documented in the module).

Known unsupported patterns (raise `DecorrelationError`):
- Skip-level correlation (a subquery referencing a query two levels up)
  needs dependent-join machinery.
- Non-equality correlation through aggregates/limits.
- Subqueries in GROUP BY or aggregate arguments; subqueries in non-INNER
  join conditions; OFFSET in correlated subqueries.
