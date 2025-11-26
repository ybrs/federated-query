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
### Current status
- Correlation analysis helper in place.
- SEMI/ANTI physical join execution implemented with tests.
- Decorrelator skeleton traverses plans/expressions and fails fast on subqueries.
- Next: implement concrete rewrites (EXISTS/NOT EXISTS first), null-safe helpers, CTE hoisting use, and validation.
