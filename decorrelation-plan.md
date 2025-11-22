# Decorrelation Implementation Plan

## Goals
- Provide a deterministic decorrelation engine that rewrites correlated and uncorrelated subqueries into join-based or CTE-based logical plans before cost-based optimization.
- Preserve SQL semantics (NULL handling, duplicate handling, outer reference visibility) identical to DuckDB/PostgreSQL behaviour.
- Keep every implementation function under 20 lines with cyclomatic complexity ≤4, per repository standards, while adding clear docstrings for every method.
- Fail fast on unsupported patterns; never silently skip decorrelation.

## Scope
- Expressions: `EXISTS`, `NOT EXISTS`, `IN`/`NOT IN` with subqueries, scalar subqueries in SELECT/WHERE/HAVING/ON, quantified comparisons (`= ANY`, `> ALL`, etc.), and correlated predicates embedded inside subqueries.
- Locations: predicates (WHERE/HAVING), projections, join conditions, and nested subqueries within derived tables.
- Logical rewrite only; physical join selection stays in the optimizer/planner.
- Out of scope for first pass: windowed subqueries and recursive CTE decorrelation (documented as future work).

## Terminology and Semantics
- **Outer reference**: ColumnRef whose table resolves to an ancestor scope, not the current subquery scope.
- **Correlated subquery**: Contains at least one outer reference.
- **Uncorrelated subquery**: No outer references; can be hoisted or executed once.
- **Null-sensitive comparisons**: `IN`, `NOT IN`, `= ANY`, `<> ALL` need `IS NOT DISTINCT FROM` semantics to preserve NULL behaviour.
- **Multiplicity**: EXISTS/Semi join ignore duplicates; scalar subqueries preserve scalar/NULL result; quantified comparisons must respect duplicates only where SQL semantics require (e.g., `ALL` evaluates over all rows, including duplicates).

## Desired Output Shape
- After decorrelation, the logical plan contains only joins, projections, aggregates, and filters—no subquery expression nodes remain.
- Subqueries that are uncorrelated are rewritten into CTEs (WITH) or cross joins executed once and reused.
- Correlated predicates become joins with explicit join conditions on correlation keys; scalar subqueries become joins to aggregated subplans keyed by correlation keys.

## Rewrite Coverage Matrix
- EXISTS / NOT EXISTS in predicates → SEMI / ANTI joins with correlation predicates.
- IN (subquery) / NOT IN (subquery) → SEMI / ANTI joins using null-safe equality; anti join includes null-blocking logic.
- Quantified comparisons (`op ANY`, `op SOME`) → SEMI joins with comparison predicate.
- `op ALL` → Anti join plus guard to ensure no violating row; may require two subplans: one for violation detection (semi join) and one for NULL check.
- Scalar subqueries in SELECT list → LEFT join against grouped subplan keyed by correlation columns with enforced single-row-per-key (or raise if multiple).
- Scalar subqueries in predicates (e.g., `a = (SELECT ...)`) → LEFT join to grouped subplan + comparison against aggregated output.
- Nested correlated subqueries → recursively decorrelate innermost first, then re-run on parent.
- Derived table containing subqueries → decorrelate inside derived table first, then treat as normal input.

## Core Invariants
- No remaining subquery expressions post-pass.
- Join keys and output schemas stay deterministic: `internal_name` drives planning; user-visible aliases preserved.
- Duplicate elimination only where required by semantics (e.g., IN does not require full distinct unless planner chooses for efficiency).
- Null semantics preserved exactly; use null-safe equality for IN/ANY; NOT IN/ALL must yield FALSE when subquery contains NULL and no match.
- If a rewrite would introduce cardinality explosion or ambiguity (e.g., scalar subquery returning multiple rows without aggregation), raise a decorrelation error immediately.

## Architecture
- Extend expression tree with explicit nodes for subqueries (`ExistsExpression`, `SubqueryExpression`, `InSubquery`, `QuantifiedComparison`) so detection is structural, not string-based.
- Decorrelator acts on bound logical plans so ColumnRef scopes are known; integrates between binder and rule optimizer.
- Multi-phase walk:
  1. **Correlation analysis**: compute free variables of subqueries and classify correlated vs uncorrelated.
  2. **Normalization**: push predicates into subqueries where safe, simplify boolean trees, and normalize comparisons to a canonical form (`lhs op rhs`).
  3. **Rewrite**: apply rule set below, producing new logical nodes.
  4. **Validation**: ensure no subquery nodes remain; verify join conditions cover all correlation keys.
- Keep helper functions single-purpose to satisfy complexity limits: detection, key extraction, join construction, aggregation construction, null-handling guard builder.

## Rewrite Rules (detailed)
- **EXISTS(subq)**:
  - Correlated: build SEMI join between outer input and rewritten subquery input; join predicate is conjunction of correlation predicates captured during analysis.
  - Uncorrelated: transform into `CROSS JOIN (SELECT 1 WHERE EXISTS ...)` evaluated once; push down as `Limit 1` scan if possible.
- **NOT EXISTS(subq)**:
  - Correlated: ANTI join on correlation predicates.
  - Uncorrelated: ANTI join against one-row check (subquery limited to 1) so we only test emptiness.
- **expr IN (subq)**:
  - Correlated: SEMI join on null-safe equality between `expr` and subquery projection; add null-rejecting filter if subquery can emit NULL and no match was found (implement with left join + IS NOT NULL guard if needed to preserve SQL NULL semantics).
  - Uncorrelated: hoist subquery as CTE with optional DISTINCT; SEMI join outer input to CTE.
- **expr NOT IN (subq)**:
  - Correlated: LEFT join on null-safe equality, then filter where match is null and subquery emitted no NULLs; if subquery nullable column produces NULL anywhere, result is FALSE/UNKNOWN, so build an `exists_null` guard via aggregate.
  - Uncorrelated: same pattern but hoisted CTE executed once.
- **Quantified comparison `lhs op ANY(subq)`**:
  - Rewrite to SEMI join with predicate `lhs op sub.col`; `<> ANY` is equivalent to `NOT lhs = ALL`.
  - Handle NULL like IN; use null-safe predicate.
- **Quantified comparison `lhs op ALL(subq)`**:
  - Build ANTI join that searches for violating rows: `lhs NOT op sub.col`; if any violation exists, predicate fails.
  - Add NULL guard: if subquery produces NULL and no violation, result is UNKNOWN → filter should not pass; enforce via additional aggregate flag.
- **Scalar subquery in SELECT list**:
  - Correlated: rewrite to LEFT join with subquery aggregated by correlation keys; aggregation enforces single row via `COUNT(*)` guard; raise if count >1.
  - Uncorrelated: compute once via CTE; cross join into outer plan.
  - Propagate alias mapping: visible name stays from SELECT alias; internal name uses qualified subquery column.
- **Scalar subquery in predicates**:
  - Same shape as projection rewrite, but comparison applied after join; ensure null-safe comparison when required.
- **Nested levels**:
  - Recurse innermost-first; after each rewrite, re-run correlation analysis until fixed point; guard against infinite loops via rewrite counter.
- **Derived tables**:
  - Decorrelate inside derived table query; treat output as scan with alias; ensure correlation keys referencing derived tables are rewritten against its output columns.

## Rewrite Examples and Templates
- **Uncorrelated scalar subquery (SELECT list)**  
  Input: `SELECT u.id, (SELECT max(o.amount) FROM orders o) AS max_amt FROM users u;`  
  Rewrite:  
  1. Build subplan `S` = `Aggregate(Scan(orders))` producing one row `{max_amount}`.  
  2. Hoist `S` as CTE `cte0`.  
  3. Plan: `Join(type=CROSS, left=Scan(users u), right=Scan(cte0), condition=None)` then `Project(id, max_amt)`.  
  Result executes subquery once and reuses for all rows.

- **Correlated scalar subquery (SELECT list)**  
  Input: `SELECT u.id, (SELECT sum(o.amount) FROM orders o WHERE o.user_id = u.id) AS total FROM users u;`  
  Rewrite:  
  1. Subplan `S` = `Aggregate(key=o.user_id, agg=sum(amount))` over `Scan(orders o)`.  
  2. Left join outer `users` to `S` on `u.id = o.user_id`.  
  3. Projection uses aggregated column; if multiple rows per key possible, add guard `count_star <= 1` or raise `DecorrelationError`.  
  4. Null result preserved via LEFT join.

- **Uncorrelated EXISTS**  
  Input: `SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 100);`  
  Rewrite:  
  1. Subplan `S` = `Limit(1, Filter(amount>100, Scan(orders)))`.  
  2. If `S` non-empty, predicate true for all outer rows: convert to `CROSS JOIN S` plus `Filter(S.present)` or compute boolean once and treat as constant filter.  
  3. Logical plan becomes constant filter; may short-circuit to either original scan (true) or empty plan (false).

- **Correlated EXISTS**  
  Input: `SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100);`  
  Rewrite:  
  1. Subplan `S` decorrelated to `Filter(o.amount>100, Scan(orders o))`.  
  2. Build `Join(type=SEMI, left=Scan(users u), right=S, condition=u.id = o.user_id)`.  
  3. Projection over SEMI join output.

- **Uncorrelated IN**  
  Input: `SELECT * FROM users WHERE country IN (SELECT code FROM countries WHERE enabled);`  
  Rewrite:  
  1. Subplan `S` = `Projection(code, Filter(enabled, Scan(countries)))`.  
  2. Optionally add `Distinct` if planner cannot ensure uniqueness.  
  3. Hoist as CTE `cte1`; plan `Join(type=SEMI, left=Scan(users), right=Scan(cte1), condition=users.country IS NOT DISTINCT FROM cte1.code)`.

- **Correlated IN**  
  Input: `SELECT * FROM users u WHERE u.city IN (SELECT c.name FROM cities c WHERE c.country = u.country);`  
  Rewrite:  
  1. Subplan `S` = `Projection(c.name, Filter(c.country = outer.country, Scan(cities c)))`.  
  2. SEMI join outer to `S` on null-safe equality `u.city IS NOT DISTINCT FROM c.name` and correlation predicate `u.country = c.country`.

- **Correlated NOT IN**  
  Input: `SELECT * FROM users u WHERE u.city NOT IN (SELECT c.name FROM cities c WHERE c.country = u.country);`  
  Rewrite:  
  1. Build subplan `S` as above.  
  2. Left join outer to `S` on null-safe equality and correlation keys.  
  3. Add aggregate on `S` to compute `has_match` and `has_null`.  
  4. Filter condition: `has_match IS NULL AND has_null = FALSE`; if `has_null = TRUE`, predicate evaluates to FALSE/UNKNOWN.

- **ANY / SOME**  
  Input: `WHERE price > ANY(SELECT amount FROM offers WHERE offers.seller = sales.seller)`  
  Rewrite: SEMI join on condition `sales.price > offers.amount` plus correlation `sales.seller = offers.seller`. Null-safe comparison identical to IN semantics.

- **ALL**  
  Input: `WHERE price <= ALL(SELECT cap FROM limits WHERE limits.region = sales.region)`  
  Rewrite:  
  1. Build violating subplan: `Filter(price > cap, limits(region=outer.region))`.  
  2. ANTI join to ensure no violations.  
  3. Add NULL guard subplan to detect NULL in `cap`; if any NULL and no violation, result UNKNOWN → filter should fail. Implement via aggregate flags.

- **Nested subqueries**  
  Input: `WHERE EXISTS (SELECT 1 FROM A WHERE A.x = outer.x AND A.id IN (SELECT B.id FROM B WHERE B.y = A.y))`  
  Rewrite:  
  1. Decorrelate innermost `IN` first (likely SEMI join).  
  2. Replace inner subquery with join in `A` subplan.  
  3. Re-run analysis; outer EXISTS becomes SEMI join of main input with rewritten `A` plan on `outer.x = A.x`.

## CTE Hoisting and Naming
- Each uncorrelated subquery becomes a reusable CTE (`cte_subq_N`) with stable numbering to allow deterministic tests.
- If the same uncorrelated subquery appears multiple times structurally equal, reuse the same CTE and reference it in all consuming joins.
- CTE columns use visible aliases from the subquery output; internal names retain source qualification when present.
- CTEs are introduced closest to the consumer query block to avoid scope leakage.

## Join Construction Rules
- SEMI/ANTI joins represent EXISTS/NOT EXISTS/IN/NOT IN/ANY/ALL patterns; LEFT joins used for scalar subqueries to preserve NULLs.
- Join condition is conjunction of correlation predicates and comparison predicates. If no correlation (uncorrelated), join degenerates to CROSS join when hoisting scalar/boolean constants.
- Null-safe comparisons use `IS NOT DISTINCT FROM` equivalent in expression tree; never rely on `=` for IN/ANY.
- Aggregated subplans for scalar rewrites must output both value and optional `row_count` flag to detect multi-row violations.

## Planner Interaction
- Decorrelator runs after binding (scopes known) and before rule-based optimizer.
- Optimizer rules must assume no subqueries remain; join ordering/costing can treat decorrelated joins normally.
- Physical planner should see SEMI/ANTI joins explicitly for efficient planning.
- For CTE hoisting, maintain a registry of reusable uncorrelated subplans and substitute references consistently.

## Error Handling and Validation
- If correlation keys cannot be derived (e.g., ambiguous column), raise a `DecorrelationError`.
- If scalar subquery cannot be proven single-row and no aggregation provided, raise immediately.
- If quantified comparison uses unsupported operator, raise.
- No catch-all exception wrappers; let specific errors surface.

## Data Structure Additions
- Expression nodes for subqueries and quantified comparisons with explicit attributes for:
  - `subquery_plan`
  - `correlation_refs` (list of ColumnRefs)
  - `comparison_op` where relevant
- Decorrelator helper outputs:
  - `correlation_predicates` list used to build join condition
  - `null_guard` expression when required
- Optional metadata on LogicalPlanNode to mark hoisted CTE identifiers for reuse.

## Testing Plan
- Unit tests per rule: EXISTS, NOT EXISTS, IN/NOT IN, ANY/ALL, scalar subqueries (correlated and uncorrelated), nested correlation, derived table correlation.
- Semantic tests for NULL handling: `value IN (SELECT NULL)`, `NOT IN` with NULL, scalar subquery returning NULL vs value.
- Cardinality enforcement tests: scalar subquery returning multiple rows should raise.
- Integration: end-to-end queries against DuckDB/PostgreSQL fixtures that mirror DuckDB’s expected plans and outputs.
- Regression: ensure existing non-subquery queries remain unchanged after decorrelation pass.

## Implementation Steps
1. Add subquery expression classes and a `DecorrelationError`.
2. Extend parser to build these expressions from sqlglot AST (Select in expressions, quantified comparisons).
3. Extend binder to tag ColumnRefs with scope information and populate `correlation_refs`.
4. Implement correlation analysis pass (pure detection, no rewrites).
5. Implement normalization utilities (predicate pushdown, null-safe comparison helper).
6. Implement rewrite rules iteratively (EXISTS/NOT EXISTS → IN/NOT IN → ANY/ALL → scalar).
7. Add CTE hoisting mechanism for uncorrelated subqueries.
8. Wire Decorrelator into the optimization pipeline; ensure optimizer assumes subqueries already removed.
9. Add comprehensive tests and update docs/examples.

## Risks and Mitigations
- **Null semantics regressions**: codify test matrix for NULL-heavy cases; use explicit null-safe operations.
- **Cardinality blow-up**: enforce aggregates on scalar rewrites and raise on multi-row results.
- **Complexity creep**: keep helpers tiny; factor shared builders for joins/aggregates.
- **Unsupported SQL variants**: fail fast with clear error, add future work notes instead of partial rewrites.

## Definition of Done
- No LogicalPlan contains subquery expressions post-decorrelation.
- All rewrite patterns listed above covered by automated tests across DuckDB/PostgreSQL.
- Decorrelator integrated and used by the optimizer on every query.
- Error cases behave predictably (structured exceptions, no silent fallbacks).
