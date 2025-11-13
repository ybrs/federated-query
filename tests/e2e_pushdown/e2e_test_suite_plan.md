# End-to-End Pushdown Test Suite Plan

The goal is to cover every meaningful pushdown scenario with explicit SQL + expectation tests. The suite will be implemented incrementally in the buckets below. Each bucket enumerates the specific combinations to cover, along with notes on expected behavior (whether pushdown should happen, where it should not, and how many datasources are involved).

---

## 1. Baseline Selects & Predicates (Single Table)
* Simple `SELECT *` with/without `WHERE`
* Column subsets (projection pushdown) with filters
* Predicates on numeric, string, boolean columns
* Compound predicates (AND/OR), IN lists, BETWEEN, LIKE
* LIMIT/OFFSET variations
* Expected: all predicates & projections push to datasource

## 2. Aggregations (Single Table)
* COUNT/SUM/AVG/MIN/MAX with/without GROUP BY
* Multiple aggregates + aliases
* HAVING clauses
* Aggregates combined with LIMIT/OFFSET
* Expected: aggregates & grouping pushed to datasource

## 3. Joins – Two Tables (Same Datasource)
### 3.1 Inner Joins
* SELECT * and specific column lists
* Join predicates expressed as `FROM A, B WHERE A.x = B.x` and explicit `JOIN`
* Additional filters on left, right, and both sides
* LIMIT prototypes
* Expected: remote join pushdown

### 3.2 Outer Joins (LEFT / RIGHT)
* Baseline outer join without filters
* Filters referencing nullable side
* Projections that include columns from both sides
* Aggregations on top of outer joins
* Expected: remote join pushdown, verifying null preservation

### 3.3 Join + Aggregation
* Aggregates after joins (e.g., `COUNT`, `GROUP BY` columns from both tables)
* HAVING after join-based aggregates
* Expected: remote join SQL should include GROUP BY + aggregates

### 3.4 Join + Order/Limits
* ORDER BY + LIMIT after joins
* Filter + ORDER BY + LIMIT combinations
* Expected: remote join still used; ORDER BY may or may not push depending on support (document behavior)

## 4. Joins – Three Tables (Same Datasource)
* Chain of joins (e.g., orders -> products -> customers)
* Mixed join types (INNER + LEFT)
* Aggregations across three tables
* Predicates referencing all tables
* Expected: evaluate whether multi-join pushdown is supported; if not, ensure tests capture fallback behavior + rationale

## 5. Combined Pushdowns
* Projection + predicate + aggregation + join + limit in a single statement
* Expressions in SELECT (computed columns) plus group by
* DISTINCT on top of joins/aggregations
* Expected: pushdowns should occur as far as support allows; tests document any unsupported combos

## 6. Multi-Datasource Guardrails
* Same queries as sections 3–5 but across different DuckDB datasources
* Ensure no remote joins span datasources
* Verify fallbacks (local hash join) and that SQL logs show separate queries per table

## 7. Edge Cases & Negative Tests
* Unsupported features (subqueries, window functions) – confirm they do not break pushdown logic
* NULL-safe predicates, IS NULL / IS NOT NULL across joins
* Unsupported join conditions (non-equi or OR conditions) – ensure graceful fallback

---

### Implementation Phasing
1. **Phase A:** Section 1 + 2 (single-table predicates & aggregates)
2. **Phase B:** Section 3 (two-table joins) without aggregates
3. **Phase C:** Section 3.3–3.4 (join + aggregate + order/limit)
4. **Phase D:** Section 4 & 5 (three tables, combined pushdowns)
5. **Phase E:** Section 6 (multi-datasource guardrails)
6. **Phase F:** Section 7 (edge cases & negative scenarios)

Each phase will:
- Introduce dedicated test modules under `tests/e2e_pushdown/`
- Document expected outcomes inline and in a summary table
- Capture datasource SQL logs for assertions
- Cover at least 25–40 explicit SQL cases per phase to surpass 200 overall by completion
