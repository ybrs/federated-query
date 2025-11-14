# End-to-End Pushdown Test Suite Plan

The goal is to cover every meaningful pushdown scenario with explicit SQL + expectation tests. The suite will be implemented incrementally in the buckets below. Each bucket enumerates the specific combinations to cover, along with notes on expected behavior (whether pushdown should happen, where it should not, and how many datasources are involved).


VERY IMPORTANT: in this exercise you can't use "string in string" checks. That is NOT allowed. Also regular expressions are forbidden. You get AST from data sources, use the ast to check exactly what is needed !!!

---
### Current Status

- ✅ **Section 0 – Foundation:** `tests/e2e_pushdown/test_foundation.py` (2 tests, 2 explain-json queries)
- ✅ **Section 1 – Single-table predicates:** `tests/e2e_pushdown/test_single_table_predicates.py` (12 tests)
- ✅ **Section 2 – Single-table aggregations:** `tests/e2e_pushdown/test_single_table_aggregations.py` (6 tests)
- ✅ **Section 3 – Two-table joins & join+agg/order:** `tests/e2e_pushdown/test_single_source_joins.py` (10 tests), `tests/e2e_pushdown/test_join_aggregations_and_order.py` (6 tests)
- ✅ **Section 4 – Three-table joins:** `tests/e2e_pushdown/test_multi_table_joins.py` (4 tests documenting remote + local join split)
- ✅ **Section 5 – Combined pushdowns:** `tests/e2e_pushdown/test_combined_pushdowns.py` (5 tests)
- ✅ **Section 6 – Multi-datasource guardrails:** `tests/e2e_pushdown/test_multi_datasource_guardrails.py` (5 tests covering cross-source joins, filters, aggregates)
- ✅ **Section 7 – Edge cases & negatives:** `tests/e2e_pushdown/test_edge_cases_negatives.py` (3 tests capturing unsupported features and fallbacks)

**Totals so far:** 53 automated E2E pushdown tests that validate ~58 datasource queries captured via `EXPLAIN (FORMAT JSON)`.

## 0. Foundation

* What we want is to see the real queries sent to datasources. To do that we need these following
- In datasources add a method named "parse_query", this will just get the query and return an AST by using sqlglot
- In the engine when we call "EXPLAIN query" it will parse/plan etc. and then will call datasources' parse_query method and in the end of the explain output it will show the queries sent to datasources.
- When we send query "EXPLAIN (FORMAT JSON) ..." the engine will return a json document with "queries": [{"datasource_name": ..., query: "... rewritten sql..."}]
- This "explain (format json) ..." when called from the code, should return a dictionary and queries should be the asts. when sending to client we can turn them into sql's
- We need this part first to start the rest of these tasks. So in tests we'll use this, first send "Explain (format json) ..." and get the queries then check if they have proper structure.
- We need at least 2 basic tests for pushdown predicate to see this path is working.  

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
# E2E Pushdown Test Suite - Comprehensive Gap Analysis

**Analysis Date:** 2025-11-14
**Current Tests:** 53 tests (all passing)
**Target Tests:** 285+ tests
**Gap:** 232 tests missing (81%)

---

## Executive Summary

**Quality:** ✅ EXCELLENT - Proper AST-based verification using `EXPLAIN (FORMAT JSON)`
- No string checking or regex (as required)
- Using sqlglot `exp.*` types for AST inspection
- Professional test structure with helper functions

**Coverage:** ⚠️ 19% COMPLETE - Major gaps in critical scenarios
- Missing 81% of needed test coverage
- Many basic SQL operators untested
- Edge cases and error handling minimal

**Priority Actions Required:**
1. 🔴 Add missing comparison operators (28 tests)
2. 🔴 Add missing aggregate functions (34 tests)
3. 🔴 Add NULL handling tests (18 tests)
4. 🔴 Add edge case tests (42 tests)

---

## Detailed Gap Analysis by Section

### Section 1: Single-Table Predicates (12/40 tests = 30%)

**Implemented (12):**
- SELECT *, specific columns ✅
- WHERE with =, >, <, AND, OR ✅
- IN, BETWEEN, LIKE ✅  
- LIMIT, OFFSET ✅

**MISSING (28 tests):**
Comparison Operators (11):
- != / <> operator
- <= operator
- >= operator
- NOT IN operator
- NOT BETWEEN operator
- NOT LIKE operator
- ILIKE (case-insensitive)
- IS NULL / IS NOT NULL
- Regex operators (~, !~)

Logical Operators (8):
- NOT operator
- 3-way AND/OR
- Nested (a AND b) OR (c AND d)
- Complex ((a OR b) AND c) AND d
- Precedence testing

Edge Cases (9):
- NULL in IN list
- Empty string predicate
- Unicode strings
- Escaped characters
- Very long strings
- Case sensitivity
- LIMIT 0
- String with newlines

---

### Section 2: Single-Table Aggregations (6/40 tests = 15%)

**Implemented (6):**
- COUNT(*) ✅
- Multiple aggregates with aliases ✅
- GROUP BY single/multiple ✅
- HAVING ✅
- MIN/MAX with LIMIT ✅

**MISSING (34 tests):**
Aggregate Functions (13):
- COUNT(col) vs COUNT(*)
- COUNT(DISTINCT col)
- SUM(col), SUM(DISTINCT col)
- AVG(col)
- STDDEV/VARIANCE
- STRING_AGG
- Aggregates on expressions: SUM(a * b)
- Conditional: SUM(CASE WHEN...)
- FILTER clause: COUNT(*) FILTER (WHERE...)

GROUP BY Scenarios (12):
- GROUP BY expressions
- GROUP BY with WHERE
- GROUP BY with NULL values
- HAVING with multiple conditions
- HAVING with different operators
- GROUP BY with LIMIT/OFFSET
- Empty groups
- Large group counts

Edge Cases (9):
- Aggregate on empty table
- All NULL values
- Single value
- Overflow in SUM
- Precision loss in AVG
- MIN/MAX on strings/dates

---

### Section 3 & 4: Joins (20/50 tests = 40%)

**Implemented (20):**
- INNER/LEFT/RIGHT JOIN ✅
- JOIN with filters/LIMIT ✅
- Cross join ✅
- Non-equi join fallback ✅
- JOIN + aggregation ✅
- JOIN + HAVING ✅
- JOIN + ORDER BY ✅
- 3-table joins (4 tests) ✅

**MISSING (30 tests):**
Join Types (7):
- FULL OUTER JOIN
- Self-join
- NATURAL JOIN
- JOIN with USING clause
- Multiple ON conditions
- Computed columns in join

3-Table Scenarios (13):
- All combinations of LEFT/RIGHT
- 3-table aggregation
- 3-table HAVING
- Star schema (fact + dimensions)
- Different join conditions
- 4-table join

Join Edge Cases (10):
- NULL in join keys
- DISTINCT after join
- Type casting in join
- Same table different aliases
- ORDER BY from both tables

---

### Section 5: Combined Pushdowns (5/30 tests = 17%)

**Implemented (5):**
- Projection + predicate + aggregation + join + limit ✅
- Computed expressions + GROUP BY ✅
- ORDER BY after aggregation ✅
- DISTINCT with JOIN ✅
- CASE expressions ✅

**MISSING (25 tests):**
Subqueries (8):
- Scalar subquery in SELECT
- Subquery in WHERE
- Subquery in HAVING
- Correlated subquery
- EXISTS / NOT EXISTS
- IN (subquery)
- ANY/ALL operators

Set Operations (4):
- UNION ALL
- UNION (DISTINCT)
- INTERSECT
- EXCEPT

Advanced (13):
- WITH (CTE)
- Multiple CTEs
- Recursive CTE
- LATERAL join
- Window functions (various)
- All features combined
- Complex WHERE + everything

---

### Section 6: Multi-Datasource (5/15 tests = 33%)

**Implemented (5):**
- Same-source join pushdown ✅
- Cross-source join fallback ✅
- 3-source join ✅
- Cross-source filters ✅
- Cross-source aggregates ✅

**MISSING (10 tests):**
- Same DB type, different instances
- Postgres + DuckDB join
- WHERE on both sources
- Cross-source UNION
- Cross-source subquery
- Aggregation result joins
- Same DB different schemas
- 4+ sources
- Computed columns across sources
- NULL handling across sources

---

### Section 7: Edge Cases (3/45 tests = 7%)

**Implemented (3):**
- Window functions (unsupported) ✅
- Scalar subqueries (unsupported) ✅
- OR in join condition ✅

**MISSING (42 tests):**

Data Types (15):
- Integer overflow
- Float precision
- DECIMAL precision
- Negative numbers
- Zero values
- Empty strings vs NULL
- Unicode strings
- Special characters (quotes, backslashes, newlines)
- Very long strings (>1KB)
- Date edge cases (epoch, far future)
- Timestamp timezone
- Boolean NULL
- JSON field access
- Array operations

Table Edge Cases (10):
- Empty table
- Single row table
- Very large table (1M+ rows)
- Table with all NULLs
- All duplicate values
- Very wide table (100+ columns)
- Reserved keyword columns
- Column names with spaces
- Quoted identifiers
- Case-sensitive columns

Error Handling (10):
- Syntax errors
- Missing table/column
- Type mismatches
- Division by zero
- Ambiguous column names
- Circular references
- Timeout errors
- Connection failures
- Permission errors

Unsupported Features (7):
- Stored procedures
- User-defined functions
- Full-text search
- Geometric operators
- XML operations
- Recursive CTEs
- LATERAL joins

---

## NEW Categories Needed (63 tests)

### Expression Pushdown (30 tests) - NEW

**Arithmetic:**
- WHERE price + tax > 100
- WHERE price * quantity
- WHERE (a + b) * (c - d)
- Modulo operator
- NULL in arithmetic

**String Functions:**
- UPPER/LOWER in WHERE
- SUBSTRING, LENGTH, TRIM
- CONCAT / || operator
- String expressions

**Date/Time:**
- EXTRACT(YEAR FROM date)
- DATE_TRUNC
- Date arithmetic
- CURRENT_DATE/TIMESTAMP
- AGE function

**Conditional:**
- CASE in SELECT/WHERE
- COALESCE, NULLIF
- Nested CASE

**Type Casts:**
- CAST(col AS type)
- Postgres :: syntax
- CAST in GROUP BY

**Math Functions:**
- ABS, ROUND, FLOOR, CEIL

### ORDER BY (15 tests) - NEW

- ORDER BY ASC/DESC
- Multiple columns
- Mixed directions (ASC, DESC)
- NULLS FIRST/LAST
- Expressions in ORDER BY
- ORDER BY with GROUP BY
- Positional ORDER BY (1, 2)
- ORDER BY + LIMIT (pagination)
- ORDER BY not in SELECT
- ORDER BY with DISTINCT

### NULL Handling (18 tests) - NEW

- IS NULL / IS NOT NULL
- NULL in AND/OR
- NULL in JOIN keys
- NULL in GROUP BY
- NULL in ORDER BY (FIRST/LAST)
- COUNT(*) vs COUNT(col) with NULLs
- SUM/AVG with NULLs
- MIN/MAX with NULLs
- COALESCE with NULLs
- IN with NULL values
- NOT IN with NULL
- Three-valued logic (AND/OR with NULL)

---

## Test Count by Priority

| Priority | Category | Tests | Status |
|----------|----------|-------|--------|
| 🔴 CRITICAL | Missing operators | 28 | 0% |
| 🔴 CRITICAL | Core aggregates | 34 | 0% |
| 🔴 CRITICAL | NULL handling | 18 | 0% |
| 🔴 CRITICAL | Error handling | 10 | 0% |
| 🟡 HIGH | Expressions | 30 | 0% |
| 🟡 HIGH | Advanced joins | 14 | 0% |
| 🟡 HIGH | ORDER BY | 15 | 0% |
| 🟡 HIGH | 3-table scenarios | 16 | 0% |
| 🟢 MEDIUM | Combined scenarios | 25 | 20% |
| 🟢 MEDIUM | Data type edges | 15 | 0% |
| 🟢 MEDIUM | Multi-datasource | 10 | 50% |
| 🔵 LOW | Table edge cases | 10 | 0% |
| 🔵 LOW | Unsupported features | 7 | 43% |

**Total Missing:** 232 tests across 13 categories

---

## Implementation Roadmap

### Sprint 1 (Week 1-2): Critical Foundation - 70 tests
**Goal:** Production-ready basic coverage

1. Comparison operators (11 tests)
   - !=, <, <=, >=, NOT IN, NOT BETWEEN, NOT LIKE
   - IS NULL, IS NOT NULL
   - Regex operators

2. Logical operators (13 tests)
   - NOT, 3-way AND/OR
   - Complex nesting
   - Precedence

3. Core aggregates (20 tests)
   - COUNT(col), COUNT(DISTINCT)
   - SUM, AVG, MIN, MAX
   - GROUP BY variations
   - HAVING scenarios

4. NULL handling basics (18 tests)
   - IS NULL/NOT NULL
   - NULL in operators
   - Three-valued logic
   - Aggregate NULL handling

5. Error handling (8 tests)
   - Syntax errors
   - Missing tables/columns
   - Type mismatches
   - Basic error recovery

### Sprint 2 (Week 3-4): Essential Coverage - 60 tests
**Goal:** Advanced query support

1. Expression pushdown (20 tests)
   - Arithmetic expressions
   - String functions
   - Date/time functions
   - CASE/COALESCE

2. ORDER BY (15 tests)
   - All ORDER BY scenarios
   - NULLS FIRST/LAST
   - ORDER BY + LIMIT

3. Advanced aggregates (14 tests)
   - Aggregate on expressions
   - FILTER clause
   - Edge cases

4. 3-table joins (11 tests)
   - All join type combinations
   - With aggregation/HAVING
   - Complex scenarios

### Sprint 3 (Week 5-6): Comprehensive - 60 tests
**Goal:** Full feature coverage

1. Combined pushdowns (20 tests)
   - Subqueries
   - Set operations
   - CTEs
   - All features combined

2. Advanced join scenarios (14 tests)
   - FULL OUTER, Self-join
   - NATURAL, USING
   - 4-table joins
   - Edge cases

3. Expression advanced (10 tests)
   - Type casts
   - Math functions
   - Nested expressions

4. Multi-datasource (10 tests)
   - Different DB types
   - Complex cross-source
   - More sources

5. Aggregate edge cases (6 tests)
   - Empty tables
   - Overflow
   - Precision

### Sprint 4 (Week 7): Edge Cases - 42 tests
**Goal:** Production hardening

1. Data type edges (15 tests)
   - All data type edge cases

2. Table edges (10 tests)
   - Empty, large, wide tables
   - Special names

3. Remaining errors (2 tests)
   - Timeout, connection

4. Unsupported features (4 tests)
   - Remaining unsupported

5. Predicate edges (9 tests)
   - Unicode, escapes, etc.

6. Remaining NULL (2 tests)
   - Complex NULL scenarios

---

## Success Criteria

### Phase 1 Complete (70 tests):
- ✅ All comparison operators tested
- ✅ All logical operators tested
- ✅ All core aggregate functions tested
- ✅ NULL handling fundamentals covered
- ✅ Basic error handling in place

### Phase 2 Complete (130 tests total):
- ✅ Expression pushdown working
- ✅ ORDER BY fully tested
- ✅ Advanced aggregation covered
- ✅ 3-table joins comprehensive

### Phase 3 Complete (190 tests total):
- ✅ Subqueries tested (where applicable)
- ✅ Set operations covered
- ✅ CTEs tested
- ✅ 4+ table joins covered
- ✅ Multi-datasource comprehensive

### Phase 4 Complete (232+ tests total):
- ✅ All data types edge cases covered
- ✅ Error handling comprehensive
- ✅ Unsupported features documented
- ✅ Production-ready test suite

---

## Conclusion

**Current State:**
- 53 high-quality tests with excellent AST-based verification
- Foundation is solid, test quality is professional
- Coverage is only 19% of target

**Required Work:**
- 232 additional tests needed
- 4 sprints (7 weeks) to complete
- Organized by priority: Critical → High → Medium → Low

**Immediate Next Steps:**
1. Add != / <> / <= / >= operators (4 tests)
2. Add IS NULL / IS NOT NULL (2 tests)
3. Add COUNT(col) / COUNT(DISTINCT) (2 tests)
4. Add AVG / MIN / MAX (3 tests)
5. Add NULL in aggregates (3 tests)

**Total:** Start with 14 tests in next 2 days to prove velocity.
