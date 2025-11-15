# End-to-End Pushdown Test Suite Plan

The goal is to cover every meaningful pushdown scenario with explicit SQL + expectation tests. The suite will be implemented incrementally in the buckets below. Each bucket enumerates the specific combinations to cover, along with notes on expected behavior (whether pushdown should happen, where it should not, and how many datasources are involved).

We need these tests to fail if the expectation is not working. Shouldn't fail silently or shouldn't be skipped or marked as fail. 

The point of this work is to see if the engine covers the areas. You can't fail the tests silently. Try/except: pass pattern is forbidden. Marking tests with skip, fail etc. is forbidden. If tests should fail for whatever reason, they will fail.  

VERY IMPORTANT: in this exercise you can't use "string in string" checks. That is NOT allowed. Also regular expressions are forbidden. You get AST from data sources, use the ast to check exactly what is needed !!!


---

## CRITICAL ISSUES FOUND

### 1. Incorrect Test Count
**ISSUE:** Document claimed 53 tests, but **89 tests actually exist**
- 36 tests were not accounted for in the status section

### 2. Missing File Documentation
**ISSUE:** `test_null_handling.py` with 13 tests is completely undocumented
- This is a critical category covering three-valued logic, NULL semantics

### 3. Incorrect Section Counts
**ISSUE:** Multiple sections have wrong test counts:
- Section 1 (Single-table predicates): Document says 12, actually **26 tests** (+14)
- Section 2 (Single-table aggregations): Document says 6, actually **15 tests** (+9)

### 4. Gap Analysis Inaccurate
**ISSUE:** Gap analysis lists many items as "MISSING" that are actually implemented:
- Comparison operators (!=, <, <=, >=) - **IMPLEMENTED**
- IS NULL / IS NOT NULL - **IMPLEMENTED**
- NOT IN, NOT BETWEEN, NOT LIKE - **IMPLEMENTED**
- NOT operator - **IMPLEMENTED**
- 3-way AND/OR, nested logic - **IMPLEMENTED**
- COUNT(col), COUNT(DISTINCT), SUM(DISTINCT) - **IMPLEMENTED**
- AVG aggregates - **IMPLEMENTED**
- Aggregates on expressions - **IMPLEMENTED**
- GROUP BY with WHERE, HAVING multiple - **IMPLEMENTED**
- All NULL handling basics - **IMPLEMENTED**

---

## Current Status (CORRECTED)

**Total Tests:** 89 (not 53 as previously stated)
**Target:** 285-295 tests
**Actual Gap:** 196-206 tests (69-72% missing)

### Implemented Test Files:

- ✅ **Section 0 – Foundation:** `tests/e2e_pushdown/test_foundation.py` (2 tests)
- ✅ **Section 1 – Single-table predicates:** `tests/e2e_pushdown/test_single_table_predicates.py` (**26 tests**, not 12)
  - Covers: SELECT *, projections, all comparison operators (=, !=, <, >, <=, >=)
  - Covers: AND, OR, NOT, IN, NOT IN, BETWEEN, NOT BETWEEN, LIKE, NOT LIKE
  - Covers: IS NULL, IS NOT NULL
  - Covers: 3-way AND/OR, nested (a AND b) OR (c AND d), complex logic
  - Covers: LIMIT, OFFSET

- ✅ **Section 2 – Single-table aggregations:** `tests/e2e_pushdown/test_single_table_aggregations.py` (**15 tests**, not 6)
  - Covers: COUNT(*), COUNT(col), COUNT(DISTINCT), SUM, SUM(DISTINCT), AVG, MIN, MAX
  - Covers: GROUP BY single/multiple columns
  - Covers: HAVING, HAVING with multiple conditions
  - Covers: GROUP BY with WHERE, GROUP BY with LIMIT/OFFSET
  - Covers: Aggregates on expressions (SUM(a * b))
  - Covers: MIN/MAX on strings

- ✅ **Section 3 – NULL Handling:** `tests/e2e_pushdown/test_null_handling.py` (**13 tests** - MISSING FROM ORIGINAL DOC!)
  - Covers: IS NULL / IS NOT NULL
  - Covers: NULL in AND/OR logic (three-valued logic)
  - Covers: NULL in JOIN keys
  - Covers: NULL in GROUP BY
  - Covers: COUNT(*) vs COUNT(col) with NULLs
  - Covers: SUM, AVG, MIN, MAX with NULLs
  - Covers: IN and NOT IN with NULL semantics
  - Covers: NULL equality comparison
  - Covers: Complex boolean logic with NULL

- ✅ **Section 4 – Two-table joins:** `tests/e2e_pushdown/test_single_source_joins.py` (10 tests)
  - Covers: INNER, LEFT, RIGHT, CROSS joins
  - Covers: JOIN with filters, LIMIT
  - Covers: Comma syntax
  - Covers: Non-equi join fallback

- ✅ **Section 5 – Join + aggregation/order:** `tests/e2e_pushdown/test_join_aggregations_and_order.py` (6 tests)
  - Covers: JOIN + GROUP BY, HAVING
  - Covers: JOIN + ORDER BY, LIMIT
  - Covers: Multiple group columns

- ✅ **Section 6 – Three-table joins:** `tests/e2e_pushdown/test_multi_table_joins.py` (4 tests)
  - Covers: 3-table inner chains
  - Covers: Mixed INNER/LEFT
  - Covers: 3-table with predicates, LIMIT

- ✅ **Section 7 – Combined pushdowns:** `tests/e2e_pushdown/test_combined_pushdowns.py` (5 tests)
  - Covers: Projection + predicate + aggregation + join + limit
  - Covers: Computed expressions + GROUP BY
  - Covers: ORDER BY after aggregation
  - Covers: DISTINCT with JOIN
  - Covers: CASE expressions in aggregates

- ✅ **Section 8 – Multi-datasource guardrails:** `tests/e2e_pushdown/test_multi_datasource_guardrails.py` (5 tests)
  - Covers: Same-source join pushdown
  - Covers: Cross-source join fallback
  - Covers: 3-source joins
  - Covers: Cross-source filters, aggregates

- ✅ **Section 9 – Edge cases & negatives:** `tests/e2e_pushdown/test_edge_cases_negatives.py` (3 tests)
  - Covers: Window functions (unsupported, errors)
  - Covers: Scalar subqueries (unsupported, errors)
  - Covers: OR in join condition (fallback)

---

## Comprehensive Gap Analysis - CORRECTED

**Current:** 89 tests with excellent AST-based verification
**Target:** 285-295 tests
**Missing:** ~200 tests

### Categories That Need Work

#### 1. Expression Pushdown (0/40 tests = 0%) 🔴 CRITICAL

**Missing:**

**Arithmetic Expressions (10 tests):**
- WHERE price * quantity > 100
- WHERE (price + tax) * 1.1
- WHERE quantity % 2 = 0 (modulo)
- WHERE price / quantity (division)
- WHERE -price (unary minus)
- Arithmetic with NULL
- Arithmetic overflow handling
- Arithmetic with CASE
- Nested arithmetic ((a + b) * (c - d))
- ORDER BY price * quantity

**String Functions (10 tests):**
- WHERE UPPER(name) = 'PRODUCT'
- WHERE LOWER(region) LIKE '%eu%'
- WHERE LENGTH(name) > 10
- WHERE SUBSTRING(name, 1, 3) = 'PRO'
- WHERE TRIM(region) = 'EU'
- WHERE name || ' suffix' (concatenation)
- WHERE CONCAT(first, ' ', last)
- SELECT UPPER(status), LOWER(region)
- String functions in GROUP BY
- String functions in ORDER BY

**Date/Time Functions (10 tests):**
- WHERE EXTRACT(YEAR FROM created_at) = 2024
- WHERE DATE_TRUNC('month', created_at)
- WHERE created_at + INTERVAL '1 day'
- WHERE created_at BETWEEN date1 AND date2
- WHERE AGE(created_at) > INTERVAL '30 days'
- SELECT CURRENT_DATE, CURRENT_TIMESTAMP
- Date functions in GROUP BY (by month, year)
- Date functions in ORDER BY
- Timestamp comparison
- Date arithmetic edge cases

**Conditional Expressions (5 tests):**
- CASE in WHERE clause
- Nested CASE expressions
- CASE in GROUP BY
- COALESCE in WHERE
- NULLIF usage

**Type Casts (5 tests):**
- CAST(column AS type) in WHERE
- PostgreSQL :: syntax
- CAST in JOIN conditions
- CAST in GROUP BY
- CAST in ORDER BY

#### 2. ORDER BY Comprehensive (0/20 tests = 0%) 🔴 CRITICAL

**Missing:**
- ORDER BY col ASC (explicit)
- ORDER BY col DESC
- ORDER BY col1 ASC, col2 DESC (mixed)
- ORDER BY col NULLS FIRST
- ORDER BY col NULLS LAST
- ORDER BY expression (price * quantity)
- ORDER BY 1, 2 (positional)
- ORDER BY column not in SELECT
- ORDER BY with DISTINCT
- ORDER BY with aggregate functions
- ORDER BY with string functions
- ORDER BY with CASE
- Complex pagination (LIMIT X OFFSET Y with ORDER)
- ORDER BY on JOIN result
- ORDER BY after UNION
- ORDER BY with NULL values (behavior)
- Multiple ORDER BY columns (3+)
- ORDER BY with WHERE
- ORDER BY with GROUP BY
- ORDER BY with HAVING

#### 3. Advanced Join Types (0/15 tests = 0%) 🔴 CRITICAL

**Missing:**
- FULL OUTER JOIN basic
- FULL OUTER JOIN with WHERE
- Self-join (table joined to itself)
- Self-join with different predicates
- NATURAL JOIN
- JOIN with USING clause (single column)
- JOIN with USING clause (multiple columns)
- JOIN with multiple ON conditions (AND)
- JOIN on computed columns (ON t1.a * 2 = t2.b)
- JOIN on expressions (ON UPPER(t1.name) = t2.code)
- Three-way join with FULL OUTER
- JOIN with COALESCE in condition
- JOIN with CASE in condition
- Chained LEFT/RIGHT combinations (5+ variations)
- Mixed INNER/FULL/LEFT/RIGHT in single query

#### 4. Four+ Table Joins (0/10 tests = 0%) 🔴 CRITICAL

**Missing:**
- 4-table INNER join chain
- 5-table INNER join chain
- 4-table with mixed join types
- Star schema (1 fact + 3 dimensions)
- 4-table with WHERE on all tables
- 4-table with aggregation
- 4-table with HAVING
- 4-table with ORDER BY LIMIT
- 6+ table join
- Complex multi-table with subquery filters

#### 5. Subqueries (0/20 tests = 0%) 🟡 HIGH PRIORITY

**Missing:**
- WHERE EXISTS (SELECT ...)
- WHERE NOT EXISTS (SELECT ...)
- WHERE col IN (SELECT ...)
- WHERE col NOT IN (SELECT ...)
- WHERE col = (SELECT MAX(...))
- WHERE col > (SELECT AVG(...))
- HAVING aggregate > (SELECT ...)
- Correlated subquery (inner references outer)
- Correlated subquery in SELECT
- WHERE col = ANY (SELECT ...)
- WHERE col = ALL (SELECT ...)
- Nested subqueries (subquery in subquery)
- Subquery in FROM (derived table)
- Subquery with JOIN
- Multiple subqueries in same query
- Subquery with aggregation
- Subquery with ORDER BY LIMIT
- Subquery fallback behavior (document limitations)
- EXISTS with complex predicates
- Subquery with UNION

#### 6. Set Operations (0/10 tests = 0%) 🟡 HIGH PRIORITY

**Missing:**
- UNION ALL basic
- UNION (DISTINCT) basic
- INTERSECT basic
- EXCEPT basic
- UNION of 3+ queries
- UNION with ORDER BY
- UNION with LIMIT
- Nested set operations ((A UNION B) INTERSECT C)
- Set operations with aggregation
- Cross-datasource UNION fallback

#### 7. CTEs (WITH clause) (0/10 tests = 0%) 🟡 HIGH PRIORITY

**Missing:**
- Simple CTE, referenced once
- CTE referenced multiple times
- Multiple CTEs (WITH cte1 AS (...), cte2 AS (...))
- CTE with JOIN
- CTE with aggregation
- CTE with WHERE
- Nested CTE (CTE referencing another CTE)
- CTE with UNION
- CTE with ORDER BY LIMIT
- Recursive CTE (if supported/fallback behavior)

#### 8. DISTINCT Variations (0/8 tests = 0%) 🟢 MEDIUM

**Missing:**
- DISTINCT single column
- DISTINCT multiple columns
- DISTINCT with WHERE
- DISTINCT with JOIN (beyond basic test)
- DISTINCT with aggregation in same query
- DISTINCT with ORDER BY not in SELECT
- DISTINCT with LIMIT/OFFSET
- DISTINCT *

#### 9. LIMIT/OFFSET Edge Cases (2/10 tests = 20%) 🟢 MEDIUM

**Implemented:**
- Basic LIMIT
- LIMIT with OFFSET

**Missing:**
- LIMIT 0
- Very large LIMIT (1000000)
- Very large OFFSET
- OFFSET without LIMIT
- OFFSET larger than table size
- LIMIT 1 optimization
- Pagination pattern (multiple LIMIT/OFFSET combinations)
- LIMIT with ORDER BY (multiple scenarios)

#### 10. Data Type Edge Cases (0/15 tests = 0%) 🟢 MEDIUM

**Missing:**
- Integer overflow (very large numbers)
- Integer underflow (very negative numbers)
- Float precision (0.1 + 0.2)
- Float special values (Infinity, -Infinity, NaN)
- DECIMAL precision testing
- Zero values in division
- Negative number operations
- Very small decimals (0.000001)
- Date at epoch (1970-01-01)
- Far future date (9999-12-31)
- BC dates (if supported)
- Boolean NULL handling
- Timestamp with timezone conversions
- Mixed numeric types (INT + FLOAT)
- Scientific notation

#### 11. String Edge Cases (0/12 tests = 0%) 🟢 MEDIUM

**Missing:**
- Empty string vs NULL distinction
- Unicode strings (UTF-8: emojis, Chinese, Arabic)
- Strings with single quotes (O'Brien)
- Strings with double quotes
- Strings with backslashes
- Strings with newlines
- Strings with tabs
- Very long strings (1KB+, 10KB+)
- SQL injection patterns (properly escaped)
- Case sensitivity in comparisons
- LIKE with % and _ wildcards (various positions)
- ILIKE (case-insensitive LIKE)

#### 12. Table/Query Edge Cases (0/10 tests = 0%) 🟢 MEDIUM

**Missing:**
- Query on empty table (0 rows)
- Query on single-row table
- Table with all NULLs in a column
- Table with all duplicate values
- Column names that are SQL keywords (select, where, from)
- Column names with spaces (requires quoting)
- Column names with special characters
- Quoted identifiers (case-sensitive)
- Very wide SELECT (50+ columns)
- SELECT with duplicate column names (aliasing)

#### 13. Error Handling & Validation (2/10 tests = 20%) 🔴 CRITICAL

**Implemented:**
- Window function error handling
- Scalar subquery error handling

**Missing:**
- Syntax error (malformed SQL)
- Missing table reference
- Missing column reference
- Ambiguous column name (needs table prefix)
- Type mismatch (string vs int comparison)
- Division by zero behavior
- Invalid date format
- Connection failure simulation

#### 14. Advanced NULL Scenarios (13/18 tests = 72%) 🟢 MEDIUM

**Implemented:** (via test_null_handling.py)
- IS NULL / IS NOT NULL
- NULL in AND/OR
- NULL in JOIN
- NULL in GROUP BY
- COUNT/SUM/AVG/MIN/MAX with NULL
- IN/NOT IN with NULL

**Missing:**
- COALESCE(col1, col2, col3) with multiple NULLs
- NULLIF usage
- NULL in arithmetic (5 + NULL)
- NULL in string concatenation
- NULL in CASE expressions
- Complex NULL propagation

#### 15. Multi-Datasource Advanced (5/15 tests = 33%) 🟡 HIGH PRIORITY

**Implemented:**
- Same-source join pushdown
- Cross-source join fallback
- 3-source joins
- Cross-source filters/aggregates

**Missing:**
- PostgreSQL + DuckDB cross-DB join
- Same DB type, different instances
- Cross-source UNION
- Cross-source subqueries
- Aggregation from both sources then join
- Same DB, different schemas
- 4+ datasource query
- Datasource with different dialects
- Cross-source with computed columns
- Cross-source with complex WHERE

#### 16. Aggregate Advanced Features (0/8 tests = 0%) 🟡 HIGH PRIORITY

**Missing:**
- FILTER clause (COUNT(*) FILTER (WHERE ...))
- STRING_AGG (if supported)
- ARRAY_AGG (if supported)
- Aggregate with DISTINCT and expression
- Conditional aggregation (SUM(CASE...))
- Multiple aggregates with different FILTER
- STDDEV, VARIANCE (if supported)
- Aggregate on aggregate (nested, via subquery)

#### 17. Predicate Edge Cases (0/8 tests = 0%) 🟢 MEDIUM

**Missing:**
- Empty IN list: WHERE col IN ()
- Single-item IN list: WHERE col IN (value)
- Large IN list (100+ items)
- BETWEEN with equal bounds (BETWEEN 5 AND 5)
- BETWEEN with inverted bounds (BETWEEN 10 AND 5)
- LIKE with only wildcards (LIKE '%%')
- LIKE with escaped wildcards (LIKE 'test\_%')
- Regex operators ~ and !~ (if supported)

---

## Revised Implementation Roadmap

### Sprint 1: Critical Expression & Sort Support (60 tests)
**Target:** Achieve basic expression and ordering coverage

1. **Arithmetic Expressions** (10 tests)
   - All basic operators in WHERE
   - Expressions in SELECT and ORDER BY
   - NULL handling

2. **ORDER BY Comprehensive** (20 tests)
   - All ORDER BY scenarios
   - ASC/DESC, NULLS FIRST/LAST
   - Expressions, positional
   - Interaction with other clauses

3. **String Functions** (10 tests)
   - UPPER, LOWER, LENGTH, SUBSTRING, TRIM
   - Concatenation
   - In WHERE, SELECT, GROUP BY, ORDER BY

4. **Date Functions** (10 tests)
   - EXTRACT, DATE_TRUNC
   - Date arithmetic
   - In WHERE, GROUP BY, ORDER BY

5. **Type Casts** (5 tests)
   - CAST syntax
   - In various clauses

6. **Conditional Expressions** (5 tests)
   - CASE, COALESCE, NULLIF
   - In WHERE, SELECT, GROUP BY

### Sprint 2: Advanced Joins & Set Operations (55 tests)

1. **Advanced Join Types** (15 tests)
   - FULL OUTER, self-join, NATURAL, USING
   - Joins on expressions
   - Complex join conditions

2. **4+ Table Joins** (10 tests)
   - 4, 5, 6 table joins
   - Star schemas
   - With aggregation

3. **Set Operations** (10 tests)
   - UNION, INTERSECT, EXCEPT
   - With ORDER BY, LIMIT
   - Nested set operations

4. **CTEs** (10 tests)
   - All CTE scenarios
   - Multiple, nested CTEs

5. **Subqueries** (10 tests)
   - WHERE IN, EXISTS, scalar
   - Basic scenarios (complex ones in Sprint 3)

### Sprint 3: Subqueries & Edge Cases (55 tests)

1. **Advanced Subqueries** (10 tests)
   - Correlated subqueries
   - ANY/ALL operators
   - Nested subqueries
   - Complex scenarios

2. **String Edge Cases** (12 tests)
   - Unicode, special characters
   - Very long strings
   - Case sensitivity

3. **Data Type Edge Cases** (15 tests)
   - All numeric edge cases
   - Date edge cases
   - Special values

4. **Table/Query Edge Cases** (10 tests)
   - Empty tables, wide tables
   - Reserved keywords
   - Quoted identifiers

5. **Error Handling** (8 tests)
   - All error scenarios
   - Graceful degradation

### Sprint 4: Polish & Multi-Datasource (36 tests)

1. **Multi-Datasource Advanced** (10 tests)
   - PostgreSQL + DuckDB
   - Complex cross-source scenarios
   - 4+ sources

2. **DISTINCT Variations** (6 tests)
   - All DISTINCT scenarios

3. **LIMIT/OFFSET Edge Cases** (8 tests)
   - All pagination edge cases

4. **Aggregate Advanced** (8 tests)
   - FILTER clause
   - Conditional aggregates
   - Advanced functions

5. **Predicate Edge Cases** (8 tests)
   - All predicate boundary conditions

6. **Advanced NULL** (6 tests)
   - Remaining NULL scenarios

---

## Success Metrics

### After Sprint 1 (149 tests total):
- ✅ All basic expressions pushdown correctly
- ✅ ORDER BY fully functional
- ✅ String/date functions working
- ✅ Type casting supported

### After Sprint 2 (204 tests total):
- ✅ All join types covered
- ✅ Large multi-table queries work
- ✅ Set operations functional
- ✅ CTEs working

### After Sprint 3 (259 tests total):
- ✅ Subqueries comprehensive
- ✅ All edge cases covered
- ✅ Error handling robust
- ✅ Data types hardened

### After Sprint 4 (295 tests total):
- ✅ Production-ready test suite
- ✅ Multi-datasource comprehensive
- ✅ All SQL features covered or documented as unsupported
- ✅ 295 tests = GOAL ACHIEVED!

---

## Test Quality Standards

All tests MUST:
1. ✅ Use AST-based verification (no string matching!)
2. ✅ Use sqlglot `exp.*` types for inspection
3. ✅ Have descriptive docstrings
4. ✅ Test one specific behavior
5. ✅ Verify pushdown happened (or didn't) as expected
6. ✅ Check query structure, not just execution success

---

## Summary

**Current State:**
- ✅ 89 high-quality tests (not 53!)
- ✅ Excellent AST-based verification methodology
- ✅ Strong foundation in predicates, aggregates, joins, NULL handling
- ⚠️ Documentation was outdated and inaccurate

**Required Work:**
- 🔴 206 additional tests needed to reach 295
- 🔴 4 sprints recommended (7-8 weeks)
- 🔴 Focus on: expressions, ORDER BY, advanced joins, subqueries

**Critical Priorities (Sprint 1):**
1. Expression pushdown (arithmetic, string, date)
2. ORDER BY comprehensive coverage
3. Type casts and conditional expressions

**High Priorities (Sprint 2):**
4. Advanced join types
5. 4+ table joins
6. Set operations and CTEs

---

**Document Updated:** 2025-11-14
**Reviewed By:** Claude (Comprehensive Review)
**Status:** ✅ Accurate - Ready for Sprint 1 Planning
