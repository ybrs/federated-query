# Decorrelation E2E Test Suite

This directory contains end-to-end tests for the subquery decorrelation engine.

## Overview

The decorrelation engine rewrites correlated and uncorrelated subqueries into join-based logical plans while preserving SQL semantics. These tests verify correct behavior across all supported patterns.

## Test Structure

Tests are organized by subquery pattern:

### test_exists.py
Tests for EXISTS and NOT EXISTS patterns:
- Uncorrelated EXISTS (evaluated once)
- Correlated EXISTS (SEMI joins)
- Uncorrelated NOT EXISTS
- Correlated NOT EXISTS (ANTI joins)
- EXISTS in SELECT list
- Multiple EXISTS in same query
- EXISTS with aggregation in subquery

**Key assertions:**
- EXISTS becomes SEMI join with proper correlation predicates
- NOT EXISTS becomes ANTI join
- Empty subqueries handled correctly
- Correlation keys properly extracted

### test_in.py
Tests for IN and NOT IN patterns:
- Uncorrelated IN (hoisted as CTE)
- Correlated IN (SEMI join with null-safe equality)
- Uncorrelated NOT IN
- Correlated NOT IN (complex NULL handling)
- Multi-column IN (tuple comparison)
- IN in SELECT list as boolean

**Key assertions:**
- IN uses null-safe equality (IS NOT DISTINCT FROM)
- NOT IN includes NULL guard when subquery can produce NULL
- Proper three-valued logic for NOT IN with NULLs
- Subqueries hoisted as CTEs where appropriate

### test_quantified_comparisons.py
Tests for ANY/SOME/ALL quantified comparisons:
- = ANY (equivalent to IN)
- >, <, >=, <= with ANY
- <> ANY patterns
- SOME (synonym for ANY)
- = ALL, <> ALL, >, < with ALL
- NULL handling in quantified comparisons
- Empty subquery handling (ALL=TRUE, ANY=FALSE)

**Key assertions:**
- ANY becomes SEMI join with comparison predicate
- ALL becomes ANTI join searching for violations
- ALL includes NULL guard for proper three-valued logic
- Null-safe comparisons used where required

### test_scalar_subqueries.py
Tests for scalar subqueries:
- Uncorrelated scalar in SELECT (hoisted as CTE, CROSS join)
- Correlated scalar in SELECT (LEFT join with aggregation)
- Scalar in WHERE clause
- Scalar in HAVING clause
- Multiple correlation keys
- Cardinality enforcement (single row requirement)
- COUNT vs other aggregates (NULL handling)

**Key assertions:**
- Scalar subqueries become LEFT join (preserve NULL)
- Aggregation ensures single row per correlation key
- Multiple rows without aggregation raises error
- NULL returned for empty result sets

### test_nested_subqueries.py
Tests for nested and complex subquery patterns:
- EXISTS containing IN
- Nested EXISTS (multiple levels)
- Scalar containing scalar
- Scalar with EXISTS inside
- Derived tables with subqueries
- Subqueries in JOIN conditions
- Multiple independent subqueries at same level
- Recursive decorrelation (fixed-point iteration)

**Key assertions:**
- Innermost subqueries decorrelated first
- Multiple passes until no subqueries remain
- Correlation chains properly handled
- Derived table scopes correctly resolved

### test_null_semantics.py
Tests for NULL handling across all patterns:
- NULL on LHS of IN
- NULL in subquery for IN/NOT IN
- NOT IN with NULL (complex three-valued logic)
- ANY/ALL with NULLs
- Scalar subqueries returning NULL
- NULL in correlation predicates
- NULL propagation through expressions
- Null-safe vs regular comparisons

**Key assertions:**
- IS NOT DISTINCT FROM used for null-safe equality
- NOT IN with NULL in subquery filters all rows (per SQL standard)
- ALL with NULL produces UNKNOWN when no violation
- Scalar subqueries preserve NULL semantics via LEFT join

### test_error_cases.py
Tests for error handling and edge cases:
- Cardinality violations (scalar returning multiple rows)
- Ambiguous column references
- Unsupported patterns (window functions, recursive CTEs)
- Unsupported operators
- Empty subqueries
- Deep nesting stress tests
- CTE reuse for identical uncorrelated subqueries
- Regression tests (non-subquery queries unchanged)
- Post-decorrelation validation

**Key assertions:**
- Clear errors for unsupported patterns
- Cardinality violations detected
- Non-subquery queries pass through unchanged
- Validation ensures no subquery nodes remain

## Test Data

The `conftest.py` fixture creates the following test tables:

- **users** (id, name, country, city): 5 users across different countries
- **orders** (id, user_id, amount, status): 6 orders with varying amounts
- **cities** (id, name, country, population): 6 cities
- **products** (id, name, price, category): 4 products
- **sales** (id, product_id, seller, price, region): 4 sales records
- **offers** (id, product_id, seller, amount): 4 offers
- **limits** (id, region, cap): 2 regional limits
- **countries** (code, name, enabled): 4 countries (3 enabled)

For NULL testing, additional data with NULL values is inserted by `setup_null_test_data`.

## Running Tests

### Run all decorrelation tests:
```bash
pytest tests/e2e_decorrelation/ -v
```

### Run specific test file:
```bash
pytest tests/e2e_decorrelation/test_exists.py -v
```

### Run specific test:
```bash
pytest tests/e2e_decorrelation/test_exists.py::TestCorrelatedExists::test_correlated_exists_basic -v
```

### Run with coverage:
```bash
pytest tests/e2e_decorrelation/ --cov=federated_query.optimizer.decorrelation --cov-report=term-missing
```

## Test Pattern

Each test follows this structure:

```python
def test_pattern_description(self, catalog, setup_test_data):
    """
    Test: Brief description

    Input SQL:
        <SQL query being tested>

    Expected plan structure:
        - Description of decorrelated plan
        - Join types expected
        - Correlation predicates

    Expected result:
        Description of expected query results
    """
    sql = """<SQL query>"""

    parser = Parser()
    binder = Binder(catalog)
    decorrelator = Decorrelator()

    logical_plan = parser.parse(sql)
    bound_plan = binder.bind(logical_plan)
    decorrelated_plan = decorrelator.decorrelate(bound_plan)

    # TODO: Add executor integration and assertions
    # assert <expected behavior>
```

## Implementation Status

**Current Status:** Tests written ahead of implementation

These tests are written before the decorrelation implementation to serve as:
1. **Specification:** Clear examples of expected behavior
2. **Test-Driven Development:** Tests guide implementation
3. **Regression Prevention:** Ensure semantics preserved as implementation evolves

**Expected Behavior:**
- Most tests will fail initially (decorrelator not implemented)
- Tests should pass incrementally as decorrelation features are implemented
- TODO comments mark where executor integration is needed

## Key Decorrelation Invariants Tested

1. **No subquery nodes remain** after decorrelation
2. **Null-safe equality** used for IN/ANY comparisons
3. **NULL guards** present for NOT IN/ALL with nullable subqueries
4. **LEFT joins** used for scalar subqueries (preserve NULL)
5. **SEMI/ANTI joins** used for EXISTS/NOT EXISTS/IN/NOT IN
6. **Correlation predicates** become join conditions
7. **Single-row enforcement** for scalar subqueries
8. **CTE hoisting** for uncorrelated subqueries
9. **Deterministic** plan structure for testing
10. **Three-valued logic** preserved per SQL standard

## Coverage Matrix

| Pattern | Uncorrelated | Correlated | With NULL | In SELECT | In WHERE | Nested |
|---------|-------------|------------|-----------|-----------|----------|--------|
| EXISTS | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| NOT EXISTS | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| IN | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| NOT IN | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| = ANY | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| > ANY | ✓ | ✓ | ✓ | - | ✓ | ✓ |
| < ANY | ✓ | ✓ | ✓ | - | ✓ | - |
| = ALL | ✓ | ✓ | ✓ | - | ✓ | - |
| <> ALL | ✓ | ✓ | ✓ | - | ✓ | - |
| Scalar | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

## References

See `decorrelation-plan.md` in the repository root for:
- Detailed rewrite rules
- Architecture overview
- NULL semantics specification
- Implementation steps
- Future work (window functions, recursive CTEs)
