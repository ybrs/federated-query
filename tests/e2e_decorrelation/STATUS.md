# Decorrelation E2E Tests - Implementation Status

## Completed Files

### ✅ test_utils.py
Utility functions for verifying decorrelated plans:
- `assert_plan_structure()` - Verifies SEMI/ANTI/LEFT joins present
- `assert_result_count()` - Verifies number of rows returned
- `assert_result_contains_ids()` - Verifies exact IDs in results
- `execute_and_fetch_all()` - Executes plans and fetches all results
- `find_nodes_of_type()` - Traverses plan tree
- `has_join_type()` - Checks for specific join types
- `verify_no_subquery_expressions()` - Ensures decorrelation complete

### ✅ test_exists.py (467 lines)
**15 tests with REAL assertions:**
- `test_uncorrelated_exists_basic` - Asserts 5 rows returned
- `test_uncorrelated_exists_empty_result` - Asserts 0 rows returned
- `test_correlated_exists_basic` - Asserts SEMI join + IDs {1,3,5}
- `test_correlated_exists_multiple_correlation_keys` - Asserts SEMI join
- `test_uncorrelated_not_exists_basic` - Asserts 5 rows
- `test_uncorrelated_not_exists_filters_all` - Asserts 0 rows
- `test_correlated_not_exists_basic` - Asserts ANTI join + IDs {1,2,4,5}
- `test_correlated_not_exists_all_pass` - Asserts ANTI join + 5 rows
- `test_exists_in_select_list` - Asserts 5 rows + has_orders column
- `test_multiple_exists_same_query` - Asserts SEMI join + results > 0
- `test_exists_with_aggregation_in_subquery` - Asserts SEMI join + aggregation + IDs {1,3,5}

**Pattern established:**
```python
# Parse, bind, decorrelate
parser = Parser()
binder = Binder(catalog)
decorrelator = Decorrelator()
executor = Executor(catalog)

logical_plan = parser.parse(sql)
bound_plan = binder.bind(logical_plan)
decorrelated_plan = decorrelator.decorrelate(bound_plan)

# Verify plan structure
assert_plan_structure(decorrelated_plan, {
    'has_semi_join': True,
    'semi_join_count': 1
})

# Execute and verify results
assert_result_contains_ids(executor, decorrelated_plan, {1, 3, 5})
```

## Files Requiring Real Assertions

### ⚠️ test_in.py (400+ lines)
14 tests - ALL need assertions replacing TODO comments:
- Need to add SEMI/ANTI join assertions
- Need to verify null-safe equality usage
- Need to add result count/ID assertions
- Need to verify NULL guard for NOT IN

### ⚠️ test_quantified_comparisons.py (500+ lines)
16 tests - ALL need assertions:
- Need to verify SEMI join for ANY
- Need to verify ANTI join for ALL
- Need to verify NULL guard for ALL
- Need result assertions for all tests

### ⚠️ test_scalar_subqueries.py (600+ lines)
17 tests - ALL need assertions:
- Need to verify LEFT join usage
- Need to verify aggregation present
- Need to verify cardinality enforcement
- Need result assertions with NULL handling

### ⚠️ test_nested_subqueries.py (500+ lines)
13 tests - ALL need assertions:
- Need to verify nested join structures
- Need to verify multi-level decorrelation
- Need result assertions for complex queries

### ⚠️ test_null_semantics.py (600+ lines)
18 tests - ALL need assertions:
- Need to verify null-safe comparisons
- Need to verify NULL guards
- Need to verify three-valued logic
- Need result assertions with NULL values

### ⚠️ test_error_cases.py (700+ lines)
23 tests - Need assertions OR pytest.raises:
- Tests that should pass: add plan/result assertions
- Tests that should fail: add `with pytest.raises(DecorrelationError)`
- Regression tests: verify plan unchanged

## Total Stats

- **Completed:** 1/7 files (test_exists.py with 15 tests)
- **Remaining:** 6/7 files (101 tests)
- **Lines with TODOs:** ~3,800 lines need real assertions

## Next Steps

1. Apply same pattern to test_in.py
2. Apply same pattern to test_quantified_comparisons.py
3. Apply same pattern to test_scalar_subqueries.py
4. Apply same pattern to test_nested_subqueries.py
5. Apply same pattern to test_null_semantics.py
6. Apply pytest.raises pattern to test_error_cases.py

All tests must:
✅ Import Executor and test_utils
✅ Create executor instance
✅ Call assert_plan_structure() to verify joins
✅ Call assert_result_count() or assert_result_contains_ids()
✅ Remove ALL TODO comments
✅ Have executable assertions that will fail until decorrelator is implemented
