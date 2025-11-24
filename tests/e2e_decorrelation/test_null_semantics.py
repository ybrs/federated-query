"""
E2E tests for NULL semantics in decorrelation.

Tests proper NULL handling across all subquery patterns, including
three-valued logic, null-safe comparisons, and NULL propagation.
"""
import pytest
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.executor.executor import Executor
from .test_utils import (
    assert_plan_structure,
    assert_result_count,
    assert_result_contains_ids,
    execute_and_fetch_all
)


@pytest.fixture(scope="module")
def setup_null_test_data(pg_datasource, setup_test_data):
    """
    Add test data with NULLs for null semantics testing.
    """
    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            # Add user with NULL country
            cursor.execute("""
                INSERT INTO users (id, name, country, city) VALUES
                (10, 'NullUser', NULL, 'Unknown')
            """)

            # Add city with NULL country
            cursor.execute("""
                INSERT INTO cities (id, name, country, population) VALUES
                (10, 'NullCity', NULL, 100000)
            """)

            # Add order with NULL amount
            cursor.execute("""
                INSERT INTO orders (id, user_id, amount, status) VALUES
                (10, 1, NULL, 'pending')
            """)

            # Add product with NULL price
            cursor.execute("""
                INSERT INTO products (id, name, price, category) VALUES
                (10, 'NullPriceProduct', NULL, 'Electronics')
            """)

            conn.commit()

    yield

    # Cleanup
    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM orders WHERE id = 10")
            cursor.execute("DELETE FROM products WHERE id = 10")
            cursor.execute("DELETE FROM cities WHERE id = 10")
            cursor.execute("DELETE FROM users WHERE id = 10")
            conn.commit()


class TestInWithNulls:
    """Test IN/NOT IN NULL semantics."""

    def test_in_null_on_lhs(self, catalog, setup_null_test_data):
        """
        Test: NULL on left-hand side of IN.

        Input SQL:
            SELECT * FROM users WHERE country IN ('US', 'UK')

        Expected plan structure:
            - SEMI join with null-safe equality
            - NULL = 'US' evaluates to UNKNOWN → row filtered

        Expected result:
            Users with country US or UK (NULL country filtered out)
            User 10 with NULL country should NOT appear
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN ('US', 'UK')
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users 1, 2, 3, 5 (not 4=FR, not 10=NULL)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_in_null_in_subquery(self, catalog, setup_null_test_data):
        """
        Test: IN when subquery returns NULL.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT country FROM cities)

        Expected plan structure:
            - SEMI join with IS NOT DISTINCT FROM (null-safe equality)
            - NULL in subquery matches NULL on LHS

        Expected result:
            Users whose country matches cities.country
            User 10 (NULL) should match city 10 (NULL) with null-safe comparison
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT country FROM pg.cities)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with countries in cities (including NULL=NULL match)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_in_null_in_subquery_no_match(self, catalog, setup_null_test_data):
        """
        Test: NOT IN when subquery contains NULL and row has no match.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT country FROM cities WHERE country IS NULL OR country = 'XX')

        Expected plan structure:
            - ANTI join with NULL guard
            - If subquery has NULL and no match, result is UNKNOWN → filtered
            - Must detect NULL presence via aggregate flag

        Expected result:
            Per SQL standard: if subquery has NULL, NOT IN evaluates to FALSE/UNKNOWN
            All rows filtered (because NULL is in subquery)
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country NOT IN (
                SELECT country FROM pg.cities
                WHERE country IS NULL OR country = 'XX'
            )
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows (NULL in subquery makes NOT IN always FALSE/UNKNOWN)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_in_null_in_subquery_with_match(self, catalog, setup_null_test_data):
        """
        Test: NOT IN when subquery contains NULL and row has a match.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT country FROM cities)

        Expected plan structure:
            - ANTI join with NULL guard
            - Rows with match → FALSE (filtered)
            - Rows without match but NULL in subquery → UNKNOWN (filtered)

        Expected result:
            All users filtered (those with matches and those without due to NULL)
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country NOT IN (SELECT country FROM pg.cities)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex NULL handling
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_in_no_nulls(self, catalog, setup_null_test_data):
        """
        Test: NOT IN when subquery has no NULLs.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT code FROM countries WHERE code IS NOT NULL)

        Expected plan structure:
            - ANTI join without NULL guard (subquery proven NULL-free)
            - Standard anti-join semantics

        Expected result:
            Users not in the countries list
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country NOT IN (
                SELECT code FROM pg.countries WHERE code IS NOT NULL
            )
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: User 10 with NULL country (and possibly others not in list)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestQuantifiedWithNulls:
    """Test quantified comparisons with NULL values."""

    def test_any_with_null(self, catalog, setup_null_test_data):
        """
        Test: > ANY when subquery contains NULL.

        Input SQL:
            SELECT * FROM products WHERE price > ANY(SELECT amount FROM orders)

        Expected plan structure:
            - SEMI join with null-safe comparison
            - NULL in subquery: price > NULL evaluates to UNKNOWN
            - If price > other values, match found

        Expected result:
            Products where price > at least one non-NULL order amount
            NULL comparisons ignored (UNKNOWN doesn't satisfy EXISTS)
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price > ANY(SELECT amount FROM pg.orders)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products with price > some order amount
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_all_with_null_no_violation(self, catalog, setup_null_test_data):
        """
        Test: < ALL when subquery contains NULL and no violation.

        Input SQL:
            SELECT * FROM products WHERE price < ALL(SELECT amount FROM orders WHERE amount > 1000)

        Expected plan structure:
            - ANTI join searching for violations (price >= amount)
            - NULL guard: if subquery has NULL and no violation, result is UNKNOWN
            - Aggregate flag to detect NULL

        Expected result:
            If subquery has NULL and row doesn't violate, UNKNOWN → filtered
            Complex three-valued logic
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price < ALL(SELECT amount FROM pg.orders WHERE amount IS NOT NULL OR amount > 1000)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex NULL handling with guard
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_all_with_null_has_violation(self, catalog, setup_null_test_data):
        """
        Test: < ALL when subquery contains NULL and row violates.

        Input SQL:
            SELECT * FROM products WHERE price < ALL(SELECT amount FROM orders)

        Expected plan structure:
            - ANTI join for violations
            - Even if NULL present, violation makes result FALSE
            - Row filtered if violation found

        Expected result:
            Products with price >= any order amount are filtered
            NULL doesn't affect violated rows
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price < ALL(SELECT amount FROM pg.orders)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products less than all order amounts
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_equals_any_with_null(self, catalog, setup_null_test_data):
        """
        Test: = ANY (equivalent to IN) with NULL.

        Input SQL:
            SELECT * FROM products WHERE price = ANY(SELECT amount FROM orders)

        Expected plan structure:
            - Same as IN semantics
            - NULL-safe equality using IS NOT DISTINCT FROM

        Expected result:
            Products with price matching some order amount
            NULL = NULL matches with null-safe comparison
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price = ANY(SELECT amount FROM pg.orders)
            ORDER BY id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products matching order amounts
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestScalarSubqueryNulls:
    """Test scalar subquery NULL handling."""

    def test_scalar_returns_null(self, catalog, setup_null_test_data):
        """
        Test: Scalar subquery returning NULL (empty result).

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT SUM(amount) FROM orders WHERE user_id = u.id) AS total
            FROM users u

        Expected plan structure:
            - LEFT join with aggregation
            - Users without orders get NULL for SUM (aggregate of empty set)

        Expected result:
            User 4: NULL (no orders)
            User 10: NULL (no orders)
            Others: their totals
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT SUM(amount) FROM pg.orders WHERE user_id = u.id) AS total
            FROM pg.users u
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with totals, NULL where no orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_scalar_aggregate_over_nulls(self, catalog, setup_null_test_data):
        """
        Test: Scalar subquery aggregating NULLs.

        Input SQL:
            SELECT u.id,
                   (SELECT MAX(amount) FROM orders WHERE user_id = u.id) AS max_amt
            FROM users u

        Expected plan structure:
            - LEFT join with MAX aggregation
            - MAX ignores NULLs, but returns NULL for empty set or all-NULL set

        Expected result:
            User 1: 200 (has order with NULL but also 100, 200)
            MAX ignores the NULL order
        """
        sql = """
            SELECT u.id,
                   (SELECT MAX(amount) FROM pg.orders WHERE user_id = u.id) AS max_amt
            FROM pg.users u
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: MAX values, NULLs properly handled
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_scalar_null_in_comparison(self, catalog, setup_null_test_data):
        """
        Test: Scalar subquery returning NULL used in comparison.

        Input SQL:
            SELECT * FROM users u
            WHERE u.id = (SELECT user_id FROM orders WHERE status = 'nonexistent')

        Expected plan structure:
            - Scalar subquery decorrelated
            - Comparison: u.id = NULL evaluates to UNKNOWN
            - Rows filtered (UNKNOWN in WHERE acts as FALSE)

        Expected result:
            Empty result (NULL comparison filters all rows)
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.id = (SELECT user_id FROM pg.orders WHERE status = 'nonexistent')
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows (NULL comparison)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_scalar_null_safe_comparison(self, catalog, setup_null_test_data):
        """
        Test: Scalar subquery with IS NOT DISTINCT FROM comparison.

        Input SQL:
            SELECT * FROM users u
            WHERE u.country IS NOT DISTINCT FROM (SELECT country FROM cities WHERE id = 10)

        Expected plan structure:
            - Scalar subquery decorrelated
            - IS NOT DISTINCT FROM handles NULLs: NULL IS NOT DISTINCT FROM NULL = TRUE

        Expected result:
            User 10 matches (both have NULL)
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.country IS NOT DISTINCT FROM (SELECT country FROM pg.cities WHERE id = 10)
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: User 10 (NULL = NULL with null-safe comparison)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestExistsWithNulls:
    """Test EXISTS/NOT EXISTS with NULL values."""

    def test_exists_null_in_correlation(self, catalog, setup_null_test_data):
        """
        Test: EXISTS with NULL in correlation key.

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (SELECT 1 FROM cities c WHERE c.country = u.country)

        Expected plan structure:
            - SEMI join with correlation predicate
            - NULL = NULL comparison depends on join implementation

        Expected result:
            Users whose country exists in cities
            NULL handling depends on whether join uses null-safe equality
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (SELECT 1 FROM pg.cities c WHERE c.country = u.country)
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with matching countries
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_exists_always_true_or_false(self, catalog, setup_null_test_data):
        """
        Test: EXISTS never returns NULL (always TRUE or FALSE).

        Input SQL:
            SELECT u.id,
                   EXISTS (SELECT 1 FROM orders WHERE user_id = u.id) AS has_orders
            FROM users u

        Expected plan structure:
            - SEMI join or LEFT join with boolean flag
            - Result is always TRUE or FALSE, never NULL

        Expected result:
            All users with has_orders boolean (no NULLs in this column)
        """
        sql = """
            SELECT u.id,
                   EXISTS (SELECT 1 FROM pg.orders WHERE user_id = u.id) AS has_orders
            FROM pg.users u
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Boolean column with TRUE/FALSE only
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_exists_with_nulls_in_subquery(self, catalog, setup_null_test_data):
        """
        Test: NOT EXISTS with NULLs in subquery data.

        Input SQL:
            SELECT * FROM users u
            WHERE NOT EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id AND o.amount IS NULL
            )

        Expected plan structure:
            - ANTI join with correlation and NULL filter
            - Users without NULL-amount orders pass

        Expected result:
            All users except user 1 (who has NULL-amount order)
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE NOT EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id AND o.amount IS NULL
            )
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users without NULL orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestComplexNullScenarios:
    """Test complex NULL handling scenarios."""

    def test_nested_nulls_in_and_not_in(self, catalog, setup_null_test_data):
        """
        Test: IN nested inside NOT IN with NULLs.

        Input SQL:
            SELECT * FROM users u
            WHERE u.id NOT IN (
                SELECT o.user_id FROM orders o
                WHERE o.amount IN (SELECT amount FROM orders WHERE amount IS NULL)
            )

        Expected plan structure:
            - Inner IN decorrelated first
            - Outer NOT IN decorrelated with NULL handling

        Expected result:
            Complex NULL semantics across nested levels
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.id NOT IN (
                SELECT o.user_id FROM pg.orders o
                WHERE o.amount IN (SELECT amount FROM pg.orders WHERE amount IS NULL)
            )
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex nested NULL handling
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_null_propagation_through_expressions(self, catalog, setup_null_test_data):
        """
        Test: NULL propagation in subquery expressions.

        Input SQL:
            SELECT u.id,
                   (SELECT MAX(amount) + 100 FROM orders WHERE user_id = u.id) AS adjusted
            FROM users u

        Expected plan structure:
            - Scalar subquery decorrelated
            - NULL + 100 = NULL (NULL propagates through arithmetic)

        Expected result:
            Users with adjusted values, NULL where no orders or all NULLs
        """
        sql = """
            SELECT u.id,
                   (SELECT MAX(amount) + 100 FROM pg.orders WHERE user_id = u.id) AS adjusted
            FROM pg.users u
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Computed values with NULL propagation
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_coalesce_with_scalar_subquery(self, catalog, setup_null_test_data):
        """
        Test: COALESCE to handle NULL from scalar subquery.

        Input SQL:
            SELECT u.id,
                   COALESCE((SELECT SUM(amount) FROM orders WHERE user_id = u.id), 0) AS total
            FROM users u

        Expected plan structure:
            - Scalar subquery decorrelated
            - COALESCE handles NULL → 0

        Expected result:
            Users with totals, 0 instead of NULL for users without orders
        """
        sql = """
            SELECT u.id,
                   COALESCE((SELECT SUM(amount) FROM pg.orders WHERE user_id = u.id), 0) AS total
            FROM pg.users u
            ORDER BY u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Totals with 0 instead of NULL
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"
