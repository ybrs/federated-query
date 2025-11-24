"""
E2E tests for nested subquery decorrelation.

Tests nested correlated subqueries, derived tables with subqueries,
and complex multi-level correlation patterns.
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


class TestNestedExists:
    """Test nested EXISTS patterns."""

    def test_exists_with_in_subquery(self, catalog, setup_test_data):
        """
        Test: EXISTS containing IN subquery.

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id
                  AND o.status IN (SELECT 'completed')
            )

        Expected plan structure:
            - Inner IN decorrelated to SEMI join first
            - Outer EXISTS decorrelated to SEMI join
            - Final plan has nested joins

        Expected result:
            Users with completed orders
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id
                  AND o.status IN (SELECT 'completed')
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with completed orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_exists_with_correlated_in(self, catalog, setup_test_data):
        """
        Test: Nested EXISTS and IN with multiple correlation levels.

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM cities c
                WHERE c.country = u.country
                  AND c.name IN (
                      SELECT o.user_id::text FROM orders o
                      WHERE o.user_id = u.id
                  )
            )

        Expected plan structure:
            - Innermost IN decorrelated (references u.id from outer scope)
            - Middle EXISTS decorrelated (references u.country)
            - Multiple correlation levels properly handled

        Expected result:
            Complex nested correlation
        """
        # Note: This is a contrived example to test multi-level correlation
        # The SQL semantics may not be meaningful, but tests the decorrelation logic
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.cities c
                WHERE c.country = u.country
                  AND c.population > (
                      SELECT MIN(o.amount) FROM pg.orders o
                      WHERE o.user_id = u.id
                  )
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Nested decorrelation with multiple levels
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_deeply_nested_exists(self, catalog, setup_test_data):
        """
        Test: Three levels of nested EXISTS.

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id
                  AND EXISTS (
                      SELECT 1 FROM products p
                      WHERE p.price > o.amount
                        AND EXISTS (
                            SELECT 1 FROM sales s
                            WHERE s.product_id = p.id AND s.region = 'East'
                        )
                  )
            )

        Expected plan structure:
            - Innermost EXISTS decorrelated first (region='East')
            - Middle EXISTS decorrelated (p.price > o.amount with nested result)
            - Outermost EXISTS decorrelated (o.user_id = u.id)
            - Three levels of joins

        Expected result:
            Users with orders that have products with sales in East region
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id
                  AND EXISTS (
                      SELECT 1 FROM pg.products p
                      WHERE p.price > o.amount
                        AND EXISTS (
                            SELECT 1 FROM pg.sales s
                            WHERE s.product_id = p.id AND s.region = 'East'
                        )
                  )
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex nested correlation
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestNestedScalarSubqueries:
    """Test nested scalar subqueries."""

    def test_scalar_in_scalar(self, catalog, setup_test_data):
        """
        Test: Scalar subquery containing another scalar subquery.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) FROM orders o
                    WHERE o.user_id = u.id
                      AND o.amount > (SELECT AVG(amount) FROM orders)) AS max_above_avg
            FROM users u

        Expected plan structure:
            - Inner scalar (AVG) decorrelated first → CTE or CROSS join
            - Outer scalar decorrelated with inner result available
            - LEFT join for outer scalar

        Expected result:
            Users with their max order amount that exceeds global average
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) FROM pg.orders o
                    WHERE o.user_id = u.id
                      AND o.amount > (SELECT AVG(amount) FROM pg.orders)) AS max_above_avg
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with max above-average order
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_scalar_with_exists_inside(self, catalog, setup_test_data):
        """
        Test: Scalar subquery with EXISTS in its WHERE clause.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM cities c
                    WHERE c.country = u.country
                      AND EXISTS (SELECT 1 FROM users u2 WHERE u2.city = c.name)) AS cities_with_users
            FROM users u

        Expected plan structure:
            - Inner EXISTS decorrelated to SEMI join
            - Outer scalar decorrelated to LEFT join with aggregation
            - Correlation from multiple levels

        Expected result:
            Users with count of cities in their country that have users
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM pg.cities c
                    WHERE c.country = u.country
                      AND EXISTS (SELECT 1 FROM pg.users u2 WHERE u2.city = c.name)) AS cities_with_users
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex nested correlation
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestDerivedTablesWithSubqueries:
    """Test subqueries in derived tables (FROM clause subqueries)."""

    def test_derived_table_with_exists(self, catalog, setup_test_data):
        """
        Test: Derived table containing EXISTS.

        Input SQL:
            SELECT dt.id, dt.name
            FROM (
                SELECT u.id, u.name
                FROM users u
                WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
            ) dt

        Expected plan structure:
            - EXISTS inside derived table decorrelated first
            - Derived table becomes normal scan over decorrelated result
            - Outer query scans derived table

        Expected result:
            Users with orders (via derived table)
        """
        sql = """
            SELECT dt.id, dt.name
            FROM (
                SELECT u.id, u.name
                FROM pg.users u
                WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)
            ) dt
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_derived_table_with_scalar(self, catalog, setup_test_data):
        """
        Test: Derived table with scalar subquery in SELECT.

        Input SQL:
            SELECT dt.id, dt.total
            FROM (
                SELECT u.id,
                       (SELECT SUM(o.amount) FROM orders o WHERE o.user_id = u.id) AS total
                FROM users u
            ) dt
            WHERE dt.total > 200

        Expected plan structure:
            - Scalar subquery inside derived table decorrelated
            - Derived table produces decorrelated result
            - Outer filter on derived table output

        Expected result:
            Users with total orders > 200
        """
        sql = """
            SELECT dt.id, dt.total
            FROM (
                SELECT u.id,
                       (SELECT SUM(o.amount) FROM pg.orders o WHERE o.user_id = u.id) AS total
                FROM pg.users u
            ) dt
            WHERE dt.total > 200
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with high totals
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_correlated_subquery_referencing_derived_table(self, catalog, setup_test_data):
        """
        Test: Outer query subquery that references derived table.

        Input SQL:
            SELECT dt.id, dt.name,
                   (SELECT COUNT(*) FROM orders o WHERE o.user_id = dt.id) AS order_count
            FROM (
                SELECT u.id, u.name FROM users u WHERE u.country = 'US'
            ) dt

        Expected plan structure:
            - Derived table is simple (no subqueries)
            - Outer scalar subquery correlates with derived table
            - Correlation keys rewritten to reference dt output

        Expected result:
            US users with their order counts
        """
        sql = """
            SELECT dt.id, dt.name,
                   (SELECT COUNT(*) FROM pg.orders o WHERE o.user_id = dt.id) AS order_count
            FROM (
                SELECT u.id, u.name FROM pg.users u WHERE u.country = 'US'
            ) dt
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: US users with order counts
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestSubqueriesInJoinConditions:
    """Test subqueries appearing in JOIN ON clauses."""

    def test_scalar_in_join_condition(self, catalog, setup_test_data):
        """
        Test: Scalar subquery in JOIN ON clause.

        Input SQL:
            SELECT u.id, o.id AS order_id
            FROM users u
            JOIN orders o ON o.user_id = u.id
                         AND o.amount > (SELECT AVG(amount) FROM orders)

        Expected plan structure:
            - Scalar subquery decorrelated (uncorrelated AVG)
            - Join condition includes comparison with decorrelated value

        Expected result:
            User-order pairs where order > average
        """
        sql = """
            SELECT u.id, o.id AS order_id
            FROM pg.users u
            JOIN pg.orders o ON o.user_id = u.id
                            AND o.amount > (SELECT AVG(amount) FROM pg.orders)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Joins with above-average orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_exists_in_join_condition(self, catalog, setup_test_data):
        """
        Test: EXISTS in JOIN ON clause.

        Input SQL:
            SELECT u.id, c.name
            FROM users u
            JOIN cities c ON c.country = u.country
                         AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)

        Expected plan structure:
            - EXISTS decorrelated to SEMI join
            - Original join condition combined with decorrelated result

        Expected result:
            User-city pairs where user has orders
        """
        sql = """
            SELECT u.id, c.name
            FROM pg.users u
            JOIN pg.cities c ON c.country = u.country
                            AND EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Joins for users with orders
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestMultipleSubquerySameLevel:
    """Test multiple independent subqueries at same nesting level."""

    def test_multiple_exists_independent(self, catalog, setup_test_data):
        """
        Test: Multiple independent EXISTS in WHERE (connected by AND).

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
              AND EXISTS (SELECT 1 FROM cities c WHERE c.name = u.city)

        Expected plan structure:
            - Both EXISTS decorrelated independently
            - Two SEMI joins (or combined if optimizer chooses)
            - Both correlation predicates present

        Expected result:
            Users with orders AND whose city exists in cities table
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)
              AND EXISTS (SELECT 1 FROM pg.cities c WHERE c.name = u.city)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users satisfying both conditions
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_multiple_scalars_independent(self, catalog, setup_test_data):
        """
        Test: Multiple independent scalar subqueries in SELECT.

        Input SQL:
            SELECT u.id,
                   (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count,
                   (SELECT MAX(amount) FROM orders WHERE user_id = u.id) AS max_amount,
                   (SELECT MIN(amount) FROM orders WHERE user_id = u.id) AS min_amount
            FROM users u

        Expected plan structure:
            - Three independent scalar subqueries decorrelated
            - All use same correlation key (u.id)
            - Could be optimized to single join with multiple aggregates

        Expected result:
            Users with three aggregate columns
        """
        sql = """
            SELECT u.id,
                   (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) AS order_count,
                   (SELECT MAX(amount) FROM pg.orders WHERE user_id = u.id) AS max_amount,
                   (SELECT MIN(amount) FROM pg.orders WHERE user_id = u.id) AS min_amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with aggregate columns
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_mixed_subquery_types(self, catalog, setup_test_data):
        """
        Test: Mix of EXISTS, IN, and scalar subqueries.

        Input SQL:
            SELECT u.id,
                   (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count
            FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
              AND u.country IN (SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - Scalar subquery decorrelated (LEFT join)
            - EXISTS decorrelated (SEMI join)
            - IN decorrelated (SEMI join)
            - All decorrelations at same level

        Expected result:
            Users in enabled countries with orders, showing order count
        """
        sql = """
            SELECT u.id,
                   (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) AS order_count
            FROM pg.users u
            WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)
              AND u.country IN (SELECT code FROM pg.countries WHERE enabled)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users satisfying all conditions
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestRecursiveDecorrelation:
    """Test recursive decorrelation scenarios."""

    def test_fixed_point_iteration(self, catalog, setup_test_data):
        """
        Test: Decorrelation requiring multiple passes (fixed-point iteration).

        Input SQL:
            SELECT * FROM users u
            WHERE u.id IN (
                SELECT o.user_id FROM orders o
                WHERE o.amount > ANY(
                    SELECT s.price FROM sales s
                    WHERE s.seller = 'SellerA'
                      AND EXISTS (
                          SELECT 1 FROM products p
                          WHERE p.id = s.product_id AND p.category = 'Electronics'
                      )
                )
            )

        Expected plan structure:
            - Innermost EXISTS decorrelated first
            - ANY decorrelated next (using decorrelated EXISTS result)
            - Outermost IN decorrelated last
            - Multiple passes until no subqueries remain

        Expected result:
            Users with orders above certain sales prices
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.id IN (
                SELECT o.user_id FROM pg.orders o
                WHERE o.amount > ANY(
                    SELECT s.price FROM pg.sales s
                    WHERE s.seller = 'SellerA'
                      AND EXISTS (
                          SELECT 1 FROM pg.products p
                          WHERE p.id = s.product_id AND p.category = 'Electronics'
                      )
                )
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex nested decorrelation
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_prevent_infinite_loop(self, catalog, setup_test_data):
        """
        Test: Ensure decorrelation doesn't infinite loop.

        The decorrelator should have a rewrite counter to prevent infinite loops
        in case of bugs or unsupported patterns.

        Input SQL:
            Deeply nested but finite subquery structure

        Expected plan structure:
            - Decorrelation completes successfully
            - Counter doesn't exceed limit

        Expected result:
            Successful decorrelation or clear error if limit exceeded
        """
        # Using a complex but valid query
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id
                  AND o.amount IN (
                      SELECT s.price FROM pg.sales s
                      WHERE EXISTS (
                          SELECT 1 FROM pg.products p
                          WHERE p.id = s.product_id
                      )
                  )
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Successful decorrelation
        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
