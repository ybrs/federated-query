"""
E2E tests for EXISTS and NOT EXISTS decorrelation.

Tests both correlated and uncorrelated EXISTS/NOT EXISTS patterns
and verifies they are rewritten to SEMI/ANTI joins.
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


class TestUncorrelatedExists:
    """Test uncorrelated EXISTS patterns."""

    def test_uncorrelated_exists_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated EXISTS in WHERE clause.

        Input SQL:
            SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 100)

        Expected plan structure:
            - No subquery expressions remain
            - May have CROSS join or filter with constant

        Expected result:
            All users (since orders with amount > 100 exist)
        """
        sql = "SELECT * FROM pg.users WHERE EXISTS (SELECT 1 FROM pg.orders WHERE amount > 100)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes remain
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify results: All 5 users
        assert_result_count(executor, decorrelated_plan, 5)

    def test_uncorrelated_exists_empty_result(self, catalog, setup_test_data):
        """
        Test: Uncorrelated EXISTS with empty subquery.

        Input SQL:
            SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 10000)

        Expected plan structure:
            - Subquery evaluates to empty
            - Entire query should return no rows

        Expected result:
            Empty result set
        """
        sql = "SELECT * FROM pg.users WHERE EXISTS (SELECT 1 FROM pg.orders WHERE amount > 10000)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes remain
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify results: 0 rows
        assert_result_count(executor, decorrelated_plan, 0)


class TestCorrelatedExists:
    """Test correlated EXISTS patterns."""

    def test_correlated_exists_basic(self, catalog, setup_test_data):
        """
        Test: Correlated EXISTS with single correlation predicate.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id AND o.amount > 100
            )

        Expected plan structure:
            - SEMI join between users and orders
            - Join condition: u.id = o.user_id AND o.amount > 100

        Expected result:
            Users with orders > 100: Alice (id=1), Charlie (id=3), Eve (id=5)
        """
        sql = """
            SELECT u.id, u.name
            FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id AND o.amount > 100
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan has SEMI join
        assert_plan_structure(decorrelated_plan, {
            'has_semi_join': True,
            'semi_join_count': 1
        })

        # Execute and verify results: 3 users
        assert_result_contains_ids(executor, decorrelated_plan, {1, 3, 5})

    def test_correlated_exists_multiple_correlation_keys(self, catalog, setup_test_data):
        """
        Test: Correlated EXISTS with multiple correlation predicates.

        Input SQL:
            SELECT u.id, u.name, u.country
            FROM users u
            WHERE EXISTS (
                SELECT 1 FROM cities c
                WHERE c.name = u.city AND c.country = u.country
            )

        Expected plan structure:
            - SEMI join with conjunction of correlation predicates

        Expected result:
            Users whose (city, country) pair exists in cities table
        """
        sql = """
            SELECT u.id, u.name, u.country
            FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.cities c
                WHERE c.name = u.city AND c.country = u.country
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan has SEMI join
        assert_plan_structure(decorrelated_plan, {
            'has_semi_join': True
        })

        # Execute and verify results
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) > 0, "Should have users with matching cities"


class TestUncorrelatedNotExists:
    """Test uncorrelated NOT EXISTS patterns."""

    def test_uncorrelated_not_exists_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT EXISTS in WHERE clause.

        Input SQL:
            SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10000)

        Expected plan structure:
            - ANTI join or equivalent

        Expected result:
            All users (since no orders with amount > 10000 exist)
        """
        sql = "SELECT * FROM pg.users WHERE NOT EXISTS (SELECT 1 FROM pg.orders WHERE amount > 10000)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes remain
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify results: All 5 users
        assert_result_count(executor, decorrelated_plan, 5)

    def test_uncorrelated_not_exists_filters_all(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT EXISTS that filters all rows.

        Input SQL:
            SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10)

        Expected plan structure:
            - Subquery non-empty, so NOT EXISTS evaluates to FALSE

        Expected result:
            Empty result set
        """
        sql = "SELECT * FROM pg.users WHERE NOT EXISTS (SELECT 1 FROM pg.orders WHERE amount > 10)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes remain
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify results: 0 rows
        assert_result_count(executor, decorrelated_plan, 0)


class TestCorrelatedNotExists:
    """Test correlated NOT EXISTS patterns."""

    def test_correlated_not_exists_basic(self, catalog, setup_test_data):
        """
        Test: Correlated NOT EXISTS with single correlation predicate.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE NOT EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id AND o.status = 'cancelled'
            )

        Expected plan structure:
            - ANTI join between users and orders

        Expected result:
            Users without cancelled orders: Alice (id=1), Bob (id=2), David (id=4), Eve (id=5)
        """
        sql = """
            SELECT u.id, u.name
            FROM pg.users u
            WHERE NOT EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id AND o.status = 'cancelled'
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan has ANTI join
        assert_plan_structure(decorrelated_plan, {
            'has_anti_join': True
        })

        # Execute and verify results: 4 users (all except Charlie)
        assert_result_contains_ids(executor, decorrelated_plan, {1, 2, 4, 5})

    def test_correlated_not_exists_all_pass(self, catalog, setup_test_data):
        """
        Test: Correlated NOT EXISTS where all rows pass.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE NOT EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id AND o.amount > 10000
            )

        Expected plan structure:
            - ANTI join

        Expected result:
            All users (no orders over 10000)
        """
        sql = """
            SELECT u.id, u.name
            FROM pg.users u
            WHERE NOT EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id AND o.amount > 10000
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan has ANTI join
        assert_plan_structure(decorrelated_plan, {
            'has_anti_join': True
        })

        # Execute and verify results: All 5 users
        assert_result_count(executor, decorrelated_plan, 5)


class TestExistsInComplexQueries:
    """Test EXISTS in complex query contexts."""

    def test_exists_in_select_list(self, catalog, setup_test_data):
        """
        Test: EXISTS as a boolean expression in SELECT list.

        Input SQL:
            SELECT u.id, u.name,
                   EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) AS has_orders
            FROM users u

        Expected plan structure:
            - LEFT join or other mechanism to produce boolean flag

        Expected result:
            All users with has_orders column (TRUE for users 1,2,3,5; FALSE for user 4)
        """
        sql = """
            SELECT u.id, u.name,
                   EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id) AS has_orders
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes remain
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify results: 5 rows with has_orders column
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) == 5, "Should have all 5 users"
        assert 'has_orders' in results[0], "Should have has_orders column"

    def test_multiple_exists_same_query(self, catalog, setup_test_data):
        """
        Test: Multiple EXISTS predicates in same query.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
              AND EXISTS (SELECT 1 FROM cities c WHERE c.name = u.city)

        Expected plan structure:
            - Multiple SEMI joins

        Expected result:
            Users with orders AND whose city exists in cities table
        """
        sql = """
            SELECT u.id, u.name
            FROM pg.users u
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

        # Verify plan has multiple SEMI joins
        assert_plan_structure(decorrelated_plan, {
            'has_semi_join': True
        })

        # Execute and verify results: Users satisfying both conditions
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) > 0, "Should have users satisfying both conditions"

    def test_exists_with_aggregation_in_subquery(self, catalog, setup_test_data):
        """
        Test: EXISTS with aggregation and HAVING in subquery.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = u.id
                GROUP BY o.user_id
                HAVING SUM(o.amount) > 200
            )

        Expected plan structure:
            - SEMI join against aggregated result

        Expected result:
            Users with total orders > 200: Alice (300), Charlie (350), Eve (500)
        """
        sql = """
            SELECT u.id, u.name
            FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = u.id
                GROUP BY o.user_id
                HAVING SUM(o.amount) > 200
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan has SEMI join and aggregation
        assert_plan_structure(decorrelated_plan, {
            'has_semi_join': True,
            'has_aggregation': True
        })

        # Execute and verify results: 3 users
        assert_result_contains_ids(executor, decorrelated_plan, {1, 3, 5})

    def test_exists_under_or_predicate(self, catalog, setup_test_data):
        """
        Test: Correlated EXISTS combined with OR condition.

        Input SQL:
            SELECT u.id
            FROM users u
            WHERE u.country = 'FR'
               OR EXISTS (
                   SELECT 1 FROM orders o
                   WHERE o.user_id = u.id AND o.amount > 250
               )

        Expected plan structure:
            - SEMI join for EXISTS branch
            - Predicate retains OR logic

        Expected result:
            Users from FR or with orders > 250 (ids 1,3,4,5)
        """
        sql = """
            SELECT u.id
            FROM pg.users u
            WHERE u.country = 'FR'
               OR EXISTS (
                   SELECT 1 FROM pg.orders o
                   WHERE o.user_id = u.id AND o.amount > 250
               )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        assert_plan_structure(decorrelated_plan, {
            'has_semi_join': True
        })

        results = execute_and_fetch_all(executor, decorrelated_plan)
        ids = set()
        for row in results:
            ids.add(row['id'])
        assert ids == {1, 3, 4, 5}, f"Unexpected ids {ids}"
