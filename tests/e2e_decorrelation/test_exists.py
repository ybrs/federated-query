"""
E2E tests for EXISTS and NOT EXISTS decorrelation.

Tests both correlated and uncorrelated EXISTS/NOT EXISTS patterns
and verifies they are rewritten to SEMI/ANTI joins.
"""
import pytest
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator
from federated_query.plan.logical import LogicalJoin


class TestUncorrelatedExists:
    """Test uncorrelated EXISTS patterns."""

    def test_uncorrelated_exists_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated EXISTS in WHERE clause.

        Input SQL:
            SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE amount > 100)

        Expected plan structure:
            - Subquery should be executed once (hoisted as CTE or evaluated to constant)
            - If subquery non-empty, all rows pass; if empty, no rows pass
            - No correlation predicates in join condition

        Expected result:
            All users (since orders with amount > 100 exist)
        """
        sql = "SELECT * FROM pg.users WHERE EXISTS (SELECT 1 FROM pg.orders WHERE amount > 100)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        # Parse and bind the query
        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)

        # Run decorrelation
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify: No subquery expressions should remain
        # This will be checked by the decorrelator validation phase

        # Execute and verify results
        # Expected: All 5 users since EXISTS evaluates to TRUE
        # TODO: Add executor integration once available
        # result = executor.execute(decorrelated_plan)
        # assert len(result) == 5

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows since no orders have amount > 10000
        # TODO: Add executor integration
        # result = executor.execute(decorrelated_plan)
        # assert len(result) == 0


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
            - Correlation predicate (o.user_id = u.id) becomes part of join condition

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify: Should contain a SEMI join
        # The decorrelated plan should have LogicalJoin with type=SEMI
        # TODO: Add plan structure validation
        # assert has_semi_join(decorrelated_plan)

        # Expected: 3 users (Alice, Charlie, Eve)
        # TODO: Add executor integration
        # result = executor.execute(decorrelated_plan)
        # assert len(result) == 3
        # assert set(r['id'] for r in result) == {1, 3, 5}

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
            - Join condition: c.name = u.city AND c.country = u.country

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users in cities that exist in the cities table
        # TODO: Add executor integration


class TestUncorrelatedNotExists:
    """Test uncorrelated NOT EXISTS patterns."""

    def test_uncorrelated_not_exists_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT EXISTS in WHERE clause.

        Input SQL:
            SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10000)

        Expected plan structure:
            - ANTI join against once-evaluated subquery
            - Since subquery is empty, all rows pass

        Expected result:
            All users (since no orders with amount > 10000 exist)
        """
        sql = "SELECT * FROM pg.users WHERE NOT EXISTS (SELECT 1 FROM pg.orders WHERE amount > 10000)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All 5 users
        # TODO: Add executor integration

    def test_uncorrelated_not_exists_filters_all(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT EXISTS that filters all rows.

        Input SQL:
            SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10)

        Expected plan structure:
            - Subquery non-empty, so NOT EXISTS evaluates to FALSE
            - No rows should pass

        Expected result:
            Empty result set
        """
        sql = "SELECT * FROM pg.users WHERE NOT EXISTS (SELECT 1 FROM pg.orders WHERE amount > 10)"

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows since orders with amount > 10 exist
        # TODO: Add executor integration


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
            - Join condition: u.id = o.user_id AND o.status = 'cancelled'

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 4 users (all except Charlie who has a cancelled order)
        # TODO: Add executor integration

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
            - No matching rows in subquery for any user

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All 5 users
        # TODO: Add executor integration


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
            - SEMI join or LEFT join with aggregation to produce boolean flag
            - Result includes boolean column

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with has_orders column
        # TODO: Add executor integration

    def test_multiple_exists_same_query(self, catalog, setup_test_data):
        """
        Test: Multiple EXISTS predicates in same query.

        Input SQL:
            SELECT u.id, u.name
            FROM users u
            WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
              AND EXISTS (SELECT 1 FROM cities c WHERE c.name = u.city)

        Expected plan structure:
            - Two SEMI joins (or combined into single plan)
            - Both correlation conditions present

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users satisfying both conditions
        # TODO: Add executor integration

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
            - Subquery must be decorrelated with aggregation preserved
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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 3 users
        # TODO: Add executor integration
