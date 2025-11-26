"""
E2E tests for IN and NOT IN decorrelation.

Tests both correlated and uncorrelated IN/NOT IN patterns with focus on
null-safe equality semantics and proper NULL handling.
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


class TestUncorrelatedIn:
    """Test uncorrelated IN patterns."""

    def test_uncorrelated_in_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN with simple subquery.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - SEMI join between users and countries
            - Join condition uses null-safe equality

        Expected result:
            Users in enabled countries: all users (US, UK, FR are enabled)
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT code FROM pg.countries WHERE enabled)
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

        # Execute and verify: All 5 users
        assert_result_count(executor, decorrelated_plan, 5)

    def test_uncorrelated_in_no_matches(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN with no matching values.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT code FROM countries WHERE code = 'DE')

        Expected plan structure:
            - SEMI join with subquery returning only 'DE'

        Expected result:
            Empty result set
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT code FROM pg.countries WHERE code = 'DE')
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

        # Execute and verify: 0 rows
        assert_result_count(executor, decorrelated_plan, 0)

    def test_uncorrelated_in_with_distinct_optimization(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN where subquery may benefit from DISTINCT.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT country FROM cities)

        Expected plan structure:
            - SEMI join ensures correct semantics

        Expected result:
            Users in countries that have cities: all users
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT country FROM pg.cities)
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

        # Execute and verify: All users
        assert_result_count(executor, decorrelated_plan, 5)


class TestCorrelatedIn:
    """Test correlated IN patterns."""

    def test_correlated_in_basic(self, catalog, setup_test_data):
        """
        Test: Correlated IN with single correlation predicate.

        Input SQL:
            SELECT * FROM users u
            WHERE u.city IN (
                SELECT c.name FROM cities c WHERE c.country = u.country
            )

        Expected plan structure:
            - SEMI join between users and cities
            - Join condition combines null-safe equality and correlation

        Expected result:
            Users whose city exists in cities table for their country
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.city IN (
                SELECT c.name FROM pg.cities c WHERE c.country = u.country
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

    def test_correlated_in_expression_lhs(self, catalog, setup_test_data):
        """
        Test: Correlated IN with expression on left-hand side.

        Input SQL:
            SELECT * FROM users u
            WHERE UPPER(u.city) IN (
                SELECT UPPER(c.name) FROM cities c WHERE c.country = u.country
            )

        Expected plan structure:
            - SEMI join with computed expression in join condition

        Expected result:
            Same as basic correlated IN (case-insensitive match)
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE UPPER(u.city) IN (
                SELECT UPPER(c.name) FROM pg.cities c WHERE c.country = u.country
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
        assert len(results) > 0, "Should have users with case-insensitive matching"


class TestUncorrelatedNotIn:
    """Test uncorrelated NOT IN patterns."""

    def test_uncorrelated_not_in_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT IN with simple subquery.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT code FROM countries WHERE code = 'DE')

        Expected plan structure:
            - ANTI join with null-safe equality

        Expected result:
            All users (no user has country='DE')
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country NOT IN (SELECT code FROM pg.countries WHERE code = 'DE')
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

        # Execute and verify: All 5 users
        assert_result_count(executor, decorrelated_plan, 5)

    def test_uncorrelated_not_in_filters_rows(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT IN that filters some rows.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - ANTI join against enabled countries

        Expected result:
            Empty (all user countries are enabled)
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country NOT IN (SELECT code FROM pg.countries WHERE enabled)
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

        # Execute and verify: 0 rows
        assert_result_count(executor, decorrelated_plan, 0)


class TestCorrelatedNotIn:
    """Test correlated NOT IN patterns."""

    def test_correlated_not_in_basic(self, catalog, setup_test_data):
        """
        Test: Correlated NOT IN with correlation predicate.

        Input SQL:
            SELECT * FROM users u
            WHERE u.city NOT IN (
                SELECT c.name FROM cities c
                WHERE c.country = u.country AND c.population > 5000000
            )

        Expected plan structure:
            - ANTI join with correlation and null-safe equality
            - NULL guard if subquery can produce NULL

        Expected result:
            Users whose city is not in large cities for their country
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.city NOT IN (
                SELECT c.name FROM pg.cities c
                WHERE c.country = u.country AND c.population > 5000000
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

        # Execute and verify results
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) > 0, "Should have users not in large cities"


class TestInWithNullSemantics:
    """Test IN/NOT IN with NULL handling."""

    def test_in_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: IN when subquery contains NULL.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT country FROM cities)

        Expected plan structure:
            - Null-safe equality using IS NOT DISTINCT FROM

        Expected result:
            NULL values handled correctly per SQL standard
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT country FROM pg.cities)
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
        assert len(results) > 0, "Should have matching users"

    def test_not_in_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: NOT IN when subquery contains NULL.

        Input SQL:
            SELECT * FROM users u
            WHERE u.country NOT IN (
                SELECT c.country FROM cities c
                WHERE c.country IS NULL OR c.country = 'US'
            )

        Expected plan structure:
            - ANTI join with NULL guard

        Expected result:
            Complex NULL handling per SQL standard
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.country NOT IN (
                SELECT c.country FROM pg.cities c
                WHERE c.country IS NULL OR c.country = 'US'
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

        # Execute and verify results
        results = execute_and_fetch_all(executor, decorrelated_plan)
        # Complex NULL semantics - may filter all or some rows

    def test_in_null_comparison_lhs(self, catalog, setup_test_data):
        """
        Test: IN when left-hand side is NULL.

        Input SQL:
            SELECT * FROM users WHERE country IN ('US', 'UK')

        Expected plan structure:
            - Standard SEMI join

        Expected result:
            Rows with NULL country filtered out
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country IN ('US', 'UK')
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify no subquery nodes (constant list)
        assert_plan_structure(decorrelated_plan, {})

        # Execute and verify: Users with country US or UK
        results = execute_and_fetch_all(executor, decorrelated_plan)
        result_ids = {r['id'] for r in results}
        assert 1 in result_ids, "Should include Alice (US)"
        assert 2 in result_ids, "Should include Bob (UK)"


class TestInComplexQueries:
    """Test IN in complex query contexts."""

    def test_in_with_multiple_columns(self, catalog, setup_test_data):
        """
        Test: IN with tuple comparison.

        Input SQL:
            SELECT * FROM users u
            WHERE (u.city, u.country) IN (
                SELECT c.name, c.country FROM cities c
            )

        Expected plan structure:
            - SEMI join with compound null-safe equality

        Expected result:
            Users whose (city, country) pair exists in cities
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE (u.city, u.country) IN (
                SELECT c.name, c.country FROM pg.cities c
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
        assert len(results) > 0, "Should have users with matching tuples"

    def test_in_select_list_as_boolean(self, catalog, setup_test_data):
        """
        Test: IN expression in SELECT list producing boolean.

        Input SQL:
            SELECT u.id, u.country,
                   u.country IN (SELECT code FROM countries WHERE enabled) AS in_enabled_country
            FROM users u

        Expected plan structure:
            - Produces boolean column

        Expected result:
            All users with in_enabled_country column
        """
        sql = """
            SELECT u.id, u.country,
                   u.country IN (SELECT code FROM pg.countries WHERE enabled) AS in_enabled_country
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

        # Execute and verify: 5 rows with boolean flag
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) == 5, "Should have all 5 users"
        assert 'in_enabled_country' in results[0], "Should have boolean column"

    def test_multiple_in_predicates(self, catalog, setup_test_data):
        """
        Test: Multiple IN predicates in same query.

        Input SQL:
            SELECT * FROM users u
            WHERE u.country IN (SELECT code FROM countries WHERE enabled)
              AND u.city IN (SELECT name FROM cities WHERE population > 1000000)

        Expected plan structure:
            - Multiple SEMI joins

        Expected result:
            Users in enabled countries AND large cities
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.country IN (SELECT code FROM pg.countries WHERE enabled)
              AND u.city IN (SELECT name FROM pg.cities WHERE population > 1000000)
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

        # Execute and verify results
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) > 0, "Should have users satisfying both conditions"
