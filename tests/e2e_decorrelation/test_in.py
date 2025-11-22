"""
E2E tests for IN and NOT IN decorrelation.

Tests both correlated and uncorrelated IN/NOT IN patterns with focus on
null-safe equality semantics and proper NULL handling.
"""
import pytest
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator


class TestUncorrelatedIn:
    """Test uncorrelated IN patterns."""

    def test_uncorrelated_in_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN with simple subquery.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - Subquery hoisted as CTE
            - SEMI join between users and CTE
            - Join condition uses null-safe equality: users.country IS NOT DISTINCT FROM cte.code

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify: Should use null-safe equality in join condition
        # Expected: All 5 users since all their countries are enabled
        # TODO: Add executor integration

    def test_uncorrelated_in_no_matches(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN with no matching values.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT code FROM countries WHERE code = 'DE')

        Expected plan structure:
            - SEMI join with subquery returning only 'DE'
            - No users have country='DE'

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows
        # TODO: Add executor integration

    def test_uncorrelated_in_with_distinct_optimization(self, catalog, setup_test_data):
        """
        Test: Uncorrelated IN where subquery may benefit from DISTINCT.

        Input SQL:
            SELECT * FROM users WHERE country IN (SELECT country FROM cities)

        Expected plan structure:
            - Subquery may have DISTINCT added by optimizer (optional)
            - SEMI join ensures correct semantics regardless

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All users
        # TODO: Add executor integration


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
            - Join condition combines:
              1. Null-safe equality: u.city IS NOT DISTINCT FROM c.name
              2. Correlation predicate: c.country = u.country

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users in cities that match their country
        # TODO: Add executor integration

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
            - Null-safe equality on computed values

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users matching with case-insensitive comparison
        # TODO: Add executor integration


class TestUncorrelatedNotIn:
    """Test uncorrelated NOT IN patterns."""

    def test_uncorrelated_not_in_basic(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT IN with simple subquery.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT code FROM countries WHERE code = 'DE')

        Expected plan structure:
            - LEFT join with null-safe equality
            - Filter: match IS NULL AND subquery has no NULLs
            - Since 'DE' is not NULL, normal anti-join semantics apply

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All 5 users
        # TODO: Add executor integration

    def test_uncorrelated_not_in_filters_rows(self, catalog, setup_test_data):
        """
        Test: Uncorrelated NOT IN that filters some rows.

        Input SQL:
            SELECT * FROM users WHERE country NOT IN (SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - ANTI join against enabled countries
            - Filter out users in enabled countries

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows
        # TODO: Add executor integration


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
            - LEFT join with correlation and null-safe equality
            - Filter: match IS NULL AND no NULLs in subquery result
            - Null guard: aggregate to check if subquery produced any NULL

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users not in large cities
        # London has 9M pop, so Bob should be filtered
        # TODO: Add executor integration


class TestInWithNullSemantics:
    """Test IN/NOT IN with NULL handling."""

    def test_in_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: IN when subquery contains NULL.

        Input SQL:
            Setup: Insert user with country=NULL
            SELECT * FROM users WHERE country IN (SELECT country FROM cities)

        Expected plan structure:
            - Null-safe equality using IS NOT DISTINCT FROM
            - NULL = NULL evaluates to TRUE with null-safe comparison

        Expected result:
            NULL values handled correctly per SQL standard
        """
        # Note: This test requires setup with NULL values
        # Will need to add test-specific data or modify fixture
        sql = """
            SELECT * FROM pg.users
            WHERE country IN (SELECT country FROM pg.cities)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Proper NULL handling with IS NOT DISTINCT FROM
        # TODO: Add executor integration with NULL test data

    def test_not_in_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: NOT IN when subquery contains NULL.

        Input SQL:
            Setup: Subquery that returns NULL
            SELECT * FROM users WHERE country NOT IN (SELECT country FROM cities WHERE country IS NULL OR country = 'US')

        Expected plan structure:
            - Must include NULL guard
            - If subquery has any NULL and no match found, result is FALSE/UNKNOWN
            - Requires aggregate flag to detect NULL presence

        Expected result:
            Proper three-valued logic: rows with match → FALSE, rows without match but NULL exists → UNKNOWN (filtered)
        """
        # This test demonstrates the complexity of NOT IN with NULL
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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Complex NULL handling
        # TODO: Add executor integration

    def test_in_null_comparison_lhs(self, catalog, setup_test_data):
        """
        Test: IN when left-hand side is NULL.

        Input SQL:
            Setup: User with NULL country
            SELECT * FROM users WHERE country IN ('US', 'UK')

        Expected plan structure:
            - Standard SEMI join
            - NULL on LHS never matches (NULL = 'US' is UNKNOWN)

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with country US or UK (no NULLs match)
        # TODO: Add executor integration


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
            - Join condition: u.city IS NOT DISTINCT FROM c.name
                         AND u.country IS NOT DISTINCT FROM c.country

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with matching (city, country) tuples
        # TODO: Add executor integration

    def test_in_select_list_as_boolean(self, catalog, setup_test_data):
        """
        Test: IN expression in SELECT list producing boolean.

        Input SQL:
            SELECT u.id, u.country,
                   u.country IN (SELECT code FROM countries WHERE enabled) AS in_enabled_country
            FROM users u

        Expected plan structure:
            - LEFT join or SEMI join with boolean flag projection
            - Result includes boolean column

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with boolean flag
        # TODO: Add executor integration

    def test_multiple_in_predicates(self, catalog, setup_test_data):
        """
        Test: Multiple IN predicates in same query.

        Input SQL:
            SELECT * FROM users u
            WHERE u.country IN (SELECT code FROM countries WHERE enabled)
              AND u.city IN (SELECT name FROM cities WHERE population > 1000000)

        Expected plan structure:
            - Multiple SEMI joins (one for each IN)
            - Both join conditions present

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

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users satisfying both IN conditions
        # TODO: Add executor integration
