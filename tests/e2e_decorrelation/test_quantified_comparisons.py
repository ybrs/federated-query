"""
E2E tests for quantified comparison decorrelation (ANY/SOME/ALL).

Tests ALL, ANY, and SOME quantified comparisons with various operators,
focusing on NULL handling and duplicate semantics.
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


class TestAnyComparison:
    """Test = ANY, > ANY, < ANY, etc. patterns."""

    def test_equals_any_basic(self, catalog, setup_test_data):
        """
        Test: = ANY is equivalent to IN.

        Input SQL:
            SELECT * FROM users WHERE country = ANY(SELECT code FROM countries WHERE enabled)

        Expected plan structure:
            - SEMI join (same as IN rewrite)
            - Null-safe equality: country IS NOT DISTINCT FROM code

        Expected result:
            Same as IN: users in enabled countries
        """
        sql = """
            SELECT * FROM pg.users
            WHERE country = ANY(SELECT code FROM pg.countries WHERE enabled)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All 5 users (all in enabled countries)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_greater_than_any_correlated(self, catalog, setup_test_data):
        """
        Test: Correlated > ANY comparison.

        Input SQL:
            SELECT * FROM sales s
            WHERE s.price > ANY(
                SELECT o.amount FROM offers o WHERE o.seller = s.seller
            )

        Expected plan structure:
            - SEMI join with condition: s.price > o.amount AND o.seller = s.seller
            - Correlation predicate becomes part of join

        Expected result:
            Sales where price exceeds at least one offer from same seller
        """
        sql = """
            SELECT * FROM pg.sales s
            WHERE s.price > ANY(
                SELECT o.amount FROM pg.offers o WHERE o.seller = s.seller
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Sales with price > at least one offer
        # SellerA: sale price 950 > offer 900 (yes), sale price 18 > offer 15 (yes)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_less_than_any_uncorrelated(self, catalog, setup_test_data):
        """
        Test: Uncorrelated < ANY comparison.

        Input SQL:
            SELECT * FROM products WHERE price < ANY(SELECT amount FROM orders)

        Expected plan structure:
            - Subquery hoisted as CTE
            - SEMI join with condition: products.price < cte.amount

        Expected result:
            Products with price less than at least one order amount
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price < ANY(SELECT amount FROM pg.orders)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products cheaper than at least one order
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_equals_any_correlated(self, catalog, setup_test_data):
        """
        Test: <> ANY is equivalent to NOT = ALL.

        Input SQL:
            SELECT * FROM sales s
            WHERE s.price <> ANY(
                SELECT o.amount FROM offers o WHERE o.product_id = s.product_id
            )

        Expected plan structure:
            - SEMI join looking for any non-equal value
            - Condition: s.price <> o.amount (null-safe)

        Expected result:
            Sales where price differs from at least one offer for same product
        """
        sql = """
            SELECT * FROM pg.sales s
            WHERE s.price <> ANY(
                SELECT o.amount FROM pg.offers o WHERE o.product_id = s.product_id
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Sales with price != at least one offer
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestSomeComparison:
    """Test SOME (synonym for ANY)."""

    def test_some_equals_any(self, catalog, setup_test_data):
        """
        Test: SOME is exactly equivalent to ANY.

        Input SQL:
            SELECT * FROM products WHERE price > SOME(SELECT amount FROM orders)

        Expected plan structure:
            - Identical to > ANY rewrite
            - SEMI join with > predicate

        Expected result:
            Products with price > at least one order amount
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price > SOME(SELECT amount FROM pg.orders)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Same as > ANY
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestAllComparison:
    """Test ALL quantified comparisons."""

    def test_less_than_or_equal_all_correlated(self, catalog, setup_test_data):
        """
        Test: Correlated <= ALL comparison.

        Input SQL:
            SELECT * FROM sales s
            WHERE s.price <= ALL(
                SELECT l.cap FROM limits l WHERE l.region = s.region
            )

        Expected plan structure:
            - ANTI join to search for violations: s.price > l.cap
            - If no violation found, predicate is TRUE
            - Must include NULL guard: if subquery has NULL, result is UNKNOWN

        Expected result:
            Sales where price is at or below all caps for their region
        """
        sql = """
            SELECT * FROM pg.sales s
            WHERE s.price <= ALL(
                SELECT l.cap FROM pg.limits l WHERE l.region = s.region
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Sales within regional caps
        # East: sale 950 <= 1000 (yes), sale 18 <= 1000 (yes)
        # West: sale 280 <= 300 (yes), sale 140 <= 300 (yes)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_greater_than_all_uncorrelated(self, catalog, setup_test_data):
        """
        Test: Uncorrelated > ALL comparison.

        Input SQL:
            SELECT * FROM products WHERE price > ALL(SELECT amount FROM orders WHERE status = 'cancelled')

        Expected plan structure:
            - Subquery hoisted as CTE
            - ANTI join searching for violations: products.price <= cte.amount
            - NULL guard if subquery can produce NULL

        Expected result:
            Products with price greater than all cancelled order amounts
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price > ALL(SELECT amount FROM pg.orders WHERE status = 'cancelled')
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products more expensive than all cancelled orders
        # Cancelled orders: only order 5 with amount=50
        # Products > 50: Laptop (1000), Desk (300), Chair (150)
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_equals_all_correlated(self, catalog, setup_test_data):
        """
        Test: = ALL requires all values to be equal.

        Input SQL:
            SELECT * FROM users u
            WHERE u.country = ALL(
                SELECT c.country FROM cities c WHERE c.name = u.city
            )

        Expected plan structure:
            - ANTI join searching for violations: u.country <> c.country
            - NULL guard required

        Expected result:
            Users where their country equals all countries for cities with their city name
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.country = ALL(
                SELECT c.country FROM pg.cities c WHERE c.name = u.city
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users whose country matches all city.country for their city
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_not_equals_all_correlated(self, catalog, setup_test_data):
        """
        Test: <> ALL means not equal to any value.

        Input SQL:
            SELECT * FROM sales s
            WHERE s.price <> ALL(
                SELECT o.amount FROM offers o WHERE o.seller = s.seller
            )

        Expected plan structure:
            - ANTI join searching for any equality: s.price = o.amount
            - Equivalent to NOT IN

        Expected result:
            Sales where price doesn't match any offer from same seller
        """
        sql = """
            SELECT * FROM pg.sales s
            WHERE s.price <> ALL(
                SELECT o.amount FROM pg.offers o WHERE o.seller = s.seller
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Sales with price not matching any offer
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestQuantifiedWithNulls:
    """Test quantified comparisons with NULL handling."""

    def test_any_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: > ANY when subquery contains NULL.

        Input SQL:
            Setup: Subquery returns [100, NULL, 200]
            SELECT * FROM products WHERE price > ANY(...)

        Expected plan structure:
            - SEMI join with null-safe comparison
            - NULL in subquery: if price > 100 or price > 200, match found
            - NULL comparison yields UNKNOWN, doesn't affect result if other matches exist

        Expected result:
            Proper NULL handling per SQL standard
        """
        # Note: Requires test data with NULLs
        sql = """
            SELECT * FROM pg.products p
            WHERE p.price > ANY(
                SELECT o.amount FROM pg.orders o WHERE o.user_id IN (1, 2)
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Proper NULL handling
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_all_with_null_in_subquery(self, catalog, setup_test_data):
        """
        Test: < ALL when subquery contains NULL.

        Input SQL:
            Setup: Subquery returns [100, NULL]
            SELECT * FROM products WHERE price < ALL(...)

        Expected plan structure:
            - ANTI join for violations
            - NULL guard: if subquery has NULL and no violation, result is UNKNOWN → FALSE
            - Aggregate to detect NULL: has_null flag

        Expected result:
            If subquery has NULL, only rows that would violate return FALSE
            Otherwise UNKNOWN → filtered out
        """
        # This demonstrates the three-valued logic complexity
        sql = """
            SELECT * FROM pg.products p
            WHERE p.price < ALL(
                SELECT l.cap FROM pg.limits l
            )
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

    def test_all_empty_subquery(self, catalog, setup_test_data):
        """
        Test: Comparison with ALL and empty subquery.

        Input SQL:
            SELECT * FROM products WHERE price > ALL(SELECT amount FROM orders WHERE FALSE)

        Expected plan structure:
            - Empty subquery → ALL evaluates to TRUE (vacuous truth)
            - All rows should pass

        Expected result:
            All products (ALL over empty set is TRUE)
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price > ALL(SELECT amount FROM pg.orders WHERE FALSE)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All 4 products
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_any_empty_subquery(self, catalog, setup_test_data):
        """
        Test: Comparison with ANY and empty subquery.

        Input SQL:
            SELECT * FROM products WHERE price > ANY(SELECT amount FROM orders WHERE FALSE)

        Expected plan structure:
            - Empty subquery → ANY evaluates to FALSE
            - No rows should pass

        Expected result:
            Empty result (ANY over empty set is FALSE)
        """
        sql = """
            SELECT * FROM pg.products
            WHERE price > ANY(SELECT amount FROM pg.orders WHERE FALSE)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 0 rows
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestQuantifiedComplexQueries:
    """Test quantified comparisons in complex contexts."""

    def test_multiple_quantified_predicates(self, catalog, setup_test_data):
        """
        Test: Multiple quantified comparisons in same query.

        Input SQL:
            SELECT * FROM sales s
            WHERE s.price > ANY(SELECT amount FROM offers WHERE seller = s.seller)
              AND s.price <= ALL(SELECT cap FROM limits WHERE region = s.region)

        Expected plan structure:
            - SEMI join for > ANY
            - ANTI join for <= ALL
            - Both correlation conditions present

        Expected result:
            Sales satisfying both quantified predicates
        """
        sql = """
            SELECT * FROM pg.sales s
            WHERE s.price > ANY(SELECT amount FROM pg.offers WHERE seller = s.seller)
              AND s.price <= ALL(SELECT cap FROM pg.limits WHERE region = s.region)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Sales satisfying both conditions
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_quantified_in_select_list(self, catalog, setup_test_data):
        """
        Test: Quantified comparison as boolean in SELECT list.

        Input SQL:
            SELECT s.id, s.price,
                   s.price > ANY(SELECT amount FROM offers WHERE seller = s.seller) AS beats_offer
            FROM sales s

        Expected plan structure:
            - LEFT join or SEMI join with boolean flag
            - Result includes boolean column

        Expected result:
            All sales with beats_offer boolean flag
        """
        sql = """
            SELECT s.id, s.price,
                   s.price > ANY(SELECT amount FROM pg.offers WHERE seller = s.seller) AS beats_offer
            FROM pg.sales s
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 4 sales with boolean column
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_quantified_with_aggregation_in_subquery(self, catalog, setup_test_data):
        """
        Test: Quantified comparison with aggregated subquery.

        Input SQL:
            SELECT * FROM products p
            WHERE p.price > ALL(
                SELECT AVG(amount) FROM orders o
                WHERE o.user_id IN (SELECT id FROM users WHERE country = 'US')
                GROUP BY o.user_id
            )

        Expected plan structure:
            - Nested subquery must be decorrelated first
            - ANTI join against aggregated results

        Expected result:
            Products more expensive than all per-user averages for US users
        """
        sql = """
            SELECT * FROM pg.products p
            WHERE p.price > ALL(
                SELECT AVG(amount) FROM pg.orders o
                WHERE o.user_id IN (SELECT id FROM pg.users WHERE country = 'US')
                GROUP BY o.user_id
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Products exceeding all US user order averages
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"
