"""
E2E tests for scalar subquery decorrelation.

Tests scalar subqueries in SELECT lists, WHERE clauses, HAVING clauses,
and join conditions. Focuses on cardinality enforcement and NULL handling.
"""
import pytest
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator


class TestUncorrelatedScalarInSelect:
    """Test uncorrelated scalar subqueries in SELECT list."""

    def test_uncorrelated_scalar_aggregate(self, catalog, setup_test_data):
        """
        Test: Uncorrelated scalar subquery with aggregation in SELECT.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) FROM orders o) AS max_order_amount
            FROM users u

        Expected plan structure:
            - Subquery hoisted as CTE: Aggregate(MAX(amount), Scan(orders))
            - CROSS join users with CTE (CTE produces single row)
            - Projection includes CTE column

        Expected result:
            All 5 users with max_order_amount = 500.00
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) FROM pg.orders o) AS max_order_amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify: CTE should be created and cross-joined
        # Expected: 5 rows, all with max_order_amount = 500.00
        # TODO: Add executor integration

    def test_uncorrelated_scalar_single_row(self, catalog, setup_test_data):
        """
        Test: Uncorrelated scalar subquery returning single row without aggregation.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT code FROM countries WHERE code = 'US') AS us_code
            FROM users u

        Expected plan structure:
            - Subquery hoisted as CTE with enforced single row
            - CROSS join with CTE
            - Must verify subquery returns at most one row

        Expected result:
            All 5 users with us_code = 'US'
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT code FROM pg.countries WHERE code = 'US') AS us_code
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with us_code = 'US'
        # TODO: Add executor integration

    def test_uncorrelated_scalar_returns_null(self, catalog, setup_test_data):
        """
        Test: Uncorrelated scalar subquery returning NULL.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT code FROM countries WHERE code = 'XX') AS missing_code
            FROM users u

        Expected plan structure:
            - Subquery returns empty result → NULL
            - CROSS join preserves NULL semantics

        Expected result:
            All 5 users with missing_code = NULL
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT code FROM pg.countries WHERE code = 'XX') AS missing_code
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with missing_code = NULL
        # TODO: Add executor integration

    def test_multiple_uncorrelated_scalars(self, catalog, setup_test_data):
        """
        Test: Multiple uncorrelated scalar subqueries in same SELECT.

        Input SQL:
            SELECT u.id,
                   (SELECT MAX(amount) FROM orders) AS max_amt,
                   (SELECT MIN(amount) FROM orders) AS min_amt,
                   (SELECT COUNT(*) FROM orders) AS order_count
            FROM users u

        Expected plan structure:
            - Multiple CTEs (or single CTE with multiple aggregates if optimized)
            - CROSS joins with each CTE
            - All subqueries execute once

        Expected result:
            All 5 users with max_amt=500, min_amt=50, order_count=6
        """
        sql = """
            SELECT u.id,
                   (SELECT MAX(amount) FROM pg.orders) AS max_amt,
                   (SELECT MIN(amount) FROM pg.orders) AS min_amt,
                   (SELECT COUNT(*) FROM pg.orders) AS order_count
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with computed values
        # TODO: Add executor integration


class TestCorrelatedScalarInSelect:
    """Test correlated scalar subqueries in SELECT list."""

    def test_correlated_scalar_aggregate_basic(self, catalog, setup_test_data):
        """
        Test: Correlated scalar subquery with aggregation.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT SUM(o.amount) FROM orders o WHERE o.user_id = u.id) AS total_orders
            FROM users u

        Expected plan structure:
            - LEFT join with aggregated subquery
            - Subquery: Aggregate(key=user_id, agg=SUM(amount))
            - Join on u.id = aggregated.user_id
            - NULL preserved for users without orders

        Expected result:
            User 1: 300.00, User 2: 150.00, User 3: 350.00, User 4: NULL, User 5: 500.00
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT SUM(o.amount) FROM pg.orders o WHERE o.user_id = u.id) AS total_orders
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify: LEFT join preserves users without orders
        # Expected: 5 rows with proper totals
        # TODO: Add executor integration

    def test_correlated_scalar_count(self, catalog, setup_test_data):
        """
        Test: Correlated scalar subquery with COUNT.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
            FROM users u

        Expected plan structure:
            - LEFT join with COUNT aggregation
            - COUNT returns 0 for users without orders (not NULL)

        Expected result:
            User 1: 2, User 2: 1, User 3: 2, User 4: 0, User 5: 1
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM pg.orders o WHERE o.user_id = u.id) AS order_count
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with counts (0 for user without orders)
        # TODO: Add executor integration

    def test_correlated_scalar_multiple_correlation_keys(self, catalog, setup_test_data):
        """
        Test: Correlated scalar with multiple correlation predicates.

        Input SQL:
            SELECT u.id, u.name, u.country,
                   (SELECT COUNT(*) FROM cities c
                    WHERE c.country = u.country AND c.population > 1000000) AS large_cities
            FROM users u

        Expected plan structure:
            - LEFT join with aggregation grouped by correlation keys
            - Join on u.country = c.country
            - Additional filter c.population > 1000000 in subquery

        Expected result:
            Count of large cities per user's country
        """
        sql = """
            SELECT u.id, u.name, u.country,
                   (SELECT COUNT(*) FROM pg.cities c
                    WHERE c.country = u.country AND c.population > 1000000) AS large_cities
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with count of large cities in their country
        # TODO: Add executor integration

    def test_correlated_scalar_with_outer_reference_in_projection(self, catalog, setup_test_data):
        """
        Test: Correlated scalar that references outer column in projection.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) + u.id FROM orders o WHERE o.user_id = u.id) AS adjusted_max
            FROM users u

        Expected plan structure:
            - LEFT join with aggregation
            - Outer reference (u.id) used in projection after join
            - Expression: MAX(o.amount) + u.id

        Expected result:
            Users with adjusted_max = MAX(orders) + user_id
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT MAX(o.amount) + u.id FROM pg.orders o WHERE o.user_id = u.id) AS adjusted_max
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with computed values
        # TODO: Add executor integration


class TestScalarInPredicates:
    """Test scalar subqueries in WHERE/HAVING clauses."""

    def test_scalar_in_where_uncorrelated(self, catalog, setup_test_data):
        """
        Test: Uncorrelated scalar subquery in WHERE clause.

        Input SQL:
            SELECT * FROM orders
            WHERE amount > (SELECT AVG(amount) FROM orders)

        Expected plan structure:
            - Subquery hoisted as CTE
            - CROSS join with CTE
            - Filter: amount > cte.avg_amount

        Expected result:
            Orders with amount above average
        """
        sql = """
            SELECT * FROM pg.orders
            WHERE amount > (SELECT AVG(amount) FROM pg.orders)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Orders above average (average ~216.67)
        # TODO: Add executor integration

    def test_scalar_in_where_correlated(self, catalog, setup_test_data):
        """
        Test: Correlated scalar subquery in WHERE clause.

        Input SQL:
            SELECT * FROM orders o1
            WHERE o1.amount > (
                SELECT AVG(o2.amount) FROM orders o2
                WHERE o2.user_id = o1.user_id
            )

        Expected plan structure:
            - LEFT join with aggregated subquery
            - Join on o1.user_id = aggregated.user_id
            - Filter: o1.amount > aggregated.avg_amount

        Expected result:
            Orders above per-user average
        """
        sql = """
            SELECT * FROM pg.orders o1
            WHERE o1.amount > (
                SELECT AVG(o2.amount) FROM pg.orders o2
                WHERE o2.user_id = o1.user_id
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Orders above their user's average
        # TODO: Add executor integration

    def test_scalar_comparison_null_safe(self, catalog, setup_test_data):
        """
        Test: Scalar subquery comparison with NULL handling.

        Input SQL:
            SELECT * FROM users u
            WHERE u.id = (SELECT user_id FROM orders WHERE status = 'pending')

        Expected plan structure:
            - LEFT join with subquery
            - Comparison must handle NULL when subquery returns empty
            - Should use null-safe comparison if required

        Expected result:
            Users matching the pending order user_id
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE u.id = (SELECT user_id FROM pg.orders WHERE status = 'pending')
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: User 2 (Bob has pending order)
        # TODO: Add executor integration

    def test_scalar_in_having_clause(self, catalog, setup_test_data):
        """
        Test: Scalar subquery in HAVING clause.

        Input SQL:
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
            HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM orders)

        Expected plan structure:
            - Subquery hoisted as CTE
            - Aggregation on orders
            - HAVING filter references CTE value

        Expected result:
            User groups with total > 2 * avg order amount
        """
        sql = """
            SELECT user_id, SUM(amount) AS total
            FROM pg.orders
            GROUP BY user_id
            HAVING SUM(amount) > (SELECT AVG(amount) * 2 FROM pg.orders)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: User groups with high totals
        # TODO: Add executor integration


class TestScalarCardinalityEnforcement:
    """Test cardinality enforcement for scalar subqueries."""

    def test_scalar_multiple_rows_should_error(self, catalog, setup_test_data):
        """
        Test: Scalar subquery returning multiple rows must raise error.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT amount FROM orders WHERE user_id = u.id) AS amount
            FROM users u

        Expected plan structure:
            - During decorrelation or execution, detect multiple rows per correlation key
            - Raise DecorrelationError or runtime error

        Expected result:
            Error raised (users 1 and 3 have multiple orders)
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT amount FROM pg.orders WHERE user_id = u.id) AS amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)

        # This should raise an error during decorrelation or execution
        # TODO: Verify proper error is raised
        # with pytest.raises(DecorrelationError):
        #     decorrelated_plan = decorrelator.decorrelate(bound_plan)

    def test_scalar_with_limit_one(self, catalog, setup_test_data):
        """
        Test: Scalar subquery with LIMIT 1 to enforce single row.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT amount FROM orders WHERE user_id = u.id LIMIT 1) AS first_amount
            FROM users u

        Expected plan structure:
            - Subquery includes LIMIT 1
            - LEFT join with limited subquery
            - No cardinality error since LIMIT enforces single row

        Expected result:
            Users with first order amount (arbitrary order)
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT amount FROM pg.orders WHERE user_id = u.id LIMIT 1) AS first_amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with first_amount
        # TODO: Add executor integration

    def test_scalar_aggregation_always_single_row(self, catalog, setup_test_data):
        """
        Test: Scalar subquery with aggregation always returns single row.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT MAX(amount) FROM orders WHERE user_id = u.id) AS max_amount
            FROM users u

        Expected plan structure:
            - Aggregation guarantees single row per group
            - No cardinality check needed
            - LEFT join preserves NULL for users without orders

        Expected result:
            Users with their max order amount
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT MAX(amount) FROM pg.orders WHERE user_id = u.id) AS max_amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: 5 rows with max amounts
        # TODO: Add executor integration


class TestScalarComplexQueries:
    """Test scalar subqueries in complex contexts."""

    def test_scalar_in_arithmetic_expression(self, catalog, setup_test_data):
        """
        Test: Scalar subquery used in arithmetic expression.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM orders WHERE user_id = u.id) * 10 AS points
            FROM users u

        Expected plan structure:
            - LEFT join with COUNT
            - Arithmetic expression in projection: count * 10

        Expected result:
            Users with points = order_count * 10
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) * 10 AS points
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with computed points
        # TODO: Add executor integration

    def test_scalar_in_case_expression(self, catalog, setup_test_data):
        """
        Test: Scalar subquery in CASE expression.

        Input SQL:
            SELECT u.id, u.name,
                   CASE
                       WHEN (SELECT COUNT(*) FROM orders WHERE user_id = u.id) > 1 THEN 'frequent'
                       WHEN (SELECT COUNT(*) FROM orders WHERE user_id = u.id) = 1 THEN 'occasional'
                       ELSE 'none'
                   END AS customer_type
            FROM users u

        Expected plan structure:
            - Subquery decorrelated and reused (or executed once if same)
            - CASE expression uses decorrelated result

        Expected result:
            Users with customer_type classification
        """
        sql = """
            SELECT u.id, u.name,
                   CASE
                       WHEN (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) > 1 THEN 'frequent'
                       WHEN (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) = 1 THEN 'occasional'
                       ELSE 'none'
                   END AS customer_type
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users classified by order count
        # TODO: Add executor integration

    def test_nested_scalar_subqueries(self, catalog, setup_test_data):
        """
        Test: Scalar subquery containing another scalar subquery.

        Input SQL:
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM orders o
                    WHERE o.user_id = u.id
                      AND o.amount > (SELECT AVG(amount) FROM orders)) AS above_avg_orders
            FROM users u

        Expected plan structure:
            - Inner scalar (AVG) decorrelated first → CTE
            - Outer scalar decorrelated with reference to CTE
            - Multiple joins in final plan

        Expected result:
            Users with count of their above-average orders
        """
        sql = """
            SELECT u.id, u.name,
                   (SELECT COUNT(*) FROM pg.orders o
                    WHERE o.user_id = u.id
                      AND o.amount > (SELECT AVG(amount) FROM pg.orders)) AS above_avg_orders
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with count of above-average orders
        # TODO: Add executor integration
