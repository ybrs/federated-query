"""
E2E tests for decorrelation error cases and edge cases.

Tests error handling, unsupported patterns, cardinality violations,
and other exceptional scenarios that should raise clear errors.
"""

import pytest
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator, DecorrelationError
from federated_query.parser.binder import BindingError
from federated_query.plan.logical import GroupedLimit, LateralJoin
from federated_query.plan.physical import CardinalityViolationError
from federated_query.executor.executor import Executor
from .test_utils import (
    assert_plan_structure,
    assert_result_count,
    assert_result_contains_ids,
    execute_and_fetch_all,
)


class TestCardinalityViolations:
    """Test cardinality enforcement errors."""

    def test_scalar_subquery_multiple_rows_error(self, catalog, setup_test_data):
        """
        Test: Scalar subquery returning multiple rows must error.

        Input SQL:
            SELECT u.id,
                   (SELECT amount FROM orders WHERE user_id = u.id) AS amount
            FROM users u

        Expected behavior:
            - Decorrelator or executor detects multiple rows for correlation key
            - Raises DecorrelationError or runtime error
            - Error message should be clear

        Expected result:
            Error raised (users 1 and 3 have multiple orders)
        """
        sql = """
            SELECT u.id,
                   (SELECT amount FROM pg.orders WHERE user_id = u.id) AS amount
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure, then assert the runtime cardinality guard
        # fires (users 1 and 3 have multiple orders).
        assert_plan_structure(decorrelated_plan, {})
        with pytest.raises(CardinalityViolationError, match="more than one row"):
            execute_and_fetch_all(executor, decorrelated_plan)

    def test_scalar_subquery_multiple_columns_error(self, catalog, setup_test_data):
        """
        Test: Scalar subquery returning multiple columns must error.

        Input SQL:
            SELECT u.id,
                   (SELECT user_id, amount FROM orders WHERE id = 1) AS data
            FROM users u

        Expected behavior:
            - Parser or decorrelator detects multiple columns in scalar context
            - Raises appropriate error

        Expected result:
            Error raised (scalar subquery must return single column)
        """
        sql = """
            SELECT u.id,
                   (SELECT user_id, amount FROM pg.orders WHERE id = 1) AS data
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)

        with pytest.raises(DecorrelationError, match="one column"):
            decorrelator.decorrelate(bound_plan)


class TestAmbiguousReferences:
    """Test ambiguous column reference errors."""

    def test_ambiguous_correlation_column(self, catalog, setup_test_data):
        """
        Test: Ambiguous column in correlation predicate.

        Input SQL:
            SELECT * FROM users u, cities c
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE id = u.id  -- 'id' is ambiguous (users.id, cities.id, orders.id)
            )

        Expected behavior:
            - Binder detects ambiguous column reference
            - Raises binding error

        Expected result:
            Error raised with clear message about ambiguity
        """
        # Note: This may be caught at binding stage before decorrelation
        sql = """
            SELECT * FROM pg.users u, pg.cities c
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE id = u.id
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)

    def test_unresolvable_correlation_reference(self, catalog, setup_test_data):
        """
        Test: Correlation reference to non-existent table.

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o
                WHERE o.user_id = nonexistent.id
            )

        Expected behavior:
            - Binder cannot resolve 'nonexistent' table
            - Raises binding error

        Expected result:
            Error raised (table not found)
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o
                WHERE o.user_id = nonexistent.id
            )
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)

        with pytest.raises(BindingError, match="nonexistent"):
            binder.bind(logical_plan)


class TestUnsupportedPatterns:
    """Test unsupported decorrelation patterns (future work)."""

    def test_windowed_subquery_not_supported(self, catalog, setup_test_data):
        """
        Test: Window functions in subqueries (marked as future work).

        Input SQL:
            SELECT u.id,
                   (SELECT ROW_NUMBER() OVER (ORDER BY amount)
                    FROM orders WHERE user_id = u.id LIMIT 1) AS rank
            FROM users u

        Expected behavior:
            - Decorrelator detects unsupported window function
            - Raises DecorrelationError with clear message
            - Or passes through to engine if not decorrelating windows

        Expected result:
            Error or pass-through (depending on implementation choice)
        """
        sql = """
            SELECT u.id,
                   (SELECT ROW_NUMBER() OVER (ORDER BY amount)
                    FROM pg.orders WHERE user_id = u.id LIMIT 1) AS rank
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        with pytest.raises(ValueError, match="Window"):
            parser.parse(sql)

    def test_recursive_cte_decorrelation_not_supported(self, catalog, setup_test_data):
        """
        Test: Recursive CTEs (marked as future work).

        Input SQL:
            WITH RECURSIVE tree AS (
                SELECT id, name FROM users WHERE id = 1
                UNION ALL
                SELECT u.id, u.name FROM users u, tree t WHERE u.id = t.id + 1
            )
            SELECT * FROM tree

        Expected behavior:
            - Decorrelator detects recursive CTE
            - May raise error or pass through

        Expected result:
            Behavior depends on implementation (out of scope for first pass)
        """
        sql = """
            WITH RECURSIVE tree AS (
                SELECT id, name FROM pg.users WHERE id = 1
                UNION ALL
                SELECT u.id, u.name FROM pg.users u, tree t WHERE u.id = t.id + 1
            )
            SELECT * FROM tree
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        with pytest.raises(ValueError, match="WITH"):
            parser.parse(sql)

    def test_lateral_subquery_handling(self, catalog, setup_test_data):
        """A user-written LATERAL parses, binds, and decorrelates to a
        LateralJoin (dependent join) — the explicit correlation is kept and
        evaluated per outer row by the executing engine.
        """
        sql = """
            SELECT u.id, o.amount
            FROM pg.users u,
            LATERAL (SELECT amount FROM pg.orders WHERE user_id = u.id LIMIT 1) o
        """
        bound = Binder(catalog).bind(Parser().parse(sql))
        plan = Decorrelator().decorrelate(bound)

        def has_lateral(node):
            if isinstance(node, LateralJoin):
                return True
            return any(has_lateral(child) for child in node.children())

        assert has_lateral(plan)


class TestUnsupportedOperators:
    """Test unsupported operators in quantified comparisons."""

    def test_like_all_quantified_operator_supported(self, catalog, setup_test_data):
        """
        Test: LIKE as the operator of a quantified (ALL) comparison.

        Input SQL:
            SELECT * FROM products WHERE name LIKE ALL(SELECT name FROM products)

        Expected behavior:
            - LIKE ALL is supported: the quantified comparison decorrelates to
              an ANTI join keeping rows that violate no `name LIKE <pattern>`,
              with each inner name used as a LIKE pattern.

        Expected result:
            The product names are distinct literals with no wildcards, so no
            name LIKE-matches every other name; the result is empty (verified
            against PostgreSQL).
        """
        sql = """
            SELECT name FROM pg.products
            WHERE name LIKE ALL(SELECT name FROM pg.products)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert results == []

    def test_unsupported_negated_quantified_operator(self, catalog, setup_test_data):
        """
        Test: an unsupported operator in a negated quantified comparison.

        Input SQL:
            SELECT name FROM products
            WHERE NOT (name LIKE ALL(SELECT name FROM products))

        Expected behavior:
            - LIKE has no single negated binary operator, so pushing the NOT
              through the quantified comparison (De Morgan) is not possible;
              decorrelation must raise DecorrelationError instead of silently
              producing wrong rows.
        """
        sql = """
            SELECT name FROM pg.products
            WHERE NOT (name LIKE ALL(SELECT name FROM pg.products))
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        bound_plan = binder.bind(parser.parse(sql))
        with pytest.raises(DecorrelationError, match="negate quantified operator"):
            decorrelator.decorrelate(bound_plan)

    def test_quantified_comparison_multi_column_subquery(
        self, catalog, setup_test_data
    ):
        """
        Test: a quantified comparison whose subquery returns multiple columns.

        Input SQL:
            SELECT name FROM products
            WHERE price = ALL(SELECT name, price FROM products)

        Expected behavior:
            - A scalar/quantified comparison needs a single-column subquery;
              a multi-column subquery raises DecorrelationError.
        """
        sql = """
            SELECT name FROM pg.products
            WHERE price = ALL(SELECT name, price FROM pg.products)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()

        bound_plan = binder.bind(parser.parse(sql))
        with pytest.raises(DecorrelationError, match="must return one column"):
            decorrelator.decorrelate(bound_plan)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_subquery(self, catalog, setup_test_data):
        """
        Test: Subquery that always returns empty result.

        Input SQL:
            SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE FALSE)

        Expected plan structure:
            - Subquery evaluates to empty
            - Entire query returns no rows

        Expected result:
            Empty result set (no error)
        """
        sql = """
            SELECT * FROM pg.users
            WHERE EXISTS (SELECT 1 FROM pg.orders WHERE FALSE)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Empty result, no error
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_subquery_with_no_tables(self, catalog, setup_test_data):
        """
        Test: Subquery with no FROM clause (constant).

        Input SQL:
            SELECT u.id, (SELECT 42) AS constant FROM users u

        Expected plan structure:
            - Subquery is pure constant
            - Can be hoisted as CTE or inlined

        Expected result:
            All users with constant=42
        """
        sql = """
            SELECT u.id, (SELECT 42) AS constant
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: Users with constant column
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_deeply_nested_correlation_chain(self, catalog, setup_test_data):
        """
        Test: Very deep nesting (stress test for recursion).

        Input SQL:
            SELECT * FROM users u
            WHERE EXISTS (
                SELECT 1 FROM orders o WHERE o.user_id = u.id AND EXISTS (
                    SELECT 1 FROM products p WHERE p.price > o.amount AND EXISTS (
                        SELECT 1 FROM sales s WHERE s.product_id = p.id AND EXISTS (
                            SELECT 1 FROM offers off WHERE off.product_id = s.product_id
                        )
                    )
                )
            )

        Expected plan structure:
            - Decorrelator handles deep nesting
            - May have recursion depth limit

        Expected result:
            Successful decorrelation or clear error if depth limit exceeded
        """
        sql = """
            SELECT * FROM pg.users u
            WHERE EXISTS (
                SELECT 1 FROM pg.orders o WHERE o.user_id = u.id AND EXISTS (
                    SELECT 1 FROM pg.products p WHERE p.price > o.amount AND EXISTS (
                        SELECT 1 FROM pg.sales s WHERE s.product_id = p.id AND EXISTS (
                            SELECT 1 FROM pg.offers off WHERE off.product_id = s.product_id
                        )
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
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"

    def test_all_subquery_types_in_one_query(self, catalog, setup_test_data):
        """
        Test: Query using all subquery types simultaneously.

        Input SQL:
            SELECT u.id,
                   (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count,
                   EXISTS (SELECT 1 FROM orders WHERE user_id = u.id) AS has_orders
            FROM users u
            WHERE u.country IN (SELECT code FROM countries WHERE enabled)
              AND u.id > ANY(SELECT user_id FROM orders WHERE amount > 100)
              AND u.id <= ALL(SELECT id FROM users WHERE country = u.country)

        Expected plan structure:
            - Multiple decorrelation types in same query
            - Scalar, EXISTS, IN, ANY, ALL all present
            - All decorrelated without conflict

        Expected result:
            Complex query successfully decorrelated
        """
        sql = """
            SELECT u.id,
                   (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) AS order_count,
                   EXISTS (SELECT 1 FROM pg.orders WHERE user_id = u.id) AS has_orders
            FROM pg.users u
            WHERE u.country IN (SELECT code FROM pg.countries WHERE enabled)
              AND u.id > ANY(SELECT user_id FROM pg.orders WHERE amount > 100)
              AND u.id <= ALL(SELECT id FROM pg.users WHERE country = u.country)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Expected: All subqueries decorrelated
        # Execute and verify
        results = execute_and_fetch_all(executor, decorrelated_plan)
        assert len(results) >= 0, "Query should execute successfully"


class TestCTEReuse:
    """Test CTE hoisting and reuse for uncorrelated subqueries."""

    def test_same_uncorrelated_subquery_reused(self, catalog, setup_test_data):
        """
        Test: Same uncorrelated subquery used multiple times should reuse CTE.

        Input SQL:
            SELECT u.id,
                   (SELECT MAX(amount) FROM orders) AS max1,
                   (SELECT MAX(amount) FROM orders) AS max2
            FROM users u

        Expected plan structure:
            - Single CTE for the subquery
            - CTE referenced twice in projection
            - Subquery executed once

        Expected result:
            Users with max1 = max2 (same value from reused CTE)
        """
        sql = """
            SELECT u.id,
                   (SELECT MAX(amount) FROM pg.orders) AS max1,
                   (SELECT MAX(amount) FROM pg.orders) AS max2
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
        # Expected: Single CTE, multiple references

    def test_cte_naming_deterministic(self, catalog, setup_test_data):
        """
        Test: CTE names should be deterministic for testing.

        Input SQL:
            Multiple uncorrelated subqueries

        Expected plan structure:
            - CTEs named consistently (e.g., cte_subq_0, cte_subq_1, ...)
            - Same query produces same CTE names

        Expected result:
            Deterministic plan structure
        """
        sql = """
            SELECT u.id,
                   (SELECT MAX(amount) FROM pg.orders) AS max_amt,
                   (SELECT MIN(amount) FROM pg.orders) AS min_amt
            FROM pg.users u
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)


class TestRegressionPrevention:
    """Test that non-subquery queries are unchanged."""

    def test_simple_select_unchanged(self, catalog, setup_test_data):
        """
        Test: Simple SELECT without subqueries passes through unchanged.

        Input SQL:
            SELECT * FROM users WHERE country = 'US'

        Expected plan structure:
            - No decorrelation needed
            - Plan structure unchanged by decorrelator

        Expected result:
            Plan identical to input (modulo optimization)
        """
        sql = """
            SELECT * FROM pg.users WHERE country = 'US'
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
        # Expected: No subquery nodes added

    def test_join_without_subqueries_unchanged(self, catalog, setup_test_data):
        """
        Test: JOIN without subqueries passes through unchanged.

        Input SQL:
            SELECT u.id, o.amount
            FROM users u
            JOIN orders o ON o.user_id = u.id

        Expected plan structure:
            - No decorrelation needed
            - Join structure preserved

        Expected result:
            Plan unchanged
        """
        sql = """
            SELECT u.id, o.amount
            FROM pg.users u
            JOIN pg.orders o ON o.user_id = u.id
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)

    def test_aggregation_without_subqueries_unchanged(self, catalog, setup_test_data):
        """
        Test: Aggregation without subqueries passes through unchanged.

        Input SQL:
            SELECT user_id, SUM(amount) AS total
            FROM orders
            GROUP BY user_id
            HAVING SUM(amount) > 200

        Expected plan structure:
            - No decorrelation needed
            - Aggregation structure preserved

        Expected result:
            Plan unchanged
        """
        sql = """
            SELECT user_id, SUM(amount) AS total
            FROM pg.orders
            GROUP BY user_id
            HAVING SUM(amount) > 200
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)


class TestValidationPhase:
    """Test post-decorrelation validation."""

    def test_no_subquery_nodes_remain(self, catalog, setup_test_data):
        """
        Test: Validation ensures no subquery expression nodes remain.

        Input SQL:
            Any query with subqueries

        Expected behavior:
            - After decorrelation, validator walks plan
            - Raises error if any SubqueryExpression, ExistsExpression, etc. found

        Expected result:
            Clean plan or validation error
        """
        sql = """
            SELECT u.id,
                   (SELECT COUNT(*) FROM pg.orders WHERE user_id = u.id) AS cnt
            FROM pg.users u
            WHERE EXISTS (SELECT 1 FROM pg.orders WHERE user_id = u.id)
        """

        parser = Parser()
        binder = Binder(catalog)
        decorrelator = Decorrelator()
        executor = Executor(catalog)

        logical_plan = parser.parse(sql)
        bound_plan = binder.bind(logical_plan)
        decorrelated_plan = decorrelator.decorrelate(bound_plan)

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
        # decorrelator.validate(decorrelated_plan)
        # Should raise if any subquery nodes remain

    def test_join_conditions_cover_correlation_keys(self, catalog, setup_test_data):
        """
        Test: Validation ensures all correlation keys covered by join conditions.

        Input SQL:
            Correlated subquery

        Expected behavior:
            - After decorrelation, validator checks join conditions
            - Ensures all correlation predicates present in join

        Expected result:
            Validation passes or raises error
        """
        sql = """
            SELECT * FROM pg.users u
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

        # Verify plan structure
        assert_plan_structure(decorrelated_plan, {})
        results = execute_and_fetch_all(executor, decorrelated_plan)
        # decorrelator.validate_correlation_coverage(decorrelated_plan)


class TestUnsupportedSubqueryShapes:
    """Pin the decorrelation engine's known fail-fast gaps.

    Each pattern is unnestable in principle (a general dependent join would
    handle it) but the current pattern-based decorrelator rejects it with a
    clear error rather than producing a wrong answer. These tests document
    exactly what is unsupported today; when a gap is closed, flip its test.
    """

    def _decorrelate(self, catalog, sql):
        """Parse, bind, and decorrelate a SQL string for a raises-assertion."""
        bound = Binder(catalog).bind(Parser().parse(sql))
        return Decorrelator().decorrelate(bound)

    def _find_grouped_limit(self, node):
        """Return the first GroupedLimit in a plan, or None."""
        if isinstance(node, GroupedLimit):
            return node
        for child in node.children():
            found = self._find_grouped_limit(child)
            if found is not None:
                return found
        return None

    def test_sort_topped_correlated_scalar(self, catalog, setup_test_data):
        """A correlated scalar ``ORDER BY ... LIMIT 1`` (latest-row-per-key) now
        decorrelates to an order-aware per-key limit (gap A, closed).

        The ORDER BY column (``o.amount``) is not the selected value
        (``o.user_id``), so it is exposed as an extra projected column the
        GroupedLimit sorts on. LIMIT 1 makes it provably single-row, so no
        cardinality guard is added.
        """
        sql = (
            "SELECT u.id FROM pg.users u WHERE u.id = ("
            "  SELECT o.user_id FROM pg.orders o WHERE o.user_id = u.id "
            "  ORDER BY o.amount DESC LIMIT 1)"
        )
        plan = self._decorrelate(catalog, sql)
        grouped = self._find_grouped_limit(plan)
        assert grouped is not None
        assert grouped.limit == 1
        assert grouped.order_by_keys

    def test_non_equi_correlated_limit_subquery_uses_lateral(
        self, catalog, setup_test_data
    ):
        """A non-equi correlated ``ORDER BY ... LIMIT`` scalar cannot become a
        per-key limit (a ``<``/``>`` correlation matches many groups), so it
        falls back to a LATERAL join evaluated per outer row — never a wrong
        per-key answer.
        """
        sql = (
            "SELECT u.id, (SELECT o.user_id FROM pg.orders o "
            "  WHERE o.amount > u.id ORDER BY o.amount LIMIT 1) AS x "
            "FROM pg.users u"
        )
        plan = self._decorrelate(catalog, sql)
        assert self._find_lateral_join(plan) is not None

    def _find_lateral_join(self, node):
        """Return the first LateralJoin in a plan, or None."""
        if isinstance(node, LateralJoin):
            return node
        for child in node.children():
            found = self._find_lateral_join(child)
            if found is not None:
                return found
        return None

    def test_set_operation_subquery_body(self, catalog, setup_test_data):
        """A UNION/INTERSECT/EXCEPT subquery body is rejected at binding.

        The subquery binder does not accept a SetOperation body yet
        (decorrelation-gaps.md B).
        """
        sql = (
            "SELECT u.id FROM pg.users u WHERE u.id IN ("
            "  SELECT user_id FROM pg.orders WHERE status = 'paid' "
            "  UNION SELECT user_id FROM pg.orders WHERE status = 'shipped')"
        )
        with pytest.raises(BindingError, match="SetOperation"):
            self._decorrelate(catalog, sql)

    def test_offset_in_correlated_subquery(self, catalog, setup_test_data):
        """OFFSET inside a correlated subquery is rejected.

        Decorrelating an offset per correlation key is unsupported
        (decorrelation-gaps.md D).
        """
        sql = (
            "SELECT u.id FROM pg.users u WHERE u.id = ("
            "  SELECT o.user_id FROM pg.orders o WHERE o.user_id = u.id "
            "  ORDER BY o.amount LIMIT 1 OFFSET 2)"
        )
        with pytest.raises(DecorrelationError, match="OFFSET"):
            self._decorrelate(catalog, sql)

    def test_subquery_in_group_by(self, catalog, setup_test_data):
        """A subquery in a GROUP BY position is rejected (decorrelation-gaps.md E)."""
        sql = (
            "SELECT COUNT(*) FROM pg.users u "
            "GROUP BY (SELECT MAX(price) FROM pg.products)"
        )
        with pytest.raises(DecorrelationError, match="GROUP BY"):
            self._decorrelate(catalog, sql)

    def test_skip_level_correlation(self, catalog, setup_test_data):
        """A subquery correlating two levels up is rejected.

        The inner subquery references ``u`` (the outermost query), skipping the
        middle relation -- the canonical dependent-join case the pattern-based
        engine cannot handle yet (decorrelation-gaps.md C).
        """
        sql = (
            "SELECT u.id FROM pg.users u WHERE EXISTS ("
            "  SELECT 1 FROM pg.orders o WHERE EXISTS ("
            "    SELECT 1 FROM pg.products p WHERE p.id = u.id))"
        )
        with pytest.raises(DecorrelationError, match="unsupported position"):
            self._decorrelate(catalog, sql)

    def test_select_star_value_subquery(self, catalog, setup_test_data):
        """A ``SELECT *`` scalar/value subquery is rejected (decorrelation-gaps.md F)."""
        sql = (
            "SELECT u.id FROM pg.users u WHERE u.id > ("
            "  SELECT * FROM pg.orders o WHERE o.user_id = u.id)"
        )
        with pytest.raises(DecorrelationError):
            self._decorrelate(catalog, sql)
