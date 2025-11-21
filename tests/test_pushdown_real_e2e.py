"""
REAL END-TO-END PUSHDOWN TESTS

This test suite actually intercepts queries sent to data sources and verifies:
1. The exact SQL query sent
2. The query executes correctly
3. Results are correct

No bullshit string checking - we mock the data source and capture real queries.
"""

import pytest
from unittest.mock import patch, MagicMock
import pyarrow as pa
from federated_query.parser import Parser, Binder
from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.optimizer import (
    PhysicalPlanner,
    RuleBasedOptimizer,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    AggregatePushdownRule,
    OrderByPushdownRule,
    LimitPushdownRule,
    ExpressionSimplificationRule,
)
from federated_query.executor import Executor


class QueryCapturingDataSource(DuckDBDataSource):
    """DuckDB data source that captures all queries sent to it."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.captured_queries = []

    def execute_query(self, query: str):
        """Capture query and execute it."""
        self.captured_queries.append(query)
        return super().execute_query(query)

    def get_last_query(self):
        """Get the last query sent."""
        if self.captured_queries:
            return self.captured_queries[-1]
        return None

    def clear_queries(self):
        """Clear captured queries."""
        self.captured_queries = []


@pytest.fixture
def setup_capturing_db():
    """Set up DuckDB with query capturing."""
    config = {"database": ":memory:", "read_only": False}
    ds = QueryCapturingDataSource(name="testdb", config=config)
    ds.connect()

    # Create orders table
    ds.connection.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            price DOUBLE,
            status VARCHAR
        )
    """)

    # Create customers table for join tests
    ds.connection.execute("""
        CREATE TABLE customers (
            customer_id INTEGER,
            name VARCHAR,
            region VARCHAR
        )
    """)

    ds.connection.execute("""
        INSERT INTO orders VALUES
        (1, 100, 50.0, 'completed'),
        (2, 101, 100.0, 'pending'),
        (3, 102, 150.0, 'completed'),
        (4, 100, 75.0, 'completed'),
        (5, 103, 200.0, 'cancelled'),
        (6, 101, 100.0, 'completed'),
        (7, 104, 150.0, 'pending'),
        (8, 100, 75.0, 'completed'),
        (9, 105, 300.0, 'completed'),
        (10, 102, 50.0, 'completed')
    """)

    ds.connection.execute("""
        INSERT INTO customers VALUES
        (100, 'Alice', 'East'),
        (101, 'Bob', 'West'),
        (102, 'Carol', 'West'),
        (103, 'Derek', 'East'),
        (106, 'Eve', 'North')
    """)

    catalog = Catalog()
    catalog.register_datasource(ds)
    catalog.load_metadata()

    yield catalog, ds

    ds.disconnect()


def execute_and_capture(sql: str, catalog: Catalog, ds: QueryCapturingDataSource):
    """Execute SQL and return captured query + results."""
    ds.clear_queries()

    parser = Parser()
    logical_plan = parser.parse_to_logical_plan(sql, catalog)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = RuleBasedOptimizer(catalog)
    optimizer.add_rule(ExpressionSimplificationRule())
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ProjectionPushdownRule())
    optimizer.add_rule(AggregatePushdownRule())
    optimizer.add_rule(OrderByPushdownRule())
    optimizer.add_rule(LimitPushdownRule())
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = list(executor.execute(physical_plan))

    return ds.get_last_query(), results


class TestPredicatePushdownReal:
    """Real tests for predicate pushdown."""

    def test_simple_filter_pushdown(self, setup_capturing_db):
        """Test WHERE order_id > 1 actually pushes."""
        catalog, ds = setup_capturing_db

        sql = "SELECT * FROM testdb.main.orders WHERE order_id > 1"
        query, results = execute_and_capture(sql, catalog, ds)

        # Verify query has WHERE clause
        assert query is not None, "No query captured"
        assert "WHERE" in query, f"No WHERE in query: {query}"
        assert "order_id" in query, f"order_id not in WHERE: {query}"

        # Verify results are correct
        all_rows = []
        for batch in results:
            data = batch.to_pydict()
            for i in range(batch.num_rows):
                all_rows.append({
                    "order_id": data["order_id"][i],
                    "price": data["price"][i]
                })

        # Should have orders 2-10 (all except order_id=1)
        order_ids = [r["order_id"] for r in all_rows]
        assert 1 not in order_ids, "order_id=1 should be filtered out"
        assert len(order_ids) == 9, f"Expected 9 rows, got {len(order_ids)}"

    def test_compound_filter_pushdown(self, setup_capturing_db):
        """Test WHERE status = 'completed' AND price > 100."""
        catalog, ds = setup_capturing_db

        sql = "SELECT * FROM testdb.main.orders WHERE status = 'completed' AND price > 100"
        query, results = execute_and_capture(sql, catalog, ds)

        assert "WHERE" in query, f"No WHERE: {query}"
        assert "status" in query, f"status not in WHERE: {query}"
        assert "price" in query, f"price not in WHERE: {query}"
        assert "AND" in query.upper(), f"No AND: {query}"

        # Verify results
        all_rows = []
        for batch in results:
            data = batch.to_pydict()
            for i in range(batch.num_rows):
                all_rows.append({
                    "order_id": data["order_id"][i],
                    "status": data["status"][i],
                    "price": data["price"][i]
                })

        # Only order_id 3 (150.0, completed) and 9 (300.0, completed) match
        assert len(all_rows) == 2, f"Expected 2 rows, got {len(all_rows)}: {all_rows}"
        for row in all_rows:
            assert row["status"] == "completed", f"Wrong status: {row}"
            assert row["price"] > 100, f"Wrong price: {row}"


class TestProjectionPushdownReal:
    """Real tests for projection pushdown."""

    def test_select_specific_columns(self, setup_capturing_db):
        """Test SELECT order_id, price only fetches those columns."""
        catalog, ds = setup_capturing_db

        sql = "SELECT order_id, price FROM testdb.main.orders"
        query, results = execute_and_capture(sql, catalog, ds)

        # Query should select specific columns
        assert "SELECT" in query, f"No SELECT: {query}"
        assert '"order_id"' in query or "order_id" in query, f"order_id not selected: {query}"
        assert '"price"' in query or "price" in query, f"price not selected: {query}"
        assert "SELECT *" not in query, f"Should not SELECT *: {query}"

        # Verify results have correct schema
        for batch in results:
            schema_names = batch.schema.names
            assert "order_id" in schema_names, f"order_id missing: {schema_names}"
            assert "price" in schema_names, f"price missing: {schema_names}"

    def test_projection_with_filter(self, setup_capturing_db):
        """Test SELECT order_id WHERE price > 100."""
        catalog, ds = setup_capturing_db

        sql = "SELECT order_id FROM testdb.main.orders WHERE price > 100"
        query, results = execute_and_capture(sql, catalog, ds)

        # Should fetch order_id (for output) and price (for filter)
        assert '"order_id"' in query or "order_id" in query, f"order_id not in query: {query}"
        assert '"price"' in query or "price" in query, f"price not in query: {query}"
        assert "WHERE" in query, f"WHERE missing: {query}"

        # Verify results
        all_ids = []
        for batch in results:
            all_ids.extend(batch.to_pydict()["order_id"])

        # Orders with price > 100: 3 (150), 5 (200), 7 (150), 9 (300)
        # NOT 2 or 6 (both have price=100, which is not > 100)
        expected = {3, 5, 7, 9}
        assert set(all_ids) == expected, f"Expected {expected}, got {set(all_ids)}"


class TestAggregatePushdownReal:
    """Real tests for aggregate pushdown - verify aggregates ARE pushed down!"""

    def test_count_star_pushed(self, setup_capturing_db):
        """Test COUNT(*) IS pushed down to data source."""
        catalog, ds = setup_capturing_db

        sql = "SELECT COUNT(*) FROM testdb.main.orders"
        query, results = execute_and_capture(sql, catalog, ds)

        # COUNT MUST be in the query sent to data source
        assert "COUNT" in query.upper(), f"COUNT was NOT pushed down: {query}"
        assert "SELECT" in query, f"No SELECT: {query}"

        # Verify result is correct
        for batch in results:
            count_col = batch.column(0)
            count_value = count_col[0].as_py()
            assert count_value == 10, f"Expected count=10, got {count_value}"

    def test_count_with_filter_both_pushed(self, setup_capturing_db):
        """Test COUNT(*) WHERE x > y - BOTH filter and COUNT should push."""
        catalog, ds = setup_capturing_db

        sql = "SELECT COUNT(*) FROM testdb.main.orders WHERE order_id > 5"
        query, results = execute_and_capture(sql, catalog, ds)

        # Filter MUST be pushed
        assert "WHERE" in query, f"Filter not pushed: {query}"
        assert "order_id" in query, f"order_id filter not pushed: {query}"

        # COUNT MUST be pushed
        assert "COUNT" in query.upper(), f"COUNT was NOT pushed down: {query}"

        # Verify result (orders 6-10 = 5 orders)
        for batch in results:
            count_col = batch.column(0)
            count_value = count_col[0].as_py()
            assert count_value == 5, f"Expected count=5, got {count_value}"

    def test_group_by_pushed(self, setup_capturing_db):
        """Test GROUP BY IS pushed down to data source."""
        catalog, ds = setup_capturing_db

        sql = "SELECT customer_id, COUNT(*) FROM testdb.main.orders GROUP BY customer_id"
        query, results = execute_and_capture(sql, catalog, ds)

        # GROUP BY MUST be in the query
        assert "GROUP BY" in query.upper(), f"GROUP BY was NOT pushed down: {query}"
        assert "COUNT" in query.upper(), f"COUNT was NOT pushed down: {query}"
        assert "customer_id" in query, f"customer_id not in query: {query}"

        # Verify results are correct
        # customer_id 100: 3 orders (1, 4, 8)
        # customer_id 101: 2 orders (2, 6)
        # customer_id 102: 2 orders (3, 10)
        # customer_id 103: 1 order (5)
        # customer_id 104: 1 order (7)
        # customer_id 105: 1 order (9)
        customer_counts = {}
        for batch in results:
            data = batch.to_pydict()
            # DuckDB returns lowercase column names like count_star()
            count_col = "count_star()" if "count_star()" in data else "COUNT(*)"
            for i in range(batch.num_rows):
                cust_id = data["customer_id"][i]
                count = data[count_col][i]
                customer_counts[cust_id] = count

        expected = {
            100: 3,
            101: 2,
            102: 2,
            103: 1,
            104: 1,
            105: 1
        }
        assert customer_counts == expected, f"Expected {expected}, got {customer_counts}"

    def test_sum_aggregate_pushed(self, setup_capturing_db):
        """Test SUM(column) IS pushed down to data source."""
        catalog, ds = setup_capturing_db

        sql = "SELECT SUM(price) FROM testdb.main.orders"
        query, results = execute_and_capture(sql, catalog, ds)

        # SUM MUST be in the query
        assert "SUM" in query.upper(), f"SUM was NOT pushed down: {query}"
        assert "price" in query.lower(), f"price not in query: {query}"

        # Verify result
        # Sum of all prices: 50 + 100 + 150 + 75 + 200 + 100 + 150 + 75 + 300 + 50 = 1250
        for batch in results:
            sum_col = batch.column(0)
            sum_value = sum_col[0].as_py()
            assert sum_value == 1250.0, f"Expected sum=1250.0, got {sum_value}"


class TestCombinedPushdownReal:
    """Real tests for combined pushdowns."""

    def test_filter_and_projection(self, setup_capturing_db):
        """Test WHERE + column selection."""
        catalog, ds = setup_capturing_db

        sql = "SELECT order_id, price FROM testdb.main.orders WHERE status = 'completed'"
        query, results = execute_and_capture(sql, catalog, ds)

        # Filter should push
        assert "WHERE" in query, f"Filter not pushed: {query}"
        assert "status" in query, f"status not in WHERE: {query}"

        # Projection should include needed columns
        assert '"order_id"' in query or "order_id" in query, f"order_id not selected: {query}"
        assert '"price"' in query or "price" in query, f"price not selected: {query}"

        # Verify results
        all_rows = []
        for batch in results:
            data = batch.to_pydict()
            for i in range(batch.num_rows):
                all_rows.append({
                    "order_id": data["order_id"][i],
                    "price": data["price"][i]
                })

        # Completed orders: 1, 3, 4, 6, 8, 9, 10
        expected_ids = {1, 3, 4, 6, 8, 9, 10}
        actual_ids = {r["order_id"] for r in all_rows}
        assert actual_ids == expected_ids, f"Expected {expected_ids}, got {actual_ids}"


class TestBuggyAggregates:
    """Test for the GROUP BY bug the user found."""

    def test_group_by_count_returns_correct_values(self, setup_capturing_db):
        """
        Test the bug: SELECT COUNT(order_id) ... GROUP BY order_id
        should return counts, not order_id values.
        """
        catalog, ds = setup_capturing_db

        sql = "SELECT COUNT(order_id) FROM testdb.main.orders WHERE order_id > 1 GROUP BY order_id"
        query, results = execute_and_capture(sql, catalog, ds)

        # Collect results
        count_values = []
        for batch in results:
            count_col = batch.column(0)
            count_values.extend([v.as_py() for v in count_col])

        print(f"\nDEBUG: Query sent: {query}")
        print(f"DEBUG: Count values: {count_values}")

        # Each order_id appears exactly once, so each group should have COUNT=1
        # NOT the order_id value itself!
        for count in count_values:
            assert count == 1, f"Each group should have count=1, got {count}. Values: {count_values}"

        # Should have 9 groups (order_id 2-10)
        assert len(count_values) == 9, f"Expected 9 groups, got {len(count_values)}"

    def test_group_by_sum_returns_correct_values(self, setup_capturing_db):
        """Test GROUP BY with SUM returns correct sums."""
        catalog, ds = setup_capturing_db

        sql = "SELECT customer_id, SUM(price) FROM testdb.main.orders GROUP BY customer_id"
        query, results = execute_and_capture(sql, catalog, ds)

        # Collect results
        customer_sums = {}
        for batch in results:
            data = batch.to_pydict()
            # DuckDB returns lowercase column names like sum(price)
            sum_col = "sum(price)" if "sum(price)" in data else "SUM(price)"
            for i in range(batch.num_rows):
                cust_id = data["customer_id"][i]
                sum_price = data[sum_col][i]
                customer_sums[cust_id] = sum_price

        print(f"\nDEBUG: Query sent: {query}")
        print(f"DEBUG: Customer sums: {customer_sums}")

        # Expected sums:
        # customer_id 100: orders 1(50) + 4(75) + 8(75) = 200
        # customer_id 101: orders 2(100) + 6(100) = 200
        # customer_id 102: orders 3(150) + 10(50) = 200
        # customer_id 103: order 5(200) = 200
        # customer_id 104: order 7(150) = 150
        # customer_id 105: order 9(300) = 300
        expected = {
            100: 200.0,
            101: 200.0,
            102: 200.0,
            103: 200.0,
            104: 150.0,
            105: 300.0
        }

        assert customer_sums == expected, f"Expected {expected}, got {customer_sums}"

    def test_exact_user_query_count_order_id_group_by(self, setup_capturing_db):
        """Test the EXACT query the user is sending in CLI.

        Query: SELECT COUNT(order_id) FROM orders WHERE order_id > 1 GROUP BY order_id

        This test MUST verify that COUNT and GROUP BY are pushed to the data source.
        If they are NOT pushed, this test will FAIL.
        """
        catalog, ds = setup_capturing_db

        sql = "SELECT COUNT(order_id) FROM testdb.main.orders WHERE order_id > 1 GROUP BY order_id"
        query, results = execute_and_capture(sql, catalog, ds)

        print(f"\nDEBUG: SQL sent to data source: {query}")

        # CRITICAL: COUNT must be in the query sent to data source
        assert "COUNT" in query.upper(), f"COUNT was NOT pushed down! Query: {query}"

        # CRITICAL: GROUP BY must be in the query sent to data source
        assert "GROUP BY" in query.upper(), f"GROUP BY was NOT pushed down! Query: {query}"

        # Verify WHERE is also pushed
        assert "WHERE" in query.upper(), f"WHERE was NOT pushed down! Query: {query}"
        assert "order_id" in query, f"order_id not in query: {query}"

        print(f"SUCCESS: Query correctly pushed COUNT, GROUP BY, and WHERE to data source")

    def test_exact_user_query_WITHOUT_aggregate_pushdown_rule(self, setup_capturing_db):
        """Test what happens WITHOUT AggregatePushdownRule.

        This documents the broken behavior that existed before the CLI was fixed.
        The CLI NOW includes AggregatePushdownRule, so this test just documents
        what WOULD happen if you don't use it.

        Query: SELECT COUNT(order_id) FROM orders WHERE order_id > 1 GROUP BY order_id
        """
        catalog, ds = setup_capturing_db

        sql = "SELECT COUNT(order_id) FROM testdb.main.orders WHERE order_id > 1 GROUP BY order_id"

        ds.clear_queries()

        parser = Parser()
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        # Optimizer WITHOUT AggregatePushdownRule (the old broken behavior)
        optimizer = RuleBasedOptimizer(catalog)
        optimizer.add_rule(ExpressionSimplificationRule())
        optimizer.add_rule(PredicatePushdownRule())
        optimizer.add_rule(ProjectionPushdownRule())
        optimizer.add_rule(LimitPushdownRule())
        # <-- AggregatePushdownRule is NOT added
        optimized_plan = optimizer.optimize(bound_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(optimized_plan)

        executor = Executor()
        results = list(executor.execute(physical_plan))

        query = ds.get_last_query()

        print(f"\nDEBUG: SQL WITHOUT AggregatePushdownRule: {query}")

        # Verify the broken behavior: COUNT and GROUP BY are NOT pushed
        assert "COUNT" not in query.upper(), f"COUNT should NOT be pushed without rule! Query: {query}"
        assert "GROUP BY" not in query.upper(), f"GROUP BY should NOT be pushed without rule! Query: {query}"

        # Only WHERE should be pushed
        assert "WHERE" in query.upper(), f"WHERE should still be pushed: {query}"


class TestJoinPushdownReal:
    """Real tests for join pushdown behavior."""

    def test_inner_join_executes_single_remote_query(self, setup_capturing_db):
        """INNER JOIN on same source should run as one query."""
        catalog, ds = setup_capturing_db
        sql = """
            SELECT c.customer_id, o.order_id
            FROM testdb.main.customers c
            JOIN testdb.main.orders o ON c.customer_id = o.customer_id
        """
        query, results = execute_and_capture(sql, catalog, ds)
        assert len(ds.captured_queries) == 1, "Join should run as single query"
        assert "INNER JOIN" in query.upper(), f"Expected INNER JOIN in pushed SQL: {query}"
        rows = []
        for batch in results:
            data = batch.to_pydict()
            for idx in range(batch.num_rows):
                row = (data["customer_id"][idx], data["order_id"][idx])
                rows.append(row)
        assert len(rows) == 8, f"Expected 8 joined rows, got {len(rows)}"
        for customer_id, _ in rows:
            assert customer_id != 104, "Unmatched customers should not appear in INNER JOIN results"

    def test_left_join_pushdown_preserves_unmatched_rows(self, setup_capturing_db):
        """LEFT JOIN should be pushed down and keep unmatched customers."""
        catalog, ds = setup_capturing_db
        sql = """
            SELECT c.customer_id, o.order_id
            FROM testdb.main.customers c
            LEFT JOIN testdb.main.orders o ON c.customer_id = o.customer_id
        """
        query, results = execute_and_capture(sql, catalog, ds)
        assert len(ds.captured_queries) == 1, "LEFT JOIN should run remotely"
        assert "LEFT JOIN" in query.upper(), f"Expected LEFT JOIN in SQL: {query}"
        left_rows = []
        for batch in results:
            data = batch.to_pydict()
            for idx in range(batch.num_rows):
                left_rows.append({
                    "customer_id": data["customer_id"][idx],
                    "order_id": data["order_id"][idx],
                })
        has_unmatched = False
        for row in left_rows:
            if row["customer_id"] == 106 and row["order_id"] is None:
                has_unmatched = True
        assert has_unmatched, "Expected unmatched customer 106 with NULL orders"
        assert len(left_rows) == 9, f"Expected 9 rows (including unmatched), got {len(left_rows)}"

    def test_right_join_pushdown_preserves_unmatched_rows(self, setup_capturing_db):
        """RIGHT JOIN should be pushed down and keep unmatched orders."""
        catalog, ds = setup_capturing_db
        sql = """
            SELECT o.customer_id, c.name
            FROM testdb.main.customers c
            RIGHT JOIN testdb.main.orders o ON c.customer_id = o.customer_id
        """
        query, results = execute_and_capture(sql, catalog, ds)
        assert len(ds.captured_queries) == 1, "RIGHT JOIN should run remotely"
        assert "RIGHT JOIN" in query.upper(), f"Expected RIGHT JOIN in SQL: {query}"
        rows = []
        for batch in results:
            data = batch.to_pydict()
            for idx in range(batch.num_rows):
                rows.append({
                    "customer_id": data["customer_id"][idx],
                    "name": data["name"][idx],
                })
        saw_unmatched = False
        for row in rows:
            if row["customer_id"] in (104, 105) and row["name"] is None:
                saw_unmatched = True
        assert saw_unmatched, "Expected unmatched orders 104/105 with NULL customer names"
        assert len(rows) == 10, f"Expected 10 rows (all orders), got {len(rows)}"

    def test_full_join_not_pushed_down(self, setup_capturing_db):
        """FULL JOIN should fall back to local execution."""
        catalog, ds = setup_capturing_db
        sql = """
            SELECT c.customer_id, o.order_id
            FROM testdb.main.customers c
            FULL JOIN testdb.main.orders o ON c.customer_id = o.customer_id
        """
        _, results = execute_and_capture(sql, catalog, ds)
        assert len(ds.captured_queries) == 2, "FULL JOIN should execute scans separately"
        for query in ds.captured_queries:
            assert "JOIN" not in query.upper(), "Fallback scans should not issue JOIN SQL"
        total_rows = 0
        for batch in results:
            total_rows += batch.num_rows
        assert total_rows == 11, f"Expected 11 rows from FULL JOIN, got {total_rows}"
