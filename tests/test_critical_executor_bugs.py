"""Tests for critical executor bugs found in code review.

This test suite verifies three critical issues:
1. PyArrow column access uses string keys instead of integer indices
2. Star projections drop subsequent expressions
3. Left/full outer joins raise NotImplementedError
"""

import pytest
import pyarrow as pa
from federated_query.parser import Parser, Binder
from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.optimizer import PhysicalPlanner
from federated_query.executor import Executor


@pytest.fixture
def setup_two_datasources():
    """Set up two DuckDB datasources for testing joins."""
    config_customers = {
        "database": ":memory:",
        "read_only": False,
    }
    ds_customers = DuckDBDataSource(name="db1", config=config_customers)
    ds_customers.connect()

    ds_customers.connection.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            city VARCHAR
        )
    """)

    ds_customers.connection.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice', 'NYC'),
        (2, 'Bob', 'LA'),
        (3, 'Charlie', 'SF'),
        (4, 'Diana', 'Seattle')
    """)

    config_orders = {
        "database": ":memory:",
        "read_only": False,
    }
    ds_orders = DuckDBDataSource(name="db2", config=config_orders)
    ds_orders.connect()

    ds_orders.connection.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            amount DOUBLE,
            status VARCHAR
        )
    """)

    ds_orders.connection.execute("""
        INSERT INTO orders VALUES
        (101, 1, 100.0, 'completed'),
        (102, 1, 200.0, 'pending'),
        (103, 2, 150.0, 'completed'),
        (104, 2, 50.0, 'cancelled')
    """)

    catalog = Catalog()
    catalog.register_datasource(ds_customers)
    catalog.register_datasource(ds_orders)
    catalog.load_metadata()

    yield catalog, ds_customers, ds_orders

    ds_customers.disconnect()
    ds_orders.disconnect()


@pytest.fixture
def setup_single_datasource():
    """Set up a single DuckDB datasource for basic query tests."""
    config = {
        "database": ":memory:",
        "read_only": False,
    }
    datasource = DuckDBDataSource(name="testdb", config=config)
    datasource.connect()

    datasource.connection.execute("""
        CREATE TABLE products (
            id INTEGER,
            name VARCHAR,
            price DOUBLE,
            category VARCHAR
        )
    """)

    datasource.connection.execute("""
        INSERT INTO products VALUES
        (1, 'Laptop', 999.99, 'Electronics'),
        (2, 'Mouse', 29.99, 'Electronics'),
        (3, 'Desk', 299.99, 'Furniture'),
        (4, 'Chair', 199.99, 'Furniture'),
        (5, 'Monitor', 399.99, 'Electronics')
    """)

    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    yield catalog, datasource

    datasource.disconnect()


class TestBug1PyArrowColumnAccess:
    """Test Bug #1: PyArrow column access uses string keys causing TypeError."""

    def test_filter_with_column_reference(self, setup_single_datasource):
        """Test that PhysicalFilter can access columns by name.

        Bug: batch.column(expr.column) in PhysicalFilter._evaluate_value
        calls with string name instead of integer index.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = "SELECT id, name FROM testdb.main.products WHERE price > 100"
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        ids = []
        for batch in results:
            ids.extend(batch.to_pydict()["id"])

        assert len(ids) == 4
        assert set(ids) == {1, 3, 4, 5}

    def test_projection_with_column_reference(self, setup_single_datasource):
        """Test that PhysicalProject can access columns by name.

        Bug: batch.column(expr.column) in PhysicalProject._project_batch
        calls with string name instead of integer index.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = "SELECT name, price FROM testdb.main.products"
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        batches = list(results)
        assert len(batches) > 0
        first_batch = batches[0]

        assert first_batch.num_columns == 2
        assert first_batch.schema.names == ["name", "price"]
        assert first_batch.num_rows == 5

    def test_hash_join_key_extraction(self, setup_two_datasources):
        """Test that PhysicalHashJoin can extract join keys by column name.

        Bug: batch.column(key.column) in PhysicalHashJoin._extract_key_values
        calls with string name instead of integer index.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.amount
            FROM db1.main.customers c
            INNER JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict["name"][i],
                    "amount": result_dict["amount"][i]
                })

        assert len(rows) == 4

        alice_rows = [r for r in rows if r["name"] == "Alice"]
        assert len(alice_rows) == 2
        assert sorted([r["amount"] for r in alice_rows]) == [100.0, 200.0]

    def test_aggregate_group_by_column_access(self, setup_single_datasource):
        """Test that PhysicalHashAggregate can access group by columns.

        Bug: batch.column(expr.column) in PhysicalHashAggregate._extract_group_key
        and table.column(col_name) in _compute_aggregate call with string names.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = "SELECT category, COUNT(*) as cnt FROM testdb.main.products GROUP BY category"
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        result_dict = {}
        for batch in results:
            pydict = batch.to_pydict()
            for i in range(batch.num_rows):
                category = pydict["category"][i]
                count = pydict["cnt"][i]
                result_dict[category] = count

        assert result_dict["Electronics"] == 3
        assert result_dict["Furniture"] == 2

    def test_nested_loop_join_condition_evaluation(self, setup_two_datasources):
        """Test that PhysicalNestedLoopJoin can evaluate conditions with column refs.

        Bug: batch.column(expr.column) in PhysicalNestedLoopJoin._evaluate_expression_on_batch
        calls with string name instead of integer index.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.amount
            FROM db1.main.customers c, db2.main.orders o
            WHERE c.id = o.customer_id AND o.amount > 100
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict["name"][i],
                    "amount": result_dict["amount"][i]
                })

        assert len(rows) == 2
        amounts = sorted([r["amount"] for r in rows])
        assert amounts == [150.0, 200.0]


class TestBug2StarProjectionDropsExpressions:
    """Test Bug #2: Star projections return early and drop subsequent expressions."""

    def test_star_with_additional_column(self, setup_single_datasource):
        """Test SELECT *, id AS id_copy.

        Bug: PhysicalProject._project_batch returns early when encountering *,
        dropping the id_copy column reference.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = "SELECT *, id AS id_copy FROM testdb.main.products"
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        batches = list(results)
        assert len(batches) > 0
        first_batch = batches[0]

        schema_names = first_batch.schema.names
        assert "id_copy" in schema_names, \
            f"Expected 'id_copy' column but got schema: {schema_names}"

        assert first_batch.num_columns == 5, \
            f"Expected 5 columns (4 original + id_copy) but got {first_batch.num_columns}"

        pydict = first_batch.to_pydict()
        assert pydict["id"] == pydict["id_copy"], "id and id_copy should have same values"

    def test_star_with_multiple_additional_columns(self, setup_single_datasource):
        """Test SELECT *, id AS id_copy, name AS name_copy.

        Bug: All column references after * should be included.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = """
            SELECT *, id AS id_copy, name AS name_copy
            FROM testdb.main.products
            WHERE id < 3
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        batches = list(results)
        assert len(batches) > 0
        first_batch = batches[0]

        schema_names = first_batch.schema.names
        assert "id_copy" in schema_names
        assert "name_copy" in schema_names
        assert first_batch.num_columns == 6

        pydict = first_batch.to_pydict()
        assert pydict["id"] == pydict["id_copy"]
        assert pydict["name"] == pydict["name_copy"]

    def test_star_with_aliased_column(self, setup_single_datasource):
        """Test SELECT *, name AS product_name.

        Bug: The aliased column should appear even with *.
        """
        catalog, datasource = setup_single_datasource

        parser = Parser()
        sql = "SELECT *, name AS product_name FROM testdb.main.products LIMIT 2"
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        batches = list(results)
        assert len(batches) > 0
        first_batch = batches[0]

        schema_names = first_batch.schema.names
        assert "product_name" in schema_names, \
            f"Expected 'product_name' column but got schema: {schema_names}"

        assert first_batch.num_columns == 5
        pydict = first_batch.to_pydict()
        assert pydict["name"] == pydict["product_name"]


class TestBug3LeftFullJoinNotImplemented:
    """Test Bug #3: Left/full outer joins raise NotImplementedError."""

    def test_left_join_with_unmatched_rows(self, setup_two_datasources):
        """Test LEFT JOIN where some left rows have no matching right rows.

        Bug: PhysicalHashJoin._create_left_outer_row raises NotImplementedError
        when a left row has no match.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.id, c.name, o.order_id, o.amount
            FROM db1.main.customers c
            LEFT JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "customer_id": result_dict["id"][i],
                    "name": result_dict["name"][i],
                    "order_id": result_dict["order_id"][i] if "order_id" in result_dict else None,
                    "amount": result_dict["amount"][i] if "amount" in result_dict else None
                })

        assert len(rows) >= 4, "Should have at least 4 customers"

        charlie_rows = [r for r in rows if r["name"] == "Charlie"]
        assert len(charlie_rows) == 1, "Charlie should appear once"
        assert charlie_rows[0]["order_id"] is None, "Charlie has no orders, should be NULL"
        assert charlie_rows[0]["amount"] is None, "Charlie has no orders, should be NULL"

        diana_rows = [r for r in rows if r["name"] == "Diana"]
        assert len(diana_rows) == 1, "Diana should appear once"
        assert diana_rows[0]["order_id"] is None, "Diana has no orders, should be NULL"

    def test_left_join_with_filter(self, setup_two_datasources):
        """Test LEFT JOIN with WHERE clause on left side.

        Bug: Should return NULLs for unmatched left rows.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.amount
            FROM db1.main.customers c
            LEFT JOIN db2.main.orders o ON c.id = o.customer_id
            WHERE c.city = 'SF'
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict["name"][i],
                    "amount": result_dict["amount"][i] if "amount" in result_dict else None
                })

        assert len(rows) == 1
        assert rows[0]["name"] == "Charlie"
        assert rows[0]["amount"] is None

    def test_nested_loop_left_join(self, setup_two_datasources):
        """Test nested loop LEFT JOIN with non-equi condition.

        Bug: PhysicalNestedLoopJoin._create_left_outer_row raises NotImplementedError.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.amount
            FROM db1.main.customers c
            LEFT JOIN db2.main.orders o ON c.id = o.customer_id AND o.amount > 150
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict["name"][i],
                    "amount": result_dict["amount"][i] if "amount" in result_dict else None
                })

        assert len(rows) == 4

        null_amounts = [r for r in rows if r["amount"] is None]
        assert len(null_amounts) == 3

        non_null_amounts = [r for r in rows if r["amount"] is not None]
        assert len(non_null_amounts) == 1
        assert non_null_amounts[0]["amount"] == 200.0


class TestBug4FullJoinMissingRightRows:
    """Test Bug #4: FULL OUTER JOIN doesn't emit unmatched right rows."""

    def test_hash_full_join_with_unmatched_right_rows(self, setup_two_datasources):
        """Test FULL OUTER JOIN where right table has unmatched rows.

        Bug: PhysicalHashJoin only emits unmatched left (probe) rows but never
        emits unmatched right (build) rows.

        Setup: 4 customers, 4 orders for only 2 customers.
        Expected: All 4 customers + 0 extra orders = 6 total rows
        (2 customers with 2 orders each = 4 rows, 2 customers with NULL = 2 rows)
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.id AS cust_id, c.name, o.order_id, o.amount
            FROM db1.main.customers c
            FULL OUTER JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "cust_id": result_dict.get("cust_id", [None] * batch.num_rows)[i],
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i],
                    "amount": result_dict.get("amount", [None] * batch.num_rows)[i]
                })

        assert len(rows) == 6, f"Expected 6 rows (4 matched + 2 unmatched customers), got {len(rows)}"

        unmatched_customers = [r for r in rows if r["order_id"] is None]
        assert len(unmatched_customers) == 2, \
            f"Expected 2 customers with no orders, got {len(unmatched_customers)}"

        customer_names = {r["name"] for r in unmatched_customers}
        assert "Charlie" in customer_names
        assert "Diana" in customer_names

        matched_rows = [r for r in rows if r["order_id"] is not None]
        assert len(matched_rows) == 4

    def test_hash_full_join_with_unmatched_both_sides(self):
        """Test FULL OUTER JOIN where both sides have unmatched rows.

        Bug: Only left unmatched rows are emitted, right unmatched rows are dropped.

        Setup: 3 customers (1,2,3), 3 orders for customers (2,3,4)
        Expected: Customer 1 with NULL order, Customers 2&3 with their orders,
                  and orphan order for customer 4 with NULL customer info.
        """
        config_customers = {
            "database": ":memory:",
            "read_only": False,
        }
        ds_customers = DuckDBDataSource(name="db1", config=config_customers)
        ds_customers.connect()

        ds_customers.connection.execute("""
            CREATE TABLE customers (
                id INTEGER,
                name VARCHAR,
                city VARCHAR
            )
        """)

        ds_customers.connection.execute("""
            INSERT INTO customers VALUES
            (1, 'Alice', 'NYC'),
            (2, 'Bob', 'LA'),
            (3, 'Charlie', 'SF')
        """)

        config_orders = {
            "database": ":memory:",
            "read_only": False,
        }
        ds_orders = DuckDBDataSource(name="db2", config=config_orders)
        ds_orders.connect()

        ds_orders.connection.execute("""
            CREATE TABLE orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE
            )
        """)

        ds_orders.connection.execute("""
            INSERT INTO orders VALUES
            (101, 2, 100.0),
            (102, 3, 200.0),
            (103, 4, 300.0)
        """)

        catalog = Catalog()
        catalog.register_datasource(ds_customers)
        catalog.register_datasource(ds_orders)
        catalog.load_metadata()

        parser = Parser()
        sql = """
            SELECT c.id AS cust_id, c.name, o.order_id, o.customer_id AS order_cust_id
            FROM db1.main.customers c
            FULL OUTER JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "cust_id": result_dict.get("cust_id", [None] * batch.num_rows)[i],
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i],
                    "order_cust_id": result_dict.get("order_cust_id", [None] * batch.num_rows)[i]
                })

        ds_customers.disconnect()
        ds_orders.disconnect()

        assert len(rows) == 4, \
            f"Expected 4 rows (1 unmatched left, 2 matched, 1 unmatched right), got {len(rows)}"

        alice_row = [r for r in rows if r.get("name") == "Alice"]
        assert len(alice_row) == 1, "Alice (customer 1) should appear once"
        assert alice_row[0]["order_id"] is None, "Alice should have NULL order"

        bob_row = [r for r in rows if r.get("name") == "Bob"]
        assert len(bob_row) == 1, "Bob should appear once"
        assert bob_row[0]["order_id"] == 101

        charlie_row = [r for r in rows if r.get("name") == "Charlie"]
        assert len(charlie_row) == 1, "Charlie should appear once"
        assert charlie_row[0]["order_id"] == 102

        orphan_order = [r for r in rows if r["order_id"] == 103]
        assert len(orphan_order) == 1, "Order 103 (for non-existent customer 4) should appear"
        assert orphan_order[0]["cust_id"] is None, "Orphan order should have NULL customer id"
        assert orphan_order[0]["name"] is None, "Orphan order should have NULL customer name"
        assert orphan_order[0]["order_cust_id"] == 4

    def test_nested_loop_full_join_with_unmatched_right_rows(self, setup_two_datasources):
        """Test nested loop FULL OUTER JOIN with unmatched right rows.

        Bug: PhysicalNestedLoopJoin only emits unmatched left rows but never
        emits unmatched right rows.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.order_id
            FROM db1.main.customers c
            FULL OUTER JOIN db2.main.orders o ON c.id = o.customer_id AND o.amount > 75
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i]
                })

        assert len(rows) >= 4, f"Should have at least 4 customers, got {len(rows)}"

        unmatched_customers = [r for r in rows if r["order_id"] is None and r["name"] is not None]
        assert len(unmatched_customers) >= 2, "Should have unmatched customers (Charlie, Diana, maybe others)"

        customer_names = {r["name"] for r in unmatched_customers}
        assert "Charlie" in customer_names
        assert "Diana" in customer_names

        unmatched_orders = [r for r in rows if r["name"] is None and r["order_id"] is not None]
        assert len(unmatched_orders) > 0, \
            "Should have unmatched orders (those that don't meet amount > 75 or other criteria)"


class TestBug5RightJoinMissingUnmatchedRows:
    """Test Bug #5: RIGHT OUTER JOIN doesn't emit unmatched right rows."""

    def test_hash_right_join_with_unmatched_right_rows(self, setup_two_datasources):
        """Test RIGHT OUTER JOIN where right table has unmatched rows.

        Bug: PhysicalHashJoin only handles LEFT and FULL joins but not RIGHT joins.
        When join_type=RIGHT, unmatched right-side rows are dropped.

        Setup: 4 customers, 4 orders for only 2 customers.
        Expected for RIGHT JOIN: 4 orders matched/unmatched
        (2 matched with customer info, 2 unmatched with NULL customer info)
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.order_id, o.amount
            FROM db1.main.customers c
            RIGHT JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i],
                    "amount": result_dict.get("amount", [None] * batch.num_rows)[i]
                })

        assert len(rows) == 4, f"Expected 4 orders (all orders), got {len(rows)}"

        order_ids = sorted([r["order_id"] for r in rows])
        assert order_ids == [101, 102, 103, 104], f"Should have all 4 orders, got {order_ids}"

        matched_rows = [r for r in rows if r["name"] is not None]
        assert len(matched_rows) == 4, "All orders belong to existing customers"

    def test_hash_right_join_with_unmatched_both_sides(self):
        """Test RIGHT OUTER JOIN where both sides have unmatched rows.

        Bug: Only matched rows are emitted, unmatched right rows are dropped.

        Setup: 3 customers (1,2,3), 3 orders for customers (2,3,4)
        Expected for RIGHT JOIN: All 3 orders
        (Orders for customers 2&3 with names, order for customer 4 with NULL name)
        """
        config_customers = {
            "database": ":memory:",
            "read_only": False,
        }
        ds_customers = DuckDBDataSource(name="db1", config=config_customers)
        ds_customers.connect()

        ds_customers.connection.execute("""
            CREATE TABLE customers (
                id INTEGER,
                name VARCHAR,
                city VARCHAR
            )
        """)

        ds_customers.connection.execute("""
            INSERT INTO customers VALUES
            (1, 'Alice', 'NYC'),
            (2, 'Bob', 'LA'),
            (3, 'Charlie', 'SF')
        """)

        config_orders = {
            "database": ":memory:",
            "read_only": False,
        }
        ds_orders = DuckDBDataSource(name="db2", config=config_orders)
        ds_orders.connect()

        ds_orders.connection.execute("""
            CREATE TABLE orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE
            )
        """)

        ds_orders.connection.execute("""
            INSERT INTO orders VALUES
            (101, 2, 100.0),
            (102, 3, 200.0),
            (103, 4, 300.0)
        """)

        catalog = Catalog()
        catalog.register_datasource(ds_customers)
        catalog.register_datasource(ds_orders)
        catalog.load_metadata()

        parser = Parser()
        sql = """
            SELECT c.id AS cust_id, c.name, o.order_id, o.customer_id AS order_cust_id
            FROM db1.main.customers c
            RIGHT JOIN db2.main.orders o ON c.id = o.customer_id
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "cust_id": result_dict.get("cust_id", [None] * batch.num_rows)[i],
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i],
                    "order_cust_id": result_dict.get("order_cust_id", [None] * batch.num_rows)[i]
                })

        ds_customers.disconnect()
        ds_orders.disconnect()

        assert len(rows) == 3, \
            f"Expected 3 rows (all orders: 2 matched, 1 unmatched right), got {len(rows)}"

        bob_row = [r for r in rows if r.get("name") == "Bob"]
        assert len(bob_row) == 1, "Bob should appear once"
        assert bob_row[0]["order_id"] == 101

        charlie_row = [r for r in rows if r.get("name") == "Charlie"]
        assert len(charlie_row) == 1, "Charlie should appear once"
        assert charlie_row[0]["order_id"] == 102

        orphan_order = [r for r in rows if r["order_id"] == 103]
        assert len(orphan_order) == 1, "Order 103 (for non-existent customer 4) should appear"
        assert orphan_order[0]["cust_id"] is None, "Orphan order should have NULL customer id"
        assert orphan_order[0]["name"] is None, "Orphan order should have NULL customer name"
        assert orphan_order[0]["order_cust_id"] == 4

        alice_rows = [r for r in rows if r.get("name") == "Alice"]
        assert len(alice_rows) == 0, "Alice (customer 1) has no orders, should not appear in RIGHT JOIN"

    def test_nested_loop_right_join_with_unmatched_right_rows(self, setup_two_datasources):
        """Test nested loop RIGHT OUTER JOIN with unmatched right rows.

        Bug: PhysicalNestedLoopJoin only handles LEFT and FULL joins but not RIGHT joins.
        """
        catalog, ds_customers, ds_orders = setup_two_datasources

        parser = Parser()
        sql = """
            SELECT c.name, o.order_id, o.amount
            FROM db1.main.customers c
            RIGHT JOIN db2.main.orders o ON c.id = o.customer_id AND o.amount > 75
        """
        logical_plan = parser.parse_to_logical_plan(sql, catalog)

        binder = Binder(catalog)
        bound_plan = binder.bind(logical_plan)

        planner = PhysicalPlanner(catalog)
        physical_plan = planner.plan(bound_plan)

        executor = Executor()
        results = executor.execute(physical_plan)

        rows = []
        for batch in results:
            result_dict = batch.to_pydict()
            for i in range(batch.num_rows):
                rows.append({
                    "name": result_dict.get("name", [None] * batch.num_rows)[i],
                    "order_id": result_dict.get("order_id", [None] * batch.num_rows)[i],
                    "amount": result_dict.get("amount", [None] * batch.num_rows)[i]
                })

        assert len(rows) == 4, f"Should have all 4 orders, got {len(rows)}"

        order_ids = sorted([r["order_id"] for r in rows])
        assert order_ids == [101, 102, 103, 104], f"Should have all 4 orders"

        matched_with_customers = [r for r in rows if r["name"] is not None]
        assert len(matched_with_customers) == 3, \
            "3 orders meet amount > 75 AND have matching customers"

        unmatched_orders = [r for r in rows if r["name"] is None and r["order_id"] is not None]
        assert len(unmatched_orders) == 1, \
            "1 order doesn't meet amount > 75 criteria, should have NULL customer"

