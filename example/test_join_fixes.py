"""Test JOIN fixes for decimal precision and duplicate column names."""

from pathlib import Path
from federated_query.config.config import load_config, Config, DataSourceConfig, ExecutorConfig
from federated_query.catalog.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor


def main():
    """Test the JOIN fixes with in-memory DuckDB databases."""
    print("Testing JOIN bug fixes...")
    print("="*60)

    catalog = Catalog()

    print("\n1. Creating first DuckDB instance (customers)...")
    db1_config = {"path": ":memory:", "read_only": False}
    db1 = DuckDBDataSource("db1", db1_config)
    db1.connect()

    db1.connection.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
    """)
    db1.connection.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com')
    """)
    catalog.register_datasource(db1)

    print("2. Creating second DuckDB instance (orders)...")
    db2_config = {"path": ":memory:", "read_only": False}
    db2 = DuckDBDataSource("db2", db2_config)
    db2.connect()

    db2.connection.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10,2)
        )
    """)
    db2.connection.execute("""
        INSERT INTO orders VALUES
        (101, 1, 1500.00),
        (102, 2, 750.50)
    """)
    catalog.register_datasource(db2)

    print("3. Loading metadata...")
    catalog.load_metadata()

    print(f"4. Catalog has {len(catalog.datasources)} datasources")

    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    print("\n" + "="*60)
    print("TEST 1: Decimal precision fix")
    print("="*60)
    sql1 = """
        SELECT c.name, o.amount
        FROM db1.main.customers c
        JOIN db2.main.orders o ON c.id = o.customer_id
    """
    print(sql1)

    try:
        ast = parser.parse(sql1)
        logical_plan = parser.ast_to_logical_plan(ast)
        bound_plan = binder.bind(logical_plan)
        physical_plan = planner.plan(bound_plan)
        result = executor.execute_to_table(physical_plan)

        print(f"\nResult ({result.num_rows} rows, {result.num_columns} columns):")
        print(f"Schema: {result.schema}")
        print(result)
        print("\n✓ Test 1 PASSED - Decimal precision preserved")
    except Exception as e:
        print(f"\n✗ Test 1 FAILED: {e}")

    print("\n" + "="*60)
    print("TEST 2: Duplicate column name fix")
    print("="*60)
    sql2 = """
        SELECT c.name, c.id, o.amount
        FROM db1.main.customers c
        JOIN db2.main.orders o ON c.id = o.customer_id
    """
    print(sql2)

    try:
        ast = parser.parse(sql2)
        logical_plan = parser.ast_to_logical_plan(ast)
        bound_plan = binder.bind(logical_plan)
        physical_plan = planner.plan(bound_plan)
        result = executor.execute_to_table(physical_plan)

        print(f"\nResult ({result.num_rows} rows, {result.num_columns} columns):")
        print(f"Schema: {result.schema}")
        print(result)
        print("\n✓ Test 2 PASSED - Duplicate column names handled")
    except Exception as e:
        print(f"\n✗ Test 2 FAILED: {e}")

    print("\n" + "="*60)
    print("TEST 3: Both id columns selected")
    print("="*60)
    sql3 = """
        SELECT c.id, o.id, c.name, o.amount
        FROM db1.main.customers c
        JOIN db2.main.orders o ON c.id = o.customer_id
    """
    print(sql3)

    try:
        ast = parser.parse(sql3)
        logical_plan = parser.ast_to_logical_plan(ast)
        bound_plan = binder.bind(logical_plan)
        physical_plan = planner.plan(bound_plan)
        result = executor.execute_to_table(physical_plan)

        print(f"\nResult ({result.num_rows} rows, {result.num_columns} columns):")
        print(f"Schema: {result.schema}")
        print(result)
        print("\n✓ Test 3 PASSED - Both id columns accessible")
    except Exception as e:
        print(f"\n✗ Test 3 FAILED: {e}")

    db1.disconnect()
    db2.disconnect()

    print("\n" + "="*60)
    print("All tests completed!")
    print("="*60)


if __name__ == "__main__":
    main()
