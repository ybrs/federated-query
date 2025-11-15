"""Test federated JOIN queries to verify bug fixes."""

from pathlib import Path
from federated_query.config.config import load_config
from federated_query.catalog.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig


def create_postgres_sample_data(datasource):
    """Create sample tables in PostgreSQL for demonstration."""
    conn = datasource._get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
            cursor.execute("""
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    amount DECIMAL(10,2),
                    order_date DATE
                )
            """)
            cursor.execute("""
                INSERT INTO orders VALUES
                (101, 1, 1500.00, '2024-01-15'),
                (102, 2, 750.00, '2024-01-16'),
                (103, 1, 2200.00, '2024-01-17')
            """)
            conn.commit()
            print("Created orders table in PostgreSQL")
    finally:
        datasource._return_connection(conn)


def create_duckdb_sample_data(datasource):
    """Create sample tables in DuckDB for demonstration."""
    conn = datasource.connection
    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO customers VALUES
        (1, 'Alice Smith', 'alice@example.com'),
        (2, 'Bob Jones', 'bob@example.com'),
        (3, 'Charlie Brown', 'charlie@example.com')
    """)
    print("Created customers table in DuckDB")


def create_datasource(name, ds_config):
    """Create datasource instance from config."""
    ds_type = ds_config.type
    if ds_type == "duckdb":
        return DuckDBDataSource(name, ds_config.config)
    if ds_type == "postgresql":
        return PostgreSQLDataSource(name, ds_config.config)
    raise ValueError(f"Unsupported datasource type: {ds_type}")


def run_query(catalog, sql, query_name):
    """Execute a SQL query and print results."""
    print(f"\n{'='*60}")
    print(f"Query: {query_name}")
    print(f"{'='*60}")
    print(sql)
    print()

    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    logical_plan = parser.parse_to_logical_plan(sql, catalog)
    bound_plan = binder.bind(logical_plan)
    physical_plan = planner.plan(bound_plan)

    result_table = executor.execute_to_table(physical_plan)

    print(f"Result ({result_table.num_rows} rows):")
    print(result_table.to_pandas())
    print()


def main():
    """Main test function."""
    config_path = Path(__file__).parent / "dbconfig.yaml"
    config = load_config(str(config_path))

    catalog = Catalog()

    for ds_name, ds_config in config.datasources.items():
        print(f"Setting up {ds_name}...")
        try:
            datasource = create_datasource(ds_name, ds_config)
            datasource.connect()

            if ds_config.type == "postgresql":
                create_postgres_sample_data(datasource)
            elif ds_config.type == "duckdb":
                create_duckdb_sample_data(datasource)

            catalog.register_datasource(datasource)
        except Exception as e:
            print(f"WARNING: Could not connect to {ds_name}: {e}")
            print(f"Skipping {ds_name}...")
            continue

    if not catalog.datasources:
        print("ERROR: No datasources available!")
        return

    catalog.load_metadata()

    print(f"\nCatalog ready with {len(catalog.datasources)} datasource(s)\n")

    try:
        run_query(
            catalog,
            """
            SELECT c.name, o.amount
            FROM local_duckdb.main.customers c
            JOIN postgres_prod.public.orders o ON c.id = o.customer_id
            """,
            "Test 1: Decimal precision fix",
        )
    except Exception as e:
        print(f"Query 1 FAILED: {e}\n")

    try:
        run_query(
            catalog,
            """
            SELECT c.name, c.id, o.amount
            FROM local_duckdb.main.customers c
            JOIN postgres_prod.public.orders o ON c.id = o.customer_id
            """,
            "Test 2: Duplicate column name fix",
        )
    except Exception as e:
        print(f"Query 2 FAILED: {e}\n")

    try:
        run_query(
            catalog,
            """
            SELECT c.name, o.amount, o.order_date
            FROM local_duckdb.main.customers c
            JOIN postgres_prod.public.orders o ON c.id = o.customer_id
            WHERE o.amount > 1000
            """,
            "Test 3: Federated JOIN with filter",
        )
    except Exception as e:
        print(f"Query 3 FAILED: {e}\n")

    print("\nCleaning up...")
    for datasource in catalog.datasources.values():
        datasource.disconnect()

    print("Done!")


if __name__ == "__main__":
    main()
