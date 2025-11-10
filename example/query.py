"""Example demonstrating federated query engine setup with config and catalog."""

import os
from pathlib import Path

from federated_query.config.config import load_config
from federated_query.catalog.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
import duckdb
import pyarrow as pa
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column, DataType
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
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
            print("  Created orders table in PostgreSQL")
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

    print("  Created customers table in DuckDB")


def create_datasource(name, ds_config):
    """Create datasource instance from config."""
    ds_type = ds_config.type

    if ds_type == "duckdb":
        return DuckDBDataSource(name, ds_config.config)

    if ds_type == "postgresql":
        return PostgreSQLDataSource(name, ds_config.config)

    raise ValueError(f"Unsupported datasource type: {ds_type}")


def main():
    """Main example function."""
    config_path = Path(__file__).parent / "dbconfig.yaml"

    config = load_config(str(config_path))

    print(f"Loaded config with {len(config.datasources)} datasource(s)")
    print()

    catalog = Catalog()

    for ds_name, ds_config in config.datasources.items():
        print(f"Setting up datasource: {ds_name} ({ds_config.type})")

        try:
            datasource = create_datasource(ds_name, ds_config)
            datasource.connect()

            if ds_config.type == "postgresql":
                create_postgres_sample_data(datasource)
            elif ds_config.type == "duckdb":
                create_duckdb_sample_data(datasource)

            catalog.register_datasource(datasource)
            print(f"  Successfully registered {ds_name}")

        except Exception as e:
            print(f"  WARNING: Could not connect to {ds_name}: {e}")
            print(f"  Skipping {ds_name}...")
            continue

        print()

    if not catalog.datasources:
        print("ERROR: No datasources were successfully registered!")
        return

    print("Loading metadata from datasources...")
    catalog.load_metadata()

    print(f"\nCatalog summary: {catalog}")

    print("\nSchemas and tables in catalog:")
    for key, schema in catalog.schemas.items():
        ds_name, schema_name = key
        if schema.tables:
            print(f"\n  {ds_name}.{schema_name}:")
            for table in schema.tables.values():
                print(f"    {table.name}:")
                for col in table.columns:
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    print(f"      - {col.name}: {col.data_type.value} {nullable}")

    print("\n" + "="*60)
    print("Federated catalog setup complete!")
    print("="*60)
    print(f"Total datasources: {len(catalog.datasources)}")
    print(f"Total schemas: {len(catalog.schemas)}")

    total_tables = sum(len(s.tables) for s in catalog.schemas.values())
    print(f"Total tables: {total_tables}")

    print("\nYou can now execute federated queries across:")
    for ds_name in catalog.datasources.keys():
        print(f"  - {ds_name}")


    sql = """
        SELECT c.name, o.amount
        FROM local_duckdb.main.customers c
        JOIN postgres_prod.public.orders o ON c.id = o.customer_id
    """
    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    ast = parser.parse(sql)
    logical_plan = parser.ast_to_logical_plan(ast)
    bound_plan = binder.bind(logical_plan)
    physical_plan = planner.plan(bound_plan)

    result_table = executor.execute_to_table(physical_plan)

    print("result table", result_table)    

    print("\nCleaning up connections...")
    for datasource in catalog.datasources.values():
        try:
            datasource.disconnect()
        except Exception as e:
            print(f"  Error disconnecting {datasource.name}: {e}")


if __name__ == "__main__":
    main()