"""Example demonstrating federated query engine setup with config and catalog."""

import os
from pathlib import Path

from federated_query.config.config import load_config
from federated_query.catalog.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource


def create_sample_data(datasource):
    """Create sample tables in DuckDB for demonstration."""
    conn = datasource.connection

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

    conn.execute("""
        CREATE TABLE orders (
            id INTEGER,
            customer_id INTEGER,
            amount DECIMAL,
            order_date DATE
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
        (101, 1, 1500.00, '2024-01-15'),
        (102, 2, 750.00, '2024-01-16'),
        (103, 1, 2200.00, '2024-01-17')
    """)


def create_datasource(name, ds_config):
    """Create datasource instance from config."""
    ds_type = ds_config.type

    if ds_type == "duckdb":
        return DuckDBDataSource(name, ds_config.config)

    raise ValueError(f"Unsupported datasource type: {ds_type}")


def main():
    """Main example function."""
    config_path = Path(__file__).parent / "dbconfig.yaml"

    config = load_config(str(config_path))

    print(f"Loaded config with {len(config.datasources)} datasource(s)")

    catalog = Catalog()

    for ds_name, ds_config in config.datasources.items():
        print(f"Creating datasource: {ds_name}")
        datasource = create_datasource(ds_name, ds_config)

        datasource.connect()

        if ds_config.type == "duckdb" and ds_config.config.get("path") == ":memory:":
            print(f"Creating sample data in {ds_name}")
            create_sample_data(datasource)

        catalog.register_datasource(datasource)

    print("\nLoading metadata from datasources...")
    catalog.load_metadata()

    print(f"\nCatalog summary: {catalog}")

    print("\nSchemas in catalog:")
    for key, schema in catalog.schemas.items():
        ds_name, schema_name = key
        print(f"  {ds_name}.{schema_name}:")
        for table in schema.tables.values():
            print(f"    - {table.name} ({len(table.columns)} columns)")
            for col in table.columns:
                print(f"      - {col.name}: {col.data_type.value}")

    print("\nCatalog setup complete!")

    for datasource in catalog.datasources.values():
        datasource.disconnect()


if __name__ == "__main__":
    main()