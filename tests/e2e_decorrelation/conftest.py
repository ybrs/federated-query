"""
Fixtures for decorrelation e2e tests.

Sets up test databases with sample data that exercises all decorrelation
patterns. The decorrelation tests reference tables with two-part names such as
``pg.users``. The engine resolves a two-part name as ``schema.table`` inside the
``default`` data source, so the PostgreSQL source is registered under the name
``default`` and the tables live in a PostgreSQL schema named ``pg``.

Connection parameters default to the local test harness (see
README-test-harness-setup.md) and may be overridden via the standard
``POSTGRES_*`` environment variables.
"""

import os

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.datasources.duckdb import DuckDBDataSource

PG_SCHEMA = "pg"


def _pg_config():
    """Build PostgreSQL connection config from the environment."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
        "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "schemas": [PG_SCHEMA],
    }


@pytest.fixture(scope="module")
def pg_datasource():
    """Connected PostgreSQL data source registered as ``default``."""
    datasource = PostgreSQLDataSource(name="default", config=_pg_config())
    datasource.connect()
    yield datasource
    datasource.disconnect()


@pytest.fixture(scope="module")
def duckdb_datasource():
    """Connected in-memory DuckDB data source."""
    config = {"database": ":memory:", "read_only": False}
    datasource = DuckDBDataSource(name="duckdb", config=config)
    datasource.connect()
    yield datasource
    datasource.disconnect()


@pytest.fixture(scope="module")
def catalog(pg_datasource, duckdb_datasource):
    """Catalog with both sources registered; metadata loaded by setup fixtures."""
    catalog = Catalog()
    catalog.register_datasource(pg_datasource)
    catalog.register_datasource(duckdb_datasource)
    return catalog


def _create_users(cursor) -> None:
    """Create and populate the users table."""
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            country VARCHAR(50),
            city VARCHAR(50)
        )
        """)
    cursor.execute("""
        INSERT INTO users (id, name, country, city) VALUES
        (1, 'Alice', 'US', 'New York'),
        (2, 'Bob', 'UK', 'London'),
        (3, 'Charlie', 'US', 'Boston'),
        (4, 'David', 'FR', 'Paris'),
        (5, 'Eve', 'UK', 'Manchester')
        """)


def _create_orders(cursor) -> None:
    """Create and populate the orders table."""
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount DECIMAL(10,2),
            status VARCHAR(20)
        )
        """)
    cursor.execute("""
        INSERT INTO orders (id, user_id, amount, status) VALUES
        (1, 1, 100.00, 'completed'),
        (2, 1, 200.00, 'completed'),
        (3, 2, 150.00, 'pending'),
        (4, 3, 300.00, 'completed'),
        (5, 3, 50.00, 'cancelled'),
        (6, 5, 500.00, 'completed')
        """)


def _create_cities(cursor) -> None:
    """Create and populate the cities table."""
    cursor.execute("""
        CREATE TABLE cities (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            country VARCHAR(50),
            population INTEGER
        )
        """)
    cursor.execute("""
        INSERT INTO cities (id, name, country, population) VALUES
        (1, 'New York', 'US', 8000000),
        (2, 'London', 'UK', 9000000),
        (3, 'Boston', 'US', 700000),
        (4, 'Paris', 'FR', 2000000),
        (5, 'Manchester', 'UK', 500000),
        (6, 'Chicago', 'US', 2700000)
        """)


def _create_products(cursor) -> None:
    """Create and populate the products table."""
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            price DECIMAL(10,2),
            category VARCHAR(50)
        )
        """)
    cursor.execute("""
        INSERT INTO products (id, name, price, category) VALUES
        (1, 'Laptop', 1000.00, 'Electronics'),
        (2, 'Mouse', 20.00, 'Electronics'),
        (3, 'Desk', 300.00, 'Furniture'),
        (4, 'Chair', 150.00, 'Furniture')
        """)


def _create_sales(cursor) -> None:
    """Create and populate the sales table."""
    cursor.execute("""
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            seller VARCHAR(50),
            price DECIMAL(10,2),
            region VARCHAR(50)
        )
        """)
    cursor.execute("""
        INSERT INTO sales (id, product_id, seller, price, region) VALUES
        (1, 1, 'SellerA', 950.00, 'East'),
        (2, 2, 'SellerA', 18.00, 'East'),
        (3, 3, 'SellerB', 280.00, 'West'),
        (4, 4, 'SellerB', 140.00, 'West')
        """)


def _create_offers(cursor) -> None:
    """Create and populate the offers table."""
    cursor.execute("""
        CREATE TABLE offers (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            seller VARCHAR(50),
            amount DECIMAL(10,2)
        )
        """)
    cursor.execute("""
        INSERT INTO offers (id, product_id, seller, amount) VALUES
        (1, 1, 'SellerA', 900.00),
        (2, 1, 'SellerA', 920.00),
        (3, 2, 'SellerA', 15.00),
        (4, 3, 'SellerB', 250.00)
        """)


def _create_limits(cursor) -> None:
    """Create and populate the limits table."""
    cursor.execute("""
        CREATE TABLE limits (
            id INTEGER PRIMARY KEY,
            region VARCHAR(50),
            cap DECIMAL(10,2)
        )
        """)
    cursor.execute("""
        INSERT INTO limits (id, region, cap) VALUES
        (1, 'East', 1000.00),
        (2, 'West', 300.00)
        """)


def _create_countries(cursor) -> None:
    """Create and populate the countries table."""
    cursor.execute("""
        CREATE TABLE countries (
            code VARCHAR(10) PRIMARY KEY,
            name VARCHAR(100),
            enabled BOOLEAN
        )
        """)
    cursor.execute("""
        INSERT INTO countries (code, name, enabled) VALUES
        ('US', 'United States', true),
        ('UK', 'United Kingdom', true),
        ('FR', 'France', true),
        ('DE', 'Germany', false)
        """)


def _build_schema(cursor) -> None:
    """Create the ``pg`` schema and all decorrelation test tables."""
    cursor.execute(f"DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE")
    cursor.execute(f"CREATE SCHEMA {PG_SCHEMA}")
    cursor.execute(f"SET search_path TO {PG_SCHEMA}")
    _create_users(cursor)
    _create_orders(cursor)
    _create_cities(cursor)
    _create_products(cursor)
    _create_sales(cursor)
    _create_offers(cursor)
    _create_limits(cursor)
    _create_countries(cursor)


@pytest.fixture(scope="module")
def setup_test_data(catalog, pg_datasource):
    """Create the ``pg`` schema with sample tables and load catalog metadata."""
    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            _build_schema(cursor)
            conn.commit()

    catalog.load_metadata()

    yield

    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {PG_SCHEMA} CASCADE")
            conn.commit()
