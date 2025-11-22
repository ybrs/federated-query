"""
Fixtures for decorrelation e2e tests.

Sets up test databases with sample data that exercises all decorrelation patterns.
"""
import pytest
import pyarrow as pa


@pytest.fixture(scope="module")
def setup_test_data(pg_datasource, duckdb_datasource):
    """
    Create test tables for decorrelation tests.

    Tables:
    - users: id, name, country, city
    - orders: id, user_id, amount, status
    - cities: id, name, country, population
    - products: id, name, price, category
    - sales: id, product_id, seller, price, region
    - offers: id, product_id, seller, amount
    - limits: id, region, cap
    - countries: code, name, enabled
    """
    # Setup PostgreSQL tables
    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            # Clean up any existing tables
            cursor.execute("DROP TABLE IF EXISTS offers CASCADE")
            cursor.execute("DROP TABLE IF EXISTS sales CASCADE")
            cursor.execute("DROP TABLE IF EXISTS limits CASCADE")
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
            cursor.execute("DROP TABLE IF EXISTS products CASCADE")
            cursor.execute("DROP TABLE IF EXISTS cities CASCADE")
            cursor.execute("DROP TABLE IF EXISTS countries CASCADE")
            cursor.execute("DROP TABLE IF EXISTS users CASCADE")

            # Users table
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

            # Orders table
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

            # Cities table
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

            # Products table
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

            # Sales table
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

            # Offers table
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

            # Limits table
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

            # Countries table
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

            conn.commit()

    yield

    # Cleanup after tests
    with pg_datasource.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS offers CASCADE")
            cursor.execute("DROP TABLE IF EXISTS sales CASCADE")
            cursor.execute("DROP TABLE IF EXISTS limits CASCADE")
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
            cursor.execute("DROP TABLE IF EXISTS products CASCADE")
            cursor.execute("DROP TABLE IF EXISTS cities CASCADE")
            cursor.execute("DROP TABLE IF EXISTS countries CASCADE")
            cursor.execute("DROP TABLE IF EXISTS users CASCADE")
            conn.commit()
