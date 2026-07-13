"""Fixtures for exhaustive pushdown E2E tests, driven by the Rust engine.

Each DuckDB source is a real file (the Rust engine opens files itself; an
in-memory database is invisible to it). A source is seeded through a read-write
DuckDB connection which is then CLOSED, and a fresh READ-ONLY connection is
opened for ground-truth comparisons. Closing the seeding connection avoids any
write-visibility ambiguity when the native engine opens the same file; a
read-only ground-truth connection coexists with the engine's handle.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import duckdb
import pytest

from federated_query.datasources.duckdb import DuckDBDataSource
from tests.duckdb_tmp import duckdb_path


class SeededSource:
    """One seeded DuckDB file: its datasource name, path, and a read-only handle.

    ``connection`` is a read-only DuckDB connection kept open for ground-truth
    queries; it coexists with the native engine's own handle on the same file.
    """

    def __init__(self, name: str, path: str, connection):
        self.name = name
        self.path = path
        self.connection = connection

    def disconnect(self) -> None:
        """Close the read-only ground-truth connection."""
        self.connection.close()


class ProxyingDuckDBDataSource(DuckDBDataSource):
    """A seeded DuckDB file built incrementally: connect, seed, then read.

    Subclasses the real ``DuckDBDataSource`` so a Python ``Catalog`` can load its
    metadata (``is_connected``/``list_schemas``/... come from the base). The write
    connection opened by ``connect`` stays open for seeding; ``path`` is the file
    the engine reads (the base names it ``db_path``, this exposes both).
    """

    @property
    def path(self) -> str:
        """The DuckDB file path (the runtime config is built from this)."""
        return self.db_path


class QueryEnvironment:
    """Catalog-free wrapper carrying the seeded sources for a test.

    ``datasources`` is the list of seeded sources (each exposing ``name``,
    ``path``, and a ground-truth ``connection``). ``source_pairs`` are the
    ``(name, path)`` tuples the Rust runtime config is built from.
    """

    def __init__(self, datasources, catalog=None):
        self.datasources = datasources
        self.catalog = catalog

    def source_pairs(self) -> List[Tuple[str, str]]:
        """Return the (datasource_name, duckdb_file_path) pairs for the config."""
        pairs = []
        for source in self.datasources:
            pairs.append((source.name, source.path))
        return pairs


def _seed_orders(cursor) -> None:
    """Create and populate the canonical orders table."""
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            price DOUBLE,
            status VARCHAR,
            region VARCHAR,
            created_at TIMESTAMP,
            "select" INTEGER
        )
        """)
    cursor.execute("""
        INSERT INTO orders VALUES
        (1, 101, 1, 3, 25.0, 'processing', 'NA', TIMESTAMP '2024-01-05 09:00:00', 0),
        (2, 102, 2, 5, 50.0, 'shipped', 'EU', TIMESTAMP '2024-02-10 12:30:00', 1),
        (3, 103, 3, 2, 75.0, 'processing', 'APAC', TIMESTAMP '2024-02-20 08:15:00', 0),
        (4, 104, 4, 1, 125.0, 'returned', 'NA', TIMESTAMP '2024-03-01 17:45:00', 1),
        (5, 101, 5, 4, 60.0, 'processing', 'EU', TIMESTAMP '2024-03-15 11:00:00', 0),
        (6, 102, 1, 7, 35.0, 'processing', 'NA', TIMESTAMP '2024-04-02 14:20:00', 1),
        (7, 103, 2, 3, 90.0, 'cancelled', 'APAC', TIMESTAMP '2024-04-18 10:05:00', 0),
        (8, 104, 3, 6, 15.0, 'shipped', 'EU', TIMESTAMP '2024-05-09 16:40:00', 1),
        (9, 105, 4, 9, 10.0, 'processing', 'NA', TIMESTAMP '2024-05-22 07:55:00', 0),
        (10, 106, 5, 8, 200.0, 'processing', 'EU', TIMESTAMP '2024-06-01 13:10:00', 1)
        """)


def _seed_products(cursor) -> None:
    """Create and populate the canonical products table."""
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER,
            category VARCHAR,
            name VARCHAR,
            price DOUBLE,
            base_price DOUBLE,
            active BOOLEAN,
            status VARCHAR
        )
        """)
    cursor.execute("""
        INSERT INTO products VALUES
        (101, 'clothing', 'jacket', 20.0, 20.0, TRUE, 'active'),
        (102, 'clothing', 'shirt', 30.0, 30.0, TRUE, 'active'),
        (103, 'electronics', 'tablet', 150.0, 150.0, TRUE, 'premium'),
        (104, 'electronics', 'phone', 300.0, 300.0, TRUE, 'premium'),
        (105, 'home', 'lamp', 40.0, 40.0, FALSE, 'discontinued'),
        (106, 'home', 'desk', 220.0, 220.0, TRUE, 'active'),
        (107, 'home', 'chair', 90.0, 90.0, TRUE, 'active'),
        (108, 'food', 'coffee', 12.0, 12.0, TRUE, 'discontinued')
        """)


def _seed_customers(cursor) -> None:
    """Create and populate the canonical customers table."""
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER,
            segment VARCHAR,
            loyalty VARCHAR
        )
        """)
    cursor.execute("""
        INSERT INTO customers VALUES
        (1, 'enterprise', 'gold'),
        (2, 'enterprise', 'silver'),
        (3, 'smb', 'silver'),
        (4, 'consumer', 'bronze'),
        (5, 'consumer', 'gold')
        """)


def build_seeded_source(name: str, seeds) -> SeededSource:
    """Seed a fresh DuckDB file with the given seed functions, return the source.

    Seeding runs on a read-write connection that is closed afterwards; a
    read-only connection is opened for ground-truth queries.
    """
    path = duckdb_path()
    writer = duckdb.connect(path)
    for seed in seeds:
        seed(writer)
    writer.close()
    reader = duckdb.connect(path, read_only=True)
    return SeededSource(name=name, path=path, connection=reader)


@pytest.fixture(scope="module")
def single_source_env() -> QueryEnvironment:
    """Single DuckDB source with orders/products/customers."""
    source = build_seeded_source(
        "duckdb_primary", (_seed_orders, _seed_products, _seed_customers)
    )
    env = QueryEnvironment(datasources=[source])
    yield env
    source.disconnect()


@pytest.fixture(scope="module")
def multi_source_env() -> QueryEnvironment:
    """Three DuckDB sources to validate cross-source pushdown."""
    orders = build_seeded_source("duckdb_orders", (_seed_orders,))
    products = build_seeded_source("duckdb_products", (_seed_products,))
    customers = build_seeded_source("duckdb_customers", (_seed_customers,))
    env = QueryEnvironment(datasources=[orders, products, customers])
    yield env
    orders.disconnect()
    products.disconnect()
    customers.disconnect()
