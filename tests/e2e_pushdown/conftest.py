"""Fixtures for exhaustive pushdown E2E tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource


class QueryCapturingDataSource(DuckDBDataSource):
    """DuckDB source that records every executed query for pushdown assertions."""

    def __init__(self, name: str, config: Dict[str, str]):
        super().__init__(name, config)
        self.captured_queries: List[str] = []

    def execute_query(self, query: str):
        self.captured_queries.append(query)
        return super().execute_query(query)

    def clear_queries(self) -> None:
        self.captured_queries = []


@dataclass
class QueryEnvironment:
    """Wrapper containing catalog + datasources for pushdown tests."""

    catalog: Catalog
    datasources: List[QueryCapturingDataSource]

    def clear_queries(self) -> None:
        for ds in self.datasources:
            ds.clear_queries()

    def query_log(self) -> Dict[str, List[str]]:
        return {ds.name: list(ds.captured_queries) for ds in self.datasources}


def _seed_orders(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            price DOUBLE,
            status VARCHAR,
            region VARCHAR
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO orders VALUES
        (1, 101, 1, 3, 25.0, 'processing', 'NA'),
        (2, 102, 2, 5, 50.0, 'shipped', 'EU'),
        (3, 103, 3, 2, 75.0, 'processing', 'APAC'),
        (4, 104, 4, 1, 125.0, 'returned', 'NA'),
        (5, 101, 5, 4, 60.0, 'processing', 'EU'),
        (6, 102, 1, 7, 35.0, 'processing', 'NA'),
        (7, 103, 2, 3, 90.0, 'cancelled', 'APAC'),
        (8, 104, 3, 6, 15.0, 'shipped', 'EU'),
        (9, 105, 4, 9, 10.0, 'processing', 'NA'),
        (10, 106, 5, 8, 200.0, 'processing', 'EU')
        """
    )


def _seed_products(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE products (
            id INTEGER,
            category VARCHAR,
            name VARCHAR,
            base_price DOUBLE,
            active BOOLEAN
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO products VALUES
        (101, 'clothing', 'jacket', 20.0, TRUE),
        (102, 'clothing', 'shirt', 30.0, TRUE),
        (103, 'electronics', 'tablet', 150.0, TRUE),
        (104, 'electronics', 'phone', 300.0, TRUE),
        (105, 'home', 'lamp', 40.0, FALSE),
        (106, 'home', 'desk', 220.0, TRUE),
        (107, 'home', 'chair', 90.0, TRUE),
        (108, 'food', 'coffee', 12.0, TRUE)
        """
    )


def _seed_customers(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER,
            segment VARCHAR,
            loyalty VARCHAR
        )
        """
    )
    cursor.execute(
        """
        INSERT INTO customers VALUES
        (1, 'enterprise', 'gold'),
        (2, 'enterprise', 'silver'),
        (3, 'smb', 'silver'),
        (4, 'consumer', 'bronze'),
        (5, 'consumer', 'gold')
        """
    )


def _build_datasource(name: str) -> QueryCapturingDataSource:
    config = {"database": ":memory:", "read_only": False}
    ds = QueryCapturingDataSource(name=name, config=config)
    ds.connect()
    return ds


@pytest.fixture(scope="module")
def single_source_env() -> QueryEnvironment:
    """Single DuckDB source with orders/products/customers."""

    ds = _build_datasource("duckdb_primary")
    cursor = ds.connection
    _seed_orders(cursor)
    _seed_products(cursor)
    _seed_customers(cursor)

    catalog = Catalog()
    catalog.register_datasource(ds)
    catalog.load_metadata()

    env = QueryEnvironment(catalog=catalog, datasources=[ds])
    yield env
    ds.disconnect()


@pytest.fixture(scope="module")
def multi_source_env() -> QueryEnvironment:
    """Two DuckDB sources to validate cross-source guardrails."""

    ds_orders = _build_datasource("duckdb_orders")
    _seed_orders(ds_orders.connection)

    ds_products = _build_datasource("duckdb_products")
    _seed_products(ds_products.connection)

    ds_customers = _build_datasource("duckdb_customers")
    _seed_customers(ds_customers.connection)

    catalog = Catalog()
    for ds in (ds_orders, ds_products, ds_customers):
        catalog.register_datasource(ds)
    catalog.load_metadata()

    env = QueryEnvironment(
        catalog=catalog,
        datasources=[ds_orders, ds_products, ds_customers],
    )

    yield env

    ds_orders.disconnect()
    ds_products.disconnect()
    ds_customers.disconnect()
