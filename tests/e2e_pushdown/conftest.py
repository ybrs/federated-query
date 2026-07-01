"""Fixtures for exhaustive pushdown E2E tests."""

from __future__ import annotations

from typing import Dict, List, Optional

import sqlglot

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.model import StateModel


class ProxyingDuckDBDataSource(DuckDBDataSource):
    """DuckDB source that captures the most recent query AST for assertions."""

    def __init__(self, name: str, config: Dict[str, str]):
        super().__init__(name, config)
        self._last_ast: Optional[sqlglot.exp.Expression] = None

    def execute_query(self, query: str):
        self._last_ast = sqlglot.parse_one(query)
        return super().execute_query(query)

    def reset_tracking(self) -> None:
        self._last_ast = None

    def last_query_ast(self) -> Optional[sqlglot.exp.Expression]:
        return self._last_ast


class QueryEnvironment(StateModel):
    """Wrapper containing catalog + datasources for pushdown tests."""

    catalog: Catalog
    datasources: List[ProxyingDuckDBDataSource]

    def reset_datasources(self) -> None:
        for ds in self.datasources:
            ds.reset_tracking()

    def snapshot_asts(self) -> Dict[str, sqlglot.exp.Expression]:
        asts: Dict[str, sqlglot.exp.Expression] = {}
        for ds in self.datasources:
            ast = ds.last_query_ast()
            if ast is not None:
                asts[ds.name] = ast
        return asts


def _seed_orders(cursor) -> None:
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


def _build_datasource(name: str) -> QueryCapturingDataSource:
    config = {"database": ":memory:", "read_only": False}
    ds = ProxyingDuckDBDataSource(name=name, config=config)
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
