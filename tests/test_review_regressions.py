"""Regression tests capturing issues from review.md."""

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig


def _run_query(catalog: Catalog, sql: str):
    """Execute a SQL string through the engine."""
    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor(ExecutorConfig())

    ast = parser.parse(sql)
    logical_plan = parser.ast_to_logical_plan(ast)
    bound_plan = binder.bind(logical_plan)
    physical_plan = planner.plan(bound_plan)

    return executor.execute_to_table(physical_plan)


def _create_orders_table(datasource: DuckDBDataSource) -> None:
    """Create orders table."""
    datasource.connection.execute(
        "CREATE TABLE orders (id INTEGER, region VARCHAR, amount DOUBLE)"
    )


def _insert_orders_rows(datasource: DuckDBDataSource) -> None:
    """Populate orders rows."""
    datasource.connection.execute(
        "INSERT INTO orders VALUES"
        " (1, 'North', 100.0),"
        " (2, 'South', 200.0),"
        " (3, 'North', 150.0),"
        " (4, 'East', 300.0)"
    )


def _create_warehouses_table(datasource: DuckDBDataSource) -> None:
    """Create warehouses table."""
    datasource.connection.execute(
        "CREATE TABLE warehouses (region VARCHAR, capacity INTEGER)"
    )


def _insert_warehouses_rows(datasource: DuckDBDataSource) -> None:
    """Populate warehouses rows."""
    datasource.connection.execute(
        "INSERT INTO warehouses VALUES"
        " ('North', 10),"
        " ('East', 20)"
    )


def _create_orders_data(datasource: DuckDBDataSource) -> None:
    """Create tables and data for aggregation and join tests."""
    _create_orders_table(datasource)
    _insert_orders_rows(datasource)
    _create_warehouses_table(datasource)
    _insert_warehouses_rows(datasource)


@pytest.fixture
def catalog_with_orders():
    """Create catalog with tables for aggregation and joins."""
    config = {"database": ":memory:", "read_only": False}
    datasource = DuckDBDataSource("test_db", config)
    datasource.connect()
    _create_orders_data(datasource)
    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    yield catalog, datasource

    datasource.disconnect()


@pytest.fixture
def catalog_with_quoted_identifiers():
    """Create catalog with identifiers requiring quoting."""
    config = {"database": ":memory:", "read_only": False}
    datasource = DuckDBDataSource("quoted_ids", config)
    datasource.connect()
    datasource.connection.execute('CREATE SCHEMA "Mixed-Case"')
    datasource.connection.execute(
        'CREATE TABLE "Mixed-Case"."Order Table" (id INTEGER)'
    )
    datasource.connection.execute(
        'INSERT INTO "Mixed-Case"."Order Table" VALUES (1)'
    )

    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    yield catalog, datasource

    datasource.disconnect()


def test_aggregate_output_order_with_aggregate_first(catalog_with_orders):
    """Aggregates before group keys should keep column alignment."""
    catalog, _ = catalog_with_orders
    sql = (
        "SELECT SUM(amount) AS total_amount, region "
        "FROM test_db.main.orders GROUP BY region"
    )
    result_table = _run_query(catalog, sql)

    totals_by_region = {}
    index = 0
    while index < result_table.num_rows:
        total_value = result_table.column(0)[index].as_py()
        region_value = result_table.column(1)[index].as_py()
        totals_by_region[region_value] = total_value
        index += 1

    assert totals_by_region["North"] == pytest.approx(250.0)


def test_left_join_emits_unmatched_rows(catalog_with_orders):
    """LEFT JOIN should return unmatched probe rows with NULL build columns."""
    catalog, _ = catalog_with_orders
    sql = (
        "SELECT o.id, w.capacity "
        "FROM test_db.main.orders AS o "
        "LEFT JOIN test_db.main.warehouses AS w ON o.region = w.region"
    )
    result_table = _run_query(catalog, sql)

    unmatched_rows = 0
    index = 0
    while index < result_table.num_rows:
        capacity_value = result_table.column(1)[index].as_py()
        if capacity_value is None:
            unmatched_rows += 1
        index += 1

    assert unmatched_rows > 0


def test_duckdb_statistics_handles_quoted_identifiers(
    catalog_with_quoted_identifiers,
):
    """Metadata queries should handle identifiers needing quotes."""
    _, datasource = catalog_with_quoted_identifiers
    stats = datasource.get_table_statistics("Mixed-Case", "Order Table")
    assert stats.row_count == 1
