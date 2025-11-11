"""End-to-end tests for aggregation queries (Phase 3)."""

import pytest
import duckdb
import pyarrow as pa

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig


@pytest.fixture
def setup_test_db():
    """Create in-memory DuckDB with test data."""
    config = {
        "database": ":memory:",
        "read_only": False,
    }
    datasource = DuckDBDataSource(name="test_db", config=config)
    datasource.connect()

    datasource.connection.execute("""
        CREATE TABLE orders (
            id INTEGER,
            region VARCHAR,
            amount DOUBLE,
            quantity INTEGER
        )
    """)

    datasource.connection.execute("""
        INSERT INTO orders VALUES
        (1, 'North', 100.0, 5),
        (2, 'South', 200.0, 10),
        (3, 'North', 150.0, 8),
        (4, 'East', 300.0, 15),
        (5, 'South', 250.0, 12),
        (6, 'North', 180.0, 9),
        (7, 'East', 220.0, 11)
    """)

    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    yield catalog, datasource

    datasource.disconnect()


def test_simple_count(setup_test_db):
    """Test simple COUNT(*) aggregation."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT COUNT(*) as total_count
        FROM test_db.main.orders
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

    assert result_table.num_rows == 1
    assert result_table.column(0)[0].as_py() == 7


def test_group_by_with_count(setup_test_db):
    """Test GROUP BY with COUNT aggregation."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, COUNT(*) as order_count
        FROM test_db.main.orders
        GROUP BY region
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

    assert result_table.num_rows == 3

    regions = []
    counts = []
    for i in range(result_table.num_rows):
        regions.append(result_table.column(0)[i].as_py())
        counts.append(result_table.column(1)[i].as_py())

    region_counts = dict(zip(regions, counts))
    assert region_counts['North'] == 3
    assert region_counts['South'] == 2
    assert region_counts['East'] == 2


def test_group_by_with_sum(setup_test_db):
    """Test GROUP BY with SUM aggregation."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, SUM(amount) as total_amount
        FROM test_db.main.orders
        GROUP BY region
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

    assert result_table.num_rows == 3

    regions = []
    sums = []
    for i in range(result_table.num_rows):
        regions.append(result_table.column(0)[i].as_py())
        sums.append(result_table.column(1)[i].as_py())

    region_sums = dict(zip(regions, sums))
    assert region_sums['North'] == 430.0
    assert region_sums['South'] == 450.0
    assert region_sums['East'] == 520.0


def test_group_by_with_avg(setup_test_db):
    """Test GROUP BY with AVG aggregation."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, AVG(amount) as avg_amount
        FROM test_db.main.orders
        GROUP BY region
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

    assert result_table.num_rows == 3

    regions = []
    avgs = []
    for i in range(result_table.num_rows):
        regions.append(result_table.column(0)[i].as_py())
        avgs.append(result_table.column(1)[i].as_py())

    region_avgs = dict(zip(regions, avgs))
    assert abs(region_avgs['North'] - 143.33) < 0.01
    assert abs(region_avgs['South'] - 225.0) < 0.01
    assert abs(region_avgs['East'] - 260.0) < 0.01


def test_group_by_with_min_max(setup_test_db):
    """Test GROUP BY with MIN and MAX aggregations."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, MIN(amount) as min_amount, MAX(amount) as max_amount
        FROM test_db.main.orders
        GROUP BY region
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

    assert result_table.num_rows == 3

    regions = []
    mins = []
    maxs = []
    for i in range(result_table.num_rows):
        regions.append(result_table.column(0)[i].as_py())
        mins.append(result_table.column(1)[i].as_py())
        maxs.append(result_table.column(2)[i].as_py())

    region_data = {}
    for i in range(len(regions)):
        region_data[regions[i]] = (mins[i], maxs[i])

    assert region_data['North'] == (100.0, 180.0)
    assert region_data['South'] == (200.0, 250.0)
    assert region_data['East'] == (220.0, 300.0)


def test_group_by_multiple_aggregates(setup_test_db):
    """Test GROUP BY with multiple aggregate functions."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg
        FROM test_db.main.orders
        GROUP BY region
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

    assert result_table.num_rows == 3
    assert result_table.num_columns == 4


@pytest.mark.skip(reason="HAVING clause evaluation not yet implemented - planned for Phase 4")
def test_having_clause(setup_test_db):
    """Test HAVING clause with aggregation."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, COUNT(*) as order_count
        FROM test_db.main.orders
        GROUP BY region
        HAVING COUNT(*) > 2
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

    assert result_table.num_rows == 1

    region = result_table.column(0)[0].as_py()
    count = result_table.column(1)[0].as_py()

    assert region == 'North'
    assert count == 3


@pytest.mark.skip(reason="HAVING clause evaluation not yet implemented - planned for Phase 4")
def test_having_with_sum(setup_test_db):
    """Test HAVING clause with SUM."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT region, SUM(amount) as total_amount
        FROM test_db.main.orders
        GROUP BY region
        HAVING SUM(amount) >= 500
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

    assert result_table.num_rows == 1

    region = result_table.column(0)[0].as_py()
    total = result_table.column(1)[0].as_py()

    assert region == 'East'
    assert total == 520.0


def test_aggregation_without_group_by(setup_test_db):
    """Test aggregation without GROUP BY (full table aggregation)."""
    catalog, datasource = setup_test_db

    sql = """
        SELECT COUNT(*) as total_orders, SUM(amount) as total_revenue
        FROM test_db.main.orders
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

    assert result_table.num_rows == 1
    assert result_table.column(0)[0].as_py() == 7
    assert result_table.column(1)[0].as_py() == 1400.0
