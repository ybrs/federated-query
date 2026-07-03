"""End-to-end tests for join queries."""

import pyarrow as pa
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column, DataType
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser import Parser, Binder
from federated_query.optimizer.physical_planner import PhysicalPlanner
from federated_query.executor.executor import Executor
from federated_query.config.config import ExecutorConfig
from federated_query.optimizer import RuleBasedOptimizer
from federated_query.processor import QueryExecutor as PipelineExecutor
from federated_query.processor import StarExpansionProcessor
from tests.duckdb_tmp import duckdb_path


def setup_test_db():
    """Create a file-backed DuckDB source populated with join test data."""
    datasource = DuckDBDataSource(
        name="testdb", config={"path": duckdb_path(), "read_only": False}
    )
    datasource.connect()
    conn = datasource.connection

    conn.execute("""
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            city VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO customers VALUES
            (1, 'Alice', 'NYC'),
            (2, 'Bob', 'LA'),
            (3, 'Charlie', 'SF')
    """)

    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            amount DOUBLE,
            product VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
            (101, 1, 100.0, 'Widget'),
            (102, 1, 150.0, 'Gadget'),
            (103, 2, 200.0, 'Widget'),
            (104, 3, 75.0, 'Gadget')
    """)

    return datasource


def setup_catalog(datasource):
    """Set up catalog with the given data source."""
    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()
    return catalog


def build_query_executor(catalog: Catalog) -> PipelineExecutor:
    """Construct a QueryExecutor with star expansion middleware."""
    parser = Parser()
    binder = Binder(catalog)
    optimizer = RuleBasedOptimizer(catalog)
    planner = PhysicalPlanner(catalog)
    physical_executor = Executor(ExecutorConfig())
    processors = [StarExpansionProcessor(catalog, dialect=parser.dialect)]
    return PipelineExecutor(
        catalog=catalog,
        parser=parser,
        binder=binder,
        optimizer=optimizer,
        planner=planner,
        physical_executor=physical_executor,
        processors=processors,
    )


def test_simple_inner_join():
    """Test simple INNER JOIN query."""
    datasource = setup_test_db()
    catalog = setup_catalog(datasource)

    sql = """
        SELECT c.name, o.order_id, o.amount
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    executor = build_query_executor(catalog)
    result = executor.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4

    names = [row.as_py() for row in result.column("name")]
    assert "Alice" in names
    assert "Bob" in names
    assert "Charlie" in names

    datasource.disconnect()


def test_join_with_where():
    """Test JOIN with WHERE clause."""
    datasource = setup_test_db()
    catalog = setup_catalog(datasource)

    sql = """
        SELECT c.name, o.amount
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
        WHERE o.amount > 100
    """

    executor = build_query_executor(catalog)
    result = executor.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 2

    amounts = [row.as_py() for row in result.column("amount")]
    assert all(amt > 100 for amt in amounts)

    datasource.disconnect()


def test_join_specific_columns():
    """Test JOIN selecting specific columns."""
    datasource = setup_test_db()
    catalog = setup_catalog(datasource)

    sql = """
        SELECT c.id, c.name, o.order_id
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    executor = build_query_executor(catalog)
    result = executor.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4
    assert result.num_columns == 3

    assert result.schema.names == ["id", "name", "order_id"]

    datasource.disconnect()


def test_join_all_columns():
    """Test JOIN with SELECT *."""
    datasource = setup_test_db()
    catalog = setup_catalog(datasource)

    sql = """
        SELECT *
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    executor = build_query_executor(catalog)
    result = executor.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4
    assert result.num_columns == 7

    datasource.disconnect()


def test_parser_creates_join_plan():
    """Test that parser creates Join logical plan node."""
    datasource = setup_test_db()
    catalog = setup_catalog(datasource)

    sql = """
        SELECT c.name, o.amount
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    parser = Parser()
    logical_plan = parser.parse_to_logical_plan(sql, catalog)

    from federated_query.plan.logical import Join, Projection

    assert isinstance(logical_plan, Projection)
    assert isinstance(logical_plan.input, Join)

    join_node = logical_plan.input
    assert join_node.condition is not None

    datasource.disconnect()
