"""End-to-end tests for simple SELECT queries."""

import pytest
import pyarrow as pa
from federated_query.parser import Parser, Binder
from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.optimizer import (
    PhysicalPlanner,
    RuleBasedOptimizer,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    LimitPushdownRule,
    ExpressionSimplificationRule,
)
from federated_query.executor import Executor


def create_optimizer(catalog: Catalog) -> RuleBasedOptimizer:
    """Create optimizer with standard rules."""
    optimizer = RuleBasedOptimizer(catalog)
    optimizer.add_rule(ExpressionSimplificationRule())
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ProjectionPushdownRule())
    optimizer.add_rule(LimitPushdownRule())
    return optimizer


@pytest.fixture
def setup_duckdb():
    """Set up DuckDB with test data."""
    # Create in-memory DuckDB
    config = {
        "database": ":memory:",
        "read_only": False,
    }
    datasource = DuckDBDataSource(name="testdb", config=config)
    datasource.connect()

    # Create test table
    datasource.connection.execute("""
        CREATE TABLE users (
            id INTEGER,
            name VARCHAR,
            age INTEGER,
            email VARCHAR
        )
    """)

    # Insert test data
    datasource.connection.execute("""
        INSERT INTO users VALUES
        (1, 'Alice', 25, 'alice@example.com'),
        (2, 'Bob', 30, 'bob@example.com'),
        (3, 'Charlie', 35, 'charlie@example.com'),
        (4, 'Diana', 28, 'diana@example.com'),
        (5, 'Eve', 22, 'eve@example.com')
    """)

    # Create catalog
    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    yield catalog, datasource

    datasource.disconnect()


def test_simple_select_all(setup_duckdb):
    """Test SELECT * FROM table."""
    catalog, datasource = setup_duckdb

    # Parse
    parser = Parser()
    sql = "SELECT * FROM testdb.main.users"
    logical_plan = parser.parse_to_logical_plan(sql)

    # Bind
    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    # Optimize
    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    # Physical planning
    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    # Execute
    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    rows = []
    for batch in results:
        rows.extend(batch.to_pydict()["id"])

    # Verify
    assert len(rows) == 5
    assert set(rows) == {1, 2, 3, 4, 5}


def test_select_specific_columns(setup_duckdb):
    """Test SELECT id, name FROM table."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "SELECT id, name FROM testdb.main.users"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    batches = list(results)
    assert len(batches) > 0

    first_batch = batches[0]
    assert first_batch.num_columns == 2
    assert first_batch.schema.names == ["id", "name"]
    assert first_batch.num_rows == 5


def test_select_with_where(setup_duckdb):
    """Test SELECT * FROM table WHERE age > 25."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "SELECT id, name FROM testdb.main.users WHERE age > 25"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    ids = []
    for batch in results:
        ids.extend(batch.to_pydict()["id"])

    # Should get Bob (30), Charlie (35), Diana (28)
    assert len(ids) == 3
    assert set(ids) == {2, 3, 4}


def test_select_with_limit(setup_duckdb):
    """Test SELECT * FROM table LIMIT 2."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "SELECT id FROM testdb.main.users LIMIT 2"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    total_rows = 0
    for batch in results:
        total_rows += batch.num_rows

    # Should get exactly 2 rows
    assert total_rows == 2


def test_select_with_where_and_limit(setup_duckdb):
    """Test SELECT * FROM table WHERE age > 25 LIMIT 2."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "SELECT id FROM testdb.main.users WHERE age > 25 LIMIT 2"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    total_rows = 0
    for batch in results:
        total_rows += batch.num_rows

    # Should get exactly 2 rows out of 3 matching
    assert total_rows == 2


def test_select_with_complex_where(setup_duckdb):
    """Test SELECT with complex WHERE clause."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "SELECT name FROM testdb.main.users WHERE age > 20 AND age < 30"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    results = executor.execute(physical_plan)

    # Collect results
    names = []
    for batch in results:
        names.extend(batch.to_pydict()["name"])

    # Should get Alice (25), Diana (28), Eve (22)
    assert len(names) == 3
    assert set(names) == {"Alice", "Diana", "Eve"}


def test_full_pipeline(setup_duckdb):
    """Test the complete query pipeline."""
    catalog, datasource = setup_duckdb

    # This tests: Parse -> Bind -> Plan -> Execute
    parser = Parser()
    binder = Binder(catalog)
    planner = PhysicalPlanner(catalog)
    executor = Executor()

    sql = "SELECT id, name, age FROM testdb.main.users WHERE age >= 30"

    # Step 1: Parse
    logical_plan = parser.parse_to_logical_plan(sql)
    assert logical_plan is not None

    # Step 2: Bind
    bound_plan = binder.bind(logical_plan)
    assert bound_plan is not None

    # Step 3: Physical planning
    physical_plan = planner.plan(bound_plan)
    assert physical_plan is not None

    # Step 4: Execute
    results = executor.execute(physical_plan)

    # Verify results
    ids = []
    for batch in results:
        ids.extend(batch.to_pydict()["id"])

    # Should get Bob (30) and Charlie (35)
    assert len(ids) == 2
    assert set(ids) == {2, 3}


def test_explain_returns_plan(setup_duckdb):
    """EXPLAIN should return textual plan rows instead of executing."""
    catalog, datasource = setup_duckdb

    parser = Parser()
    sql = "EXPLAIN SELECT id FROM testdb.main.users WHERE age > 25"
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = create_optimizer(catalog)
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    batches = list(executor.execute(physical_plan))

    assert len(batches) == 1
    batch = batches[0]
    assert batch.schema.names == ["plan"]

    plan_lines = []
    column = batch.column(0)
    for entry in column.to_pylist():
        plan_lines.append(entry)

    assert len(plan_lines) > 0
    assert plan_lines[0].startswith("PhysicalProject")

    has_scan = False
    for line in plan_lines:
        if "PhysicalScan" in line:
            has_scan = True
            break

    assert has_scan

    saw_queries_header = False
    saw_query_line = False
    for line in plan_lines:
        if line == "Queries:":
            saw_queries_header = True
        if line.startswith("- testdb"):
            saw_query_line = True
    assert saw_queries_header
    assert saw_query_line
