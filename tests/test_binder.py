"""Tests for the Binder."""

import pytest
from federated_query.parser import Parser, Binder, BindingError
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column
from federated_query.plan.expressions import DataType


@pytest.fixture
def catalog_with_test_data():
    """Create a catalog with test data."""
    catalog = Catalog()

    # Create a test schema
    schema = Schema(name="public", datasource="testdb")

    # Create users table
    users_table = Table(
        name="users",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="name", data_type=DataType.VARCHAR, nullable=False),
            Column(name="age", data_type=DataType.INTEGER, nullable=True),
            Column(name="email", data_type=DataType.VARCHAR, nullable=True),
        ],
    )
    schema.add_table(users_table)

    orders_table = Table(
        name="orders",
        columns=[
            Column(name="order_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="amount", data_type=DataType.INTEGER, nullable=True),
        ],
    )
    schema.add_table(orders_table)

    # Register schema
    catalog.schemas[("testdb", "public")] = schema

    return catalog


def test_bind_simple_scan(catalog_with_test_data):
    """Test binding a simple scan."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL
    sql = "SELECT id, name FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    # Should not raise any errors
    assert bound_plan is not None


def test_bind_scan_with_invalid_table(catalog_with_test_data):
    """Test binding fails with invalid table."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with invalid table
    sql = "SELECT id FROM testdb.public.nonexistent"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Should raise BindingError
    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)

    assert "Table not found" in str(exc_info.value)


def test_bind_scan_with_invalid_column(catalog_with_test_data):
    """Test binding fails with invalid column."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with invalid column
    sql = "SELECT id, invalid_column FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Should raise BindingError
    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)

    assert "Column" in str(exc_info.value)
    assert "not found" in str(exc_info.value)


def test_bind_with_where_clause(catalog_with_test_data):
    """Test binding with WHERE clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with WHERE
    sql = "SELECT name, age FROM testdb.public.users WHERE age > 18"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_resolves_column_types(catalog_with_test_data):
    """Test that binding resolves column types."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL
    sql = "SELECT id, name FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    # Extract the Projection node
    from federated_query.plan.logical import Projection, Filter, Limit

    # Traverse to find Projection
    current = bound_plan
    while isinstance(current, Limit):
        current = current.input

    if isinstance(current, Projection):
        # Check that expressions have types
        for expr in current.expressions:
            from federated_query.plan.expressions import ColumnRef
            if isinstance(expr, ColumnRef):
                # Should have type set
                assert expr.data_type is not None


def test_bind_with_limit(catalog_with_test_data):
    """Test binding with LIMIT clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with LIMIT
    sql = "SELECT id, name FROM testdb.public.users LIMIT 10"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_with_star_column(catalog_with_test_data):
    """Test binding with SELECT *."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with *
    sql = "SELECT * FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan - should not fail even with *
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_complex_where(catalog_with_test_data):
    """Test binding with complex WHERE clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with complex WHERE
    sql = "SELECT name FROM testdb.public.users WHERE age > 18 AND name LIKE 'John%'"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_order_by_alias(catalog_with_test_data):
    """Binder should resolve ORDER BY alias to source column."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    sql = "SELECT order_id AS oid FROM testdb.public.orders ORDER BY oid"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    bound_plan = binder.bind(plan)

    from federated_query.plan.logical import Projection, Sort

    assert isinstance(bound_plan, Sort)
    assert isinstance(bound_plan.input, Projection)
    sort_node = bound_plan
    project = bound_plan.input
    assert isinstance(sort_node.sort_keys[0], type(project.expressions[0]))
    assert sort_node.sort_keys[0].column == "order_id"
