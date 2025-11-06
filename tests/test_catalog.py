"""Tests for catalog."""

import pytest
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column
from federated_query.plan.expressions import DataType


def test_catalog_initialization():
    """Test catalog initialization."""
    catalog = Catalog()
    assert len(catalog.datasources) == 0
    assert len(catalog.schemas) == 0


def test_schema_table_column():
    """Test schema, table, and column creation."""
    # Create column
    col1 = Column(name="id", data_type=DataType.INTEGER, nullable=False)
    col2 = Column(name="name", data_type=DataType.VARCHAR, nullable=True)

    # Create table
    table = Table(name="users", columns=[col1, col2])

    assert len(table.columns) == 2
    assert table.get_column("id") == col1
    assert table.get_column("name") == col2

    # Create schema
    schema = Schema(name="public", datasource="postgres")
    schema.add_table(table)

    assert schema.get_table("users") == table
    assert table.schema == schema
