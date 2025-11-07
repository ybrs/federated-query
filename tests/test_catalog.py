"""Tests for catalog functionality."""

import pytest
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column
from federated_query.plan.expressions import DataType
from federated_query.datasources.duckdb import DuckDBDataSource


@pytest.fixture
def catalog():
    """Create a catalog instance for testing."""
    return Catalog()


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    schema = Schema(name="public", datasource="test_db")

    # Create customers table
    customers_table = Table(
        name="customers",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="name", data_type=DataType.VARCHAR, nullable=False),
            Column(name="email", data_type=DataType.VARCHAR, nullable=False),
            Column(name="region", data_type=DataType.VARCHAR, nullable=True),
        ],
    )
    schema.add_table(customers_table)

    # Create orders table
    orders_table = Table(
        name="orders",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="customer_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="amount", data_type=DataType.DOUBLE, nullable=False),
            Column(name="status", data_type=DataType.VARCHAR, nullable=False),
        ],
    )
    schema.add_table(orders_table)

    return schema


def test_catalog_initialization(catalog):
    """Test catalog initialization."""
    assert len(catalog.datasources) == 0
    assert len(catalog.schemas) == 0
    assert catalog._metadata_loaded is False


def test_register_datasource(catalog):
    """Test registering a data source."""
    ds = DuckDBDataSource("test_duck", {"path": ":memory:"})
    catalog.register_datasource(ds)

    assert "test_duck" in catalog.datasources
    assert catalog.get_datasource("test_duck") is ds


def test_get_nonexistent_datasource(catalog):
    """Test getting a non-existent data source."""
    assert catalog.get_datasource("nonexistent") is None


def test_get_schema(catalog, sample_schema):
    """Test getting a schema."""
    catalog.schemas[("test_db", "public")] = sample_schema

    schema = catalog.get_schema("test_db", "public")
    assert schema is not None
    assert schema.name == "public"
    assert schema.datasource == "test_db"


def test_get_nonexistent_schema(catalog):
    """Test getting a non-existent schema."""
    assert catalog.get_schema("nonexistent", "public") is None


def test_get_table(catalog, sample_schema):
    """Test getting a table."""
    catalog.schemas[("test_db", "public")] = sample_schema

    table = catalog.get_table("test_db", "public", "customers")
    assert table is not None
    assert table.name == "customers"
    assert len(table.columns) == 4


def test_get_nonexistent_table(catalog, sample_schema):
    """Test getting a non-existent table."""
    catalog.schemas[("test_db", "public")] = sample_schema

    assert catalog.get_table("test_db", "public", "nonexistent") is None
    assert catalog.get_table("nonexistent", "public", "customers") is None


def test_resolve_fully_qualified_table(catalog, sample_schema):
    """Test resolving a fully qualified table reference."""
    catalog.schemas[("test_db", "public")] = sample_schema

    result = catalog.resolve_table("test_db.public.customers")
    assert result is not None
    ds, schema, table_name, table = result
    assert ds == "test_db"
    assert schema == "public"
    assert table_name == "customers"
    assert table.name == "customers"


def test_resolve_schema_qualified_table(catalog, sample_schema):
    """Test resolving a schema-qualified table reference."""
    catalog.schemas[("test_db", "public")] = sample_schema

    result = catalog.resolve_table("public.customers")
    assert result is not None
    ds, schema, table_name, table = result
    assert ds == "test_db"
    assert schema == "public"
    assert table_name == "customers"


def test_resolve_unqualified_table(catalog, sample_schema):
    """Test resolving an unqualified table reference."""
    catalog.schemas[("test_db", "public")] = sample_schema

    result = catalog.resolve_table("customers")
    assert result is not None
    ds, schema, table_name, table = result
    assert ds == "test_db"
    assert schema == "public"
    assert table_name == "customers"


def test_resolve_nonexistent_table(catalog, sample_schema):
    """Test resolving a non-existent table."""
    catalog.schemas[("test_db", "public")] = sample_schema

    assert catalog.resolve_table("nonexistent") is None
    assert catalog.resolve_table("test_db.public.nonexistent") is None


def test_resolve_ambiguous_table(catalog):
    """Test resolving a table that exists in multiple schemas."""
    # Create two schemas with the same table name
    schema1 = Schema(name="schema1", datasource="db1")
    schema1.add_table(
        Table(
            name="users",
            columns=[Column(name="id", data_type=DataType.INTEGER, nullable=False)],
        )
    )

    schema2 = Schema(name="schema2", datasource="db2")
    schema2.add_table(
        Table(
            name="users",
            columns=[Column(name="id", data_type=DataType.INTEGER, nullable=False)],
        )
    )

    catalog.schemas[("db1", "schema1")] = schema1
    catalog.schemas[("db2", "schema2")] = schema2

    # Unqualified reference should return the first match
    result = catalog.resolve_table("users")
    assert result is not None

    # Qualified references should be unambiguous
    result1 = catalog.resolve_table("db1.schema1.users")
    result2 = catalog.resolve_table("db2.schema2.users")

    assert result1[0] == "db1"
    assert result2[0] == "db2"


def test_type_mapping(catalog):
    """Test database type mapping."""
    # Integer types
    assert catalog._map_type("INTEGER") == DataType.INTEGER
    assert catalog._map_type("BIGINT") == DataType.BIGINT
    assert catalog._map_type("SERIAL") == DataType.INTEGER
    assert catalog._map_type("BIGSERIAL") == DataType.BIGINT

    # Float types
    assert catalog._map_type("FLOAT") == DataType.FLOAT
    assert catalog._map_type("REAL") == DataType.FLOAT
    assert catalog._map_type("DOUBLE") == DataType.DOUBLE
    assert catalog._map_type("NUMERIC") == DataType.DOUBLE
    assert catalog._map_type("DECIMAL") == DataType.DOUBLE

    # String types
    assert catalog._map_type("VARCHAR") == DataType.VARCHAR
    assert catalog._map_type("CHAR") == DataType.TEXT
    assert catalog._map_type("TEXT") == DataType.TEXT

    # Boolean
    assert catalog._map_type("BOOLEAN") == DataType.BOOLEAN

    # Date/Time
    assert catalog._map_type("DATE") == DataType.DATE
    assert catalog._map_type("TIMESTAMP") == DataType.TIMESTAMP

    # Unknown type defaults to VARCHAR
    assert catalog._map_type("UNKNOWN_TYPE") == DataType.VARCHAR


def test_catalog_repr(catalog):
    """Test catalog string representation."""
    repr_str = repr(catalog)
    assert "Catalog" in repr_str
    assert "datasources=0" in repr_str
    assert "schemas=0" in repr_str


def test_schema_repr(sample_schema):
    """Test schema string representation."""
    repr_str = repr(sample_schema)
    assert "Schema" in repr_str
    assert "test_db.public" in repr_str
    assert "tables=2" in repr_str


def test_table_repr(sample_schema):
    """Test table string representation."""
    table = sample_schema.get_table("customers")
    repr_str = repr(table)
    assert "Table" in repr_str
    assert "customers" in repr_str
    assert "cols=4" in repr_str


def test_column_repr():
    """Test column string representation."""
    column = Column(name="id", data_type=DataType.INTEGER, nullable=False)
    repr_str = repr(column)
    assert "Column" in repr_str
    assert "id" in repr_str
    assert "INTEGER" in repr_str


def test_fully_qualified_names(sample_schema):
    """Test fully qualified name generation."""
    catalog = Catalog()
    catalog.schemas[("test_db", "public")] = sample_schema

    table = catalog.get_table("test_db", "public", "customers")
    assert table.fully_qualified_name() == "test_db.public.customers"

    column = table.get_column("id")
    assert column.fully_qualified_name() == "test_db.public.customers.id"


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


def test_column_case_insensitive_lookup(sample_schema):
    """Test that column lookups are case-insensitive."""
    table = sample_schema.get_table("customers")

    assert table.get_column("id") is not None
    assert table.get_column("ID") is not None
    assert table.get_column("Id") is not None
    assert table.get_column("iD") is not None


def test_table_case_insensitive_lookup():
    """Test that table lookups are case-insensitive."""
    schema = Schema(name="public", datasource="test_db")
    table = Table(
        name="MyTable",
        columns=[Column(name="id", data_type=DataType.INTEGER, nullable=False)],
    )
    schema.add_table(table)

    assert schema.get_table("mytable") is not None
    assert schema.get_table("MYTABLE") is not None
    assert schema.get_table("MyTable") is not None
