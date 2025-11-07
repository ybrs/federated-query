"""Tests for data source connectors."""

import pytest
import duckdb
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.base import DataSourceCapability


@pytest.fixture
def duckdb_datasource():
    """Create an in-memory DuckDB datasource for testing."""
    ds = DuckDBDataSource("test_duck", {"path": ":memory:", "read_only": False})
    ds.connect()

    # Create test schema and tables
    conn = ds.connection
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS main.test_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO main.test_table VALUES
            (1, 'Alice', 100.5),
            (2, 'Bob', 200.75),
            (3, 'Carol', 150.25)
    """)

    yield ds

    # Cleanup
    ds.disconnect()


def test_duckdb_connection(duckdb_datasource):
    """Test DuckDB connection."""
    assert duckdb_datasource.is_connected()
    assert duckdb_datasource.connection is not None


def test_duckdb_capabilities(duckdb_datasource):
    """Test DuckDB capabilities."""
    capabilities = duckdb_datasource.get_capabilities()

    assert DataSourceCapability.AGGREGATIONS in capabilities
    assert DataSourceCapability.JOINS in capabilities
    assert DataSourceCapability.WINDOW_FUNCTIONS in capabilities
    assert DataSourceCapability.SUBQUERIES in capabilities
    assert DataSourceCapability.CTE in capabilities

    # Test supports_capability method
    assert duckdb_datasource.supports_capability(DataSourceCapability.AGGREGATIONS)
    assert duckdb_datasource.supports_capability(DataSourceCapability.JOINS)


def test_duckdb_list_schemas(duckdb_datasource):
    """Test listing schemas."""
    schemas = duckdb_datasource.list_schemas()

    assert "main" in schemas
    assert len(schemas) > 0


def test_duckdb_list_tables(duckdb_datasource):
    """Test listing tables in a schema."""
    tables = duckdb_datasource.list_tables("main")

    assert "test_table" in tables


def test_duckdb_get_table_metadata(duckdb_datasource):
    """Test getting table metadata."""
    metadata = duckdb_datasource.get_table_metadata("main", "test_table")

    assert metadata.schema_name == "main"
    assert metadata.table_name == "test_table"
    assert len(metadata.columns) == 3

    # Check columns
    col_names = [col.name for col in metadata.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "value" in col_names

    # Check nullable status
    id_col = next(col for col in metadata.columns if col.name == "id")
    assert id_col.nullable is False  # PRIMARY KEY is NOT NULL


def test_duckdb_execute_query(duckdb_datasource):
    """Test executing a query and fetching results."""
    query = "SELECT * FROM main.test_table ORDER BY id"

    batches = list(duckdb_datasource.execute_query(query))

    assert len(batches) > 0

    # Check the first batch
    batch = batches[0]
    assert batch.num_rows == 3
    assert batch.num_columns == 3

    # Convert to dict to verify data
    data = batch.to_pydict()
    assert data["id"] == [1, 2, 3]
    assert data["name"] == ["Alice", "Bob", "Carol"]
    assert data["value"] == [100.5, 200.75, 150.25]


def test_duckdb_execute_aggregation_query(duckdb_datasource):
    """Test executing an aggregation query."""
    query = "SELECT COUNT(*) as count, SUM(value) as total FROM main.test_table"

    batches = list(duckdb_datasource.execute_query(query))
    batch = batches[0]
    data = batch.to_pydict()

    assert data["count"][0] == 3
    assert abs(data["total"][0] - 451.5) < 0.01  # Float comparison with tolerance


def test_duckdb_execute_filtered_query(duckdb_datasource):
    """Test executing a query with filters."""
    query = "SELECT * FROM main.test_table WHERE value > 150"

    batches = list(duckdb_datasource.execute_query(query))
    batch = batches[0]
    data = batch.to_pydict()

    assert len(data["id"]) == 2
    assert 2 in data["id"]  # Bob
    assert 3 in data["id"]  # Carol


def test_duckdb_get_table_statistics(duckdb_datasource):
    """Test getting table statistics."""
    stats = duckdb_datasource.get_table_statistics("main", "test_table")

    assert stats is not None
    assert stats.row_count == 3

    # Check column statistics
    assert "id" in stats.column_stats
    id_stats = stats.column_stats["id"]
    assert id_stats.num_distinct == 3
    assert id_stats.null_fraction == 0.0


def test_duckdb_context_manager():
    """Test using data source as context manager."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})

    assert not ds.is_connected()

    with ds:
        assert ds.is_connected()
        ds.connection.execute("CREATE TABLE test (id INTEGER)")
        result = ds.connection.execute("SELECT 1").fetchone()
        assert result[0] == 1

    assert not ds.is_connected()


def test_duckdb_disconnect():
    """Test disconnecting from DuckDB."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})
    ds.connect()

    assert ds.is_connected()

    ds.disconnect()

    assert not ds.is_connected()
    assert ds.connection is None


def test_duckdb_reconnect():
    """Test reconnecting to DuckDB."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})

    ds.connect()
    assert ds.is_connected()

    ds.disconnect()
    assert not ds.is_connected()

    ds.connect()
    assert ds.is_connected()

    ds.disconnect()


def test_duckdb_ensure_connected():
    """Test ensure_connected method."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})

    assert not ds.is_connected()

    ds.ensure_connected()
    assert ds.is_connected()

    # Calling again should not error
    ds.ensure_connected()
    assert ds.is_connected()

    ds.disconnect()


def test_duckdb_multiple_queries(duckdb_datasource):
    """Test executing multiple queries on the same connection."""
    query1 = "SELECT COUNT(*) as count FROM main.test_table"
    query2 = "SELECT MAX(value) as max_value FROM main.test_table"

    # Execute first query
    batches1 = list(duckdb_datasource.execute_query(query1))
    data1 = batches1[0].to_pydict()
    assert data1["count"][0] == 3

    # Execute second query
    batches2 = list(duckdb_datasource.execute_query(query2))
    data2 = batches2[0].to_pydict()
    assert data2["max_value"][0] == 200.75


def test_duckdb_repr():
    """Test string representation of DuckDB datasource."""
    ds = DuckDBDataSource("my_duckdb", {"path": ":memory:"})
    repr_str = repr(ds)

    assert "DuckDBDataSource" in repr_str
    assert "my_duckdb" in repr_str


def test_duckdb_error_handling():
    """Test error handling for invalid queries."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})
    ds.connect()

    # Invalid SQL should raise an exception
    with pytest.raises(Exception):
        list(ds.execute_query("SELECT * FROM nonexistent_table"))

    ds.disconnect()


def test_duckdb_empty_result():
    """Test querying with no results."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})
    ds.connect()

    ds.connection.execute("CREATE TABLE empty_table (id INTEGER)")

    batches = list(ds.execute_query("SELECT * FROM empty_table"))

    # Should have no batches or empty batches
    assert len(batches) == 0 or all(batch.num_rows == 0 for batch in batches)

    ds.disconnect()


def test_duckdb_types():
    """Test various data types."""
    ds = DuckDBDataSource("test", {"path": ":memory:", "read_only": False})
    ds.connect()

    ds.connection.execute("""
        CREATE TABLE type_test (
            int_col INTEGER,
            float_col DOUBLE,
            str_col VARCHAR,
            bool_col BOOLEAN,
            date_col DATE
        )
    """)

    ds.connection.execute("""
        INSERT INTO type_test VALUES
            (42, 3.14, 'hello', true, '2024-01-15')
    """)

    metadata = ds.get_table_metadata("main", "type_test")
    col_names = [col.name for col in metadata.columns]

    assert "int_col" in col_names
    assert "float_col" in col_names
    assert "str_col" in col_names
    assert "bool_col" in col_names
    assert "date_col" in col_names

    ds.disconnect()
