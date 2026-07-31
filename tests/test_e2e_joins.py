"""End-to-end tests for join queries."""

import duckdb
import pyarrow as pa
from tests.duckdb_tmp import duckdb_path
from tests.rust_runtime import RustRuntime


def _seed_join_tables(conn):
    """Populate the customers/orders tables used by every join test."""
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


def make_runtime():
    """Seed a fresh DuckDB file and return a Rust-engine runtime over it."""
    path = duckdb_path()
    writer = duckdb.connect(path)
    _seed_join_tables(writer)
    writer.close()
    return RustRuntime([("testdb", path)])


def test_simple_inner_join():
    """Test simple INNER JOIN query."""
    runtime = make_runtime()

    sql = """
        SELECT c.name, o.order_id, o.amount
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    result = runtime.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4

    names = [row.as_py() for row in result.column("name")]
    assert "Alice" in names
    assert "Bob" in names
    assert "Charlie" in names


def test_join_with_where():
    """Test JOIN with WHERE clause."""
    runtime = make_runtime()

    sql = """
        SELECT c.name, o.amount
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
        WHERE o.amount > 100
    """

    result = runtime.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 2

    amounts = [row.as_py() for row in result.column("amount")]
    assert all(amt > 100 for amt in amounts)


def test_join_specific_columns():
    """Test JOIN selecting specific columns."""
    runtime = make_runtime()

    sql = """
        SELECT c.id, c.name, o.order_id
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    result = runtime.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4
    assert result.num_columns == 3

    assert result.schema.names == ["id", "name", "order_id"]


def test_join_all_columns():
    """Test JOIN with SELECT *."""
    runtime = make_runtime()

    sql = """
        SELECT *
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    result = runtime.execute(sql)
    assert isinstance(result, pa.Table)

    assert result.num_rows == 4
    assert result.num_columns == 7
