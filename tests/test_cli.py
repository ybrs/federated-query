"""Tests for the fedq CLI helpers."""

from federated_query.cli.fedq import FedQRuntime
from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.config import ExecutorConfig


def test_runtime_executes_simple_select():
    """FedQRuntime should execute a basic query end-to-end."""
    datasource = DuckDBDataSource(
        name="mem",
        config={
            "path": ":memory:",
            "read_only": False,
        },
    )
    datasource.connect()
    datasource.connection.execute(
        """
        CREATE TABLE items (
            id INTEGER,
            name VARCHAR
        )
        """
    )
    datasource.connection.execute(
        """
        INSERT INTO items VALUES
        (1, 'alpha'),
        (2, 'beta')
        """
    )

    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()

    runtime = FedQRuntime(catalog, ExecutorConfig())
    table = runtime.execute("SELECT id FROM mem.main.items ORDER BY id")

    assert table.num_rows == 2
    values = table.column(0).to_pylist()
    assert values[0] == 1
    assert values[1] == 2
