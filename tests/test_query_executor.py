"""End-to-end tests for QueryExecutor with star expansion."""

from typing import Tuple

import pyarrow as pa

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.executor import Executor
from federated_query.optimizer import PhysicalPlanner, RuleBasedOptimizer
from federated_query.parser import Binder, Parser
from federated_query.processor import QueryExecutor as PipelineExecutor
from federated_query.processor import StarExpansionProcessor

_CREATE_USERS_SQL = """
CREATE TABLE users (
    id INTEGER,
    name VARCHAR
)
"""

_INSERT_USERS_SQL = """
INSERT INTO users VALUES
(1, 'Alice'),
(2, 'Bob')
"""


def _create_duckdb_catalog() -> Tuple[Catalog, DuckDBDataSource]:
    """Create a catalog with an in-memory DuckDB datasource."""
    datasource = DuckDBDataSource(
        name="testdb",
        config={"path": ":memory:", "read_only": False},
    )
    datasource.connect()
    datasource.connection.execute(_CREATE_USERS_SQL)
    datasource.connection.execute(_INSERT_USERS_SQL)
    catalog = Catalog()
    catalog.register_datasource(datasource)
    catalog.load_metadata()
    return catalog, datasource


def _build_pipeline(catalog: Catalog) -> PipelineExecutor:
    """Construct a QueryExecutor with star expansion."""
    parser = Parser()
    binder = Binder(catalog)
    optimizer = RuleBasedOptimizer(catalog)
    planner = PhysicalPlanner(catalog)
    physical_executor = Executor()
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


def test_query_executor_runs_with_star_projection() -> None:
    """QueryExecutor expands stars and returns user-visible column names."""
    catalog, datasource = _create_duckdb_catalog()
    pipeline = _build_pipeline(catalog)

    try:
        result = pipeline.execute("SELECT * FROM testdb.main.users")
        assert isinstance(result, pa.Table)
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 2
    finally:
        datasource.disconnect()
