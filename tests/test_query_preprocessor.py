"""Tests for query preprocessing and star expansion middleware."""

import pyarrow as pa
import pytest

from federated_query.catalog import Catalog
from federated_query.catalog.schema import Column, Schema, Table
from federated_query.plan.expressions import DataType
from federated_query.processor import (
    QueryContext,
    QueryPreprocessor,
    StarExpansionError,
    StarExpansionProcessor,
)


def _build_catalog() -> Catalog:
    """Create an in-memory catalog with a single users table."""
    catalog = Catalog()
    schema = Schema(name="main", datasource="testdb")
    users_table = Table(
        name="users",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="name", data_type=DataType.VARCHAR, nullable=False),
        ],
    )
    schema.add_table(users_table)
    catalog.schemas[("testdb", "main")] = schema
    return catalog


def test_preprocess_expands_star_projection() -> None:
    """Ensure SELECT * expands using catalog metadata."""
    catalog = _build_catalog()
    context = QueryContext("SELECT * FROM testdb.main.users")
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)

    assert "users.id" in rewritten
    assert "users.name" in rewritten
    assert len(context.columns) == 2
    assert context.columns[0].internal_name == "testdb.main.users.id"
    assert context.columns[0].visible_name == "id"


def test_preprocess_handles_alias_stars() -> None:
    """Ensure alias.* expansions use alias + column order."""
    catalog = _build_catalog()
    context = QueryContext("SELECT u.* FROM testdb.main.users u")
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)

    assert "u.id" in rewritten
    assert "u.name" in rewritten
    assert context.columns[0].internal_name == "testdb.u.id"
    assert context.columns[1].internal_name == "testdb.u.name"


def test_preprocess_missing_table_raises() -> None:
    """Verify missing metadata raises StarExpansionError."""
    catalog = Catalog()
    context = QueryContext("SELECT * FROM testdb.main.unknown")
    preprocessor = QueryPreprocessor(catalog)
    with pytest.raises(StarExpansionError):
        preprocessor.preprocess(context.original_sql, context)


def test_preprocess_rejects_subquery_sources() -> None:
    """Reject subqueries because we cannot enumerate columns."""
    catalog = _build_catalog()
    context = QueryContext("SELECT * FROM (SELECT * FROM testdb.main.users) t")
    preprocessor = QueryPreprocessor(catalog)
    with pytest.raises(StarExpansionError):
        preprocessor.preprocess(context.original_sql, context)


class _StubExecutor:
    """Simple executor stub for processor tests."""

    def __init__(self, sql: str):
        """Initialize stub with a query context."""
        self.query_context = QueryContext(sql)


def test_star_processor_renames_results() -> None:
    """StarExpansionProcessor renames result columns to visible names."""
    catalog = _build_catalog()
    processor = StarExpansionProcessor(catalog)
    executor = _StubExecutor("SELECT * FROM testdb.main.users")
    processor.before_execution(executor)

    table = pa.table(
        {
            "testdb.main.users.id": [1, 2],
            "testdb.main.users.name": ["a", "b"],
        }
    )

    renamed = processor.after_execution(executor, table)
    assert renamed.column_names == ["id", "name"]
