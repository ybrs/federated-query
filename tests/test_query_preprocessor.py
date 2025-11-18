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
    """Create an in-memory catalog with sample tables."""
    catalog = Catalog()
    schema = Schema(name="main", datasource="testdb")
    users_table = Table(
        name="users",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="name", data_type=DataType.VARCHAR, nullable=False),
            Column(name="email", data_type=DataType.VARCHAR, nullable=True),
            Column(name="active", data_type=DataType.BOOLEAN, nullable=True),
        ],
    )
    schema.add_table(users_table)
    orders_table = Table(
        name="orders",
        columns=[
            Column(name="order_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="user_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="product_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="region", data_type=DataType.VARCHAR, nullable=False),
            Column(name="quantity", data_type=DataType.INTEGER, nullable=False),
            Column(name="order_date", data_type=DataType.DATE, nullable=False),
        ],
    )
    schema.add_table(orders_table)
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
    assert len(context.columns) == 4
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
            "testdb.main.users.email": ["a@example.com", "b@example.com"],
            "testdb.main.users.active": [True, False],
        }
    )

    renamed = processor.after_execution(executor, table)
    assert renamed.column_names == ["id", "name", "email", "active"]


def test_preprocess_skips_subquery_without_star() -> None:
    """Ensure derived tables without stars pass through unchanged."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        SELECT id, name
        FROM (
            SELECT id, name, email FROM testdb.main.users WHERE active = true
        ) AS active_users
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    assert "active_users" in rewritten
    assert not context.columns


def test_preprocess_skips_join_with_subquery_without_star() -> None:
    """Ensure joins with derived tables work when no star exists."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        SELECT u.id, u.name, o.total
        FROM testdb.main.users u
        JOIN (
            SELECT user_id, SUM(quantity) AS total
            FROM testdb.main.orders
            GROUP BY user_id
        ) o ON u.id = o.user_id
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    assert "o.total" in rewritten
    assert not context.columns


def test_preprocess_skips_cte_without_star() -> None:
    """Ensure CTEs without stars do not raise errors."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        WITH recent_orders AS (
            SELECT user_id, COUNT(*) AS order_count
            FROM testdb.main.orders
            WHERE order_date > '2024-01-01'
            GROUP BY user_id
        )
        SELECT u.id, u.name, r.order_count
        FROM testdb.main.users u
        JOIN recent_orders r ON u.id = r.user_id
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    assert "recent_orders" in rewritten
    assert not context.columns


def test_preprocess_cte_with_star_records_columns() -> None:
    """Ensure outer SELECT after CTE records column metadata."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        WITH recent_orders AS (
            SELECT user_id, COUNT(*) AS order_count
            FROM testdb.main.orders
            GROUP BY user_id
        )
        SELECT * FROM testdb.main.users
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    preprocessor.preprocess(context.original_sql, context)
    assert len(context.columns) == 4


def test_preprocess_union_star_records_columns() -> None:
    """Ensure union roots still capture column metadata."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        SELECT * FROM testdb.main.users
        UNION
        SELECT * FROM testdb.main.users
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    preprocessor.preprocess(context.original_sql, context)
    assert len(context.columns) == 4


def test_preprocess_expands_join_alias_star() -> None:
    """Expand table2.* in joins without altering order."""
    catalog = _build_catalog()
    context = QueryContext(
        """
        SELECT u.id, o.*
        FROM testdb.main.users u
        JOIN testdb.main.orders o ON u.id = o.user_id
        """
    )
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    assert "o.order_id" in rewritten
    assert len(context.columns) == 7
    assert context.columns[0].internal_name == "testdb.u.id"
    assert context.columns[1].internal_name == "testdb.o.order_id"
    assert context.columns[-1].visible_name == "order_date"


def test_preprocess_expands_prefix_suffix_stars() -> None:
    """Ensure explicit columns before and after '*' are preserved."""
    catalog = _build_catalog()
    context = QueryContext(
        "SELECT order_id, *, order_id FROM testdb.main.orders"
    )
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    prefix = rewritten.split("FROM")[0]
    first_index = prefix.find("order_id")
    middle_index = prefix.find("orders.product_id")
    last_index = prefix.rfind("order_id")
    assert first_index != -1
    assert middle_index > first_index
    assert last_index > middle_index
    assert len(context.columns) == 8


def test_preprocess_expands_alias_then_star() -> None:
    """Ensure aliases remain when '*' is present."""
    catalog = _build_catalog()
    context = QueryContext("SELECT id AS user_id, * FROM testdb.main.users")
    preprocessor = QueryPreprocessor(catalog)
    rewritten = preprocessor.preprocess(context.original_sql, context)
    assert "AS user_id" in rewritten
    assert "users.email" in rewritten
    assert len(context.columns) == 5
