"""Unit tests for scoped subquery binding.

Uses an in-memory DuckDB catalog so no external services are needed.
"""

import pytest

from federated_query.catalog import Catalog
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder, BindingError
from federated_query.plan.logical import Filter, Projection, Scan, SubqueryScan
from federated_query.plan.expressions import (
    ColumnRef,
    ExistsExpression,
    InSubquery,
    SubqueryExpression,
    BinaryOp,
)
from tests.duckdb_tmp import duckdb_path


@pytest.fixture(scope="module")
def catalog():
    """Catalog with users/orders tables in an in-memory DuckDB."""
    ds = DuckDBDataSource(
        name="default", config={"path": duckdb_path(), "read_only": False}
    )
    ds.connect()
    ds.connection.execute(
        "CREATE SCHEMA pg;"
        "CREATE TABLE pg.users (id INTEGER, name VARCHAR, country VARCHAR, city VARCHAR);"
        "CREATE TABLE pg.orders (id INTEGER, user_id INTEGER, amount DOUBLE, status VARCHAR);"
    )
    catalog = Catalog()
    catalog.register_datasource(ds)
    catalog.load_metadata()
    yield catalog
    ds.disconnect()


def bind(catalog, sql):
    """Parse and bind a query."""
    plan = Parser().parse(sql)
    return Binder(catalog).bind(plan)


def find_filter(plan):
    """Find the first Filter node in a plan tree."""
    stack = [plan]
    while stack:
        node = stack.pop()
        if isinstance(node, Filter):
            return node
        for child in node.children():
            stack.append(child)
    raise AssertionError("No Filter node found")


def test_correlated_exists_subquery_is_bound(catalog):
    """The subquery plan inside EXISTS must come back bound with types."""
    bound = bind(
        catalog,
        "SELECT u.id FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.id)",
    )
    predicate = find_filter(bound).predicate
    assert isinstance(predicate, ExistsExpression)
    inner_filter = find_filter(predicate.subquery)
    condition = inner_filter.predicate
    assert isinstance(condition, BinaryOp)
    assert condition.left.data_type is not None
    assert condition.right.data_type is not None
    assert condition.right.table == "u"


def test_unqualified_ref_resolves_inner_first(catalog):
    """An unqualified name existing in the inner table binds inner."""
    bound = bind(
        catalog,
        "SELECT * FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE id = u.id)",
    )
    predicate = find_filter(bound).predicate
    inner_filter = find_filter(predicate.subquery)
    # 'id' exists in orders, so SQL scoping resolves it to the inner table.
    assert inner_filter.predicate.left.table == "o"


def test_unqualified_outer_ref_resolves_outward(catalog):
    """A name absent from the inner table resolves to the outer scope."""
    bound = bind(
        catalog,
        "SELECT * FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.amount > 0 AND country = 'US')",
    )
    predicate = find_filter(bound).predicate
    inner_filter = find_filter(predicate.subquery)
    right_side = inner_filter.predicate.right
    assert right_side.left.table == "u"
    assert right_side.left.column == "country"


def test_unknown_table_in_subquery_raises(catalog):
    """A reference to a nonexistent alias must raise BindingError."""
    with pytest.raises(BindingError, match="nonexistent"):
        bind(
            catalog,
            "SELECT * FROM pg.users u "
            "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = nonexistent.id)",
        )


def test_unknown_column_in_subquery_raises(catalog):
    """A column that exists nowhere must raise BindingError."""
    with pytest.raises(BindingError, match="no_such_column"):
        bind(
            catalog,
            "SELECT * FROM pg.users u "
            "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE no_such_column = 1)",
        )


def test_outer_columns_removed_from_subquery_scan(catalog):
    """Outer column names collected by the parser are dropped from the
    subquery's scan so only real table columns are fetched."""
    bound = bind(
        catalog,
        "SELECT * FROM pg.users u "
        "WHERE EXISTS (SELECT 1 FROM pg.orders o WHERE o.user_id = u.city)",
    )
    predicate = find_filter(bound).predicate
    inner_scan = predicate.subquery
    while not isinstance(inner_scan, Scan):
        inner_scan = inner_scan.children()[0]
    assert "city" not in inner_scan.columns


def test_scalar_subquery_in_projection_bound(catalog):
    """Scalar subqueries in the SELECT list get bound plans."""
    bound = bind(
        catalog,
        "SELECT u.id, (SELECT MAX(o.amount) FROM pg.orders o WHERE o.user_id = u.id) AS m "
        "FROM pg.users u",
    )
    assert isinstance(bound, Projection)
    scalar = bound.expressions[1]
    assert isinstance(scalar, SubqueryExpression)
    inner_filter = find_filter(scalar.subquery)
    assert inner_filter.predicate.right.table == "u"


def test_in_subquery_value_bound_in_outer_context(catalog):
    """The IN value binds against the outer table, the plan inside."""
    bound = bind(
        catalog,
        "SELECT * FROM pg.users WHERE country IN " "(SELECT status FROM pg.orders)",
    )
    predicate = find_filter(bound).predicate
    assert isinstance(predicate, InSubquery)
    assert isinstance(predicate.value, ColumnRef)
    assert predicate.value.data_type is not None


def test_derived_table_column_binds(catalog):
    """Columns of a derived table resolve via its synthetic schema."""
    bound = bind(
        catalog,
        "SELECT dt.id FROM (SELECT id, name FROM pg.users) dt WHERE dt.id > 1",
    )
    filter_node = find_filter(bound)
    assert filter_node.predicate.left.data_type is not None
    scan = bound
    while not isinstance(scan, SubqueryScan):
        scan = scan.children()[0]
    assert scan.alias == "dt"
