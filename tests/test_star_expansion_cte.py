"""Star expansion over CTEs and derived tables (subqueries in FROM).

The preprocessor used to raise StarExpansionError for any non-base-table
source. These cover expanding ``*`` / ``alias.*`` when the source is a derived
table or a CTE, resolving the columns from the relation's own projection (or a
CTE's explicit column list) instead of the catalog.
"""

import pytest

from federated_query.catalog import Catalog
from federated_query.catalog.schema import Column, Schema, Table
from federated_query.parser import Parser, Binder
from federated_query.plan.expressions import DataType
from federated_query.processor import (
    QueryContext,
    QueryPreprocessor,
    StarExpansionError,
)


def _catalog() -> Catalog:
    """An in-memory catalog: testdb.main with users and orders."""
    catalog = Catalog()
    schema = Schema(name="main", datasource="testdb")
    schema.add_table(Table(name="users", columns=[
        Column(name="id", data_type=DataType.INTEGER, nullable=False),
        Column(name="name", data_type=DataType.VARCHAR, nullable=False),
    ]))
    schema.add_table(Table(name="orders", columns=[
        Column(name="order_id", data_type=DataType.INTEGER, nullable=False),
        Column(name="user_id", data_type=DataType.INTEGER, nullable=False),
    ]))
    catalog.schemas[("testdb", "main")] = schema
    return catalog


def _rewrite(sql: str, context: QueryContext) -> str:
    """Run the preprocessor and return the rewritten SQL."""
    return QueryPreprocessor(_catalog()).preprocess(sql, context)


def _visible_names(context: QueryContext) -> list:
    """The visible column names captured on the context, in order."""
    names = []
    for column in context.columns:
        names.append(column.visible_name)
    return names


def test_derived_table_star_expands_from_its_projection():
    """SELECT * over a derived table expands to its subquery's output columns."""
    sql = "SELECT * FROM (SELECT id, name FROM testdb.main.users) x"
    context = QueryContext(sql)
    rewritten = _rewrite(sql, context)

    assert "x.id" in rewritten
    assert "x.name" in rewritten
    assert "*" not in rewritten
    assert _visible_names(context) == ["id", "name"]
    assert context.columns[0].internal_name == "x.id"


def test_derived_table_qualified_star_selects_only_that_source():
    """x.* over a joined derived table expands only x's columns."""
    sql = (
        "SELECT x.* FROM (SELECT id FROM testdb.main.users) x "
        "JOIN testdb.main.orders o ON x.id = o.user_id"
    )
    rewritten = _rewrite(sql, QueryContext(sql))

    assert "x.id" in rewritten
    assert "o.order_id" not in rewritten
    assert "*" not in rewritten


def test_cte_star_expands_from_body():
    """SELECT * over a CTE expands to the CTE body's output columns."""
    sql = "WITH c AS (SELECT id, name FROM testdb.main.users) SELECT * FROM c"
    context = QueryContext(sql)
    rewritten = _rewrite(sql, context)

    assert "c.id" in rewritten
    assert "c.name" in rewritten
    assert _visible_names(context) == ["id", "name"]


def test_cte_explicit_column_list_overrides_body_names():
    """WITH c(a, b) exposes a, b regardless of the body's own column names."""
    sql = "WITH c(a, b) AS (SELECT id, name FROM testdb.main.users) SELECT * FROM c"
    context = QueryContext(sql)
    rewritten = _rewrite(sql, context)

    assert "c.a" in rewritten
    assert "c.b" in rewritten
    assert _visible_names(context) == ["a", "b"]


def test_nested_subquery_inner_star_expands_first():
    """SELECT * FROM (SELECT * FROM t) x expands both stars (bottom-up)."""
    sql = "SELECT * FROM (SELECT * FROM testdb.main.users) x"
    rewritten = _rewrite(sql, QueryContext(sql))

    # Inner star -> users.id, users.name; outer star -> x.id, x.name.
    assert "users.id" in rewritten
    assert "x.id" in rewritten
    assert "x.name" in rewritten
    assert "*" not in rewritten


def test_cte_self_join_expands_each_alias():
    """A CTE self-joined under two aliases expands each alias's columns."""
    sql = (
        "WITH c AS (SELECT id, name FROM testdb.main.users) "
        "SELECT * FROM c p, c q"
    )
    rewritten = _rewrite(sql, QueryContext(sql))

    assert "p.id" in rewritten
    assert "p.name" in rewritten
    assert "q.id" in rewritten
    assert "q.name" in rewritten


def test_star_over_base_and_derived_together():
    """An unqualified * over a base table and a derived table expands both."""
    sql = (
        "SELECT * FROM testdb.main.orders o, "
        "(SELECT id FROM testdb.main.users) x"
    )
    rewritten = _rewrite(sql, QueryContext(sql))

    assert "o.order_id" in rewritten
    assert "o.user_id" in rewritten
    assert "x.id" in rewritten


def test_union_subquery_star_takes_first_branch_names():
    """A UNION subquery's output names come from its first branch."""
    sql = (
        "SELECT * FROM (SELECT id FROM testdb.main.users "
        "UNION SELECT order_id FROM testdb.main.orders) x"
    )
    context = QueryContext(sql)
    rewritten = _rewrite(sql, context)

    # First branch names the column 'id'; the second branch's name is ignored.
    assert "x.id" in rewritten
    assert _visible_names(context) == ["id"]


def test_values_source_still_raises():
    """A VALUES source has no enumerable columns and must raise."""
    sql = "SELECT * FROM (VALUES (1, 2)) x"
    with pytest.raises(StarExpansionError):
        _rewrite(sql, QueryContext(sql))


def test_expanded_cte_query_binds():
    """End to end: a CTE-star query expands and binds with no star surviving."""
    catalog = _catalog()
    sql = "WITH c AS (SELECT id, name FROM testdb.main.users) SELECT * FROM c"
    context = QueryContext(sql)
    rewritten = QueryPreprocessor(catalog).preprocess(sql, context)

    plan = Parser().parse_to_logical_plan(rewritten, catalog)
    bound = Binder(catalog).bind(plan)
    assert bound is not None
