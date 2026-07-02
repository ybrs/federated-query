"""Tests for the Binder."""

import pytest
from federated_query.parser import Parser, Binder, BindingError
from federated_query.catalog import Catalog
from federated_query.catalog.schema import Schema, Table, Column
from federated_query.plan.expressions import DataType


@pytest.fixture
def catalog_with_test_data():
    """Create a catalog with test data."""
    catalog = Catalog()

    # Create a test schema
    schema = Schema(name="public", datasource="testdb")

    # Create users table
    users_table = Table(
        name="users",
        columns=[
            Column(name="id", data_type=DataType.INTEGER, nullable=False),
            Column(name="name", data_type=DataType.VARCHAR, nullable=False),
            Column(name="age", data_type=DataType.INTEGER, nullable=True),
            Column(name="email", data_type=DataType.VARCHAR, nullable=True),
        ],
    )
    schema.add_table(users_table)

    orders_table = Table(
        name="orders",
        columns=[
            Column(name="order_id", data_type=DataType.INTEGER, nullable=False),
            Column(name="amount", data_type=DataType.INTEGER, nullable=True),
        ],
    )
    schema.add_table(orders_table)

    # Register schema
    catalog.schemas[("testdb", "public")] = schema

    return catalog


def test_bind_simple_scan(catalog_with_test_data):
    """Test binding a simple scan."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL
    sql = "SELECT id, name FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    # Should not raise any errors
    assert bound_plan is not None


def test_bind_scan_with_invalid_table(catalog_with_test_data):
    """Test binding fails with invalid table."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with invalid table
    sql = "SELECT id FROM testdb.public.nonexistent"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Should raise BindingError
    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)

    assert "Table not found" in str(exc_info.value)


def test_bind_scan_with_invalid_column(catalog_with_test_data):
    """Test binding fails with invalid column."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with invalid column
    sql = "SELECT id, invalid_column FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Should raise BindingError
    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)

    assert "Column" in str(exc_info.value)
    assert "not found" in str(exc_info.value)


def test_bind_invalid_column_in_compound_predicate_over_join(catalog_with_test_data):
    """An invalid column inside IN/BETWEEN/etc. in a join's WHERE must raise.

    These compound predicates over a join were previously returned unbound, so
    a nonexistent column inside them slipped through unvalidated.
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT u.id FROM testdb.public.users u "
        "JOIN testdb.public.orders o ON u.id = o.order_id "
        "WHERE u.nonexistent IN (1, 2)"
    )
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)
    assert "not found" in str(exc_info.value)


def test_bind_valid_compound_predicate_over_join(catalog_with_test_data):
    """A valid IN/BETWEEN in a join's WHERE still binds without error."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT u.id FROM testdb.public.users u "
        "JOIN testdb.public.orders o ON u.id = o.order_id "
        "WHERE u.name IN ('a', 'b') AND o.amount BETWEEN 1 AND 9"
    )
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    assert binder.bind(plan) is not None


def test_bind_unknown_table_qualifier_over_join_raises(catalog_with_test_data):
    """A column with an unknown table qualifier must raise, not silently rebind.

    `x` is not a table or alias in scope; previously the binder scanned the
    other tables by column name and bound to the first match, accepting a typo.
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT u.id FROM testdb.public.users u "
        "JOIN testdb.public.orders o ON u.id = o.order_id "
        "WHERE x.name = 'a'"
    )
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)
    assert "not found" in str(exc_info.value)


def test_bind_table_name_qualifier_when_aliased_raises(catalog_with_test_data):
    """Once a table is aliased, its real name is no longer a valid qualifier."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT u.id FROM testdb.public.users u "
        "JOIN testdb.public.orders o ON u.id = o.order_id "
        "WHERE users.name = 'a'"
    )
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError):
        binder.bind(plan)


def test_bind_single_table_bogus_qualifier_in_projection_raises(catalog_with_test_data):
    """A bogus qualifier on a single-table query MUST raise, not return rows.

    `x` is not the table or its alias, so `x.id` references a relation the query
    never names. Accepting it (binding `x.id` to `users.id` and returning rows)
    is an EPIC FAIL: the engine would answer an invalid query as if it were
    valid. There is one resolver path; it validates the qualifier and raises.
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT x.id FROM testdb.public.users u"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)
    assert "not found" in str(exc_info.value)


def test_bind_single_table_bogus_qualifier_in_where_raises(catalog_with_test_data):
    """A bogus qualifier in a single-table WHERE must raise, not silently rebind."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT u.id FROM testdb.public.users u WHERE x.name = 'a'"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError) as exc_info:
        binder.bind(plan)
    assert "not found" in str(exc_info.value)


def test_bind_single_table_real_name_qualifier_when_aliased_raises(
    catalog_with_test_data,
):
    """On a single aliased table, the real table name is not a valid qualifier."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT users.id FROM testdb.public.users u"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    with pytest.raises(BindingError):
        binder.bind(plan)


def test_bind_having_function_argument_is_bound(catalog_with_test_data):
    """A column inside a HAVING function is bound, not left unresolved.

    Regression guard: the HAVING binder once fell through to ``return expr`` for
    a FunctionCall, leaving its ColumnRef argument unbound (no data_type) to
    crash later at execution. Routing through the shared dispatch binds it.
    """
    from federated_query.plan.expressions import column_refs

    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT name, COUNT(*) AS n FROM testdb.public.users "
        "GROUP BY name HAVING upper(name) = 'X'"
    )
    bound_plan = binder.bind(parser.parse_to_logical_plan(sql, catalog_with_test_data))
    found = column_refs(bound_plan.predicate)
    assert len(found) > 0
    for ref in found:
        assert ref.data_type is not None


def test_bind_cast_resolves_data_type(catalog_with_test_data):
    """A CAST binds its inner column AND resolves its target into a data_type.

    Compound expressions bind through map_children, but a CAST keeps a binder
    arm: map_children only maps child expressions, so without the special arm the
    target type would stay unresolved.
    """
    from federated_query.plan.expressions import Cast, column_refs

    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT CAST(age AS BIGINT) FROM testdb.public.users"
    bound = binder.bind(parser.parse_to_logical_plan(sql, catalog_with_test_data))
    cast = bound.expressions[0]
    assert isinstance(cast, Cast)
    assert cast.data_type == DataType.BIGINT
    for ref in column_refs(cast):
        assert ref.data_type is not None


def test_bind_compound_expression_binds_every_leaf(catalog_with_test_data):
    """A CASE/BETWEEN compound binds every nested ColumnRef via map_children."""
    from federated_query.plan.expressions import column_refs

    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT CASE WHEN age BETWEEN 18 AND 65 THEN name ELSE email END "
        "FROM testdb.public.users"
    )
    bound = binder.bind(parser.parse_to_logical_plan(sql, catalog_with_test_data))
    refs = column_refs(bound.expressions[0])
    assert len(refs) >= 3
    for ref in refs:
        assert ref.data_type is not None


def test_bind_invalid_column_in_having_should_raise(catalog_with_test_data):
    """A column that exists only inside a HAVING aggregate is rejected at bind.

    The one HAVING binder walks the predicate through the shared expression
    dispatch, so a FunctionCall argument like ``SUM(nonexistent_col)`` is bound
    and an unknown column raises BindingError at bind time (it no longer leaves
    the argument unbound to crash later at execution).
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = (
        "SELECT amount FROM testdb.public.orders "
        "GROUP BY amount HAVING SUM(nonexistent_col) > 10"
    )
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    with pytest.raises(BindingError):
        binder.bind(plan)


def test_bind_with_where_clause(catalog_with_test_data):
    """Test binding with WHERE clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with WHERE
    sql = "SELECT name, age FROM testdb.public.users WHERE age > 18"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_resolves_column_types(catalog_with_test_data):
    """Test that binding resolves column types."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL
    sql = "SELECT id, name FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    # Extract the Projection node
    from federated_query.plan.logical import Projection, Filter, Limit

    # Traverse to find Projection
    current = bound_plan
    while isinstance(current, Limit):
        current = current.input

    if isinstance(current, Projection):
        # Check that expressions have types
        for expr in current.expressions:
            from federated_query.plan.expressions import ColumnRef

            if isinstance(expr, ColumnRef):
                # Should have type set
                assert expr.data_type is not None


def test_bind_with_limit(catalog_with_test_data):
    """Test binding with LIMIT clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with LIMIT
    sql = "SELECT id, name FROM testdb.public.users LIMIT 10"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_with_star_column(catalog_with_test_data):
    """Test binding with SELECT *."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with *
    sql = "SELECT * FROM testdb.public.users"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan - should not fail even with *
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_complex_where(catalog_with_test_data):
    """Test binding with complex WHERE clause."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    # Parse SQL with complex WHERE
    sql = "SELECT name FROM testdb.public.users WHERE age > 18 AND name LIKE 'John%'"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)

    # Bind the plan
    bound_plan = binder.bind(plan)

    assert bound_plan is not None


def test_bind_order_by_alias(catalog_with_test_data):
    """Binder should resolve ORDER BY alias to source column."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)

    sql = "SELECT order_id AS oid FROM testdb.public.orders ORDER BY oid"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    bound_plan = binder.bind(plan)

    from federated_query.plan.logical import Projection, Sort

    assert isinstance(bound_plan, Sort)
    assert isinstance(bound_plan.input, Projection)
    sort_node = bound_plan
    project = bound_plan.input
    assert isinstance(sort_node.sort_keys[0], type(project.expressions[0]))
    assert sort_node.sort_keys[0].column == "order_id"


def test_group_by_alias_of_aggregate_is_rejected(catalog_with_test_data):
    """GROUP BY naming an output alias that is an aggregate is invalid and raises."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT count(*) AS c FROM testdb.public.users GROUP BY c"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    with pytest.raises(BindingError):
        binder.bind(plan)


def test_group_by_output_alias_of_plain_column_is_allowed(catalog_with_test_data):
    """GROUP BY may name a SELECT alias of a plain (non-aggregate) column."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT age AS a, count(*) FROM testdb.public.users GROUP BY a"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    assert binder.bind(plan) is not None


def test_derived_column_alias_rejects_duplicate_output_names(catalog_with_test_data):
    """A column-alias list over a subquery with duplicate output names must raise.

    ``SELECT id, id`` yields two outputs both named ``id``; a positional rename
    read by name would silently give one alias the wrong column, so it raises.
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT y FROM (SELECT id, id FROM testdb.public.users) d(x, y)"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    with pytest.raises(BindingError):
        binder.bind(plan)


def test_derived_column_alias_renames_distinct_outputs(catalog_with_test_data):
    """A column-alias list renames a subquery's distinct outputs positionally."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "SELECT c FROM (SELECT id, name FROM testdb.public.users) d(k, c)"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    assert binder.bind(plan) is not None


def test_cte_column_list_arity_mismatch_raises(catalog_with_test_data):
    """A CTE column list must match the query's output arity, or it raises.

    Too few names would silently drop trailing columns; too many would index
    past the end - both are rejected with a clear BindingError.
    """
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    too_few = "WITH t(a) AS (SELECT id, name FROM testdb.public.users) SELECT a FROM t"
    plan = parser.parse_to_logical_plan(too_few, catalog_with_test_data)
    with pytest.raises(BindingError):
        binder.bind(plan)
    too_many = (
        "WITH t(a, b, c) AS (SELECT id, name FROM testdb.public.users) SELECT a FROM t"
    )
    plan = parser.parse_to_logical_plan(too_many, catalog_with_test_data)
    with pytest.raises(BindingError):
        binder.bind(plan)


def test_cte_column_list_matching_arity_is_allowed(catalog_with_test_data):
    """A CTE column list that matches the output arity renames and binds."""
    parser = Parser()
    binder = Binder(catalog_with_test_data)
    sql = "WITH t(a, b) AS (SELECT id, name FROM testdb.public.users) SELECT a, b FROM t"
    plan = parser.parse_to_logical_plan(sql, catalog_with_test_data)
    assert binder.bind(plan) is not None
