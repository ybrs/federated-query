"""A JOIN ON-clause subquery correlated to the join's RIGHT side.

_rewrite_inner_join_condition rewrites such a subquery against the LEFT input
only, producing a SEMI join whose condition references a relation (the right
side) that is not among the SEMI join's own inputs - a mis-scoped plan. Without
the scope validator this is not caught at the plan layer: single-source it is
masked by pushdown (rendered as one SQL where the reference still resolves),
cross-source it crashes cryptically in the merge engine or silently rebinds the
reference to a same-named left column.

The scope validator rejects the mis-scoped plan loudly: decorrelation raises
ScopeError naming the operator and the out-of-scope qualifier. These tests pin
that loud rejection. Real right-side correlation is not supported; the loud,
precise error is the current behavior, far better than wrong rows or a
downstream crash.
"""

import pytest

from federated_query.cli.fedq import FedQRuntime
from federated_query.config import Config
from federated_query.optimizer.scope_validator import ScopeError


def _make_people(duckdb_datasource, catalog, table):
    """Create a DuckDB people(id, country) table mirroring pg.users."""
    connection = duckdb_datasource.connection
    connection.execute(f"DROP TABLE IF EXISTS {table}")
    connection.execute(f"CREATE TABLE {table} (id INTEGER, country VARCHAR)")
    connection.execute(
        f"INSERT INTO {table} VALUES (1,'US'),(2,'UK'),(3,'US'),(4,'FR'),(5,'UK')"
    )
    catalog.load_metadata()


def test_item1_right_correlated_name_collision_raises_scope_error(
    catalog, setup_test_data, duckdb_datasource
):
    """EXISTS in ON correlating to cities.country (right side) is rejected loudly.

    The decorrelated SEMI join references cities.country, but cities is the outer
    join's right side - not a SEMI-join input. The validator raises ScopeError
    instead of letting the merge engine crash or silently rebind to people.country.
    """
    _make_people(duckdb_datasource, catalog, "people")
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT people.id AS uid "
        "FROM duckdb.main.people people "
        "JOIN default.pg.cities cities ON cities.id = people.id + 1 "
        "AND EXISTS (SELECT 1 FROM default.pg.cities big "
        "WHERE big.country = cities.country AND big.population > 5000000)"
    )
    with pytest.raises(ScopeError) as exc_info:
        runtime.execute(sql)
    assert "cities.country" in str(exc_info.value)


def test_item1_right_correlated_unique_name_raises_scope_error(
    catalog, setup_test_data, duckdb_datasource
):
    """EXISTS in ON correlating to cities.population (right side) is rejected loudly.

    population is unique to cities, so the mis-scoped SEMI reference could not even
    rebind to a left column; the validator raises ScopeError at the plan layer
    rather than deferring to a DuckDB BinderException during execution.
    """
    _make_people(duckdb_datasource, catalog, "people2")
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT people.id AS uid "
        "FROM duckdb.main.people2 people "
        "JOIN default.pg.cities cities ON cities.id = people.id + 1 "
        "AND EXISTS (SELECT 1 FROM default.pg.cities big "
        "WHERE big.id = cities.id AND cities.population > 1000000)"
    )
    with pytest.raises(ScopeError) as exc_info:
        runtime.execute(sql)
    assert "cities" in str(exc_info.value)
