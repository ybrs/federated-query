"""End-to-end probe for the item-1 decorrelation bug (cross-source).

A subquery in a JOIN ON clause that correlates to the join's RIGHT side is
rewritten by _rewrite_inner_join_condition against the LEFT input only. The
resulting SEMI join references a right-side column that is not among the SEMI
join's own inputs.

Single-source pushdown HIDES this: when every table lives in one source the
whole plan is rendered as a single SQL query where the right-side column still
resolves. To expose the bug the SEMI join must run locally in the merge engine,
so the left table lives in DuckDB and the correlated tables in PostgreSQL.

We assert the SQL-correct result. If the bug is live and uncaught these fail
with the wrong row set; if it is caught they error at execution. Either way the
outcome is visible.

PostgreSQL data (pg schema, from conftest):
  cities(id, name, country, population):
    1 New York US 8M, 2 London UK 9M, 3 Boston US 0.7M,
    4 Paris FR 2M, 5 Manchester UK 0.5M, 6 Chicago US 2.7M
DuckDB data (created here):
  people(id, country): 1 US, 2 UK, 3 US, 4 FR, 5 UK  (mirrors pg.users)
"""

import pytest

from federated_query.cli.fedq import FedQRuntime
from federated_query.config import ExecutorConfig


def _make_people(duckdb_datasource, catalog, table):
    """Create a DuckDB people(id, country) table mirroring pg.users."""
    connection = duckdb_datasource.connection
    connection.execute(f"DROP TABLE IF EXISTS {table}")
    connection.execute(f"CREATE TABLE {table} (id INTEGER, country VARCHAR)")
    connection.execute(
        f"INSERT INTO {table} VALUES (1,'US'),(2,'UK'),(3,'US'),(4,'FR'),(5,'UK')"
    )
    catalog.load_metadata()


def _uids(table):
    """Collect the uid column from a result Arrow table into a set."""
    found = set()
    for row in table.to_pylist():
        found.add(row["uid"])
    return found


@pytest.mark.xfail(
    strict=True,
    reason="item-1: ON-clause subquery correlated to the join's RIGHT side is "
    "rewritten against the LEFT input only. Cross-source the SEMI join runs in "
    "the merge engine and the correlated cities.country reference cannot resolve "
    "from its (people, big) inputs, so execution crashes (pyarrow KeyError "
    "'Field country does not exist') instead of decorrelation raising a clean "
    "DecorrelationError. Single-source the same plan is masked by pushdown.",
)
def test_item1_right_correlated_name_collision(
    catalog, setup_test_data, duckdb_datasource
):
    """EXISTS in ON correlating to cities.country (right); people also has country.

    Join pairs each person with the NEXT city id (cities.id = people.id + 1), so
    the person's own country and the joined city's country DIFFER. The EXISTS
    keeps a row only when the CITY's country has a city over 5,000,000 people
    (US via New York, UK via London; FR does not qualify).

    Correct (uses cities.country):
      uid 1 -> city 2 London  UK -> keep
      uid 2 -> city 3 Boston  US -> keep
      uid 3 -> city 4 Paris   FR -> DROP
      uid 4 -> city 5 Manch.  UK -> keep
      uid 5 -> city 6 Chicago US -> keep
    Correct uid set = {1, 2, 4, 5}.

    If the rewrite binds cities.country to people.country instead, uid 3 (US) is
    wrongly kept and uid 4 (FR) wrongly dropped -> {1, 2, 3, 5}.
    """
    _make_people(duckdb_datasource, catalog, "people")
    runtime = FedQRuntime(catalog, ExecutorConfig())
    sql = (
        "SELECT people.id AS uid "
        "FROM duckdb.main.people people "
        "JOIN default.pg.cities cities ON cities.id = people.id + 1 "
        "AND EXISTS (SELECT 1 FROM default.pg.cities big "
        "WHERE big.country = cities.country AND big.population > 5000000)"
    )
    result = _uids(runtime.execute(sql))
    assert result == {1, 2, 4, 5}, f"expected SQL-correct {{1,2,4,5}}, got {result}"


@pytest.mark.xfail(
    strict=True,
    reason="item-1: ON-clause subquery correlated to the join's RIGHT side is "
    "rewritten against the LEFT input only. The merge SEMI join emits "
    "cities.population, a column not among its inputs, so DuckDB raises "
    "BinderException 'Referenced table cities not found' instead of "
    "decorrelation raising a clean DecorrelationError. Note cities.id in the "
    "same condition was SILENTLY rebound to l.id (people.id) by bare-name "
    "fallback - the silent mis-binding the rule forbids.",
)
def test_item1_right_correlated_unique_name(
    catalog, setup_test_data, duckdb_datasource
):
    """EXISTS in ON correlating to cities.population (a name unique to the right).

    people has no 'population' column, so when the rewrite attaches the SEMI join
    to the left (people) input, the correlated reference cities.population cannot
    resolve there. Correct result keeps every joined row whose city population
    exceeds 1,000,000:
      uid 1 -> city 2 London  9M  -> keep
      uid 2 -> city 3 Boston  0.7M-> DROP
      uid 3 -> city 4 Paris   2M  -> keep
      uid 4 -> city 5 Manch.  0.5M-> DROP
      uid 5 -> city 6 Chicago 2.7M-> keep
    Correct uid set = {1, 3, 5}.
    """
    _make_people(duckdb_datasource, catalog, "people2")
    runtime = FedQRuntime(catalog, ExecutorConfig())
    sql = (
        "SELECT people.id AS uid "
        "FROM duckdb.main.people2 people "
        "JOIN default.pg.cities cities ON cities.id = people.id + 1 "
        "AND EXISTS (SELECT 1 FROM default.pg.cities big "
        "WHERE big.id = cities.id AND cities.population > 1000000)"
    )
    result = _uids(runtime.execute(sql))
    assert result == {1, 3, 5}, f"expected SQL-correct {{1,3,5}}, got {result}"
