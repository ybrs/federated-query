"""Regression: SELECT * over a same-source join with overlapping column names.

Both ``users`` and ``orders`` expose an ``id`` column, so a pushed-down
``SELECT *`` join returns two columns named ``id``. The PostgreSQL connector
must build the result positionally so the duplicate names round-trip instead of
collapsing, which would raise "Arrays were not all the same length".
"""

from federated_query.cli.fedq import FedQRuntime
from federated_query.config import Config


def test_select_star_join_with_duplicate_column_names(catalog, setup_test_data):
    """SELECT * over a join keeps duplicate column names (both id columns),
    matching PostgreSQL, and returns the expected row/column counts."""
    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT * FROM default.pg.users U, default.pg.orders O "
        "WHERE O.user_id = U.id LIMIT 1"
    )
    table = runtime.execute(sql)

    assert table.num_rows == 1
    # both id columns survive (users.id and orders.id), like PostgreSQL
    assert table.schema.names.count("id") == 2
    assert table.num_columns == 8


def test_cross_source_join_over_pushed_duplicate_columns(
    catalog, setup_test_data, duckdb_datasource
):
    """A same-source join that produces two ``id`` columns is pushed down, then
    joined cross-source on one of them. The pushed query must expose unique
    output names so the outer join can resolve the qualified key."""
    connection = duckdb_datasource.connection
    connection.execute("CREATE TABLE file_access (file_id INTEGER, label VARCHAR)")
    connection.execute("INSERT INTO file_access VALUES (1, 'a'), (2, 'b')")
    catalog.load_metadata()

    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT count(*) "
        "FROM default.pg.users U, default.pg.orders O, duckdb.main.file_access A "
        "WHERE O.user_id = U.id AND U.id = 1 AND O.id = A.file_id"
    )
    table = runtime.execute(sql)
    assert table.to_pylist() == [{"COUNT(*)": 2}]


def test_pushed_subquery_only_fetches_needed_columns(
    catalog, setup_test_data, duckdb_datasource
):
    """A pushed cross-source sub-join must emit only the columns later operators
    need (its join keys), not every table column (projection pruning, P1)."""
    connection = duckdb_datasource.connection
    connection.execute("CREATE TABLE file_access2 (file_id INTEGER, label VARCHAR)")
    connection.execute("INSERT INTO file_access2 VALUES (1, 'a')")
    catalog.load_metadata()

    runtime = FedQRuntime(catalog, Config())
    sql = (
        "SELECT count(*) "
        "FROM default.pg.users U, default.pg.orders O, duckdb.main.file_access2 A "
        "WHERE O.user_id = U.id AND U.id = 1 AND O.id = A.file_id"
    )
    plan = "\n".join(
        row["plan"] for row in runtime.execute("EXPLAIN " + sql).to_pylist()
    )
    pg_line = next(
        line for line in plan.splitlines() if "pg" in line and "SELECT" in line
    )
    # only the join-key columns are fetched; the wide columns are pruned away
    assert "user_id" in pg_line
    for pruned in ("name", "country", "city", "amount", "status"):
        assert pruned not in pg_line, f"{pruned} should have been pruned: {pg_line}"
