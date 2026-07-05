"""End-to-end differential tests for cost-based join reordering (M7).

Every query runs twice through real DuckDB sources - once with join
reordering enabled (the default) and once disabled - and both result sets
must equal the single-connection DuckDB oracle. Reordering must never change
WHAT a query returns, only how fast.
"""

import duckdb
import pytest

from federated_query.catalog import Catalog
from federated_query.cli.fedq import FedQRuntime
from federated_query.config import Config
from federated_query.config.config import OptimizerConfig
from federated_query.datasources.duckdb import DuckDBDataSource
from tests.duckdb_tmp import duckdb_path


# Deterministic per-table seeds: the oracle runs all of them; each source
# runs exactly the ones for its tables, so every copy is identical.
_TABLE_SEEDS = {
    "region": (
        "CREATE TABLE region (r_id INTEGER, r_name VARCHAR);"
        "INSERT INTO region SELECT g, 'R' || (g % 2) FROM range(0, 4) t(g);"
    ),
    "nation": (
        "CREATE TABLE nation (n_id INTEGER, n_r INTEGER, n_name VARCHAR);"
        "INSERT INTO nation SELECT g, g % 4, 'N' || g FROM range(0, 10) t(g);"
    ),
    "supplier": (
        "CREATE TABLE supplier (s_id INTEGER, s_n INTEGER);"
        "INSERT INTO supplier SELECT g, g % 10 FROM range(0, 100) t(g);"
    ),
    "customer": (
        "CREATE TABLE customer (c_id INTEGER, c_n INTEGER);"
        "INSERT INTO customer SELECT g, g % 10 FROM range(0, 200) t(g);"
    ),
    "orders": (
        "CREATE TABLE orders (o_id INTEGER, o_c INTEGER);"
        "INSERT INTO orders SELECT g, g % 200 FROM range(0, 1000) t(g);"
    ),
    "lineitem": (
        "CREATE TABLE lineitem (l_o INTEGER, l_s INTEGER, l_qty INTEGER);"
        "INSERT INTO lineitem SELECT g % 1000, g % 100, g % 7"
        " FROM range(0, 5000) t(g);"
    ),
}

# The dims/facts split: every fact-dimension join crosses sources, so the
# reordered joins execute on the coordinator (the path under test).
_DIMS = ("region", "nation", "supplier", "customer")
_FACTS = ("orders", "lineitem")


def _runtime(config) -> FedQRuntime:
    """A two-DuckDB-source runtime over the seeded star schema."""
    dims = _source_with("dims", _DIMS)
    facts = _source_with("facts", _FACTS)
    catalog = Catalog()
    catalog.register_datasource(dims)
    catalog.register_datasource(facts)
    catalog.load_metadata()
    return FedQRuntime(catalog, config)


def _source_with(name, tables) -> DuckDBDataSource:
    """A DuckDB source holding the named seeded tables."""
    source = DuckDBDataSource(name, {"path": duckdb_path(), "read_only": False})
    source.connect()
    for table in tables:
        source.connection.execute(_TABLE_SEEDS[table])
    return source


def _oracle():
    """A single DuckDB connection holding every table (the truth)."""
    connection = duckdb.connect()
    for seed in _TABLE_SEEDS.values():
        connection.execute(seed)
    return connection


def _qualify(sql: str) -> str:
    """Qualify each bare table to the source that holds it."""
    qualified = sql
    for table in _DIMS:
        qualified = qualified.replace(f" {table} ", f" dims.main.{table} ")
    for table in _FACTS:
        qualified = qualified.replace(f" {table} ", f" facts.main.{table} ")
    return qualified


def _rows(table) -> set:
    """An Arrow table as a set of positional row tuples."""
    rows = set()
    for index in range(table.num_rows):
        values = []
        for column in table.columns:
            values.append(column[index].as_py())
        rows.add(tuple(values))
    return rows


# Star and cyclic shapes in deliberately BAD FROM order (cross products and
# big-tables-first), all with a deterministic aggregate output.
_QUERIES = (
    # q09 shape: supplier x customer share no predicate in FROM order.
    "SELECT count(*) AS n FROM supplier , customer , lineitem , orders "
    "WHERE l_s = s_id AND l_o = o_id AND o_c = c_id",
    # Snowflake chain written facts-last.
    "SELECT n_name, count(*) AS n FROM region , nation , supplier , lineitem "
    "WHERE n_r = r_id AND s_n = n_id AND l_s = s_id AND r_name = 'R1' "
    "GROUP BY n_name ORDER BY n_name",
    # Cyclic (q05 shape): customer-supplier via shared nation AND via
    # orders-lineitem.
    "SELECT count(*) AS n FROM customer , orders , lineitem , supplier "
    "WHERE c_id = o_c AND l_o = o_id AND l_s = s_id AND c_n = s_n",
    # Aggregate over a 5-way join with a local filter.
    "SELECT sum(l_qty) AS q FROM nation , supplier , customer , orders , lineitem "
    "WHERE s_n = n_id AND c_n = n_id AND o_c = c_id AND l_o = o_id "
    "AND l_s = s_id AND n_id = 3",
)


@pytest.mark.parametrize("sql", _QUERIES)
def test_reordering_preserves_results(sql):
    """Reordered and FROM-order execution both match the DuckDB oracle."""
    oracle_rows = set()
    for row in _oracle().execute(sql).fetchall():
        oracle_rows.add(tuple(row))
    reordered = _runtime(Config()).execute(_qualify(sql))
    kept = _runtime(
        Config(optimizer=OptimizerConfig(enable_join_reordering=False))
    ).execute(_qualify(sql))
    assert _rows(reordered) == oracle_rows
    assert _rows(kept) == oracle_rows


def test_reordered_cross_source_join_still_gets_dynamic_filter():
    """Reordering must not lose the dynamic-filter marking on cross-source
    hash joins (semi-join reduction rides on it)."""
    runtime = _runtime(Config())
    sql = _qualify(
        "SELECT count(*) AS n FROM supplier , customer , lineitem , orders "
        "WHERE l_s = s_id AND l_o = o_id AND o_c = c_id"
    )
    plan = runtime.query_executor._plan_pipeline(sql, profiler=None)
    marked = []
    _collect_dynamic_filters(plan, marked)
    assert marked, "no scan carries dynamic_filter_keys after reordering"


def _collect_dynamic_filters(node, found):
    """Collect every physical scan annotated with dynamic filter keys."""
    keys = getattr(node, "dynamic_filter_keys", None)
    if keys:
        found.append(node)
    for child in node.children():
        _collect_dynamic_filters(child, found)
