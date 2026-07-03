"""Tests for per-source dialect-aware pushdown rendering.

The engine generates Postgres-flavored SQL internally and transpiles it to each
source's own dialect via ``to_source_sql`` before sending. This proves the
transpilation turns dialect-divergent syntax into what each source accepts.
"""

from federated_query.datasources.clickhouse import ClickHouseDataSource
from federated_query.datasources.duckdb import DuckDBDataSource
from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.plan.physical import to_source_sql
from tests.duckdb_tmp import duckdb_path


def test_each_source_declares_its_render_dialect():
    """Every connector renders pushed SQL in its own sqlglot dialect."""
    assert PostgreSQLDataSource.render_dialect == "postgres"
    assert DuckDBDataSource.render_dialect == "duckdb"
    assert ClickHouseDataSource.render_dialect == "clickhouse"


def test_to_source_sql_transpiles_functions_for_duckdb():
    """Postgres-form aggregate names transpile to DuckDB's spelling."""
    duck = DuckDBDataSource(name="d", config={"path": duckdb_path()})
    postgres_sql = 'SELECT STRING_AGG(s, \',\') AS a FROM "main"."t"'
    native = to_source_sql(duck, postgres_sql)
    assert "LISTAGG" in native
    assert "STRING_AGG" not in native


def test_to_source_sql_transpiles_tablesample_for_duckdb():
    """Postgres TABLESAMPLE (a bare count) becomes DuckDB's PERCENT form."""
    duck = DuckDBDataSource(name="d", config={"path": duckdb_path()})
    postgres_sql = 'SELECT a FROM "main"."t" TABLESAMPLE BERNOULLI (10)'
    native = to_source_sql(duck, postgres_sql)
    assert "PERCENT" in native


def test_to_source_sql_is_identity_for_postgres():
    """A Postgres source keeps the Postgres-form SQL (no spurious rewrites)."""
    pg = PostgreSQLDataSource(name="p", config={})
    postgres_sql = 'SELECT STRING_AGG(s, \',\') AS a FROM "main"."t"'
    native = to_source_sql(pg, postgres_sql)
    assert "STRING_AGG" in native
    assert "LISTAGG" not in native
