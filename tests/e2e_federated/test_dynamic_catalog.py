"""End-to-end runtime datasource DDL against the real engine.

Creates a PostgreSQL source and a DuckDB source at runtime through SQL, joins
across them, checks SHOW DATASOURCES redacts the password, drops one source
(a later reference must raise while the other keeps working), then builds a
SECOND runtime over the same config to confirm a persisted source is visible to
a new session.
"""

import os
import tempfile

import duckdb
import pytest

from tests.e2e_federated import runtime as runtime_mod

# A password distinct from every other connection field, so the redaction check
# cannot be confounded by the user or database name also appearing.
PG_PASSWORD = "redactme-9f3c7a"


def _pg_params():
    """Return the PostgreSQL connection params the suite seeds and connects to."""
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "database": os.environ.get("POSTGRES_DB", "test_db"),
        "user": os.environ.get("POSTGRES_USER", "postgres"),
    }


def _seed_pg(connection):
    """Seed a unique PostgreSQL schema with a customers table; return its name."""
    schema = "fed_dyncat_" + str(os.getpid())
    cursor = connection.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
    cursor.execute("CREATE SCHEMA " + schema)
    cursor.execute("CREATE TABLE " + schema + ".customers (id INT, name TEXT)")
    cursor.execute("INSERT INTO " + schema + ".customers VALUES (1, 'alice'), (2, 'bob')")
    cursor.close()
    return schema


def _seed_duck():
    """Seed a fresh DuckDB file with an orders table; return its path."""
    directory = tempfile.mkdtemp(prefix="fedq_dyncat_")
    path = os.path.join(directory, "orders.duckdb")
    writer = duckdb.connect(path)
    writer.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT)")
    writer.execute("INSERT INTO orders VALUES (10, 1, 100), (11, 2, 200)")
    writer.close()
    return path


def _write_config():
    """Write a datasource-free config with a source_path; return its path."""
    directory = tempfile.mkdtemp(prefix="fedq_dyncat_cfg_")
    path = os.path.join(directory, "config.yaml")
    handle = open(path, "w")
    handle.write("datasources: {}\n")
    handle.close()
    return path


def _runtime(config_path):
    """Build a native engine runtime over the config at config_path."""
    fedq = runtime_mod._import_fedq()
    return fedq.Runtime(config_path)


def _create_pg(runtime, schema):
    """Run CREATE DATASOURCE for the seeded PostgreSQL schema."""
    params = _pg_params()
    driver = runtime_mod.adbc_driver_path()
    statement = (
        "CREATE DATASOURCE pg_dyn TYPE postgres WITH ("
        "host = '" + params["host"] + "', "
        "port = '" + params["port"] + "', "
        "user = '" + params["user"] + "', "
        "password = '" + PG_PASSWORD + "', "
        "database = '" + params["database"] + "', "
        "schemas = '" + schema + "', "
        "adbc_driver = '" + driver + "')"
    )
    runtime.execute(statement)


def _create_duck(runtime, duck_path):
    """Run CREATE DATASOURCE for the seeded DuckDB file."""
    statement = "CREATE DATASOURCE duck_dyn TYPE duckdb WITH (path = '" + duck_path + "')"
    runtime.execute(statement)


def _rows(runtime, sql):
    """Execute sql and return its result as a list of value tuples."""
    import pyarrow as pa

    table = pa.table(runtime.execute(sql))
    return table.to_pylist()


def _cross_source_join(schema):
    """Return the cross-source join query over the two dynamic sources."""
    return (
        "SELECT c.name AS name, o.amount AS amount "
        "FROM pg_dyn." + schema + ".customers c "
        "JOIN duck_dyn.main.orders o ON o.customer_id = c.id "
        "ORDER BY o.amount"
    )


def _show_text(runtime):
    """Return the whole SHOW DATASOURCES result as one flattened string."""
    rows = _rows(runtime, "SHOW DATASOURCES")
    parts = []
    for row in rows:
        for value in row.values():
            parts.append(str(value))
    return "\n".join(parts)


def _assert_join(runtime, schema):
    """Assert the cross-source join returns the expected two rows."""
    rows = _rows(runtime, _cross_source_join(schema))
    assert rows == [
        {"name": "alice", "amount": 100},
        {"name": "bob", "amount": 200},
    ]


def _assert_show(runtime):
    """Assert SHOW lists both sources and never renders the password."""
    text = _show_text(runtime)
    assert "pg_dyn" in text
    assert "duck_dyn" in text
    assert PG_PASSWORD not in text


def _assert_drop_isolates(runtime, schema):
    """Drop the DuckDB source; its reference must raise, PostgreSQL must work."""
    runtime.execute("DROP DATASOURCE duck_dyn")
    with pytest.raises(Exception):
        _rows(runtime, "SELECT amount FROM duck_dyn.main.orders")
    survivors = _rows(runtime, "SELECT name FROM pg_dyn." + schema + ".customers ORDER BY id")
    assert survivors == [{"name": "alice"}, {"name": "bob"}]


def _assert_new_session_sees_persisted(config_path, schema):
    """A second runtime over the same config resolves the persisted pg source."""
    reader = _runtime(config_path)
    text = _show_text(reader)
    assert "pg_dyn" in text
    assert "duck_dyn" not in text
    rows = _rows(reader, "SELECT name FROM pg_dyn." + schema + ".customers ORDER BY id")
    assert rows == [{"name": "alice"}, {"name": "bob"}]


def test_dynamic_catalog_lifecycle(pg_state):
    """Drive CREATE / cross-join / SHOW / DROP / new-session over the engine."""
    if pg_state.skip_pg:
        pytest.skip("PostgreSQL is disabled for this run")
    schema = _seed_pg(pg_state.connection)
    duck_path = _seed_duck()
    config_path = _write_config()
    runtime = _runtime(config_path)
    _create_pg(runtime, schema)
    _create_duck(runtime, duck_path)
    _assert_join(runtime, schema)
    _assert_show(runtime)
    _assert_drop_isolates(runtime, schema)
    _assert_new_session_sees_persisted(config_path, schema)
